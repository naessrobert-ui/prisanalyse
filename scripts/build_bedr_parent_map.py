from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    Retry = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


API_BASE = "https://data.brreg.no/enhetsregisteret/api/underenheter"
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
BATCH_SIZE = int(os.getenv("MAP_BATCH_SIZE", "500"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.05"))
ONLY_BUILD_QUEUE = os.getenv("ONLY_BUILD_QUEUE", "0") == "1"
AUTO_REBUILD_QUEUE = os.getenv("AUTO_REBUILD_QUEUE", "0") == "1"
QUEUE_TABLE = os.getenv("QUEUE_TABLE", "public.regnskap_queue")
MAP_TABLE = os.getenv("MAP_TABLE", "public.bedr_parent_map")
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
DB_RETRIES = int(os.getenv("DB_RETRIES", "6"))


def load_env() -> None:
    if load_dotenv is None:
        return
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    load_dotenv()
    load_dotenv(script_dir / ".env")
    load_dotenv(project_root / ".env")


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("Mangler DATABASE_URL")
    return url


def connect_db_with_retry(database_url: str) -> psycopg.Connection:
    last_exc: Exception | None = None
    for attempt in range(1, DB_RETRIES + 1):
        try:
            return psycopg.connect(database_url, connect_timeout=DB_CONNECT_TIMEOUT)
        except psycopg.OperationalError as exc:
            last_exc = exc
            wait_s = min(60, 2 ** (attempt - 1))
            print(f"DB connect feilet (forsøk {attempt}/{DB_RETRIES}): {exc}. Venter {wait_s}s ...")
            time.sleep(wait_s)
    raise SystemExit(f"Klarte ikke koble til databasen: {last_exc}")


def reconnect_db(conn: psycopg.Connection | None, database_url: str) -> psycopg.Connection:
    try:
        if conn is not None and not conn.closed:
            conn.close()
    except Exception:
        pass
    return connect_db_with_retry(database_url)


def create_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "prisanalyse-bldr-parent-map/1.1",
        }
    )
    if Retry is not None:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    return session


def fetch_json(session: requests.Session, orgnr: str) -> tuple[int | None, dict[str, Any] | None]:
    url = f"{API_BASE}/{orgnr}"
    try:
        resp = session.get(url, timeout=TIMEOUT)
    except requests.RequestException:
        return None, None

    status = resp.status_code
    if status == 200:
        try:
            return status, resp.json()
        except Exception:
            return status, None
    return status, None


def find_key(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = find_key(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_key(item, key)
            if found is not None:
                return found
    return None


def resolve_parent_orgnr(session: requests.Session, bedr_orgnr: str) -> tuple[str | None, str, int | None]:
    status, payload = fetch_json(session, bedr_orgnr)
    if status == 200 and payload:
        parent = find_key(payload, "overordnetEnhet")
        if isinstance(parent, str) and parent.isdigit() and len(parent) == 9:
            return parent, "brreg_underenhet_api", 200
        return None, "missing_overordnetEnhet", 200
    if status == 404:
        return None, "404_not_found", 404
    if status == 410:
        return None, "410_gone", 410
    if status is None:
        return None, "request_exception", None
    return None, f"http_{status}", status


def select_unmapped_bedr(conn: psycopg.Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select e.orgnr
            from public.entity e
            left join {MAP_TABLE} m
              on m.bedr_orgnr = e.orgnr
            where upper(trim(e.orgform)) = 'BEDR'
              and coalesce(e.ansatte, 0) >= 1
              and m.bedr_orgnr is null
            order by e.orgnr
            limit %s
            """,
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]


def upsert_mapping(
    conn: psycopg.Connection,
    bedr_orgnr: str,
    parent_orgnr: str | None,
    resolved_via: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {MAP_TABLE} (bedr_orgnr, parent_orgnr, resolved_via, resolved_at)
            values (%s, %s, %s, now())
            on conflict (bedr_orgnr) do update
            set parent_orgnr = excluded.parent_orgnr,
                resolved_via = excluded.resolved_via,
                resolved_at = now()
            """,
            (bedr_orgnr, parent_orgnr, resolved_via),
        )
    conn.commit()


def print_progress(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
                count(*) as mapped_total,
                count(*) filter (where parent_orgnr is not null) as mapped_ok,
                count(*) filter (where parent_orgnr is null) as mapped_missing
            from {MAP_TABLE}
            """
        )
        mapped_total, mapped_ok, mapped_missing = cur.fetchone()

        cur.execute(
            """
            select count(*)
            from public.entity
            where upper(trim(orgform)) = 'BEDR'
              and coalesce(ansatte, 0) >= 1
            """
        )
        total_bedr = cur.fetchone()[0]

    print(
        f"Mappingstatus: total_bedr={total_bedr} mapped_total={mapped_total} "
        f"mapped_ok={mapped_ok} mapped_missing={mapped_missing}"
    )


def rebuild_queue_from_mapped_parents(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"truncate table {QUEUE_TABLE}")
        cur.execute(
            f"""
            insert into {QUEUE_TABLE}
            (orgnr, status, attempts, locked_at, last_http_status, last_error, last_fetched_at)
            select distinct
                trim(m.parent_orgnr),
                'pending',
                0,
                null::timestamptz,
                null::integer,
                null::text,
                null::timestamptz
            from {MAP_TABLE} m
            join public.entity e
              on e.orgnr = m.bedr_orgnr
            where upper(trim(e.orgform)) = 'BEDR'
              and coalesce(e.ansatte, 0) >= 1
              and nullif(trim(m.parent_orgnr), '') is not null
              and not exists (
                  select 1
                  from public.regnskap_siste r
                  where r.orgnr = trim(m.parent_orgnr)
              )
            """
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {QUEUE_TABLE}")
        count = cur.fetchone()[0]
    print(f"Ny regnskapskø bygget: {count} unike hovedenheter")


def main() -> int:
    load_env()
    database_url = get_database_url()
    session = create_http_session()
    conn: psycopg.Connection | None = None

    try:
        conn = connect_db_with_retry(database_url)
        print_progress(conn)

        if ONLY_BUILD_QUEUE:
            rebuild_queue_from_mapped_parents(conn)
            return 0

        processed = 0
        while True:
            try:
                batch = select_unmapped_bedr(conn, BATCH_SIZE)
            except psycopg.OperationalError as exc:
                print(f"DB-feil ved valg av batch: {exc}. Kobler opp på nytt ...")
                conn = reconnect_db(conn, database_url)
                continue

            if not batch:
                break

            for bedr_orgnr in batch:
                parent_orgnr, resolved_via, _ = resolve_parent_orgnr(session, bedr_orgnr)

                saved = False
                while not saved:
                    try:
                        upsert_mapping(conn, bedr_orgnr, parent_orgnr, resolved_via)
                        saved = True
                    except psycopg.OperationalError as exc:
                        print(f"DB-feil ved lagring av {bedr_orgnr}: {exc}. Kobler opp på nytt ...")
                        conn = reconnect_db(conn, database_url)

                processed += 1
                if processed % 100 == 0:
                    print(f"Prosessert {processed} BEDR")
                if SLEEP_BETWEEN > 0:
                    time.sleep(SLEEP_BETWEEN)

            try:
                print_progress(conn)
            except psycopg.OperationalError as exc:
                print(f"DB-feil ved progresjonsrapportering: {exc}. Kobler opp på nytt ...")
                conn = reconnect_db(conn, database_url)
                print_progress(conn)

        if AUTO_REBUILD_QUEUE:
            try:
                rebuild_queue_from_mapped_parents(conn)
            except psycopg.OperationalError as exc:
                print(f"DB-feil ved købygging: {exc}. Kobler opp på nytt ...")
                conn = reconnect_db(conn, database_url)
                rebuild_queue_from_mapped_parents(conn)
        else:
            print("Ferdig med mapping. Hopper over rebuild av regnskapskø (AUTO_REBUILD_QUEUE=0).")

        print("Ferdig")
        return 0
    finally:
        try:
            session.close()
        except Exception:
            pass
        try:
            if conn is not None and not conn.closed:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
