#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import psycopg
import requests

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# Last .env fra vanligste steder, hvis python-dotenv er installert.
if load_dotenv is not None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    load_dotenv()
    load_dotenv(script_dir / ".env")
    load_dotenv(project_root / ".env")
    load_dotenv(Path.cwd() / ".env")


DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit(
        "Mangler DATABASE_URL. Legg den i miljøet eller i .env i prosjektroten."
    )

BASE_URL = os.environ.get(
    "BRREG_REGNSKAP_URL",
    "https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
)

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "0.02"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP", "8"))
LOOP = os.environ.get("LOOP", "1") == "1"
DEBUG_LOG = os.environ.get("DEBUG_LOG", "0") == "1"
DEBUG_DB = os.environ.get("DEBUG_DB", "0") == "1"

QUEUE_TABLE = "public.regnskap_queue"
SISTE_TABLE = "public.regnskap_siste"
ENTITY_TABLE = "public.entity"

FILTER_ORGFORM = os.environ.get("FILTER_ORGFORM", "BEDR").strip().upper()
FILTER_MIN_ANSATTE = int(os.environ.get("FILTER_MIN_ANSATTE", "1"))
REBUILD_QUEUE = os.environ.get("REBUILD_QUEUE", "0") == "1"
QUEUE_ONLY = os.environ.get("QUEUE_ONLY", "0") == "1"

JsonDict = dict[str, Any]
PathLike = tuple[str, ...]


def log(msg: str) -> None:
    if DEBUG_LOG:
        print(msg, file=sys.stderr)


def payload_hash(payload: Any) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def normalize_orgnr(orgnr: str) -> str:
    return "".join(ch for ch in str(orgnr or "") if ch.isdigit())


def as_list(data: Any) -> list[JsonDict]:
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        likely_regnskap = (
            "regnskapsperiode" in data
            or "journalnr" in data
            or "resultatregnskapResultat" in data
            or "eiendeler" in data
            or "egenkapitalGjeld" in data
        )
        return [data] if likely_regnskap else []
    return []


def pick_latest(items: list[JsonDict]) -> JsonDict | None:
    def sort_key(item: JsonDict) -> tuple[str, str, str]:
        periode = item.get("regnskapsperiode") or {}
        til = str(periode.get("tilDato") or "")
        fra = str(periode.get("fraDato") or "")
        journalnr = str(item.get("journalnr") or "")
        return (til, fra, journalnr)

    valid = [x for x in items if isinstance(x, dict)]
    if not valid:
        return None
    return sorted(valid, key=sort_key, reverse=True)[0]


def get_nested(data: Any, *path: str) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def first_not_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def pick_path(data: Any, *paths: PathLike) -> Any:
    for path in paths:
        value = get_nested(data, *path)
        if value not in (None, "", [], {}):
            return value
    return None


def find_first_key_deep(data: Any, keys: set[str], *, max_depth: int = 10) -> Any:
    def _walk(node: Any, depth: int) -> Any:
        if depth > max_depth:
            return None
        if isinstance(node, dict):
            for key in keys:
                if key in node and node[key] not in (None, "", [], {}):
                    return node[key]
            for value in node.values():
                found = _walk(value, depth + 1)
                if found not in (None, "", [], {}):
                    return found
        elif isinstance(node, list):
            for value in node:
                found = _walk(value, depth + 1)
                if found not in (None, "", [], {}):
                    return found
        return None

    return _walk(data, 0)


def pick_value(data: Any, paths: Iterable[PathLike], fallback_keys: Iterable[str] = ()) -> Any:
    value = pick_path(data, *tuple(paths))
    if value not in (None, "", [], {}):
        return value
    keys = {k for k in fallback_keys if k}
    if keys:
        return find_first_key_deep(data, keys)
    return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, str):
            value = value.strip().replace(" ", "")
            if value.count(",") == 1 and value.count(".") == 0:
                value = value.replace(",", ".")
        return Decimal(str(value))
    except Exception:
        return None


def to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "1", "ja", "yes"}:
        return True
    if s in {"false", "0", "nei", "no"}:
        return False
    return None


def to_year(value: Any) -> int | None:
    if not value:
        return None
    s = str(value).strip()
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def flatten_latest(item: JsonDict) -> JsonDict:
    fra = pick_value(
        item,
        paths=[("regnskapsperiode", "fraDato")],
        fallback_keys=["fraDato"],
    )
    til = pick_value(
        item,
        paths=[("regnskapsperiode", "tilDato")],
        fallback_keys=["tilDato"],
    )

    regnskapsprinsipper = first_not_empty(
        item.get("regnskapsprinsipper"),
        item.get("regnkapsprinsipper"),
        find_first_key_deep(item, {"regnskapsprinsipper", "regnkapsprinsipper"}),
        {},
    )
    revisjon = first_not_empty(
        item.get("revisjon"),
        find_first_key_deep(item, {"revisjon"}),
        {},
    )
    virksomhet = first_not_empty(
        item.get("virksomhet"),
        find_first_key_deep(item, {"virksomhet"}),
        {},
    )
    resultat = first_not_empty(
        item.get("resultatregnskapResultat"),
        find_first_key_deep(item, {"resultatregnskapResultat"}),
        {},
    )
    eiendeler = first_not_empty(
        item.get("eiendeler"),
        find_first_key_deep(item, {"eiendeler"}),
        {},
    )
    egenkapital_gjeld = first_not_empty(
        item.get("egenkapitalGjeld"),
        find_first_key_deep(item, {"egenkapitalGjeld"}),
        {},
    )

    return {
        "journalnr": item.get("journalnr"),
        "regnskapsaar": to_year(til) or to_year(fra),
        "regnskapstype": item.get("regnskapstype"),
        "organisasjonsform": pick_value(
            virksomhet,
            paths=[("organisasjonsform",)],
            fallback_keys=["organisasjonsform"],
        ),
        "morselskap": to_bool(
            pick_value(virksomhet, paths=[("morselskap",)], fallback_keys=["morselskap"])
        ),
        "valuta": item.get("valuta"),
        "avviklingsregnskap": to_bool(item.get("avviklingsregnskap")),
        "oppstillingsplan": item.get("oppstillingsplan"),
        "ikke_revidert_aarsregnskap": to_bool(
            pick_value(
                revisjon,
                paths=[("ikkeRevidertAarsregnskap",)],
                fallback_keys=["ikkeRevidertAarsregnskap"],
            )
        ),
        "fravalg_revisjon": to_bool(
            pick_value(
                revisjon,
                paths=[("fravalgRevisjon",)],
                fallback_keys=["fravalgRevisjon"],
            )
        ),
        "smaa_foretak": to_bool(
            pick_value(
                regnskapsprinsipper,
                paths=[("smaaForetak",)],
                fallback_keys=["smaaForetak"],
            )
        ),
        "regnskapsregler": pick_value(
            regnskapsprinsipper,
            paths=[("regnskapsregler",)],
            fallback_keys=["regnskapsregler"],
        ),
        "sum_driftsinntekter": to_decimal(
            pick_value(
                resultat,
                paths=[
                    ("driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
                    ("driftsinntekter", "sumDriftsinntekter"),
                ],
                fallback_keys=["sumDriftsinntekter"],
            )
        ),
        "sum_driftskostnad": to_decimal(
            pick_value(
                resultat,
                paths=[
                    ("driftsresultat", "driftskostnad", "sumDriftskostnad"),
                    ("driftskostnad", "sumDriftskostnad"),
                ],
                fallback_keys=["sumDriftskostnad"],
            )
        ),
        "driftsresultat": to_decimal(
            pick_value(
                resultat,
                paths=[("driftsresultat", "driftsresultat"), ("driftsresultat",)],
                fallback_keys=["driftsresultat"],
            )
        ),
        "netto_finans": to_decimal(
            pick_value(
                resultat,
                paths=[("finansresultat", "nettoFinans")],
                fallback_keys=["nettoFinans"],
            )
        ),
        "sum_finansinntekter": to_decimal(
            pick_value(
                resultat,
                paths=[
                    ("finansresultat", "finansinntekt", "sumFinansinntekter"),
                    ("finansinntekt", "sumFinansinntekter"),
                ],
                fallback_keys=["sumFinansinntekter"],
            )
        ),
        "sum_finanskostnad": to_decimal(
            pick_value(
                resultat,
                paths=[
                    ("finansresultat", "finanskostnad", "sumFinanskostnad"),
                    ("finanskostnad", "sumFinanskostnad"),
                ],
                fallback_keys=["sumFinanskostnad"],
            )
        ),
        "ordinaert_resultat_foer_skattekostnad": to_decimal(
            pick_value(
                resultat,
                paths=[("ordinaertResultatFoerSkattekostnad",)],
                fallback_keys=["ordinaertResultatFoerSkattekostnad"],
            )
        ),
        "aarsresultat": to_decimal(
            pick_value(
                resultat,
                paths=[("aarsresultat",)],
                fallback_keys=["aarsresultat"],
            )
        ),
        "totalresultat": to_decimal(
            pick_value(
                resultat,
                paths=[("totalresultat",)],
                fallback_keys=["totalresultat"],
            )
        ),
        "sum_eiendeler": to_decimal(
            pick_value(
                eiendeler,
                paths=[("sumEiendeler",)],
                fallback_keys=["sumEiendeler"],
            )
        ),
        "sum_omloepsmidler": to_decimal(
            pick_value(
                eiendeler,
                paths=[("omloepsmidler", "sumOmloepsmidler")],
                fallback_keys=["sumOmloepsmidler"],
            )
        ),
        "sum_anleggsmidler": to_decimal(
            pick_value(
                eiendeler,
                paths=[("anleggsmidler", "sumAnleggsmidler")],
                fallback_keys=["sumAnleggsmidler"],
            )
        ),
        "sum_egenkapital": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("egenkapital", "sumEgenkapital")],
                fallback_keys=["sumEgenkapital"],
            )
        ),
        "sum_opptjent_egenkapital": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("egenkapital", "opptjentEgenkapital", "sumOpptjentEgenkapital")],
                fallback_keys=["sumOpptjentEgenkapital"],
            )
        ),
        "sum_innskutt_egenkapital": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[
                    ("egenkapital", "innskuttEgenkapital", "sumInnskuttEgenkapital"),
                    ("egenkapital", "innskuttEgenkapital", "sumInnskuttEgenkaptial"),
                ],
                fallback_keys=["sumInnskuttEgenkapital", "sumInnskuttEgenkaptial"],
            )
        ),
        "sum_gjeld": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("gjeldOversikt", "sumGjeld")],
                fallback_keys=["sumGjeld"],
            )
        ),
        "sum_kortsiktig_gjeld": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("gjeldOversikt", "kortsiktigGjeld", "sumKortsiktigGjeld")],
                fallback_keys=["sumKortsiktigGjeld"],
            )
        ),
        "sum_langsiktig_gjeld": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("gjeldOversikt", "langsiktigGjeld", "sumLangsiktigGjeld")],
                fallback_keys=["sumLangsiktigGjeld"],
            )
        ),
        "sum_egenkapital_gjeld": to_decimal(
            pick_value(
                egenkapital_gjeld,
                paths=[("sumEgenkapitalGjeld",)],
                fallback_keys=["sumEgenkapitalGjeld"],
            )
        ),
        "regnskapsperiode_fra": fra,
        "regnskapsperiode_til": til,
    }


def print_db_debug_info() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    current_user,
                    current_database(),
                    current_setting('search_path'),
                    to_regclass('regnskap_queue')::text,
                    to_regclass('public.regnskap_queue')::text,
                    has_table_privilege(current_user, 'regnskap_queue', 'select'),
                    has_table_privilege(current_user, 'regnskap_queue', 'update'),
                    has_table_privilege(current_user, 'public.regnskap_queue', 'select'),
                    has_table_privilege(current_user, 'public.regnskap_queue', 'update')
                """
            )
            print("DB DEBUG:", cur.fetchone())


def rebuild_queue(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(f"truncate table {QUEUE_TABLE}")
        cur.execute(
            f"""
            insert into {QUEUE_TABLE}
            (orgnr, status, attempts, locked_at, last_http_status, last_error, last_fetched_at)
            select
                e.orgnr,
                'pending',
                0,
                null,
                null,
                null,
                null
            from {ENTITY_TABLE} e
            where upper(trim(coalesce(e.orgform, ''))) = %s
              and coalesce(e.ansatte, 0) >= %s
            """,
            (FILTER_ORGFORM, FILTER_MIN_ANSATTE),
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def count_queue(conn: psycopg.Connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
                count(*)::int as total,
                count(*) filter (where status = 'pending')::int as pending,
                count(*) filter (where status = 'error')::int as error,
                count(*) filter (where status = 'done')::int as done,
                count(*) filter (where status = 'no_data')::int as no_data
            from {QUEUE_TABLE}
            """
        )
        total, pending, error, done, no_data = cur.fetchone()
    return {
        'total': total,
        'pending': pending,
        'error': error,
        'done': done,
        'no_data': no_data,
    }


def lock_batch(conn: psycopg.Connection, batch_size: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            with picked as (
                select orgnr
                from {QUEUE_TABLE}
                where status in ('pending', 'error')
                  and coalesce(locked_at < now() - interval '30 minutes', true)
                order by attempts asc, orgnr asc
                limit %s
                for update skip locked
            )
            update {QUEUE_TABLE} q
            set locked_at = now()
            from picked
            where q.orgnr = picked.orgnr
            returning q.orgnr
            """,
            (batch_size,),
        )
        rows = [normalize_orgnr(row[0]) for row in cur.fetchall()]
    conn.commit()
    return rows


def set_queue_status(
    conn: psycopg.Connection,
    orgnr: str,
    status: str,
    http_status: int | None = None,
    error_text: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {QUEUE_TABLE}
            set status = %s,
                attempts = attempts + 1,
                last_http_status = %s,
                last_error = nullif(left(coalesce(%s, ''), 2000), ''),
                locked_at = null,
                last_fetched_at = now()
            where orgnr = %s
            """,
            (status, http_status, error_text, orgnr),
        )
    conn.commit()


def upsert_regnskap(conn: psycopg.Connection, orgnr: str, item: JsonDict, raw_payload: Any) -> None:
    flat = flatten_latest(item)
    phash = payload_hash(raw_payload)

    params = {
        "orgnr": orgnr,
        **flat,
        "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
        "payload_hash": phash,
    }

    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {SISTE_TABLE} (
                orgnr,
                journalnr,
                regnskapsaar,
                regnskapstype,
                organisasjonsform,
                morselskap,
                valuta,
                avviklingsregnskap,
                oppstillingsplan,
                ikke_revidert_aarsregnskap,
                fravalg_revisjon,
                smaa_foretak,
                regnskapsregler,
                sum_driftsinntekter,
                sum_driftskostnad,
                driftsresultat,
                netto_finans,
                sum_finansinntekter,
                sum_finanskostnad,
                ordinaert_resultat_foer_skattekostnad,
                aarsresultat,
                totalresultat,
                sum_eiendeler,
                sum_omloepsmidler,
                sum_anleggsmidler,
                sum_egenkapital,
                sum_opptjent_egenkapital,
                sum_innskutt_egenkapital,
                sum_gjeld,
                sum_kortsiktig_gjeld,
                sum_langsiktig_gjeld,
                sum_egenkapital_gjeld,
                regnskapsperiode_fra,
                regnskapsperiode_til,
                raw_payload,
                payload_hash,
                fetched_at
            )
            values (
                %(orgnr)s,
                %(journalnr)s,
                %(regnskapsaar)s,
                %(regnskapstype)s,
                %(organisasjonsform)s,
                %(morselskap)s,
                %(valuta)s,
                %(avviklingsregnskap)s,
                %(oppstillingsplan)s,
                %(ikke_revidert_aarsregnskap)s,
                %(fravalg_revisjon)s,
                %(smaa_foretak)s,
                %(regnskapsregler)s,
                %(sum_driftsinntekter)s,
                %(sum_driftskostnad)s,
                %(driftsresultat)s,
                %(netto_finans)s,
                %(sum_finansinntekter)s,
                %(sum_finanskostnad)s,
                %(ordinaert_resultat_foer_skattekostnad)s,
                %(aarsresultat)s,
                %(totalresultat)s,
                %(sum_eiendeler)s,
                %(sum_omloepsmidler)s,
                %(sum_anleggsmidler)s,
                %(sum_egenkapital)s,
                %(sum_opptjent_egenkapital)s,
                %(sum_innskutt_egenkapital)s,
                %(sum_gjeld)s,
                %(sum_kortsiktig_gjeld)s,
                %(sum_langsiktig_gjeld)s,
                %(sum_egenkapital_gjeld)s,
                %(regnskapsperiode_fra)s,
                %(regnskapsperiode_til)s,
                %(raw_payload)s::jsonb,
                %(payload_hash)s,
                now()
            )
            on conflict (orgnr) do update
            set journalnr = excluded.journalnr,
                regnskapsaar = excluded.regnskapsaar,
                regnskapstype = excluded.regnskapstype,
                organisasjonsform = excluded.organisasjonsform,
                morselskap = excluded.morselskap,
                valuta = excluded.valuta,
                avviklingsregnskap = excluded.avviklingsregnskap,
                oppstillingsplan = excluded.oppstillingsplan,
                ikke_revidert_aarsregnskap = excluded.ikke_revidert_aarsregnskap,
                fravalg_revisjon = excluded.fravalg_revisjon,
                smaa_foretak = excluded.smaa_foretak,
                regnskapsregler = excluded.regnskapsregler,
                sum_driftsinntekter = excluded.sum_driftsinntekter,
                sum_driftskostnad = excluded.sum_driftskostnad,
                driftsresultat = excluded.driftsresultat,
                netto_finans = excluded.netto_finans,
                sum_finansinntekter = excluded.sum_finansinntekter,
                sum_finanskostnad = excluded.sum_finanskostnad,
                ordinaert_resultat_foer_skattekostnad = excluded.ordinaert_resultat_foer_skattekostnad,
                aarsresultat = excluded.aarsresultat,
                totalresultat = excluded.totalresultat,
                sum_eiendeler = excluded.sum_eiendeler,
                sum_omloepsmidler = excluded.sum_omloepsmidler,
                sum_anleggsmidler = excluded.sum_anleggsmidler,
                sum_egenkapital = excluded.sum_egenkapital,
                sum_opptjent_egenkapital = excluded.sum_opptjent_egenkapital,
                sum_innskutt_egenkapital = excluded.sum_innskutt_egenkapital,
                sum_gjeld = excluded.sum_gjeld,
                sum_kortsiktig_gjeld = excluded.sum_kortsiktig_gjeld,
                sum_langsiktig_gjeld = excluded.sum_langsiktig_gjeld,
                sum_egenkapital_gjeld = excluded.sum_egenkapital_gjeld,
                regnskapsperiode_fra = excluded.regnskapsperiode_fra,
                regnskapsperiode_til = excluded.regnskapsperiode_til,
                raw_payload = excluded.raw_payload,
                payload_hash = excluded.payload_hash,
                fetched_at = now()
            """,
            params,
        )
    conn.commit()


def fetch_one(session: requests.Session, orgnr: str) -> tuple[int, Any]:
    url = BASE_URL.format(orgnr=orgnr)
    r = session.get(
        url,
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    content_type = (r.headers.get("content-type") or "").lower()

    if r.status_code == 200:
        if "json" in content_type:
            return r.status_code, r.json()
        return r.status_code, {"raw": r.text, "content_type": content_type}

    return r.status_code, {"raw": r.text[:5000], "content_type": content_type}


def process_orgnr(session: requests.Session, orgnr: str) -> str:
    orgnr = normalize_orgnr(orgnr)
    last_status: int | None = None
    last_err: str | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            status, payload = fetch_one(session, orgnr)
            last_status = status

            if status == 200:
                items = as_list(payload)
                latest = pick_latest(items)
                if latest is None:
                    with psycopg.connect(DATABASE_URL) as conn:
                        set_queue_status(conn, orgnr, "no_data", 200, "empty_or_unexpected_payload")
                    return "no_data"

                with psycopg.connect(DATABASE_URL) as conn:
                    upsert_regnskap(conn, orgnr, latest, payload)
                    set_queue_status(conn, orgnr, "done", 200, None)
                return "done"

            if status == 404:
                with psycopg.connect(DATABASE_URL) as conn:
                    set_queue_status(conn, orgnr, "no_data", 404, None)
                return "no_data"

            if status == 429 or status >= 500:
                wait = min(BACKOFF_CAP, 2 ** attempt)
                log(f"{orgnr}: transient HTTP {status}, retry in {wait}s")
                time.sleep(wait)
                continue

            with psycopg.connect(DATABASE_URL) as conn:
                set_queue_status(conn, orgnr, "error", status, f"unexpected_http_status:{status}")
            return "error"

        except Exception as e:
            last_err = str(e)
            wait = min(BACKOFF_CAP, 2 ** attempt)
            log(f"{orgnr}: exception={last_err}, retry in {wait}s")
            time.sleep(wait)

    with psycopg.connect(DATABASE_URL) as conn:
        set_queue_status(conn, orgnr, "error", last_status, last_err or "request_failed")
    return "error"


def main() -> int:
    if DEBUG_DB:
        print_db_debug_info()

    with psycopg.connect(DATABASE_URL) as conn:
        if REBUILD_QUEUE:
            inserted = rebuild_queue(conn)
            print(
                f"Bygget ny kø i {QUEUE_TABLE}: {inserted} selskaper "
                f"(orgform={FILTER_ORGFORM}, ansatte>={FILTER_MIN_ANSATTE})"
            )

        stats = count_queue(conn)
        print(
            f"Køstatus før start: total={stats['total']} pending={stats['pending']} "
            f"error={stats['error']} done={stats['done']} no_data={stats['no_data']}"
        )

    if QUEUE_ONLY:
        print("QUEUE_ONLY=1, stopper etter å ha bygget/validert køen.")
        return 0

    session = requests.Session()
    session.headers.update({"User-Agent": "prisanalyse-regnskap-fetcher/2.3"})

    total_done = 0
    total_no_data = 0
    total_error = 0

    while True:
        with psycopg.connect(DATABASE_URL) as conn:
            batch = lock_batch(conn, BATCH_SIZE)

        if not batch:
            print("Ingen flere jobber i køen.")
            break

        for i, orgnr in enumerate(batch, start=1):
            result = process_orgnr(session, orgnr)

            if result == "done":
                total_done += 1
            elif result == "no_data":
                total_no_data += 1
            else:
                total_error += 1

            if i % 25 == 0:
                print(
                    f"Batch progress {i}/{len(batch)} | "
                    f"done={total_done} no_data={total_no_data} error={total_error}"
                )

            time.sleep(SLEEP_SECONDS)

        print(
            f"Batch ferdig. done={total_done} no_data={total_no_data} error={total_error}"
        )

        if not LOOP:
            break

    return 0


if __name__ == "__main__":
    sys.exit(main())
