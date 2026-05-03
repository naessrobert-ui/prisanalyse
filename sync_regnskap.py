#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_regnskap.py
================
Henter selskaper som har levert/gitt nye regnskapstall i en gitt periode
fra Brreg, og oppdaterer RDS-databasen.

Viktig:
  - Default er nå siste 3 dager hvis --fra ikke oppgis.
  - --steg brukes faktisk i datoløkken.
  - Hvis Brreg returnerer flere regnskap for samme orgnr, velges nyeste
    regnskapsperiode før DB sammenligning.

Typisk kjøring:
  python sync_regnskap.py
  python sync_regnskap.py --fra 2026-05-01 --steg 1
  python sync_regnskap.py --fra 2026-05-01 --til 2026-05-03 --steg 1
  python sync_regnskap.py --tørrkjør
  python sync_regnskap.py --fra 2026-05-01 --steg 1 --tørrkjør

Cron-eksempel:
  0 6 * * * cd /home/robert/projects/regnskap && .venv/bin/python sync_regnskap.py --fra $(date -d "3 days ago" +\\%Y-\\%m-\\%d) --steg 1 >> /home/robert/logs/sync_regnskap.log 2>&1

Miljøvariabler (.env):
  DATABASE_URL
  eller AWS IAM: RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, AWS_REGION
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any

import psycopg as psycopg2
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# ── Konfig ────────────────────────────────────────────────────────────────────

BRREG_KUNNGJORING_URL = "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter"
BRREG_REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
BRREG_ENHET_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"

DEFAULT_DAGER_TILBAKE = 3
STEG_DAGER = 1          # Default nå: sjekk hver dag
BATCH_WORKERS = 6       # Parallelle Brreg-kall
SLEEP_MELLOM = 0.3      # Sekunder mellom batch-kall

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "prisanalyse.no/regnskap-sync"
)


# ── Små hjelpefunksjoner ─────────────────────────────────────────────────────

def _parse_iso_date(value: Any) -> date | None:
    """Parser de vanligste Brreg-datoformatene robust."""
    if not value:
        return None

    s = str(value).strip()
    if not s:
        return None

    # Vanligst: YYYY-MM-DD eller YYYY-MM-DDTHH:MM:SS...
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            return None

    return None


def _regnskapsperiode_til_dato(payload: dict) -> date | None:
    periode = payload.get("regnskapsperiode") or {}
    if not isinstance(periode, dict):
        return None
    return _parse_iso_date(periode.get("tilDato"))


def _regnskapsaar(payload: dict) -> int | None:
    til_dato = _regnskapsperiode_til_dato(payload)
    return til_dato.year if til_dato else None


def _velg_nyeste_regnskap(payload: Any) -> dict | None:
    """
    Brreg kan returnere enten dict eller liste med flere regnskap.
    Vi velger regnskapet med nyeste regnskapsperiode.tilDato.

    Dette er viktig: hvis API-et returnerer en eldre post først i listen,
    vil scriptet ellers tro at selskapet allerede er oppdatert.
    """
    if isinstance(payload, dict):
        return payload

    if not isinstance(payload, list):
        return None

    kandidater = [p for p in payload if isinstance(p, dict)]
    if not kandidater:
        return None

    def sort_key(p: dict) -> tuple[date, str]:
        til_dato = _regnskapsperiode_til_dato(p) or date.min
        journalnr = str(p.get("journalnr") or "")
        return til_dato, journalnr

    return max(kandidater, key=sort_key)


# ── DB-tilkobling ─────────────────────────────────────────────────────────────

def _get_iam_token() -> str:
    """Generer midlertidig RDS IAM-token via AWS CLI."""
    import subprocess

    rds_host = os.getenv(
        "RDS_HOST",
        "brreg-mini-proff.cvos4q86gzmu.eu-north-1.rds.amazonaws.com",
    )
    rds_port = os.getenv("RDS_PORT", "5432")
    rds_user = os.getenv("RDS_USER", "postgres")
    aws_region = os.getenv("AWS_REGION", "eu-north-1")

    return subprocess.check_output(
        [
            "aws", "rds", "generate-db-auth-token",
            "--hostname", rds_host,
            "--port", rds_port,
            "--username", rds_user,
            "--region", aws_region,
        ],
        text=True,
    ).strip()


def _db_connect() -> psycopg2.Connection:
    """Kobler til RDS via DATABASE_URL eller IAM-token."""
    database_url = os.environ.get("DATABASE_URL", "")
    if database_url:
        return psycopg2.connect(database_url, connect_timeout=15)

    rds_host = os.getenv(
        "RDS_HOST",
        "brreg-mini-proff.cvos4q86gzmu.eu-north-1.rds.amazonaws.com",
    )
    rds_port = int(os.getenv("RDS_PORT", "5432"))
    rds_db = os.getenv("RDS_DB", "postgres")
    rds_user = os.getenv("RDS_USER", "postgres")
    token = _get_iam_token()

    return psycopg2.connect(
        host=rds_host,
        port=rds_port,
        dbname=rds_db,
        user=rds_user,
        password=token,
        sslmode="require",
        connect_timeout=15,
    )


# ── HTTP-session ──────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    sess = requests.Session()

    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return sess


# ── Steg 1: Hent orgnr fra Brreg-oppdateringer ───────────────────────────────

def _iso_z(dt: datetime) -> str:
    """Formater datetime slik Brreg forventer: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _hent_kandidater_for_vindu(
    sess: requests.Session,
    fra_tid: datetime,
    til_tid: datetime,
    depth: int = 0,
) -> set[str]:
    """
    Henter orgnr som er oppdatert i et avgrenset tidsvindu.

    Hvorfor denne finnes:
      - Brreg sitt `dato`-filter betyr "fra og med dette tidspunktet".
      - Uten `updatedBefore` henter man derfor alt fra datoen og frem til nå.
      - Brreg har også en hard pagineringsgrense på 10 000 treff per søk.

    Hvis et vindu har >= 10 000 treff, deles vinduet automatisk i to.
    """
    size = 1000
    max_pages = 10  # page 0..9 med size 1000 = maks 10 000

    def _label() -> str:
        return f"{_iso_z(fra_tid)} → {_iso_z(til_tid)}"

    def _extract(data: dict) -> set[str]:
        found: set[str] = set()
        entries = data.get("_embedded", {}).get("oppdaterteEnheter", [])
        for entry in entries:
            orgnr = str(entry.get("organisasjonsnummer") or "").strip()
            if re.match(r"^\d{9}$", orgnr):
                found.add(orgnr)
        return found

    try:
        params = {
            "dato": _iso_z(fra_tid),
            "updatedBefore": _iso_z(til_tid),
            "page": 0,
            "size": size,
            "sort": "id,ASC",
        }
        r = sess.get(BRREG_KUNNGJORING_URL, params=params, timeout=20)

        if r.status_code != 200:
            print(f"[WARN] Brreg oppdateringer {_label()} side 0: HTTP {r.status_code}")
            return set()

        data = r.json()
        page_info = data.get("page", {}) or {}
        total = int(page_info.get("totalElements") or 0)

        # Hvis vi nærmer oss Brreg-grensen, del vinduet i to før vi paginerer.
        # Dette er den viktigste beskyttelsen mot HTTP 400 på page 10.
        if total >= 10000:
            span = til_tid - fra_tid
            if span.total_seconds() > 2 and depth < 20:
                mid = fra_tid + (span / 2)
                print(f"[INFO] {total} oppdateringer i vindu {_label()} — splitter tidsvindu")
                left = _hent_kandidater_for_vindu(sess, fra_tid, mid, depth + 1)
                right = _hent_kandidater_for_vindu(sess, mid, til_tid, depth + 1)
                return left | right

            print(
                f"[WARN] Vinduet {_label()} har {total} treff og kan fortsatt være for stort. "
                "Noen treff kan mangle."
            )

        kandidater = _extract(data)

        # Hent resten av sidene, men aldri forbi Brreg sin 10 000-grense.
        pages = min((total + size - 1) // size, max_pages)
        for page in range(1, pages):
            params["page"] = page
            r = sess.get(BRREG_KUNNGJORING_URL, params=params, timeout=20)

            if r.status_code != 200:
                print(f"[WARN] Brreg oppdateringer {_label()} side {page}: HTTP {r.status_code}")
                break

            kandidater.update(_extract(r.json()))

        return kandidater

    except Exception as e:
        print(f"[WARN] Feil ved henting for vindu {_label()}: {e}")
        return set()


def _hent_orgnr_for_dato(sess: requests.Session, dato: date) -> set[str]:
    """
    Henter orgnr som er oppdatert i Brreg på én kalenderdag,
    og filtrerer ned til dem som har regnskap i Regnskapsregisteret.
    """
    start = datetime.combine(dato, datetime.min.time())
    slutt = start + timedelta(days=1)

    kandidater = _hent_kandidater_for_vindu(sess, start, slutt)
    orgnr_set: set[str] = set()

    def _har_regnskap(orgnr: str) -> str | None:
        try:
            r = sess.get(f"{BRREG_REGNSKAP_URL}/{orgnr}", timeout=10)
            if r.status_code != 200:
                return None

            nyeste = _velg_nyeste_regnskap(r.json())
            if nyeste:
                return orgnr

        except Exception:
            pass

        return None

    if kandidater:
        with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as ex:
            futures = {ex.submit(_har_regnskap, o): o for o in kandidater}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    orgnr_set.add(result)

    return orgnr_set

def hent_alle_orgnr(fra: date, til: date, steg: int = STEG_DAGER) -> list[str]:
    """Henter alle orgnr mellom fra og til, med gitt antall dager mellom hvert søk."""
    if steg < 1:
        raise ValueError("--steg må være minst 1")

    sess = _make_session()
    alle: set[str] = set()

    dato = fra
    datoer: list[date] = []

    while dato <= til:
        datoer.append(dato)
        dato += timedelta(days=steg)   # Viktig: bruk argumentet, ikke global STEG_DAGER

    print(
        f"Henter kunngjøringer for {len(datoer)} datoer "
        f"({fra} → {til}, hvert {steg}. dag)..."
    )

    for i, d in enumerate(datoer, 1):
        print(f"  [{i:2}/{len(datoer)}] {d} ...", end=" ", flush=True)

        nye = _hent_orgnr_for_dato(sess, d)
        før = len(alle)
        alle.update(nye)
        faktisk_nye = len(alle) - før

        print(f"+{faktisk_nye:4} nye  ({len(nye)} treff, totalt {len(alle)})", flush=True)
        time.sleep(0.1)

    return sorted(alle)


# ── Steg 2: Hent regnskapstall fra Brreg ─────────────────────────────────────

def _hent_regnskap(sess: requests.Session, orgnr: str) -> dict | None:
    """Returnerer nyeste rådata fra Brreg Regnskapsregisteret."""
    try:
        r = sess.get(f"{BRREG_REGNSKAP_URL}/{orgnr}", timeout=15)
        if r.status_code != 200:
            return None

        return _velg_nyeste_regnskap(r.json())

    except Exception:
        return None


def _hent_enhet(sess: requests.Session, orgnr: str) -> dict | None:
    """Henter enhetsinfo fra Enhetsregisteret."""
    try:
        r = sess.get(f"{BRREG_ENHET_URL}/{orgnr}", timeout=15)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def _parse_regnskap(payload: dict, orgnr: str) -> dict | None:
    """Trekker ut nøkkeltall fra Brreg-payload."""
    try:
        virksomhet = payload.get("virksomhet") or {}
        periode = payload.get("regnskapsperiode") or {}

        if not isinstance(virksomhet, dict) or not isinstance(periode, dict):
            return None

        til_dato = _parse_iso_date(periode.get("tilDato"))
        if not til_dato:
            return None

        year = til_dato.year

        def _get(*keys: str) -> float | None:
            d: Any = payload
            for k in keys:
                if not isinstance(d, dict):
                    return None
                d = d.get(k)

            if d is None:
                return None

            try:
                return float(d)
            except (TypeError, ValueError):
                return None

        revenue = _get(
            "resultatregnskapResultat",
            "driftsresultat",
            "driftsinntekter",
            "sumDriftsinntekter",
        )
        operating_profit = _get(
            "resultatregnskapResultat",
            "driftsresultat",
            "driftsresultat",
        )
        net_profit = _get(
            "resultatregnskapResultat",
            "aarsresultat",
        )
        total_assets = _get(
            "eiendeler",
            "sumEiendeler",
        )
        equity = _get(
            "egenkapitalGjeld",
            "egenkapital",
            "sumEgenkapital",
        )

        total_liabilities = _get(
            "egenkapitalGjeld",
            "gjeldOversikt",
            "sumGjeld",
        )

        # Hvis sumGjeld ikke finnes direkte, kan vi beregne den.
        if total_liabilities is None and total_assets is not None and equity is not None:
            total_liabilities = total_assets - equity

        return {
            "orgnr": str(virksomhet.get("organisasjonsnummer", orgnr)).strip(),
            "navn": virksomhet.get("navn") or virksomhet.get("organisasjonsnavn") or "",
            "accounting_year": year,
            "oppdatert_dato": til_dato.isoformat(),
            "revenue": revenue,
            "operating_profit": operating_profit,
            "net_profit": net_profit,
            "total_assets": total_assets,
            "equity": equity,
            "total_liabilities": total_liabilities,
            "equity_ratio": None,  # beregnes ved insert hvis ønskelig
        }

    except Exception:
        return None


# ── Steg 3: DB-operasjoner ────────────────────────────────────────────────────

def _hent_db_status(cur, orgnr: str) -> dict | None:
    """Returnerer {'maks_år': int} hvis selskapet finnes i DB, ellers None."""
    cur.execute("SELECT 1 FROM entity WHERE orgnr = %s", (orgnr,))
    if not cur.fetchone():
        return None

    cur.execute(
        """
        SELECT regnskapsaar
        FROM regnskap_siste
        WHERE orgnr = %s
        """,
        (orgnr,),
    )
    row = cur.fetchone()
    return {"maks_år": row[0] if row else None}


def _insert_entity(cur, orgnr: str, enhet: dict | None, navn: str) -> None:
    """Setter inn eller oppdaterer selskap i entity-tabellen."""
    kommune_nr = None
    orgform = None
    ansatte = None

    if enhet:
        kommune_nr = (
            (enhet.get("forretningsadresse") or {}).get("kommunenummer")
            or (enhet.get("beliggenhetsadresse") or {}).get("kommunenummer")
        )
        orgform = (enhet.get("organisasjonsform") or {}).get("kode")
        ansatte = enhet.get("antallAnsatte")
        navn = enhet.get("navn") or navn

    cur.execute(
        """
        INSERT INTO entity (orgnr, navn, orgform, kommunenummer, ansatte)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (orgnr) DO UPDATE SET
            navn          = EXCLUDED.navn,
            orgform       = COALESCE(EXCLUDED.orgform, entity.orgform),
            kommunenummer = COALESCE(EXCLUDED.kommunenummer, entity.kommunenummer),
            ansatte       = COALESCE(EXCLUDED.ansatte, entity.ansatte)
        """,
        (orgnr, navn, orgform, kommune_nr, ansatte),
    )


def _insert_regnskap(cur, rec: dict) -> None:
    """Setter inn/oppdaterer nyeste regnskap i regnskap_siste."""
    oppdatert_dato = None
    raw_dato = rec.get("oppdatert_dato")

    if raw_dato:
        try:
            oppdatert_dato = date.fromisoformat(str(raw_dato)[:10])
        except ValueError:
            pass

    revenue = rec.get("revenue")
    operating_profit = rec.get("operating_profit")
    net_profit = rec.get("net_profit")
    total_assets = rec.get("total_assets")
    equity = rec.get("equity")
    total_liabilities = rec.get("total_liabilities")

    # Bruk stabil JSON som grunnlag for hash.
    raw_payload = json.dumps(rec, ensure_ascii=False, sort_keys=True)
    payload_hash = str(hash(raw_payload))[:64]

    cur.execute(
        """
        INSERT INTO regnskap_siste (
            orgnr, regnskapsaar,
            sum_driftsinntekter, driftsresultat, aarsresultat,
            sum_eiendeler, sum_egenkapital,
            sum_gjeld,
            raw_payload, payload_hash, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (orgnr) DO UPDATE SET
            regnskapsaar        = EXCLUDED.regnskapsaar,
            sum_driftsinntekter = EXCLUDED.sum_driftsinntekter,
            driftsresultat      = EXCLUDED.driftsresultat,
            aarsresultat        = EXCLUDED.aarsresultat,
            sum_eiendeler       = EXCLUDED.sum_eiendeler,
            sum_egenkapital     = EXCLUDED.sum_egenkapital,
            sum_gjeld           = EXCLUDED.sum_gjeld,
            raw_payload         = EXCLUDED.raw_payload,
            payload_hash        = EXCLUDED.payload_hash,
            fetched_at          = NOW()
        WHERE regnskap_siste.regnskapsaar IS NULL
           OR EXCLUDED.regnskapsaar >= regnskap_siste.regnskapsaar
        """,
        (
            rec["orgnr"],
            rec["accounting_year"],
            revenue,
            operating_profit,
            net_profit,
            total_assets,
            equity,
            total_liabilities,
            raw_payload,
            payload_hash,
        ),
    )


# ── Hovedlogikk ───────────────────────────────────────────────────────────────

def prosesser_batch(
    orgnr_liste: list[str],
    tørrkjør: bool = False,
) -> dict:
    """Prosesserer en liste orgnr og oppdaterer DB."""
    stats = {
        "nye_selskap": 0,
        "nye_år": 0,
        "allerede_oppdatert": 0,
        "feil": 0,
        "år_fordeling": Counter(),
    }

    conn = None if tørrkjør else _db_connect()
    sess = _make_session()

    def _behandle_ett(orgnr: str) -> tuple[str, str, dict | None]:
        """Returnerer (orgnr, status, regnskap_rec)."""
        payload = _hent_regnskap(sess, orgnr)

        if not payload:
            return orgnr, "INGEN_REGNSKAP", None

        rec = _parse_regnskap(payload, orgnr)

        if not rec:
            return orgnr, "PARSE_FEIL", None

        return orgnr, "OK", rec

    print(f"\nProsesserer {len(orgnr_liste)} selskaper ({BATCH_WORKERS} parallelle)...")
    total = len(orgnr_liste)

    try:
        cur = None if tørrkjør else conn.cursor()

        for i in range(0, total, 50):
            batch = orgnr_liste[i:i + 50]

            resultater: dict[str, tuple[str, str, dict | None]] = {}

            with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as ex:
                futures = {ex.submit(_behandle_ett, o): o for o in batch}

                for fut in as_completed(futures):
                    o = futures[fut]
                    try:
                        resultater[o] = fut.result()
                    except Exception:
                        resultater[o] = (o, "EXCEPTION", None)

            for orgnr in batch:
                _, status, rec = resultater.get(orgnr, (orgnr, "MANGLER", None))

                if status != "OK" or not rec:
                    stats["feil"] += 1
                    continue

                stats["år_fordeling"][rec["accounting_year"]] += 1

                if tørrkjør:
                    print(
                        f"  [TØRR] {orgnr} {rec['navn'][:40]:40} "
                        f"år={rec['accounting_year']}"
                    )
                    stats["nye_år"] += 1
                    continue

                db_status = _hent_db_status(cur, orgnr)

                if db_status is None:
                    enhet = _hent_enhet(sess, orgnr)
                    _insert_entity(cur, orgnr, enhet, rec["navn"])
                    _insert_regnskap(cur, rec)
                    stats["nye_selskap"] += 1
                    print(
                        f"  [NY]   {orgnr} {rec['navn'][:40]:40} "
                        f"år={rec['accounting_year']}"
                    )

                elif (
                    db_status["maks_år"] is None
                    or rec["accounting_year"] > db_status["maks_år"]
                ):
                    _insert_regnskap(cur, rec)
                    stats["nye_år"] += 1
                    print(
                        f"  [+ÅR]  {orgnr} {rec['navn'][:40]:40} "
                        f"år={rec['accounting_year']} "
                        f"(hadde {db_status['maks_år']})"
                    )

                else:
                    stats["allerede_oppdatert"] += 1

            if not tørrkjør:
                conn.commit()

            fremgang = min(i + 50, total)
            print(
                f"  → {fremgang}/{total} behandlet | "
                f"nye={stats['nye_selskap']} "
                f"+år={stats['nye_år']} "
                f"ok={stats['allerede_oppdatert']} "
                f"feil={stats['feil']}"
            )

            time.sleep(SLEEP_MELLOM)

    finally:
        if conn:
            conn.close()

    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync regnskap fra Brreg → RDS")

    parser.add_argument(
        "--fra",
        default=None,
        help="Fra-dato YYYY-MM-DD. Default: siste 3 dager.",
    )
    parser.add_argument(
        "--til",
        default=None,
        help="Til-dato YYYY-MM-DD. Default: i dag.",
    )
    parser.add_argument(
        "--dager",
        type=int,
        default=DEFAULT_DAGER_TILBAKE,
        help=f"Antall dager tilbake hvis --fra ikke er satt. Default: {DEFAULT_DAGER_TILBAKE}.",
    )
    parser.add_argument(
        "--tørrkjør",
        action="store_true",
        help="Ikke skriv til DB.",
    )
    parser.add_argument(
        "--steg",
        type=int,
        default=STEG_DAGER,
        help=f"Dager mellom hver dato. Default: {STEG_DAGER}.",
    )

    args = parser.parse_args()

    til = date.fromisoformat(args.til) if args.til else date.today()

    if args.fra:
        fra = date.fromisoformat(args.fra)
    else:
        fra = til - timedelta(days=args.dager)

    if fra > til:
        raise ValueError("--fra kan ikke være etter --til")

    steg = args.steg

    print("=" * 60)
    print("sync_regnskap.py")
    print(f"Periode:  {fra} → {til}")
    print(f"Steg:     {steg} dager")
    print(f"Tørrkjør: {args.tørrkjør}")
    print("=" * 60)

    t0 = time.perf_counter()

    orgnr_liste = hent_alle_orgnr(fra, til, steg=steg)
    print(f"\nFant {len(orgnr_liste)} unike orgnr med regnskap-kunngjøringer\n")

    if not orgnr_liste:
        print("Ingen orgnr funnet — avslutter.")
        return

    stats = prosesser_batch(orgnr_liste, tørrkjør=args.tørrkjør)

    elapsed = time.perf_counter() - t0

    print("\n" + "=" * 60)
    print(f"Ferdig på {elapsed:.0f} sekunder")
    print(f"  Nye selskaper:       {stats['nye_selskap']}")
    print(f"  Nye regnskapsår:     {stats['nye_år']}")
    print(f"  Allerede oppdatert:  {stats['allerede_oppdatert']}")
    print(f"  Feil/ingen data:     {stats['feil']}")

    år_fordeling = stats.get("år_fordeling") or {}
    if år_fordeling:
        print("  Regnskapsår funnet:")
        for år, antall in sorted(år_fordeling.items(), reverse=True):
            print(f"    {år}: {antall}")

    print("=" * 60)


if __name__ == "__main__":
    main()
