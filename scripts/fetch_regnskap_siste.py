#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from decimal import Decimal
from typing import Any

import psycopg
import requests


DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("Mangler DATABASE_URL")

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


def payload_hash(payload: Any) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def as_list(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def pick_latest(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    def sort_key(item: dict[str, Any]) -> tuple[str, str]:
        periode = item.get("regnskapsperiode") or {}
        til = str(periode.get("tilDato") or "")
        fra = str(periode.get("fraDato") or "")
        return (til, fra)

    valid = [x for x in items if isinstance(x, dict)]
    if not valid:
        return None
    return sorted(valid, key=sort_key, reverse=True)[0]


def get_nested(data: dict[str, Any], *path: str) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
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


def flatten_latest(item: dict[str, Any]) -> dict[str, Any]:
    fra = get_nested(item, "regnskapsperiode", "fraDato")
    til = get_nested(item, "regnskapsperiode", "tilDato")

    return {
        "journalnr": item.get("journalnr"),
        "regnskapsaar": to_year(til) or to_year(fra),
        "regnskapstype": item.get("regnskapstype"),
        "organisasjonsform": get_nested(item, "virksomhet", "organisasjonsform"),
        "morselskap": to_bool(get_nested(item, "virksomhet", "morselskap")),
        "valuta": item.get("valuta"),
        "avviklingsregnskap": to_bool(item.get("avviklingsregnskap")),
        "oppstillingsplan": item.get("oppstillingsplan"),
        "ikke_revidert_aarsregnskap": to_bool(get_nested(item, "revisjon", "ikkeRevidertAarsregnskap")),
        "fravalg_revisjon": to_bool(get_nested(item, "revisjon", "fravalgRevisjon")),
        "smaa_foretak": to_bool(get_nested(item, "regnkapsprinsipper", "smaaForetak")),
        "regnskapsregler": get_nested(item, "regnkapsprinsipper", "regnskapsregler"),

        "sum_driftsinntekter": to_decimal(get_nested(item, "resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter")),
        "sum_driftskostnad": to_decimal(get_nested(item, "resultatregnskapResultat", "driftsresultat", "driftskostnad", "sumDriftskostnad")),
        "driftsresultat": to_decimal(get_nested(item, "resultatregnskapResultat", "driftsresultat", "driftsresultat")),
        "netto_finans": to_decimal(get_nested(item, "resultatregnskapResultat", "finansresultat", "nettoFinans")),
        "sum_finansinntekter": to_decimal(get_nested(item, "resultatregnskapResultat", "finansresultat", "finansinntekt", "sumFinansinntekter")),
        "sum_finanskostnad": to_decimal(get_nested(item, "resultatregnskapResultat", "finansresultat", "finanskostnad", "sumFinanskostnad")),
        "ordinaert_resultat_foer_skattekostnad": to_decimal(get_nested(item, "resultatregnskapResultat", "ordinaertResultatFoerSkattekostnad")),
        "aarsresultat": to_decimal(get_nested(item, "resultatregnskapResultat", "aarsresultat")),
        "totalresultat": to_decimal(get_nested(item, "resultatregnskapResultat", "totalresultat")),

        "sum_eiendeler": to_decimal(get_nested(item, "eiendeler", "sumEiendeler")),
        "sum_omloepsmidler": to_decimal(get_nested(item, "eiendeler", "omloepsmidler", "sumOmloepsmidler")),
        "sum_anleggsmidler": to_decimal(get_nested(item, "eiendeler", "anleggsmidler", "sumAnleggsmidler")),
        "sum_egenkapital": to_decimal(get_nested(item, "egenkapitalGjeld", "egenkapital", "sumEgenkapital")),
        "sum_opptjent_egenkapital": to_decimal(get_nested(item, "egenkapitalGjeld", "egenkapital", "opptjentEgenkapital", "sumOpptjentEgenkapital")),
        "sum_innskutt_egenkapital": to_decimal(get_nested(item, "egenkapitalGjeld", "egenkapital", "innskuttEgenkapital", "sumInnskuttEgenkaptial")),
        "sum_gjeld": to_decimal(get_nested(item, "egenkapitalGjeld", "gjeldOversikt", "sumGjeld")),
        "sum_kortsiktig_gjeld": to_decimal(get_nested(item, "egenkapitalGjeld", "gjeldOversikt", "kortsiktigGjeld", "sumKortsiktigGjeld")),
        "sum_langsiktig_gjeld": to_decimal(get_nested(item, "egenkapitalGjeld", "gjeldOversikt", "langsiktigGjeld", "sumLangsiktigGjeld")),
        "sum_egenkapital_gjeld": to_decimal(get_nested(item, "egenkapitalGjeld", "sumEgenkapitalGjeld")),

        "regnskapsperiode_fra": fra,
        "regnskapsperiode_til": til,
    }


def lock_batch(conn: psycopg.Connection, batch_size: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with picked as (
                select orgnr
                from regnskap_queue
                where status in ('pending', 'error')
                  and coalesce(locked_at < now() - interval '30 minutes', true)
                order by attempts asc, orgnr asc
                limit %s
                for update skip locked
            )
            update regnskap_queue q
            set locked_at = now()
            from picked
            where q.orgnr = picked.orgnr
            returning q.orgnr
            """,
            (batch_size,),
        )
        rows = [row[0] for row in cur.fetchall()]
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
            """
            update regnskap_queue
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



def upsert_regnskap(conn: psycopg.Connection, orgnr: str, item: dict[str, Any], raw_payload: Any) -> None:
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
            """
            insert into regnskap_siste (
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
                        set_queue_status(conn, orgnr, "no_data", 200, "empty_payload")
                    return "no_data"

                with psycopg.connect(DATABASE_URL) as conn:
                    upsert_regnskap(conn, orgnr, latest, payload)

                with psycopg.connect(DATABASE_URL) as conn:
                    set_queue_status(conn, orgnr, "done", 200, None)

                return "done"

            if status == 404:
                with psycopg.connect(DATABASE_URL) as conn:
                    set_queue_status(conn, orgnr, "no_data", 404, None)
                return "no_data"

            if status == 429 or status >= 500:
                wait = min(BACKOFF_CAP, 2 ** attempt)
                time.sleep(wait)
                continue

            with psycopg.connect(DATABASE_URL) as conn:
                set_queue_status(conn, orgnr, "error", status, f"unexpected_http_status:{status}")
            return "error"

        except Exception as e:
            last_err = str(e)
            wait = min(BACKOFF_CAP, 2 ** attempt)
            time.sleep(wait)

    with psycopg.connect(DATABASE_URL) as conn:
        set_queue_status(conn, orgnr, "error", last_status, last_err or "request_failed")
    return "error"


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": "prisanalyse-regnskap-fetcher/2.0"})

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