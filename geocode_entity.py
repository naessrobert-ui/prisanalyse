#!/usr/bin/env python3
"""
geocode_entity.py
=================
Geokoder adresser i entity-tabellen via Kartverkets gratis API.
Prioriterer selskaper med regnskap og uten underenheter.

Kjøring:
  python geocode_entity.py                    # Alle uten koordinater (med regnskap)
  python geocode_entity.py --alle             # Inkluder også de uten regnskap
  python geocode_entity.py --limit 10000      # Kun de første 10k
  python geocode_entity.py --tørrkjør         # Test uten DB-skriving

Miljøvariabler (.env):
  DATABASE_URL  eller  RDS_HOST + RDS_USER + RDS_DB + AWS_REGION
"""

from __future__ import annotations

import argparse
import os
import subprocess
import time

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

KARTVERKET_URL = "https://ws.geonorge.no/adresser/v1/sok"
BATCH_SIZE     = 100
SLEEP_BETWEEN  = 0.1
MAX_RETRIES    = 3
COMMIT_EVERY   = 500  # Commit til DB hvert N. treff


def _get_iam_token() -> str:
    rds_host   = os.getenv("RDS_HOST",   "brreg-mini-proff.cvos4q86gzmu.eu-north-1.rds.amazonaws.com")
    rds_port   = os.getenv("RDS_PORT",   "5432")
    rds_user   = os.getenv("RDS_USER",   "postgres")
    aws_region = os.getenv("AWS_REGION", "eu-north-1")
    return subprocess.check_output(
        ["aws", "rds", "generate-db-auth-token",
         "--hostname", rds_host, "--port", rds_port,
         "--username", rds_user, "--region", aws_region],
        text=True,
    ).strip()


def db_connect() -> psycopg.Connection:
    database_url = os.environ.get("DATABASE_URL", "")
    if database_url:
        return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=15)
    rds_host = os.getenv("RDS_HOST", "brreg-mini-proff.cvos4q86gzmu.eu-north-1.rds.amazonaws.com")
    rds_port = int(os.getenv("RDS_PORT", "5432"))
    rds_db   = os.getenv("RDS_DB",   "postgres")
    rds_user = os.getenv("RDS_USER", "postgres")
    return psycopg.connect(
        host=rds_host, port=rds_port, dbname=rds_db,
        user=rds_user, password=_get_iam_token(),
        sslmode="require", row_factory=dict_row, connect_timeout=15,
    )


def make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=MAX_RETRIES, backoff_factor=1.0,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": "prisanalyse.no/geocoder"})
    return sess


def geocode(sess: requests.Session, adresse: str, kommunenummer: str | None) -> tuple[float, float] | None:
    params: dict = {"sok": adresse, "treffPerSide": 1, "side": 0}
    if kommunenummer:
        params["kommunenummer"] = kommunenummer
    try:
        r = sess.get(KARTVERKET_URL, params=params, timeout=10)
        if r.status_code != 200:
            return None
        adresser = r.json().get("adresser", [])
        if not adresser:
            if kommunenummer:
                return geocode(sess, adresse, None)
            return None
        rp = adresser[0].get("representasjonspunkt", {})
        lat, lon = rp.get("lat"), rp.get("lon")
        if lat and lon:
            return float(lat), float(lon)
    except Exception as e:
        print(f"  [WARN] API-feil for '{adresse}': {e}")
    return None


def main():
    parser = argparse.ArgumentParser(description="Geokod entity via Kartverket")
    parser.add_argument("--limit",    type=int,  default=None, help="Maks antall å geokode")
    parser.add_argument("--alle",     action="store_true",     help="Inkluder selskaper uten regnskap")
    parser.add_argument("--tørrkjør", action="store_true",     help="Ikke skriv til DB")
    args = parser.parse_args()

    sess = make_session()
    conn = db_connect()

    # Bygg WHERE-klausul
    # Standard: kun selskaper med regnskap og uten underenheter
    if args.alle:
        regnskap_filter = ""
        bedr_filter = ""
    else:
        regnskap_filter = "JOIN regnskap_siste r ON r.orgnr = e.orgnr"
        bedr_filter = "AND NOT EXISTS (SELECT 1 FROM bedr_navn bn WHERE bn.parent_orgnr = e.orgnr)"

    limit_sql = f"LIMIT {args.limit}" if args.limit else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT e.orgnr, e.adresse, e.kommunenummer, e.navn
            FROM entity e
            {regnskap_filter}
            WHERE e.lat IS NULL
              AND e.adresse IS NOT NULL
              AND e.adresse != ''
              {bedr_filter}
            ORDER BY e.orgnr
            {limit_sql}
        """)
        rows = cur.fetchall()

    total = len(rows)
    print(f"Fant {total:,} selskaper å geokode")
    if not args.alle:
        print("  Kun selskaper med regnskap og uten underenheter")
    if args.tørrkjør:
        print("  TØRRKJØR — skriver ikke til DB")

    ok = 0
    feil = 0
    pending_updates: list[tuple] = []

    for i, row in enumerate(rows, 1):
        orgnr         = row["orgnr"]
        adresse       = (row["adresse"] or "").strip()
        kommunenummer = row["kommunenummer"]

        if not adresse:
            feil += 1
            continue

        result = geocode(sess, adresse, kommunenummer)

        if result:
            lat, lon = result
            ok += 1
            if not args.tørrkjør:
                pending_updates.append((lat, lon, orgnr))
            if i <= 5 or i % 500 == 0:
                print(f"  [{i:6}/{total}] {adresse[:45]:45} → {lat:.4f}, {lon:.4f}")
        else:
            feil += 1
            if feil <= 5:
                print(f"  [{i:6}/{total}] INGEN TREFF: {adresse[:60]}")

        # Batch-commit
        if not args.tørrkjør and len(pending_updates) >= COMMIT_EVERY:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE entity SET lat = %s, lon = %s WHERE orgnr = %s",
                    pending_updates,
                )
            conn.commit()
            pending_updates = []
            print(f"  → {i}/{total} lagret | ok={ok} feil={feil} ({ok/i*100:.1f}% treff)")

        time.sleep(SLEEP_BETWEEN)

    # Siste batch
    if not args.tørrkjør and pending_updates:
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE entity SET lat = %s, lon = %s WHERE orgnr = %s",
                pending_updates,
            )
        conn.commit()

    print(f"\nFerdig: {ok:,} geokodede, {feil:,} feil av {total:,} totalt ({ok/max(total,1)*100:.1f}% treff)")
    conn.close()


if __name__ == "__main__":
    main()
