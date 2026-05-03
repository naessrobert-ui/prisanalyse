#!/usr/bin/env python3
"""
geocode_bedr_navn.py
====================
Geokoder adresser i bedr_navn-tabellen via Kartverkets gratis API.
Skriver lat/lon tilbake til databasen.

Kjøring:
  python geocode_bedr_navn.py                  # Geokod alle uten koordinater
  python geocode_bedr_navn.py --limit 10000    # Kun de første 10k
  python geocode_bedr_navn.py --naeringskode 47  # Kun detaljhandel

Miljøvariabler (.env):
  DATABASE_URL  eller  RDS_HOST + RDS_USER + RDS_DB + AWS_REGION
"""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from typing import Any

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# ── Konfig ────────────────────────────────────────────────────────────────────
KARTVERKET_URL = "https://ws.geonorge.no/adresser/v1/sok"
BATCH_SIZE     = 100    # Antall adresser per DB-batch
SLEEP_BETWEEN  = 0.1   # Sekunder mellom hvert API-kall (rate limiting)
MAX_RETRIES    = 3


# ── DB-tilkobling ─────────────────────────────────────────────────────────────
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


# ── HTTP-session ──────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=MAX_RETRIES, backoff_factor=1.0,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": "prisanalyse.no/geocoder"})
    return sess


# ── Geokoding ─────────────────────────────────────────────────────────────────
def geocode(sess: requests.Session, adresse: str, kommunenummer: str | None) -> tuple[float, float] | None:
    """
    Slår opp adresse mot Kartverkets API.
    Returnerer (lat, lon) eller None hvis ikke funnet.
    """
    params: dict[str, Any] = {
        "sok": adresse,
        "treffPerSide": 1,
        "side": 0,
    }
    if kommunenummer:
        params["kommunenummer"] = kommunenummer

    try:
        r = sess.get(KARTVERKET_URL, params=params, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        adresser = data.get("adresser", [])
        if not adresser:
            # Prøv uten kommunenummer hvis første forsøk feiler
            if kommunenummer:
                return geocode(sess, adresse, None)
            return None
        hit = adresser[0]
        representasjonspunkt = hit.get("representasjonspunkt", {})
        lat = representasjonspunkt.get("lat")
        lon = representasjonspunkt.get("lon")
        if lat and lon:
            return float(lat), float(lon)
    except Exception as e:
        print(f"  [WARN] Geokoding feilet for '{adresse}': {e}")
    return None


# ── Hovedlogikk ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Geokod bedr_navn via Kartverket")
    parser.add_argument("--limit",        type=int,  default=None, help="Maks antall å geokode")
    parser.add_argument("--naeringskode", type=str,  default=None, help="Filtrer på næringskode-prefix, f.eks. '47'")
    parser.add_argument("--tørrkjør",     action="store_true",     help="Ikke skriv til DB")
    args = parser.parse_args()

    sess = make_session()
    conn = db_connect()

    # Hent adresser uten koordinater
    where_clauses = ["lat IS NULL", "adresse IS NOT NULL"]
    if args.naeringskode:
        where_clauses.append(f"naeringskode LIKE '{args.naeringskode}%'")

    where_sql = " AND ".join(where_clauses)
    limit_sql  = f"LIMIT {args.limit}" if args.limit else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT bedr_orgnr, adresse, poststed, postnummer, kommunenummer
            FROM bedr_navn
            WHERE {where_sql}
            ORDER BY bedr_orgnr
            {limit_sql}
        """)
        rows = cur.fetchall()

    total = len(rows)
    print(f"Fant {total:,} adresser å geokode")
    if args.naeringskode:
        print(f"  Filtrert på næringskode: {args.naeringskode}*")
    if args.tørrkjør:
        print("  TØRRKJØR — skriver ikke til DB")

    ok = 0
    feil = 0

    for i, row in enumerate(rows, 1):
        orgnr         = row["bedr_orgnr"]
        adresse       = row["adresse"] or ""
        poststed      = row["poststed"] or ""
        kommunenummer = row["kommunenummer"]

        # Bygg søkestreng: "Gateveien 1, OSLO"
        sok = adresse
        if poststed:
            sok = f"{adresse}, {poststed}"

        if not sok.strip():
            feil += 1
            continue

        result = geocode(sess, sok, kommunenummer)

        if result:
            lat, lon = result
            if not args.tørrkjør:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE bedr_navn SET lat = %s, lon = %s WHERE bedr_orgnr = %s",
                        (lat, lon, orgnr),
                    )
                    conn.commit()
            ok += 1
            if i % 500 == 0 or i <= 5:
                print(f"  [{i:6}/{total}] {adresse[:40]:40} → {lat:.4f}, {lon:.4f}")
        else:
            feil += 1
            if feil <= 10:
                print(f"  [{i:6}/{total}] INGEN TREFF: {sok[:60]}")

        time.sleep(SLEEP_BETWEEN)

        # Skriv fremgangsrapport hvert 1000. treff
        if i % 1000 == 0:
            print(f"  → {i}/{total} | ok={ok} feil={feil} ({ok/i*100:.1f}% treff)")

    print(f"\nFerdig: {ok:,} geokodede, {feil:,} feil av {total:,} totalt")
    conn.close()


if __name__ == "__main__":
    main()
