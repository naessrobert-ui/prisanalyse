#!/usr/bin/env python3
"""Import shareholder CSV snapshots into PostgreSQL.

Usage:
  python scripts/import_shareholders_orgnr_csv.py \
    --csv path/to/file.csv \
    --snapshot-date 2026-04-17 \
    --database-url postgresql://user:pass@host:5432/dbname
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
from pathlib import Path

import psycopg


HEADER_MAP = {
    "Orgnr": "orgnr",
    "Selskap": "company_name",
    "Aksjeklasse": "share_class",
    "Navn aksjonær": "shareholder_name",
    "Fødselsår/orgnr": "shareholder_identifier",
    "Postnr/sted": "postal_place",
    "Landkode": "country_code",
    "Antall aksjer": "shares_owned",
    "Antall aksjer selskap": "company_total_shares",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import CSV snapshot of company shareholders by orgnr.")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--snapshot-date", required=True, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""), help="PostgreSQL DSN")
    parser.add_argument("--truncate", action="store_true", help="Delete existing rows for snapshot-date before import")
    return parser.parse_args()


def _normalize_number(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip().replace(" ", "")
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return int(float(text))
    except ValueError:
        return None


def _detect_delimiter(csv_text: str) -> str:
    sample = "\n".join(csv_text.splitlines()[:5])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="\t;,")
        return dialect.delimiter
    except csv.Error:
        if "\t" in sample:
            return "\t"
        if ";" in sample:
            return ";"
        return ","


def _read_rows(csv_path: Path) -> list[dict[str, object]]:
    raw_text = csv_path.read_text(encoding="utf-8-sig")
    delimiter = _detect_delimiter(raw_text)

    reader = csv.DictReader(io.StringIO(raw_text), delimiter=delimiter)
    rows: list[dict[str, object]] = []
    for record in reader:
        normalized = {target: record.get(source, "").strip() for source, target in HEADER_MAP.items()}
        if not normalized["orgnr"] or not normalized["shareholder_name"]:
            continue
        normalized["shares_owned"] = _normalize_number(normalized["shares_owned"])
        normalized["company_total_shares"] = _normalize_number(normalized["company_total_shares"])
        rows.append(normalized)
    return rows


def main() -> None:
    args = parse_args()

    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    snapshot_date = dt.date.fromisoformat(args.snapshot_date)
    csv_path = Path(args.csv)
    source_file = csv_path.name

    rows = _read_rows(csv_path)
    if not rows:
        raise SystemExit("No valid rows found in CSV")

    sql = """
        INSERT INTO shareholder_snapshot (
            snapshot_date,
            orgnr,
            company_name,
            share_class,
            shareholder_name,
            shareholder_identifier,
            postal_place,
            country_code,
            shares_owned,
            company_total_shares,
            source_file
        ) VALUES (
            %(snapshot_date)s,
            %(orgnr)s,
            %(company_name)s,
            %(share_class)s,
            %(shareholder_name)s,
            %(shareholder_identifier)s,
            %(postal_place)s,
            %(country_code)s,
            %(shares_owned)s,
            %(company_total_shares)s,
            %(source_file)s
        )
        ON CONFLICT (snapshot_date, orgnr, shareholder_name, shareholder_identifier_norm)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            share_class = EXCLUDED.share_class,
            postal_place = EXCLUDED.postal_place,
            country_code = EXCLUDED.country_code,
            shares_owned = EXCLUDED.shares_owned,
            company_total_shares = EXCLUDED.company_total_shares,
            source_file = EXCLUDED.source_file,
            imported_at = now();
    """

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute("DELETE FROM shareholder_snapshot WHERE snapshot_date = %s", (snapshot_date,))

            payload = []
            for row in rows:
                row["snapshot_date"] = snapshot_date
                row["source_file"] = source_file
                payload.append(row)

            cur.executemany(sql, payload)
        conn.commit()

    print(f"Imported {len(rows)} rows from {csv_path} for {snapshot_date.isoformat()}")


if __name__ == "__main__":
    main()
