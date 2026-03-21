#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path

import psycopg


DEFAULT_PARENT_ORGNR_CANDIDATES = [
    "parent_orgnr",
    "morselskap_orgnr",
    "mor_orgnr",
    "parent_organisation_number",
    "mother_orgnr",
]
DEFAULT_CHILD_ORGNR_CANDIDATES = [
    "child_orgnr",
    "subsidiary_orgnr",
    "datter_orgnr",
    "orgnr",
    "organisasjonsnummer",
]
DEFAULT_PARENT_NAME_CANDIDATES = [
    "parent_name",
    "morselskap_navn",
    "mor_navn",
    "mother_name",
]
DEFAULT_CHILD_NAME_CANDIDATES = [
    "child_name",
    "subsidiary_name",
    "datter_navn",
    "navn",
    "name",
]


def normalize_orgnr(value: str) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits if len(digits) == 9 else ""


def choose_column(headers: list[str], explicit: str | None, candidates: list[str]) -> str | None:
    lowered = {header.strip().lower(): header for header in headers}
    if explicit:
        return lowered.get(explicit.strip().lower(), explicit)
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def sniff_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t,")
    except csv.Error:
        class _Fallback(csv.Dialect):
            delimiter = ";"
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL

        return _Fallback()


def ensure_schema(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subsidiary_relations (
            parent_orgnr text NOT NULL,
            child_orgnr text NOT NULL,
            parent_name text,
            child_name text,
            source_name text NOT NULL,
            raw_row jsonb NOT NULL DEFAULT '{}'::jsonb,
            imported_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (parent_orgnr, child_orgnr)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_subsidiary_relations_child_orgnr
            ON subsidiary_relations (child_orgnr)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_subsidiary_relations_parent_orgnr
            ON subsidiary_relations (parent_orgnr)
        """
    )
    cur.execute(
        """
        CREATE OR REPLACE VIEW subsidiary_children_missing_from_entity AS
        SELECT sr.*
        FROM subsidiary_relations sr
        LEFT JOIN entity e ON e.orgnr::text = sr.child_orgnr
        WHERE e.orgnr IS NULL
        """
    )


def import_csv(args: argparse.Namespace) -> None:
    csv_path = Path(args.csv_path)
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("Mangler DATABASE_URL. Sett miljøvariabel eller bruk --database-url.")
    if not csv_path.exists():
        raise SystemExit(f"Fant ikke fil: {csv_path}")

    with csv_path.open("r", encoding=args.encoding, newline="") as handle:
        sample = handle.read(8192)
        handle.seek(0)
        dialect = sniff_dialect(sample)
        reader = csv.DictReader(handle, dialect=dialect)
        if not reader.fieldnames:
            raise SystemExit("CSV-filen mangler header-rad.")

        headers = list(reader.fieldnames)
        parent_orgnr_col = choose_column(headers, args.parent_orgnr_col, DEFAULT_PARENT_ORGNR_CANDIDATES)
        child_orgnr_col = choose_column(headers, args.child_orgnr_col, DEFAULT_CHILD_ORGNR_CANDIDATES)
        parent_name_col = choose_column(headers, args.parent_name_col, DEFAULT_PARENT_NAME_CANDIDATES)
        child_name_col = choose_column(headers, args.child_name_col, DEFAULT_CHILD_NAME_CANDIDATES)

        if not parent_orgnr_col or not child_orgnr_col:
            raise SystemExit(
                "Klarte ikke finne parent/child orgnr-kolonner automatisk. "
                "Bruk --parent-orgnr-col og --child-orgnr-col."
            )

        print("Bruker kolonner:")
        print(f"  parent_orgnr: {parent_orgnr_col}")
        print(f"  child_orgnr : {child_orgnr_col}")
        print(f"  parent_name : {parent_name_col or '<ikke satt>'}")
        print(f"  child_name  : {child_name_col or '<ikke satt>'}")

        valid_rows = 0
        skipped_rows = 0
        batch_size = max(100, int(args.batch_size))

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                ensure_schema(cur)
                cur.execute("DROP TABLE IF EXISTS subsidiary_relations_stage")
                cur.execute(
                    """
                    CREATE TEMP TABLE subsidiary_relations_stage (
                        parent_orgnr text,
                        child_orgnr text,
                        parent_name text,
                        child_name text,
                        source_name text,
                        raw_row jsonb
                    ) ON COMMIT DROP
                    """
                )

                with cur.copy(
                    """
                    COPY subsidiary_relations_stage
                    (parent_orgnr, child_orgnr, parent_name, child_name, source_name, raw_row)
                    FROM STDIN
                    """
                ) as copy:
                    batch_counter = 0
                    for raw in reader:
                        parent_orgnr = normalize_orgnr(raw.get(parent_orgnr_col, ""))
                        child_orgnr = normalize_orgnr(raw.get(child_orgnr_col, ""))
                        if not parent_orgnr or not child_orgnr:
                            skipped_rows += 1
                            continue

                        copy.write_row(
                            (
                                parent_orgnr,
                                child_orgnr,
                                (raw.get(parent_name_col, "") or "").strip() if parent_name_col else "",
                                (raw.get(child_name_col, "") or "").strip() if child_name_col else "",
                                args.source_name,
                                json.dumps(raw, ensure_ascii=False),
                            )
                        )
                        valid_rows += 1
                        batch_counter += 1
                        if batch_counter >= batch_size:
                            batch_counter = 0

                cur.execute(
                    """
                    INSERT INTO subsidiary_relations (
                        parent_orgnr,
                        child_orgnr,
                        parent_name,
                        child_name,
                        source_name,
                        raw_row,
                        imported_at,
                        updated_at
                    )
                    SELECT
                        parent_orgnr,
                        child_orgnr,
                        NULLIF(parent_name, ''),
                        NULLIF(child_name, ''),
                        source_name,
                        raw_row,
                        now(),
                        now()
                    FROM subsidiary_relations_stage
                    ON CONFLICT (parent_orgnr, child_orgnr) DO UPDATE
                    SET
                        parent_name = COALESCE(EXCLUDED.parent_name, subsidiary_relations.parent_name),
                        child_name = COALESCE(EXCLUDED.child_name, subsidiary_relations.child_name),
                        source_name = EXCLUDED.source_name,
                        raw_row = EXCLUDED.raw_row,
                        updated_at = now()
                    """
                )
                conn.commit()

                cur.execute("SELECT COUNT(*)::int AS n FROM subsidiary_relations")
                total_rows = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*)::int AS n FROM subsidiary_children_missing_from_entity")
                missing_in_entity = cur.fetchone()[0]

    print("")
    print("Import ferdig.")
    print(f"  Gyldige relasjoner lastet : {valid_rows}")
    print(f"  Hoppet over rader         : {skipped_rows}")
    print(f"  Totalt i subsidiary_relations: {total_rows}")
    print(f"  Barn som mangler i entity    : {missing_in_entity}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Importer CSV med morselskap/datterselskap-relasjoner til Postgres."
    )
    parser.add_argument("csv_path", help="Sti til CSV-filen som skal importeres.")
    parser.add_argument(
        "--database-url",
        help="Postgres connection string. Hvis utelatt brukes DATABASE_URL.",
    )
    parser.add_argument(
        "--source-name",
        default="subsidiary_csv",
        help="Kildemerke som lagres i databasen.",
    )
    parser.add_argument("--parent-orgnr-col", help="Kolonnenavn for morselskap orgnr.")
    parser.add_argument("--child-orgnr-col", help="Kolonnenavn for datterselskap orgnr.")
    parser.add_argument("--parent-name-col", help="Kolonnenavn for morselskap navn.")
    parser.add_argument("--child-name-col", help="Kolonnenavn for datterselskap navn.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Filencoding. Standard: utf-8-sig.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Intern batch-størrelse ved lesing.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    import_csv(args)


if __name__ == "__main__":
    main()
