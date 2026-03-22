#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg


FIELD_RULES = [
    {"target": "orgnr", "sources": ["organisasjonsnummer"], "kind": "orgnr"},
    {"target": "navn", "sources": ["navn"], "kind": "text"},
    {"target": "orgform", "sources": ["organisasjonsform.kode"], "kind": "text"},
    {"target": "naeringskode", "sources": ["naeringskode1.kode"], "kind": "text"},
    {
        "target": "kommunenummer",
        "sources": ["forretningsadresse.kommunenummer", "postadresse.kommunenummer"],
        "kind": "text",
    },
    {
        "target": "postnummer",
        "sources": ["forretningsadresse.postnummer", "postadresse.postnummer"],
        "kind": "text",
    },
    {
        "target": "adresse",
        "sources": ["forretningsadresse.adresse", "postadresse.adresse"],
        "kind": "text",
    },
    {"target": "ansatte", "sources": ["antallAnsatte"], "kind": "int"},
    {"target": "mva", "sources": ["registrertIMvaRegisteret"], "kind": "bool"},
    {"target": "stiftet", "sources": ["stiftelsesdato"], "kind": "date"},
    {"target": "registrert", "sources": ["registreringsdatoenhetsregisteret"], "kind": "date"},
]


def choose_value(row: dict[str, str], sources: list[str]) -> str:
    for source in sources:
        value = (row.get(source, "") or "").strip()
        if value:
            return value
    return ""


def normalize_bool(value: str) -> str:
    lowered = (value or "").strip().lower()
    if lowered in {"true", "1", "ja", "j", "yes"}:
        return "true"
    if lowered in {"false", "0", "nei", "n", "no"}:
        return "false"
    return ""


def cast_expr(stage_alias: str, column: str, kind: str) -> str:
    source = f"NULLIF(BTRIM({stage_alias}.{column}), '')"

    if kind == "orgnr":
        # Behold som tekst med bare sifre
        return f"NULLIF(regexp_replace(COALESCE({stage_alias}.{column}, ''), '\\\\D', '', 'g'), '')"

    if kind == "int":
        # Bare cast hvis hele feltet faktisk er et heltall
        return (
            f"CASE "
            f"WHEN NULLIF(BTRIM({stage_alias}.{column}), '') ~ '^\\d+$' "
            f"THEN NULLIF(BTRIM({stage_alias}.{column}), '')::integer "
            f"ELSE NULL END"
        )

    if kind == "bool":
        return (
            f"CASE LOWER(BTRIM(COALESCE({stage_alias}.{column}, ''))) "
            "WHEN 'true' THEN true "
            "WHEN '1' THEN true "
            "WHEN 'ja' THEN true "
            "WHEN 'j' THEN true "
            "WHEN 'yes' THEN true "
            "WHEN 'false' THEN false "
            "WHEN '0' THEN false "
            "WHEN 'nei' THEN false "
            "WHEN 'n' THEN false "
            "WHEN 'no' THEN false "
            "ELSE NULL END"
        )

    if kind == "date":
        return (
            f"CASE "
            f"WHEN NULLIF(BTRIM({stage_alias}.{column}), '') ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' "
            f"THEN NULLIF(BTRIM({stage_alias}.{column}), '')::date "
            f"ELSE NULL END"
        )

    return source

def existing_entity_columns(cur: psycopg.Cursor) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'entity'
        """
    )
    return {row[0] for row in cur.fetchall()}


def merge_csv(args: argparse.Namespace) -> None:
    csv_path = Path(args.csv_path)
    database_url = args.database_url or os.getenv("DATABASE_URL")

    if not database_url:
        raise SystemExit("Mangler DATABASE_URL. Sett miljøvariabel eller bruk --database-url.")

    if not csv_path.exists():
        raise SystemExit(f"Fant ikke fil: {csv_path}")

    with csv_path.open("r", encoding=args.encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit("CSV-filen mangler header-rad.")

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                entity_cols = existing_entity_columns(cur)
                active_rules = [rule for rule in FIELD_RULES if rule["target"] in entity_cols]

                if "orgnr" not in {rule["target"] for rule in active_rules}:
                    raise SystemExit("Fant ikke kolonnen 'orgnr' i entity-tabellen.")

                stage_cols = [rule["target"] for rule in active_rules]

                cur.execute("DROP TABLE IF EXISTS entity_underenheter_stage")
                cur.execute(
                    f"""
                    CREATE TEMP TABLE entity_underenheter_stage (
                        {", ".join(f"{col} text" for col in stage_cols)}
                    ) ON COMMIT DROP
                    """
                )

                staged_rows = 0
                with cur.copy(
                    f"""
                    COPY entity_underenheter_stage ({", ".join(stage_cols)})
                    FROM STDIN
                    """
                ) as copy:
                    for row in reader:
                        values = []
                        for rule in active_rules:
                            value = choose_value(row, rule["sources"])
                            if rule["kind"] == "bool":
                                value = normalize_bool(value)
                            values.append(value)
                        copy.write_row(tuple(values))
                        staged_rows += 1

                join_orgnr = cast_expr("s", "orgnr", "orgnr")

                insert_cols = [rule["target"] for rule in active_rules]
                insert_vals = [cast_expr("s", rule["target"], rule["kind"]) for rule in active_rules]

                update_assignments = []
                for rule in active_rules:
                    if rule["target"] == "orgnr":
                        continue

                    expr = cast_expr("s", rule["target"], rule["kind"])

                    if rule["kind"] == "text":
                        update_assignments.append(
                            f"""{rule["target"]} = CASE
                                    WHEN NULLIF(BTRIM(COALESCE(e.{rule["target"]}::text, '')), '') IS NULL
                                     AND {expr} IS NOT NULL
                                    THEN {expr}
                                    ELSE e.{rule["target"]}
                                END"""
                        )
                    else:
                        update_assignments.append(
                            f"""{rule["target"]} = CASE
                                    WHEN e.{rule["target"]} IS NULL
                                     AND {expr} IS NOT NULL
                                    THEN {expr}
                                    ELSE e.{rule["target"]}
                                END"""
                        )

                cur.execute(
                    f"""
                    SELECT
                        COUNT(*)::int AS stage_rows,
                        COUNT(*) FILTER (WHERE e.orgnr IS NULL)::int AS inserts,
                        COUNT(*) FILTER (WHERE e.orgnr IS NOT NULL)::int AS matched_existing
                    FROM entity_underenheter_stage s
                    LEFT JOIN entity e
                      ON NULLIF(BTRIM(COALESCE(e.orgnr::text, '')), '') = {join_orgnr}
                    """
                )
                stage_rows, insert_count, matched_existing = cur.fetchone()

                if args.dry_run:
                    conn.rollback()
                    print("Dry-run ferdig.")
                    print(f"  Stagede rader   : {stage_rows}")
                    print(f"  Nye inserts     : {insert_count}")
                    print(f"  Eksisterende hit: {matched_existing}")
                    return

                cur.execute(
                    f"""
                    UPDATE entity e
                    SET {", ".join(update_assignments)}
                    FROM entity_underenheter_stage s
                    WHERE NULLIF(BTRIM(COALESCE(e.orgnr::text, '')), '') = {join_orgnr}
                    """
                )
                updated_rows = cur.rowcount

                cur.execute(
                    f"""
                    INSERT INTO entity ({", ".join(insert_cols)})
                    SELECT {", ".join(insert_vals)}
                    FROM entity_underenheter_stage s
                    LEFT JOIN entity e
                      ON NULLIF(BTRIM(COALESCE(e.orgnr::text, '')), '') = {join_orgnr}
                    WHERE e.orgnr IS NULL
                      AND {join_orgnr} IS NOT NULL
                    """
                )
                inserted_rows = cur.rowcount

                conn.commit()

    print("Merge ferdig.")
    print(f"  Stagede rader : {staged_rows}")
    print(f"  Oppdaterte    : {updated_rows}")
    print(f"  Innsatte      : {inserted_rows}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge normalisert underenheter-CSV inn i entity-tabellen."
    )
    parser.add_argument("csv_path", help="Normalisert CSV fra normalize_underenheter_csv.py")
    parser.add_argument("--database-url", help="Postgres connection string. Hvis utelatt brukes DATABASE_URL.")
    parser.add_argument("--encoding", default="utf-8", help="Filencoding. Standard: utf-8.")
    parser.add_argument("--dry-run", action="store_true", help="Vis hvor mange rader som ville blitt merged.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    merge_csv(args)


if __name__ == "__main__":
    main()