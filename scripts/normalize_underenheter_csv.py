#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def load_mapping(mapping_path: Path) -> dict:
    with mapping_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


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


def normalize_value(value: str) -> str:
    return (value or "").strip()


def build_output_fields(mapping: dict) -> list[str]:
    same_fields = mapping["same_fields"]
    alias_targets = list(mapping["aliases"].values())
    todays_only = mapping["dagens_only_fields"]
    ordered: list[str] = []
    for field in [*same_fields, *alias_targets, *todays_only]:
        if field not in ordered:
            ordered.append(field)
    ordered.extend(["source_schema", "source_extra_json"])
    return ordered


def transform_row(row: dict[str, str], mapping: dict, output_fields: list[str]) -> dict[str, str]:
    out = {field: "" for field in output_fields}
    extras: dict[str, str] = {}

    for field in mapping["same_fields"]:
        if field in row:
            out[field] = normalize_value(row.get(field, ""))

    for source_field, target_field in mapping["aliases"].items():
        if source_field in row:
            out[target_field] = normalize_value(row.get(source_field, ""))

    for field in mapping["underenheter_only_fields"]:
        if field in row and normalize_value(row.get(field, "")):
            extras[field] = normalize_value(row[field])

    for field in mapping["dagens_only_fields"]:
        out.setdefault(field, "")

    out["source_schema"] = "underenheter"
    out["source_extra_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True)
    return out


def normalize_csv(args: argparse.Namespace) -> None:
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)
    mapping_path = Path(args.mapping_file)

    if not input_path.exists():
        raise SystemExit(f"Fant ikke inputfil: {input_path}")
    mapping = load_mapping(mapping_path)
    output_fields = build_output_fields(mapping)

    with input_path.open("r", encoding=args.encoding, newline="") as in_handle:
        sample = in_handle.read(8192)
        in_handle.seek(0)
        dialect = sniff_dialect(sample)
        reader = csv.DictReader(in_handle, dialect=dialect)
        if not reader.fieldnames:
            raise SystemExit("CSV-filen mangler header-rad.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=output_fields)
            writer.writeheader()

            input_rows = 0
            output_rows = 0
            for row in reader:
                input_rows += 1
                transformed = transform_row(row, mapping, output_fields)
                writer.writerow(transformed)
                output_rows += 1

    print("Normalisering ferdig.")
    print(f"  Inputfil : {input_path}")
    print(f"  Outputfil: {output_path}")
    print(f"  Mapping  : {mapping_path}")
    print(f"  Inputrader : {input_rows}")
    print(f"  Outputrader: {output_rows}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normaliser underenheter-CSV til dagens kanoniske feltnavn."
    )
    parser.add_argument("input_csv", help="Kilde-CSV med underenheter.")
    parser.add_argument("output_csv", help="Output-CSV med dagens kanoniske feltnavn.")
    parser.add_argument(
        "--mapping-file",
        default="config/underenheter_field_map.json",
        help="JSON-fil med feltmapping.",
    )
    parser.add_argument("--encoding", default="utf-8-sig", help="Filencoding. Standard: utf-8-sig.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    normalize_csv(args)


if __name__ == "__main__":
    main()
