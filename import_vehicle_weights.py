"""Curated, conservative fallback weights for precisely identified variants."""
from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
import re
import unicodedata


CATALOG_PATH = Path(__file__).resolve().parent / "data" / "import_vehicle_weights.csv"


def _key(value):
    return re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", str(value or ""))
                  .encode("ascii", "ignore").decode().lower())


def _drive(value):
    value = str(value or "").upper()
    if value in {"AWD", "4WD", "ALL_WHEEL"}:
        return "AWD"
    if value in {"RWD", "FWD", "2WD"}:
        return "RWD" if value == "RWD" else "2WD"
    return ""


@lru_cache(maxsize=1)
def weight_catalog():
    with CATALOG_PATH.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        for field in ("year_from", "year_to", "weight_kg"):
            row[field] = int(row[field])
        for field in ("battery_min_kwh", "battery_max_kwh"):
            row[field] = float(row[field])
    return rows


def estimate_weight(listing):
    """Return one unambiguous catalog match; never guess a broad model average."""
    make, model = _key(listing.get("make")), _key(listing.get("model"))
    year, battery = listing.get("model_year"), listing.get("battery_kwh")
    drive = _drive(listing.get("drive"))
    text = str(listing.get("variant_text") or "")
    if not make or not model or year is None or battery is None or not drive:
        return None
    matches = []
    for row in weight_catalog():
        required, excluded = row["required_text"], row["excluded_text"]
        if (_key(row["make"]) == make and _key(row["model"]) == model
                and row["year_from"] <= int(year) <= row["year_to"]
                and row["battery_min_kwh"] <= float(battery) <= row["battery_max_kwh"]
                and row["drive"] == drive
                and (not required or re.search(rf"\b{re.escape(required)}\b", text, re.I))
                and (not excluded or not re.search(rf"\b{re.escape(excluded)}\b", text, re.I))):
            matches.append(row)
    return dict(matches[0]) if len(matches) == 1 else None
