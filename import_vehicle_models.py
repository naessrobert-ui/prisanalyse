"""Make/model choices backed by the same Norwegian price lookup as ImportRadar."""
from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path


LOOKUP_PATH = Path(__file__).resolve().parent / "data" / "prislookup.csv"


@lru_cache(maxsize=1)
def vehicle_model_catalog():
    choices = {}
    with LOOKUP_PATH.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            make, model = row.get("Produsent", "").strip(), row.get("Modell", "").strip()
            if (row.get("drivstoff", "").strip().casefold() != "elektrisk" or not make or not model
                    or model.casefold() in {"andre", "ukjent"}):
                continue
            choices.setdefault(make, set()).add(model)
    return {make: sorted(models, key=str.casefold)
            for make, models in sorted(choices.items(), key=lambda item: item[0].casefold())}
