#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hent_innbyggere_ssb.py
======================
Henter folketall per kommune fra SSB (tabell 07459) og skriver dem til
data/kommune_innbyggere.json, som fylkesaggregatet leser.

Kjøres der utgående nett mot data.ssb.no er åpent (f.eks. lokalt eller på
serveren) – ikke i det nettbegrensede agentmiljøet:

    python scripts/hent_innbyggere_ssb.py
    python scripts/hent_innbyggere_ssb.py --aar 2024

Kilde: SSB tabell 07459 «Befolkning, etter region, kjønn, alder, ...»,
statistikkvariabel Personer1 (folkemengde per 1. januar). Kjønn og alder
summeres bort. Uten --aar brukes siste tilgjengelige år.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

import requests

SSB_URL = "https://data.ssb.no/api/v0/no/table/07459"
DATA_STI = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "kommune_innbyggere.json",
)


def _bygg_query(aar: str | None) -> dict:
    tid = (
        {"filter": "item", "values": [aar]}
        if aar
        else {"filter": "top", "values": ["1"]}  # siste tilgjengelige år
    )
    return {
        "query": [
            {"code": "Region", "selection": {"filter": "all", "values": ["*"]}},
            {"code": "ContentsCode", "selection": {"filter": "item", "values": ["Personer1"]}},
            {"code": "Tid", "selection": tid},
        ],
        "response": {"format": "json-stat2"},
    }


def _parse_json_stat2(data: dict) -> tuple[dict[str, int], str]:
    """Trekker ut {kommunenr(4 siffer): folketall} og året fra json-stat2."""
    dimensjon = data["dimension"]
    region_kat = dimensjon["Region"]["category"]
    region_index = region_kat["index"]        # kode -> posisjon
    verdier = data["value"]

    # Årsetikett (for logging/metadata)
    tid_labels = dimensjon["Tid"]["category"]["label"]
    aar = ", ".join(tid_labels.values())

    # Med kjønn/alder summert bort ligger regionene i rekkefølge etter index.
    pos_til_kode = {pos: kode for kode, pos in region_index.items()}

    resultat: dict[str, int] = {}
    for pos, kode in pos_til_kode.items():
        if not re.fullmatch(r"\d{4}", str(kode)):
            continue  # hopp over fylker, landet, historiske aggregater
        verdi = verdier[pos] if pos < len(verdier) else None
        if verdi is None:
            continue
        try:
            resultat[str(kode)] = int(verdi)
        except (TypeError, ValueError):
            continue
    return resultat, aar


def main() -> int:
    parser = argparse.ArgumentParser(description="Hent folketall per kommune fra SSB")
    parser.add_argument("--aar", default=None, help="Årstall, f.eks. 2024. Default: siste tilgjengelige.")
    args = parser.parse_args()

    try:
        resp = requests.post(SSB_URL, json=_bygg_query(args.aar), timeout=60)
        resp.raise_for_status()
        kommuner, aar = _parse_json_stat2(resp.json())
    except requests.RequestException as exc:
        print(f"[FEIL] Klarte ikke hente fra SSB: {exc}", file=sys.stderr)
        return 1
    except (KeyError, ValueError) as exc:
        print(f"[FEIL] Uventet svarformat fra SSB: {exc}", file=sys.stderr)
        return 1

    if not kommuner:
        print("[FEIL] Fant ingen kommuner i SSB-svaret.", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(DATA_STI), exist_ok=True)
    with open(DATA_STI, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(kommuner.items())), f, ensure_ascii=False, indent=0)

    vestland = sum(1 for k in kommuner if k.startswith("46"))
    print(f"Skrev {len(kommuner)} kommuner (år: {aar}) -> {DATA_STI}")
    print(f"  herav {vestland} i Vestland (46xx)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
