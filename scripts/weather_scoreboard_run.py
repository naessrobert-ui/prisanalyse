#!/usr/bin/env python3
"""Kommandolinje for treffsikkerhetsloggen bak /ver/sammenlign.

    python -m scripts.weather_scoreboard_run              # cron: logg varsler + hent fasit
    python -m scripts.weather_scoreboard_run --rapport    # hvem traff best hittil?
    python -m scripts.weather_scoreboard_run --rapport --dager 30 --json
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import pandas as pd

from scripts.weather_scoreboard import (
    BASES,
    ELEMENTS,
    STATIONS,
    collect_observations,
    report,
    snapshot,
)


def _log(hours_back: int) -> int:
    forecasts = snapshot()
    observations = collect_observations(hours_back=hours_back)
    print(json.dumps({"prognoser": forecasts, "observasjoner": observations}, ensure_ascii=False))
    # Google kan mangle nøkkel lokalt; det skal ikke felle cron-jobben så lenge
    # minst én leverandør ble lagret.
    return 0 if forecasts["rows"] or observations["rows"] else 1


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "–"
    return f"{value:.{digits}f}"


def _print_report(data: dict[str, Any]) -> None:
    coverage = data["coverage"]
    print(f"Treffsikkerhet {coverage['from'][:16]} – {coverage['to'][:16]} (UTC)")
    stations = "; ".join(
        "{0} = {1} ({2}, {3} km unna)".format(place, s["name"], s["source_id"], s["km"])
        for place, s in STATIONS.items()
    )
    print(f"Fasit: {stations}")
    print(f"{coverage['comparisons']} sammenligninger over {coverage['hours']} varselstimer "
          f"fra {coverage['runs']} kjøringer")
    if coverage["basis"] == "instant":
        print("Temp/vind måles mot observasjonen ved timens start (Yrs konvensjon). "
              "Kjør --fasit interval for Googles.")
    else:
        print("Temp/vind måles mot snittet over timen (Googles konvensjon). "
              "Kjør --fasit instant for Yrs.")
    print()

    score = data["score"]
    if score.empty:
        print("Ingen parede timer ennå. Loggen trenger minst noen timer med både")
        print("varsel og observasjon før det går an å si hvem som er best.")
        return

    # `score` kommer allerede sortert per sted og i elementenes egen rekkefølge.
    for (place, element), group in score.groupby(["place", "element"], sort=False):
        spec = ELEMENTS[element]
        print(f"{place.capitalize()} – {spec['label']} ({spec['unit']})")
        print(f"  {'lead':>7} {'n':>6} {'Yr MAE':>8} {'Google':>8} {'Yr bias':>8} "
              f"{'G. bias':>8} {'Yr vant':>8}  dom")
        for row in group.itertuples():
            interval = ""
            if row.diff_low is not None and not pd.isna(row.diff_low):
                interval = f" [{_fmt(row.diff_low)}, {_fmt(row.diff_high)}]"
            verdict = {"yr": "Yr best", "google": "Google best"}.get(row.winner, "uavgjort")
            print(f"  {row.bucket:>7} {row.n:>6} {_fmt(row.yr_mae):>8} {_fmt(row.google_mae):>8} "
                  f"{_fmt(row.yr_bias):>8} {_fmt(row.google_bias):>8} "
                  f"{_fmt(row.yr_win_rate * 100, 0) + '%':>8}  {verdict}{interval}")
        print()

    rain = data["rain"]
    if not rain.empty:
        print("Traff de på om det regnet? (terskel 0,1 mm)")
        print(f"  {'sted':>12} {'kilde':>8} {'n':>6} {'våte t.':>8} {'riktig':>8} {'POD':>7} {'FAR':>7}")
        for row in rain.itertuples():
            print(f"  {row.place:>12} {row.provider:>8} {row.n:>6} {row.wet_hours:>8} "
                  f"{_fmt(row.accuracy * 100, 0) + '%':>8} {_fmt(row.pod * 100, 0) + '%':>7} "
                  f"{_fmt(row.far * 100, 0) + '%':>7}")
        print()

    print("MAE = gjennomsnittlig avvik, lavest er best. Bias = systematisk avvik,")
    print("positiv betyr for høyt varsel. Klammene er et 95 %-intervall for")
    print("forskjellen (Google minus Yr) med døgn som blokker; når det spenner")
    print("over null er forskjellen for liten til å kalle en vinner.")


def main(argv: list[str] | None = None) -> int:
    # Windows-konsollet bruker cp1252 som standard og kveler ae/oe/aa i tabellen.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Logg og scor Yr mot Google.")
    parser.add_argument("--rapport", action="store_true", help="Skriv ut scoretabellen i stedet for å logge.")
    parser.add_argument("--dager", type=int, default=14, help="Antall døgn bakover i rapporten (standard 14).")
    parser.add_argument("--timer-tilbake", type=int, default=12,
                        help="Hvor mange timer med observasjoner som hentes på nytt (standard 12).")
    parser.add_argument("--fasit", choices=BASES, default="instant",
                        help="Fasitgrunnlag for temperatur og vind: øyeblikksverdi ved timens "
                             "start (instant, Yrs konvensjon) eller snitt over timen "
                             "(interval, Googles konvensjon). Standard instant.")
    parser.add_argument("--json", action="store_true", help="Skriv rapporten som JSON.")
    args = parser.parse_args(argv)

    if not args.rapport:
        return _log(args.timer_tilbake)

    data = report(days=args.dager, basis=args.fasit)
    if args.json:
        print(json.dumps({
            "coverage": data["coverage"],
            "score": data["score"].to_dict(orient="records"),
            "rain": data["rain"].to_dict(orient="records"),
        }, ensure_ascii=False, default=str))
    else:
        _print_report(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
