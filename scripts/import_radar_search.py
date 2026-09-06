"""Live search callable manually or from a scheduler, using the same engine as the UI."""
import argparse
from datetime import date
import json
from pathlib import Path

from import_radar import Settings
from import_radar_search import Search, fx_rates, run_search
from scripts.import_radar import render_html


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--make", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--year-from", type=int, default=2022)
    parser.add_argument("--year-to", type=int, default=date.today().year)
    parser.add_argument("--max-km", type=int, default=90000)
    parser.add_argument("--drive", choices=["ANY", "AWD", "2WD"], default="ANY")
    parser.add_argument("--min-battery-kwh", type=float, default=0)
    parser.add_argument("--per-source", type=int, default=5)
    parser.add_argument("--include-non-vat", action="store_true")
    parser.add_argument("--eur-nok", type=float)
    parser.add_argument("--sek-nok", type=float)
    parser.add_argument("--registration-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--freight-de-nok", type=float, default=13000)
    parser.add_argument("--freight-se-nok", type=float, default=8000)
    parser.add_argument("--target-margin-nok", type=float, default=30000)
    parser.add_argument("--other-costs-nok", type=float)
    parser.add_argument("--fx-buffer-pct", type=float, default=0)
    parser.add_argument("--reserve-nok", type=float, default=0)
    parser.add_argument("--assumed-weight-kg", type=float)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.eur_nok is None) != (args.sek_nok is None):
        parser.error("Oppgi begge valutakurser eller ingen")
    try:
        search = Search(args.make, args.model, args.year_from, args.year_to, args.max_km,
                        args.drive, args.min_battery_kwh, not args.include_non_vat, args.per_source)
        fx = fx_rates() if args.eur_nok is None else {"eur_nok": args.eur_nok, "sek_nok": args.sek_nok,
                                                   "date": date.today().isoformat(), "kind": "Egne kurser"}
        settings = Settings(args.registration_date, fx["eur_nok"], fx["sek_nok"],
                            args.freight_de_nok, args.freight_se_nok, args.target_margin_nok,
                            args.fx_buffer_pct, args.other_costs_nok, args.reserve_nok)
        if args.assumed_weight_kg is not None and not 500 <= args.assumed_weight_kg <= 5000:
            raise ValueError("Anslått egenvekt må være mellom 500 og 5000 kg")
        report = run_search(search, settings, fx_info=fx, assumed_weight_kg=args.assumed_weight_kg)
    except (ValueError, KeyError) as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix(".json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    args.output.with_suffix(".html").write_text(render_html(report), encoding="utf-8")
    for source in report["sources"]:
        print(f"{source['source']}: {source['status']}, {source['matched']} biler")
        for error in source.get("errors", []):
            print("  " + error)
    print(f"Rapport: {args.output.with_suffix('.html')}")
    return 1 if all(s["status"] == "error" for s in report["sources"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
