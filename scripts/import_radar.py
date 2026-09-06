"""Kjør importkalkylen på normaliserte annonseuttrekk, se docs/import_radar.md."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
from html import escape
import json
from pathlib import Path

from import_radar import Settings, evaluate_listings


def render_html(report):
    def money(value):
        return "—" if value is None else f"{value:,.0f}".replace(",", " ")

    rows = []
    for r in report["results"]:
        c, v = r.get("calculation", {}), r["valuation"]
        observation = r.get("purchase_observation", {})
        net = observation.get("unconfirmed_net_scenario", {})
        rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in [
            f'<a href="{escape(r["url"], quote=True)}">{escape(r["make"])} {escape(r["model"])}</a>'
            f'<br>{r["model_year"]} · {money(r["mileage_km"])} km',
            escape(r["status"].replace("_", " ")),
            money(observation.get("gross_plus_freight_nok")),
            money(net.get("plus_freight_nok")),
            money(v.get("forventet_pris")), money(v.get("hurtigpris")),
            money(c.get("required_customer_price_nok")), money(c.get("margin_nok")),
            money(r.get("net_scenario_calculation", {}).get("margin_nok")),
            money(c.get("max_purchase_amount")) + " " + escape(c.get("purchase_currency", "")),
            "<br>".join(escape(x) for x in r["review_reasons"]),
        ]) + "</tr>")
    settings = report["settings"]
    source_summary = ""
    for source in report.get("sources", []):
        source_summary += "<p><strong>" + escape(source["source"]) + ": " + escape(source["status"]) \
            + "</strong> · " + str(source.get("matched", 0)) + " treff. " \
            + escape("; ".join(source.get("errors", []))) + "</p>"
    return """<!doctype html><html lang="nb"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ImportRadar – kalkyle</title><style>
body{font:15px system-ui,sans-serif;color:#172b3a;background:#f5f7fa;margin:32px}
h1{margin-bottom:8px}table{border-collapse:collapse;background:white;width:100%}
th,td{padding:12px;text-align:left;border-bottom:1px solid #ddd;vertical-align:top}
th{background:#16364a;color:white}a{color:#17608a}.scroll{overflow-x:auto}
details{margin-top:24px}pre{white-space:pre-wrap;font-size:12px}</style>
<h1>ImportRadar</h1>
""" + ("<p>Live søk. Se kildestatus og dekningsomfang i rapportdataene. Ingen daglig tidsplan er aktivert av denne kjøringen.</p>" if report.get("live_collection") else "<p>Prøvekalkyle fra innleste annonser. Ingen løpende annonsehenting er aktiv.</p>") \
        + source_summary \
        + f"<p>Frakt: Tyskland {money(settings['freight_de_nok'])} kr · Sverige " \
        f"{money(settings['freight_se_nok'])} kr. Marginmål: {money(settings['target_margin_nok'])} kr. " \
        f"Prisgrunnlag: {escape(settings['price_basis'])}. Registrering: {escape(report['registration_date'])}.</p>" \
        + "<p>Alle norske priser er i NOK. Margin er etter oppgitte kostnader og utgående moms, " \
        "før selskapsskatt og faste driftskostnader. Maks kjøpspris gjelder eksportbeløpet som faktisk betales.</p>" \
        + "<p>Innkjøp + frakt inkluderer ikke norske avgifter, klargjøring eller margin. " \
        "Nettoscenario bruker oppgitt nettopris fra annonsen; eksportvilkår er ikke bekreftet.</p>" \
        + '<div class="scroll"><table><thead><tr>' \
        + "".join(f"<th>{t}</th>" for t in ["Bil", "Status", "Bruttoinnkjøp + frakt",
                                             "Nettoscenario + frakt (ubekreftet)", "Markedspris", "Hurtigpris",
                                             "Kundepris for marginmål", "Beregnet margin",
                                             "Margin ved oppgitt nettopris (ubekreftet)",
                                             "Maks kjøpspris", "Kontrollpunkter"]) \
        + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>" \
        + f"<p>{report['input_count']} innleste rader · {report['unique_count']} unike annonser · " \
        f"{len(report['rejected'])} ugyldige rader.</p>" \
        + "<details><summary>Forutsetninger og full kalkyle per bil</summary><pre>" \
        + escape(json.dumps(report, ensure_ascii=False, indent=2)) + "</pre></details></html>"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON-liste med normaliserte annonser")
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1]
                        / "config/import_radar.json")
    parser.add_argument("--eur-nok", type=float, required=True, help="NOK per EUR")
    parser.add_argument("--sek-nok", type=float, required=True, help="NOK per SEK, ikke per 100 SEK")
    parser.add_argument("--registration-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--other-costs-nok", type=float,
                        help="Klargjøring, registreringsgebyrer, klimagassavgift mv. etter mva-fradrag")
    parser.add_argument("--price-basis", choices=["hurtigpris", "forventet_pris"])
    parser.add_argument("--output", type=Path, required=True, help="Rapportfil uten filendelse")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    kwargs = dict(config["calculation"])
    kwargs.update(registration_date=args.registration_date, eur_nok=args.eur_nok, sek_nok=args.sek_nok)
    if args.other_costs_nok is not None:
        kwargs["other_costs_nok"] = args.other_costs_nok
    if args.price_basis:
        kwargs["price_basis"] = args.price_basis
    try:
        settings = Settings(**kwargs)
        rows = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("Annonsefilen må være en JSON-liste")
        report = evaluate_listings(rows, settings)
    except (ValueError, TypeError) as exc:
        parser.error(str(exc))
    report["settings"] = asdict(settings)
    report["settings"]["registration_date"] = settings.registration_date.isoformat()
    report["saved_searches"] = config["searches"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix(".json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    args.output.with_suffix(".html").write_text(render_html(report), encoding="utf-8")
    print(f"{report['unique_count']} unike annonser; "
          f"{sum(r['status'] == 'kandidat' for r in report['results'])} kandidater. "
          f"Rapport: {args.output.with_suffix('.html')}")
    print("Annonsehenting og varsling er ikke aktivert.")


if __name__ == "__main__":
    main()
