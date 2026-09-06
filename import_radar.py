"""Importkalkyle for el-personbiler kjøpt som salgsvare av norsk mva-forhandler.

Ingen nettinnhenting eller varsling her. Normaliserte annonser mates inn av en
separat kilde. BilRadar brukes uendret til norsk markedspris og hurtigpris.
Se docs/import_radar.md for prisgrunnlag, begrensninger og kilder.
"""
from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import urlparse


BRUKSFRADRAG = (
    (0, 0), (1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12),
    (7, 14), (8, 15), (9, 16), (10, 17), (11, 18), (12, 19),
    (14, 20), (16, 21), (18, 22), (20, 23), (22, 24), (24, 26),
    (30, 30), (36, 33), (42, 37), (48, 40), (54, 44), (60, 50),
    (66, 54), (72, 58), (78, 62), (84, 66), (90, 70), (96, 74),
    (102, 78), (108, 82), (120, 90), (132, 91), (144, 92),
    (156, 93), (168, 94), (180, 95), (192, 96), (216, 98), (240, 100),
)
SOURCE_HOSTS = {
    "mobile_de": {"suchen.mobile.de", "www.mobile.de", "mobile.de", "m.mobile.de"},
    "bytbil": {"www.bytbil.com", "bytbil.com"},
}


def number(value, name, *, minimum=0):
    if isinstance(value, bool):
        raise ValueError(f"{name}: forventet tall")
    try:
        n = float(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name}: mangler gyldig tall") from None
    if not math.isfinite(n) or n < minimum:
        raise ValueError(f"{name}: må være endelig og minst {minimum}")
    return n


@dataclass(frozen=True)
class Settings:
    registration_date: date
    eur_nok: float
    sek_nok: float
    freight_de_nok: float = 13_000
    freight_se_nok: float = 8_000
    target_margin_nok: float = 30_000
    fx_buffer_pct: float = 0
    other_costs_nok: float | None = None
    reserve_nok: float = 0
    price_basis: str = "hurtigpris"

    def __post_init__(self):
        if self.registration_date.year != 2026:
            raise ValueError("Avgiftsregler er bare verifisert for registrering i 2026")
        for key in ("eur_nok", "sek_nok"):
            number(getattr(self, key), key, minimum=0.000001)
        for key in ("freight_de_nok", "freight_se_nok", "target_margin_nok",
                    "fx_buffer_pct", "reserve_nok"):
            number(getattr(self, key), key)
        if self.other_costs_nok is not None:
            number(self.other_costs_nok, "other_costs_nok")
        if self.price_basis not in {"hurtigpris", "forventet_pris"}:
            raise ValueError("price_basis må være hurtigpris eller forventet_pris")


def age_discount(first_registration: date, registration: date) -> float:
    if first_registration > registration:
        raise ValueError("Førstegangsregistrering er etter norsk registrering")
    # Siste trinn gjelder allerede fra 1. januar året bilen blir 20 år.
    if registration.year - first_registration.year >= 20:
        return 1.0
    months = ((registration.year - first_registration.year) * 12
              + registration.month - first_registration.month)
    anniversary_day = min(first_registration.day,
                          calendar.monthrange(registration.year, registration.month)[1])
    if registration.day < anniversary_day:
        months -= 1
    return max(pct for m, pct in BRUKSFRADRAG if m <= months) / 100


def weight_tax(weight_kg, first_registration: date, registration: date):
    weight = number(weight_kg, "weight_kg", minimum=1)
    discount = age_discount(first_registration, registration)
    return max(0, weight - 500) * 12.71 * (1 - discount), discount


def gross_sale(net_vehicle_price, registration_tax):
    """Kundepris inkl. mva og engangsavgift/vrakpant."""
    return net_vehicle_price + max(0, net_vehicle_price - 300_000) * 0.25 + registration_tax


def net_sale(customer_price, registration_tax):
    """Inntekt etter utgående mva, inklusive viderefakturert engangsavgift."""
    base = number(customer_price, "customer_price", minimum=1) - registration_tax
    if base < 0:
        raise ValueError("Kundepris er lavere enn engangsavgift og vrakpant")
    vat = max(0, base - 300_000) * 0.25 / 1.25
    return customer_price - vat, vat


def calculate(listing, settings: Settings, norwegian_price):
    """Estimat med full fradragsrett for importmoms på salgsvare.

    Eksportpris brukes bare etter eksplisitt bekreftelse. Uten bekreftelse
    brukes kontant bruttopris uten automatisk fradrag for utenlandsk moms.
    Frakt og øvrige kostnader oppgis etter eventuell fradragsberettiget mva.
    """
    country = listing["country"]
    if country not in {"DE", "SE"}:
        raise ValueError("Kun DE og SE støttes; selgers land må være kjent")
    currency = "EUR" if country == "DE" else "SEK"
    if listing["currency"] != currency:
        raise ValueError("Valuta stemmer ikke med landet")
    gross = number(listing["price_amount"], "price_amount", minimum=1)
    confirmed = listing.get("export_price_confirmed") is True
    purchase = (number(listing.get("export_price_amount"), "export_price_amount", minimum=1)
                if confirmed else gross)
    fx = (settings.eur_nok if country == "DE" else settings.sek_nok)
    fx *= 1 + settings.fx_buffer_pct / 100
    freight = number(listing.get("freight_nok", settings.freight_de_nok if country == "DE"
                                 else settings.freight_se_nok), "freight_nok")
    extra = listing.get("other_costs_nok", settings.other_costs_nok)
    extra = 0 if extra is None else number(extra, "other_costs_nok")
    first = date.fromisoformat(listing["first_registration"])
    tax, discount = weight_tax(listing["weight_kg"], first, settings.registration_date)
    registration_tax = tax + 2_400  # Vrakpant gis ikke bruksfradrag.
    purchase_nok = purchase * fx
    cost = purchase_nok + freight + extra + settings.reserve_nok + registration_tax
    required = gross_sale(cost + settings.target_margin_nok - registration_tax, registration_tax)
    result = {
        "purchase_currency": currency, "purchase_amount": purchase,
        "export_price_confirmed": confirmed, "fx_with_buffer": fx,
        "purchase_nok": purchase_nok, "freight_nok": freight,
        "other_costs_nok": extra, "reserve_nok": settings.reserve_nok,
        "weight_tax_nok": tax, "age_discount_pct": discount * 100,
        "scrappage_tax_nok": 2_400, "cost_net_nok": cost,
        "required_customer_price_nok": required,
    }
    if norwegian_price is not None:
        income, vat = net_sale(norwegian_price, registration_tax)
        result.update({
            "norwegian_price_nok": norwegian_price, "output_vat_nok": vat,
            "margin_nok": income - cost,
            "surplus_after_target_nok": income - cost - settings.target_margin_nok,
            # Kan være negativt: selv en gratis bil dekker da ikke kostnadene.
            "max_purchase_amount": (income - freight - extra - settings.reserve_nok
                                    - registration_tax - settings.target_margin_nok) / fx,
        })
    return {k: round(v, 2) if isinstance(v, float) else v for k, v in result.items()}


def _timestamp(value):
    d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("observed_at må ha tidssone")
    return d.astimezone(timezone.utc)


def purchase_observation(row, settings):
    """Vis innkjøp/frakt selv om data til avgiftskalkylen mangler.

    Annonsert nettopris er et separat, ubekreftet scenario og påvirker aldri
    calculate(), kandidatstatus eller maksimal kjøpspris.
    """
    fx = settings.eur_nok if row["country"] == "DE" else settings.sek_nok
    fx *= 1 + settings.fx_buffer_pct / 100
    freight = number(row.get("freight_nok", settings.freight_de_nok
                            if row["country"] == "DE" else settings.freight_se_nok), "freight_nok")
    gross = number(row["price_amount"], "price_amount", minimum=1)
    result = {"currency": row["currency"], "advertised_gross_amount": gross,
              "fx_with_buffer": fx, "freight_nok": freight,
              "gross_purchase_nok": round(gross * fx, 2),
              "gross_plus_freight_nok": round(gross * fx + freight, 2),
              "includes_norwegian_taxes": False,
              "includes_other_costs": False}
    if row.get("advertised_net_amount") is not None:
        net = number(row["advertised_net_amount"], "advertised_net_amount", minimum=1)
        if net > gross:
            raise ValueError("Annonsert nettopris overstiger bruttopris")
        result["unconfirmed_net_scenario"] = {
            "amount": net, "purchase_nok": round(net * fx, 2),
            "plus_freight_nok": round(net * fx + freight, 2),
            "export_price_confirmed": False,
        }
    return result


def normalize_listing(raw):
    """Valider kontrakten før scoring; ukjent km må aldri bli null km."""
    row = dict(raw)
    if row.get("source") not in SOURCE_HOSTS:
        raise ValueError("source må være mobile_de eller bytbil")
    if row.get("listing_id") is None or not str(row.get("listing_id", "")).strip():
        raise ValueError("listing_id mangler")
    url = urlparse(row.get("url", ""))
    if (url.scheme != "https" or url.hostname not in SOURCE_HOSTS[row["source"]]
            or url.username or url.password):
        raise ValueError("Ugyldig annonselenke")
    for key in ("make", "model"):
        if not isinstance(row.get(key), str) or not row[key].strip():
            raise ValueError(f"{key} mangler")
        row[key] = row[key].strip()
    if row.get("fuel") != "electric":
        raise ValueError("Kun elektriske personbiler støttes (fuel=electric)")
    if row.get("vehicle_type") != "passenger_car":
        raise ValueError("vehicle_type må være passenger_car")
    year = number(row.get("model_year"), "model_year", minimum=1900)
    if int(year) != year or year > date.today().year + 1:
        raise ValueError("Ugyldig modellår")
    row["model_year"] = int(year)
    mileage = number(row.get("mileage"), "mileage")
    if row.get("mileage_unit") not in {"km", "mil"}:
        raise ValueError("mileage_unit må være km eller mil")
    row["mileage_km"] = mileage * (10 if row["mileage_unit"] == "mil" else 1)
    row["observed_at"] = _timestamp(row["observed_at"]).isoformat()
    if row.get("country") not in {"DE", "SE"}:
        raise ValueError("Selgers land må være DE eller SE")
    if row.get("currency") != {"DE": "EUR", "SE": "SEK"}[row["country"]]:
        raise ValueError("Valuta stemmer ikke med landet")
    number(row.get("price_amount"), "price_amount", minimum=1)
    if row.get("advertised_net_amount") is not None:
        net = number(row["advertised_net_amount"], "advertised_net_amount", minimum=1)
        if net > float(row["price_amount"]):
            raise ValueError("Annonsert nettopris overstiger bruttopris")
    for key in ("export_price_confirmed", "vat_reclaimable", "damage_free", "variant_confirmed",
                "model_year_estimated"):
        if key in row and row[key] is not None and not isinstance(row[key], bool):
            raise ValueError(f"{key} må være true, false eller null")
    return row


def score_norwegian_prices(rows):
    import pandas as pd
    from bilradar_scorer import scorer_biler
    from bil_variant_klassifiserer import klassifiser_varianter, last_variantkatalog

    drive = {"AWD": "Firehjul", "4WD": "Firehjul", "ALL_WHEEL": "Firehjul",
             "FWD": "Tohjul", "RWD": "Tohjul", "2WD": "Tohjul"}
    frame = pd.DataFrame([{
        # Behold kilde-ID separat; FinnKode-feltet ville deduplisert på tvers av kilder.
        "_import_key": r["source"] + ":" + str(r["listing_id"]),
        "Merke": r["make"], "Modell": r["model"], "Årstall": r["model_year"],
        "Kjørelengde": r["mileage_km"], "Drivstoff": "Elektrisk",
        "Hjuldrift": drive.get(str(r.get("drive", "")).upper(), "Ukjent"),
        "Girkasse": "Automat", "Forhandler": "Forhandler", "Fylke": "Vestland",
        "Info": r.get("variant_text", ""), "Pris": 1,
        "rekkevidde_km": r.get("range_km", 0),
        "batterikapasitet_kwh": r.get("battery_kwh"),
    } for r in rows])
    if frame.empty:
        return []
    # Modellklassifisering skal bruke samme variant som resten av BilRadar.
    frame["Produsent"] = frame["Merke"]
    frame["årstall"] = frame["Årstall"]
    variant, source = klassifiser_varianter(frame, last_variantkatalog())
    frame["variant_id"], frame["variant_kilde"] = variant, source
    scored = scorer_biler(frame)
    columns = ["_import_key", "forventet_pris", "hurtigpris", "peer_konfidens",
               "modell_nivaa", "variant_id", "variant_kilde", "overstyrt"]
    # JSON-kompatible verdier, inkludert pandas NA/NaN -> None.
    import json
    return json.loads(scored.reindex(columns=columns).to_json(orient="records"))


def evaluate_listings(raw_rows, settings: Settings, *, scorer=score_norwegian_prices, now=None):
    now = now or datetime.now(timezone.utc)
    latest, rejected = {}, []
    for i, raw in enumerate(raw_rows):
        try:
            row = normalize_listing(raw)
            key = row["source"] + ":" + str(row["listing_id"])
            if key not in latest or row["observed_at"] >= latest[key]["observed_at"]:
                latest[key] = row
        except (ValueError, KeyError, TypeError) as exc:
            rejected.append({"input_row": i + 1, "status": "ugyldig", "reason": str(exc)})
    rows = list(latest.values())
    scores = {r["_import_key"]: r for r in scorer(rows)} if rows else {}
    results = []
    for row in rows:
        key = row["source"] + ":" + str(row["listing_id"])
        score = scores.get(key, {})
        price = score.get(settings.price_basis)
        if price is not None:
            price = number(price, settings.price_basis, minimum=1)
        reasons = []
        if row.get("export_price_confirmed") is not True:
            reasons.append("Eksportpris må bekreftes; kalkylen bruker annonsert bruttopris")
        if row.get("other_costs_nok", settings.other_costs_nok) is None:
            reasons.append("Øvrige kostnader er ikke satt; foreløpig beregnet som 0")
        if row.get("variant_confirmed") is not True:
            reasons.append("Variant og utstyr mot norsk prisgrunnlag må kontrolleres")
        if row.get("model_year_estimated") is True:
            reasons.append("Modellår er anslått fra registreringsår og må bekreftes")
        if row.get("damage_free") is not True:
            reasons.append("Skadehistorikk er ukjent eller bilen er skadet")
        if str(row.get("drive", "")).upper() not in {"AWD", "4WD", "ALL_WHEEL", "FWD", "RWD", "2WD"}:
            reasons.append("Hjuldrift mangler eller er ukjent")
        if not score.get("variant_id") or score.get("variant_kilde") == "ukjent":
            reasons.append("Batterivariant mangler i modellgrunnlaget")
        if score.get("variant_kilde") in {"rekkevidde", "kwh+rekkevidde"}:
            reasons.append("Batterivariant er anslått fra rekkevidde")
        if score.get("modell_nivaa") not in {"LOOKUP", "L1"}:
            reasons.append("Norsk pris har bredt eller manglende sammenligningsgrunnlag")
        age_hours = (now - _timestamp(row["observed_at"])).total_seconds() / 3600
        if age_hours > 24 or age_hours < -1:
            reasons.append("Annonsetidspunkt er gammelt eller ligger i fremtiden")
        result = {"key": key, "url": row["url"], "make": row["make"],
                  "model": row["model"], "model_year": row["model_year"],
                  "mileage_km": row["mileage_km"], "observed_at": row["observed_at"],
                  "price_basis": settings.price_basis,
                  "valuation": score, "review_reasons": reasons}
        try:
            result["purchase_observation"] = purchase_observation(row, settings)
            result["calculation"] = calculate(row, settings, price)
            if price is None:
                result["status"] = "mangler_norsk_pris"
            elif reasons:
                result["status"] = "må_kontrolleres"
            elif result["calculation"]["surplus_after_target_nok"] >= 0:
                result["status"] = "kandidat"
            else:
                result["status"] = "for_dyr"
        except (ValueError, KeyError, TypeError) as exc:
            result["status"] = "mangler_kalkyledata"
            reasons.append(str(exc))
        results.append(result)
    results.sort(key=lambda r: (r["status"] != "kandidat",
                               -r.get("calculation", {}).get("surplus_after_target_nok", -1e20)))
    return {"results": results, "rejected": rejected, "input_count": len(raw_rows),
            "unique_count": len(rows), "generated_at": now.isoformat(),
            "registration_date": settings.registration_date.isoformat(),
            "live_collection": False}
