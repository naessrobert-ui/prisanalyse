#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hent flypriser for Norwegians kjernenett + Bergen, og lagre historikk.

Investor-vinkling: Norwegian (DY) er børsnotert, og prisdata på tvers av
selskapets viktigste ruter fungerer som en *ledende proxy* for prispress og
etterspørsel. Vi henter billigste priser per rute × avreisemåned og lagrer
ALLE tilbud (opptil 30 per rute/måned) slik at vi kan skille ut Norwegians
egne priser fra konkurrentene i analysen.

Kilde: Travelpayouts / Aviasales pris-API (cachede laveste priser på tvers av
flyselskap). Norwegian sitt eget API ligger bak bot-beskyttelse (403).

Bruk:
    export TRAVELPAYOUTS_TOKEN="din_token"
    python -m scripts.flypriser_bergen               # hele nettet, 6 mnd
    python -m scripts.flypriser_bergen --gruppe bergen --months 12
    python -m scripts.flypriser_bergen --months 4 --pause 0.3

Skriver:
    data/flypriser_historikk.csv   (full historikk – append, én rad per tilbud)
    data/flypriser_beste.csv       (billigste pr. rute+måned ved siste kjøring)

Kjør daglig (GitHub Actions) for å bygge opp tidsserien som analysen bruker.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
import time
from pathlib import Path

import requests

API_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
CURRENCY = "nok"
USER_AGENT = "PrisanalyseFlypriser/2.0 (personlig analytisk bruk)"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORIKK_CSV = DATA_DIR / "flypriser_historikk.csv"
BESTE_CSV = DATA_DIR / "flypriser_beste.csv"

# --- Flyplasser (IATA -> visningsnavn) -------------------------------------
FLYPLASSER: dict[str, str] = {
    # Norge
    "OSL": "Oslo", "BGO": "Bergen", "TRD": "Trondheim", "SVG": "Stavanger",
    "TOS": "Tromsø", "BOO": "Bodø", "AES": "Ålesund", "KRS": "Kristiansand",
    "MOL": "Molde", "EVE": "Harstad/Narvik", "KKN": "Kirkenes", "BDU": "Bardufoss",
    # Norden/Europa – Norwegians viktigste internasjonale ruter
    "CPH": "København", "ARN": "Stockholm", "HEL": "Helsinki",
    "LGW": "London Gatwick", "AMS": "Amsterdam", "BER": "Berlin",
    "CDG": "Paris CDG", "NCE": "Nice", "BCN": "Barcelona", "MAD": "Madrid",
    "ALC": "Alicante", "AGP": "Málaga", "PMI": "Palma", "LPA": "Gran Canaria",
    "TFS": "Tenerife", "FCO": "Roma", "PRG": "Praha", "KRK": "Kraków",
    "GDN": "Gdańsk", "SPU": "Split", "MUC": "München", "DUS": "Düsseldorf",
}

# --- Ruter (undirected par, gruppe) – hentes i BEGGE retninger -------------
# gruppe: "bergen" = inn/ut av Bergen, "innenriks" = norsk stamnett,
#         "norwegian_intl" = Norwegians viktige internasjonale ruter (fra OSL)
_RUTER_RAW: list[tuple[str, str, str]] = [
    # Inn/ut av Bergen
    ("BGO", "OSL", "bergen"), ("BGO", "TRD", "bergen"), ("BGO", "SVG", "bergen"),
    ("BGO", "TOS", "bergen"), ("BGO", "CPH", "bergen"), ("BGO", "LGW", "bergen"),
    ("BGO", "AMS", "bergen"), ("BGO", "ALC", "bergen"), ("BGO", "AGP", "bergen"),
    ("BGO", "LPA", "bergen"), ("BGO", "NCE", "bergen"), ("BGO", "BCN", "bergen"),
    ("BGO", "BER", "bergen"), ("BGO", "GDN", "bergen"), ("BGO", "KRK", "bergen"),
    ("BGO", "PRG", "bergen"),
    # Norsk stamnett (Norwegian stor innenriks-aktør)
    ("OSL", "TRD", "innenriks"), ("OSL", "SVG", "innenriks"), ("OSL", "TOS", "innenriks"),
    ("OSL", "BOO", "innenriks"), ("OSL", "AES", "innenriks"), ("OSL", "KRS", "innenriks"),
    ("OSL", "MOL", "innenriks"), ("OSL", "EVE", "innenriks"), ("OSL", "KKN", "innenriks"),
    ("TRD", "BOO", "innenriks"), ("TRD", "TOS", "innenriks"),
    # Norwegians viktige internasjonale ruter fra Oslo
    ("OSL", "CPH", "norwegian_intl"), ("OSL", "ARN", "norwegian_intl"),
    ("OSL", "LGW", "norwegian_intl"), ("OSL", "AMS", "norwegian_intl"),
    ("OSL", "BER", "norwegian_intl"), ("OSL", "CDG", "norwegian_intl"),
    ("OSL", "ALC", "norwegian_intl"), ("OSL", "AGP", "norwegian_intl"),
    ("OSL", "LPA", "norwegian_intl"), ("OSL", "TFS", "norwegian_intl"),
    ("OSL", "PMI", "norwegian_intl"), ("OSL", "BCN", "norwegian_intl"),
    ("OSL", "MAD", "norwegian_intl"), ("OSL", "NCE", "norwegian_intl"),
    ("OSL", "FCO", "norwegian_intl"), ("OSL", "PRG", "norwegian_intl"),
    ("OSL", "KRK", "norwegian_intl"), ("OSL", "GDN", "norwegian_intl"),
    ("OSL", "MUC", "norwegian_intl"),
]

# --- Flyselskap (IATA -> navn). Norwegian = DY (D8 er gammel kode) ----------
SELSKAP: dict[str, str] = {
    "DY": "Norwegian", "D8": "Norwegian", "SK": "SAS", "WF": "Widerøe",
    "W6": "Wizz Air", "W9": "Wizz Air UK", "FR": "Ryanair", "U2": "easyJet",
    "KL": "KLM", "LH": "Lufthansa", "AY": "Finnair", "EW": "Eurowings",
    "BA": "British Airways", "AF": "Air France", "LX": "SWISS", "TP": "TAP",
    "VY": "Vueling", "FI": "Icelandair", "EN": "Air Dolomiti",
}
NORWEGIAN_KODER = {"DY", "D8"}

CSV_FELT = [
    "hentet_dato", "origin", "origin_navn", "destinasjon", "destinasjon_navn",
    "gruppe", "avreise_maaned", "pris", "valuta", "avreise", "retur",
    "selskap", "selskap_navn", "flynummer", "mellomlandinger",
    "varighet_min", "direkte", "lenke",
]


def rettede_ruter(grupper: list[str] | None = None) -> list[tuple[str, str, str]]:
    """Ekspander undirected par til begge retninger, evt. filtrert på gruppe."""
    ut: list[tuple[str, str, str]] = []
    for a, b, gruppe in _RUTER_RAW:
        if grupper and gruppe not in grupper:
            continue
        ut.append((a, b, gruppe))
        ut.append((b, a, gruppe))
    return ut


def maaneder_fremover(antall: int, start: dt.date | None = None) -> list[str]:
    """Returner ['YYYY-MM', ...] for `antall` måneder fra og med denne måneden."""
    start = start or dt.date.today()
    ut: list[str] = []
    aar, mnd = start.year, start.month
    for _ in range(antall):
        ut.append(f"{aar:04d}-{mnd:02d}")
        mnd += 1
        if mnd > 12:
            mnd = 1
            aar += 1
    return ut


def hent_maaned(
    token: str, origin: str, dest: str, maaned: str,
    *, valuta: str = CURRENCY, forsok: int = 4,
) -> list[dict]:
    """Hent billigste billetter origin->dest for en gitt avreisemåned (YYYY-MM)."""
    params = {
        "origin": origin, "destination": dest, "departure_at": maaned,
        "currency": valuta, "one_way": "false", "unique": "false",
        "sorting": "price", "limit": 30, "page": 1, "token": token,
    }
    ventetid = 2.0
    for forsok_nr in range(1, forsok + 1):
        try:
            r = requests.get(
                API_URL, params=params,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=30,
            )
        except requests.RequestException as exc:
            if forsok_nr == forsok:
                print(f"  ! nettverksfeil {origin}->{dest} {maaned}: {exc}", file=sys.stderr)
                return []
            time.sleep(ventetid)
            ventetid *= 2
            continue

        if r.status_code == 401:
            raise SystemExit(
                "401 Unauthorized – ugyldig/manglende TRAVELPAYOUTS_TOKEN. "
                "Hent en gratis token på https://www.travelpayouts.com"
            )
        if r.status_code == 429:
            time.sleep(ventetid)
            ventetid *= 2
            continue
        if r.status_code != 200:
            if forsok_nr == forsok:
                print(f"  ! HTTP {r.status_code} for {origin}->{dest} {maaned}", file=sys.stderr)
                return []
            time.sleep(ventetid)
            ventetid *= 2
            continue

        payload = r.json()
        if not payload.get("success", False):
            print(f"  ! API-feil {origin}->{dest} {maaned}: {payload.get('error')}", file=sys.stderr)
            return []
        return payload.get("data", []) or []
    return []


def normaliser(rad: dict, origin: str, dest: str, gruppe: str, maaned: str, hentet: str) -> dict:
    kode = rad.get("airline")
    return {
        "hentet_dato": hentet,
        "origin": origin,
        "origin_navn": FLYPLASSER.get(origin, origin),
        "destinasjon": dest,
        "destinasjon_navn": FLYPLASSER.get(dest, dest),
        "gruppe": gruppe,
        "avreise_maaned": maaned,
        "pris": rad.get("price"),
        "valuta": rad.get("currency", CURRENCY),
        "avreise": rad.get("departure_at"),
        "retur": rad.get("return_at"),
        "selskap": kode,
        "selskap_navn": SELSKAP.get(kode, kode),
        "flynummer": rad.get("flight_number"),
        "mellomlandinger": rad.get("transfers"),
        "varighet_min": rad.get("duration"),
        "direkte": (rad.get("transfers") == 0),
        "lenke": ("https://www.aviasales.com" + rad["link"]) if rad.get("link") else None,
    }


def skriv_csv(sti: Path, rader: list[dict], *, append: bool) -> None:
    sti.parent.mkdir(parents=True, exist_ok=True)
    ny_fil = not sti.exists()
    modus = "a" if append else "w"
    with sti.open(modus, newline="", encoding="utf-8") as f:
        skriver = csv.DictWriter(f, fieldnames=CSV_FELT)
        if ny_fil or not append:
            skriver.writeheader()
        for rad in rader:
            skriver.writerow(rad)


def beste_per_rute(rader: list[dict]) -> list[dict]:
    """Billigste rad pr. (origin, destinasjon, avreisemåned)."""
    beste: dict[tuple[str, str, str], dict] = {}
    for rad in rader:
        if rad["pris"] is None:
            continue
        nokkel = (rad["origin"], rad["destinasjon"], rad["avreise_maaned"])
        if nokkel not in beste or rad["pris"] < beste[nokkel]["pris"]:
            beste[nokkel] = rad
    return sorted(beste.values(), key=lambda r: (r["origin_navn"], r["destinasjon_navn"], r["avreise_maaned"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hent flypriser for Norwegians nett + Bergen.")
    parser.add_argument(
        "--gruppe", nargs="+", choices=["bergen", "innenriks", "norwegian_intl"],
        help="Begrens til rutegrupper (standard: alle).",
    )
    parser.add_argument("--months", type=int, default=9, help="Antall måneder fremover (standard 9).")
    parser.add_argument("--currency", default=CURRENCY, help="Valuta (standard nok).")
    parser.add_argument("--pause", type=float, default=0.35, help="Sekunders pause mellom kall.")
    args = parser.parse_args(argv)

    token = os.getenv("TRAVELPAYOUTS_TOKEN", "").strip()
    if not token:
        print(
            "Mangler TRAVELPAYOUTS_TOKEN.\n"
            "  1) Lag gratis konto på https://www.travelpayouts.com\n"
            "  2) Kopier tokenet under «Developers / API access»\n"
            "  3) export TRAVELPAYOUTS_TOKEN=\"din_token\"\n",
            file=sys.stderr,
        )
        return 2

    ruter = rettede_ruter(args.gruppe)
    maaneder = maaneder_fremover(args.months)
    hentet = dt.date.today().isoformat()

    alle: list[dict] = []
    print(f"Henter {len(ruter)} rutretninger × {len(maaneder)} måneder …")
    for origin, dest, gruppe in ruter:
        funnet = 0
        for maaned in maaneder:
            rader = hent_maaned(token, origin, dest, maaned, valuta=args.currency)
            for rad in rader:
                alle.append(normaliser(rad, origin, dest, gruppe, maaned, hentet))
            funnet += len(rader)
            time.sleep(args.pause)
        merke = " (DY)" if any(
            r["selskap"] in NORWEGIAN_KODER for r in alle[-funnet:] if funnet
        ) else ""
        print(f"  {origin}->{dest:<4} {FLYPLASSER.get(dest, dest):<16} {funnet:>3} tilbud{merke}")

    if not alle:
        print("Ingen priser hentet.", file=sys.stderr)
        return 1

    skriv_csv(HISTORIKK_CSV, alle, append=True)
    beste = beste_per_rute(alle)
    skriv_csv(BESTE_CSV, beste, append=False)

    dy = [r for r in alle if r["selskap"] in NORWEGIAN_KODER]
    print(f"\nSkrev {len(alle)} rader ({len(dy)} med Norwegian) til {HISTORIKK_CSV.name}")
    print(f"Skrev {len(beste)} beste-rader til {BESTE_CSV.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
