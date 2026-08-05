#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hent lavpriser på flybilletter ut fra Bergen (BGO) og lagre historikk.

Norwegian sitt eget lavpriskalender-API ligger bak en bot-vegg (Akamai
«Are you human?»), og gir 403 ved direkte skraping. I stedet bruker dette
skriptet Travelpayouts / Aviasales sitt gratis pris-API, som returnerer de
billigste billettene per avreisemåned og destinasjon – altså akkurat en
«lavpriskalender», men på tvers av alle flyselskap (inkl. Norwegian).

Bruk:
    # Trenger en gratis token fra https://www.travelpayouts.com (Data access)
    export TRAVELPAYOUTS_TOKEN="din_token"

    # Hent alle standarddestinasjoner for de neste 6 månedene:
    python -m scripts.flypriser_bergen

    # Bare noen destinasjoner, direktefly, 12 måneder frem:
    python -m scripts.flypriser_bergen --dest NCE ALC LGW --months 12 --direct

Resultatet skrives til:
    data/flypriser_bergen.csv          (full historikk – én rad per henting)
    data/flypriser_bergen_beste.csv    (billigste pr. destinasjon+måned nå)

Ved å kjøre skriptet regelmessig (f.eks. daglig via cron) bygger du opp en
prishistorikk du kan bruke til å se når det er billigst å reise.
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
ORIGIN = "BGO"  # Bergen Flesland
CURRENCY = "nok"
USER_AGENT = "PrisanalyseFlypriser/1.0 (personlig analytisk bruk)"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORIKK_CSV = DATA_DIR / "flypriser_bergen.csv"
BESTE_CSV = DATA_DIR / "flypriser_bergen_beste.csv"

# Populære direkte- og sesongdestinasjoner fra Bergen (IATA -> visningsnavn).
# Juster fritt – Travelpayouts dekker langt flere ruter enn Norwegian alene.
DESTINASJONER: dict[str, str] = {
    "NCE": "Nice",
    "ALC": "Alicante",
    "AGP": "Málaga",
    "BCN": "Barcelona",
    "PMI": "Palma de Mallorca",
    "LGW": "London Gatwick",
    "CPH": "København",
    "AMS": "Amsterdam",
    "BER": "Berlin",
    "GDN": "Gdańsk",
    "KRK": "Kraków",
    "PRG": "Praha",
    "SPU": "Split",
    "OSL": "Oslo",
    "TOS": "Tromsø",
    "KKN": "Kirkenes",
}

CSV_FELT = [
    "hentet_dato",
    "origin",
    "destinasjon_kode",
    "destinasjon_navn",
    "avreise_maaned",
    "pris",
    "valuta",
    "avreise",
    "retur",
    "flyselskap",
    "flynummer",
    "mellomlandinger",
    "retur_mellomlandinger",
    "varighet_min",
    "direkte",
    "lenke",
]


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
    token: str,
    dest: str,
    maaned: str,
    *,
    direkte: bool,
    valuta: str = CURRENCY,
    forsok: int = 4,
) -> list[dict]:
    """Hent billigste billetter BGO->dest for en gitt avreisemåned (YYYY-MM)."""
    params = {
        "origin": ORIGIN,
        "destination": dest,
        "departure_at": maaned,
        "currency": valuta,
        "one_way": "false",
        "unique": "false",
        "sorting": "price",
        "limit": 30,
        "page": 1,
        "token": token,
    }
    if direkte:
        params["direct"] = "true"

    ventetid = 2.0
    for forsok_nr in range(1, forsok + 1):
        try:
            r = requests.get(
                API_URL,
                params=params,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=30,
            )
        except requests.RequestException as exc:
            if forsok_nr == forsok:
                print(f"  ! nettverksfeil {dest} {maaned}: {exc}", file=sys.stderr)
                return []
            time.sleep(ventetid)
            ventetid *= 2
            continue

        if r.status_code == 401:
            raise SystemExit(
                "401 Unauthorized – ugyldig eller manglende TRAVELPAYOUTS_TOKEN. "
                "Hent en gratis token på https://www.travelpayouts.com"
            )
        if r.status_code == 429:  # rate limit
            time.sleep(ventetid)
            ventetid *= 2
            continue
        if r.status_code != 200:
            if forsok_nr == forsok:
                print(f"  ! HTTP {r.status_code} for {dest} {maaned}", file=sys.stderr)
                return []
            time.sleep(ventetid)
            ventetid *= 2
            continue

        payload = r.json()
        if not payload.get("success", False):
            print(f"  ! API-feil {dest} {maaned}: {payload.get('error')}", file=sys.stderr)
            return []
        return payload.get("data", []) or []

    return []


def normaliser(rad: dict, dest: str, maaned: str, hentet: str) -> dict:
    """Gjør en API-rad om til en flat CSV-rad."""
    return {
        "hentet_dato": hentet,
        "origin": ORIGIN,
        "destinasjon_kode": dest,
        "destinasjon_navn": DESTINASJONER.get(dest, dest),
        "avreise_maaned": maaned,
        "pris": rad.get("price"),
        "valuta": rad.get("currency", CURRENCY),
        "avreise": rad.get("departure_at"),
        "retur": rad.get("return_at"),
        "flyselskap": rad.get("airline"),
        "flynummer": rad.get("flight_number"),
        "mellomlandinger": rad.get("transfers"),
        "retur_mellomlandinger": rad.get("return_transfers"),
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


def beste_per_maaned(rader: list[dict]) -> list[dict]:
    """Billigste rad pr. (destinasjon, avreisemåned)."""
    beste: dict[tuple[str, str], dict] = {}
    for rad in rader:
        if rad["pris"] is None:
            continue
        nokkel = (rad["destinasjon_kode"], rad["avreise_maaned"])
        if nokkel not in beste or rad["pris"] < beste[nokkel]["pris"]:
            beste[nokkel] = rad
    return sorted(beste.values(), key=lambda r: (r["destinasjon_navn"], r["avreise_maaned"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hent flypriser ut fra Bergen (BGO).")
    parser.add_argument(
        "--dest", nargs="+", metavar="IATA",
        help="Destinasjonskoder (standard: hele listen i DESTINASJONER).",
    )
    parser.add_argument("--months", type=int, default=6, help="Antall måneder fremover (standard 6).")
    parser.add_argument("--direct", action="store_true", help="Kun direktefly.")
    parser.add_argument("--currency", default=CURRENCY, help="Valuta (standard nok).")
    parser.add_argument("--pause", type=float, default=0.4, help="Sekunders pause mellom kall.")
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

    destinasjoner = args.dest or list(DESTINASJONER.keys())
    maaneder = maaneder_fremover(args.months)
    hentet = dt.date.today().isoformat()

    alle: list[dict] = []
    print(f"Henter {len(destinasjoner)} destinasjoner × {len(maaneder)} måneder fra {ORIGIN} …")
    for dest in destinasjoner:
        navn = DESTINASJONER.get(dest, dest)
        funnet = 0
        for maaned in maaneder:
            rader = hent_maaned(token, dest, maaned, direkte=args.direct, valuta=args.currency)
            for rad in rader:
                alle.append(normaliser(rad, dest, maaned, hentet))
            funnet += len(rader)
            time.sleep(args.pause)
        print(f"  {dest} {navn:<20} {funnet:>3} tilbud")

    if not alle:
        print("Ingen priser hentet.", file=sys.stderr)
        return 1

    skriv_csv(HISTORIKK_CSV, alle, append=True)
    beste = beste_per_maaned(alle)
    skriv_csv(BESTE_CSV, beste, append=False)

    print(f"\nSkrev {len(alle)} rader til {HISTORIKK_CSV}")
    print(f"Skrev {len(beste)} beste-rader til {BESTE_CSV}\n")

    print("Billigste pr. destinasjon (alle måneder):")
    billigst: dict[str, dict] = {}
    for rad in beste:
        d = rad["destinasjon_kode"]
        if d not in billigst or rad["pris"] < billigst[d]["pris"]:
            billigst[d] = rad
    for rad in sorted(billigst.values(), key=lambda r: r["pris"]):
        mnd = rad["avreise_maaned"]
        print(f"  {rad['destinasjon_navn']:<20} {rad['pris']:>7} {rad['valuta']}  ({mnd})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
