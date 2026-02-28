# -*- coding: utf-8 -*-
"""
Hybrid regnskap-henter: Brreg + Proff med meny
Kjør: python regnskap_hybrid_meny.py

Velg:
1 = Brreg (kun orgnr)
2 = Proff (krever full URL til regnskap-siden)
"""

import sys
import re
import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional

# ----------------------------------------------------------------------------
# Felles konfigurasjon
# ----------------------------------------------------------------------------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "nb-NO,nb;q=0.9",
}

TIMEOUT = 15


def normalize_orgnr(org: str) -> str:
    return re.sub(r"\D", "", str(org or "").strip())


def print_data(data: Dict[str, Dict], source: str):
    """Skriver ut data pent"""
    if not data:
        print(f"Ingen data funnet fra {source}")
        return

    print(f"\n{'='*80}")
    print(f"Resultat fra {source}")
    print("="*80)

    for periode, verdier in sorted(data.items(), reverse=True):
        print(f"\nPeriode: {periode}")
        for nøkkel, verdi in verdier.items():
            if nøkkel == "periode" or nøkkel == "source" or nøkkel == "url":
                continue
            if isinstance(verdi, (int, float)):
                print(f"  {nøkkel:40} {verdi:>18,.0f} kr")
            else:
                print(f"  {nøkkel:40} {verdi}")
        print("-" * 80)


# ----------------------------------------------------------------------------
# Brreg – enkelt, kun siste år
# ----------------------------------------------------------------------------
def hent_brreg(orgnr: str) -> Dict:
    url = f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"Brreg feilet med status {r.status_code}")
            return {}

        js = r.json()
        if isinstance(js, list) and js:
            js = js[0]

        data = {}
        periode = js.get("regnskapsperiode", {})
        periode_str = f"{periode.get('fraDato')} – {periode.get('tilDato')}"

        def get_val(path, obj=js):
            cur = obj
            for k in path:
                cur = cur.get(k, {})
            return cur.get("beloep") if isinstance(cur, dict) else cur if isinstance(cur, (int, float)) else None

        data[periode_str] = {
            "periode": periode_str,
            "source": "Brreg",
            "url": url,
            "omsetning": get_val(["resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter"]),
            "driftsresultat": get_val(["resultatregnskapResultat", "driftsresultat", "driftsresultat"]),
            "årsresultat": get_val(["resultatregnskapResultat", "aarsresultat"]),
            "egenkapital": get_val(["egenkapitalGjeld", "egenkapital", "sumEgenkapital"]),
            "sum_eiendeler": get_val(["eiendeler", "sumEiendeler"]),
        }

        return data
    except Exception as e:
        print(f"Brreg-feil: {e}")
        return {}


# ----------------------------------------------------------------------------
# Proff – flere år, krever URL
# ----------------------------------------------------------------------------
def hent_proff(regnskap_url: str) -> Dict:
    try:
        r = requests.get(regnskap_url, headers=HEADERS, timeout=TIMEOUT)
        print(f"Proff status: {r.status_code}")
        if r.status_code != 200:
            return {}

        soup = BeautifulSoup(r.text, "html.parser")
        data = {}

        # Finn perioder fra første tabell-rad
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 5:
                continue

            header_cells = rows[0].find_all(["th", "td"])
            periods = [c.get_text(strip=True) for c in header_cells[1:] if re.match(r"^\d{4}-\d{2}$", c.get_text(strip=True))]

            if not periods:
                continue

            for row in rows[2:]:
                cells = row.find_all(["th", "td"])
                if len(cells) < len(periods) + 1:
                    continue

                label = cells[0].get_text(strip=True).strip()
                if not label or "Valuta" in label or "Startdato" in label or "Sluttdato" in label:
                    continue

                key = re.sub(r"[^a-z0-9_æøå]", "_", label.lower())

                for i, per in enumerate(periods):
                    val_str = cells[i+1].get_text(strip=True).replace("\xa0","").replace(" ","").replace(",",".")
                    try:
                        val = float(val_str) * 1000
                    except ValueError:
                        val = None

                    if per not in data:
                        data[per] = {"periode": per, "source": "Proff", "url": regnskap_url}

                    data[per][key] = val

        return dict(sorted(data.items(), reverse=True))
    except Exception as e:
        print(f"Proff-feil: {e}")
        return {}


# ----------------------------------------------------------------------------
# Meny og hovedlogikk
# ----------------------------------------------------------------------------
def main():
    print("\n" + "="*60)
    print("Regnskap-henter – Brreg eller Proff")
    print("="*60)
    print("1 = Brreg (kun orgnr kreves, gir siste år)")
    print("2 = Proff (krever full URL til regnskap-siden, gir flere år)")
    print("q = Avslutt")
    print("-"*60)

    valg = input("Velg (1 eller 2): ").strip().lower()

    if valg == "q":
        print("Avslutter.")
        sys.exit(0)

    if valg not in ["1", "2"]:
        print("Ugyldig valg. Prøv igjen.")
        main()
        return

    orgnr = None
    proff_url = None

    if valg == "1":
        orgnr = input("Skriv inn organisasjonsnummer (9 siffer): ").strip()
        orgnr = normalize_orgnr(orgnr)
        if len(orgnr) != 9:
            print("Ugyldig orgnr – må være 9 siffer.")
            return
        data = hent_brreg(orgnr)
        print_data(data, "Brreg")

    elif valg == "2":
        print("\nLim inn FULL URL til Proff regnskap-siden")
        print("Eksempel: https://www.proff.no/regnskap/ecocar-as/søreidgrend/biler-og-kj%C3%B8ret%C3%B8y/IF310ML0CUX")
        proff_url = input("URL: ").strip()
        if not proff_url.startswith("https://www.proff.no/regnskap/"):
            print("Ser ikke ut som en gyldig Proff regnskap-URL. Prøv igjen.")
            return
        data = hent_proff(proff_url)
        print_data(data, "Proff")

    # Spør om ny runde?
    igjen = input("\nHente mer? (j/n): ").strip().lower()
    if igjen in ["j", "ja", "y"]:
        main()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAvsluttet med Ctrl+C")