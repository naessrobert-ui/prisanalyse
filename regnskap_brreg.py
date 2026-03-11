"""
brreg_pdf_nedlasting.py
-----------------------
Last ned årsregnskap som PDF fra Brønnøysundregistrenes nettbutikk.

Krever:
    pip install playwright requests
    playwright install chromium

Bruk:
    python brreg_pdf_nedlasting.py 912078957
    python brreg_pdf_nedlasting.py "ECOCAR AS"
    python brreg_pdf_nedlasting.py 912078957 --år 2022 2021 2020
    python brreg_pdf_nedlasting.py 912078957 --alle

Bakgrunn:
    Brreg tilbyr ikke et åpent REST-API for PDF-nedlasting av historiske regnskap.
    PDFene hentes gratis via nettbutikken på w2.brreg.no/eHandelPortal/ecomsys/.
    Dette skriptet automatiserer den prosessen med Playwright (headless browser).
"""

import sys
import re
import time
import json
import argparse
from pathlib import Path

try:
    import requests
except ImportError:
    print("Mangler 'requests'. Kjør: pip install requests")
    sys.exit(1)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    print("Mangler 'playwright'. Kjør:\n  pip install playwright\n  playwright install chromium")
    sys.exit(1)

# ── Konstanter ───────────────────────────────────────────────────────────────
ENHET_URL    = "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}"
ENHET_SØK    = "https://data.brreg.no/enhetsregisteret/api/enheter"
REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}"
NETTBUTIKK   = "https://w2.brreg.no/eHandelPortal/ecomsys/"

# Søke-URL i nettbutikken for årsregnskap
BUTIKK_SØK   = (
    "https://w2.brreg.no/eHandelPortal/ecomsys/"
    "hentRegnskap?orgnr={orgnr}&regnskapsaar={år}&type=arsregnskap"
)

# ── Brreg-hjelper ─────────────────────────────────────────────────────────────

def rens_orgnr(s: str) -> str:
    return s.replace(" ", "").replace("-", "")


def hent_enhet_info(orgnr: str) -> dict | None:
    r = requests.get(ENHET_URL.format(orgnr=orgnr), timeout=15)
    return r.json() if r.status_code == 200 else None


def søk_navn(navn: str) -> list[dict]:
    r = requests.get(ENHET_SØK, params={"navn": navn, "size": 10}, timeout=15)
    if r.status_code != 200:
        return []
    return r.json().get("_embedded", {}).get("enheter", [])


def løs_orgnr(tekst: str) -> tuple[str, str] | None:
    renset = rens_orgnr(tekst)
    if renset.isdigit() and len(renset) == 9:
        enhet = hent_enhet_info(renset)
        if enhet:
            return renset, enhet.get("navn", "Ukjent")
        print(f"  ⚠  Fant ingen enhet med orgnr {renset}")
        return None
    treff = søk_navn(tekst)
    if not treff:
        print("  Ingen treff på navn.")
        return None
    if len(treff) == 1:
        e = treff[0]
        return rens_orgnr(e["organisasjonsnummer"]), e["navn"]
    print(f"\n  {len(treff)} treff:")
    for i, e in enumerate(treff, 1):
        sted = e.get("forretningsadresse", {}).get("poststed", "")
        print(f"  [{i}] {e['navn']:<45} {e['organisasjonsnummer']}  {sted}")
    valg = input("\n  Velg nummer: ").strip()
    if not valg.isdigit() or not (1 <= int(valg) <= len(treff)):
        return None
    e = treff[int(valg) - 1]
    return rens_orgnr(e["organisasjonsnummer"]), e["navn"]


def hent_tilgjengelige_år(orgnr: str) -> list[int]:
    """Henter liste med år det finnes regnskap for (via JSON-APIet)."""
    r = requests.get(REGNSKAP_URL.format(orgnr=orgnr), timeout=20)
    if r.status_code != 200:
        return []
    data = r.json()
    if not isinstance(data, list):
        data = [data]
    år_liste = []
    for item in data:
        dato = item.get("regnskapsperiode", {}).get("fraDato", "")
        if dato:
            år_liste.append(int(dato[:4]))
    return sorted(set(år_liste), reverse=True)


# ── Playwright PDF-nedlaster ─────────────────────────────────────────────────

def last_ned_pdf_for_år(
    page,
    orgnr: str,
    år: int,
    mappe: Path,
    firmnanavn: str,
) -> Path | None:
    """
    Navigerer til brreg-nettbutikken og laster ned årsregnskapet for gitt år.
    Returnerer filsti ved suksess, None ved feil.
    """
    url = BUTIKK_SØK.format(orgnr=orgnr, år=år)
    filnavn = mappe / f"{orgnr}_{år}_årsregnskap.pdf"

    if filnavn.exists():
        print(f"  ✓  {år}: Allerede lastet ned → {filnavn.name}")
        return filnavn

    print(f"  ⬇  {år}: Henter {url}")

    try:
        # Sett opp nedlastingsmappe for denne nettleserfanen
        with page.expect_download(timeout=60_000) as dl_info:
            page.goto(url, timeout=30_000)
            # Siden kan inneholde en direkte PDF-lenke eller en knapp
            # Prøv å finne PDF-lenke
            pdf_link = page.locator("a[href$='.pdf'], a:has-text('Last ned'), a:has-text('PDF')")
            if pdf_link.count() > 0:
                pdf_link.first.click()
            else:
                # Forsøk direkte hvis siden redirecter til PDF
                pass

        download = dl_info.value
        download.save_as(filnavn)
        print(f"  ✅  {år}: Lagret → {filnavn.name}")
        return filnavn

    except PWTimeout:
        print(f"  ⏱  {år}: Tidsavbrudd – prøver direkte nedlasting")
    except Exception as e:
        print(f"  ⚠  {år}: Feil ved Playwright-nedlasting: {e}")

    # Fallback: prøv direkte requests-nedlasting (fungerer hvis URL er åpen)
    return direkte_nedlasting(url, filnavn, år)


def direkte_nedlasting(url: str, filnavn: Path, år: int) -> Path | None:
    """
    Forsøk direkte HTTP-nedlasting (fungerer for noen regnskap).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; regnskapsbot/1.0)",
        "Accept": "application/pdf,*/*",
    }
    try:
        r = requests.get(url, headers=headers, timeout=30, stream=True)
        content_type = r.headers.get("Content-Type", "")
        if r.status_code == 200 and "pdf" in content_type.lower():
            with open(filnavn, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"  ✅  {år}: Lagret via direkte nedlasting → {filnavn.name}")
            return filnavn
        elif r.status_code == 200 and "html" in content_type.lower():
            # Kan inneholde en embed/iframe med PDF-URL
            tekst = r.text
            pdf_match = re.search(r'(https?://[^\s"\'<>]+\.pdf)', tekst)
            if pdf_match:
                pdf_url = pdf_match.group(1)
                return direkte_nedlasting(pdf_url, filnavn, år)
            print(f"  ℹ  {år}: Fikk HTML – ingen direkte PDF-lenke funnet")
        else:
            print(f"  ✗  {år}: Status {r.status_code}, Content-Type: {content_type}")
    except Exception as e:
        print(f"  ✗  {år}: Direkte nedlasting feilet: {e}")
    return None


def last_ned_alle(
    orgnr: str,
    firmanavn: str,
    år_liste: list[int],
    headless: bool = True,
) -> None:
    mappe = Path("regnskap") / orgnr
    mappe.mkdir(parents=True, exist_ok=True)

    print(f"\n  Laster ned {len(år_liste)} regnskap for {firmanavn} ({orgnr})")
    print(f"  Mappe: {mappe.resolve()}\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Gå til nettbutikken en gang for å sette cookies
        try:
            page.goto(NETTBUTIKK, timeout=20_000)
            time.sleep(1)
        except Exception:
            pass

        resultater = {}
        for år in år_liste:
            sti = last_ned_pdf_for_år(page, orgnr, år, mappe, firmanavn)
            resultater[år] = sti
            time.sleep(1.5)  # Vær snill mot serveren

        browser.close()

    # Oppsummering
    print(f"\n{'─'*50}")
    ok  = [å for å, s in resultater.items() if s]
    feil = [å for å, s in resultater.items() if not s]
    print(f"  Lastet ned: {len(ok)}/{len(år_liste)} filer")
    if feil:
        print(f"  Mislyktes:  {feil}")
        print(f"\n  For mislyktes år, prøv manuelt:")
        for å in feil:
            print(f"    {NETTBUTIKK} → søk på orgnr {orgnr}, år {å}")
    print(f"{'─'*50}\n")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Last ned årsregnskap (PDF) fra Brønnøysundregistrene"
    )
    parser.add_argument(
        "søk",
        nargs="?",
        help="Organisasjonsnummer eller firmanavn",
    )
    parser.add_argument(
        "--år",
        nargs="+",
        type=int,
        metavar="ÅRSTALL",
        help="Spesifikke år å laste ned (f.eks. --år 2023 2022 2021)",
    )
    parser.add_argument(
        "--alle",
        action="store_true",
        help="Last ned alle tilgjengelige år (iflg. JSON-API)",
    )
    parser.add_argument(
        "--vis-nettleser",
        action="store_true",
        help="Vis nettleservindu under nedlasting (ikke headless)",
    )
    args = parser.parse_args()

    søk_tekst = args.søk
    if not søk_tekst:
        print("=" * 55)
        print("  Brreg PDF-nedlaster")
        print("=" * 55)
        søk_tekst = input("\n  Orgnummer eller firmanavn: ").strip()
    if not søk_tekst:
        parser.print_help()
        sys.exit(1)

    resultat = løs_orgnr(søk_tekst)
    if not resultat:
        sys.exit(1)
    orgnr, firmanavn = resultat
    print(f"\n  Virksomhet: {firmanavn} ({orgnr})")

    # Bestem hvilke år
    if args.år:
        år_liste = sorted(args.år, reverse=True)
    elif args.alle:
        print("  Henter tilgjengelige år fra JSON-API ...")
        år_liste = hent_tilgjengelige_år(orgnr)
        if not år_liste:
            # Fallback: siste 5 år
            from datetime import date
            inneværende = date.today().year - 1
            år_liste = list(range(inneværende, inneværende - 5, -1))
            print(f"  (API returnerte ingen data – prøver {år_liste})")
        else:
            print(f"  Fant år: {år_liste}")
    else:
        # Interaktiv modus
        print("  Henter tilgjengelige år fra JSON-API ...")
        kjente_år = hent_tilgjengelige_år(orgnr)
        if kjente_år:
            print(f"  Tilgjengelige år (iflg. API): {kjente_år}")
        valg = input(
            "\n  Hvilke år vil du laste ned?\n"
            "  (Enter = alle kjente, eller oppgi år separert med mellomrom): "
        ).strip()
        if not valg:
            år_liste = kjente_år if kjente_år else []
        else:
            år_liste = sorted(
                [int(x) for x in valg.split() if x.isdigit()], reverse=True
            )

    if not år_liste:
        print("  Ingen år valgt. Avslutter.")
        sys.exit(0)

    last_ned_alle(
        orgnr,
        firmanavn,
        år_liste,
        headless=not args.vis_nettleser,
    )


if __name__ == "__main__":
    main()