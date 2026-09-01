"""
scripts/kupp_vakt.py – Kupp-vakt: varsler om ferske, underprisede biler
========================================================================
Kjøres ofte (f.eks. hvert 10. minutt). Scraper de NYESTE annonsene på FINN,
scorer dem med den samlede motoren (lookup/variant primær + peer-WLS fallback,
uten tung ML-modell) og sender e-postvarsel om biler som er billige mot
modellen.

«Kun helt nye biler»: en liten state-fil i S3 (calc/bil/kupp_vakt_state.json)
husker hvilke FinnKoder som allerede er varslet. Første kjøring seeder (ingen
varsler) slik at du ikke får en flom av gamle annonser; deretter varsles bare
nye annonser – én gang hver.

Hver varslede bil logges også til S3 (calc/bil/kupp_vakt_logg.json) med
egenskapene sine, slik at scripts/kupp_analyse.py senere kan se hva som
kjennetegner biler som faktisk blir solgt – grunnlag for å tune tersklene.

Terskel – trappetrinn etter pris (env, kan overstyres):
    KUPP_RABATT_TRAPP  – "maxpris:minprosent,..." (default
                         "50000:30,100000:20,150000:15,250000:7,:6"):
                         < 50k krever 30 %, < 100k 20 %, < 150k 15 %,
                         < 250k 7 %, ellers 6 %. Tom = bruk flat KUPP_RABATT_MIN.
    KUPP_RABATT_KR_MIN – valgfri flat kroneterskel i tillegg (0 = av, default).
    KUPP_UNDER_HURTIG  – "1": varsle også hvis pris < hurtigpris (default "0").

Filtre (env, valgfrie):
    KUPP_SELGER        – "privat" (default), "merkeforhandler", "annet" el.
                         "alle". Server-side. Kupp finnes hos private.
    KUPP_DRIVSTOFF     – f.eks. "Elektrisk" el. "Elektrisk,Hybrid". Tom = alle.
    KUPP_FYLKE         – fylkesnavn ("Vestland,Rogaland") el. rå FINN-kode
                         ("0.22046"). Tom = hele landet. Server-side filter.
    KUPP_STED          – delstreng på poststed/område ("Bergen,Voss"). Tom = alle.

Varsling (bruk én eller begge – sender bare via de som er konfigurert):
  Pushover (anbefalt – push til mobil):
    PUSHOVER_TOKEN    – applikasjonens API-token (fra pushover.net)
    PUSHOVER_USER     – user key, group key, ELLER flere user keys komma-separert
  E-post (gjenbruker samme SMTP som media-digest):
    SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASSWORD
    KUPP_VARSEL_TO    – mottaker(e), komma-separert
    KUPP_VARSEL_FROM  – avsender (default SMTP_USER)

Kjøring:
    python -m scripts.kupp_vakt            # normal kjøring
    python -m scripts.kupp_vakt --seed     # bare seed state, ingen varsler
    python -m scripts.kupp_vakt --dry-run  # score + skriv ut, ikke send/lagre
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bilradar_scorer import scorer_biler  # noqa: E402

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

# ---- Konfig ----
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "prisanalyse-data")
STATE_KEY = os.getenv("KUPP_VAKT_STATE_KEY", "calc/bil/kupp_vakt_state.json")
STATE_TTL_DAYS = int(os.getenv("KUPP_VAKT_TTL_DAYS", "7") or 7)

# Persistent kupp-logg (til etteranalyse: hva kjennetegner biler som blir
# solgt?). Nøklet på FinnKode -> egenskaper ved varslingstidspunktet, slik at
# scripts/kupp_analyse.py senere kan sjekke solgt-status og bryte ned treffene.
# State-fila glemmer alt annet enn FinnKoden, så uten denne loggen finnes det
# ikke noe grunnlag å tune tersklene på.
LOGG_KEY = os.getenv("KUPP_VAKT_LOGG_KEY", "calc/bil/kupp_vakt_logg.json")
LOGG_TTL_DAYS = int(os.getenv("KUPP_VAKT_LOGG_TTL_DAYS", "120") or 120)

# --- Rabattkrav: trappetrinn etter pris ---------------------------------
# Dyrere bil -> lavere prosentkrav (en billig bil må ned mye i prosent for at
# kronebeløpet skal bety noe; en dyr bil trenger bare noen få prosent).
# Format i KUPP_RABATT_TRAPP: "maxpris:minprosent" adskilt med komma, der en bil
# med pris < maxpris må ha minst minprosent rabatt. Tom maxpris (siste ledd) =
# uendelig (gjelder alle dyrere biler). Standard følger avtalt trappetrinn:
#   < 50 000 -> 30 %,  < 100 000 -> 20 %,  < 150 000 -> 15 %,
#   < 250 000 -> 7 %,  >= 250 000 -> 6 %
RABATT_TRAPP_DEFAULT = "50000:30,100000:20,150000:15,250000:7,:6"


def _parse_trapp(spec: str):
    """Tolk trappespec til sortert liste av (øvre_grense, min_prosent)."""
    bands = []
    for ledd in (spec or "").split(","):
        ledd = ledd.strip()
        if not ledd or ":" not in ledd:
            continue
        pmax, pct = ledd.split(":", 1)
        pmax, pct = pmax.strip(), pct.strip()
        try:
            pct_f = float(pct)
        except ValueError:
            continue
        if pmax in ("", "inf", "*"):
            upper = float("inf")
        else:
            try:
                upper = float(pmax)
            except ValueError:
                continue
        bands.append((upper, pct_f))
    bands.sort(key=lambda b: b[0])
    return bands


RABATT_TRAPP = _parse_trapp(os.getenv("KUPP_RABATT_TRAPP", RABATT_TRAPP_DEFAULT))


def _min_rabatt_for_pris(pris: float, bands=None) -> float:
    """Påkrevd minste rabatt-prosent for en gitt salgspris."""
    bands = RABATT_TRAPP if bands is None else bands
    if not bands:
        return RABATT_MIN
    for upper, pct in bands:
        if pris < upper:
            return pct
    return bands[-1][1]


# Legacy flat terskel – brukes bare hvis trappa er tom (KUPP_RABATT_TRAPP="").
RABATT_MIN = float(os.getenv("KUPP_RABATT_MIN", "15") or 15)
RABATT_KR_MIN = float(os.getenv("KUPP_RABATT_KR_MIN", "0") or 0)
# "Under hurtigpris" som ekstra ELLER-signal. Default AV nå som trappa styrer –
# ellers ville en billig bil så vidt under hurtigpris varsle tross høyt %-krav.
UNDER_HURTIG = os.getenv("KUPP_UNDER_HURTIG", "0").strip() not in ("0", "false", "")
MAX_VARSLER = int(os.getenv("KUPP_MAX_VARSLER", "40") or 40)


# --- Drivstoff-filter ----------------------------------------------------
def _norm_driv(s: str) -> str:
    s = (s or "").strip().lower()
    if s in ("el", "elektrisk", "elbil", "bev"):
        return "elektrisk"
    if "plug" in s or "ladbar" in s or "phev" in s:
        return "plug-in hybrid"
    if "hybrid" in s or s in ("hev", "mhev"):
        return "hybrid"
    if s in ("diesel",):
        return "diesel"
    if s in ("bensin", "petrol"):
        return "bensin"
    return s


# KUPP_DRIVSTOFF: komma-separert, f.eks. "Elektrisk" eller "Elektrisk,Hybrid".
# Tom = alle drivstoff. Biler uten gjenkjent drivstoff filtreres bort når satt.
DRIVSTOFF_FILTER = {
    _norm_driv(x) for x in os.getenv("KUPP_DRIVSTOFF", "").split(",") if x.strip()
}

# KUPP_STED: komma-separert delstreng-filter på annonsens sted (poststed/område),
# f.eks. "Bergen,Voss". Tom = hele landet. Case-uavhengig delstreng-match.
STED_FILTER = [s.strip().lower() for s in os.getenv("KUPP_STED", "").split(",") if s.strip()]


# --- Fylke-filter (server-side via FINN sin location-kode) ----------------
# Verifiserte koder for gjeldende (2024) fylkesinndeling. Brukeren kan skrive
# fylkesnavn i KUPP_FYLKE (mappes her) ELLER lime inn en rå kode "0.xxxxx".
FYLKE_LOCATION = {
    "østfold": "0.20002", "ostfold": "0.20002",
    "akershus": "0.20003",
    "oslo": "0.20061",
    "innlandet": "0.22034",
    "buskerud": "0.20007",
    "vestfold": "0.20008",
    "telemark": "0.20009",
    "agder": "0.22042",
    "rogaland": "0.20012",
    "vestland": "0.22046",
    "møre og romsdal": "0.20015", "more og romsdal": "0.20015", "møre": "0.20015",
    "trøndelag": "0.20016", "trondelag": "0.20016",
    "nordland": "0.20018",
    "troms": "0.20019",
    "finnmark": "0.20020",
}


def _location_codes():
    out = []
    for tok in os.getenv("KUPP_FYLKE", "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        if re.match(r"^\d+\.\d+$", tok):
            out.append(tok)
        else:
            code = FYLKE_LOCATION.get(tok.lower())
            if code:
                out.append(code)
            else:
                print(f"[kupp_vakt] Ukjent fylke: {tok!r} "
                      "(bruk navn som 'Vestland' eller kode '0.22046')")
    return out


LOCATION_CODES = _location_codes()
LOCATION_SUFFIX = "".join(f"&location={c}" for c in LOCATION_CODES)


# --- Selger-type (server-side via FINN sitt dealer_segment) ---------------
# Kupp finnes hos private – forhandlere priser til marked med margin/garanti,
# så "kjøp av forhandler og selg med gevinst" er nesten umulig. Default: privat.
#   privat = 3, merkeforhandler = 1, annet bilutsalg = 2, alle = ingen filter
SELGER_SEGMENT = {"privat": "3", "merkeforhandler": "1",
                  "annet": "2", "bilutsalg": "2", "forhandler": "2"}
_selger = os.getenv("KUPP_SELGER", "privat").strip().lower()
if _selger in ("", "alle", "all"):
    SELGER_SUFFIX = ""
else:
    _kode = SELGER_SEGMENT.get(_selger)
    if _kode:
        SELGER_SUFFIX = f"&dealer_segment={_kode}"
    else:
        SELGER_SUFFIX = ""
        print(f"[kupp_vakt] Ukjent KUPP_SELGER: {_selger!r} "
              "(bruk 'privat', 'merkeforhandler', 'annet' eller 'alle') – tar alle")


# --- Hjemfylke-vekting: biler utenfor hjemfylket krever større rabatt --------
# Vi kan hente biler fra hele landet, men utenfor hjemfylket koster frakt/henting,
# så der krever vi et tillegg (prosentpoeng) i rabattkravet. Biler i hjemfylket
# beholder trappa uendret. Krever at KUPP_FYLKE står tom (hele landet); ellers
# er alt allerede i ett fylke. Sett KUPP_UTENFOR_TILLEGG_PP=0 for å skru av.
_HJEMFYLKE = os.getenv("KUPP_HJEMFYLKE", "Vestland").strip()
if re.match(r"^\d+\.\d+$", _HJEMFYLKE):
    HJEMFYLKE_KODE, HJEMFYLKE_NAVN = _HJEMFYLKE, _HJEMFYLKE
else:
    HJEMFYLKE_KODE = FYLKE_LOCATION.get(_HJEMFYLKE.lower(), "")
    HJEMFYLKE_NAVN = _HJEMFYLKE or "(ingen)"
    if _HJEMFYLKE and not HJEMFYLKE_KODE:
        print(f"[kupp_vakt] Ukjent KUPP_HJEMFYLKE: {_HJEMFYLKE!r} – hjemfylke-vekting av")
UTENFOR_TILLEGG_PP = float(os.getenv("KUPP_UTENFOR_TILLEGG_PP", "8") or 8)

# Nabofylke-nivå: mellom hjemfylket (+0) og resten (+UTENFOR_TILLEGG_PP) kan vi
# legge inn nabofylker med et mindre tillegg (kortere reise). Backtesten viste at
# modellens treffsikkerhet er jevn paa tvers av fylker, saa dette er en ren
# reise-/logistikk-vekting: Vestland +0, nabo +NABO_TILLEGG_PP, resten +UTENFOR.
_NABOFYLKE = os.getenv("KUPP_NABOFYLKE", "Rogaland,Møre og Romsdal")
NABO_KODER = [FYLKE_LOCATION[t.strip().lower()]
              for t in _NABOFYLKE.split(",")
              if t.strip() and t.strip().lower() in FYLKE_LOCATION]
NABO_LOCATION_SUFFIX = "".join(f"&location={c}" for c in NABO_KODER)
NABO_TILLEGG_PP = float(os.getenv("KUPP_NABO_TILLEGG_PP", "5") or 5)

# --- Elbil: merke-tiered rabattkrav ------------------------------------------
# Backtesten (scripts/kupp_backtest.py --per produsent) viste at elbil-merker
# oppfoerer seg svaert ulikt: Tesla/Kia/Nissan/Renault/VW/MG selger raskt selv
# ved lav rabatt (taaler lav terskel), mens Volvo/Toyota/Skoda/premium trenger
# stoerre rabatt. Naar KUPP_EL_TIER=1 erstattes pris-trappa av en merke-terskel
# for elbiler. Ukjent/nisje-merke faar HOY terskel (var svakest i analysen).
EL_TIER_ON = os.getenv("KUPP_EL_TIER", "0").strip().lower() not in ("0", "false", "")


def _merkeliste(env_navn: str, default: str) -> set:
    return {m.strip().lower() for m in os.getenv(env_navn, default).split(",") if m.strip()}


EL_MERKER_LAV = _merkeliste(
    "KUPP_EL_MERKER_LAV", "Tesla,Kia,Nissan,Renault,Volkswagen,MG,Hyundai,Peugeot")
EL_MERKER_MEDIUM = _merkeliste("KUPP_EL_MERKER_MEDIUM", "BMW,Audi,Polestar,Ford")
EL_MERKER_HOY = _merkeliste(
    "KUPP_EL_MERKER_HOY", "Volvo,Toyota,Skoda,Mercedes-Benz,Porsche,BYD")
EL_TERSKEL_LAV = float(os.getenv("KUPP_EL_TERSKEL_LAV", "2") or 2)
EL_TERSKEL_MEDIUM = float(os.getenv("KUPP_EL_TERSKEL_MEDIUM", "6") or 6)
EL_TERSKEL_HOY = float(os.getenv("KUPP_EL_TERSKEL_HOY", "12") or 12)
EL_TERSKEL_DEFAULT = float(os.getenv("KUPP_EL_TERSKEL_DEFAULT", "12") or 12)

_MERKE_ALIAS = {"vw": "volkswagen", "mercedes": "mercedes-benz", "mb": "mercedes-benz"}


def _el_merke_terskel(merke) -> float:
    """Rabatt-terskel (prosent) for et elbil-merke, fra tier-listene."""
    m = (merke or "").strip().lower()
    m = _MERKE_ALIAS.get(m, m)
    if m in EL_MERKER_LAV:
        return EL_TERSKEL_LAV
    if m in EL_MERKER_MEDIUM:
        return EL_TERSKEL_MEDIUM
    if m in EL_MERKER_HOY:
        return EL_TERSKEL_HOY
    return EL_TERSKEL_DEFAULT


def _basis_terskel(row) -> float:
    """Grunnkrav til rabatt (prosent) foer fylke-/kurant-justering.
    Elbil (naar KUPP_EL_TIER=1): merke-terskel. Ellers: pris-trappa."""
    pris = float(row.get("salgspris") or 0)
    if EL_TIER_ON:
        driv = _norm_driv(row.get("Drivstoff"))
        # Naar hele soeket allerede er laast til elbil (KUPP_DRIVSTOFF=Elektrisk)
        # regnes alt som elbil selv om Drivstoff-kolonnen skulle mangle.
        er_el = driv == "elektrisk" or DRIVSTOFF_FILTER == {"elektrisk"}
        if er_el:
            return _el_merke_terskel(row.get("Merke"))
    return _min_rabatt_for_pris(pris)

# --- Kurante modeller: lettere krav (lette å omsette) ------------------------
# Komma-separert liste med "Merke Modell"-fragmenter, f.eks. "Volkswagen Golf,
# Toyota RAV4". Match = delstreng i "Merke Modell" (case-uavhengig). For disse
# senkes rabattkravet med KUPP_KURANT_LETTELSE_PP prosentpoeng.
KURANTE = [s.strip().lower() for s in os.getenv("KUPP_KURANTE", "").split(",") if s.strip()]
KURANT_LETTELSE_PP = float(os.getenv("KUPP_KURANT_LETTELSE_PP", "3") or 3)


def _match_filtre(b: dict) -> bool:
    """Klient-side filtre (drivstoff + sted) på en rå annonse fra scraperen."""
    if DRIVSTOFF_FILTER and _norm_driv(b.get("Drivstoff")) not in DRIVSTOFF_FILTER:
        return False
    if STED_FILTER:
        sted = (b.get("sted") or "").lower()
        if not any(f in sted for f in STED_FILTER):
            return False
    return True


FINN_ITEM_URL = "https://www.finn.no/mobility/item/{}"
BASE_URL = (
    "https://www.finn.no/mobility/search/car"
    "?mileage_from=1&price_from=1500&published=1&registration_class=1&sales_form=1"
    "&sort=PUBLISHED_DESC"
)
DRIFT_SEARCHES = {"Tohjul": "1&wheel_drive=3", "Firehjul": "2"}
MAX_PAGES = int(os.getenv("KUPP_MAX_PAGES", "2") or 2)  # nyeste først -> få sider holder

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]
BASE_HEADERS = {
    "Accept-Language": "nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.finn.no/",
}
HTTP_TIMEOUT = 20


def _s3():
    return boto3.client("s3")


# ======================================================
# FINN-scraping (lettvekts; gjenbruker parselogikk fra hoved-scraperen)
# ======================================================

def _build_url(page: int, drift_code: str) -> str:
    return (f"{BASE_URL}&wheel_drive={drift_code}&page={page}"
            f"{LOCATION_SUFFIX}{SELGER_SUFFIX}")


def _make_session():
    s = requests.Session()
    h = BASE_HEADERS.copy()
    h["User-Agent"] = random.choice(USER_AGENTS)
    s.headers.update(h)
    return s


def _fetch(session, url, attempts=3):
    for i in range(1, attempts + 1):
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                time.sleep(random.uniform(0.4, 0.9))
                return resp
            if resp.status_code in (403, 429):
                time.sleep(1.0 * i)
                continue
            resp.raise_for_status()
        except requests.RequestException:
            if i == attempts:
                return None
            time.sleep(0.7 * i)
    return None


def _find_cards(soup):
    seen, cards = set(), []
    for a in soup.select("a[href*='/mobility/item/']"):
        href = a.get("href", "")
        if not href or href in seen:
            continue
        seen.add(href)
        card = a.find_parent("article") or a.find_parent("li") or a.find_parent("div")
        if card:
            cards.append((a, card))
    return cards


def _finnkode(href: str) -> str:
    m = re.search(r"/item/(\d+)", href or "")
    return m.group(1) if m else ""


def _title_info(card, link_tag):
    bilmerke = ""
    for sel in ["h2", "h3", "div[data-testid='title']"]:
        tag = card.select_one(sel)
        if tag and tag.get_text(strip=True):
            bilmerke = tag.get_text(strip=True)
            break
    if not bilmerke and link_tag:
        bilmerke = link_tag.get_text(strip=True)
    bilmerke = " ".join(bilmerke.split())
    parts = bilmerke.split(maxsplit=1)
    return (parts[0] if parts else ""), (parts[1] if len(parts) > 1 else ""), bilmerke


def _sted(card) -> str:
    """Poststed/område fra kortet. FINN viser 'Sted ∙ Selger' i første
    span.truncate.block; vi tar delen før skilletegnet."""
    sp = card.select_one("span.truncate.block")
    if not sp:
        return ""
    txt = sp.get_text(" ", strip=True).replace("\xa0", " ")
    return re.split(r"[∙•·]", txt)[0].strip()


def _meta_text(card) -> str:
    root = card.select_one(".mobility-search-ad-card-content") or card
    tag = root.select_one("span.text-caption.font-bold")
    if tag:
        return re.sub(r"\s+", " ", tag.get_text(" ", strip=True).replace("\xa0", " ")).strip()
    return ""


def _fordel(verdier: str):
    """Plukk år, km, drivstoff, rekkevidde fra meta-strengen (forenklet
    variant av hoved-scraperens logikk)."""
    year_max = datetime.now().year + 1
    s = re.sub(r"[∙•]", "·", (verdier or "").replace("\xa0", " ").strip())
    tokens = [t.strip() for t in s.split("·") if t.strip()]

    aar, km = "", 0
    for i in range(len(tokens) - 1):
        if re.fullmatch(r"(19|20)\d{2}", tokens[i]) and 1950 <= int(tokens[i]) <= year_max:
            m = re.search(r"(\d[\d\s]*)\s*km\b", tokens[i + 1], re.I)
            if m:
                aar = tokens[i]
                km = int(re.sub(r"\s+", "", m.group(1)))
                break

    lt = s.lower()
    if re.search(r"\b(el|elektrisk|bev|elbil)\b", lt):
        drivstoff = "Elektrisk"
    elif re.search(r"plug[-\s]*in|ladbar|phev", lt):
        drivstoff = "Plug-in hybrid"
    elif re.search(r"\b(hybrid|hev|mhev)\b", lt):
        drivstoff = "Hybrid"
    elif re.search(r"\b(diesel|tdi|hdi|dci|cdti|crdi)\b", lt):
        drivstoff = "Diesel"
    elif re.search(r"\b(bensin|petrol|tsi|tfsi|mpi)\b", lt):
        drivstoff = "Bensin"
    else:
        drivstoff = ""

    rekkevidde = 0
    for t in tokens:
        if any(k in t.lower() for k in ("rekkevidde", "wltp", "epa", "nedc")):
            m = re.search(r"(\d[\d\s]*)\s*km\b", t, re.I)
            if m:
                rekkevidde = int(re.sub(r"\s+", "", m.group(1)))
                break
    return aar, km, drivstoff, rekkevidde


def _price(card) -> str:
    try:
        text = " ".join(card.stripped_strings)
    except Exception:
        return ""
    if "solgt" in text.lower():
        return "Solgt"
    m = re.search(r"(\d[\d\s ]*)\s*kr\b", text, re.I)
    return re.sub(r"[\s ]+", "", m.group(1)) if m else ""


def scrape_nyeste() -> list[dict]:
    """Hent de nyeste annonsene (begge hjuldrift-søk, få sider)."""
    biler: dict[str, dict] = {}
    for drift_key, drift_code in DRIFT_SEARCHES.items():
        session = _make_session()
        for page in range(1, MAX_PAGES + 1):
            resp = _fetch(session, _build_url(page, drift_code))
            if not resp:
                break
            soup = BeautifulSoup(resp.text, "lxml")
            cards = _find_cards(soup)
            if not cards:
                break
            for a, card in cards:
                fk = _finnkode(a.get("href", ""))
                if not fk or fk in biler:
                    continue
                merke, modell, tittel = _title_info(card, a)
                aar, km, drivstoff, rekkevidde = _fordel(_meta_text(card))
                pris = _price(card)
                biler[fk] = {
                    "FinnKode": fk,
                    "Merke": merke,
                    "Modell": modell,
                    "Info": tittel,
                    "Årstall": aar or None,
                    "Kjørelengde": km or None,
                    "Drivstoff": drivstoff or None,
                    "Hjuldrift": drift_key,
                    "rekkevidde_km": rekkevidde or None,
                    "Pris": pris,
                    "sted": _sted(card),
                    "url": FINN_ITEM_URL.format(fk),
                }
        session.close()
    return list(biler.values())


def _scrape_koder_for_location(location_suffix: str) -> set:
    """FinnKoder blant de nyeste annonsene for et gitt location-filter (server-
    side). Tom ved feil – da straffes ingen biler ekstra."""
    if not location_suffix:
        return set()
    koder = set()
    for _, drift_code in DRIFT_SEARCHES.items():
        session = _make_session()
        for page in range(1, MAX_PAGES + 1):
            url = (f"{BASE_URL}&wheel_drive={drift_code}&page={page}"
                   f"{location_suffix}{SELGER_SUFFIX}")
            resp = _fetch(session, url)
            if not resp:
                break
            soup = BeautifulSoup(resp.text, "lxml")
            cards = _find_cards(soup)
            if not cards:
                break
            for a, _card in cards:
                fk = _finnkode(a.get("href", ""))
                if fk:
                    koder.add(fk)
        session.close()
    return koder


def _scrape_hjemfylke_koder() -> set:
    """FinnKoder i hjemfylket (bakoverkompatibel wrapper)."""
    if not HJEMFYLKE_KODE or not UTENFOR_TILLEGG_PP:
        return set()
    return _scrape_koder_for_location(f"&location={HJEMFYLKE_KODE}")


def _hjem_sett(biler: list[dict]):
    """Sett med FinnKoder som ligger i hjemfylket, eller None hvis vektingen er
    av / ikke lot seg avgjøre. Er hovedsøket allerede låst til hjemfylket
    (KUPP_FYLKE), er alle hentede biler hjemme – da slipper vi et ekstra søk."""
    if not HJEMFYLKE_KODE or not UTENFOR_TILLEGG_PP:
        return None
    if LOCATION_CODES == [HJEMFYLKE_KODE]:
        return {b["FinnKode"] for b in biler}
    return _scrape_hjemfylke_koder() or None


def _nabo_sett():
    """Sett med FinnKoder i nabofylkene (mindre tillegg), eller None. Kun
    relevant naar hovedsoeket ikke allerede er laast til hjemfylket."""
    if not HJEMFYLKE_KODE or not UTENFOR_TILLEGG_PP or not NABO_LOCATION_SUFFIX:
        return None
    if LOCATION_CODES == [HJEMFYLKE_KODE]:
        return None
    return _scrape_koder_for_location(NABO_LOCATION_SUFFIX) or None


# ======================================================
# State (S3): hvilke FinnKoder er allerede varslet
# ======================================================

def last_state(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return {}


def lagre_state(s3, state: dict):
    # Prune eldre enn TTL
    grense = datetime.now(timezone.utc) - timedelta(days=STATE_TTL_DAYS)
    renset = {}
    for fk, ts in state.items():
        try:
            if datetime.fromisoformat(ts) >= grense:
                renset[fk] = ts
        except Exception:
            renset[fk] = ts
    s3.put_object(
        Bucket=S3_BUCKET, Key=STATE_KEY,
        Body=json.dumps(renset, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


# ======================================================
# Kupp-logg (S3): egenskaper for hver varslede bil, til etteranalyse
# ======================================================

def _logg_record(b: dict, naa: str) -> dict:
    """Plukk ut de egenskapene vi vil kunne analysere senere. Numeriske felt
    renses (NaN -> None) så loggen blir gyldig JSON."""
    def num(v):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return f if f == f else None  # NaN != NaN

    return {
        "flagget": naa,
        "Merke": (b.get("Merke") or "").strip() or None,
        "Modell": (b.get("Modell") or "").strip() or None,
        "Årstall": b.get("Årstall"),
        "Kjørelengde": num(b.get("Kjørelengde")),
        "Drivstoff": b.get("Drivstoff"),
        "Hjuldrift": b.get("Hjuldrift"),
        "sted": (b.get("sted") or b.get("Sted") or "").strip() or None,
        "pris": num(b.get("salgspris") if b.get("salgspris") is not None else b.get("Pris")),
        "forventet_pris": num(b.get("forventet_pris")),
        "hurtigpris": num(b.get("hurtigpris")),
        "innbyttepris": num(b.get("innbyttepris")),
        "rabatt_pct": num(b.get("rabatt_pct")),
        "rabatt_kr": num(b.get("rabatt_kr")),
        "modell_nivaa": b.get("modell_nivaa"),
        "selger_filter": _selger or "alle",
        "fylke_filter": os.getenv("KUPP_FYLKE", "").strip() or "hele landet",
        "i_hjemfylke": b.get("i_hjemfylke"),
        "url": b.get("url"),
    }


def logg_kupp(s3, kupp: list[dict], naa: str):
    """Legg de varslede kuppene til den persistente S3-loggen (nøklet på
    FinnKode, så gjentatte treff dedupliseres og beholder første tidspunkt).
    Prunes etter LOGG_TTL_DAYS. En feil her skal aldri stoppe varslingen."""
    if not kupp:
        return
    try:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=LOGG_KEY)
            logg = json.loads(obj["Body"].read().decode("utf-8"))
            if not isinstance(logg, dict):
                logg = {}
        except Exception:
            logg = {}

        for b in kupp:
            fk = str(b.get("FinnKode") or "")
            if fk and fk not in logg:  # behold første gang bilen ble flagget
                logg[fk] = _logg_record(b, naa)

        grense = datetime.now(timezone.utc) - timedelta(days=LOGG_TTL_DAYS)
        renset = {}
        for fk, rec in logg.items():
            try:
                if datetime.fromisoformat(rec.get("flagget", "")) >= grense:
                    renset[fk] = rec
            except Exception:
                renset[fk] = rec

        s3.put_object(
            Bucket=S3_BUCKET, Key=LOGG_KEY,
            Body=json.dumps(renset, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[kupp_vakt] Logget kupp (loggen har nå {len(renset)} biler)")
    except Exception as e:
        print(f"[kupp_vakt] Advarsel: klarte ikke skrive kupp-logg: {e}")


# ======================================================
# Varsling
# ======================================================

def _formater_bil(b: dict) -> str:
    def kr(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", " ") + " kr"
        except Exception:
            return "?"
    sted = (b.get("sted") or b.get("Sted") or "").strip()
    stedtekst = f" – {sted}" if sted else ""
    linje = (
        f"{(b.get('Merke') or '').strip()} {(b.get('Modell') or '').strip()}".strip()
        + f" ({b.get('Årstall') or '?'}, {kr(b.get('Kjørelengde')).replace(' kr', ' km')})"
        f"{stedtekst}\n"
        f"  Pris: {kr(b.get('Pris'))}  |  Forventet: {kr(b.get('forventet_pris'))}"
        f"  |  Rabatt: {b.get('rabatt_pct')} %\n"
        f"  Hurtigpris: {kr(b.get('hurtigpris'))}  |  Innbytte: {kr(b.get('innbyttepris'))}\n"
        f"  {b.get('url')}"
    )
    return linje


def _send_epost(kupp: list[dict]) -> bool:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    mottakere = [m.strip() for m in os.environ.get("KUPP_VARSEL_TO", "").split(",") if m.strip()]
    if not smtp_host or not mottakere:
        print("[kupp_vakt] SMTP_HOST/KUPP_VARSEL_TO ikke satt – hopper over e-post")
        return False

    avsender = os.environ.get("KUPP_VARSEL_FROM", os.environ.get("SMTP_USER", "")).strip()
    body = (
        f"Fant {len(kupp)} ferske, underprisede biler på FINN:\n\n"
        + "\n\n".join(_formater_bil(b) for b in kupp)
        + "\n\n— Kupp-vakt (prisanalyse.no)"
    )
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = f"🚗 Kupp-vakt: {len(kupp)} nye gode kjøp"
    msg["From"] = avsender
    msg["To"] = ", ".join(mottakere)

    port = int(os.environ.get("SMTP_PORT", "587"))
    with smtplib.SMTP(smtp_host, port, timeout=30) as server:
        server.starttls()
        smtp_user = os.environ.get("SMTP_USER", "")
        if smtp_user:
            server.login(smtp_user, os.environ.get("SMTP_PASSWORD", ""))
        server.sendmail(avsender, mottakere, msg.as_string())
    print(f"[kupp_vakt] Sendte e-post til {', '.join(mottakere)}")
    return True


def _pushover_melding(kupp: list[dict]) -> str:
    """Kompakt melding for Pushover (maks 1024 tegn)."""
    def kr(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception:
            return "?"

    linjer, brukt = [], 0
    vist = 0
    for b in kupp:
        navn = f"{(b.get('Merke') or '').strip()} {(b.get('Modell') or '').strip()}".strip()
        rab = b.get("rabatt_pct")
        rab_s = f"-{abs(float(rab)):.0f}%" if rab is not None and not pd.isna(rab) else "?"
        rab_kr = b.get("rabatt_kr")
        kr_s = f" / -{kr(rab_kr)} kr" if rab_kr is not None and not pd.isna(rab_kr) else ""
        sted = (b.get("sted") or b.get("Sted") or "").strip()
        sted_s = f" – {sted}" if sted else ""
        linje = (
            f"{navn} {b.get('Årstall') or '?'}, {kr(b.get('Kjørelengde'))} km{sted_s}\n"
            f"{kr(b.get('Pris'))} kr ({rab_s}{kr_s} mot {kr(b.get('forventet_pris'))})\n"
            f"{b.get('url')}"
        )
        if brukt + len(linje) + 2 > 980:
            break
        linjer.append(linje)
        brukt += len(linje) + 2
        vist += 1
    if vist < len(kupp):
        linjer.append(f"… +{len(kupp) - vist} flere")
    return "\n\n".join(linjer)


def _send_pushover(kupp: list[dict]) -> bool:
    """Send push-varsel via Pushover. PUSHOVER_USER kan være én user key, en
    delivery-group-nøkkel, eller flere user keys komma-separert (én melding per
    mottaker). Sender ingenting hvis token/bruker ikke er satt."""
    token = os.environ.get("PUSHOVER_TOKEN", "").strip()
    brukere = [u.strip() for u in os.environ.get("PUSHOVER_USER", "").split(",") if u.strip()]
    if not token or not brukere:
        return False

    melding = _pushover_melding(kupp)
    tittel = f"🚗 Kupp-vakt: {len(kupp)} nye gode kjøp"
    topp_url = kupp[0].get("url") if kupp else None

    ok = False
    for bruker in brukere:
        data = {"token": token, "user": bruker, "title": tittel, "message": melding}
        if topp_url:
            data["url"] = topp_url
            data["url_title"] = "Åpne på FINN"
        try:
            resp = requests.post(
                "https://api.pushover.net/1/messages.json", data=data, timeout=15
            )
            if resp.status_code == 200:
                ok = True
            else:
                print(f"[kupp_vakt] Pushover-feil ({resp.status_code}): {resp.text[:200]}")
        except requests.RequestException as e:
            print(f"[kupp_vakt] Pushover-kall feilet: {e}")
    if ok:
        print(f"[kupp_vakt] Sendte Pushover-varsel til {len(brukere)} mottaker(e)")
    return ok


# ======================================================
# Hovedlogikk
# ======================================================

def _score(biler: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(biler)
    if df.empty:
        return df
    # Kun rader med reell pris (dropp "Solgt"/tom)
    df["_pris_num"] = pd.to_numeric(
        df["Pris"].astype(str).str.replace(r"[^\d]", "", regex=True).replace("", None),
        errors="coerce",
    )
    df = df[df["_pris_num"].notna() & (df["_pris_num"] > 0)].copy()
    if df.empty:
        return df
    df["Pris"] = df["_pris_num"]
    scoret = scorer_biler(df, modeller=None)  # lookup + peer, ingen ML
    return scoret


def _er_kurant(row) -> bool:
    """Er bilen en «kurant» modell (lett å omsette) som får lettere krav?"""
    if not KURANTE:
        return False
    navn = f"{(row.get('Merke') or '').strip()} {(row.get('Modell') or '').strip()}".lower()
    return any(k in navn for k in KURANTE)


def _terskel_delta(row, hjem_koder, nabo_koder=None) -> float:
    """Justering av rabattkravet (prosentpoeng) for én bil:
      + nabo-tillegg for biler i nabofylkene (kortere reise),
      + utenfor-tillegg for resten av landet (lengre reise),
      - lettelse for kurante modeller.
    Biler i hjemfylket beholder grunnkravet (delta 0)."""
    delta = 0.0
    if hjem_koder is not None and str(row.get("FinnKode") or "") not in hjem_koder:
        fk = str(row.get("FinnKode") or "")
        if nabo_koder is not None and fk in nabo_koder:
            delta += NABO_TILLEGG_PP
        else:
            delta += UTENFOR_TILLEGG_PP
    if _er_kurant(row):
        delta -= KURANT_LETTELSE_PP
    return delta


def _fylke_tag(row, hjem_koder, nabo_koder=None) -> str:
    """Kort merkelapp om fylke-vekting for visning/logg."""
    if hjem_koder is None or str(row.get("FinnKode") or "") in hjem_koder:
        return ""
    fk = str(row.get("FinnKode") or "")
    if nabo_koder is not None and fk in nabo_koder:
        return f"nabofylke +{NABO_TILLEGG_PP:g}pp"
    return f"utenfor {HJEMFYLKE_NAVN} +{UTENFOR_TILLEGG_PP:g}pp"


def _er_kupp(row, terskel_delta: float = 0.0) -> bool:
    forv = row.get("forventet_pris")
    if forv is None or pd.isna(forv) or forv <= 0:
        return False
    pris = row.get("salgspris")
    rab = row.get("rabatt_pct")
    if (rab is not None and not pd.isna(rab)
            and pris is not None and not pd.isna(pris)):
        if float(rab) >= _basis_terskel(row) + terskel_delta:
            return True
    # Legacy flat kroneterskel (kun hvis eksplisitt satt via KUPP_RABATT_KR_MIN)
    rab_kr = row.get("rabatt_kr")
    if RABATT_KR_MIN > 0 and rab_kr is not None and not pd.isna(rab_kr) and float(rab_kr) >= RABATT_KR_MIN:
        return True
    if UNDER_HURTIG:
        hurtig = row.get("hurtigpris")
        if hurtig is not None and not pd.isna(hurtig) and pris is not None and float(pris) < float(hurtig):
            return True
    return False


def _vis_alle() -> int:
    """Tuning-modus: scor ALLE nåværende kandidater (uansett state), sortert
    etter rabatt, med [KUPP]-markering. Sender ingenting og rører ikke state."""
    print(f"[kupp_vakt] Scraper nyeste (maks {MAX_PAGES} sider per hjuldrift) ...")
    biler = scrape_nyeste()
    print(f"[kupp_vakt] {len(biler)} annonser hentet")
    kand = [b for b in biler if _match_filtre(b)]
    print(f"[kupp_vakt] {len(kand)} kandidater etter drivstoff/sted-filter (viser alle)")
    hjem = _hjem_sett(biler)
    nabo = _nabo_sett() if hjem is not None else None
    scoret = _score(kand)
    rader = [row.to_dict() for _, row in scoret.iterrows()] if not scoret.empty else []
    rader.sort(key=lambda d: -(float(d.get("rabatt_pct"))
                               if d.get("rabatt_pct") == d.get("rabatt_pct") else -999))
    n_kupp = 0
    for d in rader:
        delta = _terskel_delta(d, hjem, nabo)
        er = _er_kupp(d, delta)
        n_kupp += 1 if er else 0
        merke = []
        fylke_tag = _fylke_tag(d, hjem, nabo)
        if fylke_tag:
            merke.append(fylke_tag)
        if _er_kurant(d):
            merke.append(f"kurant -{KURANT_LETTELSE_PP:g}pp")
        tag = f"  [{', '.join(merke)}]" if merke else ""
        print(("[KUPP] " if er else "       ") + _formater_bil(d) + tag)
    print(f"[kupp_vakt] {n_kupp} av {len(rader)} kandidater over terskel")
    return n_kupp


def kjor(seed: bool = False, dry_run: bool = False, vis_alle: bool = False) -> int:
    if vis_alle:
        return _vis_alle()

    s3 = _s3()
    state = last_state(s3)
    forste_gang = len(state) == 0

    print(f"[kupp_vakt] Scraper nyeste (maks {MAX_PAGES} sider per hjuldrift) ...")
    biler = scrape_nyeste()
    print(f"[kupp_vakt] {len(biler)} annonser hentet")
    if not biler:
        return 0

    naa = datetime.now(timezone.utc).isoformat()

    # Seed / første kjøring: marker alt som sett, ikke varsle (unngå flom).
    if seed or forste_gang:
        for b in biler:
            state[b["FinnKode"]] = naa
        if not dry_run:
            lagre_state(s3, state)
        print(f"[kupp_vakt] Seed: markerte {len(biler)} annonser som sett – ingen varsler denne gangen")
        return 0

    # Kun nye annonser vi ikke har vurdert før
    nye = [b for b in biler if b["FinnKode"] not in state]
    print(f"[kupp_vakt] {len(nye)} helt nye annonser siden sist")
    if not nye:
        return 0

    # Klient-side filtre (drivstoff/sted) før scoring. Vi scorer/varsler kun
    # kandidatene, men markerer ALLE nye som sett lenger nede – så et endret
    # filter senere ikke dumper et helt baklogg av "nye" biler.
    kandidater = [b for b in nye if _match_filtre(b)]
    if DRIVSTOFF_FILTER or STED_FILTER:
        print(f"[kupp_vakt] {len(kandidater)} kandidater etter drivstoff/sted-filter")

    hjem = _hjem_sett(biler) if kandidater else None
    nabo = _nabo_sett() if hjem is not None else None
    if hjem is not None:
        nabo_txt = (f", nabofylke +{NABO_TILLEGG_PP:g} pp" if nabo else "")
        print(f"[kupp_vakt] {len(hjem)} av de nyeste ligger i hjemfylket "
              f"({HJEMFYLKE_NAVN}); resten krever +{UTENFOR_TILLEGG_PP:g} pp rabatt{nabo_txt}")
    scoret = _score(kandidater)
    kupp = []
    if not scoret.empty:
        for _, row in scoret.iterrows():
            d = row.to_dict()
            delta = _terskel_delta(d, hjem, nabo)
            if _er_kupp(d, delta):
                d["i_hjemfylke"] = (hjem is None
                                    or str(d.get("FinnKode") or "") in hjem)
                kupp.append(d)

    # Marker ALLE nye som sett (også de som ikke var kupp) så vi ikke re-vurderer.
    for b in nye:
        state[b["FinnKode"]] = naa

    kupp.sort(key=lambda d: (-(float(d.get("rabatt_pct") or 0))))
    kupp = kupp[:MAX_VARSLER]
    if RABATT_TRAPP:
        terskel = "trappetrinn " + ", ".join(
            (f"<{int(u)}k:{p:g}%" if u != float("inf") else f"ellers:{p:g}%")
            for u, p in RABATT_TRAPP)
    else:
        terskel = f"rabatt >= {RABATT_MIN}%"
    vekting = ""
    if EL_TIER_ON:
        vekting += "; elbil merke-tiered terskel"
    if hjem is not None:
        vekting += f"; resten +{UTENFOR_TILLEGG_PP:g}pp"
        if nabo:
            vekting += f", nabo +{NABO_TILLEGG_PP:g}pp"
    if KURANTE:
        vekting += f"; kurante -{KURANT_LETTELSE_PP:g}pp"
    print(f"[kupp_vakt] {len(kupp)} kupp over terskel ({terskel}"
          + (" eller under hurtigpris" if UNDER_HURTIG else "") + vekting + ")")

    if dry_run:
        for b in kupp:
            print(_formater_bil(b))
        return len(kupp)

    if kupp:
        sendt_push = _send_pushover(kupp)
        sendt_epost = _send_epost(kupp)
        if not (sendt_push or sendt_epost):
            print("[kupp_vakt] Ingen varslingskanal konfigurert (Pushover/SMTP) – "
                  "fant kupp, men sendte ingenting")
        logg_kupp(s3, kupp, naa)
    lagre_state(s3, state)
    return len(kupp)


def send_testvarsel() -> bool:
    """Send ett demo-varsel gjennom alle konfigurerte kanaler, for å bekrefte at
    Pushover/SMTP-oppsettet virker. Rører ikke FINN eller state."""
    demo = [{
        "Merke": "TEST", "Modell": "testvarsel", "Årstall": datetime.now().year,
        "Kjørelengde": 50000, "Pris": 250000, "forventet_pris": 310000,
        "rabatt_pct": 19.4, "rabatt_kr": 60000, "hurtigpris": 290000, "innbyttepris": 263500,
        "sted": "Testby",
        "url": FINN_ITEM_URL.format("000000000"),
    }]
    push = _send_pushover(demo)
    epost = _send_epost(demo)
    print(f"[kupp_vakt] Testvarsel: pushover={push}, epost={epost}")
    if not (push or epost):
        print("[kupp_vakt] Ingen kanal svarte OK – sjekk PUSHOVER_TOKEN/PUSHOVER_USER "
              "(eller SMTP_*/KUPP_VARSEL_TO).")
    return push or epost


def main():
    parser = argparse.ArgumentParser(description="Kupp-vakt: varsle om ferske underprisede biler.")
    parser.add_argument("--seed", action="store_true", help="Bare seed state, ingen varsler")
    parser.add_argument("--dry-run", action="store_true", help="Score og skriv ut, ikke send/lagre")
    parser.add_argument("--test", action="store_true",
                        help="Send ett demo-varsel (verifiser Pushover/SMTP) og avslutt")
    parser.add_argument("--vis-alle", dest="vis_alle", action="store_true",
                        help="Tuning: scor og vis ALLE nåværende kandidater "
                             "(uansett state), marker [KUPP]. Sender/lagrer ikke.")
    args = parser.parse_args()
    if args.test:
        send_testvarsel()
        return
    antall = kjor(seed=args.seed, dry_run=args.dry_run, vis_alle=args.vis_alle)
    if not args.vis_alle:
        print(f"[kupp_vakt] Ferdig ({antall} kupp).")


if __name__ == "__main__":
    main()
