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

Terskel (env, kan overstyres):
    KUPP_RABATT_MIN   – varsle hvis rabatt_pct >= denne (default 15)
    KUPP_UNDER_HURTIG – "1" (default): varsle også hvis pris < hurtigpris

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

RABATT_MIN = float(os.getenv("KUPP_RABATT_MIN", "15") or 15)
UNDER_HURTIG = os.getenv("KUPP_UNDER_HURTIG", "1").strip() not in ("0", "false", "")
MAX_VARSLER = int(os.getenv("KUPP_MAX_VARSLER", "40") or 40)

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
    return f"{BASE_URL}&wheel_drive={drift_code}&page={page}"


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
                    "url": FINN_ITEM_URL.format(fk),
                }
        session.close()
    return list(biler.values())


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
# Varsling
# ======================================================

def _formater_bil(b: dict) -> str:
    def kr(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", " ") + " kr"
        except Exception:
            return "?"
    linje = (
        f"{(b.get('Merke') or '').strip()} {(b.get('Modell') or '').strip()}".strip()
        + f" ({b.get('Årstall') or '?'}, {kr(b.get('Kjørelengde')).replace(' kr', ' km')})\n"
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
        linje = (
            f"{navn} {b.get('Årstall') or '?'}, {kr(b.get('Kjørelengde'))} km\n"
            f"{kr(b.get('Pris'))} kr ({rab_s} mot {kr(b.get('forventet_pris'))})\n"
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


def _er_kupp(row) -> bool:
    forv = row.get("forventet_pris")
    if forv is None or pd.isna(forv) or forv <= 0:
        return False
    rab = row.get("rabatt_pct")
    if rab is not None and not pd.isna(rab) and float(rab) >= RABATT_MIN:
        return True
    if UNDER_HURTIG:
        hurtig = row.get("hurtigpris")
        pris = row.get("salgspris")
        if hurtig is not None and not pd.isna(hurtig) and pris is not None and float(pris) < float(hurtig):
            return True
    return False


def kjor(seed: bool = False, dry_run: bool = False) -> int:
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

    scoret = _score(nye)
    kupp = []
    if not scoret.empty:
        for _, row in scoret.iterrows():
            if _er_kupp(row):
                d = row.to_dict()
                # ta med url fra input (scorer beholder kolonnen)
                kupp.append(d)

    # Marker ALLE nye som sett (også de som ikke var kupp) så vi ikke re-vurderer.
    for b in nye:
        state[b["FinnKode"]] = naa

    kupp.sort(key=lambda d: (-(float(d.get("rabatt_pct") or 0))))
    kupp = kupp[:MAX_VARSLER]
    print(f"[kupp_vakt] {len(kupp)} kupp over terskel (rabatt >= {RABATT_MIN}%"
          + (" eller under hurtigpris" if UNDER_HURTIG else "") + ")")

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
    lagre_state(s3, state)
    return len(kupp)


def main():
    parser = argparse.ArgumentParser(description="Kupp-vakt: varsle om ferske underprisede biler.")
    parser.add_argument("--seed", action="store_true", help="Bare seed state, ingen varsler")
    parser.add_argument("--dry-run", action="store_true", help="Score og skriv ut, ikke send/lagre")
    args = parser.parse_args()
    antall = kjor(seed=args.seed, dry_run=args.dry_run)
    print(f"[kupp_vakt] Ferdig ({antall} kupp).")


if __name__ == "__main__":
    main()
