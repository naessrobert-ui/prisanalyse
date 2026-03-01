# -*- coding: utf-8 -*-
"""Regnskap-modul for prisanalyse.

Direkte port av Proff_all.py.
Kjernen: hent https://www.proff.no/regnskap/-/<orgnr> og parse
__NEXT_DATA__ JSON – identisk logikk som fungerer i CLI-skriptet.

OPPDATERT:
- Støtter "regnskapsperiode" (typisk "YYYY-MM", f.eks. "2024-12") i tillegg til year (int).
- Robust sortering på siste regnskap ved å utlede year fra periode/år-felt.
"""

from __future__ import annotations

import csv
import io
import json
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

import requests
import urllib3
from bs4 import BeautifulSoup
from flask import Blueprint, make_response, render_template, request, session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

regnskap_bp = Blueprint("regnskap", __name__, url_prefix="/regnskap")

# ---------------------------------------------------------------------------
# Konfig – identisk med Proff_all.py
# ---------------------------------------------------------------------------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 15
BATCH_WORKERS = 4

URL_TEMPLATES = [
    "https://www.proff.no/regnskap/-/{org}",
    "https://www.proff.no/regnskap/{org}",
]

# Proff søk
PROFF_SEARCH_URL = "https://www.proff.no/bransjesøk"

# Brreg – kun for navn-søk (søk på selskapsnavn i boks 3)
BRREG_SEARCH_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"
BRREG_REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"


# ---------------------------------------------------------------------------
# Dataklasser
# ---------------------------------------------------------------------------
@dataclass
class LookupResult:
    company: str
    orgnr: str
    year: int | None
    regnskapsperiode: str | None  # <-- NYTT (typisk "YYYY-MM", f.eks. "2024-12")
    resultat_etter_skatt: float | None
    egenkapital: float | None
    omsetning: float | None
    url_used: str
    debug_message: str = ""


@dataclass
class BatchRow:
    orgnr: str
    status: str
    company: str = ""
    year: int | None = None
    regnskapsperiode: str | None = None  # <-- NYTT
    omsetning: float | None = None
    resultat_etter_skatt: float | None = None
    egenkapital: float | None = None
    url_used: str = ""
    kilde: str = ""
    error: str = ""
    debug: str = ""


@dataclass
class FinancialDataset:
    company: str
    orgnr: str
    url_used: str
    currency: str
    records: list[dict[str, Any]]

    @property
    def columns(self) -> list[str]:
        fixed = ["period", "year"]
        dynamic = sorted(
            {
                key
                for rec in self.records
                for key in rec.keys()
                if key not in fixed
            }
        )
        return fixed + dynamic


# ---------------------------------------------------------------------------
# HTTP-session
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )
    return sess


# ---------------------------------------------------------------------------
# Kjernefunksjoner – kopiert direkte fra Proff_all.py (+ litt robusthet)
# ---------------------------------------------------------------------------
def normalize_orgnr(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip())


def format_amount(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:,.0f}".replace(",", "\u00a0")


def try_fetch_payload(sess: requests.Session, url: str) -> dict | None:
    """Identisk med Proff_all.py."""
    try:
        r = sess.get(url, verify=False, timeout=TIMEOUT)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__", "type": "application/json"})
    if not script or not script.string:
        return None
    try:
        return json.loads(script.string)
    except Exception:
        return None


def get_company_name(payload: dict) -> str:
    """Identisk med Proff_all.py."""
    company = payload.get("props", {}).get("pageProps", {}).get("company", {})
    for key in ("name", "companyName", "displayName"):
        v = company.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _parse_year_and_period(y: dict) -> tuple[int | None, str | None]:
    """
    Proff kan gi år/perioder litt ulikt. UI viser ofte "YYYY-MM" (f.eks. 2024-12).
    I JSON kan det ligge som:
      - y["year"] = 2024
      - y["year"] = "2024-12"
      - y["period"] = "2024-12"
    Vi returnerer:
      - year_int: int (for sortering)
      - period_str: original streng hvis vi finner den (for visning)
    """
    period = y.get("period") or y.get("year")
    period_str = None
    if period is not None:
        period_str = str(period).strip() or None

    year_int = None
    if period_str:
        m = re.match(r"^(\d{4})(?:-(\d{2}))?$", period_str)
        if m:
            year_int = int(m.group(1))
    else:
        # siste fallback
        yr = y.get("year")
        if isinstance(yr, int):
            year_int = yr

    return year_int, period_str


def extract_accounts_records(payload: dict) -> tuple[list[dict], str]:
    """Som Proff_all.py sin extract_accounts_df, uten pandas. Nå med 'period'."""
    company = payload["props"]["pageProps"].get("company", {})
    currency = company.get("currency", "NOK")
    accounts = company.get("companyAccounts", []) or []
    records = []

    for y in accounts:
        year_int, period_str = _parse_year_and_period(y)

        rec: dict[str, Any] = {
            "year": year_int,        # int for sortering
            "period": period_str,    # streng for visning (f.eks. "2024-12")
        }

        for a in y.get("accounts", []):
            code = a.get("code")
            amt = a.get("amount")
            if isinstance(amt, str):
                amt = amt.replace(" ", "").replace("\xa0", "")
            try:
                amt = float(amt)
            except Exception:
                pass
            if code:
                rec[code] = amt

        if rec.get("year") is not None:
            records.append(rec)

    return records, currency


def parse_proff_html_accounts(html: str) -> list[dict[str, Any]]:
    """Les ut alle perioder/felter fra Proff HTML-tabeller."""
    soup = BeautifulSoup(html, "html.parser")
    per_period: dict[str, dict[str, Any]] = {}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        periods = [
            c.get_text(strip=True)
            for c in header_cells[1:]
            if re.match(r"^\d{4}-\d{2}$", c.get_text(strip=True))
        ]
        if not periods:
            continue

        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) < len(periods) + 1:
                continue

            label = cells[0].get_text(strip=True).strip().lower()
            if not label or "valuta" in label or "startdato" in label or "sluttdato" in label:
                continue

            key = re.sub(r"[^a-z0-9_æøå]", "_", label)
            for idx, period in enumerate(periods):
                value_raw = (
                    cells[idx + 1]
                    .get_text(strip=True)
                    .replace("\xa0", "")
                    .replace(" ", "")
                    .replace(",", ".")
                )
                try:
                    # Proff-tabellen viser ofte beløp i tusen
                    value = float(value_raw) * 1000
                except ValueError:
                    value = None
                period_rec = per_period.setdefault(period, {"period": period, "year": int(period[:4])})
                period_rec[key] = value

    return list(per_period.values())


def build_dataset_from_payload(payload: dict, regnskap_url: str) -> FinancialDataset | None:
    records, currency = extract_accounts_records(payload)
    if not records:
        return None
    company_data = payload.get("props", {}).get("pageProps", {}).get("company", {})
    records_sorted = sorted(records, key=lambda r: (r.get("year", 0) or 0, str(r.get("period") or "")))
    return FinancialDataset(
        company=get_company_name(payload),
        orgnr=normalize_orgnr(company_data.get("orgNumber", "")),
        url_used=regnskap_url,
        currency=currency,
        records=records_sorted,
    )


def build_dataset_from_html(http_session: requests.Session, regnskap_url: str) -> FinancialDataset | None:
    try:
        resp = http_session.get(regnskap_url, timeout=TIMEOUT, verify=False)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None

    records = parse_proff_html_accounts(resp.text)
    if not records:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    org_candidates = re.findall(r"(\d{9})", regnskap_url)

    return FinancialDataset(
        company=title.split("|")[0].strip() if title else "",
        orgnr=org_candidates[-1] if org_candidates else "",
        url_used=regnskap_url,
        currency="NOK",
        records=sorted(records, key=lambda r: (r.get("year", 0) or 0, str(r.get("period") or ""))),
    )


def proff_resolve_regnskap_url(sess: requests.Session, orgnr: str) -> str | None:
    """
    To-stegs URL-resolving:
      1. GET bransjesøk?q=<orgnr>
      2. Finn companyId via (a) data-p-stats JSON, (b) /selskap/-href
      3. Bytt /selskap/ -> /regnskap/ og returner full URL
    """
    try:
        resp = sess.get(
            PROFF_SEARCH_URL,
            params={"q": orgnr},
            timeout=TIMEOUT,
            verify=False,
        )
        if resp.status_code != 200:
            return None

        html = resp.text

        # Metode 1: data-p-stats JSON inneholder companyId direkte
        m = re.search(
            r'(?:&quot;|")companyId(?:&quot;|")\s*:\s*(?:&quot;|")([A-Z0-9]+)(?:&quot;|")',
            html,
        )
        if m:
            company_id = m.group(1)
            # Finn full /selskap/-path for å beholde slug (navn/sted/bransje)
            slug_m = re.search(
                r'href="(/selskap/[^"]*' + re.escape(company_id) + r'[^"]*)"', html
            )
            if slug_m:
                path = slug_m.group(1).replace("/selskap/", "/regnskap/")
                return "https://www.proff.no" + path

            # Fallback: bygg minimal regnskap-URL med bare companyId
            return f"https://www.proff.no/regnskap/-/{orgnr}/{company_id}"

        # Metode 2: finn /selskap/-href direkte med regex (ofte nok)
        href_m = re.search(r'href="(/selskap/[^"]+)"', html)
        if href_m:
            path = href_m.group(1).replace("/selskap/", "/regnskap/")
            return "https://www.proff.no" + path

        # Metode 3: BeautifulSoup fallback
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/selskap/" in href:
                regnskap = href.replace("/selskap/", "/regnskap/")
                base = "https://www.proff.no"
                return base + regnskap if regnskap.startswith("/") else regnskap

    except Exception:
        pass
    return None


def _get_nested_amount(obj: dict, *keys: str) -> float | None:
    current: Any = obj
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if isinstance(current, (int, float)):
        return float(current)
    if isinstance(current, dict):
        amount = current.get("beloep")
        if isinstance(amount, (int, float)):
            return float(amount)
    return None


def lookup_orgnr_brreg(http_session: requests.Session, orgnr: str) -> LookupResult | None:
    """Hent siste tilgjengelige regnskap fra Brreg for gitt orgnr."""
    try:
        resp = http_session.get(
            f"{BRREG_REGNSKAP_URL}/{orgnr}",
            timeout=TIMEOUT,
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            return None
        payload = resp.json()
        if isinstance(payload, list):
            if not payload:
                return None
            payload = payload[0]
        if not isinstance(payload, dict):
            return None

        virksomhet = payload.get("virksomhet", {})
        periode = payload.get("regnskapsperiode", {})
        period_to = str(periode.get("tilDato", "")).strip()
        period_display = None
        if periode.get("fraDato") and periode.get("tilDato"):
            period_display = f"{periode.get('fraDato')} – {periode.get('tilDato')}"
        elif period_to:
            period_display = period_to

        year = None
        if period_to and re.match(r"^\d{4}", period_to):
            year = int(period_to[:4])

        company_name = (
            virksomhet.get("navn")
            or virksomhet.get("organisasjonsnavn")
            or ""
        )

        return LookupResult(
            company=company_name,
            orgnr=normalize_orgnr(virksomhet.get("organisasjonsnummer", orgnr)),
            year=year,
            regnskapsperiode=period_display,
            resultat_etter_skatt=_get_nested_amount(payload, "resultatregnskapResultat", "aarsresultat"),
            egenkapital=_get_nested_amount(payload, "egenkapitalGjeld", "egenkapital", "sumEgenkapital"),
            omsetning=_get_nested_amount(
                payload,
                "resultatregnskapResultat",
                "driftsresultat",
                "driftsinntekter",
                "sumDriftsinntekter",
            ),
            url_used=f"{BRREG_REGNSKAP_URL}/{orgnr}",
            debug_message="Kilde: Brreg regnskapsregisteret",
        )
    except Exception:
        return None


def lookup_proff_url_html(http_session: requests.Session, regnskap_url: str) -> LookupResult | None:
    """Parser Proff-tabeller direkte fra HTML (logikk fra regnskap_core.py)."""
    try:
        resp = http_session.get(regnskap_url, timeout=TIMEOUT, verify=False)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    per_period: dict[str, dict[str, float | None]] = {}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        periods = [
            c.get_text(strip=True)
            for c in header_cells[1:]
            if re.match(r"^\d{4}-\d{2}$", c.get_text(strip=True))
        ]
        if not periods:
            continue

        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) < len(periods) + 1:
                continue
            label = cells[0].get_text(strip=True).strip().lower()
            if not label or "valuta" in label or "startdato" in label or "sluttdato" in label:
                continue

            key = re.sub(r"[^a-z0-9_æøå]", "_", label)
            for idx, period in enumerate(periods):
                value_raw = (
                    cells[idx + 1]
                    .get_text(strip=True)
                    .replace("\xa0", "")
                    .replace(" ", "")
                    .replace(",", ".")
                )
                try:
                    # Proff-tabellen viser ofte beløp i tusen
                    value = float(value_raw) * 1000
                except ValueError:
                    value = None
                per_period.setdefault(period, {})[key] = value

    if not per_period:
        return None

    latest_period = max(per_period.keys())
    latest = per_period[latest_period]
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")

    org_candidates = re.findall(r"(\d{9})", regnskap_url)
    orgnr = org_candidates[-1] if org_candidates else ""

    return LookupResult(
        company=title.split("|")[0].strip() if title else "",
        orgnr=orgnr,
        year=int(latest_period[:4]),
        regnskapsperiode=latest_period,
        resultat_etter_skatt=latest.get("årsresultat") or latest.get("aarsresultat"),
        egenkapital=latest.get("sum_egenkapital") or latest.get("egenkapital"),
        omsetning=latest.get("sum_driftsinntekter") or latest.get("driftsinntekter"),
        url_used=regnskap_url,
        debug_message="Kilde: Proff HTML-tabell",
    )


def build_urls(org: str, url_hint: str | None = None) -> list[str]:
    """Fallback-URLer hvis to-stegs resolving feiler."""
    urls = []
    if url_hint and str(url_hint).strip():
        urls.append(str(url_hint).strip())
    for tpl in URL_TEMPLATES:
        urls.append(tpl.format(org=org))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ---------------------------------------------------------------------------
# Oppslag
# ---------------------------------------------------------------------------
def lookup_orgnr(
    http_session: requests.Session,
    orgnr: str,
    url_hint: str | None = None,
) -> LookupResult | None:
    payload = None
    used_url = None
    debug_parts: list[str] = []

    # Steg 1: to-stegs resolving via bransjesøk (gir riktig intern Proff-URL)
    resolved = proff_resolve_regnskap_url(http_session, orgnr)
    if resolved:
        p = try_fetch_payload(http_session, resolved)
        if p:
            payload = p
            used_url = resolved
        else:
            debug_parts.append(f"ingen payload fra resolved: {resolved}")

    # Steg 2: fallback til direkte URL-templates
    if not payload:
        for url in build_urls(orgnr, url_hint):
            p = try_fetch_payload(http_session, url)
            if p:
                payload = p
                used_url = url
                break
            debug_parts.append(f"ingen payload: {url}")

    if not payload:
        return None

    records, _ = extract_accounts_records(payload)
    if not records:
        return None

    # "Siste" regnskap: sorter på year (int).
    latest = max(records, key=lambda r: r.get("year", 0) or 0)
    year = latest.get("year")
    period = latest.get("period")

    company_data = payload.get("props", {}).get("pageProps", {}).get("company", {})
    found_orgnr = normalize_orgnr(company_data.get("orgNumber", orgnr))

    return LookupResult(
        company=get_company_name(payload),
        orgnr=found_orgnr,
        year=year,
        regnskapsperiode=period,
        resultat_etter_skatt=latest.get("AR"),
        egenkapital=latest.get("SEK"),
        omsetning=latest.get("SDI"),
        url_used=used_url or "",
        debug_message="; ".join(debug_parts),
    )


# ---------------------------------------------------------------------------
# Søk: navn → orgnr (Brreg Enhetsregisteret)
# ---------------------------------------------------------------------------
def search_to_orgnr(http_session: requests.Session, query: str) -> str | None:
    normalized = normalize_orgnr(query)
    if len(normalized) == 9:
        return normalized
    try:
        resp = http_session.get(
            BRREG_SEARCH_URL,
            params={"navn": query, "size": 5},
            timeout=TIMEOUT,
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 200:
            hits = resp.json().get("_embedded", {}).get("enheter", [])
            if hits:
                return str(hits[0].get("organisasjonsnummer", ""))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Fil-parsing (xlsx / txt / csv)
# ---------------------------------------------------------------------------
def parse_xlsx_values(content: bytes) -> list[str]:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values: list[str] = []
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root.findall("x:si", ns):
                text = "".join(n.text or "" for n in si.findall(".//x:t", ns))
                shared.append(text)
        if "xl/worksheets/sheet1.xml" not in archive.namelist():
            return []
        sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
        for cell in sheet.findall(".//x:c", ns):
            ctype = cell.attrib.get("t")
            vnode = cell.find("x:v", ns)
            inode = cell.find("x:is/x:t", ns)
            if ctype == "s" and vnode is not None:
                try:
                    values.append(shared[int(vnode.text or "")])
                except Exception:
                    continue
            elif inode is not None and inode.text:
                values.append(inode.text)
            elif vnode is not None and vnode.text:
                values.append(vnode.text)
    return values


def parse_orgnrs_from_file(file_storage) -> list[str]:
    content = file_storage.read()
    filename = (file_storage.filename or "").lower()
    if filename.endswith(".xlsx"):
        raw = parse_xlsx_values(content)
    else:
        raw = re.split(r"[\s,;]+", content.decode("utf-8", errors="ignore"))
    orgnrs = []
    for item in raw:
        org = normalize_orgnr(item)
        if len(org) == 9:
            orgnrs.append(org)
    return list(dict.fromkeys(orgnrs))


# ---------------------------------------------------------------------------
# Presentasjon
# ---------------------------------------------------------------------------

def _pick_metric(rec: dict[str, Any], *candidates: str) -> float | None:
    for key in candidates:
        value = rec.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def build_chart_series(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    points_omsetning: list[dict[str, Any]] = []
    points_resultat_for_skatt: list[dict[str, Any]] = []
    points_margin: list[dict[str, Any]] = []

    for rec in records:
        label = rec.get("period") or str(rec.get("year") or "")
        omsetning = _pick_metric(rec, "SDI", "sum_driftsinntekter", "driftsinntekter")
        resultat_for_skatt = _pick_metric(
            rec,
            "ORFS",
            "OR", 
            "resultat_før_skattekostnad",
            "resultat_for_skattekostnad",
            "resultat_før_skatt",
            "resultat_for_skatt",
        )
        if omsetning is not None:
            points_omsetning.append({"label": label, "value": omsetning})
        if resultat_for_skatt is not None:
            points_resultat_for_skatt.append({"label": label, "value": resultat_for_skatt})
        if omsetning and resultat_for_skatt is not None:
            points_margin.append({"label": label, "value": (resultat_for_skatt / omsetning) * 100})

    return {
        "omsetning": points_omsetning,
        "resultat_for_skatt": points_resultat_for_skatt,
        "margin": points_margin,
    }


def dataset_to_session_payload(dataset: FinancialDataset) -> dict[str, Any]:
    return {
        "company": dataset.company,
        "orgnr": dataset.orgnr,
        "url_used": dataset.url_used,
        "currency": dataset.currency,
        "records": dataset.records,
    }


def dataset_from_session_payload(raw: dict[str, Any]) -> FinancialDataset | None:
    if not raw:
        return None
    records = raw.get("records")
    if not isinstance(records, list) or not records:
        return None
    return FinancialDataset(
        company=str(raw.get("company", "")),
        orgnr=str(raw.get("orgnr", "")),
        url_used=str(raw.get("url_used", "")),
        currency=str(raw.get("currency", "NOK")),
        records=records,
    )
def build_detail_rows(result: LookupResult) -> list[tuple[str, str]]:
    return [
        ("Selskap", result.company),
        ("Organisasjonsnummer", result.orgnr),
        ("Regnskapsperiode", result.regnskapsperiode or "Ukjent"),
        ("Siste regnskapsår", str(result.year) if result.year else "Ukjent"),
        ("Sum driftsinntekter", format_amount(result.omsetning) or "Mangler"),
        ("Resultat etter skatt", format_amount(result.resultat_etter_skatt) or "Mangler"),
        ("Egenkapital", format_amount(result.egenkapital) or "Mangler"),
        ("URL", result.url_used),
    ]


def serialize_batch_rows(rows: list[BatchRow]) -> list[dict[str, Any]]:
    return [row.__dict__ for row in rows]


def deserialize_batch_rows(raw: list[dict[str, Any]]) -> list[BatchRow]:
    return [BatchRow(**r) for r in raw]


# ---------------------------------------------------------------------------
# Resultatvisning
# ---------------------------------------------------------------------------
def render_regnskap_result_page(
    *,
    mode: str,
    title: str,
    batch_results: list[BatchRow] | None = None,
    batch_error: str = "",
    batch_summary: str = "",
    url_details: list[tuple[str, str]] | None = None,
    url_error: str = "",
    url_dataset: FinancialDataset | None = None,
    search_details: list[tuple[str, str]] | None = None,
    search_error: str = "",
    search_query: str = "",
) -> str:
    return render_template(
        "regnskap_result.html",
        mode=mode,
        title=title,
        batch_results=batch_results or [],
        batch_error=batch_error,
        batch_summary=batch_summary,
        url_details=url_details or [],
        url_error=url_error,
        search_details=search_details or [],
        search_error=search_error,
        search_query=search_query,
        url_dataset=url_dataset,
        chart_series=(build_chart_series(url_dataset.records) if url_dataset else {}),
        format_amount=format_amount,
    )


# ---------------------------------------------------------------------------
# Flask-ruter
# ---------------------------------------------------------------------------
@regnskap_bp.route("/batch-download.csv")
def batch_download_csv():
    rows = deserialize_batch_rows(session.get("regnskap_batch_rows", []))
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")
    writer.writerow(
        [
            "orgnr",
            "status",
            "selskap",
            "regnskapsperiode",
            "år",
            "omsetning",
            "resultat_etter_skatt",
            "egenkapital",
            "url",
            "feil",
            "debug",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.orgnr,
                row.status,
                row.company,
                row.regnskapsperiode or "",
                row.year or "",
                format_amount(row.omsetning),
                format_amount(row.resultat_etter_skatt),
                format_amount(row.egenkapital),
                row.url_used,
                row.error,
                row.debug,
            ]
        )
    csv_content = "\ufeff" + buffer.getvalue()
    resp = make_response(csv_content)
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=regnskap_resultater.csv"
    return resp


@regnskap_bp.route("/detailed-download.csv")
def detailed_download_csv():
    dataset = dataset_from_session_payload(session.get("regnskap_url_dataset", {}))
    if not dataset:
        resp = make_response("Ingen detaljer å laste ned.")
        resp.status_code = 400
        return resp

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")
    writer.writerow(dataset.columns)
    for rec in dataset.records:
        row = []
        for col in dataset.columns:
            val = rec.get(col, "")
            if isinstance(val, float):
                row.append(f"{val:.2f}")
            else:
                row.append(val)
        writer.writerow(row)

    csv_content = "﻿" + buffer.getvalue()
    resp = make_response(csv_content)
    filename_org = dataset.orgnr or "ukjent"
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f"attachment; filename=regnskap_detaljer_{filename_org}.csv"
    return resp


@regnskap_bp.route("/", methods=["GET", "POST"])
def regnskap_hub():
    http_session = make_session()

    batch_results: list[BatchRow] = []
    batch_error = ""
    batch_summary = ""
    url_details: list[tuple[str, str]] = []
    url_error = ""
    url_dataset: FinancialDataset | None = None
    search_details: list[tuple[str, str]] = []
    search_error = ""

    if request.method == "POST":
        action = request.form.get("action", "")

        # ---- BATCH --------------------------------------------------------
        if action == "batch":
            upload = request.files.get("orgnr_file")
            if not upload or not upload.filename:
                batch_error = "Velg en fil med organisasjonsnumre først."
            else:
                orgnrs = parse_orgnrs_from_file(upload)
                if not orgnrs:
                    batch_error = "Fant ingen gyldige 9-sifrede orgnr i filen (støtter txt/csv/xlsx)."
                else:
                    t0 = time.perf_counter()
                    orgnrs_batch = orgnrs[:100]

                    def fetch_one(org: str) -> tuple[str, LookupResult | None]:
                        s = make_session()
                        return org, lookup_orgnr_brreg(s, org)

                    results_map: dict[str, LookupResult | None] = {}
                    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as ex:
                        futures = {ex.submit(fetch_one, org): org for org in orgnrs_batch}
                        for future in as_completed(futures):
                            org, res = future.result()
                            results_map[org] = res

                    for org in orgnrs_batch:
                        res = results_map.get(org)
                        if res:
                            batch_results.append(
                                BatchRow(
                                    orgnr=res.orgnr,
                                    status="OK",
                                    company=res.company,
                                    year=res.year,
                                    regnskapsperiode=res.regnskapsperiode,
                                    omsetning=res.omsetning,
                                    resultat_etter_skatt=res.resultat_etter_skatt,
                                    egenkapital=res.egenkapital,
                                    url_used=res.url_used,
                                    kilde="Brreg",
                                    debug=res.debug_message,
                                )
                            )
                        else:
                            # lookup_orgnr returnerte None - prøv resolve på nytt bare for debug
                            _dbg_resolved = proff_resolve_regnskap_url(http_session, org)
                            batch_results.append(
                                BatchRow(
                                    orgnr=org,
                                    status="FEIL",
                                    error="Fant ikke regnskapsdata i Brreg.",
                                    debug=f"resolved={_dbg_resolved or 'ingen'}; Brreg ga ingen data",
                                )
                            )

                    dur = time.perf_counter() - t0
                    ok = sum(1 for r in batch_results if r.status == "OK")
                    batch_summary = (
                        f"Ferdig: {len(batch_results)} behandlet · "
                        f"{ok} treff · {len(batch_results)-ok} uten treff · {dur:.1f} sek"
                    )
                    session["regnskap_batch_rows"] = serialize_batch_rows(batch_results)

        # ---- URL ----------------------------------------------------------
        elif action == "url":
            session.pop("regnskap_url_dataset", None)
            regnskap_url = (request.form.get("proff_url") or "").strip()
            if not regnskap_url:
                url_error = "Skriv inn en URL."
            else:
                payload = try_fetch_payload(http_session, regnskap_url)
                if payload:
                    url_dataset = build_dataset_from_payload(payload, regnskap_url)
                if not url_dataset:
                    url_dataset = build_dataset_from_html(http_session, regnskap_url)

                if url_dataset:
                    latest = max(url_dataset.records, key=lambda r: r.get("year", 0) or 0)
                    res = LookupResult(
                        company=url_dataset.company,
                        orgnr=url_dataset.orgnr,
                        year=latest.get("year"),
                        regnskapsperiode=latest.get("period"),
                        resultat_etter_skatt=latest.get("AR"),
                        egenkapital=latest.get("SEK"),
                        omsetning=latest.get("SDI"),
                        url_used=regnskap_url,
                    )
                    url_details = build_detail_rows(res)
                    session["regnskap_url_dataset"] = dataset_to_session_payload(url_dataset)
                else:
                    # Trekk ut orgnr fra URL og prøv standard templates
                    candidates = re.findall(r"(\d{9})", regnskap_url)
                    if candidates:
                        res = lookup_orgnr(http_session, candidates[-1], regnskap_url)
                        if res:
                            url_details = build_detail_rows(res)
                        else:
                            url_error = "Klarte ikke hente regnskapstall. Sjekk at URL er en Proff regnskap-side."
                    else:
                        url_error = "Ingen __NEXT_DATA__ på siden og ingen orgnr funnet i URL."

        # ---- SEARCH -------------------------------------------------------
        elif action == "search":
            query = (request.form.get("search_query") or "").strip()
            if not query:
                search_error = "Skriv inn søk (navn eller orgnr)."
            else:
                orgnr = search_to_orgnr(http_session, query)
                if not orgnr:
                    search_error = "Fant ingen selskaper fra søket."
                else:
                    res = lookup_orgnr_brreg(http_session, orgnr)
                    if res:
                        search_details = build_detail_rows(res)
                    else:
                        search_error = f"Fant orgnr {orgnr}, men Brreg hadde ingen regnskapsdata."

            return render_regnskap_result_page(
                mode="search",
                title="Søkeresultat",
                search_details=search_details,
                search_error=search_error,
                search_query=query,
            )

        if action == "batch":
            return render_regnskap_result_page(
                mode="batch",
                title="Batch-resultat",
                batch_results=batch_results,
                batch_error=batch_error,
                batch_summary=batch_summary,
            )

        if action == "url":
            return render_regnskap_result_page(
                mode="url",
                title="Resultat fra Proff-URL",
                url_details=url_details,
                url_error=url_error,
                url_dataset=url_dataset,
            )

    return render_template("regnskap_hub.html")
