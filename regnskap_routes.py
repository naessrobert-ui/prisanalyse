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
import difflib
import io
import json
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from xml.etree import ElementTree as ET

import requests
import urllib3
from bs4 import BeautifulSoup
from flask import Blueprint, jsonify, make_response, render_template, request, session, url_for
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


def _build_analysis_api_candidates() -> list[str]:
    explicit = (os.environ.get("ANALYSIS_API_URL") or "").strip().rstrip("/")
    if explicit:
        return [explicit]

    candidates: list[str] = []
    port = (os.environ.get("PORT") or "8000").strip() or "8000"
    for base in (
        f"http://127.0.0.1:{port}",
        "http://127.0.0.1:8010",
        "http://192.168.86.30:8010",
    ):
        normalized = base.rstrip("/")
        if normalized not in candidates:
            candidates.append(normalized)
    return candidates


ANALYSIS_API_CANDIDATES = _build_analysis_api_candidates()
ANALYSIS_API_URL = ANALYSIS_API_CANDIDATES[0]


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
    resultat_for_skatt: float | None = None
    driftsresultat: float | None = None
    egenkapital: float | None = None
    sum_eiendeler: float | None = None
    omsetning: float | None = None
    url_used: str = ""
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
    field_labels: dict[str, str] = field(default_factory=dict)

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


FIELD_LABELS: dict[str, str] = {
    "period": "Regnskapsperiode",
    "year": "Regnskapsår",
    "SDI": "Omsetning",
    "ORFS": "Resultat før skatt",
    "OR": "Ordinært resultat",
    "DRR": "Driftsresultat",
    "ODR": "Driftsresultat (EBIT)",
    "DRI": "Driftsresultat (EBIT)",
    "AARS": "Resultat før skatt",
    "AR": "Årsresultat",
    "SEK": "Sum egenkapital",
    "sum_driftsinntekter": "Omsetning",
    "driftsinntekter": "Driftsinntekter",
    "driftsresultat": "Driftsresultat (EBIT)",
    "resultat_før_skattekostnad": "Resultat før skatt",
    "resultat_for_skattekostnad": "Resultat før skatt",
    "resultat_før_skatt": "Resultat før skatt",
    "resultat_for_skatt": "Resultat før skatt",
    "årsresultat": "Årsresultat",
    "aarsresultat": "Årsresultat",
    "sum_egenkapital": "Sum egenkapital",
    "egenkapital": "Egenkapital",
}


# ---------------------------------------------------------------------------
# HTTP-session
# ---------------------------------------------------------------------------
def make_session(use_retries: bool = True) -> requests.Session:
    sess = requests.Session()
    if use_retries:
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
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{num:,.0f}".replace(",", "\u00a0")


def format_percent(value: float | None) -> str:
    if value is None:
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{num:,.1f}".replace(",", "\u00a0").replace(".", ",") + " %"


def first_present(data: dict[str, Any] | None, *keys: str) -> Any:
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


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


def extract_accounts_records(payload: dict) -> tuple[list[dict], str, dict[str, str]]:
    """Som Proff_all.py sin extract_accounts_df, uten pandas. Nå med 'period'."""
    company = payload["props"]["pageProps"].get("company", {})
    currency = company.get("currency", "NOK")
    accounts = company.get("companyAccounts", []) or []
    records = []
    dynamic_labels: dict[str, str] = {}

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
                for label_key in (
                    "label",
                    "labelNb",
                    "labelNo",
                    "name",
                    "nameNb",
                    "nameNo",
                    "displayName",
                    "displayNameNb",
                    "description",
                    "accountDescription",
                    "title",
                    "accountName",
                    "text",
                ):
                    label_val = a.get(label_key)
                    if isinstance(label_val, str) and label_val.strip() and label_val.strip() != str(code):
                        dynamic_labels.setdefault(str(code), label_val.strip())
                        break

        if rec.get("year") is not None:
            records.append(rec)

    return records, currency, dynamic_labels


def parse_proff_html_accounts(html: str) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Les ut alle perioder/felter fra Proff HTML-tabeller."""
    soup = BeautifulSoup(html, "html.parser")
    per_period: dict[str, dict[str, Any]] = {}
    field_labels: dict[str, str] = {}

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

            raw_label = cells[0].get_text(strip=True).strip()
            label = raw_label.lower()
            if not label or "valuta" in label or "startdato" in label or "sluttdato" in label:
                continue

            key = re.sub(r"[^a-z0-9_æøå]", "_", label)
            if key:
                field_labels.setdefault(key, raw_label)
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

    return list(per_period.values()), field_labels


def build_dataset_from_payload(payload: dict, regnskap_url: str) -> FinancialDataset | None:
    extracted = extract_accounts_records(payload)
    if len(extracted) == 3:
        records, currency, dynamic_labels = extracted
    else:
        records, currency = extracted  # bakoverkompatibilitet ved eldre returverdi
        dynamic_labels = {}
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
        field_labels=dynamic_labels,
    )


def build_dataset_from_html(http_session: requests.Session, regnskap_url: str) -> FinancialDataset | None:
    try:
        resp = http_session.get(regnskap_url, timeout=TIMEOUT, verify=False)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None

    records, field_labels = parse_proff_html_accounts(resp.text)
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
        field_labels=field_labels,
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


def lookup_orgnr_brreg(http_session: requests.Session, orgnr: str, timeout: int = TIMEOUT) -> LookupResult | None:
    """Hent siste tilgjengelige regnskap fra Brreg for gitt orgnr."""
    try:
        resp = http_session.get(
            f"{BRREG_REGNSKAP_URL}/{orgnr}",
            timeout=timeout,
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
            resultat_for_skatt=_get_nested_amount(payload, "resultatregnskapResultat", "resultatFoerSkattekostnad"),
            driftsresultat=_get_nested_amount(payload, "resultatregnskapResultat", "driftsresultat", "driftsresultat"),
            egenkapital=_get_nested_amount(payload, "egenkapitalGjeld", "egenkapital", "sumEgenkapital"),
            sum_eiendeler=_get_nested_amount(payload, "eiendeler", "sumEiendeler"),
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
            raw_label = cells[0].get_text(strip=True).strip()
            label = raw_label.lower()
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
        resultat_for_skatt=latest.get("resultat_før_skatt") or latest.get("resultat_for_skatt"),
        driftsresultat=latest.get("driftsresultat"),
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

    extracted = extract_accounts_records(payload)
    records = extracted[0]
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
        resultat_for_skatt=latest.get("ORFS") or latest.get("AARS"),
        driftsresultat=latest.get("DRR") or latest.get("DRI") or latest.get("ODR"),
        egenkapital=latest.get("SEK"),
        sum_eiendeler=latest.get("SUME") or latest.get("SA"),
        omsetning=latest.get("SDI"),
        url_used=used_url or "",
        debug_message="; ".join(debug_parts),
    )


# ---------------------------------------------------------------------------
# Søk: navn → orgnr (Brreg Enhetsregisteret + Proff fallback)
# ---------------------------------------------------------------------------
def _normalize_company_name(value: str) -> str:
    cleaned = (value or "").lower()
    cleaned = re.sub(r"\b(as|asa|ans|nuf|sa|d[aå])\b", " ", cleaned)
    cleaned = re.sub(r"[^a-z0-9æøå]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _score_brreg_hit(hit: dict[str, Any], query_normalized: str) -> float:
    name = str(hit.get("navn") or "")
    name_normalized = _normalize_company_name(name)
    status = str(hit.get("slettedato") or "").strip()

    query_tokens = set(query_normalized.split())
    name_tokens = set(name_normalized.split())

    score = 0.0
    if name_normalized == query_normalized:
        score += 120
    elif name_normalized.startswith(query_normalized):
        score += 80
    elif query_normalized and query_normalized in name_normalized:
        score += 45

    # Token-overlapp gir bedre treff på delvis navn
    if query_tokens:
        overlap = len(query_tokens & name_tokens) / len(query_tokens)
        score += overlap * 50

    # Ligner stavemåte? (f.eks. små skrivefeil)
    score += difflib.SequenceMatcher(None, query_normalized, name_normalized).ratio() * 35

    # Foretrekk aktive selskaper
    if not status:
        score += 15

    # Svak bonus for lengde-likhet (reduserer feil på korte, tvetydige søk)
    score -= abs(len(name_normalized) - len(query_normalized)) * 0.3
    return score


def _search_to_orgnr_proff(http_session: requests.Session, query: str) -> str | None:
    """Fallback: finn orgnr fra Proff sin søkeside når Brreg-navnesøk bommer."""
    try:
        resp = http_session.get(
            PROFF_SEARCH_URL,
            params={"q": query},
            timeout=TIMEOUT,
            verify=False,
        )
        if resp.status_code != 200:
            return None

        # Treffer ofte både i href og i innebygget data-attributter.
        candidates = re.findall(r"\b(\d{9})\b", resp.text)
        for cand in candidates:
            if len(cand) == 9:
                return cand
    except Exception:
        return None
    return None


def search_to_orgnr(http_session: requests.Session, query: str) -> str | None:
    normalized = normalize_orgnr(query)
    if len(normalized) == 9:
        return normalized

    query_normalized = _normalize_company_name(query)
    if not query_normalized:
        return None

    all_hits: list[dict[str, Any]] = []
    for navn_query in [query, query_normalized]:
        try:
            resp = http_session.get(
                BRREG_SEARCH_URL,
                params={"navn": navn_query, "size": 100},
                timeout=TIMEOUT,
                headers={"Accept": "application/json"},
            )
            if resp.status_code == 200:
                hits = resp.json().get("_embedded", {}).get("enheter", [])
                if isinstance(hits, list):
                    all_hits.extend(hits)
        except Exception:
            continue

    if all_hits:
        ranked = sorted(
            all_hits,
            key=lambda hit: _score_brreg_hit(hit, query_normalized),
            reverse=True,
        )
        for hit in ranked[:5]:
            best_orgnr = normalize_orgnr(str(hit.get("organisasjonsnummer", "")))
            if len(best_orgnr) == 9:
                return best_orgnr

    return _search_to_orgnr_proff(http_session, query)


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


def build_column_metadata(columns: list[str], field_labels: dict[str, str] | None = None) -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    field_labels = field_labels or {}
    for col in columns:
        label = field_labels.get(col) or FIELD_LABELS.get(col)
        if not label:
            label = col.replace("_", " ").strip().capitalize()
        metadata[col] = {
            "short": col,
            "full": label,
        }
    return metadata


def _keys_matching_labels(field_labels: dict[str, str], *terms: str) -> list[str]:
    if not field_labels:
        return []
    matches: list[str] = []
    for key, label in field_labels.items():
        norm = (label or "").strip().lower()
        if any(term in norm for term in terms):
            matches.append(key)
    return matches


def build_chart_series(
    records: list[dict[str, Any]],
    field_labels: dict[str, str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    points_omsetning: list[dict[str, Any]] = []
    points_resultat_for_skatt: list[dict[str, Any]] = []
    points_resultatmargin: list[dict[str, Any]] = []
    field_labels = field_labels or {}

    omsetning_candidates = ["SDI", "sum_driftsinntekter", "driftsinntekter", *_keys_matching_labels(field_labels, "omsetning", "driftsinntekt")]
    resultat_candidates = [
        "ORFS",
        "OR",
        "AARS",
        "resultat_før_skattekostnad",
        "resultat_for_skattekostnad",
        "resultat_før_skatt",
        "resultat_for_skatt",
        *_keys_matching_labels(field_labels, "før skatt", "foer skatt", "skattekostnad"),
    ]

    for rec in records:
        label = rec.get("period") or str(rec.get("year") or "")
        omsetning = _pick_metric(rec, *omsetning_candidates)
        resultat_for_skatt = _pick_metric(rec, *resultat_candidates)
        if omsetning is not None:
            points_omsetning.append({"label": label, "value": omsetning})
        if resultat_for_skatt is not None:
            points_resultat_for_skatt.append({"label": label, "value": resultat_for_skatt})
        if omsetning not in (None, 0) and resultat_for_skatt is not None:
            points_resultatmargin.append({"label": label, "value": (resultat_for_skatt / omsetning) * 100})

    return {
        "omsetning": points_omsetning,
        "resultat_for_skatt": points_resultat_for_skatt,
        "resultatmargin": points_resultatmargin,
    }


def dataset_to_session_payload(dataset: FinancialDataset) -> dict[str, Any]:
    return {
        "company": dataset.company,
        "orgnr": dataset.orgnr,
        "url_used": dataset.url_used,
        "currency": dataset.currency,
        "records": dataset.records,
        "field_labels": dataset.field_labels,
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
        field_labels=(raw.get("field_labels") if isinstance(raw.get("field_labels"), dict) else {}),
    )


def build_brreg_annual_report_url(orgnr: str, year: int | None = None) -> str:
    del year  # Vi bruker virksomhet-oppslag som stabil og riktig lenke i UI.
    normalized = normalize_orgnr(orgnr)
    if len(normalized) != 9:
        return ""
    return f"https://virksomhet.brreg.no/nb/oppslag/enheter/{normalized}"


def build_metric_comparison(analysis_row: dict[str, Any], brreg_result: LookupResult | None) -> list[dict[str, Any]]:
    if not analysis_row or brreg_result is None:
        return []

    def _to_float(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).replace(" ", "").replace(" ", "").replace(",", "."))
        except (TypeError, ValueError):
            return None

    analysis_values = {
        "sum_driftsinntekter": _to_float(analysis_row.get("sum_driftsinntekter") if analysis_row.get("sum_driftsinntekter") is not None else analysis_row.get("omsetning")),
        "driftsresultat": _to_float(analysis_row.get("driftsresultat")),
        "aarsresultat": _to_float(analysis_row.get("aarsresultat")),
        "resultat_for_skatt": _to_float(
            analysis_row.get("resultat_for_skattekostnad")
            if analysis_row.get("resultat_for_skattekostnad") is not None
            else analysis_row.get("resultat_for_skatt")
        ),
        "sum_egenkapital": _to_float(analysis_row.get("sum_egenkapital") if analysis_row.get("sum_egenkapital") is not None else analysis_row.get("egenkapital")),
        "sum_eiendeler": _to_float(analysis_row.get("sum_eiendeler")),
    }

    brreg_values = {
        "sum_driftsinntekter": brreg_result.omsetning,
        "driftsresultat": brreg_result.driftsresultat,
        "aarsresultat": brreg_result.resultat_etter_skatt,
        "resultat_for_skatt": brreg_result.resultat_for_skatt,
        "sum_egenkapital": brreg_result.egenkapital,
        "sum_eiendeler": brreg_result.sum_eiendeler,
    }

    labels = {
        "sum_driftsinntekter": "Sum driftsinntekter",
        "driftsresultat": "Driftsresultat",
        "aarsresultat": "Årsresultat",
        "resultat_for_skatt": "Resultat før skatt",
        "sum_egenkapital": "Sum egenkapital",
        "sum_eiendeler": "Sum eiendeler",
    }

    rows: list[dict[str, Any]] = []
    for key, label in labels.items():
        analysis_value = analysis_values.get(key)
        brreg_value = brreg_values.get(key)
        equal = None
        diff = None
        if analysis_value is not None and brreg_value is not None:
            diff = analysis_value - brreg_value
            equal = abs(diff) < 0.5
        rows.append(
            {
                "label": label,
                "analysis_value": analysis_value,
                "brreg_value": brreg_value,
                "equal": equal,
                "diff": diff,
            }
        )
    return rows


def build_detail_rows(result: LookupResult) -> list[tuple[str, str]]:
    brreg_download_url = build_brreg_annual_report_url(result.orgnr, result.year)
    return [
        ("Selskap", result.company),
        ("Organisasjonsnummer", result.orgnr),
        ("Regnskapsperiode", result.regnskapsperiode or "Ukjent"),
        ("Siste regnskapsår", str(result.year) if result.year else "Ukjent"),
        ("Sum driftsinntekter", format_amount(result.omsetning) or "Mangler"),
        ("Driftsresultat", format_amount(result.driftsresultat) or "Mangler"),
        ("Resultat før skatt", format_amount(result.resultat_for_skatt) or "Mangler"),
        ("Resultat etter skatt", format_amount(result.resultat_etter_skatt) or "Mangler"),
        ("Egenkapital", format_amount(result.egenkapital) or "Mangler"),
        ("Sum eiendeler", format_amount(result.sum_eiendeler) or "Mangler"),
        ("Last ned siste års regnskap", brreg_download_url or "Mangler"),
        ("Kilde-URL", result.url_used),
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
        column_metadata=(build_column_metadata(url_dataset.columns, url_dataset.field_labels) if url_dataset else {}),
        chart_series=(build_chart_series(url_dataset.records, url_dataset.field_labels) if url_dataset else {}),
        format_amount=format_amount,
    )


def _lookup_from_query(http_session: requests.Session, query: str) -> tuple[LookupResult | None, str | None]:
    orgnr = search_to_orgnr(http_session, query)
    if not orgnr:
        return None, "Fant ingen selskaper fra søket."

    result = lookup_orgnr_brreg(http_session, orgnr)
    if not result:
        result = lookup_orgnr(http_session, orgnr)

    if not result:
        return None, f"Fant orgnr {orgnr}, men ingen regnskapsdata i verken Brreg eller Proff."

    return result, None


# ---------------------------------------------------------------------------
# Analysis API-proxy
# ---------------------------------------------------------------------------




def _proxy_analysis_api_locally(path: str, params: dict[str, Any] | None = None):
    try:
        from analysis_api_compat import (
            get_company_history_payload,
            get_analysis_health_payload,
            get_companies_filter_meta_payload,
            get_companies_filter_payload,
            get_companies_filter_summary_payload,
            get_companies_top_omsetning_payload,
            get_industry_suggest_payload,
            get_industry_overview_payload,
        )
    except Exception:
        return None

    params = params or {}
    if path == "/analysis-api/health":
        payload = get_analysis_health_payload()
    elif path == "/analysis-api/companies/filter/meta":
        payload = get_companies_filter_meta_payload()
    elif path == "/analysis-api/companies/filter":
        payload = get_companies_filter_payload(
            q=params.get("q"),
            kommune=params.get("kommune"),
            naeringskode=params.get("naeringskode"),
            adresse=params.get("adresse"),
            min_omsetning=float(params["min_omsetning"]) if params.get("min_omsetning") else None,
            max_omsetning=float(params["max_omsetning"]) if params.get("max_omsetning") else None,
            min_resultat=float(params["min_resultat"]) if params.get("min_resultat") else None,
            max_resultat=float(params["max_resultat"]) if params.get("max_resultat") else None,
            min_egenkapitalandel=float(params["min_egenkapitalandel"]) if params.get("min_egenkapitalandel") else None,
            min_netto_margin=float(params["min_netto_margin"]) if params.get("min_netto_margin") else None,
            min_ansatte=int(params["min_ansatte"]) if params.get("min_ansatte") else None,
            max_ansatte=int(params["max_ansatte"]) if params.get("max_ansatte") else None,
            min_omsetning_per_ansatt=float(params["min_omsetning_per_ansatt"]) if params.get("min_omsetning_per_ansatt") else None,
            max_omsetning_per_ansatt=float(params["max_omsetning_per_ansatt"]) if params.get("max_omsetning_per_ansatt") else None,
            min_resultat_per_ansatt=float(params["min_resultat_per_ansatt"]) if params.get("min_resultat_per_ansatt") else None,
            max_resultat_per_ansatt=float(params["max_resultat_per_ansatt"]) if params.get("max_resultat_per_ansatt") else None,
            orgform=params.get("orgform"),
            has_regnskap=str(params.get("has_regnskap") or "").strip().lower() in {"1", "true", "yes", "on"},
            regnskapsaar=int(params["regnskapsaar"]) if params.get("regnskapsaar") else None,
            innlevert_etter=params.get("innlevert_etter") or None,
            limit=int(params.get("limit") or 100),
            offset=int(params.get("offset") or 0),
            sort_by=params.get("sort_by") or "omsetning",
            sort_dir=params.get("sort_dir") or "desc",
        )
    elif path == "/analysis-api/companies/filter/summary":
        payload = get_companies_filter_summary_payload(
            q=params.get("q"),
            kommune=params.get("kommune"),
            naeringskode=params.get("naeringskode"),
            adresse=params.get("adresse"),
            min_omsetning=float(params["min_omsetning"]) if params.get("min_omsetning") else None,
            max_omsetning=float(params["max_omsetning"]) if params.get("max_omsetning") else None,
            min_resultat=float(params["min_resultat"]) if params.get("min_resultat") else None,
            max_resultat=float(params["max_resultat"]) if params.get("max_resultat") else None,
            min_egenkapitalandel=float(params["min_egenkapitalandel"]) if params.get("min_egenkapitalandel") else None,
            min_netto_margin=float(params["min_netto_margin"]) if params.get("min_netto_margin") else None,
            min_ansatte=int(params["min_ansatte"]) if params.get("min_ansatte") else None,
            max_ansatte=int(params["max_ansatte"]) if params.get("max_ansatte") else None,
            min_omsetning_per_ansatt=float(params["min_omsetning_per_ansatt"]) if params.get("min_omsetning_per_ansatt") else None,
            max_omsetning_per_ansatt=float(params["max_omsetning_per_ansatt"]) if params.get("max_omsetning_per_ansatt") else None,
            min_resultat_per_ansatt=float(params["min_resultat_per_ansatt"]) if params.get("min_resultat_per_ansatt") else None,
            max_resultat_per_ansatt=float(params["max_resultat_per_ansatt"]) if params.get("max_resultat_per_ansatt") else None,
            orgform=params.get("orgform"),
            has_regnskap=str(params.get("has_regnskap") or "").strip().lower() in {"1", "true", "yes", "on"},
            regnskapsaar=int(params["regnskapsaar"]) if params.get("regnskapsaar") else None,
            innlevert_etter=params.get("innlevert_etter") or None,
            top_n=int(params.get("top_n") or 5),
            scatter_limit=int(params.get("scatter_limit") or 800),
        )
    elif path == "/analysis-api/companies/top-omsetning":
        payload = get_companies_top_omsetning_payload(
            limit=int(params.get("limit") or 100),
            min_omsetning=float(params["min_omsetning"]) if params.get("min_omsetning") else None,
            orgform=params.get("orgform"),
        )
    elif path == "/analysis-api/industry/suggest":
        payload = get_industry_suggest_payload(
            q=str(params.get("q") or ""),
            limit=int(params.get("limit") or 8),
        )
    elif path == "/analysis-api/industry/overview":
        payload = get_industry_overview_payload(
            limit=int(params.get("limit") or 25),
        )
    elif path.startswith("/analysis-api/company-history/"):
        orgnr = path.rsplit("/", 1)[-1]
        payload = get_company_history_payload(orgnr)
    elif path == "/analysis-api/kart":
        from analysis_api_compat import get_kart_payload
        payload = get_kart_payload(
            north=float(params.get("north", 71)),
            south=float(params.get("south", 57)),
            east=float(params.get("east", 31)),
            west=float(params.get("west", 4)),
            q=params.get("q"),
            kommune=params.get("kommune"),
            naeringskode=params.get("naeringskode") or None,
            adresse=params.get("adresse") or None,
            orgform=params.get("orgform") or None,
            min_omsetning=float(params["min_omsetning"]) if params.get("min_omsetning") else None,
            max_omsetning=float(params["max_omsetning"]) if params.get("max_omsetning") else None,
            min_resultat=float(params["min_resultat"]) if params.get("min_resultat") else None,
            max_resultat=float(params["max_resultat"]) if params.get("max_resultat") else None,
            min_egenkapitalandel=float(params["min_egenkapitalandel"]) if params.get("min_egenkapitalandel") else None,
            min_netto_margin=float(params["min_netto_margin"]) if params.get("min_netto_margin") else None,
            min_ansatte=int(params["min_ansatte"]) if params.get("min_ansatte") else None,
            max_ansatte=int(params["max_ansatte"]) if params.get("max_ansatte") else None,
            min_omsetning_per_ansatt=float(params["min_omsetning_per_ansatt"]) if params.get("min_omsetning_per_ansatt") else None,
            max_omsetning_per_ansatt=float(params["max_omsetning_per_ansatt"]) if params.get("max_omsetning_per_ansatt") else None,
            min_resultat_per_ansatt=float(params["min_resultat_per_ansatt"]) if params.get("min_resultat_per_ansatt") else None,
            max_resultat_per_ansatt=float(params["max_resultat_per_ansatt"]) if params.get("max_resultat_per_ansatt") else None,
            has_regnskap=str(params.get("has_regnskap") or "").strip().lower() in {"1", "true", "yes", "on"},
            regnskapsaar=int(params["regnskapsaar"]) if params.get("regnskapsaar") else None,
            innlevert_etter=params.get("innlevert_etter") or None,
            limit=int(params.get("limit") or 500),
        )
        response = make_response(json.dumps(payload, ensure_ascii=False, default=str), 200)
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response
    else:
        return None

    response = make_response(json.dumps(payload, ensure_ascii=False, default=str), 200)
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["X-Analysis-API-Base"] = "local-compat"
    return response

def proxy_analysis_api(path: str, params: dict[str, Any] | None = None):
    if not (os.environ.get("ANALYSIS_API_URL") or "").strip():
        local_response = _proxy_analysis_api_locally(path, params)
        if local_response is not None:
            return local_response

    last_error: str | None = None

    for base_url in ANALYSIS_API_CANDIDATES:
        try:
            resp = requests.get(
                f"{base_url}{path}",
                params=params,
                timeout=20,
                headers={"Accept": "application/json"},
            )
        except requests.RequestException as exc:
            last_error = f"{base_url}: {exc}"
            continue

        if resp.status_code == 404 and len(ANALYSIS_API_CANDIDATES) > 1:
            last_error = f"{base_url}: HTTP 404"
            continue

        response = make_response(resp.text, resp.status_code)
        response.headers["Content-Type"] = resp.headers.get("Content-Type", "application/json; charset=utf-8")
        response.headers["X-Analysis-API-Base"] = base_url
        return response

    return jsonify({
        "detail": "Analysis API utilgjengelig",
        "error": last_error or "Ingen kandidater svarte.",
        "candidates": ANALYSIS_API_CANDIDATES,
    }), 503



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


@regnskap_bp.route("/api/health")
def regnskap_api_health():
    return jsonify({"ok": True, "service": "regnskap-flask-api"})


@regnskap_bp.route("/api/analysis/health")
def regnskap_api_analysis_health():
    return proxy_analysis_api("/analysis-api/health")


@regnskap_bp.route("/api/companies/filter")
def regnskap_api_companies_filter():
    allowed = {
        "q",
        "kommune",
        "naeringskode",
        "adresse",
        "min_omsetning",
        "max_omsetning",
        "min_resultat",
        "max_resultat",
        "min_egenkapitalandel",
        "min_netto_margin",
        "min_ansatte",
        "max_ansatte",
        "min_omsetning_per_ansatt",
        "max_omsetning_per_ansatt",
        "min_resultat_per_ansatt",
        "max_resultat_per_ansatt",
        "orgform",
        "has_regnskap",
        "regnskapsaar",
        "innlevert_etter",
        "limit",
        "offset",
        "sort_by",
        "sort_dir",
    }
    params = {key: value for key, value in request.args.items() if key in allowed and str(value).strip() != ""}
    query = str(params.get("q") or "").strip()
    normalized_query = normalize_orgnr(query)
    if len(normalized_query) == 9:
        params["q"] = normalized_query
    return proxy_analysis_api("/analysis-api/companies/filter", params)


@regnskap_bp.route("/api/companies/filter-meta")
def regnskap_api_companies_filter_meta():
    return proxy_analysis_api("/analysis-api/companies/filter/meta")


@regnskap_bp.route("/api/companies/filter/summary")
def regnskap_api_companies_filter_summary():
    params = {
        "q": request.args.get("q"),
        "kommune": request.args.get("kommune"),
        "naeringskode": request.args.get("naeringskode"),
        "adresse": request.args.get("adresse"),
        "min_omsetning": request.args.get("min_omsetning"),
        "max_omsetning": request.args.get("max_omsetning"),
        "min_resultat": request.args.get("min_resultat"),
        "max_resultat": request.args.get("max_resultat"),
        "min_egenkapitalandel": request.args.get("min_egenkapitalandel"),
        "min_netto_margin": request.args.get("min_netto_margin"),
        "min_ansatte": request.args.get("min_ansatte"),
        "max_ansatte": request.args.get("max_ansatte"),
        "min_omsetning_per_ansatt": request.args.get("min_omsetning_per_ansatt"),
        "max_omsetning_per_ansatt": request.args.get("max_omsetning_per_ansatt"),
        "min_resultat_per_ansatt": request.args.get("min_resultat_per_ansatt"),
        "max_resultat_per_ansatt": request.args.get("max_resultat_per_ansatt"),
        "orgform": request.args.get("orgform"),
        "has_regnskap": request.args.get("has_regnskap"),
        "regnskapsaar": request.args.get("regnskapsaar"),
        "innlevert_etter": request.args.get("innlevert_etter"),
        "top_n": request.args.get("top_n"),
        "scatter_limit": request.args.get("scatter_limit"),
    }
    return proxy_analysis_api("/analysis-api/companies/filter/summary", params)


@regnskap_bp.route("/api/naeringskode/suggest")
def regnskap_api_naeringskode_suggest():
    allowed = {"q", "limit"}
    params = {key: value for key, value in request.args.items() if key in allowed and str(value).strip() != ""}
    return proxy_analysis_api("/analysis-api/industry/suggest", params)


@regnskap_bp.route("/api/naeringskode/overview")
def regnskap_api_naeringskode_overview():
    allowed = {"limit"}
    params = {key: value for key, value in request.args.items() if key in allowed and str(value).strip() != ""}
    return proxy_analysis_api("/analysis-api/industry/overview", params)


@regnskap_bp.route("/api/companies/top-omsetning")
def regnskap_api_companies_top_omsetning():
    allowed = {"limit", "min_omsetning", "orgform"}
    params = {key: value for key, value in request.args.items() if key in allowed and str(value).strip() != ""}
    return proxy_analysis_api("/analysis-api/companies/top-omsetning", params)


@regnskap_bp.route("/selskap/<orgnr>")
def regnskap_company_detail(orgnr: str):
    normalized_orgnr = normalize_orgnr(orgnr)
    if len(normalized_orgnr) != 9:
        return render_template(
            "regnskap_company_detail.html",
            company=None,
            records=[],
            orgnr=orgnr,
            error="Ugyldig organisasjonsnummer. Bruk 9 siffer.",
            back_url=url_for("regnskap.regnskap_hub"),
            format_amount=format_amount,
            format_percent=format_percent,
        )

    response = proxy_analysis_api(f"/analysis-api/company-history/{normalized_orgnr}")

    if isinstance(response, tuple):
        flask_response = response[0]
        status_code = response[1]
    else:
        flask_response = response
        status_code = flask_response.status_code

    if status_code == 404:
        response = proxy_analysis_api(
            "/analysis-api/companies/filter",
            {
                "q": normalized_orgnr,
                "limit": 500,
                "offset": 0,
                "sort_by": "regnskapsaar",
                "sort_dir": "desc",
            },
        )
        if isinstance(response, tuple):
            flask_response = response[0]
            status_code = response[1]
        else:
            flask_response = response
            status_code = flask_response.status_code

    if status_code != 200:
        return render_template(
            "regnskap_company_detail.html",
            company=None,
            records=[],
            orgnr=normalized_orgnr,
            error=f"Klarte ikke hente selskapsdetaljer (HTTP {status_code}).",
            back_url=url_for("regnskap.regnskap_hub"),
            format_amount=format_amount,
            format_percent=format_percent,
        )

    try:
        payload = json.loads(flask_response.get_data(as_text=True))
    except json.JSONDecodeError:
        payload = {}

    rows = payload.get("results", []) if isinstance(payload, dict) else []
    entity_payload = payload.get("entity") if isinstance(payload, dict) else {}
    records = [row for row in rows if normalize_orgnr(str(row.get("orgnr", ""))) == normalized_orgnr]
    if not records and rows:
        records = rows
    records.sort(
        key=lambda row: (
            row.get("regnskapsaar") is None,
            -(int(row.get("regnskapsaar")) if str(row.get("regnskapsaar", "")).isdigit() else -1),
        ),
    )

    company = records[0] if records else None
    if company is None:
        return render_template(
            "regnskap_company_detail.html",
            company=None,
            records=[],
            orgnr=normalized_orgnr,
            error="Fant ikke selskapet i analyseplattformen.",
            back_url=url_for("regnskap.regnskap_hub"),
            format_amount=format_amount,
            format_percent=format_percent,
        )

    address_value = first_present(
        entity_payload,
        "beliggenhetsadresse_adresselinje1",
        "beliggenhetsadresse",
        "forretningsadresse_adresselinje1",
        "forretningsadresse",
        "adresse",
    )
    postal_code = first_present(entity_payload, "beliggenhetsadresse_postnummer", "forretningsadresse_postnummer", "postnummer")
    postal_place = first_present(entity_payload, "beliggenhetsadresse_poststed", "forretningsadresse_poststed", "poststed")
    lat_value = first_present(entity_payload, "latitude", "lat", "breddegrad")
    lon_value = first_present(entity_payload, "longitude", "lon", "lengdegrad")
    ansatte_value = first_present(entity_payload, "ansatte")

    brreg_result = lookup_orgnr_brreg(make_session(use_retries=False), normalized_orgnr, timeout=5)
    metric_comparison = build_metric_comparison(company, brreg_result)
    annual_report_url = build_brreg_annual_report_url(
        normalized_orgnr,
        brreg_result.year if brreg_result and brreg_result.year else (int(company.get("regnskapsaar")) if str(company.get("regnskapsaar", "")).isdigit() else None),
    )

    return render_template(
        "regnskap_company_detail.html",
        company=company,
        records=records,
        orgnr=normalized_orgnr,
        error="",
        back_url=url_for("regnskap.regnskap_hub"),
        company_meta={
            "orgform": first_present(entity_payload, "orgform", "organisasjonsform") or company.get("orgform"),
            "kommune": first_present(entity_payload, "kommune", "kommunenummer") or company.get("kommune") or company.get("kommunenummer"),
            "ansatte": ansatte_value if ansatte_value is not None else company.get("ansatte"),
            "adresse": address_value,
            "poststed": " ".join(part for part in [str(postal_code or "").strip(), str(postal_place or "").strip()] if part).strip() or None,
            "naeringskode": first_present(entity_payload, "naeringskode", "naeringskode1"),
            "naeringstekst": first_present(entity_payload, "naeringskode_beskrivelse", "naering", "naeringstekst"),
            "stiftelsesdato": first_present(entity_payload, "stiftelsesdato"),
            "registreringsdato": first_present(entity_payload, "registreringsdatoenhetsregisteret"),
            "latitude": lat_value,
            "longitude": lon_value,
        },
        entity_raw=entity_payload if isinstance(entity_payload, dict) else {},
        brreg_result=brreg_result,
        metric_comparison=metric_comparison,
        annual_report_url=annual_report_url,
        format_amount=format_amount,
        format_percent=format_percent,
    )


@regnskap_bp.route("/api/search")
def regnskap_api_search():
    query = (request.args.get("q") or request.args.get("orgnr") or "").strip()
    if not query:
        return jsonify({"detail": "Missing query parameter q or orgnr"}), 400

    http_session = make_session()
    result, error = _lookup_from_query(http_session, query)
    if error:
        return jsonify(
            {
                "total_returned": 0,
                "limit": 1,
                "offset": 0,
                "results": [],
                "detail": error,
            }
        )

    payload = {
        "orgnr": result.orgnr,
        "navn": result.company,
        "accounting_year": result.year,
        "revenue": result.omsetning,
        "net_profit": result.resultat_etter_skatt,
        "equity": result.egenkapital,
    }
    return jsonify({"total_returned": 1, "limit": 1, "offset": 0, "results": [payload]})


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
                        resultat_for_skatt=latest.get("ORFS") or latest.get("AARS"),
                        driftsresultat=latest.get("DRR") or latest.get("DRI") or latest.get("ODR"),
                        egenkapital=latest.get("SEK"),
                        sum_eiendeler=latest.get("SUME") or latest.get("SA"),
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
                    # Primærkilde for raskt svar er Brreg, men ikke alle selskaper
                    # har publiserte regnskapstall der. Da prøver vi Proff-fallback.
                    res = lookup_orgnr_brreg(http_session, orgnr)
                    if not res:
                        res = lookup_orgnr(http_session, orgnr)

                    if res:
                        search_details = build_detail_rows(res)
                    else:
                        search_error = (
                            f"Fant orgnr {orgnr}, men ingen regnskapsdata i verken Brreg eller Proff."
                        )

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

    active_module = (request.args.get("mode") or "api").strip().lower()
    if active_module not in {"api", "legacy"}:
        active_module = "api"

    return render_template(
        "regnskap_hub.html",
        analysis_api_url=ANALYSIS_API_URL,
        active_module=active_module,
        primary_module="api",
    )
