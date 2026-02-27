# -*- coding: utf-8 -*-
"""Regnskap-modul for prisanalyse."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from flask import Blueprint, render_template, request

regnskap_bp = Blueprint("regnskap", __name__, url_prefix="/regnskap")

USER_AGENT = "Mozilla/5.0"
TIMEOUT = 20
URL_TEMPLATES = [
    "https://www.proff.no/regnskap/-/{org}",
    "https://www.proff.no/regnskap/{org}",
]


@dataclass
class LookupResult:
    company: str
    orgnr: str
    year: int | None
    resultat_etter_skatt: float | None
    egenkapital: float | None
    omsetning: float | None
    url_used: str


def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


def normalize_orgnr(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip())


def build_urls(orgnr: str) -> list[str]:
    return [tpl.format(org=orgnr) for tpl in URL_TEMPLATES]


def try_fetch_payload(session: requests.Session, url: str) -> dict[str, Any] | None:
    try:
        response = session.get(url, timeout=TIMEOUT)
        if response.status_code != 200:
            return None
    except requests.RequestException:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__", "type": "application/json"})
    if not script or not script.string:
        return None

    try:
        return json.loads(script.string)
    except Exception:
        return None


def payload_company(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("props", {}).get("pageProps", {}).get("company", {})


def company_name(company: dict[str, Any]) -> str:
    for key in ("name", "companyName", "displayName"):
        value = company.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Ukjent"


def account_amount(account_year: dict[str, Any], code: str) -> float | None:
    for account in account_year.get("accounts", []):
        if account.get("code") == code:
            amount = account.get("amount")
            if isinstance(amount, str):
                amount = amount.replace(" ", "").replace("\xa0", "")
            try:
                return float(amount)
            except (TypeError, ValueError):
                return None
    return None


def latest_year_data(company: dict[str, Any]) -> dict[str, Any] | None:
    years = company.get("companyAccounts", []) or []
    if not years:
        return None

    valid_years = [y for y in years if y.get("year") is not None]
    if not valid_years:
        return None
    return sorted(valid_years, key=lambda row: row.get("year"), reverse=True)[0]


def lookup_orgnr(session: requests.Session, orgnr: str) -> LookupResult | None:
    for url in build_urls(orgnr):
        payload = try_fetch_payload(session, url)
        if not payload:
            continue

        company = payload_company(payload)
        year_data = latest_year_data(company)
        if not year_data:
            continue

        return LookupResult(
            company=company_name(company),
            orgnr=normalize_orgnr(company.get("orgNumber", orgnr)),
            year=year_data.get("year"),
            resultat_etter_skatt=account_amount(year_data, "AR"),
            egenkapital=account_amount(year_data, "SEK"),
            omsetning=account_amount(year_data, "SDI"),
            url_used=url,
        )
    return None


def parse_orgnrs_from_file(file_storage) -> list[str]:
    content = file_storage.read().decode("utf-8", errors="ignore")
    raw_items = re.split(r"[\s,;]+", content)
    orgnrs = []
    for item in raw_items:
        org = normalize_orgnr(item)
        if len(org) == 9:
            orgnrs.append(org)
    return list(dict.fromkeys(orgnrs))


def search_to_orgnr(session: requests.Session, query: str) -> str | None:
    normalized = normalize_orgnr(query)
    if len(normalized) == 9:
        return normalized

    search_url = f"https://www.proff.no/bransjesok?q={quote(query)}"
    try:
        response = session.get(search_url, timeout=TIMEOUT)
    except requests.RequestException:
        return None

    if response.status_code != 200:
        return None

    matches = re.findall(r"/regnskap/[^\"'\s<>]+", response.text)
    for match in matches:
        org_candidates = re.findall(r"(\d{9})", match)
        if org_candidates:
            return org_candidates[-1]

    return None


def build_detail_rows(result: LookupResult) -> list[tuple[str, str]]:
    def fmt(value: float | None) -> str:
        if value is None:
            return "Mangler"
        return f"{value:,.0f}".replace(",", " ")

    return [
        ("Selskap", result.company),
        ("Organisasjonsnummer", result.orgnr),
        ("Siste tilgjengelige regnskapsår", str(result.year) if result.year else "Ukjent"),
        ("Omsetning (SDI)", fmt(result.omsetning)),
        ("Resultat etter skatt (AR)", fmt(result.resultat_etter_skatt)),
        ("Egenkapital (SEK)", fmt(result.egenkapital)),
        ("Kilde", result.url_used),
    ]


@regnskap_bp.route("/", methods=["GET", "POST"])
def regnskap_hub():
    session = make_session()

    batch_results: list[LookupResult] = []
    batch_error = ""
    url_details: list[tuple[str, str]] = []
    url_error = ""
    search_details: list[tuple[str, str]] = []
    search_error = ""

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "batch":
            upload = request.files.get("orgnr_file")
            if not upload or not upload.filename:
                batch_error = "Velg en fil med organisasjonsnumre først."
            else:
                orgnrs = parse_orgnrs_from_file(upload)
                if not orgnrs:
                    batch_error = "Fant ingen gyldige organisasjonsnumre (9 siffer) i filen."
                else:
                    for orgnr in orgnrs[:100]:
                        result = lookup_orgnr(session, orgnr)
                        if result:
                            batch_results.append(result)

        if action == "url":
            regnskap_url = (request.form.get("proff_url") or "").strip()
            payload = try_fetch_payload(session, regnskap_url) if regnskap_url else None
            if not payload:
                url_error = "Klarte ikke å hente data fra URL-en. Kontroller lenken."
            else:
                company = payload_company(payload)
                orgnr = normalize_orgnr(company.get("orgNumber", ""))
                if len(orgnr) != 9:
                    url_error = "Fant ikke organisasjonsnummer i URL-resultatet."
                else:
                    result = lookup_orgnr(session, orgnr)
                    if result:
                        url_details = build_detail_rows(result)
                    else:
                        url_error = "Fant ikke regnskapstall for selskapet."

        if action == "search":
            query = (request.form.get("search_query") or "").strip()
            if not query:
                search_error = "Skriv inn søk (navn eller orgnr)."
            else:
                orgnr = search_to_orgnr(session, query)
                if not orgnr:
                    search_error = "Fant ingen selskaper fra søket."
                else:
                    result = lookup_orgnr(session, orgnr)
                    if result:
                        search_details = build_detail_rows(result)
                    else:
                        search_error = "Fant ikke regnskapstall for søket."

    return render_template(
        "regnskap_hub.html",
        batch_results=batch_results,
        batch_error=batch_error,
        url_details=url_details,
        url_error=url_error,
        search_details=search_details,
        search_error=search_error,
    )
