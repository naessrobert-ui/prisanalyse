"""Bounded public HTML searches. No logins, browser fingerprinting or retries.

Both sources are independently reported; a source failure is never an empty
successful search. Parsing is based on visible HTML fields observed Sep 2026.
"""
from __future__ import annotations

import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
import re
import time
import unicodedata
from urllib.parse import urlencode, urljoin, urlparse, parse_qs
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

from import_radar import Settings, evaluate_listings, calculate, number

ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
HOSTS = {"www.bytbil.com", "bytbil.com", "suchen.mobile.de", "www.ecb.europa.eu"}
LEASE = re.compile(r"leasingübernahme|leasinguebernahme|privatleasing|leasingövertag|\b(?:leasing|lease)\b|/\s*(?:mån|monat|month)", re.I)


class SourceError(ValueError):
    pass


def words(value):
    return re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().lower())


@dataclass(frozen=True)
class Search:
    make: str = "Kia"
    model: str = "EV6"
    year_from: int = 2022
    year_to: int = date.today().year
    max_km: int = 90000
    drive: str = "ANY"
    min_battery_kwh: float = 0
    vat_only: bool = True
    per_source: int = 5

    def __post_init__(self):
        for field in ("make", "model"):
            v = getattr(self, field)
            if not isinstance(v, str) or not re.fullmatch(r"[\w .+\-]{1,50}", v, flags=re.UNICODE) or not words(v):
                raise ValueError("Oppgi gyldig merke og modell")
        for field, lo, hi in (("year_from", 1990, date.today().year + 1),
                              ("year_to", 1990, date.today().year + 1),
                              ("max_km", 1, 500000), ("per_source", 1, 10)):
            n = number(getattr(self, field), field, minimum=lo)
            if int(n) != n or n > hi:
                raise ValueError(f"Ugyldig {field}")
        if self.year_from > self.year_to or self.drive not in {"ANY", "AWD", "2WD"}:
            raise ValueError("Kontroller årstall og hjuldrift")
        if number(self.min_battery_kwh, "Batteri") > 250:
            raise ValueError("Batteri må være mellom 0 og 250 kWh")
        if not isinstance(self.vat_only, bool):
            raise ValueError("Momsfilter må være true/false")


def fetch_html(url, *, deadline=None):
    """Allowlisted requests, capped body, explicit redirects, no automatic retry."""
    for _ in range(4):
        host = urlparse(url)
        if host.scheme != "https" or host.hostname not in HOSTS or host.username or host.password or host.port not in (None, 443):
            raise SourceError("Kilden returnerte en uventet adresse")
        remaining = (deadline - time.monotonic()) if deadline else 25
        if remaining <= 0:
            raise SourceError("Søket nådde tidsgrensen")
        try:
            with requests.get(url, timeout=(min(5, remaining), min(12, remaining)),
                              allow_redirects=False, stream=True) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    url = urljoin(url, response.headers.get("Location", ""))
                    continue
                if response.status_code != 200:
                    raise SourceError(f"Kilden svarte HTTP {response.status_code}; søket er stoppet")
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > 6_000_000 or (deadline and time.monotonic() > deadline):
                        raise SourceError("Kildesvaret overskred størrelse eller tidsgrense")
                    chunks.append(chunk)
                content = b"".join(chunks).decode("utf-8", errors="replace")
        except requests.RequestException as exc:
            raise SourceError("Kunne ikke hente kilden (nettverksfeil eller tidsavbrudd)") from exc
        # Check actual page text, not third-party script bundles mentioning captcha.
        soup = BeautifulSoup(content, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        visible = soup.get_text(" ", strip=True)
        if re.search(r"verify you are human|checking your browser|unusual traffic|automated queries|access denied", visible, re.I):
            raise SourceError("Nettstedet avviser automatisk tilgang; ingen nye forsøk utføres")
        return content
    raise SourceError("For mange videresendinger fra kilden")


def fx_rates(fetch=fetch_html, today=None):
    root = ET.fromstring(fetch(ECB_URL))
    dated = next((e for e in root.iter() if "time" in e.attrib), None)
    if dated is None:
        raise SourceError("Valutakilden mangler kursdato")
    day = date.fromisoformat(dated.attrib["time"])
    age = ((today or date.today()) - day).days
    if not 0 <= age <= 7:
        raise SourceError("Valutakursene er for gamle eller fremdaterte")
    rates = {e.attrib["currency"]: number(e.attrib["rate"], "valutakurs", minimum=.000001)
             for e in dated if "currency" in e.attrib}
    return {"eur_nok": rates["NOK"], "sek_nok": rates["NOK"] / rates["SEK"],
            "date": day.isoformat(), "source": ECB_URL, "kind": "ECB referansekurs"}


def amount(text, *, decimal=False):
    m = re.search(r"\d[\d .\u00a0\u202f]*(?:,\d+)?", text or "")
    if not m:
        raise SourceError("Et nødvendig tall mangler i annonsen")
    token = re.sub(r"[\s\u00a0\u202f]", "", m.group())
    return float(token.replace(",", ".") if decimal else token.replace(".", "").replace(",", "."))


def text_at(soup, selector):
    node = soup.select_one(selector)
    return node.get_text(" ", strip=True) if node else ""


def fields(soup):
    return {dt.get_text(" ", strip=True): dt.find_next_sibling("dd").get_text(" ", strip=True)
            for dt in soup.find_all("dt") if dt.find_next_sibling("dd")}


def battery(text):
    m = re.search(r"(\d{2,3}(?:[.,]\d+)?)\s*[- ]?kwh\b", text, re.I)
    return float(m.group(1).replace(",", ".")) if m else None


def mobile_model_code(search, fetch):
    # Resolve one model landing page; never guess IDs or loop through URL variants.
    slug = re.sub(r"[ .]+", "-", (search.make + "-" + search.model).lower())
    soup_html = fetch("https://suchen.mobile.de/auto/" + slug + ".html")
    soup = BeautifulSoup(soup_html, "html.parser")
    label = text_at(soup, '[data-testid="make_models-filter:change"]')
    if words(search.make + search.model) not in words(label):
        raise SourceError("Mobile.de-modellen kunne ikke bekreftes. Kontroller modellnavnet")
    # Serialized HTML component state backing the visible make/model filter.
    values = set(re.findall(r'\\"filterIds\\":\[\\"make_models\\"\],\\"filterValue\\":\\"(\d+;\d+;;)\\"', soup_html))
    if len(values) != 1:
        raise SourceError("Mobile.de har endret modellfilteret; krever oppdatert leser")
    return values.pop()


def search_url(source, search, mobile_code=None):
    if source == "mobile_de":
        if not mobile_code:
            raise SourceError("Mangler bekreftet Mobile.de-modell")
        args = {"vc": "Car", "s": "Car", "dam": "false", "ft": "ELECTRICITY",
                "ms": mobile_code, "fr": f"{search.year_from}:{search.year_to}",
                "ml": f":{search.max_km}", "sb": "p", "od": "up", "isSearchRequest": "true"}
        if search.drive == "AWD":
            args["dt"] = "ALL_WHEEL"
        if search.vat_only:
            args["vat"] = "1"
        return "https://suchen.mobile.de/fahrzeuge/search.html?" + urlencode(args)
    args = {"Makes[0]": search.make, "Models[0]": search.model, "Fuels": "El",
            "ModelYearRange.From": search.year_from, "ModelYearRange.To": search.year_to,
            "MilageRange.To": format(search.max_km / 10, "g"), "PriceRange.From": 50000,
            "OnlyDeductibleVAT": str(search.vat_only).lower(),
            "SortParams.IsAscending": "True", "SortParams.SortField": "price_value"}
    if search.drive == "AWD":
        args["OnlyFourWheelDrive"] = "True"
    return "https://www.bytbil.com/bil?" + urlencode(args)


def cards(source, html):
    soup = BeautifulSoup(html, "html.parser")
    found = {}
    eligible_nodes = 0
    if source == "mobile_de":
        nodes = [a for a in soup.find_all("a", href=True)
                 if re.fullmatch(r"(?:base|top)-result-listing-\d+-link", a.get("data-testid", ""))]
        for node in nodes:
            title = text_at(node, '[data-testid$="-title"]')
            if LEASE.search(title):
                continue
            eligible_nodes += 1
            href = node.get("href", "")
            ids = parse_qs(urlparse(href).query).get("id", [])
            price = text_at(node, '[data-testid="main-price-label"]')
            if len(ids) == 1 and ids[0].isdigit() and price:
                found[ids[0]] = (amount(price), "https://suchen.mobile.de/fahrzeuge/details.html?id=" + ids[0])
    else:
        nodes = soup.select("li.result-list-item")
        for node in nodes:
            anchor = node.select_one("a.js-link-target[href]")
            if not anchor or LEASE.search(anchor.get_text(" ", strip=True)):
                continue
            eligible_nodes += 1
            href = urljoin("https://www.bytbil.com", anchor["href"])
            if urlparse(href).hostname != "www.bytbil.com" or "/personbil-" not in href:
                continue
            price = text_at(node, ".car-price-main")
            if price:
                found[href] = (amount(price), href)
    if eligible_nodes and not found:
        raise SourceError("Annonselenker eller prisfelt kunne ikke leses; kildens format kan være endret")
    if not nodes:
        # Require a positive no-results marker; changed HTML is not zero cars.
        visible = soup.get_text(" ", strip=True)
        if not re.search(r"0 (?:Angebote|Fahrzeuge|fordon)|inga (?:fordon|träffar)|keine Fahrzeuge", visible, re.I):
            raise SourceError("Søkeresultatet kunne ikke tolkes; kildens format kan være endret")
    return [url for _, url in sorted(found.values())], len(nodes)


def parse_detail(source, html, url, search):
    soup = BeautifulSoup(html, "html.parser")
    f = fields(soup)
    row = {"source": source, "url": url, "make": search.make, "model": search.model,
           "country": "DE" if source == "mobile_de" else "SE",
           "currency": "EUR" if source == "mobile_de" else "SEK",
           "fuel": "electric", "vehicle_type": "passenger_car",
           "export_price_confirmed": False, "variant_confirmed": False,
           "observed_at": datetime.now(timezone.utc).isoformat()}
    if source == "mobile_de":
        title_node = soup.select_one('[data-testid="vip-ad-title"]')
        title = title_node.parent.parent.get_text(" ", strip=True) if title_node else ""
        if not title_node or words(search.make + search.model) not in words(title_node.get_text()):
            raise SourceError("Annonsen gjelder en annen eller ukjent modell")
        if not text_at(soup, '[data-testid="vip-dealer-box-seller-address2"]').startswith("DE-"):
            raise SourceError("Selger er ikke bekreftet som tysk forhandler")
        if f.get("Antriebsart") != "Elektromotor" or "Gebrauchtfahrzeug" not in f.get("Fahrzeugzustand", ""):
            raise SourceError("Annonsen er ikke bekreftet brukt elbil")
        row["listing_id"] = parse_qs(urlparse(url).query)["id"][0]
        row["price_amount"] = amount(text_at(soup, '[data-testid="vip-price-label"]'))
        price_text = text_at(soup, '[data-testid="main-price-area"]')
        net = re.search(r"([\d.\s\u00a0,]+)\s*€\s*Netto", price_text)
        if net:
            row["advertised_net_amount"] = amount(net.group(1))
        row["vat_reclaimable"] = bool(net)
        row["mileage"], row["mileage_unit"] = amount(f.get("Kilometerstand", "")), "km"
        first = re.fullmatch(r"(\d{2})/(\d{4})", f.get("Erstzulassung", ""))
        if not first:
            raise SourceError("Registreringsår mangler")
        month, year = map(int, first.groups())
        row["model_year"], row["model_year_estimated"] = year, True
        row["first_registration_month"] = f"{year}-{month:02d}"
        row["battery_kwh"] = battery(f.get("Batteriekapazität (in kWh)", ""))
        visible = text_at(soup, '[data-testid="vip-features"]') + " " + text_at(soup, '[data-testid="vip-vehicle-description-text"]')
        row["drive"] = ("AWD" if re.search(r"\bAllradantrieb\b", visible) else
                        "RWD" if "Heckantrieb" in visible else "FWD" if "Frontantrieb" in visible else "UNKNOWN")
        row["damage_free"] = "Unfallfrei" in f.get("Fahrzeugzustand", "")
        if f.get("Gewicht"):
            row["weight_kg"] = amount(f["Gewicht"])
    else:
        title = text_at(soup, "h1.vehicle-detail-title")
        if words(f.get("Märke", "")) != words(search.make) or words(f.get("Modell", "")) != words(search.model):
            raise SourceError("Annonsen gjelder en annen eller ukjent modell")
        if f.get("Drivmedel") != "El" or "/personbil-" not in url:
            raise SourceError("Annonsen er ikke bekreftet el-personbil")
        row["listing_id"] = urlparse(url).path.rsplit("-", 1)[-1]
        price_box = soup.select_one(".vehicle-detail-price")
        if not price_box:
            raise SourceError("Kontantpris mangler")
        row["price_amount"] = amount(text_at(price_box, ".car-price-details"))
        net = text_at(price_box, ".price-excluding-vat")
        if net and re.search(r"ex(?:kl)?\.?\s*moms", net, re.I):
            row["advertised_net_amount"] = amount(net)
        row["vat_reclaimable"] = "advertised_net_amount" in row
        row["model_year"] = int(amount(f.get("Årsmodell", "")))
        row["mileage"], row["mileage_unit"] = amount(f.get("Miltal", "")), "mil"
        row["drive"] = {"4WD": "AWD", "2WD": "2WD", "Framhjulsdrift": "FWD", "Bakhjulsdrift": "RWD"}.get(f.get("Drivhjul"), "UNKNOWN")
        row["battery_kwh"] = battery(title)
        row["damage_free"] = None
        for label in soup.select("div.text-gray"):
            val = label.find_next_sibling("div")
            key = label.get_text(strip=True)
            if val and key == "I trafik":
                raw_date = val.get_text(strip=True)
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
                    row["first_registration"] = date.fromisoformat(raw_date).isoformat()
            if val and key in {"Tjänstevikt", "Totalvikt"}:
                row["weight_kg" if key == "Tjänstevikt" else "total_weight_kg"] = amount(val.get_text())
    if LEASE.search(title):
        raise SourceError("Leasingtilbud er utelatt")
    row["variant_text"] = title
    return row


def matches(row, search):
    km = row["mileage"] * (10 if row["mileage_unit"] == "mil" else 1)
    return (search.year_from <= row["model_year"] <= search.year_to and km <= search.max_km
            and (not search.vat_only or row.get("vat_reclaimable") is True)
            and (search.drive == "ANY" or (row["drive"] == "AWD" if search.drive == "AWD"
                 else row["drive"] in {"RWD", "FWD", "2WD"}))
            and (not search.min_battery_kwh or (row.get("battery_kwh") or 0) >= search.min_battery_kwh))


def collect_source(source, search, fetch=fetch_html):
    deadline = time.monotonic() + 95
    def read(url):
        if time.monotonic() > deadline:
            raise SourceError("Tidsgrensen for kilden er nådd")
        return fetch(url, deadline=deadline) if fetch is fetch_html else fetch(url)
    rows, errors, scanned = [], [], 0
    url = None
    try:
        code = mobile_model_code(search, read) if source == "mobile_de" else None
        url = search_url(source, search, code)
        links, card_count = cards(source, read(url))
        # Inspect a bounded first-page pool. No claim of whole-market coverage.
        for link in links[:min(20, search.per_source * 3)]:
            try:
                scanned += 1
                row = parse_detail(source, read(link), link, search)
                if matches(row, search):
                    rows.append(row)
                    if len(rows) >= search.per_source:
                        break
            except (SourceError, ValueError, KeyError) as exc:
                errors.append(str(exc))
                if isinstance(exc, SourceError) and any(x in str(exc) for x in ("HTTP", "avviser", "Tidsgrensen", "tidsgrense")):
                    break
        status = "partial" if errors else "ok"
        if links and not rows and errors:
            status = "error"
        return rows, {"source": source, "status": status, "url": url, "cards_seen": card_count,
                      "details_checked": scanned, "matched": len(rows), "errors": errors[:5],
                      "coverage": "Første resultatside, begrenset antall detaljoppslag"}
    except (SourceError, ValueError, KeyError) as exc:
        return rows, {"source": source, "status": "error", "url": url, "matched": len(rows), "errors": [str(exc)]}


def run_search(search, settings, *, fx_info=None, collector=collect_source, evaluator=evaluate_listings,
               assumed_weight_kg=None):
    started = datetime.now(timezone.utc).isoformat()
    rows, sources = [], []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(collector, source, search): source for source in ("mobile_de", "bytbil")}
        for future in as_completed(futures):
            try:
                found, status = future.result()
                rows.extend(found)
                sources.append(status)
            except Exception:
                sources.append({"source": futures[future], "status": "error", "matched": 0,
                                "errors": ["Uventet feil i annonsehenteren"]})
    for row in rows:
        # Conservative date estimate is explicit and must not become purchase-ready.
        if not row.get("first_registration") and row.get("first_registration_month"):
            year, month = map(int, row["first_registration_month"].split("-"))
            row["first_registration"] = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
            row["registration_date_estimated"] = True
        if not row.get("weight_kg") and assumed_weight_kg is not None:
            row["weight_kg"] = number(assumed_weight_kg, "Anslått egenvekt", minimum=500)
            row["weight_estimated"] = True
    report = evaluator(rows, settings)
    by_key = {r["source"] + ":" + str(r["listing_id"]): r for r in rows}
    for result in report["results"]:
        row = by_key[result["key"]]
        result["variant_text"] = row.get("variant_text", "")
        result["source"] = row["source"]
        result["battery_kwh"] = row.get("battery_kwh")
        result["drive"] = row.get("drive")
        if row.get("advertised_net_amount") is not None and result.get("calculation"):
            scenario = calculate(dict(row, export_price_amount=row["advertised_net_amount"],
                                      export_price_confirmed=True), settings,
                                 result["valuation"].get(settings.price_basis))
            scenario.update(export_price_confirmed=False, scenario_only=True)
            result["net_scenario_calculation"] = scenario
        # Every collected export quote is unconfirmed, even if its price is displayed.
        if result["review_reasons"] and result["status"] == "kandidat":
            result["status"] = "må_kontrolleres"
    report["results"].sort(key=lambda r: r.get("purchase_observation", {}).get("gross_plus_freight_nok", float("inf")))
    report.update(live_collection=True, search=asdict(search), sources=sorted(sources, key=lambda s:s["source"]),
                  settings={**asdict(settings), "registration_date": settings.registration_date.isoformat()},
                  fx=fx_info, started_at=started, listings=rows)
    return report
