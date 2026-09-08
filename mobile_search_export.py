"""Server adapter for the standalone Mobile.de extractor; bounded public HTTP only."""
import json
import re
import time
from urllib.parse import urlsplit, urljoin, parse_qs

from bs4 import BeautifulSoup
from import_radar_search import fetch_html, SourceError
from scripts.mobile_hent import DEFAULT_SEARCH, canonical_ad, parse_ad, now


def validate_url(value, kind="search"):
    if not isinstance(value, str) or len(value) > 5000:
        raise ValueError("Oppgi en gyldig mobile.de-søkelenke")
    u = urlsplit(value.strip())
    if (u.scheme != "https" or u.hostname != "suchen.mobile.de" or u.username or u.password
            or u.port not in (None, 443) or u.fragment
            or u.path != f"/fahrzeuge/{kind}.html"):
        raise ValueError("Bruk en full HTTPS-lenke fra suchen.mobile.de")
    if kind == "details" and not canonical_ad(value):
        raise ValueError("Annonselenken mangler gyldig ID")
    if kind == "search" and not u.query:
        raise ValueError("Søkelenken mangler søkefiltre")
    return value.strip()


def snapshot_html(html, url):
    soup = BeautifulSoup(html, "html.parser")
    structured = []
    for node in soup.select('script[type="application/ld+json"]'):
        try:
            structured.append(json.loads(node.string or node.get_text()))
        except (ValueError, TypeError):
            pass
    meta = {n.get("property") or n.get("name"): n.get("content", "") for n in soup.select("meta[property],meta[name]")}
    canonical = soup.select_one('link[rel="canonical"]')
    if canonical and canonical_ad(urljoin(url, canonical.get("href", ""))) not in (None, canonical_ad(url)):
        raise SourceError("Kilden returnerte en annen annonse")
    pairs = []
    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            pairs.append([dt.get_text(" ", strip=True), dd.get_text(" ", strip=True)])
    for node in soup.select("script,style,nav,aside,[hidden],[aria-hidden='true']"):
        node.decompose()
    main = soup.select_one("main") or soup
    title_node = main.select_one('[data-testid="vip-ad-title"],h1')
    if title_node:
        meta["og:title"] = title_node.get_text(" ", strip=True)
    text = main.get_text("\n", strip=True)
    # Inline spans can separate amount, currency and net/VAT label across lines.
    text = re.sub(r"(?<=\d)\s*\n\s*(?=€|EUR)", " ", text)
    text = re.sub(r"(€|EUR)\s*\n\s*(?=\(?Netto|mtl\.|/Monat)", r"\1 ", text, flags=re.I)
    price_area = main.select_one('[data-testid="main-price-area"]')
    if price_area:
        text = price_area.get_text(" ", strip=True) + "\n" + text
    return {"url":url, "title":soup.title.get_text() if soup.title else "", "metas":meta,
            "headings":[n.get_text(" ", strip=True) for n in main.select("h1,h2")],
            "pairs":pairs, "jsonld":structured, "prices":[], "text":text, "captured_utc":now()}


def search_page(html, url):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup
    cards = main.select('[data-testid="result-listing"],[data-testid="result-list-item"],'
                        '[data-testid="search-result"],[data-testid="search-result-item"],[data-result-item]')
    links = []
    for root in cards or [main]:
        for a in ([root] if root.name == "a" else []) + root.select("a[href]"):
            target = urljoin(url, a.get("href", ""))
            link = canonical_ad(target)
            if link and link not in links:
                links.append(link)
    next_url = None
    for a in soup.select("a[href]"):
        label = a.get("aria-label", "") + " " + a.get_text(" ", strip=True)
        if "next" in a.get("rel", []) or re.search(r"\b(Nächste|Next|Weiter)\b", label, re.I) or "pagination-next" in a.get("data-testid", ""):
            if a.get("aria-disabled") == "true":
                continue
            try:
                next_url = validate_url(urljoin(url, a["href"]))
            except ValueError:
                continue
            break
    return links, next_url, bool(cards)


def collect(search_url, limit=20, *, fetch=fetch_html, checkpoint=None, pause=2):
    search_url = validate_url(search_url)
    deadline = time.monotonic() + 180
    state = {"report_type":"mobile_export", "formatversjon":1, "sok_url":search_url,
             "startet_utc":now(), "sok_sider":0, "sok_status":"pågår", "sok_merknader":[],
             "antall_lenker":0, "annonselenker":[], "biler":[], "feil":[], "kjorestatus":"pågår"}
    seen_pages, seen_ads = set(), set()
    pending, first = search_url, True

    def read(url):
        nonlocal first
        if not first:
            time.sleep(pause)
        first = False
        if time.monotonic() >= deadline:
            raise SourceError("Tidsgrensen på tre minutter er nådd. Hentede data er bevart.")
        return fetch(url, deadline=deadline)

    def save():
        state["sist_lagret_utc"] = now()
        if checkpoint:
            checkpoint(state)

    try:
        while pending and state["sok_sider"] < 5:
            if pending in seen_pages:
                raise SourceError("Siden gjentok en tidligere søkeside. Uttrekket er stoppet.")
            seen_pages.add(pending)
            links, next_url, scoped = search_page(read(pending), pending)
            state["sok_sider"] += 1
            if not scoped:
                note = "Resultatkort ble ikke gjenkjent. Anbefalte annonser kan være med; kontroller treffene mot søket."
                if note not in state["sok_merknader"]:
                    state["sok_merknader"].append(note)
            if not links:
                raise SourceError("Ingen annonselenker kunne leses. Søket kan være tomt, kreve nettleser eller ha endret oppsett.")
            for link in links:
                if link in seen_ads:
                    continue
                seen_ads.add(link)
                state["annonselenker"].append(link)
                state["antall_lenker"] = len(state["annonselenker"])
                save()
                # Access failures stop the entire job; do not retry through other URLs.
                html = read(link)
                row = parse_ad(snapshot_html(html, link), link)
                state["biler"].append(row)
                save()
                if len(state["biler"]) >= limit:
                    break
            if len(state["biler"]) >= limit:
                state["sok_status"] = f"Valgt grense på {limit} annonser nådd; flere treff kan finnes."
                break
            pending = next_url
            state["sok_status"] = ("Fem søkesider nådd; flere treff finnes." if next_url else
                                    "Ingen neste-lenke funnet. Kontroller at alle ønskede treff er med.")
        state["kjorestatus"] = "ferdig"
    except (SourceError, ValueError) as exc:
        state["feil"].append(str(exc))
        state["kjorestatus"] = "delvis" if state["biler"] else "feil"
        state["sok_status"] = str(exc)
    save()
    return state
