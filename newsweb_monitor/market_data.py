"""Kursreaksjon rundt en børsmelding: kurs før publisering vs. nå, mot OSEBX.

Intradagkurser (5 min, ca. 15 min forsinket) fra Yahoo Finance sitt chart-API.
Uoffisiell kilde uten avtale: bruk kun internt bak innlogging, og bytt til
lisensiert kilde (Euronext/Infront/Bloomberg) før bredere bruk. Kursene
publiseres aldri til det offentlige GitHub-snapshotet.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import threading
import time
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

OSLO = ZoneInfo("Europe/Oslo")
CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Prisanalyse-NewsWeb/1.0)", "Accept": "application/json"}
INDEX = "OSEBX"
TICKER_RE = re.compile(r"[A-Z0-9][A-Z0-9.\-]{0,14}")
# Kursbevegelse som regnes som kursdrivende: meravkastning mot OSEBX i prosent.
DRIVER_ABNORMAL_PCT = 3.0

_ALIAS_PATH = Path(__file__).resolve().parent / "viktige_selskaper.json"
_cache: dict = {}
_lock = threading.Lock()


def _aliases() -> dict:
    try:
        return json.loads(_ALIAS_PATH.read_text(encoding="utf-8")).get("yahoo_alias", {})
    except (OSError, ValueError):
        return {}


def yahoo_symbol(ticker: str) -> str | None:
    ticker = str(ticker or "").strip().upper()
    if ticker == INDEX:
        return "OSEBX.OL"
    if not TICKER_RE.fullmatch(ticker):
        return None
    alias = _aliases().get(ticker, ticker)
    if alias is None:  # kjent ikke tilgjengelig hos kilden
        return None
    return alias if "." in alias else alias + ".OL"


def market_open(now: dt.datetime | None = None) -> bool:
    """Oslo Børs kontinuerlig handel 09:00–16:20 hverdager (helligdager ikke håndtert)."""
    now = (now or dt.datetime.now(OSLO)).astimezone(OSLO)
    return now.weekday() < 5 and dt.time(9, 0) <= now.time() <= dt.time(16, 25)


def parse_chart(payload: dict) -> dict | None:
    try:
        res = payload["chart"]["result"][0]
        quote_ = res["indicators"]["quote"][0]
        bars = [(int(t), float(c), int(v or 0))
                for t, c, v in zip(res.get("timestamp") or [], quote_["close"], quote_["volume"])
                if c is not None]
        meta = res.get("meta") or {}
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    if not bars:
        return None
    return {"bars": bars, "price": meta.get("regularMarketPrice") or bars[-1][1],
            "currency": meta.get("currency") or "", "name": meta.get("longName") or meta.get("shortName") or ""}


def fetch_chart(ticker: str) -> dict | None:
    """5 dager med 5-minutters kurser. Cache 60 s når børsen er åpen, ellers 15 min."""
    symbol = yahoo_symbol(ticker)
    if not symbol:
        return None
    ttl = 60 if market_open() else 900
    now = time.monotonic()
    with _lock:
        hit = _cache.get(symbol)
        if hit and hit[0] > now:
            return hit[1]
    url = CHART_URL + quote(symbol) + "?" + urlencode({"interval": "5m", "range": "5d"})
    try:
        with urlopen(Request(url, headers=HEADERS), timeout=8) as response:
            data = parse_chart(json.loads(response.read(3_000_001)))
    except Exception:
        data = None
    with _lock:
        if len(_cache) > 400:
            _cache.clear()
        # Ved feil: kort negativ cache, så siden ikke hamrer kilden.
        _cache[symbol] = (now + (ttl if data else 120), data)
    return data


def _before(bars: list, ts: float):
    prior = [b for b in bars if b[0] < ts]
    return prior[-1] if prior else None


def reaction(ticker: str, published_at: str, *, chart=None, index_chart=None,
             now: dt.datetime | None = None) -> dict:
    """Kurs siste handel før meldingen vs. siste kurs nå, og meravkastning mot OSEBX."""
    ticker = str(ticker or "").upper()
    try:
        published = dt.datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
        if published.tzinfo is None:
            raise ValueError
    except ValueError:
        return {"ticker": ticker, "error": "Ugyldig tidspunkt"}
    data = chart if chart is not None else fetch_chart(ticker)
    if not data:
        return {"ticker": ticker, "error": "Ingen kursdata"}
    ts = published.timestamp()
    before = _before(data["bars"], ts)
    if not before:
        return {"ticker": ticker, "error": "Ingen kurs før meldingen i datagrunnlaget"}
    price = float(data["price"])
    out = {"ticker": ticker, "name": data.get("name", ""), "currency": data.get("currency", ""),
           "price_before": round(before[1], 4), "price_now": round(price, 4),
           "change_pct": round((price / before[1] - 1) * 100, 2),
           "market_open": market_open(now)}
    # Relativt volum: volum i dag mot snitt av tidligere dager i datagrunnlaget.
    days: dict = {}
    for t, _c, v in data["bars"]:
        day = dt.datetime.fromtimestamp(t, OSLO).date()
        days[day] = days.get(day, 0) + v
    last_day = max(days)
    history = [v for d, v in days.items() if d != last_day and v]
    if history:
        out["rel_volume"] = round(days[last_day] / (sum(history) / len(history)), 2)
    out["traded_since"] = any(b[0] >= ts for b in data["bars"])
    idx = index_chart if index_chart is not None else fetch_chart(INDEX)
    if idx:
        ib = _before(idx["bars"], ts)
        if ib:
            out["index_pct"] = round((float(idx["price"]) / ib[1] - 1) * 100, 2)
            out["abnormal_pct"] = round(out["change_pct"] - out["index_pct"], 2)
    abnormal = out.get("abnormal_pct", out["change_pct"])
    out["price_driver"] = bool(out["traded_since"] and abs(abnormal) >= DRIVER_ABNORMAL_PCT)
    return out
