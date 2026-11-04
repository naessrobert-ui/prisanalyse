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


def session_start(now: dt.datetime | None = None) -> dt.datetime:
    """Børsslutt (16:20 Oslo) dagen før gjeldende handelsdag.

    Gjeldende handelsdag er i dag på hverdager, ellers siste fredag. Nyheter etter
    dette tidspunktet er de som kan drive kursen i gjeldende handelsdag.
    """
    now = (now or dt.datetime.now(OSLO)).astimezone(OSLO)
    session = now.date()
    while session.weekday() >= 5:
        session -= dt.timedelta(days=1)
    prev = session - dt.timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= dt.timedelta(days=1)
    return dt.datetime.combine(prev, dt.time(16, 20), OSLO)


def _peer_signal(driver_move: float, peer: dict, expected: str) -> str:
    """Har det sammenlignbare selskapet fulgt etter? 'følger', 'etterslep' eller 'motsatt'."""
    move = peer.get("abnormal_pct", peer.get("change_pct"))
    if move is None:
        return ""
    want = {"positiv": 1, "negativ": -1}.get(expected) or (1 if driver_move > 0 else -1)
    if abs(move) < 1.0:
        return "etterslep"
    return "følger" if move * want > 0 else "motsatt"


def drivers(items: list[dict], *, now: dt.datetime | None = None, fetch=None,
            peers_for=None, min_score: int = 75, max_drivers: int = 12, max_peers: int = 6) -> dict:
    """Dagens kursdrivende aksjer (nyhet siden forrige børsslutt) med sammenlignbare selskaper.

    Kursdrivende = minst DRIVER_ABNORMAL_PCT meravkastning mot OSEBX etter meldingen,
    eller (før handel har startet) AI-skår >= min_score.
    """
    from concurrent.futures import ThreadPoolExecutor
    fetch = fetch or fetch_chart
    if peers_for is None:
        from .importance import sector_peers as peers_for
    now = now or dt.datetime.now(OSLO)
    start = session_start(now)
    by_ticker: dict = {}
    for item in items:
        if item.get("skipped") or not item.get("ticker"):
            continue
        try:
            when = dt.datetime.fromisoformat(str(item.get("published_at")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if when < start or when > now:
            continue
        best = by_ticker.get(item["ticker"])
        # Første melding styrer kursreferansen; høyeste skår styrer overskriften.
        if not best:
            by_ticker[item["ticker"]] = {"item": item, "first": item["published_at"], "affected": []}
            best = by_ticker[item["ticker"]]
        elif int(item.get("score") or 0) > int(best["item"].get("score") or 0):
            best["item"] = item
        if str(item["published_at"]) < str(best["first"]):
            best["first"] = item["published_at"]
        for a in item.get("affected") or []:
            if a.get("ticker") and all(x["ticker"] != a["ticker"] for x in best["affected"]):
                best["affected"].append(a)
    if not by_ticker:
        return {"since": start.isoformat(), "market_open": market_open(now), "drivers": []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        charts = dict(zip(list(by_ticker) + [INDEX], pool.map(fetch, list(by_ticker) + [INDEX])))
    idx = charts.get(INDEX)
    chosen = []
    for ticker, info in by_ticker.items():
        item = info["item"]
        r = reaction(ticker, info["first"], chart=charts.get(ticker), index_chart=idx, now=now) \
            if charts.get(ticker) else {"ticker": ticker, "error": "Ingen kursdata"}
        score = int(item.get("score") or 0)
        traded = bool(r.get("traded_since"))
        no_data = bool(r.get("error"))
        if r.get("price_driver") or (not traded and score >= min_score):
            move = r.get("abnormal_pct", r.get("change_pct", 0)) or 0
            chosen.append({"ticker": ticker, "issuer": item.get("issuer", ""),
                           "headline": item.get("headline") or item.get("title", ""),
                           "url": item.get("url", ""), "published_at": info["first"], "score": score,
                           "direction": item.get("direction", ""), "reaction": r,
                           "status": "kursdrivende" if r.get("price_driver") else
                                     ("ingen kursdata" if no_data else "venter på handel"),
                           "_move": move, "_affected": info["affected"]})
    rank = {"kursdrivende": 0, "venter på handel": 1, "ingen kursdata": 2}
    chosen.sort(key=lambda d: (rank[d["status"]], -abs(d["_move"]), -d["score"]))
    chosen = chosen[:max_drivers]
    # Sammenlignbare: AI-berørte først (med forventet retning), deretter samme sektor.
    peer_lists = {}
    for d in chosen:
        rows = [{"ticker": a["ticker"], "expected": a.get("direction", "uklar"), "reason": a.get("reason", ""),
                 "basis": "AI"} for a in d.pop("_affected")]
        # Minste sektorgruppe først: den er mest spesifikk (Offshore-rederi før Oljeservice).
        for sector, members in sorted((peers_for(d["ticker"]) or {}).items(), key=lambda kv: len(kv[1])):
            for t in members:
                if t != d["ticker"] and all(x["ticker"] != t for x in rows):
                    rows.append({"ticker": t, "expected": "", "reason": sector, "basis": "sektor"})
        peer_lists[d["ticker"]] = rows[:max_peers]
    need = sorted({p["ticker"] for rows in peer_lists.values() for p in rows} - set(charts))
    with ThreadPoolExecutor(max_workers=8) as pool:
        charts.update(zip(need, pool.map(fetch, need)))
    for d in chosen:
        move = d.pop("_move")
        for p in peer_lists[d["ticker"]]:
            c = charts.get(p["ticker"])
            p["reaction"] = reaction(p["ticker"], d["published_at"], chart=c, index_chart=idx, now=now) \
                if c else {"ticker": p["ticker"], "error": "Ingen kursdata"}
            p["signal"] = _peer_signal(move, p["reaction"], p["expected"]) if not p["reaction"].get("error") \
                and p["reaction"].get("traded_since") else ""
        d["peers"] = peer_lists[d["ticker"]]
    return {"since": start.isoformat(), "market_open": market_open(now), "drivers": chosen}
