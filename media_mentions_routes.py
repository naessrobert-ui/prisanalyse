"""Media monitoring page for Robert Naess mentions."""

from __future__ import annotations

import email.utils
import html
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus, urlparse

import requests
from flask import Blueprint, jsonify, render_template, request


media_mentions_bp = Blueprint("media_mentions", __name__, url_prefix="/media")

SEARCH_TERMS = ['"Robert Næss"', '"Robert Naess"']
SOURCE_DOMAINS = [
    "dn.no",
    "finansavisen.no",
    "e24.no",
    "bt.no",
    "nettavisen.no",
    "vg.no",
    "aftenposten.no",
    "nordea.com",
    "nordea.no",
]
CACHE_TTL_SECONDS = 15 * 60
_CACHE: dict[tuple[int, str], tuple[float, dict[str, Any]]] = {}


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass

    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[: len(fmt)], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean_text(value: str | None) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _domain_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _source_bucket(domain: str) -> str:
    for source in SOURCE_DOMAINS:
        if domain == source or domain.endswith("." + source):
            return source
    return domain or "ukjent"


def _matches_name(*values: str) -> bool:
    haystack = " ".join(values).lower()
    return "robert næss" in haystack or "robert naess" in haystack


def _google_news_urls(days: int) -> list[tuple[str, str]]:
    queries = ['"Robert Naess"', '"Robert Naess" Nordea']
    return [
        (
            query,
            "https://news.google.com/rss/search?"
            f"q={quote_plus(f'{query} when:{days}d')}&hl=nb&gl=NO&ceid=NO:nb",
        )
        for query in queries
    ]


def _fetch_google_news(days: int) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for matched_query, url in _google_news_urls(days):
        articles.extend(_fetch_google_news_url(url, matched_query))
    return articles


def _fetch_google_news_url(url: str, matched_query: str) -> list[dict[str, Any]]:
    response = requests.get(
        url,
        timeout=8,
        headers={"User-Agent": "Prisanalyse media monitor/1.0"},
    )
    response.raise_for_status()

    root = ET.fromstring(response.content)
    articles: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title"))
        description = _clean_text(item.findtext("description"))
        published = _parse_date(item.findtext("pubDate"))
        link = item.findtext("link") or ""
        source_node = item.find("source")
        source_name = _clean_text(source_node.text if source_node is not None else "")
        domain = _domain_from_url(source_node.attrib.get("url", "") if source_node is not None else link)
        articles.append(
            {
                "title": title,
                "url": link,
                "source": source_name or _source_bucket(domain),
                "domain": _source_bucket(domain),
                "published_at": published,
                "snippet": description,
                "origin": "Google News",
                "matched_query": matched_query,
            }
        )
    return articles


def _fetch_gdelt(days: int) -> list[dict[str, Any]]:
    query = '"Robert Naess"'
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "sort": "datedesc",
        "maxrecords": "100",
        "timespan": f"{days}d",
    }
    try:
        response = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params=params,
            timeout=10,
            headers={"User-Agent": "Prisanalyse media monitor/1.0"},
        )
    except requests.RequestException:
        return []
    if response.status_code == 429:
        return []
    response.raise_for_status()
    if "json" not in (response.headers.get("content-type") or "").lower():
        return []
    payload = response.json()

    articles: list[dict[str, Any]] = []
    for article in payload.get("articles", []):
        title = _clean_text(article.get("title"))
        snippet = _clean_text(article.get("seendate") or "")
        url = article.get("url") or ""
        published = _parse_date(article.get("seendate"))
        domain = _domain_from_url(url)
        if not _matches_name(title, url):
            continue
        articles.append(
            {
                "title": title,
                "url": url,
                "source": _clean_text(article.get("sourceCountry")) or _source_bucket(domain),
                "domain": _source_bucket(domain),
                "published_at": published,
                "snippet": snippet,
                "origin": "GDELT",
            }
        )
    return articles


def _dedupe_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for article in articles:
        key = article["url"] or re.sub(r"\W+", "", article["title"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(article)
    return sorted(
        deduped,
        key=lambda item: item["published_at"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


def _build_payload(days: int, force_refresh: bool = False) -> dict[str, Any]:
    cache_key = (days, "robert-naess")
    cached = _CACHE.get(cache_key)
    now = time.time()
    if not force_refresh and cached and now - cached[0] < CACHE_TTL_SECONDS:
        return cached[1]

    errors: list[str] = []
    articles: list[dict[str, Any]] = []
    for fetcher in (_fetch_google_news, _fetch_gdelt):
        try:
            articles.extend(fetcher(days))
        except Exception as exc:  # pragma: no cover - external service resilience
            errors.append(f"{fetcher.__name__}: {exc}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = [
        article for article in _dedupe_articles(articles)
        if article["published_at"] is None or article["published_at"] >= cutoff
    ]

    daily_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    for article in articles:
        day = (article["published_at"] or datetime.now(timezone.utc)).date().isoformat()
        daily_counts[day] += 1
        source_counts[article["domain"]] += 1

    trend = []
    start_date = (datetime.now(timezone.utc) - timedelta(days=days - 1)).date()
    for offset in range(days):
        day = (start_date + timedelta(days=offset)).isoformat()
        trend.append({"date": day, "mentions": daily_counts.get(day, 0)})

    ranked_articles = sorted(
        articles,
        key=lambda article: (
            source_counts[article["domain"]],
            article["published_at"] or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )[:10]

    payload = {
        "days": days,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_mentions": len(articles),
        "articles": [_serialize_article(article) for article in articles],
        "trend": trend,
        "sources": [{"source": key, "mentions": value} for key, value in source_counts.most_common()],
        "ranked_articles": [_serialize_article(article) for article in ranked_articles],
        "errors": errors,
    }
    _CACHE[cache_key] = (now, payload)
    return payload


def _serialize_article(article: dict[str, Any]) -> dict[str, Any]:
    published = article.get("published_at")
    return {
        **article,
        "published_at": published.isoformat() if isinstance(published, datetime) else None,
    }


@media_mentions_bp.route("/robert-naess/")
def robert_naess_page():
    days = _requested_days()
    return render_template("media_robert_naess.html", initial_data=_build_payload(days), days=days)


@media_mentions_bp.route("/api/robert-naess")
def robert_naess_api():
    force_refresh = request.args.get("refresh") in {"1", "true", "yes"}
    return jsonify(_build_payload(_requested_days(), force_refresh=force_refresh))


def _requested_days() -> int:
    try:
        days = int(request.args.get("days", "7"))
    except ValueError:
        days = 7
    return max(1, min(days, 90))
