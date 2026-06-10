"""Media monitoring page for Robert Naess mentions.

Treff bygges som union av:
  - live henting fra åpne kilder (media_sources.fetch_all_sources)
  - det persistente arkivet på S3 (media_arkiv), som cron-jobben
    scripts/oppdater_media_arkiv.py fyller og beriker (sentiment, tema,
    sitat, og:image)

Arkivet gjør lange perioder reelle: live-API-ene husker bare et begrenset
antall treff, mens arkivet akkumulerer alt som er sett.
"""

from __future__ import annotations

import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Blueprint, jsonify, render_template, request

from media_arkiv import (
    archive_articles,
    archive_status,
    enrichment_lookup,
    load_archive,
    update_archive_from_sources,
)
from media_sources import (
    X_RECENT_MAX_DAYS,
    article_id,
    cluster_articles,
    dedupe_articles,
    fetch_all_sources,
    fetch_x_counts,
)

media_mentions_bp = Blueprint("media_mentions", __name__, url_prefix="/media")

CACHE_TTL_SECONDS = 15 * 60
_CACHE: dict[tuple[int, str], tuple[float, dict[str, Any]]] = {}

MAX_ARTICLES_IN_PAYLOAD = 400
MAX_CLUSTERS_IN_PAYLOAD = 150

_SOCIAL_CATEGORIES = {"sosialt"}


def _build_payload(days: int, force_refresh: bool = False) -> dict[str, Any]:
    cache_key = (days, "robert-naess")
    cached = _CACHE.get(cache_key)
    now = time.time()
    if not force_refresh and cached and now - cached[0] < CACHE_TTL_SECONDS:
        return cached[1]

    live_articles, x_status, errors = fetch_all_sources(days)

    archive_df = load_archive(use_cache=not force_refresh)
    archived = archive_articles(archive_df, days)
    enrichment = enrichment_lookup(archive_df)

    # Arkivrader først slik at beriket versjon vinner ved URL-duplikat.
    articles = dedupe_articles(archived + live_articles)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = [
        article for article in articles
        if article["published_at"] is None or article["published_at"] >= cutoff
    ]

    for article in articles:
        extra = enrichment.get(article_id(article))
        if extra:
            for key in ("sentiment", "tema", "sitat", "image"):
                if not article.get(key):
                    article[key] = extra[key]

    daily_news: Counter[str] = Counter()
    daily_social: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    sentiment_counts: Counter[str] = Counter()
    for article in articles:
        day = (article["published_at"] or datetime.now(timezone.utc)).date().isoformat()
        category = article.get("category") or "nyheter"
        if category in _SOCIAL_CATEGORIES:
            daily_social[day] += 1
        else:
            daily_news[day] += 1
        source_counts[article["domain"]] += 1
        category_counts[category] += 1
        sentiment_counts[article.get("sentiment") or "ukjent"] += 1

    x_day_counts = fetch_x_counts(days, x_status)

    trend = []
    start_date = (datetime.now(timezone.utc) - timedelta(days=days - 1)).date()
    for offset in range(days):
        day = (start_date + timedelta(days=offset)).isoformat()
        news_mentions = daily_news.get(day, 0)
        social_mentions = daily_social.get(day, 0) + x_day_counts.get(day, 0)
        trend.append({
            "date": day,
            "mentions": news_mentions + social_mentions,
            "news_mentions": news_mentions,
            "social_mentions": social_mentions,
            "x_mentions": x_day_counts.get(day, 0),
        })

    clusters = cluster_articles(articles)
    ranked_clusters = sorted(
        clusters,
        key=lambda cluster: (
            cluster["count"],
            cluster["primary"].get("engagement") or 0,
            cluster["primary"].get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )

    payload = {
        "days": days,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_mentions": len(articles),
        "x_recent_enabled": x_status["enabled"],
        "x_recent_days": X_RECENT_MAX_DAYS,
        "x_status": x_status,
        "archive": archive_status(archive_df),
        "articles": [_serialize_article(article) for article in articles[:MAX_ARTICLES_IN_PAYLOAD]],
        "clusters": [_serialize_cluster(cluster) for cluster in clusters[:MAX_CLUSTERS_IN_PAYLOAD]],
        "trend": trend,
        "sources": [{"source": key, "mentions": value} for key, value in source_counts.most_common()],
        "categories": dict(category_counts),
        "sentiments": dict(sentiment_counts),
        "ranked_articles": [_serialize_cluster(cluster) for cluster in ranked_clusters[:6]],
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


def _serialize_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        "primary": _serialize_article(cluster["primary"]),
        "also_in": cluster["also_in"],
        "count": cluster["count"],
    }


@media_mentions_bp.route("/robert-naess/")
def robert_naess_page():
    days = _requested_days()
    ytd_days = (datetime.now(timezone.utc).date() - datetime(datetime.now(timezone.utc).year, 1, 1).date()).days + 1
    return render_template(
        "media_robert_naess.html",
        initial_data=_build_payload(days),
        days=days,
        ytd_days=ytd_days,
    )


@media_mentions_bp.route("/api/robert-naess")
def robert_naess_api():
    force_refresh = request.args.get("refresh") in {"1", "true", "yes"}
    return jsonify(_build_payload(_requested_days(), force_refresh=force_refresh))


@media_mentions_bp.route("/api/robert-naess/arkiv/oppdater", methods=["POST"])
def robert_naess_archive_update():
    """Manuell arkivoppdatering (samme jobb som cron-en kjører)."""
    result = update_archive_from_sources(days=3)
    _CACHE.clear()
    return jsonify(result)


def _requested_days() -> int:
    try:
        days = int(request.args.get("days", "7"))
    except ValueError:
        days = 7
    return max(1, min(days, 3650))
