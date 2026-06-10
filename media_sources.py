"""Source fetchers and clustering for the Robert Naess media monitor.

Framework-free so both the Flask routes (media_mentions_routes.py) and the
Render cron job (scripts/oppdater_media_arkiv.py) can share it.

Article dict fields:
    title, url, source, domain, published_at (datetime|None), snippet,
    origin, category ("nyheter"|"sosialt"|"podkast"|"video"),
    matched_query, engagement (int)
"""

from __future__ import annotations

import email.utils
import hashlib
import html
import os
import re
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse, urlunparse

import requests

USER_AGENT = "Prisanalyse media monitor/2.0"
SEARCH_TERMS = ['"Robert Næss"', '"Robert Naess"']
SOURCE_DOMAINS = [
    "dn.no",
    "finansavisen.no",
    "e24.no",
    "bt.no",
    "nettavisen.no",
    "vg.no",
    "aftenposten.no",
    "nrk.no",
    "kapital.no",
    "nordea.com",
    "nordea.no",
]
X_RECENT_MAX_DAYS = 7
X_QUERIES = [
    '"Robert Næss" -is:retweet',
    '"Robert Naess" -is:retweet',
    'Robert Næss -is:retweet',
    'Robert Naess -is:retweet',
]

# Direkte feeds fra norske medier. Items filtreres lokalt på navnet, så feeds
# som forsvinner eller aldri nevner navnet er ufarlige (feil svelges per feed).
DIRECT_FEEDS = [
    ("e24.no", "https://e24.no/rss2/"),
    ("nrk.no", "https://www.nrk.no/nyheter/siste.rss"),
    ("vg.no", "https://www.vg.no/rss/feed/"),
    ("nettavisen.no", "https://www.nettavisen.no/service/rich-rss"),
    ("dn.no", "https://services.dn.no/api/feed/rss"),
]

_TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "ref"}


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

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d"):
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


def source_bucket(domain: str) -> str:
    for source in SOURCE_DOMAINS:
        if domain == source or domain.endswith("." + source):
            return source
    return domain or "ukjent"


def matches_name(*values: str) -> bool:
    haystack = " ".join(value or "" for value in values).lower()
    return "robert næss" in haystack or "robert naess" in haystack


def unwrap_redirect(url: str) -> str:
    """Pakk ut ekte artikkel-URL fra redirect-lenker.

    Bing News RSS bruker bing.com/news/apiclick.aspx med ny tid-parameter per
    henting - uten utpakking får samme artikkel ny id hver gang.
    """
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.netloc.lower().endswith("bing.com") and "apiclick" in parsed.path:
        target = dict(parse_qsl(parsed.query)).get("url", "")
        if target.startswith("http"):
            return target
    return url


def normalize_url(url: str) -> str:
    """Strip tracking params and fragments so the same article gets one id."""
    if not url:
        return ""
    parsed = urlparse(unwrap_redirect(url.strip()))
    query = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() not in _TRACKING_PARAMS]
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return urlunparse((parsed.scheme.lower() or "https", host, parsed.path.rstrip("/"), "", urlencode(query), ""))


def article_id(article: dict[str, Any]) -> str:
    key = normalize_url(article.get("url") or "")
    if not key:
        key = re.sub(r"\W+", "", (article.get("title") or "").lower())
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _make_article(
    *,
    title: str,
    url: str,
    source: str,
    domain: str,
    published_at: datetime | None,
    snippet: str,
    origin: str,
    category: str,
    matched_query: str = "",
    engagement: int = 0,
) -> dict[str, Any]:
    return {
        "title": title,
        "url": url,
        "source": source,
        "domain": source_bucket(domain),
        "published_at": published_at,
        "snippet": snippet,
        "origin": origin,
        "category": category,
        "matched_query": matched_query,
        "engagement": engagement,
    }


def _http_get(url: str, **kwargs: Any) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    kwargs.setdefault("timeout", 10)
    return requests.get(url, headers=headers, **kwargs)


# ---------------------------------------------------------------------------
# RSS-baserte kilder (Google News, Bing News, direkte feeds)
# ---------------------------------------------------------------------------

def _parse_rss_items(content: bytes, origin: str, matched_query: str = "", require_name: bool = False) -> list[dict[str, Any]]:
    root = ET.fromstring(content)
    articles: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title"))
        description = _clean_text(item.findtext("description"))
        if require_name and not matches_name(title, description):
            continue
        link = unwrap_redirect((item.findtext("link") or "").strip())
        published = _parse_date(item.findtext("pubDate"))
        source_node = item.find("source")
        source_name = _clean_text(source_node.text if source_node is not None else "")
        domain = _domain_from_url(source_node.attrib.get("url", "") if source_node is not None else link)
        articles.append(
            _make_article(
                title=title,
                url=link,
                source=source_name or source_bucket(domain),
                domain=domain,
                published_at=published,
                snippet=description,
                origin=origin,
                category="nyheter",
                matched_query=matched_query,
            )
        )
    return articles


def fetch_google_news(days: int) -> list[dict[str, Any]]:
    queries = ['"Robert Naess"', '"Robert Naess" Nordea']
    articles: list[dict[str, Any]] = []
    for query in queries:
        url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(f'{query} when:{days}d')}&hl=nb&gl=NO&ceid=NO:nb"
        )
        response = _http_get(url, timeout=8)
        response.raise_for_status()
        articles.extend(_parse_rss_items(response.content, "Google News", matched_query=query))
    return articles


def fetch_bing_news(days: int) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for query in SEARCH_TERMS:
        url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss&cc=no"
        response = _http_get(url, timeout=8)
        response.raise_for_status()
        articles.extend(_parse_rss_items(response.content, "Bing News", matched_query=query, require_name=True))
    return articles


def fetch_direct_feeds(days: int) -> list[dict[str, Any]]:
    """Direkte RSS fra norske medier, filtrert lokalt på navnet.

    Feeds er spekulative (kan 404e eller endre adresse), så feil per feed
    svelges stille - de skal ikke utløse feilvarsler i UI.
    """
    articles: list[dict[str, Any]] = []
    for domain, feed_url in DIRECT_FEEDS:
        try:
            response = _http_get(feed_url, timeout=8)
            response.raise_for_status()
            items = _parse_rss_items(response.content, "RSS", require_name=True)
        except Exception:
            continue
        for item in items:
            if not item["url"]:
                continue
            item["domain"] = source_bucket(item["domain"] or domain)
            item["source"] = item["source"] or domain
            articles.append(item)
    return articles


# ---------------------------------------------------------------------------
# GDELT
# ---------------------------------------------------------------------------

def fetch_gdelt(days: int) -> list[dict[str, Any]]:
    params = {
        "query": '"Robert Naess"',
        "mode": "artlist",
        "format": "json",
        "sort": "datedesc",
        "maxrecords": "100",
        "timespan": f"{days}d",
    }
    try:
        response = _http_get("https://api.gdeltproject.org/api/v2/doc/doc", params=params)
    except requests.RequestException:
        return []
    if response.status_code == 429:
        return []
    response.raise_for_status()
    if "json" not in (response.headers.get("content-type") or "").lower():
        return []

    articles: list[dict[str, Any]] = []
    for article in response.json().get("articles", []):
        title = _clean_text(article.get("title"))
        url = article.get("url") or ""
        if not matches_name(title, url):
            continue
        domain = _domain_from_url(url)
        articles.append(
            _make_article(
                title=title,
                url=url,
                source=_clean_text(article.get("sourceCountry")) or source_bucket(domain),
                domain=domain,
                published_at=_parse_date(article.get("seendate")),
                snippet="",
                origin="GDELT",
                category="nyheter",
            )
        )
    return articles


# ---------------------------------------------------------------------------
# Bluesky (åpent søke-API, ingen nøkkel)
# ---------------------------------------------------------------------------

def fetch_bluesky(days: int) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for query in SEARCH_TERMS:
        response = _http_get(
            "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
            params={"q": query, "limit": "50", "sort": "latest"},
        )
        if response.status_code != 200:
            continue
        for post in response.json().get("posts", []):
            record = post.get("record") or {}
            text = _clean_text(record.get("text"))
            if not matches_name(text):
                continue
            author = post.get("author") or {}
            handle = author.get("handle") or "bsky"
            rkey = (post.get("uri") or "").rsplit("/", 1)[-1]
            engagement = int(post.get("likeCount") or 0) + int(post.get("repostCount") or 0) + int(post.get("replyCount") or 0)
            articles.append(
                _make_article(
                    title=text[:180],
                    url=f"https://bsky.app/profile/{handle}/post/{rkey}",
                    source=f"@{handle}",
                    domain="bsky.app",
                    published_at=_parse_date(record.get("createdAt")),
                    snippet=text,
                    origin="Bluesky",
                    category="sosialt",
                    matched_query=query,
                    engagement=engagement,
                )
            )
    return articles


# ---------------------------------------------------------------------------
# Reddit (åpent JSON-søk, ingen nøkkel)
# ---------------------------------------------------------------------------

def fetch_reddit(days: int) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for query in SEARCH_TERMS:
        response = _http_get(
            "https://www.reddit.com/search.json",
            params={"q": query, "sort": "new", "limit": "50"},
        )
        if response.status_code != 200:
            continue
        for child in (response.json().get("data") or {}).get("children", []):
            post = child.get("data") or {}
            title = _clean_text(post.get("title"))
            selftext = _clean_text(post.get("selftext"))[:300]
            if not matches_name(title, selftext):
                continue
            created = post.get("created_utc")
            published = datetime.fromtimestamp(created, tz=timezone.utc) if created else None
            engagement = int(post.get("score") or 0) + int(post.get("num_comments") or 0)
            articles.append(
                _make_article(
                    title=title,
                    url=f"https://www.reddit.com{post.get('permalink', '')}",
                    source=f"r/{post.get('subreddit', 'reddit')}",
                    domain="reddit.com",
                    published_at=published,
                    snippet=selftext,
                    origin="Reddit",
                    category="sosialt",
                    matched_query=query,
                    engagement=engagement,
                )
            )
    return articles


# ---------------------------------------------------------------------------
# Podkast-episoder (iTunes Search, gratis og uten nøkkel)
# ---------------------------------------------------------------------------

def fetch_podcasts(days: int) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    seen_terms = set()
    for term in ("Robert Næss", "Robert Naess"):
        normalized = term.lower().replace("æ", "ae")
        if normalized in seen_terms:
            continue
        seen_terms.add(normalized)
        response = _http_get(
            "https://itunes.apple.com/search",
            params={
                "term": term,
                "media": "podcast",
                "entity": "podcastEpisode",
                "limit": "50",
                "country": "NO",
            },
        )
        if response.status_code != 200:
            continue
        for episode in response.json().get("results", []):
            title = _clean_text(episode.get("trackName"))
            description = _clean_text(episode.get("description"))[:300]
            if not matches_name(title, description):
                continue
            show = _clean_text(episode.get("collectionName"))
            articles.append(
                _make_article(
                    title=title,
                    url=episode.get("trackViewUrl") or episode.get("collectionViewUrl") or "",
                    source=show or "Podkast",
                    domain="podkast",
                    published_at=_parse_date(episode.get("releaseDate")),
                    snippet=description,
                    origin="Podkast",
                    category="podkast",
                )
            )
    return articles


# ---------------------------------------------------------------------------
# YouTube (krever YOUTUBE_API_KEY)
# ---------------------------------------------------------------------------

def fetch_youtube(days: int) -> list[dict[str, Any]]:
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        return []
    published_after = (datetime.now(timezone.utc) - timedelta(days=min(days, 365))).strftime("%Y-%m-%dT%H:%M:%SZ")
    response = _http_get(
        "https://www.googleapis.com/youtube/v3/search",
        params={
            "part": "snippet",
            "q": '"Robert Næss" Nordea',
            "type": "video",
            "maxResults": "25",
            "order": "date",
            "publishedAfter": published_after,
            "key": api_key,
        },
    )
    if response.status_code != 200:
        return []
    articles: list[dict[str, Any]] = []
    for item in response.json().get("items", []):
        snippet = item.get("snippet") or {}
        title = _clean_text(snippet.get("title"))
        description = _clean_text(snippet.get("description"))
        if not matches_name(title, description):
            continue
        video_id = (item.get("id") or {}).get("videoId")
        if not video_id:
            continue
        articles.append(
            _make_article(
                title=title,
                url=f"https://www.youtube.com/watch?v={video_id}",
                source=_clean_text(snippet.get("channelTitle")) or "YouTube",
                domain="youtube.com",
                published_at=_parse_date(snippet.get("publishedAt")),
                snippet=description[:300],
                origin="YouTube",
                category="video",
            )
        )
    return articles


# ---------------------------------------------------------------------------
# X (krever X_BEARER_TOKEN, maks 7 døgn tilbake)
# ---------------------------------------------------------------------------

def empty_x_status(days: int) -> dict[str, Any]:
    if not os.environ.get("X_BEARER_TOKEN"):
        state = "missing_token"
    elif days > X_RECENT_MAX_DAYS:
        state = "period_too_long"
    else:
        state = "not_checked"
    return {
        "enabled": bool(os.environ.get("X_BEARER_TOKEN")) and days <= X_RECENT_MAX_DAYS,
        "state": state,
        "recent_days": X_RECENT_MAX_DAYS,
        "post_count": 0,
        "count_total": 0,
        "http_statuses": [],
        "queries": X_QUERIES,
    }


def fetch_x_recent(days: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    bearer_token = os.environ.get("X_BEARER_TOKEN")
    status = empty_x_status(days)
    if not bearer_token or days > X_RECENT_MAX_DAYS:
        return [], status

    articles: list[dict[str, Any]] = []
    for query in X_QUERIES:
        try:
            response = _http_get(
                "https://api.x.com/2/tweets/search/recent",
                params={
                    "query": query,
                    "max_results": "50",
                    "tweet.fields": "created_at,public_metrics,author_id",
                    "expansions": "author_id",
                    "user.fields": "username,name",
                },
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
        except requests.RequestException as exc:
            status["state"] = "request_error"
            status["last_error"] = str(exc)
            continue

        status["http_statuses"].append(response.status_code)
        if response.status_code in {401, 403, 429}:
            status["state"] = f"http_{response.status_code}"
            continue
        response.raise_for_status()

        payload = response.json()
        status["post_count"] += int((payload.get("meta") or {}).get("result_count") or 0)
        users = {user.get("id"): user for user in payload.get("includes", {}).get("users", [])}
        for post in payload.get("data", []):
            user = users.get(post.get("author_id"), {})
            username = user.get("username") or post.get("author_id") or "x"
            text = _clean_text(post.get("text"))
            metrics = post.get("public_metrics") or {}
            engagement = sum(int(metrics.get(key, 0) or 0) for key in ("like_count", "retweet_count", "reply_count", "quote_count"))
            articles.append(
                _make_article(
                    title=text[:180],
                    url=f"https://x.com/{username}/status/{post.get('id')}",
                    source=f"@{username}",
                    domain="x.com",
                    published_at=_parse_date(post.get("created_at")),
                    snippet=text,
                    origin="X",
                    category="sosialt",
                    matched_query=query,
                    engagement=engagement,
                )
            )

    if articles:
        status["state"] = "ok"
    elif status["state"] == "not_checked":
        status["state"] = "ok_no_results"
    return articles, status


def fetch_x_counts(days: int, status: dict[str, Any]) -> Counter:
    bearer_token = os.environ.get("X_BEARER_TOKEN")
    if not bearer_token or days > X_RECENT_MAX_DAYS:
        return Counter()

    counts: Counter = Counter()
    for query in X_QUERIES:
        try:
            response = _http_get(
                "https://api.x.com/2/tweets/counts/recent",
                params={"query": query, "granularity": "day"},
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
        except requests.RequestException as exc:
            status["last_count_error"] = str(exc)
            continue

        if response.status_code in {401, 403, 429}:
            status["count_status"] = f"http_{response.status_code}"
            continue
        response.raise_for_status()

        for bucket in response.json().get("data", []):
            day = (bucket.get("start") or "")[:10]
            if day:
                tweet_count = int(bucket.get("tweet_count") or 0)
                counts[day] += tweet_count
                status["count_total"] += tweet_count
    return counts


# ---------------------------------------------------------------------------
# Samlet henting
# ---------------------------------------------------------------------------

FETCHERS: list[Callable[[int], list[dict[str, Any]]]] = [
    fetch_google_news,
    fetch_bing_news,
    fetch_gdelt,
    fetch_direct_feeds,
    fetch_bluesky,
    fetch_reddit,
    fetch_podcasts,
    fetch_youtube,
]


def fetch_all_sources(days: int, include_x: bool = True) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    """Hent alle kilder i parallell. Returnerer (artikler, x_status, feil)."""
    articles: list[dict[str, Any]] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetcher, days): fetcher.__name__ for fetcher in FETCHERS}
        if include_x:
            futures[pool.submit(fetch_x_recent, days)] = "fetch_x_recent"
        x_status = empty_x_status(days)
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - ekstern robusthet
                errors.append(f"{name}: {exc}")
                continue
            if name == "fetch_x_recent":
                x_articles, x_status = result
                articles.extend(x_articles)
            else:
                articles.extend(result)

    if not include_x:
        x_status = empty_x_status(days)
    return articles, x_status, errors


def title_dedupe_keys(article: dict[str, Any]) -> list[str]:
    """Domene + renset tittel, med og uten siste «- Kilde»-segment.

    Fanger samme artikkel via ulike indekser: Google News legger kildenavn
    bakerst i tittelen («Sak - Nettavisen»), Bing/RSS gjør det ikke. Begge
    varianter genereres og matches mot hverandre.
    """
    domain = article.get("domain", "")
    raw = (article.get("title") or "").lower()
    keys: list[str] = []
    full = re.sub(r"\W+", "", raw)
    if full:
        keys.append(f"{domain}|{full}")
    stripped = re.sub(r"\s*[-–|][^-–|]*$", "", raw)
    short = re.sub(r"\W+", "", stripped)
    if short and short != full and len(short) >= 20:
        keys.append(f"{domain}|{short}")
    return keys


def dedupe_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for article in articles:
        url_key = normalize_url(article.get("url") or "")
        keys = ([url_key] if url_key else []) + title_dedupe_keys(article)
        if not keys or any(key in seen for key in keys):
            continue
        seen.update(keys)
        deduped.append(article)
    return sorted(
        deduped,
        key=lambda item: item.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


# ---------------------------------------------------------------------------
# Klynging: samme sak i flere medier grupperes
# ---------------------------------------------------------------------------

_SOURCE_SUFFIX = re.compile(r"\s*[-–|]\s*[^-–|]{2,40}$")


def _title_tokens(title: str) -> set[str]:
    cleaned = _SOURCE_SUFFIX.sub("", title or "").lower()
    cleaned = cleaned.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    tokens = set(re.findall(r"[a-z0-9]{3,}", cleaned))
    return tokens - {"robert", "naess", "ness", "nordea"}


def _similar_titles(tokens_a: set[str], tokens_b: set[str]) -> bool:
    if not tokens_a or not tokens_b:
        return False
    overlap = len(tokens_a & tokens_b)
    return overlap / min(len(tokens_a), len(tokens_b)) >= 0.6 and overlap >= 3


def cluster_articles(articles: list[dict[str, Any]], max_gap_days: int = 3) -> list[dict[str, Any]]:
    """Grupper nyhetsartikler som dekker samme sak.

    Input antas sortert nyeste først. Sosialt/podkast/video klynges ikke
    (hver post er sin egen sak). Returnerer klynger nyeste først, der
    `primary` er artikkelen med best metadata og `also_in` er øvrige kilder.
    """
    clusters: list[dict[str, Any]] = []
    for article in articles:
        if article.get("category", "nyheter") != "nyheter":
            clusters.append({"articles": [article], "tokens": set()})
            continue
        tokens = _title_tokens(article.get("title") or "")
        published = article.get("published_at")
        placed = False
        for cluster in clusters:
            if not cluster["tokens"]:
                continue
            rep = cluster["articles"][0]
            rep_published = rep.get("published_at")
            if published and rep_published and abs((published - rep_published).days) > max_gap_days:
                continue
            if _similar_titles(tokens, cluster["tokens"]):
                cluster["articles"].append(article)
                placed = True
                break
        if not placed:
            clusters.append({"articles": [article], "tokens": tokens})

    result: list[dict[str, Any]] = []
    for cluster in clusters:
        members = cluster["articles"]
        primary = max(
            members,
            key=lambda art: (bool(art.get("snippet")), bool(art.get("published_at")), art.get("published_at") or datetime.min.replace(tzinfo=timezone.utc)),
        )
        also_in: list[str] = []
        for member in members:
            domain = member.get("domain") or ""
            if member is not primary and domain and domain != primary.get("domain") and domain not in also_in:
                also_in.append(domain)
        result.append({"primary": primary, "also_in": also_in, "count": len(members)})
    return result
