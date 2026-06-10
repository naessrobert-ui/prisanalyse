"""Persistent archive for the Robert Naess media monitor.

Live APIs (Google News RSS, GDELT, X) have weak or capped history, so the
monitor accumulates every observed mention in a parquet file on S3 (local
file fallback for dev). A Render cron job (scripts/oppdater_media_arkiv.py)
fetches sources, merges new rows into the archive and enriches them:

  - sentiment + tema via Anthropic (batch, only new rows, capped per run)
  - og:image + sitat (setningen som nevner navnet) fra artikkelsiden

Schema (one row per unique article id):
    id | title | url | source | domain | published_at | snippet | origin
    category | matched_query | engagement | first_seen_at
    sentiment | tema | sitat | image
"""

from __future__ import annotations

import io
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from media_sources import USER_AGENT, article_id, matches_name, title_dedupe_keys

load_dotenv()

_DEFAULT_S3_KEY = "media-mentions/robert-naess.parquet"
_LOCAL_PATH = Path(__file__).resolve().parent / "data" / "media_mentions_robert_naess.parquet"

_COLUMNS = [
    "id", "title", "url", "source", "domain", "published_at", "snippet",
    "origin", "category", "matched_query", "engagement", "first_seen_at",
    "sentiment", "tema", "sitat", "image",
]

_MEM_LOCK = threading.RLock()
_MEM_DF: Optional[pd.DataFrame] = None
_MEM_TS: float = 0.0
_MEM_TTL = 600.0

_MAX_LLM_PER_RUN = int(os.environ.get("MEDIA_ENRICH_MAX_LLM", "40"))
_MAX_PAGE_FETCH_PER_RUN = int(os.environ.get("MEDIA_ENRICH_MAX_PAGES", "15"))
_ENRICH_MODEL = os.environ.get("MEDIA_ENRICH_MODEL", "claude-haiku-4-5-20251001")


# ---------------------------------------------------------------------------
# S3 helpers (same pattern as scripts/station_metrics_cache.py)
# ---------------------------------------------------------------------------

def _s3_config() -> Optional[tuple[str, str]]:
    bucket = (
        os.environ.get("MEDIA_MENTIONS_S3_BUCKET", "").strip()
        or os.environ.get("S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        return None
    key = os.environ.get("MEDIA_MENTIONS_S3_KEY", _DEFAULT_S3_KEY).strip() or _DEFAULT_S3_KEY
    return bucket, key


def _s3_client():
    import boto3  # local import keeps boto3 optional outside Render
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")),
    )


def _empty_archive() -> pd.DataFrame:
    return pd.DataFrame(columns=_COLUMNS)


def load_archive(use_cache: bool = True) -> pd.DataFrame:
    """Les arkivet fra S3 (eller lokal fil). In-memory-cache for webruter."""
    global _MEM_DF, _MEM_TS
    with _MEM_LOCK:
        if use_cache and _MEM_DF is not None and time.time() - _MEM_TS < _MEM_TTL:
            return _MEM_DF

    df: Optional[pd.DataFrame] = None
    s3 = _s3_config()
    if s3 is not None:
        bucket, key = s3
        try:
            obj = _s3_client().get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        except Exception:
            df = None
    if df is None and _LOCAL_PATH.exists():
        try:
            df = pd.read_parquet(_LOCAL_PATH)
        except Exception:
            df = None
    if df is None:
        df = _empty_archive()

    for column in _COLUMNS:
        if column not in df.columns:
            df[column] = "" if column != "engagement" else 0
    df = df[_COLUMNS]

    with _MEM_LOCK:
        _MEM_DF = df
        _MEM_TS = time.time()
    return df


def save_archive(df: pd.DataFrame) -> str:
    global _MEM_DF, _MEM_TS
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    body = buffer.getvalue()

    destination: str
    s3 = _s3_config()
    if s3 is not None:
        bucket, key = s3
        _s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")
        destination = f"s3://{bucket}/{key}"
    else:
        _LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LOCAL_PATH.write_bytes(body)
        destination = str(_LOCAL_PATH)

    with _MEM_LOCK:
        _MEM_DF = df
        _MEM_TS = time.time()
    return destination


# ---------------------------------------------------------------------------
# Merge av nye artikler
# ---------------------------------------------------------------------------

def _article_to_row(article: dict[str, Any]) -> dict[str, Any]:
    published = article.get("published_at")
    return {
        "id": article_id(article),
        "title": article.get("title") or "",
        "url": article.get("url") or "",
        "source": article.get("source") or "",
        "domain": article.get("domain") or "",
        "published_at": published.isoformat() if isinstance(published, datetime) else (published or ""),
        "snippet": article.get("snippet") or "",
        "origin": article.get("origin") or "",
        "category": article.get("category") or "nyheter",
        "matched_query": article.get("matched_query") or "",
        "engagement": int(article.get("engagement") or 0),
        "first_seen_at": datetime.now(timezone.utc).isoformat(),
        "sentiment": "",
        "tema": "",
        "sitat": "",
        "image": "",
    }


def merge_articles(df: pd.DataFrame, articles: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
    """Legg nye artikler inn i arkivet. Eksisterende rader beholdes urørt
    (bortsett fra engagement, som oppdateres for sosiale poster)."""
    existing_ids = set(df["id"].astype(str)) if len(df) else set()
    existing_titles: set[str] = set()
    if len(df):
        for row in df.loc[:, ["title", "domain"]].to_dict("records"):
            existing_titles.update(title_dedupe_keys(row))
    new_rows: list[dict[str, Any]] = []
    engagement_updates: dict[str, int] = {}
    seen_new: set[str] = set()
    for article in articles:
        row_id = article_id(article)
        if row_id in existing_ids:
            engagement = int(article.get("engagement") or 0)
            if engagement:
                engagement_updates[row_id] = engagement
            continue
        title_keys = title_dedupe_keys(article)
        if any(key in existing_titles for key in title_keys):
            continue
        if row_id in seen_new:
            continue
        seen_new.add(row_id)
        existing_titles.update(title_keys)
        new_rows.append(_article_to_row(article))

    if engagement_updates and len(df):
        mask = df["id"].isin(engagement_updates)
        df.loc[mask, "engagement"] = df.loc[mask, "id"].map(engagement_updates)

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df, len(new_rows)


def archive_articles(df: pd.DataFrame, days: int) -> list[dict[str, Any]]:
    """Arkivrader som artikkel-dicts (published_at som datetime) for payload."""
    if not len(df):
        return []
    cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=days)
    articles: list[dict[str, Any]] = []
    for row in df.to_dict("records"):
        published = None
        raw = row.get("published_at") or ""
        if raw:
            try:
                published = datetime.fromisoformat(str(raw))
                if published.tzinfo is None:
                    published = published.replace(tzinfo=timezone.utc)
            except ValueError:
                published = None
        if published is not None and published < cutoff:
            continue
        articles.append(
            {
                "title": row.get("title") or "",
                "url": row.get("url") or "",
                "source": row.get("source") or "",
                "domain": row.get("domain") or "",
                "published_at": published,
                "snippet": row.get("snippet") or "",
                "origin": row.get("origin") or "",
                "category": row.get("category") or "nyheter",
                "matched_query": row.get("matched_query") or "",
                "engagement": int(row.get("engagement") or 0),
                "sentiment": _clean_value(row.get("sentiment")),
                "tema": _clean_value(row.get("tema")),
                "sitat": _clean_value(row.get("sitat")),
                "image": _clean_value(row.get("image")),
            }
        )
    return articles


def _clean_value(value: Any) -> str:
    """'-' markerer 'forsøkt, ingenting funnet' i arkivet - vis som tomt."""
    text = str(value or "")
    return "" if text == "-" else text


def enrichment_lookup(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """id -> {sentiment, tema, sitat, image} for rader som er beriket."""
    lookup: dict[str, dict[str, str]] = {}
    if not len(df):
        return lookup
    for row in df.loc[:, ["id", "sentiment", "tema", "sitat", "image"]].to_dict("records"):
        sentiment = _clean_value(row.get("sentiment"))
        tema = _clean_value(row.get("tema"))
        sitat = _clean_value(row.get("sitat"))
        image = _clean_value(row.get("image"))
        if sentiment or sitat or image:
            lookup[str(row["id"])] = {
                "sentiment": sentiment,
                "tema": tema,
                "sitat": sitat,
                "image": image,
            }
    return lookup


def archive_status(df: pd.DataFrame) -> dict[str, Any]:
    last_seen = ""
    if len(df) and df["first_seen_at"].astype(str).str.len().max() > 0:
        last_seen = df["first_seen_at"].astype(str).max()
    return {
        "enabled": _s3_config() is not None or _LOCAL_PATH.exists(),
        "rows": int(len(df)),
        "last_updated": last_seen,
    }


# ---------------------------------------------------------------------------
# Beriking: sentiment + tema via Anthropic, og:image + sitat fra siden
# ---------------------------------------------------------------------------

def _llm_client():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        import anthropic
        return anthropic.Anthropic()
    except Exception:
        return None


_ENRICH_PROMPT = """Du får en JSON-liste med medieomtaler av Robert Næss (investeringsdirektør i Nordea).
For hver omtale, vurder sentimentet FOR ROBERT NÆSS (ikke for markedet generelt) og sett ett kort tema.

Svar KUN med en JSON-liste, ingen annen tekst:
[{"id": "...", "sentiment": "positiv"|"nøytral"|"negativ", "tema": "1-3 ord på norsk"}]

Omtaler:
"""


def _enrich_sentiment_llm(df: pd.DataFrame, max_items: int = _MAX_LLM_PER_RUN) -> int:
    client = _llm_client()
    if client is None or not len(df):
        return 0

    pending = df[(df["sentiment"].astype(str) == "") & (df["title"].astype(str) != "")]
    pending = pending.sort_values("first_seen_at", ascending=False).head(max_items)
    if not len(pending):
        return 0

    items = [
        {"id": row["id"], "tittel": row["title"], "utdrag": (row.get("snippet") or "")[:200]}
        for row in pending.to_dict("records")
    ]
    try:
        response = client.messages.create(
            model=_ENRICH_MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": _ENRICH_PROMPT + json.dumps(items, ensure_ascii=False)}],
        )
        text = response.content[0].text.strip()
        match = re.search(r"\[.*\]", text, re.DOTALL)
        results = json.loads(match.group(0) if match else text)
    except Exception:
        return 0

    valid_sentiments = {"positiv", "nøytral", "negativ"}
    updated = 0
    by_id = {str(item.get("id")): item for item in results if isinstance(item, dict)}
    for index, row in pending.iterrows():
        item = by_id.get(str(row["id"]))
        if not item:
            continue
        sentiment = str(item.get("sentiment") or "").lower()
        if sentiment not in valid_sentiments:
            continue
        df.at[index, "sentiment"] = sentiment
        df.at[index, "tema"] = str(item.get("tema") or "")[:60]
        updated += 1
    return updated


_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def _extract_page_details(url: str) -> tuple[str, str]:
    """Hent og:image og setningen som nevner navnet fra artikkelsiden."""
    try:
        response = requests.get(url, timeout=8, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
    except requests.RequestException:
        return "", ""

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(response.text, "lxml")
    image = ""
    og_image = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "og:image"})
    if og_image and og_image.get("content", "").startswith("http"):
        image = og_image["content"][:500]

    quote = ""
    body_text = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p"))
    for sentence in _SENTENCE_SPLIT.split(body_text):
        if matches_name(sentence) and 40 <= len(sentence) <= 320:
            quote = sentence.strip()
            break
    return image, quote


def _enrich_page_details(df: pd.DataFrame, max_items: int = _MAX_PAGE_FETCH_PER_RUN) -> int:
    if not len(df):
        return 0
    pending = df[
        (df["category"].astype(str) == "nyheter")
        & (df["image"].astype(str) == "")
        & (df["sitat"].astype(str) == "")
        & df["url"].astype(str).str.startswith("http")
        & ~df["url"].astype(str).str.contains("news.google.com")
    ]
    pending = pending.sort_values("first_seen_at", ascending=False).head(max_items)

    updated = 0
    for index, row in pending.iterrows():
        image, quote = _extract_page_details(str(row["url"]))
        df.at[index, "image"] = image or "-"
        df.at[index, "sitat"] = quote or "-"
        if image or quote:
            updated += 1
    return updated


def update_archive_from_sources(days: int = 3, include_x: bool = True) -> dict[str, Any]:
    """Full oppdateringssyklus: hent kilder, merge, berik, lagre.

    Brukes av cron-jobben og av det manuelle API-endepunktet.
    """
    from media_sources import dedupe_articles, fetch_all_sources

    articles, x_status, errors = fetch_all_sources(days, include_x=include_x)
    articles = dedupe_articles(articles)

    df = load_archive(use_cache=False)
    df, new_count = merge_articles(df, articles)
    llm_count = _enrich_sentiment_llm(df)
    page_count = _enrich_page_details(df)
    destination = save_archive(df)

    return {
        "fetched": len(articles),
        "new": new_count,
        "llm_enriched": llm_count,
        "page_enriched": page_count,
        "rows_total": int(len(df)),
        "destination": destination,
        "x_state": x_status.get("state"),
        "errors": errors,
    }
