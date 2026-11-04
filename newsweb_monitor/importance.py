"""Vekting av viktige selskaper og sektorhint, styrt av viktige_selskaper.json."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

PATH = Path(__file__).resolve().parent / "viktige_selskaper.json"


@lru_cache(maxsize=1)
def config() -> dict:
    try:
        return json.loads(PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def tier(ticker: str) -> tuple[str, int]:
    ticker = str(ticker or "").upper()
    for name, level in (config().get("nivaer") or {}).items():
        if ticker in (level.get("tickere") or []):
            return name, int(level.get("tillegg") or 0)
    return "", 0


def apply(entry: dict) -> dict:
    """Legg til vekting for viktige selskaper. AI-skåren beholdes i score_ai."""
    name, boost = tier(entry.get("ticker"))
    if not name:
        return entry
    entry["importance"] = name
    ai_score = int(entry.get("score") or 0)
    if boost and ai_score >= int(config().get("min_ai_score", 40)):
        entry["score_ai"] = ai_score
        entry["score"] = min(100, ai_score + boost)
    return entry


def sector_peers(ticker: str) -> dict[str, list[str]]:
    ticker = str(ticker or "").upper()
    return {sector: [t for t in members if t != ticker]
            for sector, members in (config().get("sektorer") or {}).items() if ticker in members}
