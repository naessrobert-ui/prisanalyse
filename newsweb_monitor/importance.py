"""Vekting av viktige selskaper og sektorhint, styrt av viktige_selskaper.json."""
from __future__ import annotations

import json
import re
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


ASA = re.compile(r"\bASA\b")


def is_asa(issuer: str) -> bool:
    return bool(ASA.search(str(issuer or "")))


def apply(entry: dict) -> dict:
    """Juster AI-skåren: viktige selskaper opp, ikke-ASA ned. AI-skåren beholdes i score_ai."""
    name, boost = tier(entry.get("ticker"))
    ai_score = int(entry.get("score") or 0)
    score = ai_score
    if name:
        entry["importance"] = name
        if boost and ai_score >= int(config().get("min_ai_score", 40)):
            score += boost
    else:
        rule = config().get("ikke_asa") or {}
        issuer = str(entry.get("issuer") or "")
        exempt = rule.get("unntak_regex") and re.search(rule["unntak_regex"], issuer, re.I)
        if issuer and not is_asa(issuer) and not exempt and rule.get("trekk"):
            entry["company_form"] = "ikke ASA"
            score -= int(rule["trekk"])
    score = min(100, max(0, score))
    if score != ai_score:
        entry["score_ai"] = ai_score
        entry["score"] = score
    return entry


def sector_peers(ticker: str) -> dict[str, list[str]]:
    ticker = str(ticker or "").upper()
    return {sector: [t for t in members if t != ticker]
            for sector, members in (config().get("sektorer") or {}).items() if ticker in members}
