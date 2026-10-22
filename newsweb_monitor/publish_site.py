"""Publiser et MINIMALT, offentlig dashboard-snapshot fra GitHub Actions.

Only issuer-disclosed public headlines and short OWN summaries are exported.
Never publish the original PDFs, full texts, raw AI prompts, credentials or consensus.
The public GitHub repository is not suitable for private investment research.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from .generation import NEWSWEB_GENERATION
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
PUBLIC = ROOT / "public" / "latest.json"
LATEST = ROOT / "reports" / "latest.json"
BOOTSTRAP = ROOT / "bootstrap" / "latest.json"
MAX_ITEMS = 240

def limited(value: object, length: int) -> str:
    return str(value or "").strip()[:length]

def safe_url(value: object, *, document: bool = False) -> str:
    url=limited(value,400)
    try:
        parts=urlparse(url)
        if parts.scheme != "https":
            return ""
        hostname=(parts.hostname or "").lower()
        if document and hostname=="api3.oslo.oslobors.no" and parts.path=="/v1/newsreader/attachment":
            return url
        if not document and hostname=="newsweb.oslobors.no" and parts.path.startswith("/message/"):
            return url
    except ValueError:
        pass
    return ""

def num(value: object):
    if isinstance(value,bool) or value is None:
        return None
    try:
        f=float(value)
        if not (-1e16 < f < 1e16):
            return None
        return round(f, 4)
    except (TypeError,ValueError,OverflowError):
        return None

def sanitize(item: dict) -> dict | None:
    if not isinstance(item,dict):
        return None
    ident=limited(item.get("id"),35)
    ticker=limited(item.get("ticker"),30)
    if not ident or not ticker:
        return None
    # The publisher is for publicly disseminated XOSL announcements.
    if item.get("market","XOSL")!="XOSL":
        return None
    score=num(item.get("score"))
    score=int(score) if score is not None else 0
    score=min(max(score,0),100)
    result={
        "id":ident,"market":"XOSL","ticker":ticker,
        "issuer":limited(item.get("issuer"),130),
        "title":limited(item.get("title"),260),
        "headline":limited(item.get("headline"),115),
        "lead":limited(item.get("lead"),270),
        "published_at":limited(item.get("published_at"),48),
        "url":safe_url(item.get("url")),
        "summary":limited(item.get("summary"),550),
        "reason":limited(item.get("reason"),750),
        "score":score,
        "skipped":bool(item.get("skipped")),
        "direction":limited(item.get("direction"),25),
        "assessment_source":limited(item.get("assessment_source"),90)
    }
    # Public original message links, including both language versions.
    versions=item.get("language_versions")
    if isinstance(versions,list):
        clean=[]
        seen_versions=set()
        for version in versions[:8]:
            if not isinstance(version,dict):continue
            vid=limited(version.get("id"),35)
            url=safe_url(version.get("url"))
            if vid and url and vid not in seen_versions:
                clean.append({"id":vid,"title":limited(version.get("title"),260),
                    "url":url,"published_at":limited(version.get("published_at"),48),
                    "language":limited(version.get("language"),20)})
                seen_versions.add(vid)
        if len(clean)>1:result["language_versions"]=clean
    report=item.get("financial_report")
    if isinstance(report,dict):
        document=report.get("document") or {}
        if not isinstance(document,dict): document={}
        fin={
          "status":limited(report.get("status"),50),
          "period":limited(report.get("period"),12),
          "previous_report":limited(report.get("previous_report"),60),
          "document":{
              "name":limited(document.get("name"),160),
              "url":safe_url(document.get("url"),document=True)
          },
          "metrics":[]
        }
        for metric in (report.get("metrics") or [])[:14]:
            if not isinstance(metric,dict):continue
            fin["metrics"].append({
                "label":limited(metric.get("label"),65),
                "name":limited(metric.get("name"),40),
                "unit":limited(metric.get("unit"),50),
                "current":num(metric.get("current")),
                "prior_year":num(metric.get("prior_year")),
                "prev_quarter":num(metric.get("prev_quarter")),
                "consensus":None, # Proprietary estimates must not be published.
                "yoy_pct":num(metric.get("yoy_pct")),
                "qoq_pct":num(metric.get("qoq_pct")),
                "surprise_pct":None,
                "evidence":limited(metric.get("evidence"),160)
            })
        result["financial_report"]=fin
    return result

def build_snapshot(report:dict, old:dict|None=None, bootstrap:dict|None=None) -> tuple[dict,bool]:
    """Merge new announcements without losing earlier items when next run finds zero."""
    generation=report.get("generation") or ""
    # A change of generation is an intentional reset. Never resurrect records
    # from the previous public snapshot or legacy bootstrap artifact.
    reset=bool(generation) and (old or {}).get("generation")!=generation
    if reset:
        old=None
        bootstrap=None
    old_items=(old or {}).get("items") or []
    first_items=(bootstrap or {}).get("items") or []
    current_items=report.get("items") or []
    selected={}
    for item in [*first_items,*old_items,*current_items]:
        cleaned=sanitize(item)
        if cleaned:
            selected[cleaned["id"]]=cleaned
    if not selected:
        raise ValueError("No NewsWeb data. Refusing to replace dashboard with an empty report.")
    # Merge only near-simultaneous, confidently equivalent NO/EN disclosures.
    # Apply to OLD + NEW together, so existing duplicate pairs disappear with
    # the next normal scheduled publication, without a cache/version reset.
    from .dedup import collapse_stories
    rows=collapse_stories(list(selected.values()))[:MAX_ITEMS]
    merged={
        "mode":"live",
        "generation": generation or (old or {}).get("generation",""),
        "source":"NewsWeb – automatisk fra GitHub Actions",
        "generated_at": limited(report.get("generated_at"),48) if current_items else
                        limited((old or bootstrap or {}).get("generated_at"),48),
        "items":rows
    }
    # Heartbeat updates on every completed source check, even if zero new
    # disclosures. Do not confuse last_checked_at with news published_at.
    diag=report.get("diagnostics")
    if isinstance(diag,dict) and diag:
        allowed=("fetched","recent","already_seen","eligible","filtered",
                 "analyzed","ai_failed","details_failed","capped","source_latest_at")
        merged["last_checked_at"]=limited(report.get("generated_at"),48)
        merged["monitor_stats"]={key:diag.get(key) for key in allowed
                                 if isinstance(diag.get(key),(int,str))}
    old_rows=(old or {}).get("items") or []
    updated = not old or rows!=old_rows or (
        "last_checked_at" in merged and (
            merged["last_checked_at"]!=(old or {}).get("last_checked_at") or
            merged.get("monitor_stats")!=(old or {}).get("monitor_stats")))
    return merged,updated

def read_json(path:Path) -> dict|None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        value=json.load(f)
    return value if isinstance(value,dict) else None

def main() -> None:
    report=read_json(LATEST)
    if not report or report.get("source")!="NEWSWEB":
        print("Not publishing: this was not a NewsWeb production run")
        return
    old=read_json(PUBLIC)
    bootstrap=None  # Never import old rule-based archives after the reset
    result,changed=build_snapshot(report,old,bootstrap)
    if not changed:
        print(f"No new disclosures; preserved {len(result['items'])} published messages")
        return
    PUBLIC.parent.mkdir(parents=True,exist_ok=True)
    PUBLIC.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(f"Published {len(result['items'])} public summaries (excluded full body/PDF/secrets)")

if __name__=="__main__":
    main()
