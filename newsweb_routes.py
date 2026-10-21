"""NewsWeb dashboard fed by SANITIZED GitHub Actions JSON, never legacy demo data."""
from __future__ import annotations

import json
import threading
import time
from urllib.request import Request, urlopen

from flask import Blueprint, render_template

newsweb_bp = Blueprint("newsweb", __name__, url_prefix="/newsweb")
# Public metadata only. Never use this path for proprietary consensus or PDFs.
PUBLIC_REPORT_URL = (
    "https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/"
    "main/newsweb_monitor/public/latest.json"
)
_CACHE_SECONDS = 120
_cache_lock = threading.Lock()
_cache = {"checked_at": 0.0, "report": None}

def _empty_report(message: str) -> dict:
    return {"mode": "pending", "source": "NewsWeb – venter på rapport",
            "generated_at": "", "items": [], "error": message}

def read_report() -> tuple[dict, bool]:
    """Fetch once per 120 s per worker; keep last good report if GitHub is briefly down."""
    now = time.monotonic()
    with _cache_lock:
        if now - _cache["checked_at"] < _CACHE_SECONDS and _cache["report"]:
            result=_cache["report"]
            return result, False
        _cache["checked_at"] = now

    result = None
    try:
        req = Request(PUBLIC_REPORT_URL, headers={
            "Accept": "application/json", "User-Agent": "Prisanalyse-NewsWeb/1.0"})
        with urlopen(req, timeout=5) as response:
            data = response.read(750_001)
        if len(data)>750_000:
            raise ValueError("NewsWeb snapshot exceeds expected file size")
        payload=json.loads(data)
        if not isinstance(payload,dict) or payload.get("mode")!="live" or not isinstance(payload.get("items"),list):
            raise ValueError("Invalid published NewsWeb snapshot")
        result=payload
    except (OSError,ValueError,TypeError) as exc:
        with _cache_lock:
            if _cache["report"]:
                result=dict(_cache["report"])
                result["error"]="Viser sist tilgjengelige rapport. Klarte ikke å hente ferske data."
        if result is None:
            result=_empty_report(
                "GitHub-jobben er aktiv, men den første publiserte rapporten er ennå ikke tilgjengelig. "
                "Se GitHub Actions for kjørestatus.")
    with _cache_lock:
        # Do not replace last valid cache with an error/empty response.
        if result.get("mode")=="live":
            _cache["report"]=result
        else:
            _cache["report"]=None
    return result, False

@newsweb_bp.get("/")
def dashboard():
    report,demo=read_report()
    items=report.get("items",[])[:240]
    items=sorted(items,key=lambda x:(int(x.get("score") or 0),str(x.get("published_at") or "")),reverse=True)
    focus=sum(1 for x in items if int(x.get("score") or 0)>=75 and not x.get("skipped"))
    interesting=sum(1 for x in items if 45<=int(x.get("score") or 0)<75 and not x.get("skipped"))
    ignored=sum(1 for x in items if x.get("skipped") or int(x.get("score") or 0)<45)
    return render_template("newsweb_dashboard.html",report=report,items=items,
                           demo=demo,focus=focus,interesting=interesting,ignored=ignored)
