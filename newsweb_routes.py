"""NewsWeb dashboard fed by SANITIZED GitHub Actions JSON, never legacy demo data."""
from __future__ import annotations

import json
import threading
import time
from urllib.request import Request, urlopen

import os
import secrets
from flask import Blueprint, render_template, request, redirect, session, url_for, abort

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

def _can_view_consensus() -> bool:
    # Reuse the existing site-wide access code, not an additional password.
    # Fail CLOSED if the site's password is ever disabled.
    return bool(os.environ.get("SITE_ACCESS_CODE", "").strip()) and (
        session.get("access_granted") is True
    )

def _private_report_comparisons(items: list[dict]) -> tuple[int,str]:
    """Add private values only to the admin's in-memory response.

    No amounts or derived surprises get pushed into public GitHub.
    """
    from newsweb_consensus import list_snapshots,load_snapshot,safe_comparisons
    if not _can_view_consensus():
        return 0,""
    try:
        stored=list_snapshots(limit=30)
        snapshots=[load_snapshot(x["key"]) for x in stored]
    except Exception:
        return 0,"Privat estimatlagring er midlertidig utilgjengelig."
    count=0
    for item in items:
        result=safe_comparisons(item,snapshots)
        if result:
            item["consensus_comparison"]=result
            count+=1
    return count,""

def _admin_form(*,message="",error="",summary=None,history=None):
    from flask import make_response
    # CSRF protection is still necessary even after reusing existing login.
    token=session.get("newsweb_consensus_csrf")
    if not token:
        token=secrets.token_urlsafe(24)
        session["newsweb_consensus_csrf"]=token
    response=make_response(render_template("newsweb_consensus.html",
        allowed=_can_view_consensus(),message=message,error=error,
        summary=summary,history=history or [],csrf=token))
    response.headers["Cache-Control"]="private, no-store, max-age=0"
    response.headers["Pragma"]="no-cache"
    return response

@newsweb_bp.route("/konsensus/",methods=["GET","POST"])
def consensus_upload():
    """Protected by exactly the same SITE_ACCESS_CODE session as the rest of the site."""
    if not _can_view_consensus():
        # Critical: if site login configuration is absent, NEVER expose uploads.
        abort(403)
    if request.method=="POST":
        expected=session.get("newsweb_consensus_csrf")
        actual=request.form.get("csrf","")
        if not expected or not secrets.compare_digest(str(expected),actual):
            abort(400)
        if request.form.get("action")!="upload":
            abort(400)
        from newsweb_consensus import (MAX_XLSX_BYTES,parse_excel,
                                       store_snapshot,simulate_today)
        doc=request.files.get("file")
        if not doc or not doc.filename or not doc.filename.lower().endswith(".xlsx"):
            return _admin_form(error="Velg en Excel-fil (*.xlsx)."),400
        content=doc.stream.read(MAX_XLSX_BYTES+1)
        try:
            year_text=request.form.get("annual_year","")
            year=int(year_text) if year_text else None
            confirmed=request.form.get("confirmed_units")=="yes"
            parsed=parse_excel(content,annual_year=year,
                amount_units_confirmed=confirmed)
        except (ValueError,TypeError) as exc:
            return _admin_form(error=str(exc)),400

        # Rehearsal immediately on ALL announcements from current Oslo date
        # already published by the hourly NewsWeb job; no private data exported.
        # The snapshot is SIMULATED as if available one day earlier. Actual
        # upload time is recorded by store_snapshot and NEVER backdated.
        report,_=read_report()
        try:
            parsed["simulation"]=simulate_today(
                parsed,report.get("items",[]))
        except Exception:
            return _admin_form(error="Testkjøring feilet. Ingen estimater er lagret."),503
        try:
            store_snapshot(parsed,original_name=doc.filename)
        except (ValueError,RuntimeError) as exc:
            return _admin_form(error=str(exc)),400
        except Exception:
            return _admin_form(error="Lagring i privat S3 feilet. Sjekk Render-oppsettet."),503
        return redirect(url_for("newsweb.consensus_upload",uploaded="1"))

    try:
        from newsweb_consensus import list_snapshots,load_snapshot
        history=list_snapshots(limit=12)
        top=load_snapshot(history[0]["key"]) if history else None
    except Exception:
        return _admin_form(error="Fant ikke den private S3-bøtten. Sjekk NEWSWEB_CONSENSUS_BUCKET, Block Public Access og AWS-tilgang.")
    summary=top and {k:top.get(k) for k in (
        "uploaded_at","company_count","value_count","invalid_or_missing_cells",
        "periods","annual_year","original_name","amount_units_confirmed","simulation")}
    message=""
    if request.args.get("uploaded")=="1":
        message=("Estimatene er lagret privat. Simulert gjennomgang av dagens "
                 "meldinger er fullført og vises nedenfor.")
    return _admin_form(message=message,summary=summary,history=history)

@newsweb_bp.get("/")
def dashboard():
    report,demo=read_report()
    # Copy records: private values must never be inserted into the site-wide cache.
    items=[dict(item) for item in report.get("items",[])[:240]]
    matches,private_error=_private_report_comparisons(items)
    items=sorted(items,key=lambda x:(int(x.get("score") or 0),str(x.get("published_at") or "")),reverse=True)
    focus=sum(1 for x in items if int(x.get("score") or 0)>=75 and not x.get("skipped"))
    interesting=sum(1 for x in items if 45<=int(x.get("score") or 0)<75 and not x.get("skipped"))
    ignored=sum(1 for x in items if x.get("skipped") or int(x.get("score") or 0)<45)
    response=render_template("newsweb_dashboard.html",report=report,items=items,
                           demo=demo,focus=focus,interesting=interesting,ignored=ignored,
                           can_view_consensus=_can_view_consensus(),private_error=private_error,
                           consensus_matches=matches)
    if _can_view_consensus():
        from flask import make_response
        secure=make_response(response)
        secure.headers["Cache-Control"]="private, no-store, max-age=0"
        secure.headers["Pragma"]="no-cache"
        return secure
    return response
