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
    from newsweb_consensus import list_snapshots,load_snapshot,overlay_private_consensus
    if not _can_view_consensus():
        return 0,0,""
    try:
        stored=list_snapshots(limit=30)
        snapshots=[load_snapshot(x["key"]) for x in stored]
    except Exception:
        return 0,0,"Privat estimatlagring er midlertidig utilgjengelig."
    actual,simulated=overlay_private_consensus(items,snapshots)
    return actual,simulated,""

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

def _private_consensus_headline(item:dict)->None:
    """Promote a verified pre-publication earnings surprise in private view only.

    Never rewrite the public GitHub news JSON or promote simulated estimates
    to factual 'better than expected' claims.
    """
    if item.get("consensus_kind")!="actual":
        return
    comparisons=item.get("consensus_comparison") or []
    priority={"ebit":0,"ebitda":1,"net_income":2,"eps":3,"net_interest_income":4}
    eligible=[row for row in comparisons
              if row.get("metric") in priority
              and isinstance(row.get("surprise_pct"),(int,float))
              and isinstance(row.get("actual"),(int,float))
              and isinstance(row.get("consensus"),(int,float))
              and row.get("consensus")>0]
    if not eligible:return
    best=min(eligible,key=lambda x:priority[x["metric"]])
    delta=float(best["surprise_pct"])
    if abs(delta)<5:return
    company=str(item.get("issuer") or item.get("ticker") or "Selskapet")
    company=company.replace(" ASA","").replace(" AS","")[:42]
    direction="over" if delta>0 else "under"
    label=str(best.get("label") or best["metric"])
    item["headline"]=f"{company}: {label} {abs(delta):.0f} % {direction} konsensus (indikativt)"
    item["lead"]=(f"{label} ble {best['actual']:g} mot forventet {best['consensus']:g} "
                  f"({delta:+.1f} %). Sammenlignet med private estimater lagret før "
                  "børsmeldingen. Valuta og definisjoner må verifiseres.").strip()
    item["consensus_headline"]=True

_dispatch_lock=threading.Lock()
_last_dispatch=[0.0]

@newsweb_bp.post("/kjor-na/")
def run_now():
    """Explicit, authenticated, CSRF-protected GitHub workflow dispatch."""
    if not _can_view_consensus():
        abort(403)
    expected=session.get("newsweb_refresh_csrf")
    supplied=request.form.get("csrf","")
    if not expected or not secrets.compare_digest(str(expected),str(supplied)):
        abort(400)
    token=os.getenv("NEWSWEB_GITHUB_TOKEN","").strip()
    if not token:
        return redirect(url_for("newsweb.dashboard",run_status="not_configured"))
    with _dispatch_lock:
        now=time.monotonic()
        if now-_last_dispatch[0]<180:
            return redirect(url_for("newsweb.dashboard",run_status="cooldown"))
        _last_dispatch[0]=now
    url=("https://api.github.com/repos/naessrobert-ui/prisanalyse/"
         "actions/workflows/newsweb-monitor.yml/dispatches")
    payload=json.dumps({"ref":"main","inputs":{
        "mode":"newsweb","dry_run":"true","history_backfill":"true"}}).encode()
    try:
        req=Request(url,data=payload,method="POST",headers={
            "Authorization":"Bearer "+token,
            "Accept":"application/vnd.github+json",
            "X-GitHub-Api-Version":"2022-11-28",
            "Content-Type":"application/json",
            "User-Agent":"Prisanalyse-NewsWeb/1.0"})
        with urlopen(req,timeout=12) as response:
            if response.status!=204:
                raise ValueError("Unexpected workflow dispatch status")
    except Exception:
        with _dispatch_lock:
            _last_dispatch[0]=0.0
        return redirect(url_for("newsweb.dashboard",run_status="failed"))
    return redirect(url_for("newsweb.dashboard",run_status="queued"))

@newsweb_bp.get("/")
def dashboard():
    report,demo=read_report()
    # Copy records: private values must never be inserted into the site-wide cache.
    items=[dict(item) for item in report.get("items",[])[:240]]
    actual_matches,simulated_matches,private_error=_private_report_comparisons(items)
    # Also fold the current public snapshot in memory, so historical bilingual
    # duplicates disappear immediately after the web deploy, before cron runs.
    from newsweb_monitor.dedup import collapse_stories
    items=collapse_stories(items)
    for item in items:
        _private_consensus_headline(item)
        comparable=item.get("consensus_comparison") or []
        valid=[abs(float(row["surprise_pct"])) for row in comparable
               if isinstance(row.get("surprise_pct"),(int,float))]
        item["largest_surprise_abs"]=max(valid,default=-1)
    items=sorted(items,key=lambda x:str(x.get("published_at") or ""),reverse=True)
    focus=sum(1 for x in items if int(x.get("score") or 0)>=75 and not x.get("skipped"))
    interesting=sum(1 for x in items if 45<=int(x.get("score") or 0)<75 and not x.get("skipped"))
    ignored=sum(1 for x in items if x.get("skipped") or int(x.get("score") or 0)<45)
    if _can_view_consensus() and not session.get("newsweb_refresh_csrf"):
        session["newsweb_refresh_csrf"]=secrets.token_urlsafe(32)
    run_status=request.args.get("run_status","")
    if run_status not in ("queued","cooldown","failed","not_configured"):
        run_status=""
    response=render_template("newsweb_dashboard.html",report=report,items=items,
                           refresh_csrf=session.get("newsweb_refresh_csrf","") if _can_view_consensus() else "",
                           run_status=run_status,
                           demo=demo,focus=focus,interesting=interesting,ignored=ignored,
                           can_view_consensus=_can_view_consensus(),private_error=private_error,
                           consensus_matches=actual_matches,
                           simulated_matches=simulated_matches)
    if _can_view_consensus():
        from flask import make_response
        secure=make_response(response)
        secure.headers["Cache-Control"]="private, no-store, max-age=0"
        secure.headers["Pragma"]="no-cache"
        return secure
    return response
