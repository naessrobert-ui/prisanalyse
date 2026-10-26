"""NewsWeb dashboard fed by SANITIZED GitHub Actions JSON, never legacy demo data."""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import os
import secrets
from flask import Blueprint, render_template, request, redirect, session, url_for, abort

newsweb_bp = Blueprint("newsweb", __name__, url_prefix="/newsweb")


_OSLO=ZoneInfo("Europe/Oslo")


@newsweb_bp.app_template_filter("norsk_tid")
def norsk_tid(verdi) -> str:
    """ISO-tidspunkt (UTC eller med sone) som norsk tid, «2026-10-26 09:12». Ukjent format vises som det er."""
    tekst=str(verdi or "").strip()
    if not tekst:
        return ""
    try:
        t=datetime.fromisoformat(tekst.replace("Z","+00:00"))
    except ValueError:
        return tekst
    if t.tzinfo is None:
        t=t.replace(tzinfo=timezone.utc)
    return t.astimezone(_OSLO).strftime("%Y-%m-%d %H:%M")
# Public metadata only. Never use this path for proprietary consensus or PDFs.
PUBLIC_REPORT_URL = (
    "https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/"
    "main/newsweb_monitor/public/latest.json"
)
# Same file via the API: no 5-minute raw CDN cache, so a finished run shows up
# immediately. Only used with NEWSWEB_GITHUB_TOKEN (unauthenticated = 60 req/h).
PUBLIC_REPORT_API_URL = (
    "https://api.github.com/repos/naessrobert-ui/prisanalyse/"
    "contents/newsweb_monitor/public/latest.json?ref=main"
)
_CACHE_SECONDS = 120
# With a token, 304 responses are free, so the page can check much more often.
_API_CACHE_SECONDS = 30
_cache_lock = threading.Lock()
_cache = {"checked_at": 0.0, "report": None, "etag": "", "fresh_until": 0.0}

def _empty_report(message: str) -> dict:
    return {"mode": "pending", "source": "NewsWeb – venter på rapport",
            "generated_at": "", "items": [], "error": message}

def _parse_snapshot(data: bytes) -> dict:
    if len(data)>750_000:
        raise ValueError("NewsWeb snapshot exceeds expected file size")
    payload=json.loads(data)
    if not isinstance(payload,dict) or payload.get("mode")!="live" or not isinstance(payload.get("items"),list):
        raise ValueError("Invalid published NewsWeb snapshot")
    return payload

def _fetch_from_api(token: str, etag: str) -> tuple[dict|None, str]:
    """Return (report, etag); report None means 304 Not Modified."""
    headers={"Accept": "application/vnd.github.raw+json",
             "Authorization": "Bearer "+token,
             "X-GitHub-Api-Version": "2022-11-28",
             "User-Agent": "Prisanalyse-NewsWeb/1.0"}
    if etag:
        headers["If-None-Match"]=etag
    try:
        with urlopen(Request(PUBLIC_REPORT_API_URL, headers=headers), timeout=5) as response:
            data=response.read(750_001)
            new_etag=response.headers.get("ETag","")
    except HTTPError as exc:
        if exc.code==304:
            return None, etag
        raise
    return _parse_snapshot(data), new_etag

def _fetch_from_raw() -> dict:
    req = Request(PUBLIC_REPORT_URL, headers={
        "Accept": "application/json", "User-Agent": "Prisanalyse-NewsWeb/1.0"})
    with urlopen(req, timeout=5) as response:
        return _parse_snapshot(response.read(750_001))

def expect_fresh_report(seconds: float = 900) -> None:
    """After a manual run: skip the cache window so the new snapshot shows up at once."""
    with _cache_lock:
        _cache["fresh_until"]=time.monotonic()+seconds

def read_report() -> tuple[dict, bool]:
    """Fetch at most every 30 s (API) / 120 s (raw) per worker; keep last good report if GitHub is briefly down."""
    token=os.getenv("NEWSWEB_GITHUB_TOKEN","").strip()
    now = time.monotonic()
    with _cache_lock:
        ttl=_API_CACHE_SECONDS if token else _CACHE_SECONDS
        if now < _cache["fresh_until"]:
            ttl=min(ttl,10)
        if now - _cache["checked_at"] < ttl and _cache["report"]:
            result=_cache["report"]
            return result, False
        _cache["checked_at"] = now
        etag=_cache["etag"] if _cache["report"] else ""

    result = None
    try:
        payload=None
        if token:
            try:
                payload,etag=_fetch_from_api(token,etag)
                if payload is None:
                    with _cache_lock:
                        if _cache["report"]:
                            return _cache["report"], False
                    raise ValueError("304 without cached report")
            except (OSError,ValueError,TypeError):
                payload=None
                etag=""
        if payload is None:
            payload=_fetch_from_raw()
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
            if not result.get("error"):
                _cache["report"]=result
                _cache["etag"]=etag
        else:
            _cache["report"]=None
            _cache["etag"]=""
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
    expect_fresh_report()
    return redirect(url_for("newsweb.dashboard",run_status="queued"))

_TRADES_CACHE_SECONDS = 1800
_trades_lock = threading.Lock()
_trades_lists_lock = threading.Lock()
_trades_cache: dict = {}

def _trades_db_key() -> float:
    import handler_data as hd
    try:
        return os.path.getmtime(hd.HANDLER_DB_PATH)
    except OSError:
        return 0.0

def _trades_list_ids(conn) -> dict[str, list[str]]:
    """Investor-IDer for Viktige/Beste, cachet per databaseversjon."""
    import handler_routes
    from newsweb_handler import LISTER
    key=("lister",_trades_db_key())
    # The dashboard fires many panel requests at once: compute once, let the rest wait.
    with _trades_lists_lock:
        with _trades_lock:
            hit=_trades_cache.get(key)
        if hit and hit[0]>time.monotonic():
            return hit[1]
        ids={liste:handler_routes._load_investor_ids_for_beste_viktige(conn,liste,None,None)
             for liste in LISTER}
        ids={liste:v for liste,v in ids.items() if v}
        with _trades_lock:
            _trades_cache[key]=(time.monotonic()+_TRADES_CACHE_SECONDS,ids)
    return ids

_TRADES_BATCH_MAX = 40

def _trades_request(raw_ticker:str, raw_published:str):
    """Validated (ticker, date) or None."""
    import re
    from newsweb_handler import meldingsdato
    ticker=str(raw_ticker or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,14}",ticker):
        return None
    dato=meldingsdato(raw_published)
    return (ticker,dato) if dato else None

def _trades_lookup(wanted:list[tuple]) -> dict:
    """{(ticker,date): data}. One DB connection, queries run one after another:
    many parallel requests on a single worker are much slower than one batch."""
    from newsweb_handler import handler_for_melding
    db_key=_trades_db_key()
    found,missing={},[]
    now=time.monotonic()
    with _trades_lock:
        for want in wanted:
            hit=_trades_cache.get((want[0],want[1].isoformat(),db_key))
            if hit and hit[0]>now:found[want]=hit[1]
            elif want not in missing:missing.append(want)
    if not missing:
        return found
    import handler_data as hd
    try:
        conn=hd.db_connect()
    except Exception:
        return {**found,**{w:{"error":"Handler-databasen er ikke tilgjengelig."} for w in missing}}
    try:
        lists=_trades_list_ids(conn)
        for ticker,dato in missing:
            try:
                data=handler_for_melding(conn,ticker,dato,lists)
            except Exception:
                from flask import current_app
                current_app.logger.exception("NewsWeb handler for %s %s",ticker,dato)
                found[(ticker,dato)]={"error":"Klarte ikke hente handler."}
                continue
            found[(ticker,dato)]=data
            with _trades_lock:
                if len(_trades_cache)>500:
                    _trades_cache.clear()
                _trades_cache[(ticker,dato.isoformat(),db_key)]=(time.monotonic()+_TRADES_CACHE_SECONDS,data)
    except Exception:
        from flask import current_app
        current_app.logger.exception("NewsWeb handler: investorlister")
        for w in missing:
            found.setdefault(w,{"error":"Klarte ikke hente handler."})
    finally:
        conn.close()
    return found

@newsweb_bp.get("/handler")
def trades_before_message():
    """Private: trades in the stock before a message. Never published to GitHub.

    Single: ?ticker=ABC&published_at=ISO. Batch: ?m=ABC|ISO&m=DEF|ISO
    returns {"results": {"ABC|ISO": data, ...}}.
    """
    from flask import jsonify, make_response
    if not _can_view_consensus():
        abort(403)
    batch=request.args.getlist("m")
    if batch:
        if len(batch)>_TRADES_BATCH_MAX:
            abort(400)
        parsed={}
        for raw in batch:
            ticker,_,published=raw.partition("|")
            want=_trades_request(ticker,published)
            if want is None:
                abort(400)
            parsed[raw]=want
        found=_trades_lookup(list(parsed.values()))
        body={"results":{raw:found[want] for raw,want in parsed.items()}}
        status=200
    else:
        want=_trades_request(request.args.get("ticker",""),request.args.get("published_at",""))
        if want is None:
            abort(400)
        body=_trades_lookup([want])[want]
        status=503 if body.get("error")=="Handler-databasen er ikke tilgjengelig." else (
            500 if body.get("error") else 200)
    response=make_response(jsonify(body),status)
    response.headers["Cache-Control"]="private, no-store, max-age=0"
    return response

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
