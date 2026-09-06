"""ImportRadar UI and bounded background jobs shared between Flask workers."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time

from flask import Blueprint, current_app, jsonify, render_template, request, session, Response

from import_radar import Settings, number
from import_radar_search import Search, SourceError, fx_rates, run_search

import_radar_bp = Blueprint("import_radar", __name__, url_prefix="/bil/import-radar")
_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="import-radar")


def connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS jobs
               (id TEXT PRIMARY KEY, owner TEXT NOT NULL, created REAL NOT NULL,
                status TEXT NOT NULL, payload TEXT, error TEXT)""")
    return db


def db_path():
    return current_app.config.get("IMPORT_RADAR_DB_PATH") or os.environ.get(
        "IMPORT_RADAR_DB_PATH", "/tmp/prisanalyse-import-radar/jobs.sqlite3")


def parse_request(data):
    if not isinstance(data, dict):
        raise ValueError("Ugyldig søk")
    def num(key, default=None):
        raw = data.get(key, default)
        if raw in (None, ""):
            return None
        return number(raw, key)
    search = Search(make=str(data.get("make", "")).strip(), model=str(data.get("model", "")).strip(),
                    year_from=num("year_from", 2022), year_to=num("year_to", date.today().year),
                    max_km=num("max_km", 90000), drive=data.get("drive", "ANY"),
                    min_battery_kwh=num("min_battery_kwh", 0), vat_only=data.get("vat_only", True),
                    per_source=num("per_source", 5))
    # Dataclass fields are annotated int; normalize validated integer input explicitly.
    from dataclasses import replace
    search = replace(search, year_from=int(search.year_from), year_to=int(search.year_to),
                     max_km=int(search.max_km), per_source=int(search.per_source))
    eur, sek = num("eur_nok"), num("sek_nok")
    if (eur is None) != (sek is None):
        raise ValueError("Oppgi begge valutakurser eller la begge være tomme")
    registration = date.fromisoformat(data.get("registration_date") or date.today().isoformat())
    options = {"registration_date": registration,
               "freight_de_nok": num("freight_de_nok", 13000), "freight_se_nok": num("freight_se_nok", 8000),
               "target_margin_nok": num("target_margin_nok", 30000),
               "fx_buffer_pct": num("fx_buffer_pct", 0), "other_costs_nok": num("other_costs_nok"),
               "reserve_nok": num("reserve_nok", 0), "price_basis": "hurtigpris"}
    Settings(eur_nok=eur if eur is not None else 1, sek_nok=sek if sek is not None else 1, **options)
    if options["fx_buffer_pct"] > 25:
        raise ValueError("Valutapåslag må være mellom 0 og 25 prosent")
    weight = num("assumed_weight_kg")
    if weight is not None and not 500 <= weight <= 5000:
        raise ValueError("Anslått egenvekt må være mellom 500 og 5000 kg")
    return search, options, eur, sek, weight


def perform(path, job_id, parsed):
    try:
        search, options, eur, sek, weight = parsed
        fx = fx_rates() if eur is None else {"eur_nok": eur, "sek_nok": sek,
                                          "date": date.today().isoformat(), "kind": "Egne kurser"}
        settings = Settings(eur_nok=fx["eur_nok"], sek_nok=fx["sek_nok"], **options)
        report = run_search(search, settings, fx_info=fx, assumed_weight_kg=weight)
        payload = json.dumps(report, ensure_ascii=False, allow_nan=False)
        with connect(path) as db:
            db.execute("UPDATE jobs SET status='done', payload=? WHERE id=? AND status='running'", (payload, job_id))
    except Exception as exc:
        message = str(exc) if isinstance(exc, (SourceError, ValueError)) else "Søket kunne ikke fullføres. Prøv igjen senere."
        with connect(path) as db:
            db.execute("UPDATE jobs SET status='error', error=? WHERE id=? AND status='running'", (message, job_id))


@import_radar_bp.get("/")
def index():
    if "import_radar_owner" not in session:
        session["import_radar_owner"] = secrets.token_urlsafe(24)
    if "import_radar_csrf" not in session:
        session["import_radar_csrf"] = secrets.token_urlsafe(24)
    return render_template("import_radar.html", csrf=session["import_radar_csrf"],
                           today=date.today().isoformat(), year=date.today().year)


@import_radar_bp.post("/api/search")
def start_search():
    expected = session.get("import_radar_csrf", "")
    if not expected or not secrets.compare_digest(request.headers.get("X-CSRF-Token", ""), expected):
        return jsonify(error="Last søkesiden på nytt før du søker"), 403
    try:
        parsed = parse_request(request.get_json(silent=True))
    except (ValueError, TypeError, KeyError) as exc:
        return jsonify(error=str(exc)), 400
    path, owner, now = db_path(), session["import_radar_owner"], time.time()
    job_id = secrets.token_urlsafe(24)
    with connect(path) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM jobs WHERE created < ?", (now - 86400,))
        db.execute("UPDATE jobs SET status='error', error='Søket ble avbrutt eller overskred tidsgrensen' WHERE status='running' AND created < ?", (now - 240,))
        active = db.execute("SELECT owner FROM jobs WHERE status='running'").fetchall()
        if len(active) >= 2 or any(r["owner"] == owner for r in active):
            return jsonify(error="Et søk kjører allerede. Vent til det er ferdig."), 429
        recent = db.execute("SELECT COUNT(*) FROM jobs WHERE owner=? AND created>?", (owner, now - 300)).fetchone()[0]
        if recent >= 5:
            return jsonify(error="Vent litt før du starter flere søk"), 429
        db.execute("INSERT INTO jobs (id, owner, created, status) VALUES (?, ?, ?, 'running')", (job_id, owner, now))
    try:
        _POOL.submit(perform, path, job_id, parsed)
    except RuntimeError:
        with connect(path) as db:
            db.execute("UPDATE jobs SET status='error', error='Tjenesten starter på nytt' WHERE id=?", (job_id,))
        return jsonify(error="Tjenesten starter på nytt. Prøv igjen."), 503
    return jsonify(id=job_id, status="running"), 202


def owned_job(job_id):
    with connect(db_path()) as db:
        row = db.execute("SELECT * FROM jobs WHERE id=? AND owner=? AND created>?",
                         (job_id, session.get("import_radar_owner", ""), time.time() - 86400)).fetchone()
        if row and row["status"] == "running" and row["created"] < time.time() - 240:
            db.execute("UPDATE jobs SET status='error', error='Søket ble avbrutt eller overskred tidsgrensen' WHERE id=?", (job_id,))
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return row


@import_radar_bp.get("/api/search/<job_id>")
def search_status(job_id):
    row = owned_job(job_id)
    if not row:
        return jsonify(error="Søket finnes ikke i denne økten eller har utløpt"), 404
    response = jsonify(id=job_id, status=row["status"], error=row["error"],
                       report=json.loads(row["payload"]) if row["payload"] else None)
    response.headers["Cache-Control"] = "no-store"
    return response


@import_radar_bp.get("/api/search/<job_id>/download")
def download(job_id):
    row = owned_job(job_id)
    if not row or row["status"] != "done":
        return jsonify(error="Rapporten er ikke tilgjengelig"), 404
    return Response(row["payload"], mimetype="application/json", headers={
        "Content-Disposition": 'attachment; filename="import-radar.json"', "Cache-Control": "no-store"})
