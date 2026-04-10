# -*- coding: utf-8 -*-
"""Routes for visualisering av skipstrafikk gjennom Hormuz-stredet."""

from datetime import datetime
import os
from pathlib import Path
import re
import sqlite3
import subprocess

from flask import Blueprint, jsonify, render_template, request

hormuz_bp = Blueprint("hormuz", __name__, url_prefix="/hormuz")
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("HORMUZ_DB_PATH", str(BASE_DIR / "data" / "hormuz_ais.sqlite")))
MAP_PATH = Path(os.getenv("HORMUZ_MAP_PATH", str(BASE_DIR / "data" / "hormuz_map.html")))
COLLECT_SCRIPT = BASE_DIR / "scripts" / "hormuz" / "collect_ais.py"
BUILD_MAP_SCRIPT = BASE_DIR / "scripts" / "hormuz" / "build_map.py"


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _run_bootstrap(minutes: float = 0.5, max_messages: int = 120):
    api_key = os.getenv("AISTREAM_API_KEY", "").strip()
    if not api_key:
        return False, "Mangler AISTREAM_API_KEY i miljøvariabler", None
    if not COLLECT_SCRIPT.exists():
        return False, f"Manglende script: {COLLECT_SCRIPT}", None

    minutes = max(0.1, min(float(minutes), 5.0))
    max_messages = max(10, min(int(max_messages), 2000))
    cmd = [
        "python", str(COLLECT_SCRIPT), "--db", str(DB_PATH), "--minutes", str(minutes),
        "--max-messages", str(max_messages), "--ws-timeout", "10", "--include-static",
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        return False, "Timeout ved innhenting av AIS-data", None

    if proc.returncode != 0:
        err = "AIS-innhenting feilet"
        out = {"stderr": proc.stderr[-1200:], "stdout": proc.stdout[-1200:]}
        return False, err, out

    saved_count = None
    match = re.search(r"Lagret\s+(\d+)\s+meldinger", proc.stdout)
    if match:
        saved_count = int(match.group(1))

    if saved_count == 0:
        return (
            False,
            "Ingen AIS-meldinger mottatt i tidsvinduet. Prøv /hormuz/api/bootstrap?minutes=3 eller vent litt og prøv igjen.",
            {"stdout": proc.stdout[-1200:]},
        )

    return True, "AIS-data hentet inn", {"stdout": proc.stdout[-1200:], "saved_messages": saved_count}


def _run_build_map(hours: int = 24, latest_only: bool = True, trails: bool = True):
    if not BUILD_MAP_SCRIPT.exists():
        return False, f"Manglende script: {BUILD_MAP_SCRIPT}", None

    hours = max(1, min(int(hours), 24 * 30))
    cmd = [
        "python", str(BUILD_MAP_SCRIPT), "--db", str(DB_PATH), "--output", str(MAP_PATH), "--hours", str(hours),
    ]
    if latest_only:
        cmd.append("--latest-only")
    if trails:
        cmd.append("--trails")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        return False, "Timeout ved bygging av kart", None

    if proc.returncode != 0:
        return False, "Kartbygging feilet", {"stderr": proc.stderr[-1200:], "stdout": proc.stdout[-1200:]}

    return True, "Kart bygget", {"stdout": proc.stdout[-1200:], "map_path": str(MAP_PATH)}


@hormuz_bp.route("/")
def hormuz_dashboard():
    return render_template("hormuz_traffic.html")


@hormuz_bp.route("/map")
def hormuz_map_page():
    rebuild = request.args.get("rebuild", default="0") == "1"
    if rebuild or not MAP_PATH.exists():
        ok, message, extra = _run_build_map(hours=24, latest_only=True, trails=True)
        if not ok:
            payload = {
                "ok": False,
                "generated_at": _utc_now_iso(),
                "error": message,
                "db_path": str(DB_PATH),
                "map_path": str(MAP_PATH),
                "hint": "Sjekk at DB har data via /hormuz/api/status og prøv /hormuz/map?rebuild=1",
            }
            if extra:
                payload.update(extra)
            return jsonify(payload), 503

    return render_template("hormuz_map_embed.html", map_path=f"/{MAP_PATH.as_posix()}")


@hormuz_bp.route("/api/bootstrap", methods=["POST"])
def hormuz_bootstrap():
    """Forsøk å hente inn ferske AIS-meldinger hvis DB mangler/tom."""
    minutes = request.args.get("minutes", default=0.5, type=float)
    max_messages = request.args.get("max_messages", default=120, type=int)
    ok, message, extra = _run_bootstrap(minutes=minutes, max_messages=max_messages)
    if not ok:
        payload = {"ok": False, "generated_at": _utc_now_iso(), "error": message, "db_path": str(DB_PATH)}
        if extra:
            payload.update(extra)
        return jsonify(payload), 500

    payload = {
        "ok": True,
        "generated_at": _utc_now_iso(),
        "message": message,
        "db_path": str(DB_PATH),
    }
    if extra:
        payload.update(extra)
    return jsonify(payload)


@hormuz_bp.route("/api/status")
def hormuz_status():
    if not DB_PATH.exists():
        return jsonify(
            {
                "ok": False,
                "generated_at": _utc_now_iso(),
                "error": f"DB mangler: {DB_PATH}",
                "db_path": str(DB_PATH),
            }
        ), 503

    with sqlite3.connect(DB_PATH) as conn:
        if not _table_exists(conn, "ais_messages"):
            return jsonify({"ok": False, "generated_at": _utc_now_iso(), "error": "Tabell 'ais_messages' mangler", "db_path": str(DB_PATH)}), 503

        total_rows = conn.execute("SELECT COUNT(*) FROM ais_messages").fetchone()[0]
        last_received = conn.execute("SELECT MAX(received_at_utc) FROM ais_messages").fetchone()[0]

    return jsonify(
        {
            "ok": True,
            "generated_at": _utc_now_iso(),
            "db_path": str(DB_PATH),
            "total_rows": int(total_rows or 0),
            "last_received_at_utc": last_received,
        }
    )


@hormuz_bp.route("/api/db-path")
def hormuz_db_path():
    return jsonify({
        "generated_at": _utc_now_iso(),
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "base_dir": str(BASE_DIR),
    })


@hormuz_bp.route("/api/traffic")
def hormuz_traffic_data():
    hours = request.args.get("hours", default=24, type=int)
    hours = max(1, min(hours, 24 * 30))

    if not DB_PATH.exists():
        auto_bootstrap = request.args.get("auto_bootstrap", default="1") == "1"
        if auto_bootstrap:
            ok, message, _ = _run_bootstrap(minutes=0.5, max_messages=120)
            if ok and DB_PATH.exists():
                pass
            else:
                return jsonify({"generated_at": _utc_now_iso(), "source": "sqlite", "rows": [], "error": f"DB mangler: {DB_PATH}. Bootstrap: {message}", "db_path": str(DB_PATH)}), 503
        else:
            return jsonify({"generated_at": _utc_now_iso(), "source": "sqlite", "rows": [], "error": f"DB mangler: {DB_PATH}", "db_path": str(DB_PATH)}), 503

    sql = """
        SELECT
            date(received_at_utc) AS date,
            SUM(CASE WHEN COALESCE(cog, true_heading, 0) < 180 THEN 1 ELSE 0 END) AS northbound,
            SUM(CASE WHEN COALESCE(cog, true_heading, 0) >= 180 THEN 1 ELSE 0 END) AS southbound,
            SUM(CASE WHEN lower(COALESCE(ship_name, '')) LIKE '%tanker%' THEN 1 ELSE 0 END) AS tankers,
            SUM(CASE
                WHEN lower(COALESCE(ship_name, '')) LIKE '%lng%'
                  OR lower(COALESCE(ship_name, '')) LIKE '%gas%'
                THEN 1 ELSE 0 END) AS lng
        FROM ais_messages
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND received_at_utc >= datetime('now', ?)
        GROUP BY date(received_at_utc)
        ORDER BY date(received_at_utc)
    """

    try:
        with sqlite3.connect(DB_PATH) as conn:
            if not _table_exists(conn, "ais_messages"):
                return jsonify({"generated_at": _utc_now_iso(), "source": "sqlite", "rows": [], "error": "Tabell 'ais_messages' mangler", "db_path": str(DB_PATH)}), 503

            total_rows = conn.execute(
                "SELECT COUNT(*) FROM ais_messages WHERE received_at_utc >= datetime('now', ?)",
                (f"-{hours} hours",),
            ).fetchone()[0]
            last_received = conn.execute("SELECT MAX(received_at_utc) FROM ais_messages").fetchone()[0]

            rows = []
            for date, northbound, southbound, tankers, lng in conn.execute(sql, (f"-{hours} hours",)).fetchall():
                nb = int(northbound or 0)
                sb = int(southbound or 0)
                rows.append(
                    {
                        "date": date,
                        "northbound": nb,
                        "southbound": sb,
                        "total": nb + sb,
                        "tankers": int(tankers or 0),
                        "lng": int(lng or 0),
                    }
                )
    except sqlite3.Error as exc:
        return jsonify({"generated_at": _utc_now_iso(), "source": "sqlite", "rows": [], "error": f"SQLite-feil: {exc}", "db_path": str(DB_PATH)}), 500

    return jsonify(
        {
            "generated_at": _utc_now_iso(),
            "source": "sqlite",
            "hours": hours,
            "rows": rows,
            "db_path": str(DB_PATH),
            "meta": {
                "raw_messages_in_window": int(total_rows or 0),
                "last_received_at_utc": last_received,
            },
        }
    )


@hormuz_bp.route("/api/demo")
def hormuz_demo_data():
    demo_rows = [
        {"date": "2026-04-01", "northbound": 53, "southbound": 49, "tankers": 37, "lng": 11},
        {"date": "2026-04-02", "northbound": 56, "southbound": 52, "tankers": 39, "lng": 13},
        {"date": "2026-04-03", "northbound": 58, "southbound": 54, "tankers": 41, "lng": 12},
        {"date": "2026-04-04", "northbound": 61, "southbound": 57, "tankers": 43, "lng": 14},
        {"date": "2026-04-05", "northbound": 64, "southbound": 60, "tankers": 44, "lng": 15},
        {"date": "2026-04-06", "northbound": 62, "southbound": 58, "tankers": 42, "lng": 13},
        {"date": "2026-04-07", "northbound": 65, "southbound": 61, "tankers": 46, "lng": 16},
    ]
    for row in demo_rows:
        row["total"] = row["northbound"] + row["southbound"]

    return jsonify({"generated_at": _utc_now_iso(), "source": "demo", "rows": demo_rows})
