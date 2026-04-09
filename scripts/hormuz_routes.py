# -*- coding: utf-8 -*-
"""Routes for visualisering av skipstrafikk gjennom Hormuz-stredet."""

from datetime import datetime
import sqlite3
from pathlib import Path
from flask import Blueprint, jsonify, render_template, request

hormuz_bp = Blueprint("hormuz", __name__, url_prefix="/hormuz")
DB_PATH = Path("data/hormuz_ais.sqlite")
MAP_PATH = Path("data/hormuz_map.html")


@hormuz_bp.route("/")
def hormuz_dashboard():
    return render_template("hormuz_traffic.html")


@hormuz_bp.route("/map")
def hormuz_map_page():
    if not MAP_PATH.exists():
        return (
            "Kartfil finnes ikke enda. Kjør: python scripts/hormuz/build_map.py --hours 12 --latest-only --trails",
            404,
        )
    return render_template("hormuz_map_embed.html", map_path=f"/{MAP_PATH.as_posix()}")


@hormuz_bp.route("/api/traffic")
def hormuz_traffic_data():
    hours = request.args.get("hours", default=24, type=int)
    hours = max(1, min(hours, 24 * 30))

    if not DB_PATH.exists():
        return jsonify(
            {
                "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "source": "sqlite",
                "rows": [],
                "message": f"Fant ikke DB: {DB_PATH}",
            }
        )

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

    rows = []
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(sql, (f"-{hours} hours",))
        for date, northbound, southbound, tankers, lng in cur.fetchall():
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

    return jsonify(
        {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source": "sqlite",
            "hours": hours,
            "rows": rows,
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

    return jsonify(
        {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source": "demo",
            "rows": demo_rows,
        }
    )
