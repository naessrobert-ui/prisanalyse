"""Visning for NewsWeb-prototypen. Ingen NewsWeb-scraping fra nettsiden."""
from __future__ import annotations
import json
import os
from pathlib import Path
from flask import Blueprint, render_template

newsweb_bp = Blueprint("newsweb", __name__, url_prefix="/newsweb")
BASE = Path(__file__).resolve().parent
SNAPSHOT = BASE / "newsweb_monitor" / "sample" / "reviewed_2026_10_21.json"
DEMO = BASE / "newsweb_monitor" / "sample" / "demo_report.json"

def read_report() -> tuple[dict, bool]:
    """Live-fil er valgfri og må tilføres av en GODKJENT kilde/lagringsmekanisme."""
    setting = os.environ.get("NEWSWEB_DASHBOARD_REPORT_PATH", "").strip()
    path = Path(setting) if setting else (SNAPSHOT if SNAPSHOT.exists() else DEMO)
    try:
        # Les bare en eksplisitt konfigurert lokal fil, aldri brukerlevert filbane.
        if path.stat().st_size > 2_000_000:
            raise ValueError("Report exceeds 2 MB")
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("items", []), list):
            raise ValueError("Invalid report format")
        return data, data.get("mode") == "demo"
    except (OSError, ValueError, json.JSONDecodeError):
        return {
            "source": "unavailable",
            "generated_at": "",
            "items": [],
            "error": "Ingen rapport tilgjengelig. Sjekk konfigurasjon eller siste GitHub-kjøring.",
        }, False

@newsweb_bp.get("/")
def dashboard():
    report, demo = read_report()
    items = report.get("items", [])[:200]
    # Resultatet sorteres etter alvorlighetsgrad, deretter tidspunkt.
    items = sorted(items, key=lambda item: (int(item.get("score") or 0), str(item.get("published_at") or "")), reverse=True)
    focus = sum(1 for x in items if int(x.get("score") or 0) >= 75 and not x.get("skipped"))
    interesting = sum(1 for x in items if int(x.get("score") or 0) >= 45 and not x.get("skipped"))
    ignored = sum(1 for x in items if x.get("skipped") or int(x.get("score") or 0) < 45)
    return render_template("newsweb_dashboard.html", report=report, items=items,
                           demo=demo, focus=focus, interesting=interesting, ignored=ignored)
