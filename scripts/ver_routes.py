from datetime import date as _date

from flask import Blueprint, request, render_template

from snow_map import build_snow_map_html
from precip_map import build_precip_map_html

ver_bp = Blueprint("ver", __name__)


# ------------------
# HUB / MENY (INLINE HTML – ingen template)
# ------------------
@ver_bp.route("/")
def ver_hub():
    return """
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Vær – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background:#f5f7fb; }
      .page { max-width: 1000px; margin: 32px auto; padding: 0 16px; }
      .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
      .card { background:white; border-radius:16px; padding:18px 22px; box-shadow:0 18px 45px rgba(15,23,42,.08); }
      a.btn { display:inline-block; margin-top:10px; padding:8px 14px; border-radius:999px; background:#2563eb; color:white; text-decoration:none; }
      p { margin: 8px 0 0; color:#334155; }
      h1 { margin: 0 0 12px; }
      h2 { margin: 0 0 6px; }
    </style>
  </head>
  <body>
    <div class="page">
      <h1>Vær</h1>
      <div class="grid">
        <div class="card">
          <h2>Snømengde</h2>
          <p>Snødybde fra Frost. Velg dato.</p>
          <a class="btn" href="/ver/sno">Åpne</a>
        </div>
        <div class="card">
          <h2>Nedbør</h2>
          <p>Siste 24 timer (rullerende) + MTD/YTD.</p>
          <a class="btn" href="/ver/nedbor">Åpne</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""


# ------------------
# SNØ
# ------------------
@ver_bp.route("/sno")
def sno_index():
    today_str = _date.today().isoformat()
    return render_template("ver/snow_index.html", default_date=today_str)


@ver_bp.route("/snomengde-kart")
def snomengde_kart():
    date_str = request.args.get("date")  # kan være None
    html_map = build_snow_map_html(date_str=date_str, show_heatmap=True)
    return html_map


# ------------------
# NEDBØR
# ------------------
@ver_bp.route("/nedbor")
def nedbor_index():
    today_str = _date.today().isoformat()
    return render_template("ver/precip_index.html", default_date=today_str)


@ver_bp.route("/nedbor-kart")
def nedbor_kart():
    date_str = request.args.get("date")
    mode = request.args.get("mode", "last24h")  # last24h|day|mtd|ytd
    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"
    return build_precip_map_html(date_str=date_str, mode=mode, show_heatmap=True)
