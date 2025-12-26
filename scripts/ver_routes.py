from datetime import date as _date

from flask import Blueprint, request, render_template

from snow_map import build_snow_map_html
from precip_map import build_precip_map_html

ver_bp = Blueprint("ver", __name__)


# ------------------
# HUB / MENY
# ------------------
@ver_bp.route("/")
def ver_hub():
    # Hub-side der vi klikker oss videre til snø/nedbør
    return render_template("ver/ver_hub.html")


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
    date_str = request.args.get("date")  # kan være None (ignoreres for last24h)
    mode = request.args.get("mode", "last24h")  # last24h|day|mtd|ytd
    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"

    html_map = build_precip_map_html(date_str=date_str, mode=mode, show_heatmap=True)
    return html_map
