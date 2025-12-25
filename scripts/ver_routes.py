from datetime import date as _date

from flask import Blueprint, request, render_template
from snow_map import build_snow_map_html

ver_bp = Blueprint("ver", __name__)


@ver_bp.route("/")
def ver_index():
    today_str = _date.today().isoformat()
    return render_template("ver/snow_index.html", default_date=today_str)


@ver_bp.route("/snomengde-kart")
def snomengde_kart():
    date_str = request.args.get("date")  # kan være None
    html_map = build_snow_map_html(date_str=date_str, show_heatmap=True)
    # Vi returnerer ren HTML som iFrame viser
    return html_map
