from __future__ import annotations

from datetime import date as _date
from flask import Blueprint, request, render_template, Response

from snow_map import build_snow_map_html


ver = Blueprint("ver", __name__, url_prefix="/ver")

# først etter dette er det lov å bruke @ver.route(...)


import traceback
from flask import Response, request

@ver.route("/snomengde-kart")
def snomengde_kart():
    try:
        date_str = request.args.get("date")
        mode = request.args.get("mode", "latest")
        region = request.args.get("region")
        bbox = request.args.get("bbox")
        z = request.args.get("z")
        clat = request.args.get("clat")
        clon = request.args.get("clon")

        html = build_snow_map_html(
            date_str=date_str,
            mode=mode,
            bbox=bbox,
            region=region,
            z=z,
            clat=clat,
            clon=clon,
            show_heatmap=True,
            debug_timing=True,   # <- slå på midlertidig
        )
        return Response(html, mimetype="text/html; charset=utf-8")

    except Exception as e:
        traceback.print_exc()
        # VIKTIG: returner feilen som tekst så du kan se den i Network->Response
        return Response(
            "SNØ-KART FEIL:\n"
            + repr(e)
            + "\n\nTRACEBACK:\n"
            + traceback.format_exc(),
            status=500,
            mimetype="text/plain; charset=utf-8",
        )
