# -*- coding: utf-8 -*-
"""
kart_routes.py
Geografisk selskapsutforsker for prisanalyse.no
Bruker proxy_analysis_api fra regnskap_routes for DB-tilgang.
"""

import re
import uuid

from flask import Blueprint, current_app, make_response, render_template, request, jsonify

kart_bp = Blueprint("kart", __name__, url_prefix="/kart")


@kart_bp.route("/")
def kart_index():
    return render_template("kart.html")


@kart_bp.route("/api/selskaper")
def api_selskaper():
    from regnskap_routes import proxy_analysis_api
    params = {k: v for k, v in request.args.items()}
    try:
        response = make_response(proxy_analysis_api("/analysis-api/kart", params))
    except Exception as exc:
        reference = uuid.uuid4().hex[:12]
        current_app.logger.exception("Kartoppslag feilet, referanse %s", reference)
        # Never send database messages, credentials or SQL to the browser.
        # Keep only the exception class for diagnosing otherwise opaque 500s.
        error_code = type(exc).__name__
        detail = getattr(exc, "detail", "")
        match = re.match(r"DB error: ([A-Za-z][A-Za-z0-9_]*)\(", detail if isinstance(detail, str) else "")
        if match:
            error_code = match.group(1)
        response = make_response(jsonify(
            detail="Serveren klarte ikke å hente kartdata.",
            error_code=error_code,
            reference=reference,
        ), 500)
    response.headers["Cache-Control"] = "no-store"
    return response
