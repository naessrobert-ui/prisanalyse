# -*- coding: utf-8 -*-
"""
kart_routes.py
Geografisk selskapsutforsker for prisanalyse.no
Bruker proxy_analysis_api fra regnskap_routes for DB-tilgang.
"""

from flask import Blueprint, render_template, request, jsonify

kart_bp = Blueprint("kart", __name__, url_prefix="/kart")


@kart_bp.route("/")
def kart_index():
    return render_template("kart.html")


@kart_bp.route("/api/selskaper")
def api_selskaper():
    from regnskap_routes import proxy_analysis_api
    params = {k: v for k, v in request.args.items()}
    return proxy_analysis_api("/analysis-api/kart", params)
