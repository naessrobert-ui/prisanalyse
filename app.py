# -*- coding: utf-8 -*-
"""Main Flask entrypoint for prisanalyse."""

import os
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, render_template, redirect, request, abort, Response
from flask_session import Session

# Blueprints
from handler_routes import handler_bp
from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp
from bil_import import bil_import_bp
from gemini_routes import gemini_bp
from scripts.ver_routes import ver
from regnskap_routes import regnskap_bp
from dash_apps.strom import create_dash_app



load_dotenv()

BLOCKED_BOTS = [
    'SemrushBot', 'AhrefsBot', 'MJ12bot', 'DotBot',
    'BLEXBot', 'PetalBot', 'Bytespider',
]


def create_app() -> Flask:
    """Opprett og konfigurer Flask-appen."""
    app = Flask(__name__)

    # Sessions / konfig
    app.config["SECRET_KEY"] = os.environ.get(
        "FLASK_SECRET_KEY",
        "dev-secret-change-me",
    )
    app.config["SESSION_PERMANENT"] = False
    app.config["SESSION_TYPE"] = "filesystem"

    Session(app)

    # --- Bot-blokkering (kjører før ALLE requests) ---
    @app.before_request
    def block_bots():
        ua = request.headers.get('User-Agent', '')
        if any(bot in ua for bot in BLOCKED_BOTS):
            abort(403)

    # --- robots.txt for å be botter holde seg unna ---
    @app.route("/robots.txt")
    def robots_txt():
        lines = [
            "User-agent: SemrushBot",
            "Disallow: /",
            "",
            "User-agent: AhrefsBot",
            "Disallow: /",
            "",
            "User-agent: MJ12bot",
            "Disallow: /",
            "",
            "User-agent: *",
            "Disallow: /bolig/priser-sted/",
            "Disallow: /bolig/priser-gate/",
            "",
            "User-agent: Googlebot",
            "Allow: /",
            "",
            "Sitemap: https://prisanalyse.no/sitemap.xml",
        ]
        return Response("\n".join(lines), mimetype="text/plain")

    # Registrer seksjonene (blueprints)
    app.register_blueprint(bolig_bp)
    app.register_blueprint(fritids_bp)
    app.register_blueprint(bil_bp)
    app.register_blueprint(bil_import_bp, url_prefix="/bil/import")
    app.register_blueprint(gemini_bp)
    app.register_blueprint(ver)
    app.register_blueprint(handler_bp)
    app.register_blueprint(regnskap_bp)

    # -----------------------
    # Forside / generelle sider
    # -----------------------
    @app.route("/")
    def forside():
        return render_template("landing_page.html")

    @app.route("/jobb/")
    def jobb_side():
        return render_template("jobb_analyse.html")

    @app.route("/strom")
    def strom():
        return redirect("/stromdash/")


    # Dash-apper
    create_dash_app(app)

    return app


app: Optional[Flask] = create_app()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False,
    )
