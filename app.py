# -*- coding: utf-8 -*-
"""Main Flask entrypoint for prisanalyse."""

import os
import subprocess
import time
from typing import Optional
from urllib.error import URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import urlopen

from dotenv import load_dotenv
from flask import Flask, render_template, redirect, request, abort, Response
from flask_session import Session

# Blueprints
from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp
from bil_import import bil_import_bp
from gemini_routes import gemini_bp
from scripts.ver_routes import ver
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

    def _get_handler_base_url() -> tuple[str, bool, str]:
        app_url = request.args.get("app_url", "").strip()
        if app_url:
            return app_url, True, "query:app_url"

        env_candidates = [
            "HANDLER_OSLO_BORS_URL",
            "HANDLER_OSLO_BORS_APP_URL",
            "HANDLER_URL",
            "STREAMLIT_HANDLER_URL",
        ]
        for name in env_candidates:
            value = os.environ.get(name, "").strip()
            if value:
                return value, False, f"env:{name}"

        fallback_url = f"{request.host_url.rstrip('/')}/handler"
        return fallback_url, False, "fallback:same-origin-/handler"

    def _handler_is_reachable(app_url: str) -> bool:
        try:
            with urlopen(app_url, timeout=1.5):
                return True
        except (URLError, ValueError):
            return False

    def _autostart_handler_if_configured(app_url: str, from_query: bool) -> str:
        """Forsøk å starte handler-prosess automatisk hvis konfigurert via env."""
        if from_query:
            return ""

        autostart_cmd = os.environ.get("HANDLER_OSLO_BORS_AUTOSTART_CMD", "").strip()
        if not autostart_cmd:
            return ""

        if _handler_is_reachable(app_url):
            return ""

        try:
            subprocess.Popen(
                autostart_cmd,
                shell=True,
                cwd=os.environ.get("HANDLER_OSLO_BORS_AUTOSTART_CWD") or None,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(float(os.environ.get("HANDLER_OSLO_BORS_AUTOSTART_WAIT", "2.5")))
            if _handler_is_reachable(app_url):
                return "Handler ble startet automatisk."
            return "Forsøkte å starte handler automatisk, men appen svarte ikke ennå."
        except Exception:
            return "Forsøket på automatisk oppstart feilet. Sjekk HANDLER_OSLO_BORS_AUTOSTART_CMD."

    def _build_handler_target(app_url: str, subpath: str = "") -> str:
        """Bygg endelig URL mot handler-appen og bevar øvrige query params."""
        parsed = urlsplit(app_url)
        if parsed.scheme not in {"http", "https"}:
            return ""

        cleaned_base = parsed._replace(path=parsed.path.rstrip("/"))
        combined_path = cleaned_base.path
        if subpath:
            combined_path = f"{combined_path}/{subpath.lstrip('/')}"

        passthrough = [
            (key, value)
            for key, value in request.args.items()
            if key not in {"app_url", "mode"}
        ]
        existing = parse_qsl(parsed.query, keep_blank_values=True)
        merged_query = urlencode(existing + passthrough)

        return urlunsplit(
            (
                cleaned_base.scheme,
                cleaned_base.netloc,
                combined_path,
                merged_query,
                cleaned_base.fragment,
            )
        )

    @app.route("/handler-oslo-bors/")
    @app.route("/handler-oslo-bors/<path:subpath>")
    def handler_oslo_bors(subpath: str = ""):
        """Åpne Handler Oslo Børs (Streamlit) via redirect eller embed.

        Streamlit-apper blokkerer ofte cross-origin iframes i standardoppsett.
        Derfor bruker vi redirect som standard, og iframe bare når mode=embed.
        """
        app_url, from_query, config_source = _get_handler_base_url()
        startup_hint = _autostart_handler_if_configured(app_url, from_query)
        target_url = _build_handler_target(app_url, subpath=subpath)

        if target_url:
            mode = request.args.get("mode", "redirect").strip().lower()
            if mode == "embed":
                return render_template(
                    "embed_streamlit.html",
                    app_url=target_url,
                )
            return redirect(target_url)

        repo_url = os.environ.get(
            "HANDLER_OSLO_BORS_REPO_URL",
            "https://github.com/naessrobert-ui/handler",
        ).strip()
        return render_template(
            "handler_oslo_bors_setup.html",
            repo_url=repo_url,
            startup_hint=startup_hint,
            config_source=config_source,
        )

    create_dash_app(app)

    return app


app: Optional[Flask] = create_app()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False,
    )
