# -*- coding: utf-8 -*-
"""Main Flask entrypoint for prisanalyse."""

import os
import threading
import time
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, render_template, redirect, request, abort, Response
from flask_session import Session

# Blueprints
from handler_routes import handler_bp
from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp, start_bilradar_warmup
from bil_import import bil_import_bp
from gemini_routes import gemini_bp
from scripts.ver_routes import ver
from scripts.station_metrics_cache import start_warmup as start_station_metrics_warmup
from regnskap_routes import regnskap_bp
from portfolio_rebalancer_routes import portfolio_rebalancer_bp
from dash_apps.strom import create_dash_app
from scripts.kvamskogen_routes import kvamskogen_bp
from scripts.kvamskogen_sommer_routes import kvamskogen_sommer_bp
from scripts.sno_routes import sno_bp, start_warmup
from scripts.kart_routes import kart_bp
from scripts.hormuz_routes import hormuz_bp
from aksjonaer_routes import aksjonaer_bp
from scripts.kvam_routes import kvam_bp





load_dotenv()

BLOCKED_BOTS = [
    'SemrushBot', 'AhrefsBot', 'MJ12bot', 'DotBot',
    'BLEXBot', 'PetalBot', 'Bytespider',
]

# Enkel in-memory rate-limit for tunge endepunkt.
# Nøkkel: "<ip>|<gruppe>" -> liste med request-tidspunkter (sekunder).
_RATE_LIMIT_BUCKETS: dict[str, list[float]] = {}
_RATE_LIMIT_LOCK = threading.Lock()
_HEAVY_RATE_LIMIT_RULES = [
    # Mange bots prøver /bolig/priser-sted i loop med ulike query-parametere.
    {"prefix": "/bolig/priser-sted/", "window_sec": 60, "max_requests": 35},
    # Datatung beregning som kan trigges gjentatte ganger av samme klient.
    {"prefix": "/handler-oslo-bors/api/beste-investorer/run", "window_sec": 60, "max_requests": 8},
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
    upload_limit_raw = (os.environ.get("HANDLER_DB_UPLOAD_MAX_MB", "300") or "300").strip()
    try:
        upload_limit_mb = int(upload_limit_raw)
    except ValueError:
        upload_limit_mb = 300
    app.config["MAX_CONTENT_LENGTH"] = max(1, upload_limit_mb) * 1024 * 1024

    Session(app)

    # --- Bot-blokkering (kjører før ALLE requests) ---
    @app.before_request
    def block_bots():
        ua = request.headers.get('User-Agent', '')
        if any(bot in ua for bot in BLOCKED_BOTS):
            abort(403)

        # Begrens spikete trafikk mot tunge ruter for å unngå CPU/RAM-spikes.
        req_path = request.path or ""
        for rule in _HEAVY_RATE_LIMIT_RULES:
            if not req_path.startswith(rule["prefix"]):
                continue
            ip = (request.headers.get("X-Forwarded-For", "") or request.remote_addr or "unknown").split(",")[0].strip()
            bucket_key = f"{ip}|{rule['prefix']}"
            now = time.time()
            cutoff = now - float(rule["window_sec"])
            with _RATE_LIMIT_LOCK:
                hits = _RATE_LIMIT_BUCKETS.get(bucket_key, [])
                hits = [t for t in hits if t >= cutoff]
                if len(hits) >= int(rule["max_requests"]):
                    abort(429)
                hits.append(now)
                _RATE_LIMIT_BUCKETS[bucket_key] = hits

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
    app.register_blueprint(portfolio_rebalancer_bp)
    app.register_blueprint(kvamskogen_bp)
    app.register_blueprint(kvamskogen_sommer_bp)
    app.register_blueprint(sno_bp)
    app.register_blueprint(kart_bp)
    app.register_blueprint(hormuz_bp)
    app.register_blueprint(aksjonaer_bp)
    app.register_blueprint(kvam_bp)

    # Start bakgrunnsjobb som prefetcher snødybde for hele Norge
    start_warmup()

    # Forhåndsberegn vær-stasjonsmetrikker i bakgrunnen for å gjøre
    # /ver/min-temp (og senere /ver/vind, /ver/nedbor) raskt.
    start_station_metrics_warmup()

    # Bilradar-warmup deaktivert midlertidig: 150 MB-modellen forårsaket
    # OOM-loop på 2 GB-instansen. Aktiveres igjen når modellen er retrent
    # med høyere MIN_OBS_L1 og lavere max_iter (~30-40 MB komprimert).
    # start_bilradar_warmup()



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


    # Dash-apper: init i bakgrunnen for å unngå treg oppstart/forside-timeout
    def _init_dash_async() -> None:
        try:
            create_dash_app(app)
        except Exception as exc:
            app.logger.exception("Dash-init feilet: %s", exc)

    threading.Thread(target=_init_dash_async, daemon=True).start()

    return app


app: Optional[Flask] = create_app()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False,
        threaded=True,
    )
