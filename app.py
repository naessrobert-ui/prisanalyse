# app.py
# -*- coding: utf-8 -*-
"""Main Flask entrypoint for prisanalyse."""

import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, render_template, send_file
from flask_session import Session

# Blueprints
from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp
from bil_import import bil_import_bp
from gemini_routes import gemini_bp

load_dotenv()


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

    # Registrer seksjonene (blueprints)
    app.register_blueprint(bolig_bp)
    app.register_blueprint(fritids_bp)
    app.register_blueprint(bil_bp)
    app.register_blueprint(bil_import_bp, url_prefix="/bil/import")
    app.register_blueprint(gemini_bp)

    # -----------------------
    # Forside / generelle sider
    # -----------------------
    @app.route("/")
    def forside():
        # Landing page med kortene (Bolig, Fritidsbolig, Bil, …)
        return render_template("landing_page.html")

    @app.route("/ver/")
    def ver_side():
        return render_template("ver_analyse.html")

    def _auto_generate_snow_map(map_path: str) -> None:
        enabled = os.environ.get("SNOW_MAP_AUTO_GENERATE", "true").lower() in {"1", "true", "yes"}
        if not enabled:
            return
        if not os.environ.get("FROST_CLIENT_ID"):
            return

        script_path = os.path.join(app.root_path, "scripts", "snow_map.py")
        if not os.path.exists(script_path):
            return

        max_age_hours = int(os.environ.get("SNOW_MAP_MAX_AGE_HOURS", "12"))
        if os.path.exists(map_path):
            age_seconds = datetime.now(tz=timezone.utc).timestamp() - os.path.getmtime(map_path)
            if age_seconds < max_age_hours * 3600:
                return

        os.makedirs(os.path.dirname(map_path), exist_ok=True)
        today = datetime.now(tz=timezone.utc).date().isoformat()
        subprocess.run(
            [sys.executable, script_path, "--date", today, "--out-html", map_path],
            check=True,
        )

    @app.route("/ver/sno/")
    def ver_sno_side():
        return render_template("ver_sno.html")

    @app.route("/ver/sno/kart")
    def ver_sno_kart():
        map_path = os.path.join(app.root_path, "static", "snow_map.html")
        try:
            _auto_generate_snow_map(map_path)
        except Exception:
            pass
        if os.path.exists(map_path):
            return send_file(map_path)
        return render_template("ver_sno_kart_missing.html")

    @app.route("/jobb/")
    def jobb_side():
        return render_template("jobb_analyse.html")

    return app


# Slik at Flask CLI også kan finne appen
app: Optional[Flask] = create_app()

if __name__ == "__main__":
    # Kjør kun Flask – ingen Streamlit
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=True,
    )
