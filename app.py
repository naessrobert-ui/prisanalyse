# app.py
# -*- coding: utf-8 -*-
"""Main Flask entrypoint for prisanalyse."""

import os
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, render_template
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
