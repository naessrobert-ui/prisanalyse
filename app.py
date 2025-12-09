# -*- coding: utf-8 -*-
"""Main Flask entrypoint for the prisanalyse project."""

import os
import subprocess
import sys
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, render_template
from flask_session import Session

load_dotenv()


def start_streamlit(script: str, port: int) -> None:
    """Start a Streamlit app if it is not already running on the given port."""

    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(("127.0.0.1", port))
    sock.close()

    if result == 0:
        print(f"Streamlit app on port {port} is already running.")
        return

    python_path = sys.executable
    subprocess.Popen(
        [
            python_path,
            "-m",
            "streamlit",
            "run",
            script,
            "--server.port",
            str(port),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )


def startup_apps() -> None:
    """Start all Streamlit helper apps once at startup."""

    start_streamlit("app_region.py", 8501)
    start_streamlit("appKupp.py", 8502)
    start_streamlit("app_buzz.py", 8503)
    start_streamlit("app_varme.py", 8504)
    print("Attempted to start all Streamlit apps.")


def create_app() -> Flask:
    """Create and configure the Flask app once."""

    app = Flask(__name__)

    # 🔐 Secret key for sessions
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
    app.config["SESSION_PERMANENT"] = False
    app.config["SESSION_TYPE"] = "filesystem"

    Session(app)

    # Register "sections"
    from bil_import import bil_import_bp
    from bil_routes import bil_bp
    from bolig_routes import bolig_bp
    from fritidsbolig_routes import fritids_bp
    from gemini_routes import gemini_bp

    app.register_blueprint(bolig_bp)
    app.register_blueprint(fritids_bp)
    app.register_blueprint(bil_bp)
    app.register_blueprint(bil_import_bp, url_prefix="/bil/import")
    app.register_blueprint(gemini_bp)

    return app


app: Optional[Flask] = create_app()


@app.route("/")
def forside():
    return render_template("landing_page.html")


@app.route("/ver/")
def ver_side():
    return render_template("ver_analyse.html")


@app.route("/jobb/")
def jobb_side():
    return render_template("jobb_analyse.html")


if __name__ == "__main__":
    # Start Streamlit helper apps when running locally. Deployment environments
    # (e.g., Render) can disable this by setting START_STREAMLIT_APPS=0.
    should_start_streamlit = os.environ.get("START_STREAMLIT_APPS", "1") != "0"

    if should_start_streamlit:
        startup_apps()
    else:
        print("Skipping Streamlit helper startup (controlled by START_STREAMLIT_APPS)")

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
