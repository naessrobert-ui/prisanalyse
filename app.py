# app.py (din justerte fil)
from flask import Flask, render_template

from flask_session import Session
from dotenv import load_dotenv
import os

load_dotenv()
from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp
from bil_import import bil_import_bp
from gemini_routes import gemini_bp  # <-- 1. LEGG TIL DENNE LINJEN

app = Flask(__name__)

# 🔐 Secret key for sessions
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"

Session(app)

# Registrer «seksjonene»
app.register_blueprint(bolig_bp)
app.register_blueprint(fritids_bp)
app.register_blueprint(bil_bp)
app.register_blueprint(bil_import_bp, url_prefix="/bil/import")
app.register_blueprint(gemini_bp)  # <-- 2. LEGG TIL DENNE LINJEN


@app.route("/")
def forside():
    # Forvent at templates/landing_page.html inneholder den nye, lyse forsiden
    return render_template("landing_page.html")


@app.route("/ver/")
def ver_side():
    # Enkel placeholder – kan senere byttes ut med ekte væranalyse
    return render_template("ver_analyse.html")


@app.route("/jobb/")
def jobb_side():
    # Enkel placeholder – kan senere byttes ut med ekte jobbanalyse
    return render_template("jobb_analyse.html")


if __name__ == "__main__":
    app.run(debug=True)