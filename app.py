# app.py (din justerte fil)
from flask import Flask, render_template

from bolig_routes import bolig_bp
from fritidsbolig_routes import fritids_bp
from bil_routes import bil_bp
from gemini_routes import gemini_bp  # <-- 1. LEGG TIL DENNE LINJEN

app = Flask(__name__)

# Registrer «seksjonene»
app.register_blueprint(bolig_bp)
app.register_blueprint(fritids_bp)
app.register_blueprint(bil_bp)
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