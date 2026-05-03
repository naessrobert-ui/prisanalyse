# -*- coding: utf-8 -*-
"""Blueprint that serves the static /kvam/ site (Kvamskogen-siden).

Plasser denne fila i `scripts/`-mappa di (samme sted som
`kvamskogen_routes.py`), og legg `kvam`-mappa med HTML/CSS/JSX/bilder
i prosjektroten (samme nivå som `app.py`).

Aktiver i `app.py` ved å legge til:

    from scripts.kvam_routes import kvam_bp
    ...
    app.register_blueprint(kvam_bp)
"""

import os
from flask import Blueprint, send_from_directory, abort

# Mappen som inneholder index.html og resten av filene.
# Endre `STATIC_DIR` hvis du legger mappa et annet sted.
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, os.pardir))
STATIC_DIR = os.path.join(PROJECT_ROOT, "kvam")

kvam_bp = Blueprint("kvam", __name__, url_prefix="/kvam")


@kvam_bp.route("/")
def kvam_index():
    """Server index.html når noen besøker /kvam/."""
    return send_from_directory(STATIC_DIR, "index.html")


@kvam_bp.route("/<path:filename>")
def kvam_static(filename):
    """Server alle øvrige filer (CSS, JSX, bilder, ...) under /kvam/."""
    full_path = os.path.join(STATIC_DIR, filename)
    if not os.path.isfile(full_path):
        abort(404)
    return send_from_directory(STATIC_DIR, filename)
