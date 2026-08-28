import csv
import io

import pandas as pd
from flask import Flask

import bolig_routes


def _sample_master():
    return pd.DataFrame({
        "finnkode": ["1001", "1002"],
        "fylke": ["Oslo", "Oslo"],
        "kommune_navn": ["Oslo", "Oslo"],
        "sted": ["Oslo", "Oslo"],
        "address": ["Gate 1, 0001 Oslo", "Gate 2, 0002 Oslo"],
        "full_title": ["Bolig 1", "Bolig 2"],
        "boligtype": ["Leilighet", "Leilighet"],
        "ny_brukt": ["Brukt", "Brukt"],
        "areal": [50, 50],
        "m2_pris": [100_000, 120_000],
        "totalpris": [5_000_000, 6_000_000],
        "publisert_dato": pd.to_datetime(["2026-08-01", "2026-08-02"]),
        "dato_første": pd.to_datetime(["2026-08-01", "2026-08-02"]),
        "dato_siste": pd.to_datetime(["2026-08-03", "2026-08-02"]),
    })


def test_historikk_detail_csv_contains_daily_graph_data(monkeypatch):
    monkeypatch.setattr(
        bolig_routes,
        "load_normalized_master_cached",
        lambda _bucket, _key: _sample_master(),
    )
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(bolig_routes.bolig_bp)

    response = app.test_client().get(
        "/bolig/historikk/detalj/eksport.csv",
        query_string={
            "level": "Fylke",
            "value": "Oslo",
            "ny_brukt": "Brukt",
            "start": "2026-08-01",
            "end": "2026-08-03",
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert response.headers["Content-Disposition"] == (
        'attachment; filename="bolighistorikk_Oslo_20260801_20260803.csv"'
    )

    text = response.data.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    assert [row["Dato"] for row in rows] == ["2026-08-01", "2026-08-02", "2026-08-03"]
    assert rows[1]["Aktive annonser"] == "2"
    assert rows[1]["Median m²-pris (aktive)"] == "110000,0"
    assert rows[1]["Nye annonser"] == "1"
    assert rows[1]["Forsvunne annonser"] == "1"


def test_historikk_detail_csv_returns_message_when_filter_has_no_data(monkeypatch):
    monkeypatch.setattr(
        bolig_routes,
        "load_normalized_master_cached",
        lambda _bucket, _key: _sample_master(),
    )
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(bolig_routes.bolig_bp)

    response = app.test_client().get(
        "/bolig/historikk/detalj/eksport.csv",
        query_string={
            "level": "Fylke",
            "value": "Vestland",
            "ny_brukt": "Brukt",
            "start": "2026-08-01",
            "end": "2026-08-03",
        },
    )

    assert response.status_code == 200
    assert "Ingen data matchet" in response.data.decode("utf-8-sig")


def test_historikk_detail_page_shows_csv_download_button(monkeypatch):
    monkeypatch.setattr(
        bolig_routes,
        "load_normalized_master_cached",
        lambda _bucket, _key: _sample_master(),
    )
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(bolig_routes.bolig_bp)

    response = app.test_client().get(
        "/bolig/historikk/detalj/",
        query_string={
            "level": "Fylke",
            "value": "Oslo",
            "ny_brukt": "Brukt",
            "start": "2026-08-01",
            "end": "2026-08-03",
        },
    )

    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "Last ned CSV" in html
    assert 'formaction="/bolig/historikk/detalj/eksport.csv"' in html
