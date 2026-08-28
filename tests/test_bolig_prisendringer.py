import pandas as pd
from flask import Flask

import bolig_routes
from bolig_historikk_service import build_price_change_analysis, build_price_change_details


def _sample_master():
    return pd.DataFrame({
        "finnkode": ["1001", "1002", "1003", "1004", "1005"],
        "fylke": ["Oslo", "Oslo", "Oslo", "Vestland", "Vestland"],
        "kommune_navn": ["Oslo", "Oslo", "Oslo", "Bergen", "Bergen"],
        "sted": ["Oslo", "Oslo", "Oslo", "Bergen", "Bergen"],
        "ny_brukt": ["Brukt", "Brukt", "Nybygg", "Brukt", "Nybygg"],
        "full_title": ["Bolig 1", "Bolig 2", "Bolig 3", "Bolig 4", "Bolig 5"],
        "address": ["Gate 1", "Gate 2", "Gate 3", "Gate 4", "Gate 5"],
        "publisert_dato": pd.to_datetime([
            "2026-07-01", "2026-07-10", "2026-08-01", "2026-07-05", "2026-08-05",
        ]),
        "dato_første": pd.to_datetime([
            "2026-07-01", "2026-07-10", "2026-08-01", "2026-07-05", "2026-08-05",
        ]),
        "dato_siste": pd.to_datetime([
            "2026-08-31", "2026-08-31", "2026-08-31", "2026-08-31", "2026-08-22",
        ]),
        "pris_første": [5_000_000, 4_000_000, 6_000_000, 3_000_000, 4_000_000],
        "pris_ny": [4_500_000, 3_980_000, 6_300_000, 2_900_000, 3_800_000],
        "dato_prisendring": pd.to_datetime([
            "2026-08-10", "2026-08-12", "2026-08-15", "2026-07-20", "2026-08-20",
        ]),
    })


def test_price_change_analysis_counts_ads_and_denominator_by_segment():
    table, summary = build_price_change_analysis(
        _sample_master(),
        level="Fylke",
        start_day=pd.Timestamp("2026-08-01"),
        end_day=pd.Timestamp("2026-08-31"),
        direction="Ned",
        threshold_pct=1.0,
        segment_choice="Begge",
    )

    rows = table.set_index(["område", "segment"])
    assert rows.loc[("Oslo", "Brukt"), "endret_antall"] == 1
    assert rows.loc[("Oslo", "Brukt"), "annonser_i_perioden"] == 2
    assert rows.loc[("Oslo", "Brukt"), "andel_pct"] == 50.0
    assert rows.loc[("Vestland", "Nybygg"), "endret_antall"] == 1
    assert rows.loc[("Vestland", "Nybygg"), "aktive_ved_slutt"] == 0
    assert summary == {
        "endret_antall": 2,
        "annonser_i_perioden": 5,
        "andel_pct": 40.0,
        "aktive_ved_slutt": 4,
    }


def test_price_change_analysis_supports_upward_changes_and_segment_filter():
    table, summary = build_price_change_analysis(
        _sample_master(),
        level="Kommune",
        start_day=pd.Timestamp("2026-08-01"),
        end_day=pd.Timestamp("2026-08-31"),
        direction="Opp",
        threshold_pct=4.0,
        segment_choice="Nybygg",
    )

    rows = table.set_index(["område", "segment"])
    assert rows.loc[("Oslo", "Nybygg"), "endret_antall"] == 1
    assert rows.loc[("Bergen", "Nybygg"), "endret_antall"] == 0
    assert summary["endret_antall"] == 1
    assert summary["annonser_i_perioden"] == 2
    assert summary["andel_pct"] == 50.0


def test_published_from_limits_both_changed_ads_and_denominator():
    table, summary = build_price_change_analysis(
        _sample_master(),
        level="Fylke",
        start_day=pd.Timestamp("2026-08-01"),
        end_day=pd.Timestamp("2026-08-31"),
        direction="Ned",
        threshold_pct=1.0,
        segment_choice="Begge",
        published_from=pd.Timestamp("2026-08-01"),
    )

    assert set(table["segment"]) == {"Nybygg"}
    assert summary == {
        "endret_antall": 1,
        "annonser_i_perioden": 2,
        "andel_pct": 50.0,
        "aktive_ved_slutt": 1,
    }


def test_price_change_details_return_ads_behind_selected_row():
    details = build_price_change_details(
        _sample_master(),
        level="Fylke",
        area="Oslo",
        start_day=pd.Timestamp("2026-08-01"),
        end_day=pd.Timestamp("2026-08-31"),
        direction="Ned",
        threshold_pct=1.0,
        segment_choice="Begge",
        detail_segment="Brukt",
        published_from=pd.Timestamp("2026-07-01"),
    )

    assert details["finnkode"].tolist() == ["1001"]
    assert details.iloc[0]["endring_pct"] == -10.0


def test_price_change_page_is_available_from_bolig_hub(monkeypatch):
    monkeypatch.setattr(
        bolig_routes,
        "load_normalized_master_cached",
        lambda _bucket, _key: _sample_master(),
    )
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(bolig_routes.bolig_bp)
    client = app.test_client()

    response = client.get(
        "/bolig/prisendringer/",
        query_string={
            "level": "Fylke",
            "ny_brukt": "Begge",
            "direction": "Ned",
            "threshold": "1",
            "start": "2026-08-01",
            "end": "2026-08-31",
            "published_from": "2026-07-01",
            "area": "Oslo",
            "detail_segment": "Brukt",
        },
    )
    hub_response = client.get("/bolig/")

    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "Prisjusteringer" in html
    assert "40,0 %" in html
    assert "Brukt og nybygg, separat" in html
    assert "Lagt ut tidligst" in html
    assert "Kontrolliste: Oslo" in html
    assert "Bolig 1" in html
    assert "1001" in html
    assert html.index("<th>Endrede annonser") < html.index("<th>Område")
    assert hub_response.status_code == 200
    assert 'href="/bolig/prisendringer/"' in hub_response.data.decode("utf-8")
