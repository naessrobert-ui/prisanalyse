"""Tests for municipality top-companies map data."""
from unittest.mock import patch

import analysis_api_compat as api


def test_top_by_municipality_preserves_ranking_and_missing_geocodes():
    ranked = {
        "total_returned": 2,
        "results": [
            {"orgnr": "111111111", "navn": "A", "omsetning": 1000, "driftsresultat": 250},
            {"orgnr": "222222222", "navn": "B", "omsetning": 1000, "driftsresultat": -20},
        ],
    }
    with patch.object(api, "get_companies_filter_payload", return_value=ranked) as fetch_ranked, patch.object(
        api, "fetch_all", return_value=[
            {"orgnr": "111111111", "adresse": "Testvegen 1", "postnummer": "5500", "lat": 59.4, "lon": 5.3},
        ]
    ):
        result = api.get_top_companies_by_municipality_payload(
            kommunenummer="1106", regnskapsaar=2025, limit=100
        )

    fetch_ranked.assert_called_once_with(
        kommune="1106", regnskapsaar=2025, has_regnskap=True,
        limit=100, sort_by="driftsresultat", sort_dir="desc",
    )
    assert result["count"] == 2
    assert result["with_coordinates"] == 1
    assert result["results"][0]["driftsmargin_prosent"] == 25.0
    assert result["results"][0]["farge"] == "green"
    assert result["results"][1]["driftsmargin_prosent"] == -2.0
    assert result["results"][1]["farge"] == "red"
    assert result["results"][1]["has_coordinates"] is False


def test_top_by_municipality_rejects_bad_municipality():
    import pytest
    with pytest.raises(ValueError):
        api.get_top_companies_by_municipality_payload(kommunenummer="Haugesund")
