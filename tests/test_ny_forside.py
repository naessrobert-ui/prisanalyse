"""Ny forside på blirdetfint.no: mal, farevarsel og radar for alle norske steder."""
from unittest.mock import patch

import pytest
from flask import Flask

from scripts import vaer_varsel as vv


@pytest.fixture(autouse=True)
def _ren_cache(monkeypatch):
    monkeypatch.setenv("VAER_FORVARM", "0")
    vv._CACHE.clear()
    yield
    vv._CACHE.clear()


def _client():
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(vv.vaer_varsel)
    return app.test_client()


def test_ny_forside_er_standard_og_klassisk_finnes(monkeypatch):
    c = _client()
    ny = c.get("/ver/varsel/bergen").get_data(as_text=True)
    assert "blir det fint" in ny and 'const STED = "bergen"' in ny
    assert "const HAR_RADAR = true" in ny and "const RADAR_OMRAADE = null" in ny
    klassisk = c.get("/ver/varsel/bergen?klassisk=1").get_data(as_text=True)
    assert "blir det fint<span" not in klassisk and 'const STED = "bergen"' in klassisk
    monkeypatch.setenv("VAER_NY_FORSIDE", "0")
    assert "blir det fint<span" not in c.get("/ver/varsel/bergen").get_data(as_text=True)


def test_norsk_punkt_faar_radaromraade():
    side = _client().get("/ver/varsel/p63.430_10.395?navn=Trondheim").get_data(as_text=True)
    assert 'const RADAR_OMRAADE = "central_norway"' in side


def test_radar_omraade():
    assert vv.radar_omraade(60.39, 5.32) == "western_norway"
    assert vv.radar_omraade(59.91, 10.75) in {"eastern_norway", "southeastern_norway"}
    assert vv.radar_omraade(58.97, 5.73) == "southwestern_norway"
    assert vv.radar_omraade(69.65, 18.96) == "troms"
    assert vv.radar_omraade(70.66, 23.68) == "finnmark"
    assert all(vv.radar_omraade(lat, lon) in vv.RADAR_OMRAADER
               for lat in range(58, 71) for lon in range(5, 30, 3))


def test_radar_omraade_rute():
    c = _client()
    with patch.object(vv, "_fetch_radar", return_value=b"GIF89a") as hent:
        res = c.get("/ver/api/radar/omraade/central_norway.gif")
    assert res.status_code == 200 and res.mimetype == "image/gif"
    hent.assert_called_once_with("central_norway")
    # Mangler området hos MET, brukes hele Norge.
    with patch.object(vv, "_fetch_radar", side_effect=[vv.requests.HTTPError("404"), b"GIF89a"]) as hent:
        res = c.get("/ver/api/radar/omraade/troms.gif")
    assert res.status_code == 200 and [a.args[0] for a in hent.call_args_list] == ["troms", "norway"]
    assert c.get("/ver/api/radar/omraade/../../etc.gif").status_code == 404
    assert c.get("/ver/api/radar/omraade/mars.gif").status_code == 404


def test_parse_farevarsel_alvorligste_forst_og_taaler_mangler():
    payload = {"features": [
        {"properties": {"title": "Kuling, gult nivå", "awareness_level": "2; yellow; Moderate",
                        "description": "Stiv kuling.", "instruction": "Sikre løse gjenstander."},
         "when": {"interval": ["2026-11-21T06:00:00+00:00", "2026-11-21T18:00:00+00:00"]}},
        {"properties": {"riskMatrixColor": "Orange", "eventAwarenessName": "Jordskredfare", "area": "Vestland"}},
        {"properties": {"status": "Test", "title": "Øvelse"}},
    ]}
    v = vv.parse_farevarsel(payload)
    assert [x["farge"] for x in v] == ["orange", "yellow"]
    assert v[0]["tittel"] == "Jordskredfare Vestland" and v[0]["slutt"] is None
    assert v[1]["rad"] == "Sikre løse gjenstander." and v[1]["slutt"].startswith("2026-11-21T18")
    assert vv.parse_farevarsel({}) == []


def test_farevarsel_api_feiler_aldri_synlig():
    c = _client()
    with patch.object(vv, "_fetch_farevarsel", side_effect=RuntimeError("nede")):
        res = c.get("/ver/api/farevarsel/bergen")
    assert res.status_code == 200 and res.json == {"varsler": []}
    with patch.object(vv, "_fetch_farevarsel", return_value={"features": [{"properties": {"title": "Is", "severity": "Extreme"}}]}):
        res = c.get("/ver/api/farevarsel/bergen")
    assert res.json["varsler"][0]["farge"] == "red"
    assert c.get("/ver/api/farevarsel/oslo").status_code == 404


def test_utenlandsk_punkt_spør_ikke_met_om_farevarsel():
    with patch.object(vv, "_fetch_farevarsel") as hent:
        res = _client().get("/ver/api/farevarsel/p36.720_-4.420?navn=M%C3%A1laga")
    assert res.json == {"varsler": []}
    hent.assert_not_called()


def test_ikonene_finnes_for_alle_yr_symboler():
    import os
    mappe = os.path.join(os.path.dirname(__file__), "..", "static", "ver", "ikoner")
    for kode in ("clearsky_day", "fair_night", "partlycloudy_polartwilight", "cloudy", "fog", "lightrain",
                 "heavyrainshowersandthunder_day", "lightssleetshowersandthunder_night", "snow"):
        assert os.path.exists(os.path.join(mappe, kode + ".svg")), kode


def test_rainviewer_liste_via_oss():
    class Svar:
        def json(self):
            return {"host": "https://tilecache.rainviewer.com", "radar": {"past": [{"time": 1, "path": "/v2/radar/abc"}]}}
    c = _client()
    with patch.object(vv.requests, "get", return_value=Svar()) as hent:
        res = c.get("/ver/api/rainviewer.json")
        c.get("/ver/api/rainviewer.json")
    assert res.status_code == 200 and res.json["radar"]["past"][0]["path"] == "/v2/radar/abc"
    assert hent.call_count == 1  # cachet
    vv._CACHE.clear()
    with patch.object(vv.requests, "get", side_effect=vv.requests.ConnectionError):
        assert c.get("/ver/api/rainviewer.json").status_code == 502
