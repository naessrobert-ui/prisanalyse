from datetime import datetime, timedelta, timezone

import pytest

from scripts import klima_norge as kn

NAA = datetime(2026, 11, 22, 10, 0, tzinfo=timezone.utc)

STASJONER = [
    {"id": "SN50540", "sted": "Bergen", "fylke": "Vestland", "lat": 60.38, "lon": 5.33,
     "temp": True, "nedbor": True, "sol": False, "sno": False},
    {"id": "SN18700", "sted": "Oslo", "fylke": "Oslo", "lat": 59.94, "lon": 10.72,
     "temp": True, "nedbor": True, "sol": True, "sno": True},
    {"id": "SN97251", "sted": "Karasjok", "fylke": "Finnmark", "lat": 69.46, "lon": 25.5,
     "temp": True, "nedbor": True, "sol": False, "sno": True},
    {"id": "SN25830", "sted": "Finse", "fylke": "Vestland", "lat": 60.59, "lon": 7.52,
     "temp": True, "nedbor": True, "sol": False, "sno": True},
]


def _rad(sid, el, dag, v, time="06"):
    return {"id": sid, "el": el, "t": f"2026-11-{dag:02d}T{time}:00:00.000Z", "v": v}


def _frost_sist(ids, elementer, referencetime):
    rader = []
    for dag in (20, 21):
        rader += [
            _rad("SN50540", "max(air_temperature P1D)", dag, 11.0 + dag / 10, "00"),
            _rad("SN18700", "max(air_temperature P1D)", dag, 3.0, "00"),
            _rad("SN97251", "min(air_temperature P1D)", dag, -20.0 - dag, "00"),
            _rad("SN18700", "min(air_temperature P1D)", dag, -4.0, "00"),
            _rad("SN50540", "sum(precipitation_amount P1D)", dag, 40.0),
            _rad("SN25830", "sum(precipitation_amount P1D)", dag, 12.0),
            _rad("SN50540", "max(wind_speed_of_gust P1D)", dag, 26.0, "00"),
            _rad("SN18700", "sum(duration_of_sunshine P1D)", dag, 4.5, "00"),
            _rad("SN25830", "surface_snow_thickness", dag, 90.0 + (dag - 20) * 18, "06"),
        ]
    rader.append(_rad("SN25830", "max(air_temperature P1D)", 21, 99.0, "00"))  # målefeil
    rader.append(_rad("SN18700", "max(air_temperature P1D)", 22, 5.0, "00"))  # én tidlig rapport
    return rader


def test_sist_velger_siste_hele_dogn_og_lager_tekst(monkeypatch):
    monkeypatch.setattr(kn, "_frost_hent", _frost_sist)
    p = kn.bygg_sist(STASJONER, NAA)
    assert p["dato"] == "2026-11-21"
    assert p["varm"]["sted"] == "Bergen" and p["varm"]["v"] == 13.1  # 99 er filtrert bort
    assert p["varm"]["topp"] == [["Oslo", 3.0]]
    assert p["kald"]["v"] == -41.0 and p["kald"]["sted"] == "Karasjok"
    assert p["vind"]["kast"] is True and "storms styrke" in p["vind"]["merk"]
    assert p["sno"]["v"] == 108 and "18 cm nysnø" in p["sno"]["merk"]
    assert p["spenn"] == 54.1
    assert "lørdag 21. november" in p["lead"]


def _frost_mnd(ids, elementer, referencetime):
    rader = []
    for dag in range(1, 22):
        rader += [
            _rad("SN50540", "mean(air_temperature P1D)", dag, 9.0, "00"),
            _rad("SN18700", "mean(air_temperature P1D)", dag, 1.0, "00"),
            _rad("SN50540", "max(air_temperature P1D)", dag, 10.0 + (dag == 3) * 6, "00"),
            _rad("SN18700", "min(air_temperature P1D)", dag, -2.0, "00"),
            _rad("SN50540", "sum(precipitation_amount P1D)", dag, 20.0),
            _rad("SN18700", "sum(precipitation_amount P1D)", dag, 0.2),
        ]
    return rader


def test_mnd_og_uvanlig(monkeypatch):
    monkeypatch.setattr(kn, "_frost_hent", _frost_mnd)
    monkeypatch.setattr(kn, "_normaler", lambda: {
        "SN50540": {"temp": [0] * 10 + [5.4, 0], "nedbor": [0] * 10 + [266.5, 0]},
        "SN18700": {"temp": [0] * 10 + [2.2, 0], "nedbor": [0] * 10 + [80.3, 0]},
    })
    p, uvanlig, agg = kn.bygg_mnd(STASJONER, NAA)
    assert p["tittel"] == "i november"
    assert p["varm"]["v"] == 16.0 and "3. november" in p["varm"]["merk"]
    assert p["vaat"]["v"] == 420 and "% av normalen" in p["vaat"]["merk"]
    tekster = " ".join(u["sted"] + " " + u["tekst"] for u in uvanlig)
    assert "Bergen har vært 3,6° varmere" in tekster
    assert "Oslo har fått 4 mm" in tekster
    assert agg["maaned"] == "2026-11"


def test_aar_slaar_sammen_maaneder_og_inneverende(monkeypatch):
    def frost(ids, elementer, referencetime):
        assert "P1M" in elementer[0]
        return [{"id": "SN50540", "el": "max(air_temperature P1M)", "t": "2026-07-01T00:00:00.000Z", "v": 29.5},
                {"id": "SN97251", "el": "min(air_temperature P1M)", "t": "2026-01-01T00:00:00.000Z", "v": -41.2}]
    monkeypatch.setattr(kn, "_frost_hent", frost)
    agg = {"maaned": "2026-11", "agg": {"tmax": {"SN18700": [31.0, "2026-11-02"]}}}
    p = kn.bygg_aar(STASJONER, agg, NAA)
    assert p["varm"]["sted"] == "Oslo" and "2. november" in p["varm"]["merk"]
    assert p["kald"]["v"] == -41.2 and "(januar)" in p["kald"]["merk"]


def _yr(lat, lon):
    start = NAA
    ts = []
    for h in range(0, 60):
        t = start + timedelta(hours=h)
        ts.append({"time": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "data": {
            "instant": {"details": {"air_temperature": -2.0 if lat > 60.5 else 4.0, "wind_speed_of_gust": lat / 3,
                                    "cloud_area_fraction": 0.0}},
            "next_1_hours": {"details": {"precipitation_amount": 1.0, "precipitation_amount_min": 0.5,
                                         "precipitation_amount_max": 2.0}}}})
    for h in range(60, 240, 6):
        t = start + timedelta(hours=h)
        ts.append({"time": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "data": {
            "instant": {"details": {"air_temperature": 0.0, "cloud_area_fraction": 50.0}},
            "next_6_hours": {"details": {"precipitation_amount": 3.0}}}})
    return ts


def test_varsel_neste_og_uke():
    neste, uke = kn.bygg_varsel(STASJONER, NAA, hent=_yr)
    assert neste["vaat"]["v"] == 24.0
    assert "Kan bli 12 til 48 mm" in neste["vaat"]["merk"]
    assert neste["kald"]["v"] == -2.0
    assert neste["sno"]["nysno"] is True and neste["sno"]["v"] == 26  # 24 mm ved −2° gir 26 cm
    assert neste["sol"]["v"] > 0
    assert uke["vaat"]["v"] == pytest.approx(60 + 18 * 3, abs=1)
    assert uke["n"] == 4


def test_oppfrisk_lagrer_og_hent_leser(monkeypatch, tmp_path):
    monkeypatch.setattr(kn, "_DATA", {})
    monkeypatch.setattr(kn, "_DATA_TS", 0.0)
    monkeypatch.setattr(kn, "_LOKAL", tmp_path / "k.json")
    monkeypatch.setattr(kn, "_s3", lambda: None)
    monkeypatch.setattr(kn, "_stasjoner", lambda: STASJONER)
    monkeypatch.setenv("FROST_CLIENT_ID", "x")
    monkeypatch.setattr(kn, "bygg_sist", lambda s: {"tittel": "siste døgn", "varm": {"v": 1}})
    monkeypatch.setattr(kn, "bygg_mnd", lambda s: ({"tittel": "i november"}, [{"sted": "A"}], {}))
    monkeypatch.setattr(kn, "bygg_varsel", lambda s: ({"tittel": "neste døgn"}, {"tittel": "neste 7 døgn"}))

    def aar_feiler(s, agg):
        raise RuntimeError("Frost nede")
    monkeypatch.setattr(kn, "bygg_aar", aar_feiler)
    data = kn.oppfrisk(tving=True)
    assert data["sist"]["varm"]["v"] == 1 and "aar" not in data
    kn._DATA, kn._DATA_TS = {}, 0.0
    ut = kn.offentlig(kn.hent())
    assert set(ut) >= {"sist", "neste", "uke", "mnd", "uvanlig", "oppdatert"}
    assert "_oppdatert" not in ut


def test_klimaside_og_api(monkeypatch):
    from flask import Flask
    from scripts import vaer_varsel as vv

    varm = {"v": 12.4, "sted": "Sunndalsøra", "fylke": "Møre og Romsdal", "lat": 62.68, "lon": 8.56, "merk": "", "topp": []}
    data = {"sist": {"tittel": "siste døgn", "lead": "Målt", "varm": varm},
            "uvanlig": [], "oppdatert": "2026-11-22T09:00:00+00:00", "_oppdatert": {}}
    monkeypatch.setattr(kn, "hent", lambda: data)
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(vv.vaer_varsel)
    c = app.test_client()
    side = c.get("/ver/klima").get_data(as_text=True)
    assert "Sunndals\\u00f8ra" in side or "Sunndalsøra" in side
    api = c.get("/ver/api/klima/norge").get_json()
    assert api["sist"]["varm"]["v"] == 12.4 and "_oppdatert" not in api
    assert vv._klima_kort()["varm"] == {"v": 12.4, "sted": "Sunndalsøra"}
