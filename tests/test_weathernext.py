from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

from scripts import weathernext as wn

INIT = datetime(2026, 9, 26, 6, tzinfo=timezone.utc)
INIT_MS = int(INIT.timestamp() * 1000)
BANDS = [f"{basis}_{s}" for basis in wn.VARIABLER for s in wn.STATS]


def tabell(leads, temp_k=283.15, regn_m=0.0012, init_ms=INIT_MS):
    hode = ["id", "longitude", "latitude", "time", *BANDS, "ledetid"]
    rader = []
    for lead in leads:
        verdier = []
        for b in BANDS:
            if b.startswith("temperature_2m"):
                verdier.append(temp_k + lead)  # stiger 1 grad per time
            elif b.startswith("dewpoint"):
                verdier.append(temp_k - 3)
            elif "tp_1hr" in b or "precipitation_1hr" in b:
                verdier.append(regn_m * lead)
            elif b.startswith("u_component"):
                verdier.append(-5.0)  # blåser mot vest = fra øst
            elif b.startswith("v_component"):
                verdier.append(0.0)
            elif "cloud" in b:
                verdier.append(0.5)
            else:
                verdier.append(1.0)
        rader.append(["x", 5.3, 60.4, init_ms, *verdier, float(lead)])
    return [hode, *rader]


def test_rader_fra_tabell_gir_en_rad_per_ledetid():
    rader = wn.rader_fra_tabell(tabell([2, 1, 3]), INIT_MS)
    assert [r["ledetid"] for r in rader] == [1, 2, 3]
    assert rader[0]["gyldig"] == INIT + timedelta(hours=1)
    assert "ledetid" not in rader[0]["baand"]


def test_timesum_hentes_fra_timen_som_slutter_etter():
    """Nedbør for [t, t+1) ligger i bildet med gyldighetstid t+1."""
    rader = wn.rader_fra_tabell(tabell([1, 2, 3]), INIT_MS)
    timer = wn.til_timer(wn.slaa_sammen(rader))
    forste = timer[0]
    assert forste["t"] == "2026-09-26T07:00:00Z"
    assert forste["temp"]["mean"] == pytest.approx(11.0)       # øyeblikk ved t (ledetid 1)
    assert forste["regn"]["mean"] == pytest.approx(2.4)        # fra ledetid 2: 0.0012*2 m
    assert "regn" not in timer[-1]                              # siste time mangler t+1
    assert forste["vindretning"] == 90


def test_vindretning_er_hvor_vinden_kommer_fra():
    assert wn.vindretning(0, -5) == pytest.approx(0)     # blåser sørover = fra nord
    assert wn.vindretning(-5, 0) == pytest.approx(90)    # blåser vestover = fra øst
    assert wn.vindretning(5, 0) == pytest.approx(270)


def test_nyeste_kjoring_vinner_per_time():
    lang = wn.rader_fra_tabell(tabell(range(1, 10), temp_k=280.0), INIT_MS)
    ny_init = INIT + timedelta(hours=3)
    kort = wn.rader_fra_tabell(tabell(range(1, 4), temp_k=290.0, init_ms=int(ny_init.timestamp() * 1000)),
                               int(ny_init.timestamp() * 1000))
    varsel = wn.bygg_varsel({"lang": lang, "kort": kort}, 60.39, 5.32, now=INIT + timedelta(hours=9))
    per_time = {t["t"]: t for t in varsel["timer"]}
    assert per_time["2026-09-26T08:00:00Z"]["init"] == "2026-09-26T06:00:00Z"
    assert per_time["2026-09-26T10:00:00Z"]["init"] == "2026-09-26T09:00:00Z"
    assert varsel["init_lang"] == "2026-09-26T06:00:00Z"
    assert varsel["init_kort"] == "2026-09-26T09:00:00Z"
    assert varsel["celle"] == {"lat": 60.4, "lon": 5.3}


def test_negativ_nedbor_klippes_og_kompakt_filtrerer():
    rader = wn.rader_fra_tabell(tabell([1, 2, 3], regn_m=-0.0001), INIT_MS)
    varsel = wn.bygg_varsel({"lang": rader, "kort": []}, 60.39, 5.32, now=INIT)
    assert varsel["timer"][0]["regn"]["mean"] == 0.0
    liten = wn.kompakt(varsel, fra=INIT + timedelta(hours=2))
    assert [t["t"] for t in liten["timer"]] == ["2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z"]
    assert set(liten["timer"][0]["temp"]) == {"mean", "p10", "p25", "p75", "p90"}
    assert "u10" not in liten["timer"][0]


def test_scoreboard_rader_bruker_kjoretimen_som_ledetid():
    rader = wn.rader_fra_tabell(tabell(range(1, 60)), INIT_MS)
    varsel = wn.bygg_varsel({"lang": rader, "kort": []}, 60.39, 5.32, now=INIT)
    run_hour = INIT + timedelta(hours=8)
    ut = wn.scoreboard_rader(varsel, "bergen", run_hour, run_hour)
    assert ut[0]["valid_start"] == "2026-09-26T14:00:00Z"
    assert ut[0]["lead_hours"] == 0
    assert max(r["lead_hours"] for r in ut) == 48
    assert {r["provider"] for r in ut} == {"weathernext"}
    assert ut[0]["gust"] is None


def test_lagring_og_fast_sted(tmp_path, monkeypatch):
    monkeypatch.setenv("WEATHERNEXT_DIR", str(tmp_path))
    monkeypatch.delenv("S3_BUCKET_NAME", raising=False)
    monkeypatch.delenv("WEATHER_SCOREBOARD_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("EE_PROJECT", raising=False)
    rader = wn.rader_fra_tabell(tabell([1, 2, 3]), INIT_MS)
    now = datetime.now(timezone.utc)
    varsel = wn.bygg_varsel({"lang": rader, "kort": []}, 60.393, 5.3242, now=now)
    wn.lagre_siste("bergen", varsel)
    assert wn.arkiver_kjoring("bergen", rader) is not None
    assert wn.arkiver_kjoring("bergen", rader) is None  # samme kjøring skrives ikke to ganger

    svar = wn.for_punkt(60.39299, 5.32415)
    assert svar["modus"] == "fast"
    with pytest.raises(wn.WeatherNextError):
        wn.for_punkt(59.91, 10.75)  # Oslo: ikke fast, og EE er ikke konfigurert


def test_live_oppslag_caches_per_celle(monkeypatch):
    monkeypatch.setenv("EE_PROJECT", "test")
    monkeypatch.setenv("WEATHERNEXT_LIVE", "1")
    wn._CACHE.clear()
    kall = []

    def fake(lat, lon, now=None):
        kall.append((lat, lon))
        return {"lang": wn.rader_fra_tabell(tabell([1, 2]), INIT_MS), "kort": []}

    monkeypatch.setattr(wn, "hent_kjoringer", fake)
    assert wn.for_punkt(59.912, 10.751)["modus"] == "live"
    assert wn.for_punkt(59.93, 10.77)["modus"] == "cache"  # samme 0.1°-celle
    assert kall == [(59.9, 10.8)]


def test_endepunkt_svarer_503_uten_oppsett(monkeypatch):
    from scripts.ver_routes import ver

    monkeypatch.delenv("EE_PROJECT", raising=False)
    monkeypatch.setattr(wn, "les_siste", lambda place: None)
    app = Flask(__name__)
    app.register_blueprint(ver, url_prefix="/ver")
    with app.test_client() as client:
        assert client.get("/ver/api/weathernext").status_code == 400
        svar = client.get("/ver/api/weathernext?lat=59.91&lon=10.75")
        assert svar.status_code == 503
        assert svar.get_json()["aktiv"] is False


def test_init_med_egen_innlogging(monkeypatch):
    import ee

    fanget = {}
    monkeypatch.setattr(ee, "Initialize", lambda creds=None, project=None: fanget.update(creds=creds, project=project))
    monkeypatch.setattr(wn, "_EE_KLAR", False)
    monkeypatch.setenv("EE_PROJECT", "mitt-prosjekt")
    monkeypatch.delenv("EE_SERVICE_ACCOUNT_KEY", raising=False)
    monkeypatch.setenv("EE_USER_CREDENTIALS", '{"refresh_token": "abc", "project": "mitt-prosjekt"}')
    wn._init_ee()
    assert fanget["project"] == "mitt-prosjekt"
    assert fanget["creds"].refresh_token == "abc"
    monkeypatch.setattr(wn, "_EE_KLAR", False)


def test_live_er_av_som_standard(monkeypatch):
    monkeypatch.setenv("EE_PROJECT", "test")
    monkeypatch.delenv("WEATHERNEXT_LIVE", raising=False)
    monkeypatch.setattr(wn, "les_siste", lambda place: None)
    monkeypatch.setattr(wn, "hent_kjoringer", lambda *a, **k: pytest.fail("skal ikke hente live"))
    with pytest.raises(wn.WeatherNextError, match="ikke hentet ennå"):
        wn.for_punkt(60.39299, 5.32415)
    with pytest.raises(wn.WeatherNextError, match="bare for Bergen"):
        wn.for_punkt(59.91, 10.75)
