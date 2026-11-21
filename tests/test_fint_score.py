from unittest.mock import patch

from scripts import fint_score as fs


def _s(**kw):
    base = dict(temp=12.0, foles=12.0, regn=0.0, vind=2.0, kast=4.0, sky=0.0, lyst=True,
                tordenvaer=False, maaned=6)
    base.update(kw)
    return fs.score(**base)


def test_sol_og_opphold_er_straalende():
    s, grunn = _s(temp=18, foles=18)
    assert s >= 90 and grunn is None
    assert fs.nivaa(s) == "Strålende"


def test_regn_trekker_mest_og_er_grunnen():
    s, grunn = _s(regn=2.0, sky=100)
    assert s < 30 and grunn == "regn"
    assert fs.nivaa(s) == "Unngå"


def test_generelt_er_aarstidsjustert():
    # Fem grader og sol er fint i november, men ikke i juli.
    nov, _ = _s(temp=5, foles=5, maaned=11)
    jul, grunn = _s(temp=5, foles=5, maaned=7)
    assert nov >= 80 and jul < nov and grunn == "kaldt"


def test_moerke_timer_blir_aldri_straalende():
    s, _ = _s(lyst=False)
    assert s < 90


def test_torden_og_kraftig_vind():
    s, grunn = _s(tordenvaer=True)
    assert s <= 55 and grunn == "torden"
    s, grunn = _s(vind=14, kast=24)
    assert grunn == "vind" and s < 50


def test_profiler_vekter_ulikt():
    sykkel, _ = _s(vind=9, kast=14, profil="sykkel")
    lop, _ = _s(vind=9, kast=14, profil="lop")
    assert sykkel < lop
    strand, grunn = _s(temp=18, foles=18, profil="strand")
    assert grunn == "kaldt" and strand < 90


def test_skydekke_fra_symbol():
    assert fs.skydekke("clearsky_day") == 0
    assert fs.skydekke("partlycloudy_night") == 55
    assert fs.skydekke("lightrain") == 100
    assert fs.torden("heavyrainandthunder") and not fs.torden("rain")


def test_merk_timer_legger_paa_alle_profiler():
    rows = [{"time": "2026-11-21T12:00:00+01:00", "temp": 4.0, "rain": 0.0, "wind": 3.0, "gust": 6.0,
             "symbol": "fair_day"}, {"time": "2026-11-21T00:00:00+01:00", "hour": 0, "past": True}]
    fs.merk_timer(rows, lambda h: True, lambda h: h["temp"] - 2)
    h = rows[0]
    assert set(h["fint"]) == set(fs.PROFILER) and h["lyst"] is True and h["foles"] == 2.0
    assert all(0 <= v <= 100 for v in h["fint"].values())
    assert "fint" not in rows[1]


def test_varsel_api_har_fint_score():
    from scripts import vaer_varsel as vv
    from tests.test_vaer_varsel import NOW, google, yr_payload

    with patch.object(vv, "_fetch_yr", return_value=yr_payload(rain_by_hour={3: 2.0})), \
         patch.object(vv, "_fetch_nowcast", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_google", return_value=google()), \
         patch.object(vv, "_fetch_google_days", return_value=[]), \
         patch.object(vv, "_fetch_observations", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_reference", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_weathernext", side_effect=RuntimeError), \
         patch.object(vv, "_trust_rows", return_value=None):
        p = vv.build_payload("bergen", NOW)
    regn = p["detail_hours"][3]
    assert regn["fint"]["generelt"] < 30 and regn["fint_grunn"]["generelt"] == "regn"
    assert all("fint" in h for h in p["hours"])
    assert all("fint" in h for h in p["today"] if not h["past"])
    # Kl. 08 i Bergen i slutten av september er det lyst, kl. 02 natt til mandag er det mørkt.
    assert p["detail_hours"][0]["lyst"] is True
    natt = next(h for h in p["detail_hours"] if h["time"].startswith("2026-09-28T02"))
    assert natt["lyst"] is False


def test_seks_timersblokker_har_omtrentlig_fint_score():
    from scripts import vaer_varsel as vv
    from tests.test_vaer_varsel import NOW, google, yr_payload

    with patch.object(vv, "_fetch_yr", return_value=yr_payload()), \
         patch.object(vv, "_fetch_nowcast", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_google", return_value=google()), \
         patch.object(vv, "_fetch_google_days", return_value=[]), \
         patch.object(vv, "_fetch_observations", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_reference", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_weathernext", side_effect=RuntimeError), \
         patch.object(vv, "_trust_rows", return_value=None):
        p = vv.build_payload("bergen", NOW)
    assert p["blocks"]
    # Nedbørsspenn og sannsynlighet følger med på timer, blokker og dager.
    assert {"rain_min", "rain_max", "pop"} <= set(p["detail_hours"][0])
    assert {"rain_min", "rain_max", "pop"} <= set(p["blocks"][0])
    assert all("pop_max" in d and "rain_min" in d and "rain_max" in d for d in p["days"])
    for b in p["blocks"]:
        assert set(b["fint"]) == set(fs.PROFILER) and isinstance(b["lyst"], bool)
        assert b["wind"] == 3.0  # vinden ved blokkens start
    # 1,5 mm på seks timer er lett regn, ikke et skybrudd: blokken blir ikke "Unngå" bare av regnet.
    dag = next(b for b in p["blocks"] if b["lyst"])
    assert dag["fint"]["generelt"] >= 30
