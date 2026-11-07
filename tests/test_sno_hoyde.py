from datetime import datetime, timedelta, timezone

import pytest

from scripts import sno_hoyde as sh

T0 = datetime(2026, 11, 7, 0, 0, tzinfo=timezone.utc)


def test_utm33_sentralmeridian():
    x, y = sh.utm33(60.0, 15.0)
    assert x == pytest.approx(500000.0, abs=0.01)
    assert y == pytest.approx(6651411.19, abs=1.0)


def test_utm33_vest_for_sentralmeridian():
    x, y = sh.utm33(60.3983, 5.9728)  # Kvamskogen ligger langt vest i sone 33
    assert x < 100000
    assert 6_700_000 < y < 6_760_000


def test_nedbør_faktor():
    assert sh.nedbør_faktor(800, 500) == pytest.approx(1.15)
    assert sh.nedbør_faktor(None, 500) == 1.0
    assert sh.nedbør_faktor(3000, 0) == sh.NEDBØR_FAKTOR_MAKS
    assert sh.nedbør_faktor(0, 3000) == sh.NEDBØR_FAKTOR_MIN


def _wn(timer, temp=2.0, regn=0.5):
    return [{"t": (T0 + timedelta(hours=i)).isoformat(), "temp": {"mean": temp, "p10": temp - 1, "p90": temp + 1},
             "regn": {"mean": regn, "p10": 0.0, "p90": regn * 2}, "vind": {"mean": 4.0}, "vindretning": 250}
            for i in range(timer)]


def test_temperatur_avvik_bruker_bare_timesintervaller():
    yr = [{"start": T0 + timedelta(hours=i), "timer": 1, "temp": -1.0} for i in range(24)]
    yr.append({"start": T0 + timedelta(hours=24), "timer": 6, "temp": 50.0})
    assert sh.temperatur_avvik(yr, _wn(48, temp=2.0)) == pytest.approx(-3.0)


def test_temperatur_avvik_for_få_timer():
    yr = [{"start": T0 + timedelta(hours=i), "timer": 1, "temp": -1.0} for i in range(5)]
    assert sh.temperatur_avvik(yr, _wn(48)) is None


def test_weathernext_intervaller_blokker_og_justering():
    etter = T0 + timedelta(hours=9)  # midt i blokken 06-12
    iv = sh.weathernext_intervaller(_wn(48), etter, temp_avvik=-3.0, nedbør_skala=1.1)
    assert iv[0]["start"] == etter and iv[0]["timer"] == 3
    assert iv[1]["start"] == T0 + timedelta(hours=12) and iv[1]["timer"] == 6
    assert iv[1]["temp"] == pytest.approx(-1.0)
    assert iv[1]["nedbør_mm"] == pytest.approx(6 * 0.5 * 1.1)
    assert iv[1]["nedbør_maks_mm"] == pytest.approx(6 * 1.0 * 1.1)
    assert all(x["kilde"] == "weathernext" for x in iv)
    # siste blokk 42-48 er hel, ingenting etter 48 t
    assert iv[-1]["slutt"] == T0 + timedelta(hours=48)


def test_tilpass_høyde_regresjon():
    celler = [{"moh": 400, "sd_cm": 10}, {"moh": 600, "sd_cm": 30}, {"moh": 800, "sd_cm": 50}]
    r = sh.tilpass_høyde(celler, 700)
    assert r["metode"] == "regresjon"
    assert r["sd_cm"] == pytest.approx(40)
    assert r["cm_pr_100m"] == pytest.approx(10)
    # ekstrapolering begrenses til 150 m over høyeste celle
    assert sh.tilpass_høyde(celler, 2000)["sd_cm"] == pytest.approx(65)


def test_tilpass_høyde_lite_spenn_bruker_nærmeste():
    celler = [{"moh": 500, "sd_cm": 10}, {"moh": 530, "sd_cm": 14}]
    r = sh.tilpass_høyde(celler, 800)
    assert r["metode"] == "nærmeste celle" and r["sd_cm"] == 14
    assert sh.tilpass_høyde([], 800) is None


def test_senorge_snødybde_med_falsk_nve(monkeypatch):
    sh._CACHE.clear()
    kall = []

    def falsk(x, y, dag, session):
        kall.append((x, y))
        moh = 500 + (y - y0) / 10  # 100 m høyere per km nordover
        return {"sd_cm": (moh - 400) / 5, "moh": moh}

    x0, y0 = sh.utm33(60.4, 6.0)
    monkeypatch.setattr(sh, "_gts_celle", falsk)
    r = sh.senorge_snødybde(60.4, 6.0, 700)
    assert len(kall) == 9
    assert r["sd_cm"] == pytest.approx(60, abs=0.5)
    assert r["kilde"] == "seNorge (NVE)"
