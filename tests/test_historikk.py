from datetime import date

import numpy as np
import pandas as pd

from scripts import historikk as h


def _df(til="2026-11-22"):
    idx = pd.date_range("2000-01-01", til, freq="D")
    df = pd.DataFrame(index=idx)
    df["rr"] = 1.0
    df["tmid"] = 5.0
    df["tmin"] = 0.0
    df["tmax"] = 10.0
    df["stasjon"] = "SN50540"
    return df


def test_paagaaende_periode_sammenlignes_med_samme_dager():
    df = _df()
    df.loc["2010-11-01":"2010-11-22", "rr"] = 10.0   # 2010 våtest i 1.-22. nov
    df.loc["2010-11-23":"2010-11-30", "rr"] = 0.0    # men resten av november teller ikke
    df.loc["2026-11-05", "rr"] = 50.0
    r = h.beregn(df, (11, 1), (11, 30))
    assert r["i_aar"] == 2026 and r["periode"]["pagaar"] is True
    assert r["periode"]["brukt_til"] == "11-22"
    assert r["nedbor"]["verdi"] == 71.0              # 21 dager à 1 mm + 50
    assert r["nedbor"]["vaateste"][0] == [2010, 220.0]
    assert r["nedbor"]["rang_vaat"] == 2
    assert r["nedbor"]["dognrekord"][0] == ["2026-11-05", 50.0]


def test_periode_over_nyttaar_hoerer_til_aaret_den_slutter():
    df = _df("2026-02-12")
    df.loc["2025-12-24", "rr"] = 30.0
    r = h.beregn(df, (12, 1), (2, 28))
    assert r["i_aar"] == 2026 and r["periode"]["pagaar"] is True and r["periode"]["over_nyttaar"]
    # 1. des 2025 til 12. feb 2026 = 74 dager
    assert r["nedbor"]["verdi"] == 74 - 1 + 30
    aar = {a["aar"]: a for a in r["aar"]}
    assert 2000 not in aar  # perioden for 2000 startet før serien
    assert aar[2001]["rr"] == 74.0


def test_avsluttet_periode_og_regndager():
    df = _df()
    df.loc[(df.index.month == 7), "rr"] = 0.0
    df.loc["2025-07-10", "rr"] = 6.0
    r = h.beregn(df, (7, 1), (7, 31), grense=5)
    assert r["i_aar"] == 2026 and r["periode"]["pagaar"] is False
    assert r["nedbor"]["regndager"] == 0
    aar = {a["aar"]: a for a in r["aar"]}
    assert aar[2025]["regndager"] == 1 and aar[2025]["rr"] == 6.0


def test_aar_med_hull_utelates():
    df = _df()
    df.loc["2005-03-01":"2005-03-10", "rr"] = np.nan
    r = h.beregn(df, (3, 1), (3, 31))
    aar = {a["aar"]: a for a in r["aar"]}
    assert aar[2005]["rr"] is None
    assert r["nedbor"]["antall_aar"] == 26


def test_temperatur_rangeres():
    df = _df()
    df.loc["2014-11-01":"2014-11-22", "tmid"] = 9.0
    df.loc["2026-11-01":"2026-11-22", "tmid"] = 7.0
    r = h.beregn(df, (11, 1), (11, 30))
    t = r["temperatur"]
    assert t["verdi"] == 7.0 and t["rang_varm"] == 2 and t["varmeste"][0] == [2014, 9.0]


def test_nedbor_fra_frost_forskyves_til_samme_dato_som_serien():
    df = _df("2026-02-12")
    df["rr"] = np.arange(len(df)) % 7 + 0.5
    # Frost leverer samme verdier datert én dag senere.
    rader = []
    for d in pd.date_range("2026-01-20", "2026-02-12"):
        rader.append({"el": "sum(precipitation_amount P1D)", "t": (d + pd.Timedelta(days=1)).strftime("%Y-%m-%dT06:00:00Z"),
                      "v": float(df.at[d, "rr"])})
    rader.append({"el": "sum(precipitation_amount P1D)", "t": "2026-02-14T06:00:00Z", "v": 3.3})
    nye = h._flett_inn(df, rader, "SN50540", date(2026, 11, 22))
    assert nye == 1 and df.at[pd.Timestamp("2026-02-13"), "rr"] == 3.3


def test_seed_er_hele_serien():
    df = h._les_seed("bergen")
    assert df.index.min() == pd.Timestamp("1861-01-01")
    assert len(df) > 60000
    aar = df.loc["1991":"2020", "rr"].resample("YE").sum().mean()
    assert 2200 < aar < 2700  # Bergen Florida-normalen er ca. 2500 mm


def test_ruter(monkeypatch):
    from flask import Flask
    from scripts import vaer_varsel as vv

    monkeypatch.setattr(h, "hent", lambda serie: _df())
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(vv.vaer_varsel)
    c = app.test_client()
    r = c.get("/ver/api/historikk/bergen?fra=10-15&til=11-15&grense=1").get_json()
    assert r["periode"]["tekst"] == "15. oktober til 15. november" and r["periode"]["grense"] == 1.0
    assert c.get("/ver/historikk/bergen").status_code == 200
    assert c.get("/ver/historikk/oslo").status_code == 404


def test_oppdater_henter_nye_dager_og_temperatur(monkeypatch):
    """Regresjon: nye rader ble lagt bakerst uten sortering, så neste
    datoutsnitt feilet og ingenting ble lagret."""
    from datetime import timedelta

    base = _df("2026-02-12")
    base[["tmid", "tmin", "tmax"]] = np.nan
    base = base.loc["2024-01-01":]
    lagret = {}
    monkeypatch.setitem(h.SERIER["bergen"], "kjede", [("SN50540", "2024-01-01", None)])
    monkeypatch.setattr(h, "_les_lagret", lambda serie: None)
    monkeypatch.setattr(h, "_les_seed", lambda serie: base.copy())
    monkeypatch.setattr(h, "_lagre", lambda serie, df: lagret.__setitem__(serie, df.copy()))
    monkeypatch.setattr(h, "_DATA", {})

    def frost(stasjon, elementer, fra, til):
        ut, d = [], fra
        while d <= til:
            for el in elementer:
                tid = "06" if "precip" in el else "00"
                ut.append({"el": el, "t": d.strftime(f"%Y-%m-%dT{tid}:00:00.000Z"), "v": 1.0 if "precip" in el else 4.0})
            d += timedelta(days=1)
        return ut
    monkeypatch.setattr(h, "_frost", frost)
    df = h.oppdater("bergen")
    assert df is not None and df.index.is_monotonic_increasing
    assert df.index.max().date() >= date.today() - timedelta(days=2)
    assert df["tmid"].notna().sum() > 900
    assert "bergen" in lagret and h._STATUS["bergen"]["feil"] is None
