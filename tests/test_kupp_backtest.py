"""Tester for scripts/kupp_backtest.py – rene hjelpefunksjoner + ende-til-ende
paa et syntetisk datasett (uten S3)."""
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
import pytest

from scripts import kupp_backtest as kb


# ---------------------------------------------------------------- pure helpers

def test_parse_trapp_og_min_rabatt():
    bands = kb.parse_trapp(kb.RABATT_TRAPP_DEFAULT)
    assert bands[0] == (50000.0, 30.0)
    assert bands[-1][0] == float("inf")
    assert kb.min_rabatt_for_pris(40_000, bands) == 30.0
    assert kb.min_rabatt_for_pris(120_000, bands) == 15.0
    assert kb.min_rabatt_for_pris(300_000, bands) == 6.0


def test_norm_drivstoff():
    assert kb.norm_drivstoff("El") == "elektrisk"
    assert kb.norm_drivstoff("Elektrisk") == "elektrisk"
    assert kb.norm_drivstoff("Ladbar hybrid") == "plug-in hybrid"
    assert kb.norm_drivstoff("Bensin") == "bensin"


def test_er_privat_mask():
    df = pd.DataFrame({"Selger": ["", "  ", None, "Bilhuset AS"]})
    assert list(kb.er_privat_mask(df)) == [True, True, True, False]
    # Uten Selger-kolonne: alt privat (fail-open)
    assert kb.er_privat_mask(pd.DataFrame({"x": [1, 2]})).all()


def test_flagg_kupp_terskel_og_selger():
    df = pd.DataFrame({
        "salgspris": [200_000, 200_000, 300_000, 40_000, 200_000],
        "forventet_pris": [300_000, 210_000, 305_000, 300_000, 300_000],
        "Selger": ["", "", "", "", "Forhandler AS"],
    })
    df = kb.beregn_rabatt(df)
    bands = kb.parse_trapp(kb.RABATT_TRAPP_DEFAULT)
    er = kb.flagg_kupp(df, bands, kun_privat=True, maks_rabatt_pct=70.0)
    # rad0: 33% rabatt, <250k krever 7% -> kupp
    assert er.iloc[0]
    # rad1: ~4.8% rabatt, krever 7% -> ikke kupp
    assert not er.iloc[1]
    # rad3: forventet 300k vs pris 40k = 86.7% -> over maks 70% -> droppes
    assert not er.iloc[3]
    # rad4: nok rabatt men forhandler -> ikke kupp naar kun_privat
    assert not er.iloc[4]
    # Med alle selgere teller rad4
    er_alle = kb.flagg_kupp(df, bands, kun_privat=False, maks_rabatt_pct=70.0)
    assert er_alle.iloc[4]


def test_solgt_innen():
    df = pd.DataFrame({
        "Solgt": ["JA", "FJERNET", "NEI", "JA"],
        "dager_til_salg": [1, 2, 1, 5],
    })
    s = kb.solgt_innen(df, dager=2, kun_ja=False)
    assert list(s) == [True, True, False, False]
    s_ja = kb.solgt_innen(df, dager=2, kun_ja=True)
    assert list(s_ja) == [True, False, False, False]


def test_standard_fra_dato():
    assert kb.standard_fra_dato(date(2026, 9, 1)) == date(2026, 8, 1)
    assert kb.standard_fra_dato(date(2026, 3, 1)) == date(2025, 8, 1)


def test_filtrer_fylke():
    df = pd.DataFrame({
        "fylke": ["Vestland", "Oslo", None, ""],
        "sted": ["Bergen, Vestland", "Oslo, Oslo", "Voss, Vestland", "Ukjent"],
    })
    m = kb.filtrer_fylke(df, "Vestland")
    # rad0 via fylke-kolonnen, rad2 via sted-fallback
    assert list(m) == [True, False, True, False]
    # Tom fylke = alt
    assert kb.filtrer_fylke(df, "").all()


def test_filtrer_drivstoff():
    df = pd.DataFrame({"drivstoff": ["Elektrisk", "El", "Bensin", "Diesel"]})
    assert list(kb.filtrer_drivstoff(df, "Elektrisk")) == [True, True, False, False]
    assert kb.filtrer_drivstoff(df, "").all()


def test_terskel_sweep():
    # 4 elbiler: to solgt raskt (rabatt 10% og 3%), to ikke solgt (rabatt 8%, 0%)
    seg = pd.DataFrame({
        "rabatt_pct": [10.0, 3.0, 8.0, 0.0],
        "solgt_innen": [True, True, False, False],
        "Selger": ["", "", "", ""],
    })
    df, meta = kb.terskel_sweep(seg, [5.0, 2.0], "solgt_innen",
                                kun_privat=True, maks_rabatt_pct=70.0)
    assert meta["n_segment"] == 4 and meta["n_raske"] == 2
    r5 = df[df["terskel_pct"] == 5.0].iloc[0]
    # terskel 5%: flagger rabatt>=5 -> {10%(solgt), 8%(ikke)} = 2 flagget, 1 solgt
    assert r5["n_flagget"] == 2 and r5["n_solgt_innen"] == 1
    assert r5["presisjon_pct"] == 50.0 and r5["recall_pct"] == 50.0
    r2 = df[df["terskel_pct"] == 2.0].iloc[0]
    # terskel 2%: flagger {10,3,8} = 3 flagget, 2 solgt (10% og 3%)
    assert r2["n_flagget"] == 3 and r2["n_solgt_innen"] == 2
    assert r2["recall_pct"] == 100.0


# ---------------------------------------------------------------- end-to-end

def _syntetisk_db(tmp_path) -> str:
    """Bygg en liten database_biler.parquet med én peer-gruppe: nok solgte
    treningsbiler + kandidater (kupp, ikke-kupp, bommet elbil)."""
    rng = np.random.default_rng(42)
    naa = pd.Timestamp.now().normalize()
    aarstall = naa.year - 3
    rader = []

    # 30 solgte treningsbiler (samme peer-gruppe), pris ~300k
    for i in range(30):
        pris = 300_000 * float(np.exp(rng.normal(0, 0.03)))
        rader.append({
            "FinnKode": 1000 + i, "Produsent": "TestMerke", "Modell": "TestModell",
            "Overskrift": "TestMerke TestModell", "drivstoff": "Elektrisk",
            "hjuldrift": "Tohjul", "årstall": aarstall, "kjørelengde": 40_000,
            "Pris": pris, "Pris_ny": pris, "Solgt": "JA",
            "Dato": naa - pd.Timedelta(days=40), "Dato_ny": naa - pd.Timedelta(days=30),
            "Selger": "", "fylke": "Vestland", "sted": "Bergen, Vestland",
            "url": f"https://finn.no/{1000+i}",
        })

    def kandidat(fk, pris, solgt, dager, selger="", driv="Elektrisk"):
        dato = naa - pd.Timedelta(days=20)
        return {
            "FinnKode": fk, "Produsent": "TestMerke", "Modell": "TestModell",
            "Overskrift": "TestMerke TestModell", "drivstoff": driv,
            "hjuldrift": "Tohjul", "årstall": aarstall, "kjørelengde": 40_000,
            "Pris": pris, "Pris_ny": pris, "Solgt": solgt,
            "Dato": dato, "Dato_ny": dato + pd.Timedelta(days=dager),
            "Selger": selger, "fylke": "Vestland", "sted": "Bergen, Vestland",
            "url": f"https://finn.no/{fk}",
        }

    # kupp (33% under modell) solgt paa 1 dag
    rader.append(kandidat(2001, 200_000, "JA", 1))
    # ikke-kupp (riktig pris) solgt paa 1 dag -> bommet elbil (ikke underpriset)
    rader.append(kandidat(2002, 300_000, "JA", 1))
    # naer-bom: 5% under, krever 6% (>=250k) -> ikke kupp, solgt raskt
    rader.append(kandidat(2003, 285_000, "JA", 1))
    # ikke-kupp, ikke solgt raskt (aktiv)
    rader.append(kandidat(2004, 305_000, "NEI", 30))
    # kupp men forhandler -> skal ikke telle som kupp (kun_privat)
    rader.append(kandidat(2005, 200_000, "JA", 1, selger="Bilhuset AS"))

    df = pd.DataFrame(rader)
    df["Dato"] = pd.to_datetime(df["Dato"])
    df["Dato_ny"] = pd.to_datetime(df["Dato_ny"])
    path = os.path.join(tmp_path, "syntetisk_db.parquet")
    df.to_parquet(path, index=False)
    return path


def test_ende_til_ende(tmp_path):
    path = _syntetisk_db(str(tmp_path))
    utdir = os.path.join(str(tmp_path), "ut")
    fra = (pd.Timestamp.now().normalize() - pd.Timedelta(days=25)).date()

    res = kb.kjor_backtest(
        input_path=path, fra=fra, dager=2, utdir=utdir,
        frys=False, kun_privat=True, bruk_overrides=False,
    )

    # Grunnleggende struktur
    assert set(res) >= {"kupp_rate", "ikke_rate", "ev_profil", "n_eligible"}
    # Minst ett kupp ble flagget (bil 2001) og det solgte innen 2 doegn
    assert res["kupp_rate"]["n"] >= 1
    assert res["kupp_rate"]["n_solgt"] >= 1
    # Bommede elbiler finnes (2002/2003 solgt raskt, ikke kupp)
    assert res["ev_profil"]["bommet"] >= 1
    # Filer skrevet
    for navn in ("kupp_flagget.csv", "kupp_moenster.csv", "bommet_elbiler.csv"):
        assert os.path.exists(os.path.join(utdir, navn))

    # Forhandler-kuppet (2005) skal IKKE vaere blant flaggede kupp
    kupp = pd.read_csv(os.path.join(utdir, "kupp_flagget.csv"), sep=";")
    assert 2005 not in set(kupp["FinnKode"].astype(int))
    assert 2001 in set(kupp["FinnKode"].astype(int))


def test_ende_til_ende_fylke_og_sweep(tmp_path):
    path = _syntetisk_db(str(tmp_path))
    utdir = os.path.join(str(tmp_path), "ut2")
    fra = (pd.Timestamp.now().normalize() - pd.Timedelta(days=25)).date()

    res = kb.kjor_backtest(
        input_path=path, fra=fra, dager=2, utdir=utdir,
        frys=False, kun_privat=True, bruk_overrides=False,
        fylke="Vestland", min_pris=50_000, drivstoff="Elektrisk",
        sweep=True, terskler=[0, 5, 10, 20],
    )
    # Sweep-resultat finnes og har en rad per terskel
    assert res["sweep"] is not None and len(res["sweep"]) == 4
    assert os.path.exists(os.path.join(utdir, "terskel_sweep.csv"))
    # Alle syntetiske biler er i Vestland og >= 50k -> segmentet er ikke tomt
    assert res["n_eligible"] >= 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
