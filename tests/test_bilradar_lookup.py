import numpy as np
import pandas as pd

from bilradar_lookup import apply_lookup


def _basis_df():
    return pd.DataFrame([
        {
            "FinnKode": 1,
            "Produsent": "Tesla",
            "Modell": "Model Y",
            "drivstoff": "Elektrisk",
            "hjuldrift": "Firehjul",
            "årstall": 2022,
            "kjørelengde": 50_000,
            "salgspris": 380_000,
            "forventet_pris": 410_000.0,  # ML-fallback
            "peer_konfidens": 200,
            "modell_nivaa": "L1",
        },
        {
            "FinnKode": 2,
            "Produsent": "Ukjent-Merke",
            "Modell": "Sjelden",
            "drivstoff": "Elektrisk",
            "hjuldrift": "Tohjul",
            "årstall": 2022,
            "kjørelengde": 20_000,
            "salgspris": 200_000,
            "forventet_pris": 250_000.0,  # ML-fallback
            "peer_konfidens": 50,
            "modell_nivaa": "GEN",
        },
    ])


def _lookup():
    return pd.DataFrame([{
        "Produsent": "Tesla",
        "Modell": "Model Y",
        "hjuldrift": "Firehjul",
        "drivstoff": "Elektrisk",
        "årstall": 2022,
        "n_obs": 1382,
        "median_pris": 350_000.0,
        "median_km": 60_000.0,
        "km_slope": -0.7,
    }])


def test_lookup_overskriver_ml_for_match():
    df = _basis_df()
    res = apply_lookup(df, _lookup())

    # Bil 1 (Tesla Model Y) skal få lookup-pris med km-justering:
    # 350000 + (-0.7) * (50000 - 60000) = 350000 + 7000 = 357000
    assert res.loc[0, "forventet_pris"] == 357_000
    assert res.loc[0, "modell_nivaa"] == "LOOKUP"
    assert res.loc[0, "peer_konfidens"] == 1382


def test_lookup_lar_ikke_matchende_biler_uendret():
    df = _basis_df()
    res = apply_lookup(df, _lookup())

    # Bil 2 har ingen lookup-treff -> beholder ML-pris
    assert res.loc[1, "forventet_pris"] == 250_000.0
    assert res.loc[1, "modell_nivaa"] == "GEN"
    assert res.loc[1, "peer_konfidens"] == 50


def test_tom_lookup_endrer_ingenting():
    df = _basis_df()
    res = apply_lookup(df, pd.DataFrame())
    assert (res["forventet_pris"] == df["forventet_pris"]).all()
    assert (res["modell_nivaa"] == df["modell_nivaa"]).all()


def test_kmjustering_med_negativ_slope_oker_pris_for_lavere_km():
    df = _basis_df()
    df.loc[0, "kjørelengde"] = 30_000  # mindre enn median 60k
    res = apply_lookup(df, _lookup())
    # 350000 + (-0.7) * (30000 - 60000) = 350000 + 21000 = 371000
    assert res.loc[0, "forventet_pris"] == 371_000


def test_lookup_clip_til_minst_1000():
    df = _basis_df()
    df.loc[0, "kjørelengde"] = 10_000_000  # urealistisk hoy km
    res = apply_lookup(df, _lookup())
    # Skal ikke gaa under 1000 NOK uansett
    assert res.loc[0, "forventet_pris"] >= 1_000.0
