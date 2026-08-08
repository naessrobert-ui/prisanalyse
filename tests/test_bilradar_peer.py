import numpy as np
import pandas as pd

from bilradar_peer import apply_peer, km_normalisert


def _koef():
    # tier 1: Nissan Leaf Elektrisk Tohjul; tier 2: Nissan Leaf Elektrisk (uansett hjuldrift)
    return pd.DataFrame([
        {"prod_key": "nissan", "modell_key": "leaf", "driv_key": "elektrisk",
         "hjul_key": "tohjul", "tier": 1, "n_obs": 40,
         "beta0": 13.0, "beta1": -0.05, "beta2": -0.2},
        {"prod_key": "nissan", "modell_key": "leaf", "driv_key": "elektrisk",
         "hjul_key": "", "tier": 2, "n_obs": 120,
         "beta0": 12.9, "beta1": -0.05, "beta2": -0.2},
    ])


def _pris(beta0, beta1, beta2, alder, km):
    km_norm = km_normalisert([alder], [km])[0]
    return float(np.exp(beta0 + beta1 * alder + beta2 * (km_norm / 100_000.0)))


def test_apply_peer_tier1_treff():
    df = pd.DataFrame([{
        "FinnKode": 1, "Produsent": "Nissan", "Modell": "Leaf",
        "drivstoff": "Elektrisk", "hjuldrift": "Forhjulsdrift",  # -> Tohjul
        "alder": 3, "kjørelengde": 40_000,
    }])
    res = apply_peer(df, koef=_koef())
    assert res.loc[0, "modell_nivaa"] == "PEER-T1"
    assert np.isclose(res.loc[0, "forventet_pris"], _pris(13.0, -0.05, -0.2, 3, 40_000))


def test_apply_peer_faller_til_tier2_uten_hjuldrift_treff():
    df = pd.DataFrame([{
        "FinnKode": 2, "Produsent": "Nissan", "Modell": "Leaf",
        "drivstoff": "Elektrisk", "hjuldrift": "Firehjul",  # ingen tier1 for firehjul
        "alder": 5, "kjørelengde": 80_000,
    }])
    res = apply_peer(df, koef=_koef())
    assert res.loc[0, "modell_nivaa"] == "PEER-T2"
    assert np.isclose(res.loc[0, "forventet_pris"], _pris(12.9, -0.05, -0.2, 5, 80_000))


def test_apply_peer_rorer_ikke_biler_med_pris():
    df = pd.DataFrame([{
        "FinnKode": 3, "Produsent": "Nissan", "Modell": "Leaf",
        "drivstoff": "Elektrisk", "hjuldrift": "Tohjul",
        "alder": 3, "kjørelengde": 40_000,
        "forventet_pris": 999_000.0, "modell_nivaa": "LOOKUP",
    }])
    res = apply_peer(df, koef=_koef())
    assert res.loc[0, "forventet_pris"] == 999_000.0
    assert res.loc[0, "modell_nivaa"] == "LOOKUP"


def test_apply_peer_uten_treff_gir_ingen_pris():
    df = pd.DataFrame([{
        "FinnKode": 4, "Produsent": "Ukjent", "Modell": "Sjelden",
        "drivstoff": "Bensin", "hjuldrift": "Tohjul",
        "alder": 4, "kjørelengde": 60_000,
    }])
    res = apply_peer(df, koef=_koef())
    assert pd.isna(res.loc[0, "forventet_pris"])


def test_apply_peer_tom_tabell_er_noop():
    df = pd.DataFrame([{
        "FinnKode": 5, "Produsent": "Nissan", "Modell": "Leaf",
        "drivstoff": "Elektrisk", "hjuldrift": "Tohjul",
        "alder": 3, "kjørelengde": 40_000,
    }])
    res = apply_peer(df, koef=pd.DataFrame())
    assert "forventet_pris" not in res.columns or pd.isna(res.loc[0, "forventet_pris"])
