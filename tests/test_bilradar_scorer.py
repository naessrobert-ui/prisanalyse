"""Tester for FlipModels-baserte bilradar_scorer.

Modellen predikerer nå kun markedspris (forventet_pris). Hurtigpris og
innbyttepris kommer fra lookup-tabellen — den separate hurtig-ML-modellen er
fjernet. I disse testene finnes BMW 3-Serie ikke i lookup-tabellen, så
hurtigpris blir NaN og innbyttepris utledes som 15 % under forventet_pris.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from bilradar_modell_skjema import FlipModels, SegmentModel
from bilradar_scorer import INNBYTTE_RABATT, scorer_biler


class DummyPipeline:
    """Etterligner sklearn Pipeline.predict — returnerer log1p av en
    enkel regelbasert pris så vi får forutsigbare verdier i testen."""

    def __init__(self, base: float, alder_drag: float = 5_000.0, km_drag: float = 0.5):
        self.base = base
        self.alder_drag = alder_drag
        self.km_drag = km_drag

    def predict(self, X: pd.DataFrame):
        alder = X["alder"].astype(float).to_numpy()
        km = X["kjørelengde"].astype(float).to_numpy()
        pris = self.base - alder * self.alder_drag - km * self.km_drag
        pris = np.maximum(pris, 1_000.0)
        return np.log1p(pris)


def _bygg_modell(market_base: float) -> FlipModels:
    market = SegmentModel(model=DummyPipeline(market_base), n_obs=42, label="market test")
    return FlipModels(
        market_l1={"BMW | 3-Serie | Bensin": market},
        trained_at="2026-05-03 12:00:00",
    )


def _bygg_df():
    return pd.DataFrame([
        {
            "FinnKode": 1,
            "Merke": "BMW",
            "Modell": "3-Serie",
            "Drivstoff": "Bensin",
            "Kjørelengde": 100_000,
            "Årstall": 2018,
            "Pris": 200_000,
            "Hjuldrift": "Tohjulstrekk",
            "Girkasse": "Automat",
            "Karosseri": "Sedan",
            "Forhandler": "Forhandler",
            "Fylke": "Oslo",
        }
    ])


def test_scorer_gir_markedspris_fra_ml():
    df = _bygg_df()
    modeller = _bygg_modell(market_base=400_000)
    res = scorer_biler(df, modeller)

    rad = res.iloc[0]
    assert rad["modell_nivaa"] == "L1"
    assert rad["forventet_pris"] > 0
    # peer_konfidens reflekterer n_obs i den brukte modellen
    assert int(rad["peer_konfidens"]) == 42


def test_hurtigpris_er_nan_uten_lookup_treff():
    """BMW 3-Serie finnes ikke i lookup-tabellen -> ingen hurtigpris."""
    df = _bygg_df()
    modeller = _bygg_modell(market_base=400_000)
    res = scorer_biler(df, modeller)
    assert pd.isna(res.iloc[0]["hurtigpris"])


def test_innbyttepris_utledes_fra_forventet_nar_hurtig_mangler():
    df = _bygg_df()
    modeller = _bygg_modell(market_base=400_000)
    res = scorer_biler(df, modeller)
    rad = res.iloc[0]
    # Uten hurtigpris skal innbytte = forventet * (1 - rabatt)
    assert np.isclose(rad["innbyttepris"], rad["forventet_pris"] * (1 - INNBYTTE_RABATT))


def test_uten_treff_faller_tilbake_til_ingen_modell():
    df = _bygg_df()
    df.loc[0, "Merke"] = "Ukjent merke"
    modeller = _bygg_modell(market_base=400_000)
    res = scorer_biler(df, modeller)

    rad = res.iloc[0]
    assert rad["modell_nivaa"] == "Ingen modell"
    assert pd.isna(rad["forventet_pris"])
    assert pd.isna(rad["hurtigpris"])
    assert pd.isna(rad["innbyttepris"])


def test_scorer_uten_ml_modell_krasjer_ikke():
    """modeller=None: /finn-sok-stien. Uten lookup/peer-treff blir forventet_pris
    NaN, men scoringen skal ikke kaste (ingen tung ML-modell lastes)."""
    df = _bygg_df()  # BMW 3-Serie finnes ikke i lookup/peer i testmiljøet
    res = scorer_biler(df, modeller=None)
    rad = res.iloc[0]
    assert "forventet_pris" in res.columns
    assert pd.isna(rad["forventet_pris"])
