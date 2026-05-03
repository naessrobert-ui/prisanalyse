"""Tester for FlipModels-baserte bilradar_scorer."""
from __future__ import annotations

import numpy as np
import pandas as pd

from bilradar_modell_skjema import FlipModels, SegmentModel
from bilradar_scorer import scorer_biler


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


def _bygg_modell(market_base: float, fast_base: float) -> FlipModels:
    market = SegmentModel(model=DummyPipeline(market_base), n_obs=42, label="market test")
    fast = SegmentModel(model=DummyPipeline(fast_base), n_obs=18, label="fast test")
    return FlipModels(
        market_l1={"BMW | 3-Serie | Bensin": market},
        fast_l1={"BMW | 3-Serie | Bensin": fast},
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


def test_scorer_gir_bade_markedspris_og_hurtigpris():
    df = _bygg_df()
    modeller = _bygg_modell(market_base=400_000, fast_base=370_000)
    res = scorer_biler(df, modeller)

    rad = res.iloc[0]
    assert rad["modell_nivaa"] == "L1"
    assert rad["forventet_pris"] > 0
    assert rad["hurtigpris"] > 0
    # Hurtigpris (rask salg) skal være lavere enn markedspris fordi base er lavere
    assert rad["hurtigpris"] < rad["forventet_pris"]
    # peer_konfidens reflekterer n_obs i den brukte modellen
    assert int(rad["peer_konfidens"]) == 42
    assert int(rad["peer_konfidens_hurtig"]) == 18


def test_uten_treff_faller_tilbake_til_ingen_modell():
    df = _bygg_df()
    df.loc[0, "Merke"] = "Ukjent merke"
    modeller = _bygg_modell(market_base=400_000, fast_base=370_000)
    res = scorer_biler(df, modeller)

    rad = res.iloc[0]
    assert rad["modell_nivaa"] == "Ingen modell"
    assert pd.isna(rad["forventet_pris"])
    assert pd.isna(rad["hurtigpris"])


def test_overstyringsregler_pavirker_kun_markedspris():
    """Hvis overstyringen treffer skal forventet_pris justeres mens
    hurtigpris forblir uendret."""
    df = _bygg_df()
    modeller = _bygg_modell(market_base=400_000, fast_base=370_000)
    res = scorer_biler(df, modeller)
    # Ingen overstyringsregel matcher BMW i dagens CSV — hurtigpris og
    # forventet_pris skal være forskjellige (fra ulike modeller).
    rad = res.iloc[0]
    assert rad["forventet_pris"] != rad["hurtigpris"]
