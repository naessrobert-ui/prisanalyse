"""
bilradar_modell_skjema.py – Felles skjema for trening og scoring
=================================================================
Definerer feature-lister og dataclasses som brukes av både treningsskriptet
(scripts/tren_prismodell.py) og live-scoreren (bilradar_scorer.py).
Holdes minimal og uten ML-avhengigheter slik at scoring-koden kan importere
dette uten å dra inn sklearn ved oppstart.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Numeriske features brukt i begge segment-modeller (L1, L2, GEN)
SEG_FEATURES_NUM = [
    "alder",
    "kjørelengde",
    "km_per_aar",
    "garanti_mnd",
    "rekkevidde_km",
]

# Kategoriske features for L1/L2 (segment-spesifikke modeller)
SEG_FEATURES_CAT = [
    "hjuldrift",
    "girkasse",
    "Karosseri",
]

# Kategoriske features for generell fallback-modell
# Inkluderer Produsent/Modell/drivstoff fordi den generelle modellen ikke er
# splittet per merke.
GEN_FEATURES_CAT = [
    "Produsent",
    "drivstoff",
    "hjuldrift",
    "girkasse",
    "Karosseri",
    "forhandler_type",
    "fylke",
]


@dataclass
class SegmentModel:
    """En trent prismodell for ett segment (L1, L2 eller generell)."""
    model: Any           # sklearn Pipeline (ColumnTransformer + HGB)
    n_obs: int           # antall salg som ble brukt til trening
    label: str           # menneskelesbar etikett
    mae_cv: float | None = None


@dataclass
class FlipModels:
    """Pakken som lagres til disk og lastes ved scoring.

    Modellen predikerer kun markedspris. Hurtigpris kommer nå fra den
    transparente lookup-tabellen (median blant biler solgt raskt), så den
    tidligere separate hurtig-ML-modellen (fast_l1/l2/general) er fjernet.
    Gamle pickles med fast_*-felter lastes fortsatt — ekstra attributter
    ignoreres av scoreren."""
    market_l1: dict[str, SegmentModel] = field(default_factory=dict)
    market_l2: dict[str, SegmentModel] = field(default_factory=dict)
    market_general: SegmentModel | None = None

    trained_at: str = ""
    n_market_train: int = 0
