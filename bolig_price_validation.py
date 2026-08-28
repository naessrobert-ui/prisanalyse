"""Felles rimelighetskontroller for observerte boligpriser."""

import pandas as pd


MIN_BOLIG_PRICE = 100_000
MAX_BOLIG_PRICE = 200_000_000
MIN_PRICE_CHANGE_RATIO = 0.40
MAX_PRICE_CHANGE_RATIO = 2.00


def plausible_price(values: pd.Series) -> pd.Series:
    """True for numeriske boligpriser innenfor et bredt, realistisk intervall."""
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.between(MIN_BOLIG_PRICE, MAX_BOLIG_PRICE, inclusive="both")


def plausible_price_change(first: pd.Series, new: pd.Series) -> pd.Series:
    """True når begge priser og forholdet mellom dem er rimelige."""
    first_numeric = pd.to_numeric(first, errors="coerce")
    new_numeric = pd.to_numeric(new, errors="coerce")
    ratio = new_numeric / first_numeric
    return (
        plausible_price(first_numeric)
        & plausible_price(new_numeric)
        & ratio.between(
            MIN_PRICE_CHANGE_RATIO,
            MAX_PRICE_CHANGE_RATIO,
            inclusive="both",
        )
    )
