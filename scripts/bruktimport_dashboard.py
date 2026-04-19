"""Streamlit-dashboard for å finne nylig bruktimporterte biler.

Kjør:
    streamlit run scripts/bruktimport_dashboard.py -- --parquet database_biler.parquet
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class ColumnConfig:
    bruktimport_col: str
    reg_date_col: str
    make_col: str
    model_col: str


def pick_first_existing(columns: Iterable[str], candidates: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]
    return None


def resolve_columns(df: pd.DataFrame) -> ColumnConfig:
    bruktimport_col = pick_first_existing(
        df.columns,
        [
            "bruktimportert",
            "bruktimport",
            "is_bruktimport",
            "er_bruktimportert",
            "import_flag",
            "importert",
        ],
    )
    reg_date_col = pick_first_existing(
        df.columns,
        [
            "forstegangsregistrert_norge",
            "forstegangsregistrering_norge",
            "forste_gang_registrert_norge",
            "forste_gang_reg_norge",
            "reg_dato_norge",
            "første_gang_reg_norge",
            "first_registered_norway",
            "reg_norge",
        ],
    )
    make_col = pick_first_existing(df.columns, ["merke", "make", "brand"])
    model_col = pick_first_existing(df.columns, ["modell", "model"])

    missing = [
        name
        for name, value in {
            "bruktimport-kolonne": bruktimport_col,
            "dato for førstegangsregistrering i Norge": reg_date_col,
            "merke": make_col,
            "modell": model_col,
        }.items()
        if value is None
    ]
    if missing:
        raise ValueError(
            "Mangler nødvendige kolonner: "
            + ", ".join(missing)
            + "\nFant kolonner: "
            + ", ".join(df.columns)
        )

    return ColumnConfig(
        bruktimport_col=bruktimport_col,
        reg_date_col=reg_date_col,
        make_col=make_col,
        model_col=model_col,
    )


def to_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    truthy = {"1", "true", "ja", "yes", "y", "t", "bruktimport", "import"}
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(truthy)
        .fillna(False)
    )


def build_filtered_dataset(df: pd.DataFrame, config: ColumnConfig, months: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    working = df.copy()
    working[config.reg_date_col] = pd.to_datetime(working[config.reg_date_col], errors="coerce")
    working["_is_bruktimport"] = to_bool(working[config.bruktimport_col])

    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(months=months)

    mask = working["_is_bruktimport"] & working[config.reg_date_col].ge(cutoff)
    filtered = working.loc[mask].copy()

    grouped = (
        filtered.groupby([config.make_col, config.model_col], dropna=False)
        .size()
        .reset_index(name="antall")
        .sort_values("antall", ascending=False)
    )

    return filtered, grouped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--parquet", type=Path, default=Path("database_biler.parquet"))
    parser.add_argument("--months", type=int, default=3)
    known, _ = parser.parse_known_args()
    return known


def main() -> None:
    args = parse_args()

    st.set_page_config(page_title="Bruktimport-analyse", layout="wide")
    st.title("Nylig bruktimporterte biler")
    st.caption(
        "Viser biler som er bruktimportert og førstegangsregistrert i Norge nyere enn valgt tidsvindu."
    )

    parquet_path = st.sidebar.text_input("Parquet-fil", str(args.parquet))
    months = st.sidebar.slider("Nyere enn (måneder)", min_value=1, max_value=24, value=args.months)

    if not Path(parquet_path).exists():
        st.error(f"Fant ikke filen: {parquet_path}")
        st.stop()

    df = pd.read_parquet(parquet_path)
    config = resolve_columns(df)
    filtered, grouped = build_filtered_dataset(df, config, months)

    total = len(df)
    hits = len(filtered)
    share = (hits / total * 100) if total else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Totalt antall biler", f"{total:,}".replace(",", " "))
    c2.metric("Nylig bruktimporterte", f"{hits:,}".replace(",", " "))
    c3.metric("Andel", f"{share:.1f}%")

    st.subheader("Fordeling på merke og modell")
    st.dataframe(grouped, use_container_width=True, hide_index=True)

    st.subheader("Detaljer per merke/modell")
    options = grouped.apply(lambda r: f"{r[config.make_col]} | {r[config.model_col]} ({r['antall']})", axis=1).tolist()

    if not options:
        st.info("Ingen biler matchet filtrene.")
        st.stop()

    selected = st.selectbox("Velg merke/modell", options=options)
    selected_make, rest = selected.split(" | ", maxsplit=1)
    selected_model = rest.rsplit(" (", maxsplit=1)[0]

    details = filtered[
        (filtered[config.make_col].astype(str) == selected_make)
        & (filtered[config.model_col].astype(str) == selected_model)
    ].sort_values(config.reg_date_col, ascending=False)

    preferred_cols = [
        config.make_col,
        config.model_col,
        config.reg_date_col,
        config.bruktimport_col,
        "pris",
        "km",
        "aar",
        "finnkode",
        "url",
    ]
    display_cols = [c for c in preferred_cols if c in details.columns]
    fallback_cols = [c for c in details.columns if c not in display_cols][:10]

    st.dataframe(
        details[display_cols + fallback_cols],
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
