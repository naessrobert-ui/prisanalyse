"""
scripts/sammenlign_priser.py
============================
Sammenligner ML-modellens pris mot lookup-tabellens pris for filtrerte
biler — typisk for aa se hvilken metode treffer best paa et utvalg.

Eksempler:
    # Aktive elbiler paa Vestlandet (Vestland, Rogaland, More og Romsdal)
    python -m scripts.sammenlign_priser \\
        --input C:\\Users\\Rober\\AppData\\Local\\Temp\\database_biler.parquet \\
        --lookup data/prislookup.csv \\
        --model bil_prismodell.joblib \\
        --drivstoff Elektrisk \\
        --fylke Vestland Rogaland "More og Romsdal" \\
        --output sammenligning_vestlandet_el.csv
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bilradar_scorer import last_modell_lokal_eller_s3, scorer_biler  # noqa: E402


def les_aktive_biler(parquet_path: str) -> pd.DataFrame:
    """Leser parquet og returnerer kun radene som er aktive i siste import."""
    df = pd.read_parquet(parquet_path)
    if "Dato_ny" in df.columns:
        df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")
        siste = df["Dato_ny"].max()
        df = df[df["Dato_ny"] == siste].copy()
    return df


def filtrer(df: pd.DataFrame, fylker: list[str] | None,
            drivstoff: str | None, merke: str | None,
            modell: str | None) -> pd.DataFrame:
    if fylker:
        df = df[df["fylke"].isin(fylker)].copy()
    if drivstoff:
        df = df[df["drivstoff"].fillna("").str.lower() == drivstoff.lower()].copy()
    if merke:
        df = df[df["Produsent"].fillna("").str.lower() == merke.lower()].copy()
    if modell:
        df = df[df["Modell"].fillna("").str.lower().str.contains(modell.lower())].copy()
    return df


def scor_med_lookup(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Bruker lookup-tabellen til aa gi en pris per bil. Slaar opp paa
    (Produsent, Modell, hjuldrift, drivstoff, aarstall) og justerer for
    kjorelengde med per-gruppe slope."""
    df = df.copy()
    keys = ["Produsent", "Modell", "hjuldrift", "drivstoff", "årstall"]

    # Sikre samme dtype/whitespace paa beggee sider
    for c in ["Produsent", "Modell", "hjuldrift", "drivstoff"]:
        df[c] = df[c].fillna("Ukjent").astype(str).str.strip()
        if c in lookup.columns:
            lookup[c] = lookup[c].fillna("Ukjent").astype(str).str.strip()
    df["årstall"] = pd.to_numeric(df["årstall"], errors="coerce").astype("Int64")
    lookup["årstall"] = pd.to_numeric(lookup["årstall"], errors="coerce").astype("Int64")

    merged = df.merge(
        lookup[keys + ["n_obs", "median_pris", "median_km", "km_slope", "km_slope_kilde"]],
        on=keys, how="left", suffixes=("", "_lookup"),
    )

    km = pd.to_numeric(merged.get("kjørelengde", 0), errors="coerce").fillna(0)
    merged["lookup_pris"] = (
        merged["median_pris"]
        + merged["km_slope"] * (km - merged["median_km"])
    ).round()
    merged["lookup_konfidens"] = merged["n_obs"]
    merged["lookup_kilde"] = merged["km_slope_kilde"]
    return merged


def main():
    parser = argparse.ArgumentParser(description="Sammenlign ML-pris og lookup-pris.")
    parser.add_argument("--input", required=True, help="Parquet med biler (database_biler.parquet)")
    parser.add_argument("--lookup", required=True, help="Lookup-CSV fra lag_prislookup.py")
    parser.add_argument("--model", required=True, help="ML-modell (bil_prismodell.joblib)")
    parser.add_argument("--fylke", nargs="*", default=[], help="Filtrer paa fylkenavn")
    parser.add_argument("--drivstoff", default="", help="F.eks. Elektrisk, Bensin")
    parser.add_argument("--merke", default="", help="Filtrer paa merke")
    parser.add_argument("--modell", default="", help="Filtrer paa modell-substring")
    parser.add_argument("--output", default="sammenligning.csv")
    args = parser.parse_args()

    print(f"[LES] {args.input}")
    df = les_aktive_biler(args.input)
    print(f"[DATA] Aktive biler: {len(df):,}")

    df = filtrer(df, args.fylke or None, args.drivstoff or None,
                 args.merke or None, args.modell or None)
    print(f"[FILT] Etter filter: {len(df):,}")
    if df.empty:
        print("Ingen biler etter filter.")
        return

    print(f"[ML]  Laster modell {args.model}")
    modeller = last_modell_lokal_eller_s3(args.model)
    # scorer_biler ser etter "Merke" og setter Produsent="Ukjent" hvis den
    # mangler — speil Produsent dit slik at scoringen treffer riktig segment.
    df_for_ml = df.copy()
    if "Merke" not in df_for_ml.columns and "Produsent" in df_for_ml.columns:
        df_for_ml["Merke"] = df_for_ml["Produsent"]
    ml_scoret = scorer_biler(df_for_ml, modeller)

    print(f"[LU]  Leser lookup {args.lookup}")
    lookup = pd.read_csv(args.lookup)

    keys = ["FinnKode", "Produsent", "Modell", "hjuldrift", "drivstoff", "årstall", "kjørelengde"]
    keys = [k for k in keys if k in ml_scoret.columns]
    base = ml_scoret[keys + ["forventet_pris", "salgspris", "fylke",
                              "modell_nivaa", "peer_konfidens", "url"]
                     if "url" in ml_scoret.columns
                     else keys + ["forventet_pris", "salgspris", "fylke",
                                   "modell_nivaa", "peer_konfidens"]].copy()

    lu_scoret = scor_med_lookup(df, lookup)
    base = base.merge(
        lu_scoret[keys + ["lookup_pris", "lookup_konfidens", "lookup_kilde"]],
        on=keys, how="left",
    )

    base = base.rename(columns={
        "forventet_pris": "ml_pris",
        "modell_nivaa": "ml_nivaa",
        "peer_konfidens": "ml_konfidens",
    })

    base["ml_avvik_pct"] = ((base["ml_pris"] - base["salgspris"]) / base["salgspris"] * 100).round(1)
    base["lookup_avvik_pct"] = ((base["lookup_pris"] - base["salgspris"]) / base["salgspris"] * 100).round(1)
    base["ml_vs_lookup_pct"] = ((base["ml_pris"] - base["lookup_pris"]) / base["lookup_pris"] * 100).round(1)

    cols_order = [
        "FinnKode", "Produsent", "Modell", "årstall", "kjørelengde", "drivstoff",
        "hjuldrift", "fylke", "salgspris", "ml_pris", "lookup_pris",
        "ml_avvik_pct", "lookup_avvik_pct", "ml_vs_lookup_pct",
        "ml_nivaa", "ml_konfidens", "lookup_konfidens", "lookup_kilde",
    ]
    if "url" in base.columns:
        cols_order.append("url")
    base = base[[c for c in cols_order if c in base.columns]]

    base.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[SAVE] Skrev {len(base):,} biler til {args.output}")
    print()

    # Oppsummering
    valid = base.dropna(subset=["ml_pris", "lookup_pris", "salgspris"])
    if not valid.empty:
        ml_mae = (valid["ml_pris"] - valid["salgspris"]).abs().median()
        lu_mae = (valid["lookup_pris"] - valid["salgspris"]).abs().median()
        ml_med_avvik = valid["ml_avvik_pct"].median()
        lu_med_avvik = valid["lookup_avvik_pct"].median()
        print(f"Median absolutt avvik fra annonsepris (n={len(valid):,}):")
        print(f"  ML:     {ml_mae:>10,.0f} NOK   median bias: {ml_med_avvik:+.1f}%")
        print(f"  Lookup: {lu_mae:>10,.0f} NOK   median bias: {lu_med_avvik:+.1f}%")
        print()
        print("Topp 10 hvor ML og lookup spriker mest (|ml_vs_lookup_pct|):")
        spread = valid.copy()
        spread["abs_diff"] = spread["ml_vs_lookup_pct"].abs()
        topp = spread.nlargest(10, "abs_diff")[
            ["Produsent", "Modell", "årstall", "kjørelengde",
             "salgspris", "ml_pris", "lookup_pris", "ml_vs_lookup_pct"]
        ]
        print(topp.to_string(index=False))


if __name__ == "__main__":
    main()
