"""
scripts/tren_prismodell.py
==========================
Trener Bilradar-prismodellen (FlipModels: markedspris + hurtigpris) og
laster opp pkl-fila til S3.

Kjør lokalt:
    python -m scripts.tren_prismodell --input database_biler.parquet --output bil_prismodell.pkl

Kjør mot S3 (anbefalt for ukentlig retrening):
    python -m scripts.tren_prismodell --s3 --upload

Modellen er deterministisk (random_state=42 i HGB) — samme input gir
samme prediksjon. Trening og scoring deler feature-skjema fra
bilradar_modell_skjema.py.
"""
from __future__ import annotations

import argparse
import io
import os
import pickle
import sys
import tempfile
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# Sørg for at vi kan importere skjema fra repo-rot uavhengig av hvor
# skriptet kjøres fra.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Last .env hvis tilgjengelig — gjør AWS-credentials tilgjengelig for
# config.py når skriptet kjøres direkte fra terminal. find_dotenv leter
# oppover slik at en worktree-undermappe også finner repo-rotens .env.
try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass  # python-dotenv ikke installert; faller tilbake til shell-env

from bilradar_modell_skjema import (  # noqa: E402
    FlipModels,
    GEN_FEATURES_CAT,
    SEG_FEATURES_CAT,
    SEG_FEATURES_NUM,
    SegmentModel,
)

# ---- Konfigurasjon ----
S3_INPUT_KEY = "calc/bil/database_biler.parquet"
S3_OUTPUT_KEY = "calc/bil/bil_prismodell.pkl"

MIN_OBS_L1 = 30   # Produsent | Modell | drivstoff
MIN_OBS_L2 = 60   # Produsent | drivstoff
MIN_OBS_GENERAL = 800

HURTIG_DAGER = 3
MAX_DAGER_FOR_MARKED_TRENING = 120
MIN_OBS_L1_FAST = 15
MIN_OBS_L2_FAST = 30


# ---- Datalast / forprosessering ----

def _to_datetime(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], format="%d.%m.%Y %H:%M", errors="coerce")


def _to_numeric(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


def _parse_rekkevidde(val) -> float:
    if pd.isna(val):
        return float("nan")
    s = str(val).lower().replace(",", ".")
    digits = "".join(ch if (ch.isdigit() or ch == ".") else " " for ch in s).split()
    if not digits:
        return float("nan")
    try:
        return float(digits[0])
    except Exception:
        return float("nan")


def load_and_prepare(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)

    for c in ["forventet_pris", "rabatt_pct"]:
        if c in df.columns:
            df = df.drop(columns=[c])

    _to_datetime(df, "Dato")
    _to_datetime(df, "Dato_ny")
    for c in ["Pris", "Pris_ny", "kjørelengde", "årstall", "garanti_mnd"]:
        _to_numeric(df, c)

    df["salgspris"] = df["Pris_ny"].fillna(df["Pris"])
    df["alder"] = (datetime.now().year - df["årstall"]).clip(lower=0)
    df["dager_paa_markedet"] = (df["Dato_ny"] - df["Dato"]).dt.days

    cat_cols = [
        "Produsent", "Modell", "drivstoff", "hjuldrift",
        "girkasse", "Karosseri", "forhandler_type", "fylke",
    ]
    for c in cat_cols:
        if c not in df.columns:
            df[c] = "Ukjent"
        df[c] = df[c].fillna("Ukjent").astype(str).str.strip()

    if "rekkevidde_str" in df.columns:
        df["rekkevidde_km"] = df["rekkevidde_str"].apply(_parse_rekkevidde)
    elif "rekkevidde_km" not in df.columns:
        df["rekkevidde_km"] = np.nan

    df["segment_1"] = df["Produsent"] + " | " + df["Modell"] + " | " + df["drivstoff"]
    df["segment_2"] = df["Produsent"] + " | " + df["drivstoff"]

    siste = df["Dato_ny"].max()
    df["er_aktiv"] = df["Dato_ny"] == siste
    df["er_solgt"] = df["Dato_ny"] < siste

    df = df.dropna(subset=["salgspris", "kjørelengde", "årstall", "Dato", "Dato_ny"])
    df = df[(df["salgspris"] > 0) & (df["kjørelengde"] >= 0)]
    df = df[(df["alder"] >= 0) & (df["alder"] <= 60)]
    df = df[df["dager_paa_markedet"] >= 0]

    df["km_per_aar"] = df["kjørelengde"] / (df["alder"] + 1)
    df["garanti_mnd"] = df["garanti_mnd"].fillna(0).clip(lower=0)
    df["rekkevidde_km"] = df["rekkevidde_km"].fillna(0)

    # Sorter for deterministisk treningsrekkefølge
    df = df.sort_values(["Dato", "salgspris"], kind="mergesort").reset_index(drop=True)

    return df


# ---- Trening ----

def _build_pipeline(is_general: bool) -> Pipeline:
    cat_cols = GEN_FEATURES_CAT if is_general else SEG_FEATURES_CAT
    pre = ColumnTransformer(
        transformers=[
            ("num", "passthrough", SEG_FEATURES_NUM),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ],
        remainder="drop",
    )
    model = HistGradientBoostingRegressor(
        max_depth=8 if is_general else 6,
        learning_rate=0.06,
        max_iter=600 if is_general else 400,
        random_state=42,
    )
    return Pipeline([("pre", pre), ("model", model)])


def _train_one(df_train: pd.DataFrame, label: str, is_general: bool) -> SegmentModel:
    cat_cols = GEN_FEATURES_CAT if is_general else SEG_FEATURES_CAT
    cols = SEG_FEATURES_NUM + cat_cols
    X = df_train.reindex(columns=cols).copy()
    for c in SEG_FEATURES_NUM:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    for c in cat_cols:
        X[c] = X[c].fillna("Ukjent").astype(str)
    y = np.log1p(df_train["salgspris"].values)

    pipe = _build_pipeline(is_general=is_general)
    pipe.fit(X, y)
    return SegmentModel(model=pipe, n_obs=len(df_train), label=label)


def train_hierarchical(df_sold: pd.DataFrame, min_l1: int, min_l2: int, label_prefix: str):
    l1, l2, general = {}, {}, None

    seg1_counts = df_sold["segment_1"].value_counts()
    for seg in seg1_counts[seg1_counts >= min_l1].index:
        d = df_sold[df_sold["segment_1"] == seg]
        l1[seg] = _train_one(d, f"{label_prefix} L1: {seg}", is_general=False)

    seg2_counts = df_sold["segment_2"].value_counts()
    for seg in seg2_counts[seg2_counts >= min_l2].index:
        d = df_sold[df_sold["segment_2"] == seg]
        l2[seg] = _train_one(d, f"{label_prefix} L2: {seg}", is_general=False)

    if len(df_sold) >= MIN_OBS_GENERAL:
        general = _train_one(df_sold, f"{label_prefix} GEN", is_general=True)

    return l1, l2, general


def train_all(df: pd.DataFrame) -> FlipModels:
    df_sold = df[df["er_solgt"]].copy()
    df_sold = df_sold[df_sold["dager_paa_markedet"].between(0, MAX_DAGER_FOR_MARKED_TRENING)]
    n_market = len(df_sold)
    print(f"[TRAIN] Markedspris treningssett: {n_market}")

    market_l1, market_l2, market_gen = train_hierarchical(
        df_sold, MIN_OBS_L1, MIN_OBS_L2, "MARKET"
    )
    print(
        f"[TRAIN] Market modeller: L1={len(market_l1)} L2={len(market_l2)} "
        f"GEN={'JA' if market_gen else 'NEI'}"
    )

    df_fast = df_sold[df_sold["dager_paa_markedet"] <= HURTIG_DAGER].copy()
    n_fast = len(df_fast)
    print(f"[TRAIN] Hurtigsalg treningssett (<= {HURTIG_DAGER}d): {n_fast}")

    fast_l1, fast_l2, fast_gen = train_hierarchical(
        df_fast, MIN_OBS_L1_FAST, MIN_OBS_L2_FAST, "FAST"
    )
    print(
        f"[TRAIN] Fast modeller: L1={len(fast_l1)} L2={len(fast_l2)} "
        f"GEN={'JA' if fast_gen else 'NEI'}"
    )

    return FlipModels(
        market_l1=market_l1, market_l2=market_l2, market_general=market_gen,
        fast_l1=fast_l1, fast_l2=fast_l2, fast_general=fast_gen,
        trained_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        n_market_train=n_market, n_fast_train=n_fast,
    )


# ---- I/O ----

def hent_parquet_fra_s3(local_cache: str | None = None) -> str:
    """Last ned database_biler.parquet til lokal sti og returner stien."""
    import boto3
    from config import S3_BUCKET_NAME, AWS_KEY, AWS_SECRET, AWS_REGION

    s3 = boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )
    if not local_cache:
        local_cache = os.path.join(tempfile.gettempdir(), "database_biler.parquet")

    print(f"[S3] Laster ned s3://{S3_BUCKET_NAME}/{S3_INPUT_KEY} -> {local_cache}")
    s3.download_file(S3_BUCKET_NAME, S3_INPUT_KEY, local_cache)
    return local_cache


def upload_modell_til_s3(local_path: str, key: str = S3_OUTPUT_KEY):
    import boto3
    from config import S3_BUCKET_NAME, AWS_KEY, AWS_SECRET, AWS_REGION

    s3 = boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )
    print(f"[S3] Laster opp {local_path} -> s3://{S3_BUCKET_NAME}/{key}")
    s3.upload_file(local_path, S3_BUCKET_NAME, key)


def lagre_modell(models: FlipModels, path: str):
    with open(path, "wb") as f:
        pickle.dump(models, f, protocol=pickle.HIGHEST_PROTOCOL)
    storrelse_mb = os.path.getsize(path) / 1024 / 1024
    print(f"[SAVE] Lagret {path} ({storrelse_mb:.1f} MB)")


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="Tren Bilradar prismodell (FlipModels)")
    parser.add_argument("--input", help="Lokal parquet-fil med biler")
    parser.add_argument("--s3", action="store_true", help="Hent input-data fra S3")
    parser.add_argument("--output", default="bil_prismodell.pkl", help="Lokal output-pkl")
    parser.add_argument("--upload", action="store_true", help="Last opp pkl til S3 etter trening")
    args = parser.parse_args()

    if args.s3:
        input_path = hent_parquet_fra_s3()
    elif args.input:
        input_path = args.input
    else:
        parser.error("Spesifiser enten --input <fil> eller --s3")

    print("=" * 70)
    print(f"BILRADAR PRISMODELL — trening startet {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    df = load_and_prepare(input_path)
    print(
        f"[DATA] Rader: {len(df):,} | Aktiv i dag: {int(df['er_aktiv'].sum()):,} | "
        f"Solgt/fjernet: {int(df['er_solgt'].sum()):,}"
    )

    models = train_all(df)
    lagre_modell(models, args.output)

    if args.upload:
        upload_modell_til_s3(args.output)

    print("=" * 70)
    print(f"FERDIG. Modell trent: {models.trained_at}")
    print("=" * 70)


if __name__ == "__main__":
    main()
