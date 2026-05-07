"""
bilradar_lookup.py – Transparent lookup-prising fra salgsdata
=============================================================
Leser en CSV-tabell (lokal eller S3) bygget av scripts/lag_prislookup.py
og bruker den til aa estimere markedspris direkte fra median-salg per
(Produsent, Modell, hjuldrift, drivstoff, aarstall)-gruppe.

Ved oppslag: pris = median_pris + km_slope * (kjorelengde - median_km)

Brukes som primaer prisestimering i bilradar_scorer; ML-modellen
beholdes som fallback for biler hvor lookup ikke har data.

CSV-format (kolonner):
    Produsent, Modell, hjuldrift, drivstoff, aarstall,
    n_obs, median_pris, median_km, km_slope, ...

Caches paa samme maate som overrides-tabellen — last_lookup() er
trådsikker og kan kalles fritt fra Flask-routes.
"""

import io
import os
import threading
from datetime import datetime

import numpy as np
import pandas as pd

LOOKUP_KEYS = ["Produsent", "Modell", "hjuldrift", "drivstoff", "årstall"]
LOOKUP_VALS = ["n_obs", "median_pris", "median_km", "km_slope"]
LOOKUP_COLUMNS = LOOKUP_KEYS + LOOKUP_VALS

_LOOKUP_LOCK = threading.Lock()
_LOOKUP_CACHE = {"df": None, "loaded_at": None}


def _normaliser_hjuldrift(verdi) -> str:
    """Mapper alle hjuldrift-varianter til "Tohjul"/"Firehjul"/"Ukjent".

    Treningsdata (database_biler.parquet) bruker "Tohjul"/"Firehjul",
    mens FINN-detaljparseren leverer "Forhjulsdrift"/"Bakhjulsdrift"/
    "Firehjulsdrift" — uten normalisering matcher lookup-tabellen ingenting
    paa "NY – IKKE I DB"-biler.
    """
    if verdi is None:
        return "Ukjent"
    s = str(verdi).strip().lower()
    if not s or s in ("ukjent", "nan", "none"):
        return "Ukjent"
    if "firehjul" in s or "awd" in s or s == "4x4":
        return "Firehjul"
    if "tohjul" in s or "forhjul" in s or "bakhjul" in s or "fwd" in s or "rwd" in s:
        return "Tohjul"
    return str(verdi).strip()


def _les_csv(buf_or_path) -> pd.DataFrame:
    df = pd.read_csv(buf_or_path)
    df.columns = [c.strip() for c in df.columns]
    for col in LOOKUP_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    for c in ["Produsent", "Modell", "drivstoff"]:
        df[c] = df[c].fillna("Ukjent").astype(str).str.strip()
    df["hjuldrift"] = df["hjuldrift"].map(_normaliser_hjuldrift)
    df["årstall"] = pd.to_numeric(df["årstall"], errors="coerce").astype("Int64")
    for c in ["n_obs", "median_pris", "median_km", "km_slope"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["median_pris", "median_km", "km_slope", "årstall"])
    return df.reset_index(drop=True)


def last_lookup(local_path: str = "", s3_client=None, bucket: str = "", key: str = "") -> pd.DataFrame:
    """Last lookup-tabell. Returnerer tom DataFrame hvis intet finnes (ikke en feil)."""
    with _LOOKUP_LOCK:
        if _LOOKUP_CACHE["df"] is not None:
            return _LOOKUP_CACHE["df"]

        df = pd.DataFrame(columns=LOOKUP_COLUMNS)

        if local_path and os.path.exists(local_path):
            try:
                df = _les_csv(local_path)
                print(f"[BilRadar] Lookup lastet ({len(df)} grupper) fra {local_path}")
            except Exception as e:
                print(f"[BilRadar] Klarte ikke lese {local_path}: {e}")
        elif s3_client and bucket and key:
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                df = _les_csv(io.BytesIO(obj["Body"].read()))
                print(f"[BilRadar] Lookup lastet ({len(df)} grupper) fra s3://{bucket}/{key}")
            except Exception as e:
                print(f"[BilRadar] Ingen lookup fra S3 ({e}) – fortsetter uten")

        _LOOKUP_CACHE["df"] = df
        _LOOKUP_CACHE["loaded_at"] = datetime.now()
        return df


def reload_lookup():
    """Tving re-lasting ved neste kall (etter at CSV er endret)."""
    with _LOOKUP_LOCK:
        _LOOKUP_CACHE["df"] = None
        _LOOKUP_CACHE["loaded_at"] = None


def apply_lookup(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Overskriv forventet_pris/peer_konfidens/modell_nivaa for biler som
    matcher en (merke, modell, hjuldrift, drivstoff, aarstall)-gruppe i
    lookup. Biler uten match beholder ML-prediksjon. Idempotent: tom
    lookup = ingen endring."""
    if lookup is None or lookup.empty:
        return df

    df = df.copy()

    # Sikre samme dtype paa beggee sider
    venstre = df.copy()
    for c in ["Produsent", "Modell", "drivstoff"]:
        if c in venstre.columns:
            venstre[c] = venstre[c].fillna("Ukjent").astype(str).str.strip()
    if "hjuldrift" in venstre.columns:
        venstre["hjuldrift"] = venstre["hjuldrift"].map(_normaliser_hjuldrift)
    venstre["årstall"] = pd.to_numeric(venstre.get("årstall"), errors="coerce").astype("Int64")
    venstre["_idx"] = np.arange(len(venstre))

    merged = venstre.merge(
        lookup[LOOKUP_COLUMNS], on=LOOKUP_KEYS, how="left", suffixes=("", "_lu"),
    )
    merged = merged.sort_values("_idx").set_index("_idx")

    mask = merged["median_pris"].notna() & merged["km_slope"].notna()
    if not mask.any():
        return df

    km = pd.to_numeric(merged.get("kjørelengde"), errors="coerce").fillna(0)
    lookup_pris = (
        merged["median_pris"]
        + merged["km_slope"] * (km - merged["median_km"])
    )
    # Sikkerhet: ikke la km-justering trekke prisen under 1000 NOK
    lookup_pris = lookup_pris.clip(lower=1_000.0)

    df.loc[mask.values, "forventet_pris"] = lookup_pris[mask].values
    df.loc[mask.values, "peer_konfidens"] = merged.loc[mask, "n_obs"].values

    if "modell_nivaa" not in df.columns:
        df["modell_nivaa"] = "Ingen modell"
    df.loc[mask.values, "modell_nivaa"] = "LOOKUP"

    return df
