"""
bilradar_overrides.py – Manuell overstyring av modellpriser
============================================================
Leser en CSV-tabell (lokal eller S3) med justeringsregler og bruker
dem på `forventet_pris` etter at modellen har predikert.

Format på CSV (data/pris_overstyring.csv):
    match_type,merke,modell,drivstoff,år_fra,år_til,variant_keyword,justering_type,verdi,kommentar
    variant,Tesla,Model Y,Elektrisk,2020,2024,Long Range,mult,1.18,LR ~18% over snittet

match_type:
    variant   – krever treff på variant_keyword i Modell+Info (case-insensitiv substring)
    modell    – treffer alle av (merke, modell, drivstoff) innen år-intervall
    absolutt  – som "modell" men setter pris direkte; vinner over alt annet

justering_type:
    mult – multipliser forventet_pris med verdi (f.eks. 1.18, 0.95)
    add  – legg verdi (kr) til forventet_pris (kan være negativ)
    abs  – sett forventet_pris til verdi (kr)

Mer spesifikke regler vinner: absolutt > variant > modell.
"""

import io
import os
import threading
from datetime import datetime

import numpy as np
import pandas as pd

OVERRIDES_COLUMNS = [
    "match_type", "merke", "modell", "drivstoff",
    "år_fra", "år_til", "variant_keyword",
    "justering_type", "verdi", "kommentar",
]

_PRECEDENCE = {"modell": 0, "variant": 1, "absolutt": 2}

_OVERRIDES_LOCK = threading.Lock()
_OVERRIDES_CACHE = {"df": None, "loaded_at": None}


def _les_csv(buf_or_path) -> pd.DataFrame:
    df = pd.read_csv(buf_or_path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    for col in OVERRIDES_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[OVERRIDES_COLUMNS].copy()
    for col in ["år_fra", "år_til"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["verdi"] = pd.to_numeric(df["verdi"], errors="coerce")
    df = df.dropna(subset=["verdi"])
    df = df[df["match_type"].isin(_PRECEDENCE.keys())]
    df = df[df["justering_type"].isin(["mult", "add", "abs"])]
    df["_prio"] = df["match_type"].map(_PRECEDENCE).fillna(0).astype(int)
    df = df.sort_values("_prio", ascending=True).drop(columns=["_prio"]).reset_index(drop=True)
    return df


def last_overrides(local_path: str = "", s3_client=None, bucket: str = "", key: str = ""):
    """Last overstyringstabell. Returnerer tom DataFrame hvis intet finnes (ikke en feil)."""
    with _OVERRIDES_LOCK:
        if _OVERRIDES_CACHE["df"] is not None:
            return _OVERRIDES_CACHE["df"]

        df = pd.DataFrame(columns=OVERRIDES_COLUMNS)

        if local_path and os.path.exists(local_path):
            try:
                df = _les_csv(local_path)
                print(f"[BilRadar] Overstyringer lastet ({len(df)} regler) fra {local_path}")
            except Exception as e:
                print(f"[BilRadar] Klarte ikke lese {local_path}: {e}")
        elif s3_client and bucket and key:
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                df = _les_csv(io.BytesIO(obj["Body"].read()))
                print(f"[BilRadar] Overstyringer lastet ({len(df)} regler) fra s3://{bucket}/{key}")
            except Exception as e:
                print(f"[BilRadar] Ingen overstyringer fra S3 ({e}) – fortsetter uten")

        _OVERRIDES_CACHE["df"] = df
        _OVERRIDES_CACHE["loaded_at"] = datetime.now()
        return df


def reload_overrides():
    """Tving re-lasting ved neste kall (etter at CSV er endret)."""
    with _OVERRIDES_LOCK:
        _OVERRIDES_CACHE["df"] = None
        _OVERRIDES_CACHE["loaded_at"] = None


def _str_eq(s: pd.Series, target: str) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.lower() == str(target).strip().lower()


def _str_contains(s: pd.Series, target: str) -> pd.Series:
    """Sjekk om target forekommer som delstreng (case-insensitivt). Brukes på Modell-feltet
    slik at rule.modell="Model Y" treffer både "Model Y" og "Model Y Long Range AWD"."""
    t = str(target).strip().lower()
    if not t:
        return pd.Series([True] * len(s), index=s.index)
    return s.fillna("").astype(str).str.lower().str.contains(t, regex=False, na=False)


def apply_overrides(df: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    """Bruk overstyringer på `forventet_pris`. Idempotent: tom regelsett = ingen endring."""
    if overrides is None or overrides.empty or "forventet_pris" not in df.columns:
        df["forventet_pris_raa"] = df.get("forventet_pris")
        df["overstyrt"] = False
        df["overstyring_kommentar"] = ""
        return df

    df = df.copy()
    df["forventet_pris_raa"] = df["forventet_pris"]
    df["overstyrt"] = False
    df["overstyring_kommentar"] = ""

    info_col = df["Info"] if "Info" in df.columns else pd.Series([""] * len(df), index=df.index)
    sok_tekst = (
        df.get("Modell", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
        + " "
        + info_col.fillna("").astype(str)
    ).str.lower()

    for _, rule in overrides.iterrows():
        mask = (
            _str_eq(df["Produsent"], rule["merke"])
            & _str_contains(df["Modell"], rule["modell"])
            & _str_eq(df["drivstoff"], rule["drivstoff"])
            & df["årstall"].between(rule["år_fra"], rule["år_til"], inclusive="both")
            & df["forventet_pris"].notna()
        )

        if rule["match_type"] == "variant":
            kw = str(rule["variant_keyword"]).strip().lower()
            if not kw:
                continue
            mask &= sok_tekst.str.contains(kw, regex=False, na=False)

        if not mask.any():
            continue

        if rule["justering_type"] == "mult":
            df.loc[mask, "forventet_pris"] = df.loc[mask, "forventet_pris"] * rule["verdi"]
        elif rule["justering_type"] == "add":
            df.loc[mask, "forventet_pris"] = df.loc[mask, "forventet_pris"] + rule["verdi"]
        elif rule["justering_type"] == "abs":
            df.loc[mask, "forventet_pris"] = rule["verdi"]

        df.loc[mask, "overstyrt"] = True
        df.loc[mask, "overstyring_kommentar"] = str(rule.get("kommentar", "") or "")

    return df
