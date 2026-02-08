# bolig_historikk_service.py
# -*- coding: utf-8 -*-

import io
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal, Optional

import numpy as np
import pandas as pd

import boto3
from botocore.config import Config


Level = Literal["Fylke", "Kommune", "Sted"]


@dataclass(frozen=True)
class HistConfig:
    s3_bucket: str = os.environ.get("BOLIG_S3_BUCKET", "prisanalyse-data")
    master_key: str = os.environ.get("BOLIG_MASTER_KEY", "calc/bolig/bolig_master/bolig_master.parquet")


CFG = HistConfig()


def _s3_client():
    config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=30,
        read_timeout=120,
        tcp_keepalive=True,
    )
    return boto3.client("s3", config=config)


@lru_cache(maxsize=1)
def load_master_s3_cached(bucket: str, key: str) -> pd.DataFrame:
    """
    Leser master parquet fra S3 én gang per prosess.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    return df


def extract_poststed(address: pd.Series) -> pd.Series:
    s = address.astype(str)
    s = s.str.split(",").str[-1]
    s = s.str.replace(r"\b\d{4}\b", "", regex=True)
    s = s.str.strip()
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
    return s


def normalize_master(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    required = [
        "finnkode", "fylke", "kommune_nr", "kommune_navn",
        "address", "full_title",
        "totalpris", "m2_pris", "ny_brukt",
        "latitude", "longitude",
        "dato_første", "dato_siste",
        "pris_første", "pris_ny", "dato_prisendring",
    ]
    for c in required:
        if c not in d.columns:
            d[c] = pd.NA

    # Dates
    d["dato_første"] = pd.to_datetime(d["dato_første"], errors="coerce").dt.normalize()
    d["dato_siste"] = pd.to_datetime(d["dato_siste"], errors="coerce").dt.normalize()
    d["dato_prisendring"] = pd.to_datetime(d["dato_prisendring"], errors="coerce").dt.normalize()

    # Numerics
    for col in ["totalpris", "m2_pris", "latitude", "longitude", "pris_første", "pris_ny"]:
        d[col] = pd.to_numeric(d[col], errors="coerce")

    # Strings
    for c in ["fylke", "kommune_navn", "ny_brukt", "address", "full_title"]:
        d[c] = d[c].astype(str).replace({"None": "", "nan": ""})

    d["finnkode"] = d["finnkode"].astype(str)

    d["sted"] = extract_poststed(d["address"])
    mask = d["sted"].isna() | (d["sted"].astype(str).str.strip() == "")
    d.loc[mask, "sted"] = d["kommune_navn"]

    return d


def apply_ny_brukt_filter(df: pd.DataFrame, choice: str) -> pd.DataFrame:
    if choice in {"Begge", "NBrukt"}:
        return df.copy()
    s = df["ny_brukt"].astype(str).str.strip().str.lower()
    if choice == "Brukt":
        return df[s == "brukt"].copy()
    # Nybygg
    return df[s.isin(["nybygg", "ny", "nytt"])].copy()


def group_key(df: pd.DataFrame, level: Level) -> pd.Series:
    if level == "Fylke":
        return df["fylke"].fillna("").astype(str)
    if level == "Kommune":
        return df["kommune_navn"].fillna("").astype(str)
    return df["sted"].fillna("").astype(str)


def filter_by_level(df: pd.DataFrame, level: Level, value: str) -> pd.DataFrame:
    value = str(value)
    if level == "Fylke":
        return df[df["fylke"].fillna("").astype(str) == value].copy()
    if level == "Kommune":
        return df[df["kommune_navn"].fillna("").astype(str) == value].copy()
    return df[df["sted"].fillna("").astype(str) == value].copy()


def snapshot_metrics(df: pd.DataFrame, day: pd.Timestamp, level: Level) -> pd.DataFrame:
    day = pd.to_datetime(day).normalize()
    d = df.dropna(subset=["dato_første", "dato_siste"]).copy()

    active = d[(d["dato_første"] <= day) & (d["dato_siste"] >= day)].copy()
    if active.empty:
        return pd.DataFrame(columns=["group", "active_count", "median_totalpris", "median_m2", "mean_days_on_market"])

    active["group"] = group_key(active, level)
    active["days_on_market_today"] = (day - active["dato_første"]).dt.days

    out = active.groupby("group", dropna=False).agg(
        active_count=("finnkode", "count"),
        median_totalpris=("totalpris", "median"),
        median_m2=("m2_pris", "median"),
        mean_days_on_market=("days_on_market_today", "median")  # median for active ads that day,
    ).reset_index()

    return out


def pct_change(end: pd.Series, start: pd.Series) -> pd.Series:
    start = pd.to_numeric(start, errors="coerce")
    end = pd.to_numeric(end, errors="coerce")
    base = start.replace({0: np.nan})
    pct = (end - start) / base * 100.0
    return pct.round(0)


def build_table(df: pd.DataFrame, level: Level, start_day: pd.Timestamp, end_day: pd.Timestamp) -> pd.DataFrame:
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()

    m_start = snapshot_metrics(df, start_day, level).set_index("group")
    m_end = snapshot_metrics(df, end_day, level).set_index("group")

    groups = m_start.index.union(m_end.index)
    out = pd.DataFrame({"Sted": groups.astype(str)})

    def get(name: str, frame: pd.DataFrame, default=np.nan) -> pd.Series:
        if frame.empty:
            return pd.Series([default] * len(out))
        s = frame.reindex(groups)[name]
        return s.reset_index(drop=True)

    # Slutt-metrikker
    out["Aktive (slutt)"] = get("active_count", m_end)
    out["M2 pris (slutt)"] = get("median_m2", m_end)
    out["Totalpris median (slutt)"] = get("median_totalpris", m_end)
    out["Dager på markedet (slutt)"] = get("mean_days_on_market", m_end)

    # Start-metrikker (brukes kun for endringer)
    out["M2 pris (start)"] = get("median_m2", m_start)
    out["Dager på markedet (start)"] = get("mean_days_on_market", m_start)

    # Endringer
    out["Endring M2 (%)"] = pct_change(out["M2 pris (slutt)"], out["M2 pris (start)"])
    out["Endring dager på markedet"] = (
        pd.to_numeric(out["Dager på markedet (slutt)"], errors="coerce")
        - pd.to_numeric(out["Dager på markedet (start)"], errors="coerce")
    )

    # Velg kolonner som skal vises (Sted + ønskede kolonner)
    out = out[
        [
            "Sted",
            "Endring M2 (%)",
            "M2 pris (slutt)",
            "Endring dager på markedet",
            "Totalpris median (slutt)",
            "Aktive (slutt)",
            "Dager på markedet (slutt)",
        ]
    ].copy()

    # --- Formatering for visning ---
    def _fmt_int_space(v) -> str:
        if pd.isna(v):
            return ""
        try:
            return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception:
            return str(v)

    def _to_int(v):
        if pd.isna(v):
            return pd.NA
        try:
            return int(round(float(v)))
        except Exception:
            return pd.NA

    # Gjør om til int der ønsket
    out["Endring M2 (%)"] = out["Endring M2 (%)"].apply(_to_int).astype("Int64")
    out["Endring dager på markedet"] = out["Endring dager på markedet"].apply(_to_int).astype("Int64")
    out["Dager på markedet (slutt)"] = out["Dager på markedet (slutt)"].apply(_to_int).astype("Int64")
    out["Aktive (slutt)"] = out["Aktive (slutt)"].apply(_to_int).astype("Int64")
    out["M2 pris (slutt)"] = out["M2 pris (slutt)"].apply(_to_int).astype("Int64")
    out["Totalpris median (slutt)"] = out["Totalpris median (slutt)"].apply(_to_int).astype("Int64")

    # Sorter default på Endring M2 (%) (numerisk), før vi formatter til tekst
    out = out.sort_values(by="Endring M2 (%)", ascending=False, na_position="last").reset_index(drop=True)

    # Formatter priser med tusenskille (mellomrom) til tekst for visning
    out["M2 pris (slutt)"] = out["M2 pris (slutt)"].apply(_fmt_int_space)
    out["Totalpris median (slutt)"] = out["Totalpris median (slutt)"].apply(_fmt_int_space)

    # Resten som vanlige heltall-tekster
    for c in ["Endring M2 (%)", "Endring dager på markedet", "Aktive (slutt)", "Dager på markedet (slutt)"]:
        out[c] = out[c].apply(lambda v: "" if pd.isna(v) else str(int(v)))

    return out



def daily_series_fast(df: pd.DataFrame, start_day: pd.Timestamp, end_day: pd.Timestamp) -> pd.DataFrame:
    """
    Daglig serie for et filtert df (f.eks. ett fylke/kommune/sted).
    """
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()
    days = pd.date_range(start_day, end_day, freq="D")

    d = df.dropna(subset=["dato_første", "dato_siste"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["dato", "active_count", "median_m2", "mean_m2", "median_totalpris", "mean_totalpris", "mean_days_on_market"])

    start_i = d["dato_første"].values.astype("datetime64[D]").astype(np.int64)
    end_i = d["dato_siste"].values.astype("datetime64[D]").astype(np.int64)

    m2 = pd.to_numeric(d.get("m2_pris"), errors="coerce").to_numpy(dtype=float)
    tp = pd.to_numeric(d.get("totalpris"), errors="coerce").to_numpy(dtype=float)

    day_i = days.values.astype("datetime64[D]").astype(np.int64)
    out_rows = []

    for di, day in zip(day_i, days):
        mask = (start_i <= di) & (end_i >= di)
        if not mask.any():
            out_rows.append((day, 0, np.nan, np.nan, np.nan, np.nan, np.nan))
            continue

        m2v = m2[mask]
        tpv = tp[mask]

        days_on_market = (di - start_i[mask]).astype(float)
        mean_dom = float(np.nanmedian(days_on_market)) if days_on_market.size else np.nan  # median days on market for active ads that day

        out_rows.append((
            day,
            int(mask.sum()),
            float(np.nanmedian(m2v)) if np.isfinite(np.nanmedian(m2v)) else np.nan,
            float(np.nanmean(m2v)),
            float(np.nanmedian(tpv)) if np.isfinite(np.nanmedian(tpv)) else np.nan,
            float(np.nanmean(tpv)),
            mean_dom,
        ))

    return pd.DataFrame(out_rows, columns=["dato", "active_count", "median_m2", "mean_m2", "median_totalpris", "mean_totalpris", "mean_days_on_market"])
