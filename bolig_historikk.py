# bolig_historikk.py
# Historikk-funksjoner for Flask-ruten /bolig/historikk/

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Iterable

import boto3
import pandas as pd

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME


@dataclass(frozen=True)
class HistorikkConfig:
    prefix: str = "raw/bolig-daglig/"
    filename_regex: str = r"bolig_X_(\d{2}-\d{2}-\d{4})\.csv"


CFG = HistorikkConfig()


METRIC_LABELS = {
    "median_totalpris": "Median totalpris",
    "mean_totalpris": "Snitt totalpris",
    "median_m2pris": "Median m²-pris",
    "mean_m2pris": "Snitt m²-pris",
    "count": "Antall annonser",
}


def _get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )


@lru_cache(maxsize=1)
def _list_bolig_files() -> dict[pd.Timestamp, str]:
    s3_client = _get_s3_client()
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=CFG.prefix)
    if "Contents" not in response:
        return {}

    results: dict[pd.Timestamp, str] = {}
    for obj in response["Contents"]:
        key = obj["Key"]
        match = re.search(CFG.filename_regex, key)
        if not match:
            continue
        file_date = datetime.strptime(match.group(1), "%d-%m-%Y").date()
        results[pd.Timestamp(file_date)] = key
    return results


def get_available_bolig_dates() -> list[pd.Timestamp]:
    return sorted(_list_bolig_files().keys())


def get_default_dates_for_ui() -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    dates = get_available_bolig_dates()
    if not dates:
        return None, None
    if len(dates) == 1:
        return dates[0], dates[0]
    return dates[-2], dates[-1]


def _coerce_price(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    col_map = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> str | None:
        for name in names:
            if name in col_map:
                return col_map[name]
        return None

    fylke_col = pick("fylke")
    if fylke_col is None:
        df["fylke"] = ""
        fylke_col = "fylke"
    df["fylke"] = df[fylke_col].fillna("").astype(str).str.strip()

    m2_col = pick("m2-pris", "m2_pris", "m2pris")
    if m2_col:
        df["m2pris_num"] = _coerce_price(df[m2_col])
    else:
        df["m2pris_num"] = pd.NA

    total_col = pick("totalpris", "total_pris", "total pris")
    if total_col:
        df["totalpris_num"] = _coerce_price(df[total_col])
    else:
        df["totalpris_num"] = pd.NA

    return df


def _read_bolig_csv(key: str) -> pd.DataFrame:
    s3_client = _get_s3_client()
    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    df = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep=";",
        encoding="utf-16",
        on_bad_lines="skip",
    )
    return _normalize_columns(df)


def _closest_date(target: pd.Timestamp, dates: Iterable[pd.Timestamp]) -> pd.Timestamp:
    sorted_dates = sorted(dates)
    if not sorted_dates:
        raise ValueError("Ingen tilgjengelige historikkdatoer.")
    candidates = [d for d in sorted_dates if d <= target]
    if candidates:
        return candidates[-1]
    return sorted_dates[0]


def _metric_series(df: pd.DataFrame, metric_col: str) -> pd.Series:
    if metric_col == "count":
        return df.groupby("fylke")["fylke"].count()
    if metric_col in {"median_totalpris", "mean_totalpris"}:
        series = df.groupby("fylke")["totalpris_num"]
    elif metric_col in {"median_m2pris", "mean_m2pris"}:
        series = df.groupby("fylke")["m2pris_num"]
    else:
        raise ValueError(f"Ukjent metrikk: {metric_col}")

    if metric_col.startswith("median"):
        return series.median()
    return series.mean()


def _build_snapshot(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    metrics = _metric_series(df, metric_col).reset_index()
    metrics.columns = ["Fylke", "metric"]

    if metric_col == "count":
        norge_value = int(df.shape[0])
    elif metric_col.startswith("median"):
        if metric_col.endswith("totalpris"):
            norge_value = df["totalpris_num"].median()
        else:
            norge_value = df["m2pris_num"].median()
    else:
        if metric_col.endswith("totalpris"):
            norge_value = df["totalpris_num"].mean()
        else:
            norge_value = df["m2pris_num"].mean()

    metrics = pd.concat(
        [
            pd.DataFrame([{"Fylke": "Hele Norge", "metric": norge_value}]),
            metrics,
        ],
        ignore_index=True,
    )
    return metrics


def build_historikk_tabell(
    metric_col: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    metric_col = metric_col or "median_totalpris"
    if metric_col not in METRIC_LABELS:
        raise ValueError(f"Ukjent metrikk: {metric_col}")

    available = get_available_bolig_dates()
    if not available:
        raise ValueError("Ingen tilgjengelige historikkfiler.")

    start_dt = _closest_date(pd.to_datetime(start_date), available)
    end_dt = _closest_date(pd.to_datetime(end_date), available)

    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    keys = _list_bolig_files()
    start_key = keys.get(start_dt)
    end_key = keys.get(end_dt)
    if not start_key or not end_key:
        raise ValueError("Fant ikke historikkfil for valgt dato.")

    start_df = _read_bolig_csv(start_key)
    end_df = _read_bolig_csv(end_key)

    start_snapshot = _build_snapshot(start_df, metric_col).set_index("Fylke")
    end_snapshot = _build_snapshot(end_df, metric_col).set_index("Fylke")

    all_rows = start_snapshot.index.union(end_snapshot.index)
    start_vals = start_snapshot.reindex(all_rows)["metric"]
    end_vals = end_snapshot.reindex(all_rows)["metric"]

    change = end_vals - start_vals
    pct = (change / start_vals.replace({0: pd.NA})) * 100

    metric_label = METRIC_LABELS[metric_col]
    out = pd.DataFrame(
        {
            "Fylke": all_rows,
            f"{metric_label} (start)": start_vals,
            f"{metric_label} (slutt)": end_vals,
            "Endring": change,
            "Endring %": pct.round(1),
        }
    ).reset_index(drop=True)

    if metric_col == "count":
        out[f"{metric_label} (start)"] = out[f"{metric_label} (start)"].round(0)
        out[f"{metric_label} (slutt)"] = out[f"{metric_label} (slutt)"].round(0)
        out["Endring"] = out["Endring"].round(0)
    else:
        out[f"{metric_label} (start)"] = out[f"{metric_label} (start)"].round(0)
        out[f"{metric_label} (slutt)"] = out[f"{metric_label} (slutt)"].round(0)
        out["Endring"] = out["Endring"].round(0)

    out = out.sort_values("Fylke")
    return out, start_dt, end_dt
