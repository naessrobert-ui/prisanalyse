# bolig_historikk.py
# Historikk-funksjoner for Flask-ruten /bolig/historikk/
# (Ren backend-modul: ingen Streamlit/UI-kode, ingen side-effekter ved import)

from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Iterable

import boto3
import pandas as pd


# ----------------- Konfig -----------------

@dataclass(frozen=True)
class HistorikkConfig:
    # S3-keys (daglige csv-filer)
    prefix: str = os.environ.get("BOLIG_HIST_PREFIX", "raw/bolig-daglig/")
    filename_regex: str = os.environ.get(
        "BOLIG_HIST_FILENAME_REGEX",
        r"bolig_X_(\d{2}-\d{2}-\d{4})\.csv",
    )

    # AWS / bucket (fra env eller config.py)
    s3_bucket: str = os.environ.get("S3_BUCKET_NAME", "")


CFG = HistorikkConfig()

METRIC_LABELS: dict[str, str] = {
    "median_totalpris": "Median totalpris",
    "mean_totalpris": "Snitt totalpris",
    "median_m2pris": "Median m²-pris",
    "mean_m2pris": "Snitt m²-pris",
    "count": "Antall annonser",
}


# ----------------- AWS creds (env først, fallback til config.py) -----------------

def _get_aws_settings() -> dict[str, str]:
    aws_region = os.environ.get("AWS_REGION", "")
    aws_key = os.environ.get("AWS_KEY", "")
    aws_secret = os.environ.get("AWS_SECRET", "")
    bucket = CFG.s3_bucket

    # fallback til config.py hvis du bruker det lokalt/Render
    if (not aws_region or not aws_key or not aws_secret or not bucket):
        try:
            # type: ignore
            from config import AWS_REGION, AWS_KEY, AWS_SECRET, S3_BUCKET_NAME

            aws_region = aws_region or AWS_REGION
            aws_key = aws_key or AWS_KEY
            aws_secret = aws_secret or AWS_SECRET
            bucket = bucket or S3_BUCKET_NAME
        except Exception:
            pass

    if not bucket:
        raise RuntimeError(
            "Mangler S3_BUCKET_NAME / BOLIG_S3_BUCKET. Sett env-var eller legg i config.py."
        )

    return {
        "region": aws_region,
        "key": aws_key,
        "secret": aws_secret,
        "bucket": bucket,
    }


@lru_cache(maxsize=1)
def _get_s3_client():
    s = _get_aws_settings()

    # Hvis du kjører på Render med IAM role/ambient creds, kan key/secret være tomme.
    # boto3 håndterer det fint hvis du ikke sender dem inn.
    kwargs = {}
    if s["region"]:
        kwargs["region_name"] = s["region"]
    if s["key"] and s["secret"]:
        kwargs["aws_access_key_id"] = s["key"]
        kwargs["aws_secret_access_key"] = s["secret"]

    return boto3.client("s3", **kwargs)


# ----------------- S3 listing -----------------

@lru_cache(maxsize=1)
def _list_bolig_files() -> dict[pd.Timestamp, str]:
    """
    Returnerer mapping: dato -> s3 key for historikkfil.
    Dato trekkes ut fra filnavn ved CFG.filename_regex.
    """
    s = _get_aws_settings()
    s3 = _get_s3_client()

    results: dict[pd.Timestamp, str] = {}

    token = None
    while True:
        kwargs = {"Bucket": s["bucket"], "Prefix": CFG.prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []) or []:
            key = obj.get("Key", "")
            m = re.search(CFG.filename_regex, key)
            if not m:
                continue
            file_date = datetime.strptime(m.group(1), "%d-%m-%Y").date()
            results[pd.Timestamp(file_date)] = key

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    return results


def get_available_bolig_dates() -> list[pd.Timestamp]:
    return sorted(_list_bolig_files().keys())


# ----------------- CSV parsing/normalisering -----------------

def _coerce_price(series: pd.Series) -> pd.Series:
    # Tåler "1 234 567", "1.234.567", "1 234 567 kr", osv.
    cleaned = (
        series.astype(str)
        .str.replace("kr", "", regex=False)
        .str.replace("KR", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("\u00A0", "", regex=False)  # non-breaking space
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliserer kolonnenavn og lager:
      - fylke (string)
      - totalpris_num (float)
      - m2pris_num (float)
    """
    d = df.copy()
    d.columns = d.columns.astype(str).str.strip()

    col_map = {c.lower(): c for c in d.columns}

    def pick(*names: str) -> str | None:
        for n in names:
            c = col_map.get(n.lower())
            if c:
                return c
        return None

    fylke_col = pick("fylke")
    if not fylke_col:
        d["fylke"] = ""
    else:
        d["fylke"] = d[fylke_col].fillna("").astype(str).str.strip()

    m2_col = pick("m2-pris", "m2_pris", "m2pris", "m2 pris")
    if m2_col:
        d["m2pris_num"] = _coerce_price(d[m2_col])
    else:
        d["m2pris_num"] = pd.NA

    total_col = pick("totalpris", "total_pris", "total pris")
    if total_col:
        d["totalpris_num"] = _coerce_price(d[total_col])
    else:
        d["totalpris_num"] = pd.NA

    return d


def _read_bolig_csv(key: str) -> pd.DataFrame:
    s = _get_aws_settings()
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=s["bucket"], Key=key)

    df = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep=";",
        encoding="utf-16",
        on_bad_lines="skip",
    )
    return _normalize_columns(df)


# ----------------- Metrics -----------------

def _closest_date(target: pd.Timestamp, dates: Iterable[pd.Timestamp]) -> pd.Timestamp:
    sorted_dates = sorted(dates)
    if not sorted_dates:
        raise ValueError("Ingen tilgjengelige historikkdatoer.")
    target = pd.to_datetime(target).normalize()

    candidates = [d for d in sorted_dates if d <= target]
    if candidates:
        return candidates[-1]
    return sorted_dates[0]


def _metric_series(df: pd.DataFrame, metric_col: str) -> pd.Series:
    """
    Returnerer en Series indeksert på fylke med valgt metrikk.
    """
    metric_col = metric_col or "median_totalpris"

    if metric_col == "count":
        return df.groupby("fylke")["fylke"].count()

    if metric_col in {"median_totalpris", "mean_totalpris"}:
        grp = df.groupby("fylke")["totalpris_num"]
    elif metric_col in {"median_m2pris", "mean_m2pris"}:
        grp = df.groupby("fylke")["m2pris_num"]
    else:
        raise ValueError(f"Ukjent metrikk: {metric_col}")

    if metric_col.startswith("median"):
        return grp.median()
    return grp.mean()


def _build_snapshot(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    metrics = _metric_series(df, metric_col).reset_index()
    metrics.columns = ["Fylke", "metric"]

    # Legg inn "Hele Norge" øverst
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
        [pd.DataFrame([{"Fylke": "Hele Norge", "metric": norge_value}]), metrics],
        ignore_index=True,
    )
    return metrics


# ----------------- Public API brukt av Flask -----------------

def build_historikk_tabell(
    metric_col: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """
    Bygger tabell med start/slutt snapshot (fylke), endring og endring %.
    Returnerer: (df, start_dt, end_dt) der start_dt/end_dt er faktiske (nærmeste) tilgjengelige historikkdatoer.
    """
    metric_col = metric_col or "median_totalpris"
    if metric_col not in METRIC_LABELS:
        raise ValueError(f"Ukjent metrikk: {metric_col}")

    available = get_available_bolig_dates()
    if not available:
        raise ValueError("Ingen tilgjengelige historikkfiler i S3 (prefix/regex/bucket).")

    start_dt = _closest_date(pd.to_datetime(start_date), available)
    end_dt = _closest_date(pd.to_datetime(end_date), available)

    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    keys = _list_bolig_files()
    start_key = keys.get(start_dt)
    end_key = keys.get(end_dt)
    if not start_key or not end_key:
        raise ValueError("Fant ikke historikkfil for valgt dato (sjekk regex/prefix).")

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

    # Rund av til “pent” format
    for c in [f"{metric_label} (start)", f"{metric_label} (slutt)", "Endring"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(0)

    out = out.sort_values("Fylke")
    return out, start_dt, end_dt
