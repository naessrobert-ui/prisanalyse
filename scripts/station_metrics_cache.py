#!/usr/bin/env python3
"""Precomputed station-metric cache backed by parquet on S3.

The three weather pages (`/ver/min-temp`, `/ver/vind`, `/ver/nedbor`) are slow
because they call Frost / Yr on every request. This module precomputes the
common scenarios (last observation, current month, current year, the three
forecast windows) for every station, stores the result as a single parquet
file on S3, and exposes a reader so the routes can serve from cache instead.

Schema (one row per station × metric × period):
    sourceId | name | shortName | county | lat | lon
    metric    -- "temp_min" | "temp_max" | "temp_mean" | "precip_sum"
                 | "wind_max" | "wind_mean" | "wind_gust"
    period    -- "last" | "month_current" | "year_current"
                 | "next24h" | "next48h" | "next7d"
    value | value_min | value_max | unit
    reference_time | quality_code | element_used | updated_at
"""
from __future__ import annotations

import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests

from ver_station_db import load_station_db
from yr_forecast import (
    FORECAST_MODES,
    fetch_precip_forecast,
    fetch_temp_forecast,
)


_FROST_BASE = "https://frost.met.no"
_DEFAULT_TIMEOUT = 25
_BATCH_SIZE = 60

_DEFAULT_S3_KEY = "weather-station-metrics/cache.parquet"
_LOCAL_CACHE_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "weather_station_metrics.parquet"
)

_TTL_SECONDS = int(os.getenv("WEATHER_STATION_METRICS_TTL_SECONDS", "10800"))  # 3 h
_STATION_LIMIT = int(
    os.getenv(
        "WEATHER_STATION_METRICS_LIMIT",
        os.getenv("WEATHER_LEADERBOARD_STATION_LIMIT", "220"),
    )
)
_FORECAST_WORKERS = int(os.getenv("WEATHER_STATION_METRICS_FORECAST_WORKERS", "16"))

_MEM_LOCK = threading.RLock()
_MEM_DF: Optional[pd.DataFrame] = None
_MEM_TS: float = 0.0
_MEM_TTL = 60.0
_REFRESH_LOCK = threading.RLock()
_REFRESH_IN_PROGRESS = False
_LAST_ERROR: Optional[str] = None


# ---------------------------------------------------------------------------
# Auth + Frost helpers (self-contained to avoid circular imports)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> _FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID", "").strip()
    if not cid:
        raise RuntimeError("FROST_CLIENT_ID mangler i miljøet.")
    return _FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _frost_aggregate(
    session: requests.Session,
    station_ids: list[str],
    *,
    element: str,
    referencetime: str,
    timeout: int = _DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Call /observations and return latest value per station for `element`."""
    auth = _env_auth()
    rows: list[dict[str, Any]] = []
    for i in range(0, len(station_ids), _BATCH_SIZE):
        batch = station_ids[i : i + _BATCH_SIZE]
        params = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": element,
            "timeoffsets": "default",
            "levels": "default",
            "limit": 1000,
            "qualities": "0,1,2,3,4",
        }
        try:
            resp = session.get(
                f"{_FROST_BASE}/observations/v0.jsonld",
                params=params,
                auth=(auth.client_id, auth.client_secret),
                headers={"Accept": "application/json"},
                timeout=timeout,
            )
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except requests.RequestException:
            continue

        for item in payload.get("data", []) or []:
            sid = str(item.get("sourceId") or "").split(":")[0]
            if not sid:
                continue
            for obs in item.get("observations", []) or []:
                val = _as_float(obs.get("value"))
                if val is None:
                    continue
                rows.append(
                    {
                        "sourceId": sid,
                        "value": val,
                        "unit": obs.get("unit") or "",
                        "qualityCode": _as_float(obs.get("qualityCode")),
                        "referenceTime": obs.get("referenceTime")
                        or item.get("referenceTime"),
                    }
                )

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["referenceTime"] = pd.to_datetime(df["referenceTime"], errors="coerce", utc=True)
    df = df.dropna(subset=["sourceId", "value"])
    if df.empty:
        return df
    return (
        df.sort_values(["sourceId", "referenceTime"])
        .groupby("sourceId", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Metric configuration
# ---------------------------------------------------------------------------

# (metric_name, frost_element, asc_for_min) — one entry per Frost-derived
# scalar we precompute. `asc_for_min` is informational only.
_TEMP_METRICS = [
    ("temp_min", "min", "degC"),
    ("temp_max", "max", "degC"),
    ("temp_mean", "mean", "degC"),
]


def _frost_temp_element(fn: str, suffix: str) -> list[str]:
    return [
        f"best_estimate_{fn}(air_temperature {suffix})",
        f"{fn}(air_temperature {suffix})",
    ]


def _last_two_days() -> str:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=2)
    return f"{start.isoformat()}/{now.isoformat()}"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_config() -> Optional[tuple[str, str]]:
    bucket = (
        os.environ.get("WEATHER_STATION_METRICS_S3_BUCKET", "").strip()
        or os.environ.get("S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        return None
    key = os.environ.get(
        "WEATHER_STATION_METRICS_S3_KEY", _DEFAULT_S3_KEY
    ).strip() or _DEFAULT_S3_KEY
    return bucket, key


def _s3_client():
    import boto3  # local import keeps boto3 optional outside Render
    return boto3.client(
        "s3",
        region_name=os.environ.get(
            "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
        ),
    )


def _read_parquet_bytes(buf: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(buf))


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    return out.getvalue()


def _save_dataframe(df: pd.DataFrame) -> str:
    """Write to S3 if configured, else local path. Returns destination string."""
    body = _df_to_parquet_bytes(df)
    s3 = _s3_config()
    if s3 is not None:
        bucket, key = s3
        _s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/octet-stream",
        )
        return f"s3://{bucket}/{key}"
    _LOCAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _LOCAL_CACHE_PATH.write_bytes(body)
    return str(_LOCAL_CACHE_PATH)


def _load_dataframe() -> Optional[pd.DataFrame]:
    s3 = _s3_config()
    if s3 is not None:
        bucket, key = s3
        try:
            obj = _s3_client().get_object(Bucket=bucket, Key=key)
            return _read_parquet_bytes(obj["Body"].read())
        except Exception:
            return None
    if _LOCAL_CACHE_PATH.exists():
        try:
            return pd.read_parquet(_LOCAL_CACHE_PATH)
        except Exception:
            return None
    return None


def _cache_is_fresh() -> bool:
    s3 = _s3_config()
    if s3 is not None:
        bucket, key = s3
        try:
            head = _s3_client().head_object(Bucket=bucket, Key=key)
            last_modified = head["LastModified"].timestamp()
            return (time.time() - last_modified) < _TTL_SECONDS
        except Exception:
            return False
    if not _LOCAL_CACHE_PATH.exists():
        return False
    return (time.time() - _LOCAL_CACHE_PATH.stat().st_mtime) < _TTL_SECONDS


# ---------------------------------------------------------------------------
# Build payload
# ---------------------------------------------------------------------------

def _stations_for_refresh() -> pd.DataFrame:
    df = load_station_db().copy()
    if df.empty:
        raise RuntimeError("Stasjons-DB er tom.")
    df = df.dropna(subset=["baseId", "lat", "lon"]).copy()
    df["baseId"] = df["baseId"].astype(str)
    if _STATION_LIMIT > 0:
        df = df.head(max(50, _STATION_LIMIT))
    return df.reset_index(drop=True)


def _attach_meta(values: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    if values.empty:
        return values
    meta_cols = [c for c in ["baseId", "name", "shortName", "county", "lat", "lon"] if c in stations.columns]
    meta = stations[meta_cols].rename(columns={"baseId": "sourceId"}).copy()
    return values.merge(meta, on="sourceId", how="left")


def _temp_last_rows(stations: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if stations.empty:
        return rows
    station_ids = stations["baseId"].astype(str).tolist()
    referencetime = _last_two_days()

    with requests.Session() as session:
        for metric_name, fn, unit in _TEMP_METRICS:
            df_obs = pd.DataFrame()
            element_used = ""
            for el in _frost_temp_element(fn, "P1D"):
                tmp = _frost_aggregate(
                    session,
                    station_ids,
                    element=el,
                    referencetime=referencetime,
                )
                if not tmp.empty:
                    df_obs = tmp
                    element_used = el
                    break
            if df_obs.empty:
                continue
            df_obs = _attach_meta(df_obs, stations)
            for _, row in df_obs.iterrows():
                lat = _as_float(row.get("lat"))
                lon = _as_float(row.get("lon"))
                if lat is None or lon is None:
                    continue
                rt = row.get("referenceTime")
                rows.append(
                    {
                        "sourceId": str(row.get("sourceId") or ""),
                        "name": str(row.get("name") or ""),
                        "shortName": str(row.get("shortName") or ""),
                        "county": str(row.get("county") or ""),
                        "lat": float(lat),
                        "lon": float(lon),
                        "metric": metric_name,
                        "period": "last",
                        "value": float(row.get("value")),
                        "value_min": None,
                        "value_max": None,
                        "unit": unit,
                        "reference_time": rt.isoformat() if hasattr(rt, "isoformat") else str(rt or ""),
                        "quality_code": _as_float(row.get("qualityCode")),
                        "element_used": element_used,
                    }
                )
    return rows


def _temp_forecast_rows(stations: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if stations.empty:
        return rows
    src_meta = stations.copy()
    for metric_name, fn, unit in _TEMP_METRICS:
        kind = {"temp_min": "min", "temp_max": "max", "temp_mean": "mean"}[metric_name]
        for mode in ("next24h", "next48h", "next7d"):
            try:
                fc = fetch_temp_forecast(
                    src_meta,
                    mode=mode,
                    temp_kind=kind,
                    max_workers=_FORECAST_WORKERS,
                )
            except Exception:
                fc = pd.DataFrame()
            if fc.empty:
                continue
            fc = fc.rename(columns={"sourceId": "sourceId"})  # noop, explicit
            joined = _attach_meta(
                fc[["sourceId", "value", "referenceTime", "qualityCode", "unit"]].copy(),
                stations,
            )
            for _, row in joined.iterrows():
                lat = _as_float(row.get("lat"))
                lon = _as_float(row.get("lon"))
                if lat is None or lon is None:
                    continue
                rt = row.get("referenceTime")
                rows.append(
                    {
                        "sourceId": str(row.get("sourceId") or ""),
                        "name": str(row.get("name") or ""),
                        "shortName": str(row.get("shortName") or ""),
                        "county": str(row.get("county") or ""),
                        "lat": float(lat),
                        "lon": float(lon),
                        "metric": metric_name,
                        "period": mode,
                        "value": float(row.get("value")),
                        "value_min": None,
                        "value_max": None,
                        "unit": unit,
                        "reference_time": rt.isoformat() if hasattr(rt, "isoformat") else str(rt or ""),
                        "quality_code": _as_float(row.get("qualityCode")),
                        "element_used": "yr_locationforecast",
                    }
                )
    return rows


def build_payload() -> pd.DataFrame:
    """Fetch all metrics for all stations and return as a tidy DataFrame."""
    stations = _stations_for_refresh()
    rows: list[dict[str, Any]] = []
    rows.extend(_temp_last_rows(stations))
    rows.extend(_temp_forecast_rows(stations))

    if not rows:
        raise RuntimeError("Ingen rader produsert ved bygging av station-metrics-cachen.")

    df = pd.DataFrame(rows)
    df["updated_at"] = datetime.now(timezone.utc).isoformat()
    df["station_count"] = int(len(stations))
    return df


def refresh_all() -> dict[str, Any]:
    """Build payload and persist. Used by the cron entrypoint."""
    global _LAST_ERROR
    started = datetime.now(timezone.utc)
    df = build_payload()
    destination = _save_dataframe(df)

    with _MEM_LOCK:
        global _MEM_DF, _MEM_TS
        _MEM_DF = df
        _MEM_TS = time.time()

    _LAST_ERROR = None
    finished = datetime.now(timezone.utc)
    summary = {
        "status": "ok",
        "destination": destination,
        "rows": int(len(df)),
        "stations": int(df["sourceId"].nunique()),
        "metrics": sorted(df["metric"].unique().tolist()),
        "periods": sorted(df["period"].unique().tolist()),
        "generated_at": started.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 2),
    }
    return summary


# ---------------------------------------------------------------------------
# Reader API
# ---------------------------------------------------------------------------

def _get_dataframe() -> Optional[pd.DataFrame]:
    """Return the cached DataFrame (in-memory if fresh, else from S3)."""
    global _MEM_DF, _MEM_TS
    with _MEM_LOCK:
        if _MEM_DF is not None and (time.time() - _MEM_TS) < _MEM_TTL:
            return _MEM_DF
    df = _load_dataframe()
    if df is None:
        return None
    with _MEM_LOCK:
        _MEM_DF = df
        _MEM_TS = time.time()
    return df


def load_metric(
    metric: str,
    period: str,
    *,
    counties: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Return precomputed rows for `metric` × `period`.

    Output columns mirror what the route builders expect:
        sourceId | value | referenceTime | qualityCode | unit | name
        | shortName | lat | lon | county
    Returns empty DataFrame when the cache is missing or doesn't cover the
    requested combination — caller should fall back to live API.
    """
    df = _get_dataframe()
    if df is None or df.empty:
        return pd.DataFrame()
    sub = df[(df["metric"] == metric) & (df["period"] == period)].copy()
    if sub.empty:
        return sub

    if counties:
        wanted = {c for c in counties if c}
        if wanted:
            sub = sub[sub["county"].isin(wanted)].copy()

    if sub.empty:
        return sub

    sub = sub.rename(
        columns={
            "reference_time": "referenceTime",
            "quality_code": "qualityCode",
        }
    )
    sub["referenceTime"] = pd.to_datetime(sub["referenceTime"], errors="coerce", utc=True)
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    sub["lat"] = pd.to_numeric(sub["lat"], errors="coerce")
    sub["lon"] = pd.to_numeric(sub["lon"], errors="coerce")
    return sub.dropna(subset=["lat", "lon", "value"]).reset_index(drop=True)


def cache_status() -> dict[str, Any]:
    df = _get_dataframe()
    return {
        "fresh": _cache_is_fresh(),
        "ttl_seconds": _TTL_SECONDS,
        "rows": int(len(df)) if df is not None else 0,
        "metrics": sorted(df["metric"].unique().tolist()) if df is not None and not df.empty else [],
        "periods": sorted(df["period"].unique().tolist()) if df is not None and not df.empty else [],
        "last_error": _LAST_ERROR,
        "destination": (
            f"s3://{_s3_config()[0]}/{_s3_config()[1]}"
            if _s3_config() is not None
            else str(_LOCAL_CACHE_PATH)
        ),
    }


# ---------------------------------------------------------------------------
# Background warmup (boots cache when app starts on Render)
# ---------------------------------------------------------------------------

def _refresh_in_thread() -> None:
    global _REFRESH_IN_PROGRESS, _LAST_ERROR
    with _REFRESH_LOCK:
        if _REFRESH_IN_PROGRESS:
            return
        _REFRESH_IN_PROGRESS = True
    try:
        refresh_all()
    except Exception as exc:  # pragma: no cover - defensive
        _LAST_ERROR = f"{type(exc).__name__}: {exc}"
    finally:
        with _REFRESH_LOCK:
            _REFRESH_IN_PROGRESS = False


def start_warmup() -> None:
    """Schedule a build on app boot, then keep refreshing on a TTL cadence."""
    interval = max(900, _TTL_SECONDS)

    def _tick() -> None:
        try:
            if not _cache_is_fresh():
                _refresh_in_thread()
        finally:
            t = threading.Timer(interval, _tick)
            t.daemon = True
            t.start()

    boot = threading.Thread(target=_tick, daemon=True)
    boot.start()
