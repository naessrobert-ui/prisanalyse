#!/usr/bin/env python3
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

YR_COMPACT_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
USER_AGENT = "prisanalyse.no/1.0 (kontakt@prisanalyse.no)"

DEFAULT_TIMEOUT = 12
MAX_WORKERS = 20
RETRY_WAIT = 2.0

FORECAST_MODES: set[str] = {"next24h", "next48h", "next7d"}
HOURS_MAP: dict[str, int] = {"next24h": 24, "next48h": 48, "next7d": 168}

PRECIP_TITLES: dict[str, str] = {
    "next24h": "Forventet nedbør – neste 24 timer",
    "next48h": "Forventet nedbør – neste 48 timer",
    "next7d": "Forventet nedbør – neste 7 dager",
}

TEMP_TITLES: dict[str, dict[str, str]] = {
    "min": {
        "next24h": "Forventet minimumstemperatur – neste 24 timer",
        "next48h": "Forventet minimumstemperatur – neste 48 timer",
        "next7d": "Forventet minimumstemperatur – neste 7 dager",
    },
    "max": {
        "next24h": "Forventet maksimumstemperatur – neste 24 timer",
        "next48h": "Forventet maksimumstemperatur – neste 48 timer",
        "next7d": "Forventet maksimumstemperatur – neste 7 dager",
    },
    "mean": {
        "next24h": "Forventet middeltemperatur – neste 24 timer",
        "next48h": "Forventet middeltemperatur – neste 48 timer",
        "next7d": "Forventet middeltemperatur – neste 7 dager",
    },
}


def _fetch_yr_timeseries(
    lat: float,
    lon: float,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[list[dict]]:
    params = {"lat": round(lat, 4), "lon": round(lon, 4)}
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(3):
        try:
            r = session.get(
                YR_COMPACT_URL,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            if r.status_code == 200:
                return r.json().get("properties", {}).get("timeseries", [])
            if r.status_code == 429:
                time.sleep(RETRY_WAIT * (attempt + 1))
                continue
            if r.status_code >= 500:
                time.sleep(RETRY_WAIT)
                continue
            return None
        except requests.RequestException:
            time.sleep(1.0)

    return None


def _parse_time(item: dict) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(item["time"].replace("Z", "+00:00"))
    except (KeyError, ValueError, TypeError):
        return None


def _hourly_precip_triplet_mm(item: dict) -> tuple[float, float, float]:
    data = item.get("data", {})
    n1 = data.get("next_1_hours", {}).get("details", {})

    if "precipitation_amount" in n1:
        exp = float(n1.get("precipitation_amount") or 0.0)
        low = float(n1.get("precipitation_amount_min") or exp)
        high = float(n1.get("precipitation_amount_max") or exp)
        return exp, low, high

    inst = data.get("instant", {}).get("details", {})
    exp = float(inst.get("precipitation_amount") or 0.0)
    return exp, exp, exp


def _instant_temp_c(item: dict) -> Optional[float]:
    details = item.get("data", {}).get("instant", {}).get("details", {})
    val = details.get("air_temperature")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _window_bounds(mode: str) -> tuple[datetime, datetime]:
    if mode not in FORECAST_MODES:
        raise ValueError(f"Ukjent mode: {mode!r}. Gyldige: {sorted(FORECAST_MODES)}")
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = now + timedelta(hours=HOURS_MAP[mode])
    return now, end_utc


def fetch_precip_forecast(
    stations: pd.DataFrame,
    mode: str = "next24h",
    timeout: int = DEFAULT_TIMEOUT,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    start_utc, end_utc = _window_bounds(mode)

    def _process(row: dict) -> Optional[dict]:
        lat = float(row["lat"])
        lon = float(row["lon"])
        base_id = str(row["baseId"])

        with requests.Session() as sess:
            ts = _fetch_yr_timeseries(lat, lon, sess, timeout=timeout)
        if ts is None:
            return None

        total_expected = 0.0
        total_min = 0.0
        total_max = 0.0
        for item in ts:
            t = _parse_time(item)
            if t is None or t < start_utc or t >= end_utc:
                continue
            exp, low, high = _hourly_precip_triplet_mm(item)
            total_expected += exp
            total_min += low
            total_max += high

        return {
            "sourceId": base_id,
            "value": round(total_expected, 2),
            "value_min": round(total_min, 2),
            "value_max": round(total_max, 2),
            "referenceTime": end_utc,
            "qualityCode": float("nan"),
            "unit": "mm",
            "name": row.get("name") or base_id,
            "shortName": row.get("shortName") or base_id,
            "lat": lat,
            "lon": lon,
        }

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, row): row for row in stations.to_dict("records")}
        for fut in as_completed(futures):
            result = fut.result()
            if result is not None:
                rows.append(result)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["lat", "lon", "value"])


def fetch_temp_forecast(
    stations: pd.DataFrame,
    *,
    mode: str = "next24h",
    temp_kind: str = "min",
    timeout: int = DEFAULT_TIMEOUT,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    if temp_kind not in {"min", "max", "mean"}:
        temp_kind = "min"

    start_utc, end_utc = _window_bounds(mode)

    def _process(row: dict) -> Optional[dict]:
        lat = float(row["lat"])
        lon = float(row["lon"])
        base_id = str(row["baseId"])

        with requests.Session() as sess:
            ts = _fetch_yr_timeseries(lat, lon, sess, timeout=timeout)
        if ts is None:
            return None

        vals: list[float] = []
        for item in ts:
            t = _parse_time(item)
            if t is None or t < start_utc or t >= end_utc:
                continue
            tv = _instant_temp_c(item)
            if tv is not None:
                vals.append(tv)

        if not vals:
            return None

        if temp_kind == "max":
            val = max(vals)
        elif temp_kind == "mean":
            val = sum(vals) / len(vals)
        else:
            val = min(vals)

        return {
            "sourceId": base_id,
            "value": round(float(val), 2),
            "referenceTime": end_utc,
            "qualityCode": float("nan"),
            "unit": "degC",
            "name": row.get("name") or base_id,
            "shortName": row.get("shortName") or base_id,
            "lat": lat,
            "lon": lon,
        }

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, row): row for row in stations.to_dict("records")}
        for fut in as_completed(futures):
            result = fut.result()
            if result is not None:
                rows.append(result)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["lat", "lon", "value"])
