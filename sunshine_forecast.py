#!/usr/bin/env python3
"""
sunshine_forecast.py
====================
Estimerer forventet solskinn per stasjon ved å hente YR Locationforecast 2.0
(MET Norges offisielle prognose-API — samme kilde som yr.no).

Frost har IKKE et forecast-endepunkt; det er kun et historisk arkiv.
YR Locationforecast er den rette MET-kilden for fremtidsdata.

Metode
------
YR compact-endepunktet leverer time-for-time skydekke (cloud_area_fraction, %).
Vi estimerer solskinns-timer slik:

  For hver time t i ønsket intervall:
    1. Er solen over horisonten? (astronomisk kalkulator, ingen ekstern dep.)
    2. sky_fraction = cloud_area_fraction / 100  (0.0–1.0)
    3. sol_bidrag   = (1 - sky_fraction)
    4. summer over alle timer i intervallet

Modi
----
  next24h  – neste 24 timer fra nå
  next48h  – neste 48 timer fra nå
  next7d   – neste 7 dager (total sum)
"""

from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Konfigurasjon
# ---------------------------------------------------------------------------

YR_COMPACT_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"

# MET krever en identifiserende User-Agent. Oppdater til din kontakt-e-post.
# Ref: https://developer.yr.no/doc/TermsOfService/
USER_AGENT = "prisanalyse.no/1.0 (kontakt@prisanalyse.no)"

DEFAULT_TIMEOUT = 12
MAX_WORKERS = 20
RETRY_WAIT = 2.0

ForecastMode = str
FORECAST_MODES: set[str] = {"next24h", "next48h", "next7d"}

# Brukes av sunshine_map.py / make_map()
HEAT_CLIP_HOURS: dict[str, float] = {
    "next24h": 12.0,
    "next48h": 20.0,
    "next7d":  60.0,
}

TITLES: dict[str, str] = {
    "next24h": "Forventet solskinn – neste 24 timer",
    "next48h": "Forventet solskinn – neste 48 timer",
    "next7d":  "Forventet solskinn – neste 7 dager",
}


# ---------------------------------------------------------------------------
# Astronomisk dagslys (ingen ekstern avhengighet)
# ---------------------------------------------------------------------------

def _solar_elevation_deg(dt_utc: datetime, lat: float, lon: float) -> float:
    """
    Beregner solhøyde (grader) etter NOAA Solar Calculator (forenklet).
    Negativ = solen er under horisonten.
    """
    jd = dt_utc.timestamp() / 86400.0 + 2440587.5
    n = jd - 2451545.0

    L = (280.46646 + 0.9856474 * n) % 360.0
    g = math.radians((357.52911 + 0.98560028 * n) % 360.0)

    eq_center = 1.914602 * math.sin(g) + 0.019993 * math.sin(2 * g)
    sun_lon = L + eq_center

    sin_dec = math.sin(math.radians(23.439)) * math.sin(math.radians(sun_lon))
    dec = math.asin(sin_dec)

    y = math.tan(math.radians(23.439 / 2)) ** 2
    e = 0.016708634
    eq_time_min = 4 * math.degrees(
        y * math.sin(math.radians(2 * L))
        - 2 * e * math.sin(g)
        + 4 * e * y * math.sin(g) * math.cos(math.radians(2 * L))
        - 0.5 * y * y * math.sin(math.radians(4 * L))
        - 1.25 * e * e * math.sin(2 * g)
    )

    day_frac = (dt_utc.hour * 60 + dt_utc.minute + dt_utc.second / 60.0) / 1440.0
    solar_noon_frac = (720.0 - 4.0 * lon - eq_time_min) / 1440.0
    hour_angle = math.radians((day_frac - solar_noon_frac) * 360.0)

    lat_r = math.radians(lat)
    sin_el = (
        math.sin(lat_r) * math.sin(dec)
        + math.cos(lat_r) * math.cos(dec) * math.cos(hour_angle)
    )
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_el))))


def _sun_above_horizon(dt_utc: datetime, lat: float, lon: float) -> bool:
    return _solar_elevation_deg(dt_utc, lat, lon) > 0.0


# ---------------------------------------------------------------------------
# YR API
# ---------------------------------------------------------------------------

def _fetch_yr_timeseries(
    lat: float,
    lon: float,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[list[dict]]:
    """
    Henter timeseries fra YR Locationforecast 2.0 compact.
    Returnerer None ved vedvarende feil.
    """
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


# ---------------------------------------------------------------------------
# Estimering
# ---------------------------------------------------------------------------

def _estimate_sun_hours(
    timeseries: list[dict],
    lat: float,
    lon: float,
    start_utc: datetime,
    end_utc: datetime,
) -> float:
    """
    Summer estimert sol (timer) for alle YR-timesteps i [start_utc, end_utc).

    Bidrag per time:
      0.0                      hvis solen er under horisonten
      (1 - cloud_frac) * 1.0   ellers  (0.0 til 1.0 timer)
    """
    total = 0.0
    for item in timeseries:
        try:
            t = datetime.fromisoformat(item["time"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue

        if t < start_utc or t >= end_utc:
            continue

        if not _sun_above_horizon(t, lat, lon):
            continue

        details = (
            item.get("data", {})
                .get("instant", {})
                .get("details", {})
        )
        cloud_pct = details.get("cloud_area_fraction")
        if cloud_pct is None:
            continue

        cloud_frac = max(0.0, min(1.0, cloud_pct / 100.0))
        total += 1.0 - cloud_frac

    return round(total, 2)


# ---------------------------------------------------------------------------
# Offentlig API
# ---------------------------------------------------------------------------

def fetch_sunshine_forecast(
    stations: pd.DataFrame,
    mode: ForecastMode = "next24h",
    timeout: int = DEFAULT_TIMEOUT,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    """
    Hent YR-prognoser for alle stasjoner og estimer solskinns-timer.

    Parameters
    ----------
    stations    : DataFrame med kolonner [baseId, name, shortName, lat, lon]
    mode        : "next24h" | "next48h" | "next7d"
    timeout     : sekunder per HTTP-kall
    max_workers : parallelle kall

    Returns
    -------
    DataFrame med kolonner:
        sourceId, name, shortName, lat, lon,
        sun_hours, referenceTime, qualityCode
    (Samme format som Frost-stien — kan sendes direkte til make_map().)
    """
    if mode not in FORECAST_MODES:
        raise ValueError(f"Ukjent mode: {mode!r}. Gyldige: {sorted(FORECAST_MODES)}")

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hours_map = {"next24h": 24, "next48h": 48, "next7d": 168}
    start_utc = now
    end_utc = now + timedelta(hours=hours_map[mode])

    def _process(row: dict) -> Optional[dict]:
        lat = float(row["lat"])
        lon = float(row["lon"])
        base_id = str(row["baseId"])

        with requests.Session() as sess:
            ts = _fetch_yr_timeseries(lat, lon, sess, timeout=timeout)

        if ts is None:
            return None

        sun_h = _estimate_sun_hours(ts, lat, lon, start_utc, end_utc)
        return {
            "sourceId":      base_id,
            "name":          row.get("name") or base_id,
            "shortName":     row.get("shortName") or base_id,
            "lat":           lat,
            "lon":           lon,
            "sun_hours":     sun_h,
            "referenceTime": end_utc,
            "qualityCode":   float("nan"),
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
    df["sun_hours"] = pd.to_numeric(df["sun_hours"], errors="coerce")
    return df.dropna(subset=["lat", "lon", "sun_hours"])
