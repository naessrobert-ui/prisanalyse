#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
snow_map.py – raskt snøkart fra Frost (bbox-basert + latest som default).

Hovedidé:
- Frontend sender bbox (south,west,north,east).
- Backend henter stasjoner i bbox via /sources?geometry=POLYGON(...)
- Backend henter snødybde:
  - mode=latest: referencetime=latest&limit=1 (rask)
  - mode=day: kalenderdøgn + fallback ±window_days (kun for stasjoner i bbox)
- Returnerer folium-HTML for iframe.

Krever:
- miljøvariabel FROST_CLIENT_ID (evt FROST_CLIENT_SECRET)
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

import pandas as pd
import requests
import folium
from folium.plugins import HeatMap, MarkerCluster

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20
ELEMENT_SNOW = "surface_snow_thickness"  # cm, -1 = "ikke snø"

# Standard for kvalitet (du kan gjøre strengere om du vil)
DEFAULT_QUALITIES = "0,1,2,3,4"

# “Default” timeoffset/levels for å redusere duplikate tidsserier
DEFAULT_TIMEOFFSETS = "default"
DEFAULT_LEVELS = "default"

# Enkle region-bbox’er (south,west,north,east)
REGION_BBOX: dict[str, Tuple[float, float, float, float]] = {
    "south": (57.0, 4.0, 62.5, 12.5),
    "mid": (62.0, 4.0, 66.8, 18.0),
    "north": (66.5, 10.0, 71.5, 31.5),
    "all": (57.0, 4.0, 71.5, 31.5),
}

# ------------------------------
# Enkel TTL-cache i minne
# ------------------------------
@dataclass
class _CacheEntry:
    expires_at: float
    value: Any


_CACHE: dict[str, _CacheEntry] = {}


def _cache_get(key: str) -> Any:
    e = _CACHE.get(key)
    if not e:
        return None
    if time.time() > e.expires_at:
        _CACHE.pop(key, None)
        return None
    return e.value


def _cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    _CACHE[key] = _CacheEntry(expires_at=time.time() + ttl_seconds, value=value)


# ------------------------------
# Auth
# ------------------------------
@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett FROST_CLIENT_ID i miljøvariabler (evt .env).")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


# ------------------------------
# Frost HTTP med retries
# ------------------------------
def frost_get_json(
    session: requests.Session,
    path: str,
    params: Optional[dict[str, str | int]] = None,
    *,
    auth: FrostAuth,
    retries: int = 6,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    url = f"{FROST_BASE}{path}"
    backoff = 1.0

    for _ in range(retries):
        r = session.get(
            url,
            params=params or None,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 404:
            # kan være normalt (“No data found”)
            return r.json()

        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Frost-feil {r.status_code} {url}: {detail}")

    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk: {url}")


def iter_pages(
    session: requests.Session,
    path: str,
    params: dict[str, str | int],
    *,
    auth: FrostAuth,
    timeout: int,
) -> Iterator[dict[str, Any]]:
    """
    Frost v0 bruker offset/itemsPerPage/totalItemCount på mange endepunkter.
    """
    first = frost_get_json(session, path, params, auth=auth, timeout=timeout)
    yield first

    if first.get("@type") == "ErrorResponse":
        return

    try:
        total = int(first.get("totalItemCount", 0))
        offset = int(first.get("offset", 0))
        per_page = int(first.get("itemsPerPage", 0))
    except Exception:
        return

    if total <= 0 or per_page <= 0:
        return

    next_offset = offset + per_page
    while next_offset < total:
        p2 = dict(params)
        p2["offset"] = next_offset
        page = frost_get_json(session, path, p2, auth=auth, timeout=timeout)
        yield page

        if page.get("@type") == "ErrorResponse":
            return

        try:
            offset = int(page.get("offset", next_offset))
            per_page = int(page.get("itemsPerPage", per_page))
            total = int(page.get("totalItemCount", total))
        except Exception:
            return

        next_offset = offset + per_page


def chunked(xs: list[str], n: int) -> Iterator[list[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def base_source_id(source_id: str) -> str:
    return source_id.split(":")[0]


# ------------------------------
# BBOX helpers
# ------------------------------
def parse_bbox(bbox: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    if not bbox:
        return None
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
        if len(parts) != 4:
            return None
        south, west, north, east = parts
        if south > north or west > east:
            return None
        return south, west, north, east
    except Exception:
        return None


def bbox_to_wkt_polygon(b: Tuple[float, float, float, float]) -> str:
    south, west, north, east = b
    # WKT: lon lat
    return (
        f"POLYGON(({west} {south}, {east} {south}, {east} {north}, "
        f"{west} {north}, {west} {south}))"
    )


# ------------------------------
# Hent stasjoner i bbox
# ------------------------------
def list_sources_in_bbox(
    session: requests.Session,
    *,
    auth: FrostAuth,
    bbox: Tuple[float, float, float, float],
    timeout: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Bruk /sources med geometry-filter (WKT).
    Returnerer baseId + lat/lon + navn.
    """
    cache_key = f"sources_bbox:{bbox}"
    cached = _cache_get(cache_key)
    if isinstance(cached, pd.DataFrame):
        return cached.copy()

    path = "/sources/v0.jsonld"
    wkt = bbox_to_wkt_polygon(bbox)
    params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "geometry": wkt,
        "fields": "id,name,shortName,country,geometry",
        "limit": limit,
    }

    rows: list[dict[str, Any]] = []
    for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
        if page.get("@type") == "ErrorResponse":
            continue
        for item in page.get("data", []):
            geom = item.get("geometry") or {}
            coords = geom.get("coordinates")  # [lon, lat]
            lon = lat = None
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
            rows.append(
                {
                    "baseId": item.get("id"),
                    "name": item.get("name"),
                    "shortName": item.get("shortName"),
                    "country": item.get("country"),
                    "lat": lat,
                    "lon": lon,
                }
            )

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["baseId"]).reset_index(drop=True)

    # cache stasjoner litt lengre (de endrer seg sjelden)
    _cache_set(cache_key, df, ttl_seconds=6 * 3600)
    return df.copy()


# ------------------------------
# Observasjoner
# ------------------------------
def fetch_observations(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    referencetime: str,
    timeout: int,
    batch_size: int = 120,
    limit: int = 1000,
    qualities: str = DEFAULT_QUALITIES,
    timeoffsets: str = DEFAULT_TIMEOFFSETS,
    levels: str = DEFAULT_LEVELS,
) -> pd.DataFrame:
    """
    Hent snødybde (surface_snow_thickness) for en liste sources.
    Returnerer lang DF: sourceId, referenceTime, value, unit, qualityCode
    """
    if not sources:
        return pd.DataFrame()

    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    for batch in chunked(sources, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": ELEMENT_SNOW,
            "limit": limit,
            "timeoffsets": timeoffsets,
            "levels": levels,
        }
        if qualities:
            params["qualities"] = qualities

        for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                continue

            for item in page.get("data", []):
                sid = item.get("sourceId")
                item_rt = item.get("referenceTime")
                for obs in item.get("observations", []):
                    rows.append(
                        {
                            "sourceId": sid,
                            "referenceTime": obs.get("referenceTime") or item_rt,
                            "elementId": obs.get("elementId"),
                            "value": obs.get("value"),
                            "unit": obs.get("unit"),
                            "qualityCode": obs.get("qualityCode"),
                        }
                    )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["referenceTime"] = pd.to_datetime(df["referenceTime"], errors="coerce", utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["referenceTime", "value"])
    return df


def choose_latest_per_station(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d = (
        d.sort_values(["sourceId", "referenceTime"], ascending=[True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return d


def choose_nearest_per_station(df: pd.DataFrame, *, target_day: _date) -> pd.DataFrame:
    if df.empty:
        return df

    target_dt = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    target_ts = pd.Timestamp(target_dt)

    d = df.copy()
    d["abs_diff_s"] = (d["referenceTime"] - target_ts).abs().dt.total_seconds()
    d = (
        d.sort_values(["sourceId", "abs_diff_s", "referenceTime"], ascending=[True, True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    d["diff_hours"] = (d["referenceTime"] - target_ts).dt.total_seconds() / 3600.0
    return d.drop(columns=["abs_diff_s"])


# ------------------------------
# Folium map
# ------------------------------
def _color_cm(cm: float) -> str:
    if cm >= 150:
        return "darkblue"
    if cm >= 75:
        return "blue"
    if cm >= 30:
        return "cadetblue"
    if cm >= 10:
        return "green"
    if cm >= 1:
        return "orange"
    return "lightgray"


def make_map(
    df: pd.DataFrame,
    *,
    out_html: Optional[str] = None,
    cluster: bool = True,
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 200.0,
) -> str:
    if df.empty:
        raise RuntimeError("Ingen data å plotte (df er tom).")

    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])

    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")

    clipped = d["value"].clip(lower=0, upper=heat_clip_cm)
    weights = (clipped / heat_clip_cm) ** 0.5
    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]

    heat_layer = folium.FeatureGroup(name="Heatmap snødybde", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=8).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    for _, r in d.iterrows():
        cm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or "cm"
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"

        diff_part = ""
        if "diff_hours" in d.columns and pd.notna(r.get("diff_hours")):
            diff_part = f"<br>Avvik: {float(r['diff_hours']):+.1f} timer"

        html = f"{name}<br>Snødybde: <b>{cm:.0f} {unit}</b><br>Tid: {t_str}{diff_part}"

        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=5,
            color=_color_cm(cm),
            fill=True,
            fill_opacity=0.85,
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=320),
        ).add_to(layer_for_markers)

    folium.LayerControl().add_to(m)

    if out_html:
        m.save(out_html)

    return m.get_root().render()


# ------------------------------
# Builder: latest / day
# ------------------------------
def build_snow_df_latest(
    *,
    bbox_coords: Tuple[float, float, float, float],
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 160,
    qualities: str = DEFAULT_QUALITIES,
    debug_timing: bool = False,
) -> tuple[pd.DataFrame, _date]:
    """
    Rask: siste tilgjengelige snødybde per stasjon i bbox.
    Bruker referencetime=latest&limit=1 (via iter_pages+limit=1).
    """
    auth = _env_auth()
    t0 = time.perf_counter()

    with requests.Session() as sess:
        meta = list_sources_in_bbox(sess, auth=auth, bbox=bbox_coords, timeout=timeout)
        if debug_timing:
            print(f"[snow] sources_in_bbox: {time.perf_counter() - t0:.2f}s ({len(meta)})")
            t0 = time.perf_counter()

        if meta.empty:
            raise RuntimeError("Fant ingen stasjoner i valgt bbox.")

        base_ids = meta["baseId"].astype(str).tolist()

        # referencetime=latest + limit=1 gjør dette veldig lett
        obs = fetch_observations(
            sess,
            auth=auth,
            sources=base_ids,
            referencetime="latest",
            timeout=timeout,
            batch_size=batch_size,
            limit=1,
            qualities=qualities,
        )
        if debug_timing:
            print(f"[snow] observations_latest: {time.perf_counter() - t0:.2f}s ({len(obs)})")

    if obs.empty:
        raise RuntimeError("Ingen 'latest' snøobservasjoner i området.")

    latest = choose_latest_per_station(obs)

    # koble obs.sourceId (SNxxxx:0) mot meta.baseId (SNxxxx)
    latest["baseId"] = latest["sourceId"].astype(str).map(base_source_id)
    df = latest.merge(meta, on="baseId", how="left").drop(columns=["baseId"])
    return df, datetime.now(timezone.utc).date()


def build_snow_df_for_day(
    *,
    date_str: Optional[str],
    bbox_coords: Tuple[float, float, float, float],
    window_days: int = 2,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 120,
    qualities: str = DEFAULT_QUALITIES,
    debug_timing: bool = False,
) -> tuple[pd.DataFrame, _date]:
    """
    Dag-modus: kalenderdøgn for valgt dato, med fallback ±window_days.
    Alt begrenses til stasjoner i bbox (ingen expensive Norge-scan).
    """
    auth = _env_auth()
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        day = _date.today()

    day_rt = f"{day.isoformat()}/{(day + timedelta(days=1)).isoformat()}"

    t0 = time.perf_counter()
    with requests.Session() as sess:
        meta = list_sources_in_bbox(sess, auth=auth, bbox=bbox_coords, timeout=timeout)
        if debug_timing:
            print(f"[snow] sources_in_bbox: {time.perf_counter() - t0:.2f}s ({len(meta)})")
            t0 = time.perf_counter()

        if meta.empty:
            raise RuntimeError("Fant ingen stasjoner i valgt bbox.")

        base_ids = meta["baseId"].astype(str).tolist()

        df_day = fetch_observations(
            sess,
            auth=auth,
            sources=base_ids,
            referencetime=day_rt,
            timeout=timeout,
            batch_size=batch_size,
            limit=1000,
            qualities=qualities,
        )
        if debug_timing:
            print(f"[snow] observations_day: {time.perf_counter() - t0:.2f}s ({len(df_day)})")
            t0 = time.perf_counter()

        df_fb = pd.DataFrame()
        if window_days > 0:
            have = set(df_day["sourceId"].unique()) if not df_day.empty else set()
            # Finn hvilke baseIds vi mangler (obs har SNxxxx:0 osv)
            # Vi tar “missing” som baseIds som ikke har noen obs
            have_base = {base_source_id(s) for s in have}
            missing_base = [bid for bid in base_ids if bid not in have_base]

            if missing_base:
                fb_start = day - timedelta(days=window_days)
                fb_end = day + timedelta(days=window_days + 1)
                fb_rt = f"{fb_start.isoformat()}/{fb_end.isoformat()}"

                df_fb = fetch_observations(
                    sess,
                    auth=auth,
                    sources=missing_base,
                    referencetime=fb_rt,
                    timeout=timeout,
                    batch_size=batch_size,
                    limit=1000,
                    qualities=qualities,
                )
                if debug_timing:
                    print(f"[snow] observations_fallback: {time.perf_counter() - t0:.2f}s ({len(df_fb)})")

    df_all = pd.concat([df_day, df_fb], ignore_index=True)
    if df_all.empty:
        raise RuntimeError("Ingen observasjoner funnet for valgt dag (eller fallback).")

    nearest = choose_nearest_per_station(df_all, target_day=day)
    nearest["baseId"] = nearest["sourceId"].astype(str).map(base_source_id)
    df = nearest.merge(meta, on="baseId", how="left").drop(columns=["baseId"])
    return df, day


# ------------------------------
# Public: HTML builder
# ------------------------------
def build_snow_map_html(
    date_str: Optional[str] = None,
    *,
    mode: str = "latest",  # default: raskest
    bbox: Optional[str] = None,
    region: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    window_days: int = 2,
    timeout: int = DEFAULT_TIMEOUT,
    qualities: str = DEFAULT_QUALITIES,
    cluster: bool = True,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 200.0,
    cache_ttl_seconds: int = 60,
    debug_timing: bool = False,
) -> str:
    """
    Bygger HTML for snøkart. Bruk i iframe.
    - Hvis region er satt og bbox ikke er satt: bruk region-bbox.
    - Hvis bbox mangler helt: bruk region=south som fallback.
    """
    _ = (z, clat, clon)  # beholdes for kompatibilitet/URL-lagring

    if not bbox:
        if region and region in REGION_BBOX:
            bbox_coords = REGION_BBOX[region]
        else:
            bbox_coords = REGION_BBOX["south"]
        bbox = ",".join(str(x) for x in bbox_coords)
    else:
        bbox_coords = parse_bbox(bbox) or REGION_BBOX["south"]

    cache_key = f"snow_html:{mode}:{date_str}:{bbox}:{window_days}:{qualities}:{cluster}:{show_heatmap}:{heat_radius}:{heat_blur}:{heat_clip_cm}"
    cached = _cache_get(cache_key)
    if isinstance(cached, str):
        return cached

    if mode == "day":
        df, day = build_snow_df_for_day(
            date_str=date_str,
            bbox_coords=bbox_coords,
            window_days=window_days,
            timeout=timeout,
            qualities=qualities,
            debug_timing=debug_timing,
        )
        _ = day
    else:
        df, day = build_snow_df_latest(
            bbox_coords=bbox_coords,
            timeout=timeout,
            qualities=qualities,
            debug_timing=debug_timing,
        )
        _ = day

    html = make_map(
        df,
        out_html=None,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_cm=heat_clip_cm,
    )

    _cache_set(cache_key, html, ttl_seconds=cache_ttl_seconds)
    return html
