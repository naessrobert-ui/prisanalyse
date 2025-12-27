#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Literal

import pandas as pd
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# --- .env loading -------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

# Solskinn: sum av "duration_of_sunshine" per time
# (Frost returnerer typisk minutter per time for dette elementet; vi summerer og konverterer til timer.)
ELEMENT_SUN_HOURLY = "sum(duration_of_sunshine PT1H)"

UNIT_MIN = "min"
Mode = Literal["last24h", "day", "mtd", "ytd"]

# -----------------------
# Defaults mot 502/timeout
# -----------------------
MAX_SOURCES_PRE_OBS = 1200
MAX_AREA_BY_MODE: dict[str, float] = {
    "last24h": 20.0,
    "day": 12.0,
    "mtd": 8.0,
    "ytd": 6.0,
}


# ======================================================================
# Auth / Frost helpers
# ======================================================================

@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env).")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(
    session: requests.Session,
    path: str,
    params: Optional[dict[str, str | int]] = None,
    *,
    auth: FrostAuth,
    retries: int = 6,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    GET med retries (429/5xx).

    VIKTIG:
    - 404 og 412 returneres som JSON (ErrorResponse) og håndteres høyere opp.
      412 brukes bl.a. når ingen tidsserie finnes for kombinasjonen parametere.
    """
    url = f"{FROST_BASE}{path}"
    backoff = 1.0

    for _attempt in range(1, retries + 1):
        r = session.get(
            url,
            params=params or None,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )

        if r.status_code == 200:
            return r.json()

        # ✅ Behandle som "tomt" (ErrorResponse), ikke hard crash
        if r.status_code in (404, 412):
            return r.json()

        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Frost-feil {r.status_code} for {url}: {detail}")

    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk for {url}")


def iter_pages(
    session: requests.Session,
    path: str,
    params: dict[str, str | int],
    *,
    auth: FrostAuth,
    timeout: int,
) -> Iterator[dict[str, Any]]:
    """Paginering via offset/itemsPerPage/totalItemCount."""
    p = params.copy()
    first = frost_get_json(session, path, p, auth=auth, timeout=timeout)
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
        p2 = params.copy()
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


# ======================================================================
# BBOX + Sources
# ======================================================================

def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox må være 'west,south,east,north'")
    w, s, e, n = map(float, parts)
    if not (-180 <= w <= 180 and -180 <= e <= 180 and -90 <= s <= 90 and -90 <= n <= 90):
        raise ValueError("bbox-koordinater utenfor gyldig område")
    if e <= w or n <= s:
        raise ValueError("bbox ugyldig: east<=west eller north<=south")
    return w, s, e, n


def _bbox_polygon_wkt(w: float, s: float, e: float, n: float) -> str:
    return f"POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))"


def fetch_sources_in_bbox(
    session: requests.Session,
    *,
    auth: FrostAuth,
    w: float,
    s: float,
    e: float,
    n: float,
    timeout: int,
) -> pd.DataFrame:
    """
    Hent stasjoner i bbox via /sources geometry-filter.
    Returnerer DF med baseId, name, shortName, lat, lon.
    """
    path = "/sources/v0.jsonld"
    poly = _bbox_polygon_wkt(w, s, e, n)

    params: dict[str, str | int] = {
        "country": "NO",
        "geometry": poly,
        "fields": "id,name,shortName,country,geometry",
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
    if df.empty:
        return df
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    return df.dropna(subset=["baseId", "lat", "lon"])


def pre_sample_sources_grid(
    src_meta: pd.DataFrame,
    *,
    w: float,
    s: float,
    e: float,
    n: float,
    max_sources: int = MAX_SOURCES_PRE_OBS,
) -> pd.DataFrame:
    """
    Billig pre-sampling: én stasjon per gridcelle før observations.
    """
    if src_meta.empty or len(src_meta) <= max_sources:
        return src_meta

    df = src_meta.copy()

    width = max(e - w, 1e-9)
    height = max(n - s, 1e-9)
    target_side = max(int(max_sources ** 0.5), 10)

    cell_lon = max(width / target_side, 0.02)
    cell_lat = max(height / target_side, 0.02)

    df["ix"] = ((df["lon"] - w) / cell_lon).astype(int)
    df["iy"] = ((df["lat"] - s) / cell_lat).astype(int)

    df2 = df.groupby(["iy", "ix"], as_index=False).head(1).reset_index(drop=True)

    if len(df2) > max_sources:
        df2 = df2.head(max_sources)

    return df2.drop(columns=["ix", "iy"], errors="ignore")


# ======================================================================
# Available time series filter (viktig for solskinn)
# ======================================================================

def filter_sources_with_timeseries(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    referencetime: str,
    elements: str,
    timeout: int,
    batch_size: int = 250,
    qualities: str = "0,1,2,3,4",
) -> list[str]:
    """
    Bruk /observations/availableTimeSeries for å beholde kun sources som faktisk
    har tidsserie for (elements + referencetime). Dette hindrer 412/“ingen tidsserie”.
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    keep: set[str] = set()

    for batch in chunked(sources, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": elements,
            "timeoffsets": "default",
            "levels": "default",
        }
        if qualities:
            params["qualities"] = qualities

        js = frost_get_json(session, path, params, auth=auth, timeout=timeout)
        if js.get("@type") == "ErrorResponse":
            continue

        for item in js.get("data", []):
            sid = item.get("sourceId")
            if not sid:
                continue
            keep.add(base_source_id(str(sid)))

    # behold rekkefølge
    return [s for s in sources if base_source_id(s) in keep]


# ======================================================================
# Observasjoner
# ======================================================================

def fetch_observations_interval(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    referencetime: str,
    elements: str,
    timeout: int,
    batch_size: int,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
) -> pd.DataFrame:
    """
    Hent observasjoner i et intervall for mange stasjoner.
    Returnerer lang DF.
    """
    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    for batch in chunked(sources, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": elements,
            "timeoffsets": "default",
            "levels": "default",
            "limit": limit,
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
    df["qualityCode"] = pd.to_numeric(df["qualityCode"], errors="coerce")
    return df.dropna(subset=["referenceTime", "value"])


def aggregate_sun_per_station(df: pd.DataFrame, *, count_col: str) -> pd.DataFrame:
    """
    Summerer solskinn (minutter) over intervallet per stasjon.
    Lager også sun_hours = minutter/60.
    """
    if df.empty:
        return df
    out = (
        df.groupby("sourceId", as_index=False)
        .agg(value=("value", "sum"), n=("value", "size"), rt_max=("referenceTime", "max"), qmin=("qualityCode", "min"))
        .reset_index(drop=True)
    )
    out.rename(columns={"n": count_col}, inplace=True)
    out["unit"] = UNIT_MIN
    out["referenceTime"] = out["rt_max"]
    out["qualityCode"] = out["qmin"]
    out.drop(columns=["rt_max", "qmin"], inplace=True)

    out["sun_hours"] = pd.to_numeric(out["value"], errors="coerce") / 60.0
    return out


# ======================================================================
# Downsample (kun hvis for mange) - etter data
# ======================================================================

def downsample_spatial_best_quality(
    df: pd.DataFrame,
    *,
    w: float,
    s: float,
    e: float,
    n: float,
    max_points: int = 1200,
    keep_top_n: int = 10,
    value_col: str = "sun_hours",
) -> pd.DataFrame:
    """
    Hvis df <= max_points: behold alt.
    Ellers: 1 pr gridcelle, prioritert på:
      - lavest qualityCode (best)
      - nyeste referenceTime
    + behold topp keep_top_n høyeste value_col.
    """
    if df.empty or len(df) <= max_points:
        return df

    d = df.copy()
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce").fillna(999999)

    width = max(e - w, 1e-9)
    height = max(n - s, 1e-9)
    target_side = max(int(max_points ** 0.5), 10)

    cell_lon = max(width / target_side, 0.02)
    cell_lat = max(height / target_side, 0.02)

    d["ix"] = ((d["lon"] - w) / cell_lon).astype(int)
    d["iy"] = ((d["lat"] - s) / cell_lat).astype(int)

    d_sorted = d.sort_values(["iy", "ix", "qualityCode", "referenceTime"], ascending=[True, True, True, False])
    grid_pick = d_sorted.groupby(["iy", "ix"], as_index=False).head(1).reset_index(drop=True)

    top_pick = d.sort_values(value_col, ascending=False).head(keep_top_n)

    out = pd.concat([grid_pick, top_pick], ignore_index=True).drop_duplicates(subset=["sourceId"], keep="first")

    if len(out) > max_points:
        out = out.sort_values(["qualityCode", "referenceTime"], ascending=[True, False]).head(max_points)

    return out.reset_index(drop=True)


# ======================================================================
# UI helpers
# ======================================================================

def _loading_overlay_js() -> str:
    return """
<div id="loadingOverlay" style="
  display:none;
  position: fixed; inset: 0; z-index: 10000;
  background: rgba(15, 23, 42, 0.55);
  backdrop-filter: blur(2px);
  font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
">
  <div style="
    position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
    background: rgba(255,255,255,0.97); border-radius: 16px;
    padding: 16px 18px; min-width: 280px;
    box-shadow: 0 18px 45px rgba(15,23,42,.25);
  ">
    <div style="font-weight:900; margin-bottom:6px;">Henter data…</div>
    <div style="color:#334155; font-size:13px;">
      Dette kan ta litt tid for store perioder/områder.
    </div>
    <div style="margin-top:10px; font-size:14px;">
      Tid: <b><span id="loadingSeconds">0</span>s</b>
    </div>
  </div>
</div>

<script>
  window._loadingTimer = null;
  window.showLoading = function() {
    const ov = document.getElementById('loadingOverlay');
    const s = document.getElementById('loadingSeconds');
    if (!ov || !s) return;
    ov.style.display = 'block';
    let t = 0;
    s.textContent = '0';
    if (window._loadingTimer) clearInterval(window._loadingTimer);
    window._loadingTimer = setInterval(function() {
      t += 1;
      s.textContent = String(t);
    }, 1000);
  }
</script>
"""


def _save_view_listener_js() -> str:
    # Del samme key med nedbør for å beholde samme utsnitt når du bytter kart
    return """
<script>
  const STORE_KEY = "precip_view_v1";

  function findLeafletMap() {
    for (const k of Object.keys(window)) {
      const v = window[k];
      if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function' && typeof v.getZoom === 'function') {
        return v;
      }
    }
    return null;
  }

  function saveViewToParent() {
    try {
      const map = findLeafletMap();
      if (!map) return;
      const b = map.getBounds();
      const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(",");
      const c = map.getCenter();
      const z = map.getZoom();

      if (window.parent && window.parent.sessionStorage) {
        window.parent.sessionStorage.setItem(
          STORE_KEY,
          JSON.stringify({ bbox: bbox, z: String(z), clat: String(c.lat), clon: String(c.lng) })
        );
      }
    } catch (e) {}
  }

  (function attachViewListener() {
    const map = findLeafletMap();
    if (!map) return;
    map.on("moveend", saveViewToParent);
    map.on("zoomend", saveViewToParent);
    saveViewToParent();
  })();
</script>
"""


def make_empty_map_with_button(*, title: str, mode: str, date_str: str) -> str:
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    ui = f"""
    {_loading_overlay_js()}
    {_save_view_listener_js()}

    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 360px;
    ">
      <div style="font-weight:800; margin-bottom:6px;">{title}</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Zoom/pan til ønsket område og trykk hent.
      </div>
      <button id="bboxFetchBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#2563eb; color:white; cursor:pointer;
      ">Hent data for synlig område</button>
    </div>

    <script>
      function findLeafletMap2() {{
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function') {{
            return v;
          }}
        }}
        return null;
      }}

      document.getElementById('bboxFetchBtn').addEventListener('click', function() {{
        const map = findLeafletMap2();
        if (!map) {{
          alert('Fant ikke kart-objektet. Prøv å reloade siden.');
          return;
        }}
        const b = map.getBounds();
        const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(',');

        const c = map.getCenter();
        const z = map.getZoom();

        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        qs.set('bbox', bbox);
        qs.set('z', String(z));
        qs.set('clat', String(c.lat));
        qs.set('clon', String(c.lng));

        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/solskinn-kart?' + qs.toString();
      }});
    </script>
    """
    folium.Element(ui).add_to(m.get_root().html)
    return m.get_root().render()


def make_info_map(
    *,
    title: str,
    message: str,
    mode: str,
    date_str: str,
    center: Optional[tuple[float, float]] = None,
    zoom: Optional[int] = None,
) -> str:
    if center and zoom is not None:
        m = folium.Map(location=[center[0], center[1]], zoom_start=int(zoom), tiles="OpenStreetMap")
    else:
        m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    ui = f"""
    {_loading_overlay_js()}
    {_save_view_listener_js()}

    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 420px;
    ">
      <div style="font-weight:900; margin-bottom:6px;">{title}</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        {message}
      </div>
      <div style="font-size:12px; color:#64748b; margin-bottom:10px;">
        Tips: Zoom inn litt og trykk “Oppdater dette utsnittet”.
      </div>
      <button id="refreshBBoxBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer;
      ">Oppdater dette utsnittet</button>
    </div>

    <script>
      function findLeafletMap2() {{
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function') {{
            return v;
          }}
        }}
        return null;
      }}

      document.getElementById('refreshBBoxBtn').addEventListener('click', function() {{
        const map = findLeafletMap2();
        if (!map) {{
          alert('Fant ikke kart-objektet. Prøv å reloade siden.');
          return;
        }}
        const b = map.getBounds();
        const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(',');

        const c = map.getCenter();
        const z = map.getZoom();

        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        qs.set('bbox', bbox);
        qs.set('z', String(z));
        qs.set('clat', String(c.lat));
        qs.set('clon', String(c.lng));

        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/solskinn-kart?' + qs.toString();
      }});
    </script>
    """
    folium.Element(ui).add_to(m.get_root().html)
    return m.get_root().render()


# ======================================================================
# Kart med data
# ======================================================================

def make_map(
    df: pd.DataFrame,
    *,
    title: str,
    out_html: Optional[str] = None,
    cluster: bool = True,
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_hours: float = 12.0,
    bounds: Optional[tuple[float, float, float, float]] = None,  # (w,s,e,n)
    center: Optional[tuple[float, float]] = None,                # (lat,lon)
    zoom: Optional[int] = None,
    top_n: int = 10,
    mode: str = "last24h",
    date_str: str = "",
) -> str:
    if df.empty:
        raise RuntimeError("Ingen data å plotte (df er tom).")

    d = df.dropna(subset=["lat", "lon", "sun_hours"]).copy()
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["sun_hours"] = pd.to_numeric(d["sun_hours"], errors="coerce")
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    d = d.dropna(subset=["lat", "lon", "sun_hours"])

    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    vals = d["sun_hours"].astype(float)
    q10 = float(vals.quantile(0.10))
    q20 = float(vals.quantile(0.20))
    q80 = float(vals.quantile(0.80))
    q90 = float(vals.quantile(0.90))

    def color_for(hours: float) -> str:
        # Mye sol = varme farger, lite sol = blått
        if hours >= q90:
            return "#ffb703"
        if hours >= q80:
            return "#f77f00"
        if hours <= q10:
            return "#1d4ed8"
        if hours <= q20:
            return "#2563eb"
        return "#808080"

    def radius_for(hours: float) -> float:
        r = 3.0 + 6.0 * math.sqrt(max(hours, 0.0))
        return float(max(3.0, min(r, 22.0)))

    if center and zoom is not None:
        m = folium.Map(location=[center[0], center[1]], zoom_start=int(zoom), tiles="OpenStreetMap")
    else:
        center_lat = float(d["lat"].mean())
        center_lon = float(d["lon"].mean())
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")
        if bounds:
            w, s, e, n = bounds
            m.fit_bounds([[s, w], [n, e]])

    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)
    folium.Element(_save_view_listener_js()).add_to(m.get_root().html)

    refresh_box = f"""
    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 360px;
    ">
      <div style="font-weight:900; margin-bottom:6px;">Oppdater</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Hent nye tall/stasjoner for synlig utsnitt.
      </div>
      <button id="refreshBBoxBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer;
      ">Oppdater dette utsnittet</button>
    </div>

    <script>
      function findLeafletMap3() {{
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function') {{
            return v;
          }}
        }}
        return null;
      }}

      document.getElementById('refreshBBoxBtn').addEventListener('click', function() {{
        const map = findLeafletMap3();
        if (!map) {{
          alert('Fant ikke kart-objektet. Prøv å reloade siden.');
          return;
        }}
        const b = map.getBounds();
        const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(',');

        const c = map.getCenter();
        const z = map.getZoom();

        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        qs.set('bbox', bbox);
        qs.set('z', String(z));
        qs.set('clat', String(c.lat));
        qs.set('clon', String(c.lng));

        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/solskinn-kart?' + qs.toString();
      }});
    </script>
    """
    folium.Element(refresh_box).add_to(m.get_root().html)

    legend_html = """
    <div style="
      position: fixed; top: 120px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 360px;
      font-size: 13px; color:#0f172a;
    ">
      <div style="font-weight:900; margin-bottom:6px;">Forklaring</div>
      <div style="margin-bottom:6px;">Farge = percentiler i utsnittet</div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#ffb703;"></span>
        <span>Topp 10% (mest sol)</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#f77f00;"></span>
        <span>80–90%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#808080;"></span>
        <span>Midten</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#2563eb;"></span>
        <span>10–20%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#1d4ed8;"></span>
        <span>Bunn 10% (minst sol)</span>
      </div>
      <div style="margin-top:8px; color:#334155;">
        Radius ~ √(timer)
      </div>
    </div>
    """
    folium.Element(legend_html).add_to(m.get_root().html)

    clipped = d["sun_hours"].clip(lower=0, upper=heat_clip_hours)
    weights = (clipped / heat_clip_hours) ** 0.5
    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name=f"Heatmap – {title}", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    marker_map: dict[str, str] = {}

    for _, r in d.iterrows():
        hours = float(r["sun_hours"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"

        qc = r.get("qualityCode")
        qc_str = f"{int(qc)}" if pd.notna(qc) else "ukjent"

        html = f"{name}<br>Solskinn: <b>{hours:.2f} t</b><br>Tid: {t_str}<br>Kvalitet: {qc_str}"

        col = color_for(hours)
        rad = radius_for(hours)

        marker = folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=rad,
            color=col,
            fill=True,
            fill_color=col,
            fill_opacity=0.85,
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=360),
        ).add_to(layer_for_markers)

        sid = str(r["sourceId"])
        marker_map[sid] = marker.get_name()

    folium.LayerControl().add_to(m)

    top = d.sort_values("sun_hours", ascending=False).head(int(top_n)).copy()
    rows_html: list[str] = []
    for i, r in enumerate(top.itertuples(index=False), start=1):
        sid = str(getattr(r, "sourceId"))
        nm = getattr(r, "name", None) or getattr(r, "shortName", None) or sid
        hours = float(getattr(r, "sun_hours"))
        rows_html.append(
            f"""
            <tr>
              <td style="padding:6px 8px; color:#64748b;">{i}</td>
              <td style="padding:6px 8px;">
                <a href="javascript:void(0)" onclick="focusStation('{sid}')" style="color:#2563eb;text-decoration:none;">
                  {nm}
                </a>
                <div style="font-size:12px;color:#64748b;">{sid}</div>
              </td>
              <td style="padding:6px 8px; text-align:right; font-weight:900;">{hours:.2f} t</td>
            </tr>
            """
        )

    topbox_html = f"""
    <div style="
      position: fixed; bottom: 12px; left: 12px; z-index: 9999;
      background: rgba(255,255,255,.97); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 520px;
    ">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="font-weight:900;">Topp {int(top_n)} i utsnittet</div>
        <button onclick="toggleToplist()" style="border:none;background:#e2e8f0;border-radius:999px;padding:6px 10px;cursor:pointer;">
          Vis/skjul
        </button>
      </div>
      <div id="toplistBody" style="margin-top:8px; max-height: 240px; overflow:auto;">
        <table style="border-collapse:collapse; width:100%;">
          <thead>
            <tr>
              <th style="text-align:left; padding:6px 8px; color:#64748b; font-size:12px;">#</th>
              <th style="text-align:left; padding:6px 8px; color:#64748b; font-size:12px;">Stasjon</th>
              <th style="text-align:right; padding:6px 8px; color:#64748b; font-size:12px;">timer</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
      </div>
    </div>
    """
    folium.Element(topbox_html).add_to(m.get_root().html)

    map_var = m.get_name()
    mapping_lines = ",\n".join([f'"{sid}": {jsname}' for sid, jsname in marker_map.items()])

    js = f"""
    <script>
      function findLeafletMap() {{
        if (typeof {map_var} !== 'undefined') return {map_var};
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.setView === 'function' && typeof v.getCenter === 'function') return v;
        }}
        return null;
      }}

      window._sunMarkers = {{
        {mapping_lines}
      }};

      function focusStation(sourceId) {{
        const map = findLeafletMap();
        const mk = window._sunMarkers[sourceId];
        if (!map || !mk) return;

        const ll = mk.getLatLng();
        const z = Math.max(map.getZoom(), 10);
        map.setView(ll, z, {{animate:true}});
        if (typeof mk.openPopup === 'function') {{
          mk.openPopup();
        }}
      }}

      function toggleToplist() {{
        const el = document.getElementById("toplistBody");
        if (!el) return;
        el.style.display = (el.style.display === "none") ? "block" : "none";
      }}
    </script>
    """
    folium.Element(js).add_to(m.get_root().html)

    html_str = m.get_root().render()
    if out_html:
        m.save(out_html)
    return html_str


# ======================================================================
# Hoved: bygg HTML for solskinn (tomt kart eller bbox)
# ======================================================================

def build_sunshine_map_html(
    date_str: Optional[str] = None,
    *,
    mode: Mode = "day",
    bbox: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
    cluster: bool = True,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_hours: float = 12.0,
) -> str:
    day_str = date_str or _date.today().isoformat()

    center: Optional[tuple[float, float]] = None
    zoom: Optional[int] = None
    try:
        if clat is not None and clon is not None:
            center = (float(clat), float(clon))
        if z is not None:
            zoom = int(float(z))
    except Exception:
        center = None
        zoom = None

    if not bbox:
        title = "Solskinn"
        if mode == "last24h":
            title = "Solskinn siste 24 timer (rullerende)"
        elif mode == "day":
            title = "Solskinn kalenderdøgn (valgt dato)"
        elif mode == "mtd":
            title = "Solskinn hittil i måneden"
        elif mode == "ytd":
            title = "Solskinn hittil i året"
        return make_empty_map_with_button(title=title, mode=str(mode), date_str=day_str)

    w, s, e, n = _parse_bbox(bbox)

    area = (e - w) * (n - s)
    max_area = MAX_AREA_BY_MODE.get(str(mode), 8.0)
    if area > max_area:
        return make_info_map(
            title="Området er for stort",
            message=f"Valgt område er for stort for periode '{mode}'. Zoom litt mer inn og prøv igjen.",
            mode=str(mode),
            date_str=day_str,
            center=center,
            zoom=zoom,
        )

    auth = _env_auth()
    day = datetime.strptime(day_str, "%Y-%m-%d").date()

    if mode == "last24h":
        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(hours=24)
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        title = "Solskinn siste 24 timer (rullerende)"
        sum_count_col = "n_hours"
    elif mode == "day":
        start = day
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = "Solskinn kalenderdøgn (valgt dato)"
        sum_count_col = "n_hours"
    elif mode == "mtd":
        start = _date(day.year, day.month, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = f"Solskinn hittil i måneden ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_hours"
    elif mode == "ytd":
        start = _date(day.year, 1, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = f"Solskinn hittil i året ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_hours"
    else:
        raise ValueError(f"Ukjent mode: {mode}")

    elements = ELEMENT_SUN_HOURLY

    with requests.Session() as sess:
        src_meta = fetch_sources_in_bbox(sess, auth=auth, w=w, s=s, e=e, n=n, timeout=timeout)
        if src_meta.empty:
            return make_info_map(
                title="Ingen stasjoner i området",
                message="Zoom litt ut eller flytt utsnittet.",
                mode=str(mode),
                date_str=day_str,
                center=center,
                zoom=zoom,
            )

        src_meta = pre_sample_sources_grid(src_meta, w=w, s=s, e=e, n=n, max_sources=MAX_SOURCES_PRE_OBS)
        sources = src_meta["baseId"].astype(str).tolist()

        # ✅ Viktig for solskinn: behold bare stasjoner som har tidsserie
        sources = filter_sources_with_timeseries(
            sess,
            auth=auth,
            sources=sources,
            referencetime=referencetime,
            elements=elements,
            timeout=timeout,
            batch_size=250,
            qualities=qualities,
        )

        if not sources:
            return make_info_map(
                title="Ingen solskinn-stasjoner i området",
                message="Utsnittet ser ut til å mangle stasjoner med solskinnsensor for perioden. Zoom ut/flytt utsnittet og prøv igjen.",
                mode=str(mode),
                date_str=day_str,
                center=center,
                zoom=zoom,
            )
        print(f"Antall stasjoner med solskinn-tidsserie i området: {len(sources)}")
        print("Eksempelstasjoner:", sources[:20])

        obs = fetch_observations_interval(
            sess,
            auth=auth,
            sources=sources,
            referencetime=referencetime,
            elements=elements,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

    if obs.empty:
        return make_info_map(
            title="Ingen data i perioden",
            message="Prøv en annen periode eller zoom litt ut/inn og oppdater utsnittet.",
            mode=str(mode),
            date_str=day_str,
            center=center,
            zoom=zoom,
        )

    out = aggregate_sun_per_station(obs, count_col=sum_count_col)

    out["baseId"] = out["sourceId"].astype(str).map(base_source_id)
    merged = out.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "sun_hours"])

    if merged.empty:
        return make_info_map(
            title="Ingen plottbare punkter",
            message="Data finnes, men mangler koordinater/verdi for plotting.",
            mode=str(mode),
            date_str=day_str,
            center=center,
            zoom=zoom,
        )

    merged = downsample_spatial_best_quality(
        merged,
        w=w, s=s, e=e, n=n,
        max_points=1200,
        keep_top_n=10,
        value_col="sun_hours",
    )

    if mode == "last24h":
        updated = datetime.now(timezone.utc).strftime("%H:%M UTC")
        title = f"{title}<br><small>Oppdatert ca. {updated}</small>"

    return make_map(
        merged,
        title=title,
        out_html=None,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_hours=heat_clip_hours,
        bounds=(w, s, e, n),
        center=center,
        zoom=zoom,
        top_n=10,
        mode=str(mode),
        date_str=day_str if mode != "last24h" else "",
    )


# ======================================================================
# CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Solskinn-kart (bbox on-demand).")
    ap.add_argument("--mode", default="last24h", choices=["last24h", "day", "mtd", "ytd"])
    ap.add_argument("--date", default=_date.today().isoformat())
    ap.add_argument("--bbox", default="", help="west,south,east,north")
    args = ap.parse_args()

    html = build_sunshine_map_html(
        date_str=args.date,
        mode=args.mode,  # type: ignore[arg-type]
        bbox=args.bbox or None,
        show_heatmap=True,
    )
    print(html[:800])


if __name__ == "__main__":
    main()
