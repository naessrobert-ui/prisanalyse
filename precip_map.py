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

# Elementer
ELEMENT_PRECIP_DAY = "sum(precipitation_amount P1D)"      # døgnsum
ELEMENT_PRECIP_HOURLY = "sum(precipitation_amount PT1H)"  # time (summerer 24 timer for last24h)

UNIT_MM = "mm"
Mode = Literal["last24h", "day", "mtd", "ytd"]


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
    """GET med retries (429/5xx). Returnerer JSON (inkl. ErrorResponse ved 404)."""
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

        if r.status_code == 404:
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


def pick_day_value_per_station(df: pd.DataFrame, *, day: _date) -> pd.DataFrame:
    """Velg verdien som hører til valgt kalenderdøgn (UTC)."""
    if df.empty:
        return df

    start = pd.Timestamp(datetime(day.year, day.month, day.day, tzinfo=timezone.utc))
    end = start + pd.Timedelta(days=1)
    d = df[(df["referenceTime"] >= start) & (df["referenceTime"] < end)].copy()
    if d.empty:
        return d

    d = (
        d.sort_values(["sourceId", "referenceTime"], ascending=[True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return d


def aggregate_sum_per_station(df: pd.DataFrame, *, count_col: str) -> pd.DataFrame:
    """Summerer value over intervallet per stasjon."""
    if df.empty:
        return df
    out = (
        df.groupby("sourceId", as_index=False)
        .agg(value=("value", "sum"), n=("value", "size"), rt_max=("referenceTime", "max"), qmin=("qualityCode", "min"))
        .reset_index(drop=True)
    )
    out.rename(columns={"n": count_col}, inplace=True)
    out["unit"] = UNIT_MM
    out["referenceTime"] = out["rt_max"]
    out["qualityCode"] = out["qmin"]
    out.drop(columns=["rt_max", "qmin"], inplace=True)
    return out


# ======================================================================
# Downsample (kun hvis for mange)
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
) -> pd.DataFrame:
    """
    Hvis df <= max_points: behold alt.
    Ellers: 1 pr gridcelle, prioritert på:
      - lavest qualityCode (best)
      - nyeste referenceTime
    + behold topp keep_top_n høyeste value.
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

    d_sorted = d.sort_values(
        ["iy", "ix", "qualityCode", "referenceTime"],
        ascending=[True, True, True, False],
    )
    grid_pick = d_sorted.groupby(["iy", "ix"], as_index=False).head(1).reset_index(drop=True)

    top_pick = d.sort_values("value", ascending=False).head(keep_top_n)

    out = pd.concat([grid_pick, top_pick], ignore_index=True).drop_duplicates(subset=["sourceId"], keep="first")

    if len(out) > max_points:
        out = out.sort_values(["qualityCode", "referenceTime"], ascending=[True, False]).head(max_points)

    return out.reset_index(drop=True)


# ======================================================================
# UI: tomt kart + knapp
# ======================================================================

def make_empty_map_with_button(*, title: str, mode: str, date_str: str) -> str:
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    # Flyttet til top's RIGHT så den ikke dekker zoom-knappene.
    button_html = f"""
    <div style="
      position: fixed; top: 12px; right: 12px; left: auto; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 340px;
    ">
      <div style="font-weight:700; margin-bottom:6px;">{title}</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Zoom/pan til ønsket område og trykk hent.
      </div>
      <button id="bboxFetchBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#2563eb; color:white; cursor:pointer;
      ">Hent data for synlig område</button>
    </div>

    <script>
      function findLeafletMap() {{
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function') {{
            return v;
          }}
        }}
        return null;
      }}

      document.getElementById('bboxFetchBtn').addEventListener('click', function() {{
        const map = findLeafletMap();
        if (!map) {{
          alert('Fant ikke kart-objektet. Prøv å reloade siden.');
          return;
        }}
        const b = map.getBounds();
        const west = b.getWest();
        const south = b.getSouth();
        const east = b.getEast();
        const north = b.getNorth();
        const bbox = [west, south, east, north].join(',');

        const c = map.getCenter();
        const z = map.getZoom();

        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        qs.set('bbox', bbox);
        qs.set('z', String(z));
        qs.set('clat', String(c.lat));
        qs.set('clon', String(c.lng));

        window.location.href = '/ver/nedbor-kart?' + qs.toString();
      }});
    </script>
    """
    folium.Element(button_html).add_to(m.get_root().html)
    return m.get_root().render()


# ======================================================================
# Kart med data: styling + legend + toppliste
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
    heat_clip_mm: float = 80.0,
    bounds: Optional[tuple[float, float, float, float]] = None,       # (w,s,e,n)
    center: Optional[tuple[float, float]] = None,                     # (lat,lon)
    zoom: Optional[int] = None,
    top_n: int = 10,
) -> str:
    if df.empty:
        raise RuntimeError("Ingen data å plotte (df er tom).")

    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])

    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    # Kvantiler for farger
    vals = d["value"].astype(float)
    q10 = float(vals.quantile(0.10))
    q20 = float(vals.quantile(0.20))
    q80 = float(vals.quantile(0.80))
    q90 = float(vals.quantile(0.90))

    def color_for(mm: float) -> str:
        if mm >= q90:
            return "#ff0000"   # topp 10% (klar rød)
        if mm >= q80:
            return "#8b0000"   # 80-90% (mørk rød)
        if mm <= q10:
            return "#00b300"   # bunn 10% (grønn)
        if mm <= q20:
            return "#006400"   # 10-20% (mørk grønn)
        return "#808080"       # resten (grå)

    def radius_for(mm: float) -> float:
        # sqrt-skala: god visuelt
        r = 3.0 + 4.5 * math.sqrt(max(mm, 0.0))
        return float(max(3.0, min(r, 20.0)))

    # Map init: hvis vi har center+zoom -> behold nøyaktig utsnitt
    if center and zoom is not None:
        m = folium.Map(location=[center[0], center[1]], zoom_start=int(zoom), tiles="OpenStreetMap")
    else:
        center_lat = float(d["lat"].mean())
        center_lon = float(d["lon"].mean())
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")
        if bounds:
            w, s, e, n = bounds
            m.fit_bounds([[s, w], [n, e]])

    # Legend (top-right, under knapp)
    legend_html = f"""
    <div style="
      position: fixed; top: 90px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 340px;
      font-size: 13px; color:#0f172a;
    ">
      <div style="font-weight:700; margin-bottom:6px;">Forklaring</div>
      <div style="margin-bottom:6px;">Farge = percentiler i utsnittet</div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#ff0000;"></span>
        <span>Topp 10%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#8b0000;"></span>
        <span>80–90%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#808080;"></span>
        <span>Midten</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#006400;"></span>
        <span>10–20%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#00b300;"></span>
        <span>Bunn 10%</span>
      </div>
      <div style="margin-top:8px; color:#334155;">
        Radius ~ √(mm) (proporsjonal, men dempet).
      </div>
    </div>
    """
    folium.Element(legend_html).add_to(m.get_root().html)

    # Heatmap
    clipped = d["value"].clip(lower=0, upper=heat_clip_mm)
    weights = (clipped / heat_clip_mm) ** 0.5
    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name=f"Heatmap – {title}", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    # Markører
    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    marker_map: dict[str, str] = {}

    for _, r in d.iterrows():
        mm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or UNIT_MM
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"

        qc = r.get("qualityCode")
        qc_str = f"{int(qc)}" if pd.notna(qc) else "ukjent"

        html = f"{name}<br>{title}: <b>{mm:.1f} {unit}</b><br>Tid: {t_str}<br>Kvalitet: {qc_str}"

        col = color_for(mm)
        rad = radius_for(mm)

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

    # Toppliste (topp N)
    top = d.sort_values("value", ascending=False).head(int(top_n)).copy()
    rows_html: list[str] = []
    for i, r in enumerate(top.itertuples(index=False), start=1):
        sid = str(getattr(r, "sourceId"))
        nm = getattr(r, "name", None) or getattr(r, "shortName", None) or sid
        mm = float(getattr(r, "value"))
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
              <td style="padding:6px 8px; text-align:right; font-weight:700;">{mm:.1f} mm</td>
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
        <div style="font-weight:800;">Topp {int(top_n)} i utsnittet</div>
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
              <th style="text-align:right; padding:6px 8px; color:#64748b; font-size:12px;">mm</th>
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

    # JS mapping: sourceId -> Leaflet marker, og focusStation()
    map_var = m.get_name()
    mapping_lines = ",\n".join([f'"{sid}": {jsname}' for sid, jsname in marker_map.items()])

    js = f"""
    <script>
      function findLeafletMap() {{
        // Bruk folium-map var hvis mulig, fallback til scanning.
        if (typeof {map_var} !== 'undefined') return {map_var};
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.setView === 'function' && typeof v.getCenter === 'function') return v;
        }}
        return null;
      }}

      window._precipMarkers = {{
        {mapping_lines}
      }};

      function focusStation(sourceId) {{
        const map = findLeafletMap();
        const mk = window._precipMarkers[sourceId];
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
# Hoved: bygg HTML for nedbør (tomt kart eller bbox)
# ======================================================================

def build_precip_map_html(
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
    heat_clip_mm: float = 80.0,
) -> str:
    # UI-dato for day/mtd/ytd
    day_str = date_str or _date.today().isoformat()

    # Ingen bbox: lett kart med knapp
    if not bbox:
        title = "Nedbør"
        if mode == "last24h":
            title = "Nedbør siste 24 timer (rullerende)"
        elif mode == "day":
            title = "Nedbør kalenderdøgn (valgt dato)"
        elif mode == "mtd":
            title = "Nedbør hittil i måneden"
        elif mode == "ytd":
            title = "Nedbør hittil i året"

        return make_empty_map_with_button(title=title, mode=str(mode), date_str=day_str)

    # bbox: hent data kun for utsnittet
    w, s, e, n = _parse_bbox(bbox)
    auth = _env_auth()

    # center/zoom (for å ikke “utvide” når vi bytter periode)
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

    day = datetime.strptime(day_str, "%Y-%m-%d").date()

    if mode == "last24h":
        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(hours=24)
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        elements = ELEMENT_PRECIP_HOURLY
        title = "Nedbør siste 24 timer (rullerende)"
        sum_count_col = "n_hours"
    elif mode == "day":
        start = day
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = "Nedbør kalenderdøgn (valgt dato)"
        sum_count_col = "n_days"
    elif mode == "mtd":
        start = _date(day.year, day.month, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = f"Nedbør hittil i måneden ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_days"
    elif mode == "ytd":
        start = _date(day.year, 1, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = f"Nedbør hittil i året ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_days"
    else:
        raise ValueError(f"Ukjent mode: {mode}")

    with requests.Session() as sess:
        src_meta = fetch_sources_in_bbox(sess, auth=auth, w=w, s=s, e=e, n=n, timeout=timeout)
        if src_meta.empty:
            return make_empty_map_with_button(title=f"{title} (ingen stasjoner i området)", mode=str(mode), date_str=day_str)

        sources = src_meta["baseId"].astype(str).tolist()

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
        return make_empty_map_with_button(title=f"{title} (ingen data i perioden)", mode=str(mode), date_str=day_str)

    if mode == "day":
        picked = pick_day_value_per_station(obs, day=day)
        out = picked[["sourceId", "referenceTime", "value", "unit", "qualityCode"]].copy()
    else:
        out = aggregate_sum_per_station(obs, count_col=sum_count_col)

    out["baseId"] = out["sourceId"].astype(str).map(base_source_id)
    merged = out.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "value"])

    if merged.empty:
        return make_empty_map_with_button(title=f"{title} (ingen plottbare punkter)", mode=str(mode), date_str=day_str)

    merged = downsample_spatial_best_quality(
        merged,
        w=w, s=s, e=e, n=n,
        max_points=1200,
        keep_top_n=10,
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
        heat_clip_mm=heat_clip_mm,
        bounds=(w, s, e, n),
        center=center,
        zoom=zoom,
        top_n=10,
    )


# ======================================================================
# CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Nedbør-kart (bbox on-demand).")
    ap.add_argument("--mode", default="last24h", choices=["last24h", "day", "mtd", "ytd"])
    ap.add_argument("--date", default=_date.today().isoformat())
    ap.add_argument("--bbox", default="", help="west,south,east,north")
    args = ap.parse_args()

    html = build_precip_map_html(
        date_str=args.date,
        mode=args.mode,  # type: ignore[arg-type]
        bbox=args.bbox or None,
        show_heatmap=True,
    )
    print(html[:500])


if __name__ == "__main__":
    main()
