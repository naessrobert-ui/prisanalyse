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
from typing import Any, Iterator, Optional, Literal, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap  # MarkerCluster fjernet

from ver_station_db import load_station_db, stations_in_bbox_swne_with

# --- .env loading -------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

# Aggregert solskinn fra Frost:
# sum(duration_of_sunshine P1D)  -> timer siste døgn / kalenderdøgn
# sum(duration_of_sunshine P1M)  -> timer så langt i måneden (eller månedlig)
# sum(duration_of_sunshine P1Y)  -> timer så langt i året (eller årlig)  (ofte ikke tilgjengelig)
ELEMENT_SUN_DAILY = "sum(duration_of_sunshine P1D)"
ELEMENT_SUN_MONTHLY = "sum(duration_of_sunshine P1M)"
ELEMENT_SUN_YEARLY = "sum(duration_of_sunshine P1Y)"

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
    """
    GET med retries (429/5xx).

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

    Vi bruker ferdig aggregerte elementer (P1D, P1M, P1Y),
    og summerer kun selv der vi eksplisitt ønsker det (ytd med P1M).
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
    Velg siste tilgjengelige aggregat-verdi per stasjon i perioden.

    Verdien fra Frost for P1D / P1M / P1Y er allerede i **timer**,
    så sun_hours = value.
    """
    if df.empty:
        return df

    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["referenceTime"] = pd.to_datetime(d["referenceTime"], errors="coerce", utc=True)
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    d = d.dropna(subset=["referenceTime", "value"])

    d = d.sort_values(["sourceId", "referenceTime"])

    grouped = (
        d.groupby("sourceId", as_index=False)
        .agg(
            referenceTime=("referenceTime", "last"),
            value=("value", "last"),
            qualityCode=("qualityCode", "min"),
            **{count_col: ("value", "size")},
        )
        .reset_index(drop=True)
    )

    grouped["unit"] = "hours"
    grouped["sun_hours"] = grouped["value"].astype(float)
    return grouped


def sum_monthly_to_period_per_station(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summer månedsverdier (P1M) per stasjon i perioden.

    Brukes for 'ytd':
      - Hent P1M fra 1. januar til (dato + 1 dag)
      - Velg siste verdi per (stasjon, måned) og summer.
    """
    if df.empty:
        return df

    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["referenceTime"] = pd.to_datetime(d["referenceTime"], errors="coerce", utc=True)
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    d = d.dropna(subset=["referenceTime", "value"])

    d["month"] = d["referenceTime"].dt.to_period("M").astype(str)
    d = d.sort_values(["sourceId", "referenceTime"])

    last_per_month = (
        d.groupby(["sourceId", "month"], as_index=False)
        .agg(
            referenceTime=("referenceTime", "last"),
            value=("value", "last"),
            qualityCode=("qualityCode", "min"),
        )
    )

    out = (
        last_per_month.groupby("sourceId", as_index=False)
        .agg(
            referenceTime=("referenceTime", "max"),
            value=("value", "sum"),
            qualityCode=("qualityCode", "min"),
            n_obs=("value", "size"),
        )
        .reset_index(drop=True)
    )
    out["unit"] = "hours"
    out["sun_hours"] = out["value"].astype(float)
    return out


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
      Dette kan ta litt tid for store perioder.
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


def make_info_map(*, title: str, message: str, mode: str, date_str: str) -> str:
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")
    ui = f"""
    {_loading_overlay_js()}
    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 420px;
    ">
      <div style="font-weight:900; margin-bottom:6px;">{title}</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">{message}</div>
      <button id="refreshBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer;
      ">Oppdater</button>
    </div>

    <script>
      document.getElementById('refreshBtn').addEventListener('click', function() {{
        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
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
    cluster: bool = False,  # default endret: ingen clustering
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_hours: float = 12.0,
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

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")

    m.fit_bounds(
        [
            [float(d["lat"].min()), float(d["lon"].min())],
            [float(d["lat"].max()), float(d["lon"].max())],
        ]
    )

    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)

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
        Hent nye tall for valgt periode.
      </div>
      <button id="refreshBtn" style="
        padding:8px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer;
      ">Oppdater</button>
    </div>

    <script>
      document.getElementById('refreshBtn').addEventListener('click', function() {{
        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/solskinn-kart?' + qs.toString();
      }});
    </script>
    """
    folium.Element(refresh_box).add_to(m.get_root().html)

    clipped = d["sun_hours"].clip(lower=0, upper=heat_clip_hours)
    weights = (clipped / heat_clip_hours) ** 0.5
    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name=f"Heatmap – {title}", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)

    layer_for_markers = points_layer

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
        <div style="font-weight:900;">Topp {int(top_n)}</div>
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
# Hoved: bygg HTML for solskinn (parquet som kildeliste)
# ======================================================================

def _parse_bbox_swne(bbox: str) -> Tuple[float, float, float, float]:
    # forventer "south,west,north,east"
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox må være 'south,west,north,east'")
    s, w, n, e = map(float, parts)
    return (s, w, n, e)


def build_sunshine_map_html(
    date_str: Optional[str] = None,
    *,
    mode: Mode = "day",
    bbox: Optional[str] = None,   # hvis satt: begrens stasjoner til bbox
    z: Optional[str] = None,      # ignoreres (bakoverkompatibilitet)
    clat: Optional[str] = None,   # ignoreres
    clon: Optional[str] = None,   # ignoreres
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
    cluster: bool = False,        # default endret: ingen clustering
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_hours: float = 12.0,
) -> str:
    _ = (z, clat, clon)  # eksplisitt: ikke i bruk

    day_str = date_str or _date.today().isoformat()
    auth = _env_auth()

    # Forenklet parsing:
    # - Normalt forventer vi YYYY-MM-DD
    # - For ytd kan vi også få YYYY (tolkes som 31.12 samme år)
    if mode == "ytd" and day_str.isdigit() and len(day_str) == 4:
        year = int(day_str)
        day = _date(year, 12, 31)
    else:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()

    # Velg element for observasjoner + referencetime per modus
    if mode == "last24h":
        elements_obs = ELEMENT_SUN_DAILY
        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(days=2)  # rom for siste verdi
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        title = "Solskinn siste døgn"
        sum_count_col = "n_obs"

    elif mode == "day":
        elements_obs = ELEMENT_SUN_DAILY
        start = day
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = f"Solskinn kalenderdøgn {day.isoformat()}"
        sum_count_col = "n_obs"

    elif mode == "mtd":
        # Viktig: slutt er eksklusiv => "til og med day" = day + 1
        elements_obs = ELEMENT_SUN_MONTHLY
        start = _date(day.year, day.month, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = f"Solskinn hittil i måneden ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_obs"

    elif mode == "ytd":
        # NY LOGIKK: Hent månedsverdier fra 1. januar til og med valgt dato (day+1),
        # og summer månedene per stasjon.
        elements_obs = ELEMENT_SUN_MONTHLY
        start = _date(day.year, 1, 1)
        end = day + timedelta(days=1)  # "til og med day"
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title = f"Solskinn hittil i året {day.year} ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_obs"

    else:
        raise ValueError(f"Ukjent mode: {mode}")

    # --- Stasjoner fra parquet ------------------------------------------------
    if bbox:
        bbox_swne = _parse_bbox_swne(bbox)
        st = stations_in_bbox_swne_with(bbox_swne, require_sun=True)
    else:
        st = load_station_db()
        if not st.empty:
            st = st[st["has_sun"] == True].copy()

    if st.empty:
        return make_info_map(
            title="Ingen solskinn-stasjoner",
            message="Fant ingen stasjoner med has_sun=True i stasjonsdatabasen.",
            mode=str(mode),
            date_str=day_str if mode != "last24h" else "",
        )

    st = st.dropna(subset=["baseId", "lat", "lon"]).copy()
    sources = st["baseId"].astype(str).tolist()
    src_meta = st[["baseId", "name", "shortName", "lat", "lon"]].copy()

    # --- Observasjoner --------------------------------------------------------
    with requests.Session() as sess:
        obs = fetch_observations_interval(
            sess,
            auth=auth,
            sources=sources,
            referencetime=referencetime,
            elements=elements_obs,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

    if obs.empty:
        return make_info_map(
            title="Ingen data i perioden",
            message="Stasjoner finnes (has_sun=True), men ingen observasjoner ble returnert for perioden.",
            mode=str(mode),
            date_str=day_str if mode != "last24h" else "",
        )

    # --- Aggregering ----------------------------------------------------------
    if mode == "ytd":
        out = sum_monthly_to_period_per_station(obs)
    else:
        out = aggregate_sun_per_station(obs, count_col=sum_count_col)

    # Join mot parquet-metadata
    out["baseId"] = out["sourceId"].astype(str).map(base_source_id)
    merged = out.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "sun_hours"])

    if merged.empty:
        return make_info_map(
            title="Ingen plottbare punkter",
            message="Data finnes, men mangler koordinater/verdi for plotting.",
            mode=str(mode),
            date_str=day_str if mode != "last24h" else "",
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
        top_n=10,
        mode=str(mode),
        date_str=day_str if mode != "last24h" else "",
    )


# ======================================================================
# CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Solskinn-kart (stasjonsliste fra parquet).")
    ap.add_argument("--mode", default="last24h", choices=["last24h", "day", "mtd", "ytd"])
    ap.add_argument("--date", default=_date.today().isoformat())
    ap.add_argument("--bbox", default="", help="Valgfritt: 'south,west,north,east'")
    args = ap.parse_args()

    html = build_sunshine_map_html(
        date_str=args.date,
        mode=args.mode,  # type: ignore[arg-type]
        bbox=args.bbox or None,
        show_heatmap=True,
        cluster=False,
    )
    print(html[:800])


if __name__ == "__main__":
    main()
