#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

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

UNIT_C = "°C"

# En pragmatisk fylkeliste (etter 2024-endringene).
NORWAY_COUNTIES: list[str] = [
    "Oslo",
    "Akershus",
    "Østfold",
    "Buskerud",
    "Innlandet",
    "Vestfold",
    "Telemark",
    "Agder",
    "Rogaland",
    "Vestland",
    "Møre og Romsdal",
    "Trøndelag",
    "Nordland",
    "Troms",
    "Finnmark",
]


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
    """GET med retries (429/5xx). Returnerer JSON (inkl. ErrorResponse ved 404/412)."""
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

        # 404 og 412 returneres som ErrorResponse (ikke kast)
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
# Sources per fylke
# ======================================================================

def fetch_sources_in_county(
    session: requests.Session,
    *,
    auth: FrostAuth,
    county: str,
    timeout: int,
) -> pd.DataFrame:
    """
    Hent stasjoner i fylke via /sources.
    Bruk 'county' og ev. 'countyid'. (Ikke 'countyname'.)

    Returnerer DF med: baseId, name, shortName, county, countyid, lat, lon.
    """
    path = "/sources/v0.jsonld"

    base_params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "fields": "id,name,shortName,geometry,county,countyid",
        "county": county,
    }

    def _fetch(params: dict[str, str | int]) -> pd.DataFrame:
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
                        "county": item.get("county"),
                        "countyid": item.get("countyid"),
                        "lat": lat,
                        "lon": lon,
                    }
                )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        return df.dropna(subset=["baseId", "lat", "lon"]).reset_index(drop=True)

    df = _fetch(base_params)
    if not df.empty:
        return df

    # Wildcard-fallback (kan hjelpe ved navnevariasjoner)
    county_wild = county.replace(" og ", "*")
    if county_wild != county:
        p2 = base_params.copy()
        p2["county"] = county_wild
        return _fetch(p2)

    return df


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
    """Hent observasjoner i et intervall for mange stasjoner. Returnerer lang DF."""
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


def pick_latest_value_per_station(df: pd.DataFrame) -> pd.DataFrame:
    """Velg nyeste observasjon per stasjon."""
    if df.empty:
        return df
    return (
        df.sort_values(["sourceId", "referenceTime"], ascending=[True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )


def pick_value_in_day(df: pd.DataFrame, *, day: _date) -> pd.DataFrame:
    """Velg verdien som hører til valgt kalenderdøgn (UTC)."""
    if df.empty:
        return df
    start = pd.Timestamp(datetime(day.year, day.month, day.day, tzinfo=timezone.utc))
    end = start + pd.Timedelta(days=1)
    d = df[(df["referenceTime"] >= start) & (df["referenceTime"] < end)].copy()
    if d.empty:
        return d
    return pick_latest_value_per_station(d)


# ======================================================================
# Period helpers (UTC)
# ======================================================================

def _month_bounds_utc(ym: str) -> tuple[datetime, datetime]:
    y, m = map(int, ym.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(y, m + 1, 1, tzinfo=timezone.utc)
    return start, end


def _year_bounds_utc(y: str) -> tuple[datetime, datetime]:
    yy = int(y)
    start = datetime(yy, 1, 1, tzinfo=timezone.utc)
    end = datetime(yy + 1, 1, 1, tzinfo=timezone.utc)
    return start, end


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
      Dette går fortere når du velger fylke først.
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


def make_empty_map_with_dropdown(*, selected_county: str = "") -> str:
    """
    Tomt kart: kun fylkevelger.
    Periodemeny kommer når data er lastet (for å holde default rask/enkelt).
    """
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")
    opts = "\n".join(
        f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
        for c in NORWAY_COUNTIES
    )

    ui = f"""
    {_loading_overlay_js()}

    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 12px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 360px;
    ">
      <div style="font-weight:900; margin-bottom:8px;">Minimumstemperatur</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Velg fylke for å hente nyeste døgn-min per stasjon.
      </div>

      <label style="font-size:12px; color:#64748b;">Fylke</label>
      <select id="countySel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        <option value="">– Velg –</option>
        {opts}
      </select>

      <button id="fetchBtn" style="
        margin-top:10px; width:100%;
        padding:9px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer; font-weight:700;
      ">Hent siste døgn</button>
    </div>

    <script>
      function go() {{
        const c = document.getElementById('countySel').value;
        if (!c) {{
          alert('Velg fylke først.');
          return;
        }}
        const qs = new URLSearchParams();
        qs.set('county', c);
        // default period=last
        qs.set('period', 'last');
        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/min-temp-kart?' + qs.toString();
      }}
      document.getElementById('fetchBtn').addEventListener('click', go);
    </script>
    """
    folium.Element(ui).add_to(m.get_root().html)
    return m.get_root().render()


# ======================================================================
# Kart med data
# ======================================================================

def make_temp_map(
    df: pd.DataFrame,
    *,
    title: str,
    selected_county: str,
    selected_period: str,
    selected_date: str,
    selected_month: str,
    selected_year: str,
    element_used: str,
    cluster: bool = True,
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cold: float = -25.0,
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

    vals = d["value"].astype(float)
    q10 = float(vals.quantile(0.10))
    q20 = float(vals.quantile(0.20))
    q80 = float(vals.quantile(0.80))
    q90 = float(vals.quantile(0.90))

    def color_for(tc: float) -> str:
        if tc <= q10:
            return "#1d4ed8"
        if tc <= q20:
            return "#2563eb"
        if tc >= q90:
            return "#dc2626"
        if tc >= q80:
            return "#ef4444"
        return "#64748b"

    def radius_for(tc: float) -> float:
        med = float(vals.median())
        strength = abs(tc - med)
        r = 4.0 + 2.5 * math.sqrt(max(strength, 0.0))
        return float(max(4.0, min(r, 18.0)))

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")

    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)

    # Header: fylke + periode + inputs
    opts = "\n".join(
        f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
        for c in NORWAY_COUNTIES
    )

    period_opts = {
        "last": "Siste døgn",
        "day": "Valgt døgn",
        "month": "Valgt måned",
        "year": "Valgt år",
    }
    period_html = "\n".join(
        f'<option value="{k}" {"selected" if k == selected_period else ""}>{v}</option>'
        for k, v in period_opts.items()
    )

    header = f"""
    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 12px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 410px;
    ">
      <div style="font-weight:900; margin-bottom:6px;">{title}</div>
      <div style="font-size:12px; color:#64748b; margin-bottom:10px;">
        Element: <code style="font-size:11px;">{element_used}</code>
      </div>

      <label style="font-size:12px; color:#64748b;">Fylke</label>
      <select id="countySel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        <option value="">– Velg –</option>
        {opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Periode</label>
      <select id="periodSel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {period_html}
      </select>

      <div id="dayBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">Dato</label>
        <input id="dayInput" type="date" value="{selected_date}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <div id="monthBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">Måned</label>
        <input id="monthInput" type="month" value="{selected_month}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <div id="yearBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">År</label>
        <input id="yearInput" type="number" min="1900" max="2100" value="{selected_year}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <button id="refreshBtn" style="
        margin-top:10px; width:100%;
        padding:9px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer; font-weight:800;
      ">Oppdater</button>
    </div>

    <script>
      const periodSel = document.getElementById("periodSel");
      const dayBox = document.getElementById("dayBox");
      const monthBox = document.getElementById("monthBox");
      const yearBox = document.getElementById("yearBox");

      function syncPeriodUI() {{
        const p = periodSel.value || "last";
        dayBox.style.display = (p === "day") ? "block" : "none";
        monthBox.style.display = (p === "month") ? "block" : "none";
        yearBox.style.display = (p === "year") ? "block" : "none";
      }}
      periodSel.addEventListener("change", syncPeriodUI);
      syncPeriodUI();

      function go() {{
        const c = document.getElementById('countySel').value;
        if (!c) {{
          alert('Velg fylke først.');
          return;
        }}

        const p = periodSel.value || "last";
        const qs = new URLSearchParams();
        qs.set('county', c);
        qs.set('period', p);

        if (p === "day") {{
          const d = document.getElementById("dayInput").value;
          if (!d) {{ alert("Velg dato"); return; }}
          qs.set("date", d);
        }} else if (p === "month") {{
          const m = document.getElementById("monthInput").value;
          if (!m) {{ alert("Velg måned"); return; }}
          qs.set("month", m);
        }} else if (p === "year") {{
          const y = document.getElementById("yearInput").value;
          if (!y) {{ alert("Velg år"); return; }}
          qs.set("year", y);
        }}

        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/min-temp-kart?' + qs.toString();
      }}

      document.getElementById('refreshBtn').addEventListener('click', go);
    </script>
    """
    folium.Element(header).add_to(m.get_root().html)

    legend = """
    <div style="
      position: fixed; top: 320px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 360px;
      font-size: 13px; color:#0f172a;
    ">
      <div style="font-weight:900; margin-bottom:6px;">Forklaring</div>
      <div style="margin-bottom:6px;">Farge = percentiler i fylket</div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#1d4ed8;"></span>
        <span>Kaldest 10%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#2563eb;"></span>
        <span>10–20%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#64748b;"></span>
        <span>Midten</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#ef4444;"></span>
        <span>80–90%</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; margin:4px 0;">
        <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#dc2626;"></span>
        <span>Varmest 10%</span>
      </div>
    </div>
    """
    folium.Element(legend).add_to(m.get_root().html)

    # Heatmap: kaldere => høyere vekt
    clipped = d["value"].clip(lower=heat_clip_cold, upper=10.0)
    weights = (10.0 - clipped) / (10.0 - heat_clip_cold)  # 0..1
    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name="Heatmap", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.25, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    # For toppliste-oppdatering i JS (uten nye kall)
    points_js: list[dict[str, Any]] = []

    for _, r in d.iterrows():
        tc = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or UNIT_C
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"
        qc = r.get("qualityCode")
        qc_str = f"{int(qc)}" if pd.notna(qc) else "ukjent"

        html = f"{name}<br>Min temp: <b>{tc:.1f} {unit}</b><br>Tid: {t_str}<br>Kvalitet: {qc_str}"

        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=radius_for(tc),
            color=color_for(tc),
            fill=True,
            fill_color=color_for(tc),
            fill_opacity=0.85,
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=360),
        ).add_to(layer_for_markers)

        points_js.append(
            {
                "sid": str(r["sourceId"]),
                "name": str(name),
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "value": float(tc),
            }
        )

    folium.LayerControl().add_to(m)

    # Toppliste-boks (nå dynamisk i utsnittet)
    topbox_html = f"""
    <div style="
      position: fixed; bottom: 12px; left: 12px; z-index: 9999;
      background: rgba(255,255,255,.97); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 520px;
    ">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="font-weight:900;">
          <span id="topTitle">Kaldest {int(top_n)} i utsnittet</span>
        </div>
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
              <th style="text-align:right; padding:6px 8px; color:#64748b; font-size:12px;">°C</th>
            </tr>
          </thead>
          <tbody id="toplistTbody">
            <tr><td colspan="3" style="padding:8px; color:#64748b;">Oppdaterer…</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <script>
      function toggleToplist() {{
        const el = document.getElementById("toplistBody");
        if (!el) return;
        el.style.display = (el.style.display === "none") ? "block" : "none";
      }}

      function findLeafletMap() {{
        for (const k of Object.keys(window)) {{
          const v = window[k];
          if (v && typeof v.getBounds === 'function' && typeof v.getCenter === 'function') {{
            return v;
          }}
        }}
        return null;
      }}

      window._tempPoints = {json.dumps(points_js, ensure_ascii=False)};

      function updateToplist() {{
        const map = findLeafletMap();
        if (!map) return;

        const b = map.getBounds();
        const inView = window._tempPoints.filter(p => b.contains([p.lat, p.lon]));
        inView.sort((a,b) => a.value - b.value);

        const top = inView.slice(0, {int(top_n)});
        const tb = document.getElementById("toplistTbody");
        const tt = document.getElementById("topTitle");
        if (!tb) return;

        if (tt) {{
          tt.textContent = "Kaldest {int(top_n)} i utsnittet (" + inView.length + " stasjoner)";
        }}

        if (top.length === 0) {{
          tb.innerHTML = '<tr><td colspan="3" style="padding:8px; color:#64748b;">Ingen punkter i utsnittet.</td></tr>';
          return;
        }}

        tb.innerHTML = top.map((p, i) => `
          <tr>
            <td style="padding:6px 8px; color:#64748b;">${{i+1}}</td>
            <td style="padding:6px 8px;">
              ${{p.name}}
              <div style="font-size:12px;color:#64748b;">${{p.sid}}</div>
            </td>
            <td style="padding:6px 8px; text-align:right; font-weight:900;">${{p.value.toFixed(1)}} °C</td>
          </tr>
        `).join("");
      }}

      (function attachToplistUpdater() {{
        const map = findLeafletMap();
        if (!map) return;
        map.on("moveend", updateToplist);
        map.on("zoomend", updateToplist);
        updateToplist();
      }})();
    </script>
    """
    folium.Element(topbox_html).add_to(m.get_root().html)

    return m.get_root().render()


# ======================================================================
# Hoved: bygg HTML for min-temp
# ======================================================================

def build_min_temp_map_html(
    *,
    county: Optional[str] = None,
    period: str = "last",                 # last|day|month|year
    date_str: Optional[str] = None,       # YYYY-MM-DD
    month_str: Optional[str] = None,      # YYYY-MM
    year_str: Optional[str] = None,       # YYYY
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
) -> str:
    if not county:
        return make_empty_map_with_dropdown()

    auth = _env_auth()
    now = datetime.now(timezone.utc)

    # Default values for UI inputs
    ui_date = (date_str or _date.today().isoformat())
    ui_month = (month_str or now.strftime("%Y-%m"))
    ui_year = (year_str or now.strftime("%Y"))

    # Bestem referencetime + element-kandidater
    period = (period or "last").lower()

    if period == "day":
        day = _date.fromisoformat(ui_date)
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        element_candidates = [
            "best_estimate_min(air_temperature P1D)",
            "min(air_temperature P1D)",
        ]
        title_base = f"Minimumstemperatur – døgn ({ui_date}) – {county}"
    elif period == "month":
        start, end = _month_bounds_utc(ui_month)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        element_candidates = [
            "best_estimate_min(air_temperature P1M)",
            "min(air_temperature P1M)",
        ]
        title_base = f"Minimumstemperatur – måned ({ui_month}) – {county}"
    elif period == "year":
        start, end = _year_bounds_utc(ui_year)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        element_candidates = [
            "best_estimate_min(air_temperature P1Y)",
            "min(air_temperature P1Y)",
        ]
        title_base = f"Minimumstemperatur – år ({ui_year}) – {county}"
    else:
        # last: hent siste ~2 døgn og plukk nyeste P1D per stasjon
        start_dt = now - timedelta(days=2)
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        element_candidates = [
            "best_estimate_min(air_temperature P1D)",
            "min(air_temperature P1D)",
        ]
        title_base = f"Minimumstemperatur – siste døgn ({county})"

    with requests.Session() as sess:
        src_meta = fetch_sources_in_county(sess, auth=auth, county=county, timeout=timeout)
        if src_meta.empty:
            return make_empty_map_with_dropdown(selected_county=county)

        sources = src_meta["baseId"].astype(str).tolist()

        obs = pd.DataFrame()
        element_used: str = ""
        for el in element_candidates:
            tmp = fetch_observations_interval(
                sess,
                auth=auth,
                sources=sources,
                referencetime=referencetime,
                elements=el,
                timeout=timeout,
                batch_size=batch_size,
                limit=limit,
                qualities=qualities,
            )
            if not tmp.empty:
                obs = tmp
                element_used = el
                break

    if obs.empty:
        m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")
        folium.Element(_loading_overlay_js()).add_to(m.get_root().html)
        msg = f"""
        <div style="
          position: fixed; top: 12px; right: 12px; z-index: 9999;
          background: rgba(255,255,255,.95); padding: 12px 12px;
          border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
          font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
          max-width: 420px;
        ">
          <div style="font-weight:900; margin-bottom:6px;">Ingen data</div>
          <div style="font-size:13px; color:#334155;">
            Fant ingen tidsserier for valgt kombinasjon i <b>{county}</b>.
            Prøv en annen periode eller annet fylke.
          </div>
        </div>
        """
        folium.Element(msg).add_to(m.get_root().html)
        return m.get_root().render()

    # Velg verdi per stasjon (avhengig av modus)
    if period == "day":
        picked = pick_value_in_day(obs, day=_date.fromisoformat(ui_date))
    else:
        picked = pick_latest_value_per_station(obs)

    if picked.empty:
        return make_empty_map_with_dropdown(selected_county=county)

    picked["baseId"] = picked["sourceId"].astype(str).map(base_source_id)
    merged = picked.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "value"])

    if merged.empty:
        return make_empty_map_with_dropdown(selected_county=county)

    updated = now.strftime("%Y-%m-%d %H:%M UTC")
    title = f"{title_base}<br><small>Oppdatert ca. {updated}</small>"

    return make_temp_map(
        merged,
        title=title,
        selected_county=county,
        selected_period=period,
        selected_date=ui_date,
        selected_month=ui_month,
        selected_year=ui_year,
        element_used=element_used or "(ukjent)",
        cluster=True,
        heatmap_show=True,
        heat_radius=25,
        heat_blur=18,
        top_n=10,
    )


# ======================================================================
# CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Min-temp-kart (velg fylke).")
    ap.add_argument("--county", default="", help="Fylke, f.eks. 'Innlandet'")
    ap.add_argument("--period", default="last", choices=["last", "day", "month", "year"])
    ap.add_argument("--date", default="", help="YYYY-MM-DD (for period=day)")
    ap.add_argument("--month", default="", help="YYYY-MM (for period=month)")
    ap.add_argument("--year", default="", help="YYYY (for period=year)")
    args = ap.parse_args()

    html = build_min_temp_map_html(
        county=args.county or None,
        period=args.period,
        date_str=args.date or None,
        month_str=args.month or None,
        year_str=args.year or None,
    )
    print(html[:700])


if __name__ == "__main__":
    main()
