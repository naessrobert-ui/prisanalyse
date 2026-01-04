#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import time
from dataclasses import dataclass
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

# Element: døgn-min temp (best estimate)
ELEMENT_TMIN_DAY = "best_estimate_min(air_temperature P1D)"
UNIT_C = "°C"

# En pragmatisk fylkeliste (etter 2024-endringene). Hvis Frost county-navn avviker,
# håndterer vi fallback (se fetch_sources_in_county()).
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

    Merk: /sources støtter ikke feltet 'countyname' (gir 400).
    Bruk 'county' og ev. 'countyid' i stedet.

    Returnerer DF med: baseId, name, shortName, county, countyid, lat, lon.
    """
    path = "/sources/v0.jsonld"

    # Frost /sources: supported fields inkluderer county og countyid (men ikke countyname).
    base_params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "fields": "id,name,shortName,geometry,county,countyid",
        "county": county,  # filter på fylke
    }

    rows: list[dict[str, Any]] = []
    for page in iter_pages(session, path, base_params, auth=auth, timeout=timeout):
        if page.get("@type") == "ErrorResponse":
            # Behold gjerne mer logging her om du vil
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
        # Lite robust wildcard-forsøk (kan hjelpe hvis fylkesnavn matcher litt annerledes i Frost)
        # Eksempel: "Møre og Romsdal" -> "Møre*Romsdal"
        county_wild = county.replace(" og ", "*")
        if county_wild != county:
            params2 = base_params.copy()
            params2["county"] = county_wild

            rows2: list[dict[str, Any]] = []
            for page in iter_pages(session, path, params2, auth=auth, timeout=timeout):
                if page.get("@type") == "ErrorResponse":
                    continue
                for item in page.get("data", []):
                    geom = item.get("geometry") or {}
                    coords = geom.get("coordinates")
                    lon = lat = None
                    if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                        lon, lat = coords[0], coords[1]
                    rows2.append(
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

            df = pd.DataFrame(rows2)

    if df.empty:
        return df

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    return df.dropna(subset=["baseId", "lat", "lon"]).reset_index(drop=True)


# ======================================================================
# Observasjoner: siste døgn (nyeste P1D pr stasjon)
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


def pick_latest_value_per_station(df: pd.DataFrame) -> pd.DataFrame:
    """Velg nyeste observasjon per stasjon (brukes for 'siste døgn' P1D)."""
    if df.empty:
        return df
    out = (
        df.sort_values(["sourceId", "referenceTime"], ascending=[True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return out


# ======================================================================
# UI helpers (dropdown i kart)
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
      <div style="font-weight:900; margin-bottom:8px;">Minimumstemperatur – siste døgn</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Velg fylke for å hente stasjoner og nyeste døgn-min (P1D).
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
      ">Hent</button>
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
        if (window.showLoading) window.showLoading();
        // Hvis du kjører dette som ren CLI/fil: bytt til bare "?county=".
        // I webapp: bruk din route, f.eks. '/ver/min-temp-kart'
        window.location.href = '/ver/min-temp-kart?' + qs.toString();
      }}
      document.getElementById('fetchBtn').addEventListener('click', go);
      document.getElementById('countySel').addEventListener('change', function() {{
        // auto-fetch hvis du vil:
        // go();
      }});
    </script>
    """
    folium.Element(ui).add_to(m.get_root().html)
    return m.get_root().render()


# ======================================================================
# Kart med data (tilpasset temperatur)
# ======================================================================

def make_temp_map(
    df: pd.DataFrame,
    *,
    title: str,
    selected_county: str,
    cluster: bool = True,
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cold: float = -25.0,  # klipp kaldt for heat-weight
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
    q10 = float(vals.quantile(0.10))  # kaldest 10%
    q20 = float(vals.quantile(0.20))
    q80 = float(vals.quantile(0.80))
    q90 = float(vals.quantile(0.90))  # varmest 10%

    def color_for(tc: float) -> str:
        # kaldt -> blått, varmt -> rødt
        if tc <= q10:
            return "#1d4ed8"   # kaldest 10%
        if tc <= q20:
            return "#2563eb"   # 10–20%
        if tc >= q90:
            return "#dc2626"   # varmest 10%
        if tc >= q80:
            return "#ef4444"   # 80–90%
        return "#64748b"       # midten

    def radius_for(tc: float) -> float:
        # litt større bobler ved mer “ekstremt” (kaldere)
        # bruk avstand fra median
        med = float(vals.median())
        strength = abs(tc - med)
        r = 4.0 + 2.5 * math.sqrt(max(strength, 0.0))
        return float(max(4.0, min(r, 18.0)))

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")

    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)

    # Dropdown + refresh
    opts = "\n".join(
        f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
        for c in NORWAY_COUNTIES
    )
    header = f"""
    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 12px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 380px;
    ">
      <div style="font-weight:900; margin-bottom:6px;">{title}</div>
      <div style="font-size:12px; color:#64748b; margin-bottom:10px;">
        Nyeste døgn-min (P1D) per stasjon i valgt fylke.
      </div>

      <div style="display:flex; gap:8px; align-items:end;">
        <div style="flex:1;">
          <label style="font-size:12px; color:#64748b;">Fylke</label>
          <select id="countySel" style="
            width:100%; margin-top:4px;
            padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
            background:white;
          ">
            <option value="">– Velg –</option>
            {opts}
          </select>
        </div>
        <button id="refreshBtn" style="
          padding:9px 12px; border:none; border-radius:999px;
          background:#0f172a; color:white; cursor:pointer; font-weight:800;
        ">Oppdater</button>
      </div>
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
        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/min-temp-kart?' + qs.toString();
      }}
      document.getElementById('refreshBtn').addEventListener('click', go);
      document.getElementById('countySel').addEventListener('change', go);
    </script>
    """
    folium.Element(header).add_to(m.get_root().html)

    legend = """
    <div style="
      position: fixed; top: 140px; right: 12px; z-index: 9999;
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

    # Heatmap: gi mer vekt til kaldt (klipp fra heat_clip_cold til f.eks. +10)
    # Vi mapper "kaldere => høyere vekt".
    clipped = d["value"].clip(lower=heat_clip_cold, upper=10.0)
    weights = (10.0 - clipped) / (10.0 - heat_clip_cold)  # 0..1
    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name="Heatmap", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.25, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

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

    folium.LayerControl().add_to(m)

    # Toppliste = kaldest N
    top = d.sort_values("value", ascending=True).head(int(top_n)).copy()
    rows_html: list[str] = []
    for i, r in enumerate(top.itertuples(index=False), start=1):
        sid = str(getattr(r, "sourceId"))
        nm = getattr(r, "name", None) or getattr(r, "shortName", None) or sid
        tc = float(getattr(r, "value"))
        rows_html.append(
            f"""
            <tr>
              <td style="padding:6px 8px; color:#64748b;">{i}</td>
              <td style="padding:6px 8px;">
                {nm}
                <div style="font-size:12px;color:#64748b;">{sid}</div>
              </td>
              <td style="padding:6px 8px; text-align:right; font-weight:900;">{tc:.1f} °C</td>
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
        <div style="font-weight:900;">Kaldest {int(top_n)} i fylket</div>
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
          <tbody>
            {''.join(rows_html)}
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
    </script>
    """
    folium.Element(topbox_html).add_to(m.get_root().html)

    return m.get_root().render()


# ======================================================================
# Hoved: bygg HTML for min-temp (dropdown -> fylke)
# ======================================================================

def build_min_temp_map_html(
    *,
    county: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
) -> str:
    if not county:
        return make_empty_map_with_dropdown()

    auth = _env_auth()

    # Vi henter siste ~2 døgn og plukker nyeste P1D per stasjon
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=2)
    referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"

    with requests.Session() as sess:
        src_meta = fetch_sources_in_county(sess, auth=auth, county=county, timeout=timeout)
        if src_meta.empty:
            return make_empty_map_with_dropdown(selected_county=county)

        sources = src_meta["baseId"].astype(str).tolist()

        # Prøv først "best_estimate_*" hvis tilgjengelig, ellers fall tilbake til standard "min(...)"
        element_candidates: list[str] = [
            "best_estimate_min(air_temperature P1D)",
            "min(air_temperature P1D)",
        ]

        obs = pd.DataFrame()
        element_used: str | None = None

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
        # Vis kartet med dropdown og “ingen data”
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
            Fant ingen døgn-min temperatur i siste 2 døgn for fylket <b>{county}</b>.
            Prøv et annet fylke.
          </div>
        </div>
        """
        folium.Element(msg).add_to(m.get_root().html)
        return m.get_root().render()

    latest = pick_latest_value_per_station(obs)
    latest["baseId"] = latest["sourceId"].astype(str).map(base_source_id)
    merged = latest.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "value"])

    if merged.empty:
        return make_empty_map_with_dropdown(selected_county=county)

    updated = now.strftime("%Y-%m-%d %H:%M UTC")
    used_txt = f" ({element_used})" if element_used else ""
    title = f"Minimumstemperatur – siste døgn ({county}){used_txt}<br><small>Oppdatert ca. {updated}</small>"

    return make_temp_map(
        merged,
        title=title,
        selected_county=county,
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
    args = ap.parse_args()

    html = build_min_temp_map_html(county=args.county or None)
    print(html[:700])


if __name__ == "__main__":
    main()
