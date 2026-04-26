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
from typing import Any, Iterator, Optional, Literal

import pandas as pd
from ver_station_db import stations_in_county
from yr_forecast import FORECAST_MODES, TEMP_TITLES, fetch_temp_forecast
import requests
from dotenv import load_dotenv

try:
    from scripts.station_metrics_cache import load_metric as _load_station_metric
except ImportError:  # pragma: no cover - fallback when running as a script
    _load_station_metric = None  # type: ignore[assignment]

import folium
from folium.plugins import HeatMap, MarkerCluster


# --- .env loading -------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20
UNIT_C = "°C"

TempType = Literal["min", "max", "mean"]
Period = Literal["last", "day", "month", "year", "next24h", "next48h", "next7d"]

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

# Temperaturtyper
TEMP_TYPES: dict[TempType, dict[str, str]] = {
    "min": {"label": "Minimumstemperatur", "fn": "min"},
    "max": {"label": "Maksimumstemperatur", "fn": "max"},
    "mean": {"label": "Middeltemperatur", "fn": "mean"},
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
    df["qualityCode"] = pd.to_numeric(df.get("qualityCode"), errors="coerce")
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


def aggregate_ytd_temp_per_station(df: pd.DataFrame, temp: TempType) -> pd.DataFrame:
    """
    Aggreger daglige (P1D) temperaturer i en periode til en verdi per stasjon.
    Brukes for period=year (hittil i år / hele året).
    """
    if df.empty:
        return df

    d = df.dropna(subset=["sourceId", "referenceTime", "value"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    d = d.dropna(subset=["value"])
    d = d.sort_values(["sourceId", "referenceTime"])

    if temp == "min":
        out = d.groupby("sourceId", as_index=False).agg(
            referenceTime=("referenceTime", "max"),
            value=("value", "min"),
            qualityCode=("qualityCode", "min"),
        )
    elif temp == "max":
        out = d.groupby("sourceId", as_index=False).agg(
            referenceTime=("referenceTime", "max"),
            value=("value", "max"),
            qualityCode=("qualityCode", "min"),
        )
    else:  # mean
        out = d.groupby("sourceId", as_index=False).agg(
            referenceTime=("referenceTime", "max"),
            value=("value", "mean"),
            qualityCode=("qualityCode", "min"),
        )

    return out.reset_index(drop=True)


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


def _elements_for(temp: TempType, period: Period) -> list[str]:
    """Returnerer element-kandidater (best_estimate først) for valgt temp og periode."""
    fn = TEMP_TYPES[temp]["fn"]
    suffix = {"last": "P1D", "day": "P1D", "month": "P1M", "year": "P1Y"}[period]
    return [
        f"best_estimate_{fn}(air_temperature {suffix})",
        f"{fn}(air_temperature {suffix})",
    ]


# ======================================================================
# UI helpers
# ======================================================================

# Pedagogisk fast skala: temperatur (°C) -> farge. Brukes for både markører
# og fargestolpe-legenden, slik at samme blåfarge alltid betyr samme grad.
_TEMP_COLOR_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (-30.0, (30, 58, 138)),    # mørk blå
    (-15.0, (37, 99, 235)),    # blå
    (-5.0,  (96, 165, 250)),   # lys blå
    (0.0,   (103, 232, 249)),  # cyan
    (8.0,   (74, 222, 128)),   # grønn
    (15.0,  (250, 204, 21)),   # gul
    (22.0,  (249, 115, 22)),   # oransje
    (30.0,  (185, 28, 28)),    # rød
    (38.0,  (127, 29, 29)),    # mørk rød
]


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def color_for_temp(tc: float) -> str:
    """Lineær interpolasjon i RGB mellom faste temperaturstopp."""
    if tc <= _TEMP_COLOR_STOPS[0][0]:
        return _rgb_to_hex(_TEMP_COLOR_STOPS[0][1])
    if tc >= _TEMP_COLOR_STOPS[-1][0]:
        return _rgb_to_hex(_TEMP_COLOR_STOPS[-1][1])
    for (t1, c1), (t2, c2) in zip(_TEMP_COLOR_STOPS, _TEMP_COLOR_STOPS[1:]):
        if t1 <= tc <= t2:
            f = 0.0 if t2 == t1 else (tc - t1) / (t2 - t1)
            r = int(round(c1[0] + (c2[0] - c1[0]) * f))
            g = int(round(c1[1] + (c2[1] - c1[1]) * f))
            b = int(round(c1[2] + (c2[2] - c1[2]) * f))
            return _rgb_to_hex((r, g, b))
    return _rgb_to_hex(_TEMP_COLOR_STOPS[-1][1])


def _temp_legend_html() -> str:
    """Horisontal fargestolpe-legende (CSS-gradient) plassert nede til høyre."""
    stops = ", ".join(
        f"{_rgb_to_hex(rgb)} {((t - _TEMP_COLOR_STOPS[0][0]) / (_TEMP_COLOR_STOPS[-1][0] - _TEMP_COLOR_STOPS[0][0])) * 100:.1f}%"
        for t, rgb in _TEMP_COLOR_STOPS
    )
    ticks = [-30, -20, -10, 0, 10, 20, 30]
    tick_html = "".join(
        f'<span style="position:absolute; left:{((t - _TEMP_COLOR_STOPS[0][0]) / (_TEMP_COLOR_STOPS[-1][0] - _TEMP_COLOR_STOPS[0][0])) * 100:.1f}%; transform:translateX(-50%); top:14px; font-size:11px; color:#334155;">{t:+d}°</span>'
        for t in ticks
    )
    return f"""
    <div style="
      position: fixed; bottom: 12px; right: 12px; z-index: 9998;
      background: rgba(255,255,255,.97); padding: 10px 14px 22px 14px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      width: 280px;
    ">
      <div style="font-size:12px; font-weight:700; color:#0f172a; margin-bottom:6px;">
        Temperaturskala
      </div>
      <div style="position:relative; height:12px; border-radius:6px;
        background: linear-gradient(to right, {stops});
        box-shadow: inset 0 0 0 1px rgba(15,23,42,.1);">
        {tick_html}
      </div>
    </div>
    """


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


def make_empty_map_with_dropdown(
    *,
    selected_county: str = "",
    selected_temp: TempType = "min",
    selected_period: Period = "last",
    selected_top_n: int = 20,
) -> str:
    """Tomt kart med fylke + temperaturtype + periode. Ingen API-kall før 'Hent'."""
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    county_opts = "\n".join(
        [
            f'<option value="ALL" {"selected" if selected_county == "ALL" else ""}>Hele landet</option>',
            *[
                f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
                for c in NORWAY_COUNTIES
            ],
        ]
    )

    temp_opts = "\n".join(
        f'<option value="{k}" {"selected" if k == selected_temp else ""}>{v["label"]}</option>'
        for k, v in TEMP_TYPES.items()
    )

    period_opts: dict[Period, str] = {
        "last": "Siste døgn",
        "day": "Valgt døgn",
        "month": "Valgt måned",
        "year": "Valgt år (hittil nå hvis inneværende år)",
        "next24h": "Forventet neste 24 timer (Yr)",
        "next48h": "Forventet neste 48 timer (Yr)",
        "next7d": "Forventet neste 7 dager (Yr)",
    }
    period_html = "\n".join(
        f'<option value="{k}" {"selected" if k == selected_period else ""}>{v}</option>'
        for k, v in period_opts.items()
    )

    ui = f"""
    {_loading_overlay_js()}

    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 12px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 380px;
    ">
      <div style="font-weight:900; margin-bottom:8px;">Temperatur</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Velg fylke, temperaturtype og periode.
      </div>

      <label style="font-size:12px; color:#64748b;">Fylke</label>
      <select id="countySel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        <option value="">– Velg –</option>
        {county_opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Temperatur</label>
      <select id="tempSel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {temp_opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Periode</label>
      <select id="periodSel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {period_html}
      </select>

      <label style="font-size:12px; color:#64748b; margin-top:10px;">Vis topp</label>
      <select id="topSel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        <option value="20" {"selected" if selected_top_n == 20 else ""}>20</option>
        <option value="50" {"selected" if selected_top_n == 50 else ""}>50</option>
        <option value="100" {"selected" if selected_top_n == 100 else ""}>100</option>
        <option value="200" {"selected" if selected_top_n == 200 else ""}>200</option>
        <option value="500" {"selected" if selected_top_n == 500 else ""}>500</option>
        <option value="0" {"selected" if selected_top_n == 0 else ""}>Alle (kun «Hele landet»)</option>
      </select>

      <div id="dayBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">Dato</label>
        <input id="dayInput" type="date" value="{_date.today().isoformat()}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <div id="monthBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">Måned</label>
        <input id="monthInput" type="month" value="{datetime.now(timezone.utc).strftime("%Y-%m")}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <div id="yearBox" style="margin-top:8px; display:none;">
        <label style="font-size:12px; color:#64748b;">År</label>
        <input id="yearInput" type="number" min="1900" max="2100" value="{datetime.now(timezone.utc).strftime("%Y")}" style="
          width:100%; margin-top:4px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <button id="fetchBtn" style="
        margin-top:10px; width:100%;
        padding:9px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer; font-weight:800;
      ">Hent</button>
    </div>

    <script>
      const periodSel = document.getElementById("periodSel");
      const dayBox = document.getElementById("dayBox");
      const monthBox = document.getElementById("monthBox");
      const yearBox = document.getElementById("yearBox");

      function syncPeriodUI() {{
        const p = periodSel.value || "last";
        const isForecast = ["next24h", "next48h", "next7d"].includes(p);
        dayBox.style.display = (!isForecast && p === "day") ? "block" : "none";
        monthBox.style.display = (!isForecast && p === "month") ? "block" : "none";
        yearBox.style.display = (!isForecast && p === "year") ? "block" : "none";
      }}
      periodSel.addEventListener("change", syncPeriodUI);
      syncPeriodUI();

      function go() {{
        const c = document.getElementById('countySel').value;
        if (!c) {{
          alert('Velg fylke først.');
          return;
        }}
        const t = document.getElementById('tempSel').value || 'min';
        const p = document.getElementById('periodSel').value || 'last';

        const qs = new URLSearchParams();
        qs.set('county', c);
        qs.set('temp', t);
        qs.set('period', p);
        const topN = parseInt(document.getElementById('topSel').value || '20', 10);
        qs.set('top', String(topN));

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
    selected_temp: TempType,
    selected_period: Period,
    selected_date: str,
    selected_month: str,
    selected_year: str,
    selected_top_n: int = 20,
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

    def color_for(tc: float) -> str:
        return color_for_temp(tc)

    def radius_for(tc: float) -> float:
        # Med fast fargeskala bærer fargen all info — hold radius lik for
        # ryddig kart, men la outliere være litt større.
        med = float(vals.median())
        strength = abs(tc - med)
        return float(max(5.0, min(5.0 + 0.4 * math.sqrt(max(strength, 0.0)), 9.0)))

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")
    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)
    folium.Element(_temp_legend_html()).add_to(m.get_root().html)
    map_var = m.get_name()

    # Header: fylke + temp + periode + inputs
    county_opts = "\n".join(
        [
            f'<option value="ALL" {"selected" if selected_county == "ALL" else ""}>Hele landet</option>',
            *[
                f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
                for c in NORWAY_COUNTIES
            ],
        ]
    )
    temp_opts = "\n".join(
        f'<option value="{k}" {"selected" if k == selected_temp else ""}>{v["label"]}</option>'
        for k, v in TEMP_TYPES.items()
    )
    period_opts: dict[Period, str] = {
        "last": "Siste døgn",
        "day": "Valgt døgn",
        "month": "Valgt måned",
        "year": "Valgt år (hittil nå hvis inneværende år)",
        "next24h": "Forventet neste 24 timer (Yr)",
        "next48h": "Forventet neste 48 timer (Yr)",
        "next7d": "Forventet neste 7 dager (Yr)",
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
      max-width: 430px;
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
        {county_opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Temperatur</label>
      <select id="tempSel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {temp_opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Periode</label>
      <select id="periodSel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {period_html}
      </select>

      <label style="font-size:12px; color:#64748b; margin-top:10px;">Vis topp</label>
      <select id="topSel" style="
        width:100%; margin-top:4px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        <option value="20" {"selected" if selected_top_n == 20 else ""}>20</option>
        <option value="50" {"selected" if selected_top_n == 50 else ""}>50</option>
        <option value="100" {"selected" if selected_top_n == 100 else ""}>100</option>
        <option value="200" {"selected" if selected_top_n == 200 else ""}>200</option>
        <option value="500" {"selected" if selected_top_n == 500 else ""}>500</option>
        <option value="0" {"selected" if selected_top_n == 0 else ""}>Alle (kun «Hele landet»)</option>
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
        const isForecast = ["next24h", "next48h", "next7d"].includes(p);
        dayBox.style.display = (!isForecast && p === "day") ? "block" : "none";
        monthBox.style.display = (!isForecast && p === "month") ? "block" : "none";
        yearBox.style.display = (!isForecast && p === "year") ? "block" : "none";
      }}
      periodSel.addEventListener("change", syncPeriodUI);
      syncPeriodUI();

      function go() {{
        const c = document.getElementById('countySel').value;
        if (!c) {{
          alert('Velg fylke først.');
          return;
        }}
        const t = document.getElementById('tempSel').value || 'min';
        const p = document.getElementById('periodSel').value || 'last';

        const qs = new URLSearchParams();
        qs.set('county', c);
        qs.set('temp', t);
        qs.set('period', p);
        const topN = parseInt(document.getElementById('topSel').value || '20', 10);
        qs.set('top', String(topN));

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

    # Heatmap-vekting: "ekstremt" i valgt retning
    v = d["value"].astype(float)
    if selected_temp == "min":
        clipped = v.clip(lower=heat_clip_cold, upper=10.0)
        weights = (10.0 - clipped) / (10.0 - heat_clip_cold)
    elif selected_temp == "max":
        clipped = v.clip(lower=-10.0, upper=30.0)
        weights = (clipped - (-10.0)) / (30.0 - (-10.0))
    else:
        med = float(v.median())
        dev = (v - med).abs()
        maxd = float(dev.max()) if float(dev.max()) > 0 else 1.0
        weights = (dev / maxd).clip(lower=0.0, upper=1.0)

    heat_data = [[float(lat), float(lon), float(wt)] for lat, lon, wt in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name="Heatmap", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.25, max_zoom=10).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    points_js: list[dict[str, Any]] = []

    for _, r in d.iterrows():
        tc = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or UNIT_C
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"
        qc = r.get("qualityCode")
        qc_str = f"{int(qc)}" if pd.notna(qc) else "ukjent"

        html = f"{name}<br>Temperatur: <b>{tc:.1f} {unit}</b><br>Tid: {t_str}<br>Kvalitet: {qc_str}"

        col = color_for(tc)
        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=radius_for(tc),
            color="#0f172a",
            weight=1,
            fill=True,
            fill_color=col,
            fill_opacity=0.95,
            opacity=0.6,
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

    points_json = json.dumps(points_js, ensure_ascii=False)
    default_sort = "warm" if selected_temp == "max" else "cold"

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
          <span id="topTitle">Oppdaterer…</span>
        </div>

        <div style="display:flex; gap:8px; align-items:center;">
          <select id="topSortSel" style="
            padding:6px 10px; border-radius:999px; border:1px solid #e2e8f0;
            background:white; font-weight:700; font-size:13px; color:#0f172a;
          ">
            <option value="cold" {"selected" if default_sort=="cold" else ""}>Kaldest først</option>
            <option value="warm" {"selected" if default_sort=="warm" else ""}>Varmest først</option>
          </select>

          <button onclick="toggleToplist()" style="border:none;background:#e2e8f0;border-radius:999px;padding:6px 10px;cursor:pointer;">
            Vis/skjul
          </button>
        </div>
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

      function getMap() {{
        try {{
          if (typeof {map_var} !== "undefined") return {map_var};
        }} catch (e) {{}}
        return null;
      }}

      window._tempPoints = {points_json};

      function updateToplist(map) {{
        const b = map.getBounds();
        const inView = window._tempPoints.filter(p => b.contains([p.lat, p.lon]));

        const sel = document.getElementById("topSortSel");
        const mode = sel ? (sel.value || "cold") : "cold";
        const desc = (mode === "warm");  // warm => høyeste først

        inView.sort((a,b) => desc ? (b.value - a.value) : (a.value - b.value));

        const TOPN = {int(top_n)};
        const showAll = TOPN === 0;
        const limit = showAll ? Math.min(inView.length, 200) : TOPN;
        const top = inView.slice(0, limit);
        const tb = document.getElementById("toplistTbody");
        const tt = document.getElementById("topTitle");
        if (!tb) return;

        if (tt) {{
          const prefix = desc ? "Varmest" : "Kaldest";
          if (showAll) {{
            const shownTxt = inView.length > 200 ? " (viser første 200)" : "";
            tt.textContent = prefix + " · " + inView.length + " stasjoner i utsnittet" + shownTxt;
          }} else {{
            tt.textContent = prefix + " " + TOPN + " i utsnittet (" + inView.length + " stasjoner)";
          }}
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
        let tries = 0;
        function tick() {{
          const map = getMap();
          if (!map) {{
            tries += 1;
            if (tries < 60) setTimeout(tick, 150);
            return;
          }}

          const sel = document.getElementById("topSortSel");
          if (sel) sel.addEventListener("change", () => updateToplist(map));

          map.on("moveend", () => updateToplist(map));
          map.on("zoomend", () => updateToplist(map));
          updateToplist(map);
        }}
        if (document.readyState === "loading") {{
          document.addEventListener("DOMContentLoaded", tick);
        }} else {{
          tick();
        }}
      }})();
    </script>
    """
    folium.Element(topbox_html).add_to(m.get_root().html)

    return m.get_root().render()


# ======================================================================
# Hoved: bygg HTML
# ======================================================================

def build_min_temp_map_html(
    *,
    county: Optional[str] = None,
    temp: TempType = "min",
    period: Period = "last",
    date_str: Optional[str] = None,   # YYYY-MM-DD for day
    month_str: Optional[str] = None,  # YYYY-MM for month
    year_str: Optional[str] = None,   # YYYY for year
    top_n: int = 20,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
) -> str:
    if not county:
        return make_empty_map_with_dropdown(selected_temp=temp, selected_period=period, selected_top_n=top_n)

    if temp not in TEMP_TYPES:
        temp = "min"
    if period not in {"last", "day", "month", "year", "next24h", "next48h", "next7d"}:
        period = "last"

    try:
        top_n = int(top_n)
    except Exception:
        top_n = 20
    # 0 = vis alle stasjoner (kun meningsfullt ved county_is_all)
    top_n = max(0, min(top_n, 5000))

    now = datetime.now(timezone.utc)

    # UI defaults
    ui_date = date_str or _date.today().isoformat()
    ui_month = month_str or now.strftime("%Y-%m")
    ui_year = year_str or now.strftime("%Y")


    county_is_all = (county == "ALL")
    if county_is_all:
        frames = [stations_in_county(c) for c in NORWAY_COUNTIES]
        frames = [df for df in frames if df is not None and not df.empty]
        src_meta = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        src_meta = stations_in_county(county)

    if src_meta.empty:
        return make_empty_map_with_dropdown(
            selected_county=county,
            selected_temp=temp,
            selected_period=period,
            selected_top_n=top_n,
        )

    # ---- Forsøk å lese fra precomputed station-metrics-cache. -----------
    # Cachen dekker {last, next24h, next48h, next7d}. Andre perioder
    # (day/month/year) faller fortsatt tilbake til live API under.
    if _load_station_metric is not None and period in {"last", "next24h", "next48h", "next7d"}:
        metric_name = {"min": "temp_min", "max": "temp_max", "mean": "temp_mean"}[temp]
        wanted_counties = NORWAY_COUNTIES if county_is_all else [county]
        try:
            cached = _load_station_metric(metric_name, period, counties=wanted_counties)
        except Exception:
            cached = pd.DataFrame()
        if not cached.empty:
            cached = cached.dropna(subset=["lat", "lon", "value"]).copy()
            if county_is_all and top_n > 0:
                if temp == "min":
                    cached = cached.nsmallest(top_n, "value")
                else:
                    cached = cached.nlargest(top_n, "value")
            if not cached.empty:
                updated = now.strftime("%Y-%m-%d %H:%M UTC")
                if period in FORECAST_MODES:
                    title = (
                        f"{TEMP_TITLES[temp][period]} – {county}"
                        f"<br><small>Oppdatert ca. {updated}</small>"
                    )
                    element_used_label = "YR locationforecast (cache)"
                else:
                    title = (
                        f'{TEMP_TYPES[temp]["label"]} – siste døgn ({county})'
                        f"<br><small>Oppdatert ca. {updated}</small>"
                    )
                    element_used_label = (
                        f'{TEMP_TYPES[temp]["fn"]}(air_temperature P1D) (cache)'
                    )
                return make_temp_map(
                    cached,
                    title=title,
                    selected_county=county,
                    selected_temp=temp,
                    selected_period=period,
                    selected_date=ui_date,
                    selected_month=ui_month,
                    selected_year=ui_year,
                    selected_top_n=top_n,
                    element_used=element_used_label,
                    cluster=True,
                    heatmap_show=True,
                    heat_radius=25,
                    heat_blur=18,
                    top_n=top_n,
                )
        # Cache-miss → faller tilbake til live API under.

    if period in FORECAST_MODES:
        merged = fetch_temp_forecast(src_meta, mode=period, temp_kind=temp, timeout=timeout)
        if merged.empty:
            return make_empty_map_with_dropdown(
                selected_county=county,
                selected_temp=temp,
                selected_period=period,
                selected_top_n=top_n,
            )

        if county_is_all and top_n > 0:
            if temp == "min":
                merged = merged.nsmallest(top_n, "value")
            else:
                merged = merged.nlargest(top_n, "value")

        if merged.empty:
            return make_empty_map_with_dropdown(
                selected_county=county,
                selected_temp=temp,
                selected_period=period,
                selected_top_n=top_n,
            )

        updated = now.strftime("%Y-%m-%d %H:%M UTC")
        title = f"{TEMP_TITLES[temp][period]} – {county}<br><small>Oppdatert ca. {updated}</small>"

        return make_temp_map(
            merged,
            title=title,
            selected_county=county,
            selected_temp=temp,
            selected_period=period,
            selected_date=ui_date,
            selected_month=ui_month,
            selected_year=ui_year,
            selected_top_n=top_n,
            element_used="YR locationforecast",
            cluster=True,
            heatmap_show=True,
            heat_radius=25,
            heat_blur=18,
            top_n=top_n,
        )

    # referencetime + title
    if period == "day":
        day = _date.fromisoformat(ui_date)
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title_base = f'{TEMP_TYPES[temp]["label"]} – døgn ({ui_date}) – {county}'

    elif period == "month":
        start, end = _month_bounds_utc(ui_month)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        title_base = f'{TEMP_TYPES[temp]["label"]} – måned ({ui_month}) – {county}'

    elif period == "year":
        # ✅ NYTT: "Valgt år" = hittil i år dersom inneværende år, ellers hele året.
        yy = int(ui_year)
        start = datetime(yy, 1, 1, tzinfo=timezone.utc)
        today = _date.today()
        if yy == today.year:
            # slutt eksklusiv => i morgen 00:00 UTC
            end = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) + timedelta(days=1)
            title_base = f'{TEMP_TYPES[temp]["label"]} – hittil i år ({yy}) – {county}'
        else:
            end = datetime(yy + 1, 1, 1, tzinfo=timezone.utc)
            title_base = f'{TEMP_TYPES[temp]["label"]} – år ({yy}) – {county}'
        referencetime = f"{start.isoformat()}/{end.isoformat()}"

    else:
        # last: hent siste ~2 døgn og plukk nyeste P1D per stasjon
        start_dt = now - timedelta(days=2)
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        title_base = f'{TEMP_TYPES[temp]["label"]} – siste døgn ({county})'

    # ✅ Element-kandidater:
    # - year: bruk P1D (ikke P1Y) og aggregér selv
    # - ellers: som før
    if period == "year":
        element_candidates = _elements_for(temp, "day")  # P1D
    else:
        element_candidates = _elements_for(temp, period)

    auth = _env_auth()

    with requests.Session() as sess:
        sources = src_meta["baseId"].astype(str).tolist()

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
            Prøv annen temperaturtype eller periode.
          </div>
        </div>
        """
        folium.Element(msg).add_to(m.get_root().html)
        return m.get_root().render()

    # plukk/agg per stasjon
    if period == "day":
        picked = pick_value_in_day(obs, day=_date.fromisoformat(ui_date))
    elif period == "year":
        picked = aggregate_ytd_temp_per_station(obs, temp=temp)
    else:
        picked = pick_latest_value_per_station(obs)

    if picked.empty:
        return make_empty_map_with_dropdown(
            selected_county=county,
            selected_temp=temp,
            selected_period=period,
            selected_top_n=top_n,
        )

    picked["baseId"] = picked["sourceId"].astype(str).map(base_source_id)
    merged = picked.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "value"])

    if county == "ALL" and top_n > 0:
        # For "Hele landet": plott kun topp N for å holde kartet raskt
        if temp == "min":
            merged = merged.nsmallest(top_n, "value")
        else:
            merged = merged.nlargest(top_n, "value")

    if merged.empty:
        return make_empty_map_with_dropdown(
            selected_county=county,
            selected_temp=temp,
            selected_period=period,
            selected_top_n=top_n,
        )

    updated = now.strftime("%Y-%m-%d %H:%M UTC")
    used_txt = f" ({element_used})" if element_used else ""
    title = f"{title_base}{used_txt}<br><small>Oppdatert ca. {updated}</small>"

    return make_temp_map(
        merged,
        title=title,
        selected_county=county,
        selected_temp=temp,
        selected_period=period,
        selected_date=ui_date,
        selected_month=ui_month,
        selected_year=ui_year,
        selected_top_n=top_n,
        element_used=element_used or "(ukjent)",
        cluster=True,
        heatmap_show=True,
        heat_radius=25,
        heat_blur=18,
        top_n=top_n,
    )


# ======================================================================
# CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Temperatur-kart (velg fylke).")
    ap.add_argument("--county", default="", help="Fylke, f.eks. 'Innlandet'")
    ap.add_argument("--temp", default="min", choices=["min", "max", "mean"])
    ap.add_argument("--period", default="last", choices=["last", "day", "month", "year", "next24h", "next48h", "next7d"])
    ap.add_argument("--date", default="", help="YYYY-MM-DD (for period=day)")
    ap.add_argument("--month", default="", help="YYYY-MM (for period=month)")
    ap.add_argument("--year", default="", help="YYYY (for period=year)")
    ap.add_argument("--top", default="20", help="topp N ved ALL")
    args = ap.parse_args()

    html = build_min_temp_map_html(
        county=args.county or None,
        temp=args.temp,  # type: ignore[arg-type]
        period=args.period,  # type: ignore[arg-type]
        date_str=args.date or None,
        month_str=args.month or None,
        year_str=args.year or None,
        top_n=int(args.top) if args.top.isdigit() else 20,
    )
    print(html[:700])


if __name__ == "__main__":
    main()
