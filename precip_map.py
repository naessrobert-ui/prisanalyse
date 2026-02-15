#!/usr/bin/env python3
from __future__ import annotations

import html as _html
import logging
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Literal

logger = logging.getLogger(__name__)

import pandas as pd
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# Local station DB (same as temperature flow)
from ver_station_db import stations_in_county

# --- .env loading -------------------------------------------------------
# Load .env next to this script, then fall back to working-directory .env
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

UNIT_MM = "mm"
Mode = Literal["last24h", "day", "mtd", "ytd"]
Rank = Literal["max", "min"]

WET_DAY_THRESHOLD_MM = 0.1  # antall dager med nedbør >= 0.1 mm

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

# Elementer
ELEMENT_PRECIP_DAY = "sum(precipitation_amount P1D)"      # døgnsum
ELEMENT_PRECIP_HOURLY = "sum(precipitation_amount PT1H)"  # timesum (summerer 24 timer for last24h)


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

    for attempt in range(1, retries + 1):
        r = session.get(
            url,
            params=params,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code in (404, 412):
            return r.json()

        if r.status_code == 429 or 500 <= r.status_code < 600:
            logger.warning(
                "Frost %s %s — attempt %d/%d, backing off %.1fs",
                r.status_code, url, attempt, retries, backoff,
            )
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
    # Quality codes may contain string suffixes; coerce to numeric but keep NaN rather than dropping
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


def aggregate_sum_per_station(
    df: pd.DataFrame,
    *,
    count_col: str,
    wet_day_threshold_mm: float = WET_DAY_THRESHOLD_MM,
) -> pd.DataFrame:
    """Summerer value over intervallet per stasjon + teller 'wet_days' (>= terskel)."""
    if df.empty:
        return df

    d2 = df.assign(wet_flag=(df["value"] >= wet_day_threshold_mm).astype(int))

    out = (
        d2.groupby("sourceId", as_index=False)
        .agg(
            value=("value", "sum"),
            n=("value", "size"),
            wet_days=("wet_flag", "sum"),
            rt_max=("referenceTime", "max"),
            qmin=("qualityCode", "min"),
        )
        .reset_index(drop=True)
    )
    out.rename(columns={"n": count_col}, inplace=True)
    out["unit"] = UNIT_MM
    out["referenceTime"] = out["rt_max"]
    out["qualityCode"] = out["qmin"]
    out.drop(columns=["rt_max", "qmin"], inplace=True)
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
      Dette går fortere når du velger fylke først. Hele landet kan ta litt tid.
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
    selected_mode: Mode = "last24h",
    selected_date: str = "",
    selected_top_n: int = 50,
    selected_rank: Rank = "max",
) -> str:
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    county_opts = "\n".join(
        [
            f'<option value="ALL" {"selected" if selected_county == "ALL" else ""}>Hele landet (tregere)</option>',
            *[
                f'<option value="{c}" {"selected" if c == selected_county else ""}>{c}</option>'
                for c in NORWAY_COUNTIES
            ],
        ]
    )

    mode_labels: dict[str, str] = {
        "last24h": "Siste 24 timer (rullerende)",
        "day": "Kalenderdøgn (valgt dato)",
        "mtd": "Hittil i måneden",
        "ytd": "Hittil i året",
    }
    mode_opts = "\n".join(
        f'<option value="{k}" {"selected" if k == selected_mode else ""}>{v}</option>'
        for k, v in mode_labels.items()
    )

    rank_opts = "\n".join(
        [
            f'<option value="max" {"selected" if selected_rank == "max" else ""}>Mest nedbør</option>',
            f'<option value="min" {"selected" if selected_rank == "min" else ""}>Minst nedbør</option>',
        ]
    )

    if not selected_date:
        selected_date = _date.today().isoformat()

    ui = f"""
    {_loading_overlay_js()}

    <div style="
      position: fixed; top: 12px; right: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 12px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 380px;
    ">
      <div style="font-weight:900; margin-bottom:8px;">Nedbør</div>
      <div style="font-size:13px; color:#334155; margin-bottom:10px;">
        Velg fylke, periode og evt. dato. (Dato brukes ikke for “Siste 24 timer”.)
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

      <label style="font-size:12px; color:#64748b;">Periode</label>
      <select id="modeSel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {mode_opts}
      </select>

      <div id="dateBox" style="margin-top:0; display:none;">
        <label style="font-size:12px; color:#64748b;">Dato</label>
        <input id="dateInput" type="date" value="{selected_date}" style="
          width:100%; margin-top:4px; margin-bottom:10px;
          padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        ">
      </div>

      <label style="font-size:12px; color:#64748b;">Hele landet: velg</label>
      <select id="rankSel" style="
        width:100%; margin-top:4px; margin-bottom:10px;
        padding:8px 10px; border:1px solid #e2e8f0; border-radius:10px;
        background:white;
      ">
        {rank_opts}
      </select>

      <label style="font-size:12px; color:#64748b;">Vis topp</label>
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
      </select>

      <button id="fetchBtn" style="
        margin-top:10px; width:100%;
        padding:9px 12px; border:none; border-radius:999px;
        background:#0f172a; color:white; cursor:pointer; font-weight:800;
      ">Hent</button>
    </div>

    <script>
      const modeSel = document.getElementById("modeSel");
      const dateBox = document.getElementById("dateBox");

      function syncModeUI() {{
        const m = modeSel.value || "last24h";
        dateBox.style.display = (m === "last24h") ? "none" : "block";
      }}
      modeSel.addEventListener("change", syncModeUI);
      syncModeUI();

      function go() {{
        const c = document.getElementById('countySel').value;
        if (!c) {{
          alert('Velg fylke først.');
          return;
        }}
        const m = document.getElementById('modeSel').value || 'last24h';

        const qs = new URLSearchParams();
        qs.set('county', c);
        qs.set('mode', m);
        const topN = parseInt(document.getElementById('topSel').value || '50', 10);
        qs.set('top', String(topN));

        const rank = document.getElementById('rankSel').value || 'max';
        qs.set('rank', rank);

        if (m !== "last24h") {{
          const d = document.getElementById("dateInput").value;
          if (!d) {{ alert("Velg dato"); return; }}
          qs.set("date", d);
        }}

        if (window.showLoading) window.showLoading();
        window.location.href = '/ver/nedbor-kart?' + qs.toString();
      }}
      document.getElementById('fetchBtn').addEventListener('click', go);
    </script>
    """
    folium.Element(ui).add_to(m.get_root().html)
    return m.get_root().render()


# ======================================================================
# Map rendering
# ======================================================================

def make_map(
    df: pd.DataFrame,
    *,
    title: str,
    selected_county: str,
    selected_mode: Mode,
    selected_date: str,
    selected_top_n: int,
    selected_rank: Rank,
    cluster: bool = True,
    heatmap_show: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_mm: float = 80.0,
    top_n: int = 10,
) -> str:
    if df.empty:
        return make_empty_map_with_dropdown(
            selected_county=selected_county,
            selected_mode=selected_mode,
            selected_date=selected_date,
            selected_top_n=selected_top_n,
            selected_rank=selected_rank,
        )

    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])
    if d.empty:
        return make_empty_map_with_dropdown(
            selected_county=selected_county,
            selected_mode=selected_mode,
            selected_date=selected_date,
            selected_top_n=selected_top_n,
            selected_rank=selected_rank,
        )

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")

    folium.Element(_loading_overlay_js()).add_to(m.get_root().html)

    # heatmap weights (clip)
    if heatmap_show:
        clipped = d["value"].clip(0, heat_clip_mm).astype(float)
        weights = (clipped / max(float(clipped.max() or 1.0), 1e-9)).fillna(0.0)
        hm_data = list(zip(d["lat"].tolist(), d["lon"].tolist(), weights.tolist()))
        HeatMap(hm_data, radius=heat_radius, blur=heat_blur, min_opacity=0.25).add_to(m)

    layer = MarkerCluster() if cluster else None
    if layer is not None:
        layer.add_to(m)

    marker_map: dict[str, str] = {}
    for _, r in d.iterrows():
        val = float(r["value"])
        name = _html.escape(str(r.get("name") or r.get("shortName") or r.get("sourceId") or ""))
        sid = _html.escape(str(r.get("sourceId") or ""))
        rt = r.get("referenceTime")
        rt_s = ""
        try:
            rt_s = pd.to_datetime(rt, utc=True).strftime("%Y-%m-%d %H:%M UTC") if rt is not None else ""
        except Exception:
            rt_s = str(rt or "")

        wet_days_line = ""
        if "wet_days" in d.columns and pd.notna(r.get("wet_days")):
            wet_days_line = (
                f"<br><small>Våte dager (≥{WET_DAY_THRESHOLD_MM:.1f} mm): "
                f"<b>{int(r['wet_days'])}</b></small>"
            )

        popup = folium.Popup(
            folium.IFrame(
                html=f"<b>{name}</b><br>{val:.1f} {UNIT_MM}{wet_days_line}<br><small>{rt_s}</small>",
                width=270,
                height=115 if wet_days_line else 90,
            ),
            max_width=310,
        )

        mk = folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=6,
            weight=1,
            fill=True,
            fill_opacity=0.85,
            popup=popup,
        )
        if layer is not None:
            mk.add_to(layer)
        else:
            mk.add_to(m)

        jsname = mk.get_name()
        if sid:
            marker_map[sid] = jsname

    # Mest / Minst lister
    most = d.sort_values("value", ascending=False).head(int(top_n)).copy()
    least = d.sort_values("value", ascending=True).head(int(top_n)).copy()

    def _rows(tbl: pd.DataFrame) -> str:
        rows: list[str] = []
        for _, rr in tbl.iterrows():
            sid = _html.escape(str(rr.get("sourceId") or ""))
            nm = _html.escape(str(rr.get("name") or rr.get("shortName") or sid))
            rows.append(
                f'<tr style="cursor:pointer;" onclick="focusStation(\'{sid}\')">'
                f"<td style='padding:6px 8px; border-bottom:1px solid #e2e8f0;'>{nm}</td>"
                f"<td style='padding:6px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:800;'>{float(rr['value']):.1f}</td>"
                f"</tr>"
            )
        return "".join(rows)

    rows_most = _rows(most)
    rows_least = _rows(least)

    topbox_html = f"""
    <div style="
      position: fixed; left: 12px; bottom: 12px; z-index: 9999;
      background: rgba(255,255,255,.95);
      padding: 10px 12px; border-radius: 12px;
      box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 460px;
    ">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="font-weight:900;">Stasjoner</div>
        <button onclick="toggleToplist()" style="
          border:none; border-radius:999px; padding:6px 10px;
          background:#0f172a; color:white; cursor:pointer; font-weight:800;
        ">Vis/skjul</button>
      </div>

      <div id="toplistBody" style="margin-top:8px; max-height: 280px; overflow:auto;">
        <div style="display:flex; gap:8px; margin-bottom:8px;">
          <button onclick="showMost()" style="border:none; border-radius:999px; padding:6px 10px; background:#0f172a; color:white; cursor:pointer; font-weight:800;">
            Mest nedbør
          </button>
          <button onclick="showLeast()" style="border:none; border-radius:999px; padding:6px 10px; background:#e2e8f0; color:#0f172a; cursor:pointer; font-weight:800;">
            Minst nedbør
          </button>
        </div>

        <table style="border-collapse:collapse; width:100%; font-size:13px;">
          <thead>
            <tr>
              <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #e2e8f0;">Stasjon</th>
              <th style="text-align:right; padding:6px 8px; border-bottom:1px solid #e2e8f0;">mm</th>
            </tr>
          </thead>

          <tbody id="tbodyMost">
            {rows_most}
          </tbody>
          <tbody id="tbodyLeast" style="display:none;">
            {rows_least}
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

      function showMost() {{
        const a = document.getElementById("tbodyMost");
        const b = document.getElementById("tbodyLeast");
        if (a) a.style.display = "table-row-group";
        if (b) b.style.display = "none";
      }}

      function showLeast() {{
        const a = document.getElementById("tbodyMost");
        const b = document.getElementById("tbodyLeast");
        if (a) a.style.display = "none";
        if (b) b.style.display = "table-row-group";
      }}
    </script>
    """
    folium.Element(js).add_to(m.get_root().html)

    # Title in top-left
    title_html = f"""
    <div style="
      position: fixed; top: 12px; left: 12px; z-index: 9999;
      background: rgba(255,255,255,.92);
      padding: 8px 12px; border-radius: 12px;
      box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 560px;
    ">
      <div style="font-weight:900;">{title}</div>
      <div style="font-size:12px; color:#475569;">
        Fylke: {selected_county} · Periode: {selected_mode}{(" · Hele landet: " + ("mest" if selected_rank=="max" else "minst")) if selected_county=="ALL" else ""}
      </div>
    </div>
    """
    folium.Element(title_html).add_to(m.get_root().html)

    return m.get_root().render()


# ======================================================================
# Hoved: bygg HTML (tomt eller med data)
# ======================================================================

def build_precip_county_map_html(
    *,
    county: Optional[str] = None,
    mode: Mode = "last24h",
    date_str: Optional[str] = None,
    top_n: int | str = 50,
    rank: Rank = "max",
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
    cluster: bool = True,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_mm: float = 80.0,
    session: Optional[requests.Session] = None,
) -> str:
    t0 = time.monotonic()
    day_str = date_str or _date.today().isoformat()

    try:
        top_n_i = int(top_n)  # type: ignore[arg-type]
    except Exception:
        top_n_i = 50
    top_n_i = max(1, min(top_n_i, 5000))

    if rank not in {"max", "min"}:
        rank = "max"

    # Shared kwargs for the empty-map fallback
    empty_kw: dict[str, Any] = dict(
        selected_county=county or "",
        selected_mode=mode,
        selected_date=day_str,
        selected_top_n=top_n_i,
        selected_rank=rank,
    )

    if not county:
        return make_empty_map_with_dropdown(**empty_kw)

    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"

    auth = _env_auth()
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
    else:  # ytd
        start = _date(day.year, 1, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = f"Nedbør hittil i året ({start.isoformat()} → {day.isoformat()})"
        sum_count_col = "n_days"

    # Allow caller to pass in a reusable session (avoids new TCP pool per call)
    owns_session = session is None
    sess = session or requests.Session()
    try:
        county_is_all = (county == "ALL")
        if county_is_all:
            frames = [stations_in_county(c) for c in NORWAY_COUNTIES]
            frames = [df for df in frames if df is not None and not df.empty]
            src_meta = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            src_meta = stations_in_county(county)

        if src_meta.empty:
            return make_empty_map_with_dropdown(**empty_kw)

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
    finally:
        if owns_session:
            sess.close()

    elapsed = time.monotonic() - t0
    logger.info("Data fetch completed in %.1fs (%d sources, mode=%s)", elapsed, len(sources) if 'sources' in dir() else 0, mode)

    if obs.empty:
        return make_empty_map_with_dropdown(**empty_kw)

    if mode == "day":
        picked = pick_day_value_per_station(obs, day=day)
        out = picked[["sourceId", "referenceTime", "value", "unit", "qualityCode"]].copy()
        # Våte dager for "day" blir 0/1 (basert på døgnsummen)
        out["wet_days"] = (out["value"] >= WET_DAY_THRESHOLD_MM).astype(int)
    else:
        out = aggregate_sum_per_station(obs, count_col=sum_count_col)
        # For last24h ønsker vi ikke å vise "våte dager" (det blir våte timer).
        if mode == "last24h" and "wet_days" in out.columns:
            out = out.drop(columns=["wet_days"])

    out["baseId"] = out["sourceId"].astype(str).map(base_source_id)
    merged = out.merge(src_meta, on="baseId", how="left").drop(columns=["baseId"])
    merged = merged.dropna(subset=["lat", "lon", "value"])

    if merged.empty:
        return make_empty_map_with_dropdown(**empty_kw)

    # For "Hele landet": plott kun topp N for fart, og la bruker velge mest/minst.
    if county == "ALL":
        asc = (rank == "min")
        merged = merged.sort_values("value", ascending=asc).head(top_n_i)

    total_elapsed = time.monotonic() - t0
    logger.info("Total build time: %.1fs", total_elapsed)

    return make_map(
        merged,
        title=title,
        selected_county=county,
        selected_mode=mode,
        selected_date=day_str if mode != "last24h" else "",
        selected_top_n=top_n_i,
        selected_rank=rank,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_mm=heat_clip_mm,
        top_n=min(10, len(merged)),
    )


# ======================================================================
# CLI entry point
# ======================================================================

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Generate precipitation map HTML")
    parser.add_argument("--county", default=None, help="County name or 'ALL'")
    parser.add_argument("--mode", default="last24h", choices=["last24h", "day", "mtd", "ytd"])
    parser.add_argument("--date", default=None, dest="date_str", help="Date (YYYY-MM-DD)")
    parser.add_argument("--top-n", default=50, type=int, help="Top N stations to show")
    parser.add_argument("--rank", default="max", choices=["max", "min"])
    parser.add_argument("-o", "--output", default="precip_map.html", help="Output HTML file")
    args = parser.parse_args()

    html_out = build_precip_county_map_html(
        county=args.county,
        mode=args.mode,
        date_str=args.date_str,
        top_n=args.top_n,
        rank=args.rank,
    )
    Path(args.output).write_text(html_out, encoding="utf-8")
    logger.info("Wrote %s", args.output)
