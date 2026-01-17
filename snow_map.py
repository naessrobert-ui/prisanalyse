#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Tuple

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# NYTT: stasjonsutvalg fra parquet (has_snow)
from ver_station_db import load_station_db, stations_in_bbox_swne_with

# --- .env loading (robust på Windows/PyCharm) ----------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

ELEMENT_SNOW = "surface_snow_thickness"  # snødybde (cm)

# ----------------------------------------------------------------------
#  Enkle caches i minne for å unngå unødvendige kall i samme prosess
# ----------------------------------------------------------------------
_LATEST_DF_CACHE: dict[tuple, tuple[datetime, pd.DataFrame]] = {}
LATEST_TTL_SECONDS = 3600  # 1 time


# ======================================================================
#  Auth / Frost helpers
# ======================================================================

@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError(
            "Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env + python-dotenv)."
        )
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
    """Robust paginering for Frost v0 basert på offset/itemsPerPage/totalItemCount."""
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


def _parse_bbox(bbox: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parse bbox 'south,west,north,east' -> tuple."""
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


REGION_BBOX: dict[str, Tuple[float, float, float, float]] = {
    "south": (57.0, 4.0, 62.5, 12.5),
    "mid": (62.0, 4.0, 66.7, 16.5),
    "north": (66.3, 10.0, 71.5, 31.5),
    "all": (57.0, 4.0, 71.5, 31.5),
    # hvis du vil: "vestland": (59.4754202, 4.0875274, 62.3823948, 8.322053),
}


# ======================================================================
#  Observasjoner
# ======================================================================

def fetch_observations_interval(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    referencetime: str,
    timeout: int,
    batch_size: int,
    limit: int = 1000,
    qualities: str = "",
) -> pd.DataFrame:
    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    if not sources:
        return pd.DataFrame()

    for batch in chunked(sources, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": ELEMENT_SNOW,
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
    return df


def choose_nearest_per_baseid(df: pd.DataFrame, *, target_day: _date) -> pd.DataFrame:
    """Velg nærmeste observasjon til target_day (00:00 UTC) per baseId."""
    if df.empty:
        return df

    target_dt = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    target_ts = pd.Timestamp(target_dt)

    d = df.dropna(subset=["referenceTime", "value", "baseId"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["value"])

    d["abs_diff_s"] = (d["referenceTime"] - target_ts).abs().dt.total_seconds()
    d = (
        d.sort_values(["baseId", "abs_diff_s", "referenceTime"], ascending=[True, True, False])
        .groupby(["baseId"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    d["diff_hours"] = (d["referenceTime"] - target_ts).dt.total_seconds() / 3600.0
    return d.drop(columns=["abs_diff_s"])


def fetch_latest_snow_for_sources(
    session: requests.Session,
    *,
    auth: FrostAuth,
    source_base_ids: list[str],
    timeout: int,
    batch_size: int,
    qualities: str = "0,1,2,3,4",
) -> pd.DataFrame:
    if not source_base_ids:
        return pd.DataFrame()

    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    for batch in chunked(source_base_ids, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": "latest",
            "elements": ELEMENT_SNOW,
            "limit": 1,
            "timeoffsets": "default",
            "levels": "default",
        }
        if qualities:
            params["qualities"] = qualities

        data = frost_get_json(session, path, params, auth=auth, timeout=timeout)
        if data.get("@type") == "ErrorResponse":
            continue

        for item in data.get("data", []):
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
    return df.dropna(subset=["referenceTime", "value"])


def fetch_snow_for_baseids_with_fallback(
    session: requests.Session,
    *,
    auth: FrostAuth,
    base_ids: list[str],
    target_day: _date,
    window_days: int,
    timeout: int,
    batch_size: int,
    limit: int = 1000,
    qualities: str = "",
) -> pd.DataFrame:
    if not base_ids:
        return pd.DataFrame()

    day_rt = f"{target_day.isoformat()}/{(target_day + timedelta(days=1)).isoformat()}"

    df_day = fetch_observations_interval(
        session,
        auth=auth,
        sources=base_ids,
        referencetime=day_rt,
        timeout=timeout,
        batch_size=batch_size,
        limit=limit,
        qualities=qualities,
    )

    df_fb = pd.DataFrame()
    if window_days > 0:
        fb_start = target_day - timedelta(days=window_days)
        fb_end = target_day + timedelta(days=window_days + 1)
        fb_rt = f"{fb_start.isoformat()}/{fb_end.isoformat()}"
        df_fb = fetch_observations_interval(
            session,
            auth=auth,
            sources=base_ids,
            referencetime=fb_rt,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

    df_all = pd.concat([df_day, df_fb], ignore_index=True)
    if df_all.empty:
        return df_all

    df_all["baseId"] = df_all["sourceId"].astype(str).map(base_source_id)
    return choose_nearest_per_baseid(df_all, target_day=target_day)


# ======================================================================
#  Stasjoner fra parquet (has_snow)
# ======================================================================

def _stations_meta_for_bbox(
    bbox_coords: Optional[Tuple[float, float, float, float]]
) -> pd.DataFrame:
    """
    Returnerer stasjoner (metadata) fra parquet, filtrert på bbox og has_snow=True.
    Output-kolonner minst: baseId,name,shortName,country,lat,lon
    """
    if bbox_coords is not None:
        st = stations_in_bbox_swne_with(bbox_coords, require_snow=True)
    else:
        st = load_station_db()
        if not st.empty:
            st = st[st["has_snow"] == True].copy()

    if st.empty:
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "lat", "lon"])

    keep = [c for c in ["baseId", "name", "shortName", "country", "lat", "lon"] if c in st.columns]
    st = st[keep].copy()
    st["lat"] = pd.to_numeric(st["lat"], errors="coerce")
    st["lon"] = pd.to_numeric(st["lon"], errors="coerce")
    st = st.dropna(subset=["baseId", "lat", "lon"]).drop_duplicates(subset=["baseId"], keep="first")
    return st.reset_index(drop=True)


# ======================================================================
#  Kartbygging (uendret)
# ======================================================================

def _color_cm_quantile(cm: float, p10: float, p90: float, blue_threshold: float = 50.0) -> str:
    try:
        v = float(cm)
    except Exception:
        return "gray"
    if not np.isfinite(v):
        return "gray"
    if v <= p10:
        return "#9ca3af"
    if v >= max(p90, blue_threshold):
        return "#2563eb"
    if v >= blue_threshold:
        return "#60a5fa"
    if v >= 70:
        return "#f59e0b"
    if v >= 40:
        return "#fbbf24"
    if v >= 20:
        return "#22c55e"
    return "#86efac"


def _heat_params_for_zoom(z: int | None) -> tuple[int, int]:
    if z is None:
        return 18, 14
    if z <= 4:
        return 30, 22
    if z == 5:
        return 26, 20
    if z == 6:
        return 22, 18
    if z == 7:
        return 18, 16
    if z == 8:
        return 16, 14
    if z == 9:
        return 14, 12
    return 12, 10


def make_map(
    df: pd.DataFrame,
    *,
    out_html: Optional[str] = None,
    cluster: bool,
    heatmap_show: bool,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 0.0,
    zoom_start: Optional[int] = None,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    bbox_coords: Optional[Tuple[float, float, float, float]] = None,
) -> str:
    if df.empty:
        m = folium.Map(location=[65.0, 13.0], zoom_start=4, tiles="OpenStreetMap")
        folium.LayerControl().add_to(m)
        html_str = m.get_root().render()
        if out_html:
            m.save(out_html)
        return html_str

    d = df.copy()
    for c in ("lat", "lon", "value"):
        if c not in d.columns:
            raise RuntimeError(f"Mangler kolonne '{c}' i df.")
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])
    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    is_change = ("delta_cm" in d.columns) or ("value_latest" in d.columns and "value_since" in d.columns)

    if center_lat is None:
        center_lat = float(d["lat"].mean())
    if center_lon is None:
        center_lon = float(d["lon"].mean())
    if zoom_start is None:
        zoom_start = 5

    m = folium.Map(location=[center_lat, center_lon], zoom_start=int(zoom_start), tiles="OpenStreetMap")

    if bbox_coords is not None:
        south, west, north, east = bbox_coords
        try:
            m.fit_bounds([[south, west], [north, east]])
        except Exception:
            pass

    if is_change:
        vals = d["value"].abs().clip(lower=0.0)
    else:
        vals = d["value"].clip(lower=0.0)

    if heat_clip_cm and heat_clip_cm > 0:
        clip_cm = float(heat_clip_cm)
    else:
        q = float(vals.quantile(0.98)) if len(vals) else 1.0
        clip_cm = max(q, 10.0)

    clipped = vals.clip(upper=clip_cm)
    weights = (clipped / clip_cm).clip(0.0, 1.0) ** 0.7
    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]

    SNOW_GRADIENT = {
        0.00: "#08306b",
        0.20: "#2171b5",
        0.40: "#41b6c4",
        0.55: "#ffffff",
        0.70: "#c7a9ff",
        0.85: "#7a3db8",
        1.00: "#d73027",
    }

    zr, zb = _heat_params_for_zoom(int(zoom_start) if zoom_start is not None else None)
    heat_radius = int(zr) if zr else int(heat_radius)
    heat_blur = int(zb) if zb else int(heat_blur)

    heat_layer = folium.FeatureGroup(
        name=("Endring (styrke)" if is_change else "Snødybde (interpolert)"),
        show=heatmap_show,
    )
    HeatMap(
        heat_data,
        radius=heat_radius,
        blur=heat_blur,
        min_opacity=0.20,
        gradient=SNOW_GRADIENT,
        max_zoom=10,
    ).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)

    if cluster:
        icon_create_function = """
        function(cluster) {
          var children = cluster.getAllChildMarkers();
          var vals = [];
          for (var i=0; i<children.length; i++){
            var v = children[i].options && children[i].options.snow;
            if (v !== undefined && v !== null && !isNaN(v)) vals.push(Number(v));
          }
          if (vals.length === 0){
            return L.divIcon({
              html: '<div class="snow-cluster snow-cluster-empty">-</div>',
              className: '',
              iconSize: new L.Point(38, 38)
            });
          }

          vals.sort(function(a,b){return a-b;});
          var maxv = vals[vals.length-1];
          var mid = Math.floor(vals.length/2);
          var med = (vals.length % 2) ? vals[mid] : (vals[mid-1]+vals[mid])/2;

          var maxcm = Math.round(maxv);
          var medcm = Math.round(med);

          var cls = 'snow-cluster-mid';
          if (maxcm >= 50) cls = 'snow-cluster-high';
          else if (maxcm <= 10) cls = 'snow-cluster-low';

          var html =
            '<div class="snow-cluster ' + cls + '">' +
              '<div class="big">' + maxcm + '</div>' +
              '<div class="small">med ' + medcm + '</div>' +
              '<div class="unit">cm</div>' +
            '</div>';

          return L.divIcon({ html: html, className: '', iconSize: new L.Point(52, 52) });
        }
        """
        layer_for_markers = MarkerCluster(
            icon_create_function=icon_create_function,
            options={"maxClusterRadius": 35, "disableClusteringAtZoom": 8},
        ).add_to(points_layer)
    else:
        layer_for_markers = points_layer

    _vals = pd.to_numeric(d["value"].abs() if is_change else d["value"], errors="coerce").dropna()
    if len(_vals) > 0:
        p10 = float(_vals.quantile(0.10))
        p90 = float(_vals.quantile(0.90))
        p98 = float(_vals.quantile(0.98))
    else:
        p10, p90, p98 = 0.0, 50.0, 100.0

    blue_threshold = 50.0
    vmax_size = max(p98, blue_threshold, 10.0)

    def _marker_size_cm(cm_abs: float) -> int:
        x = max(0.0, min(float(cm_abs), vmax_size)) / vmax_size
        size = 12 + int(22 * (x ** 0.5))
        return max(10, min(36, size))

    for _, r in d.iterrows():
        val = float(r["value"])
        cm_abs = abs(val) if is_change else val
        name = (r.get("name") or r.get("shortName") or r.get("sourceId"))
        unit = r.get("unit") or "cm"

        if "delta_cm" in d.columns and pd.notna(r.get("delta_cm")) and pd.notna(r.get("value_latest")) and pd.notna(r.get("value_since")):
            delta = float(r["delta_cm"])
            latest_v = float(r["value_latest"])
            since_v = float(r["value_since"])
            since_txt = r.get("since_label") or r.get("since_day") or "referanse"

            t_latest = r.get("referenceTime_latest")
            t_since = r.get("referenceTime_since")
            t_latest_str = pd.to_datetime(t_latest).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t_latest) else "ukjent"
            t_since_str = pd.to_datetime(t_since).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t_since) else "ukjent"

            html = (
                f"{name}"
                f"<br><b>Endring ({since_txt}): {delta:+.0f} cm</b>"
                f"<br>Nå: {latest_v:.0f} cm ({t_latest_str})"
                f"<br>Da: {since_v:.0f} cm ({t_since_str})"
            )
        else:
            t = r.get("referenceTime")
            t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"
            diff_part = ""
            if "diff_hours" in d.columns and pd.notna(r.get("diff_hours")):
                diff_part = f"<br>Avvik: {float(r['diff_hours']):+.1f} timer"
            html = f"{name}<br>Snødybde: <b>{val:.0f} {unit}</b><br>Tid: {t_str}{diff_part}"

        label = f"{val:+.0f}" if is_change else f"{val:.0f}"
        size = _marker_size_cm(cm_abs)
        color_val = _color_cm_quantile(cm_abs, p10, p90, blue_threshold)

        folium.Marker(
            location=[float(r["lat"]), float(r["lon"])],
            icon=folium.DivIcon(
                html=(
                    f'<div style="'
                    f'width:{size}px;height:{size}px;'
                    f'border-radius:999px;'
                    f'background:{color_val};'
                    f'border:2px solid {color_val};'
                    f'opacity:0.90;'
                    f'display:flex;align-items:center;justify-content:center;'
                    f'color:white;font-weight:900;'
                    f'font-size:{max(9, int(size*0.33))}px;'
                    f'line-height:1;'
                    f'text-shadow:0 1px 2px rgba(0,0,0,.45);'
                    f'">{label}</div>'
                ),
                icon_size=(size, size),
                icon_anchor=(size // 2, size // 2),
            ),
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=320),
            snow=float(cm_abs if is_change else val),
        ).add_to(layer_for_markers)

    # (overlay + refresh JS beholdt som i din fil)
    try:
        import html as _html

        dd = d.copy()
        dd["value"] = pd.to_numeric(dd["value"], errors="coerce")
        dd = dd.dropna(subset=["value"]).sort_values("value", ascending=False).head(20)

        title = "Størst endring i utsnittet" if is_change else "Mest snø i utsnittet"
        sub = "(topp 20)"

        rows = []
        for i, rr in enumerate(dd.itertuples(index=False), start=1):
            nm = getattr(rr, "name", None) or getattr(rr, "shortName", None) or getattr(rr, "sourceId", "")
            sid = getattr(rr, "sourceId", "")
            v = float(getattr(rr, "value"))
            v_txt = f"{v:+.0f} cm" if is_change else f"{v:.0f} cm"
            rows.append(
                f"""<tr>
<td style="padding:6px 8px; color:#64748b; width:28px;">{i}</td>
<td style="padding:6px 8px;">
  <div style="font-weight:800;">{_html.escape(str(nm))}</div>
  <div style="color:#64748b; font-size:12px;">{_html.escape(str(sid))}</div>
</td>
<td style="padding:6px 8px; text-align:right; font-weight:900; white-space:nowrap;">{v_txt}</td>
</tr>"""
            )

        rows_html = "\n".join(rows)
        css = """<style>
#snow-panel{
  position: fixed; left: 16px; bottom: 16px; z-index: 99999;
  width: 380px; max-width: calc(100vw - 32px);
  background: rgba(255,255,255,0.96); backdrop-filter: blur(6px);
  border-radius: 14px; box-shadow: 0 18px 45px rgba(15,23,42,.20);
  overflow: hidden; font-family: system-ui,-apple-system,"Segoe UI",sans-serif;
}
#snow-panel .hdr{ padding: 10px 12px; display:flex; align-items:center; justify-content:space-between; gap: 10px; }
#snow-panel .title{ font-weight: 900; }
#snow-panel button{ border:none; background:#e2e8f0; padding:6px 10px; border-radius:999px; cursor:pointer; font-weight:800; }
#snow-panel .body{ border-top: 1px solid #e2e8f0; max-height: 320px; overflow: auto; }
#snow-panel table{ width:100%; border-collapse:collapse; font-size:13px; }
#snow-panel thead th{
  position: sticky; top: 0; background: white; text-align:left;
  padding:6px 8px; color:#64748b; font-weight:800;
}
#snow-panel thead th:last-child{ text-align:right; }

/* cluster */
.snow-cluster{ width:52px; height:52px; border-radius:999px; display:flex; flex-direction:column;
  align-items:center; justify-content:center; box-shadow: 0 10px 24px rgba(15,23,42,.25);
  color:#fff; border:2px solid rgba(255,255,255,.65);
}
.snow-cluster .big{ font-weight:900; font-size:16px; line-height:16px; }
.snow-cluster .small{ font-weight:800; font-size:11px; line-height:12px; opacity:.95; margin-top:1px; }
.snow-cluster .unit{ font-weight:800; font-size:10px; line-height:10px; opacity:.9; margin-top:1px; }
.snow-cluster-high{ background:#2563eb; }
.snow-cluster-mid{ background:#16a34a; }
.snow-cluster-low{ background:#9ca3af; }
.snow-cluster-empty{ background:#e2e8f0; color:#0f172a; }
</style>"""

        panel = f"""<div id="snow-panel">
  <div class="hdr">
    <div class="title">{title} <span style="color:#64748b; font-weight:700;">{sub}</span></div>
    <div style="display:flex; gap:8px; align-items:center;">
      <button id="snow-refresh" type="button">Oppdater</button>
      <button type="button" onclick="var b=document.getElementById('snow-panel-body'); b.style.display=(b.style.display==='none'?'block':'none');">Vis/skjul</button>
    </div>
  </div>
  <div class="body" id="snow-panel-body">
    <table>
      <thead><tr><th>#</th><th>Stasjon</th><th>cm</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>"""

        root = m.get_root()
        root.header.add_child(folium.Element(css))
        root.html.add_child(folium.Element(panel))

        script = """
<script>
(function(){
  function findMap(){
    for (var k in window){
      if (k.startsWith('map_') && window[k] && window[k] instanceof L.Map) return window[k];
    }
    return null;
  }
  function buildAndReload(){
    var map = findMap();
    if (!map) return;
    var b = map.getBounds();
    var bbox = [b.getSouth().toFixed(6), b.getWest().toFixed(6), b.getNorth().toFixed(6), b.getEast().toFixed(6)].join(',');
    var z = map.getZoom();
    var c = map.getCenter();
    var u = new URL(window.location.href);
    u.searchParams.set('bbox', bbox);
    u.searchParams.set('z', String(z));
    u.searchParams.set('clat', c.lat.toFixed(6));
    u.searchParams.set('clon', c.lng.toFixed(6));
    window.location.href = u.toString();
  }
  var btn = document.getElementById('snow-refresh');
  if (btn){
    btn.addEventListener('click', function(e){ e.preventDefault(); buildAndReload(); });
  }
  var map = findMap();
  if (map){
    var timer = null;
    map.on('zoomend', function(){
      if (timer) clearTimeout(timer);
      timer = setTimeout(buildAndReload, 650);
    });
  }
})();
</script>
"""
        root.html.add_child(folium.Element(script))
    except Exception as e:
        print("Snow overlay failed:", e)

    folium.LayerControl().add_to(m)
    html_str = m.get_root().render()
    if out_html:
        m.save(out_html)
    return html_str


# ======================================================================
#  Data builders (nå uten /sources)
# ======================================================================

def _get_latest_cached(
    session: requests.Session,
    *,
    auth: FrostAuth,
    bbox_coords: Tuple[float, float, float, float],
    timeout: int,
    batch_size: int,
    qualities_latest: str,
) -> tuple[pd.DataFrame, pd.DataFrame, datetime]:
    """Returnerer (latest_per_baseId, meta_df, now_dt). Cacher latest i minne i 1 time."""
    now = datetime.now(timezone.utc)
    cache_key = (tuple(round(x, 6) for x in bbox_coords), str(qualities_latest).strip())

    meta = _stations_meta_for_bbox(bbox_coords)
    if meta.empty:
        raise RuntimeError("Fant ingen has_snow-stasjoner i valgt bbox (parquet).")
    base_ids = meta["baseId"].astype(str).tolist()

    hit = _LATEST_DF_CACHE.get(cache_key)
    if hit:
        ts, cached = hit
        if (now - ts).total_seconds() < LATEST_TTL_SECONDS and not cached.empty:
            return cached.copy(), meta, now

    latest = fetch_latest_snow_for_sources(
        session,
        auth=auth,
        source_base_ids=base_ids,
        timeout=timeout,
        batch_size=batch_size,
        qualities=qualities_latest,
    )
    if latest.empty:
        raise RuntimeError("Ingen 'latest' snøobservasjoner i området.")

    latest["baseId"] = latest["sourceId"].astype(str).map(base_source_id)
    latest = (
        latest.sort_values(["baseId", "referenceTime"], ascending=[True, False])
        .groupby("baseId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    _LATEST_DF_CACHE[cache_key] = (now, latest.copy())
    return latest, meta, now


def build_snow_df_latest_fast_south_first(
    *,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    qualities: str = "0,1,2,3,4",
    bbox_coords: Tuple[float, float, float, float] = (57.0, 4.0, 62.5, 12.5),
) -> tuple[pd.DataFrame, datetime]:
    auth = _env_auth()

    meta = _stations_meta_for_bbox(bbox_coords)
    if meta.empty:
        raise RuntimeError("Fant ingen has_snow-stasjoner i valgt bbox (parquet).")
    base_ids = meta["baseId"].astype(str).tolist()

    with requests.Session() as sess:
        obs = fetch_latest_snow_for_sources(
            sess,
            auth=auth,
            source_base_ids=base_ids,
            timeout=timeout,
            batch_size=batch_size,
            qualities=qualities,
        )

    if obs.empty:
        raise RuntimeError("Ingen 'latest' snøobservasjoner i området.")

    now = datetime.now(timezone.utc)
    obs["baseId"] = obs["sourceId"].astype(str).map(base_source_id)
    df = obs.merge(meta, on="baseId", how="left").drop(columns=["baseId"])
    return df, now


def build_snow_df_change_since(
    *,
    since_day: _date,
    window_days: int = 1,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities_latest: str = "0,1,2,3,4",
    qualities_since: str = "",
    bbox_coords: Tuple[float, float, float, float] = (57.0, 4.0, 62.5, 12.5),
) -> tuple[pd.DataFrame, datetime]:
    auth = _env_auth()

    with requests.Session() as sess:
        latest, meta, now = _get_latest_cached(
            sess,
            auth=auth,
            bbox_coords=bbox_coords,
            timeout=timeout,
            batch_size=batch_size,
            qualities_latest=qualities_latest,
        )
        base_ids = meta["baseId"].astype(str).tolist()

        base = fetch_snow_for_baseids_with_fallback(
            sess,
            auth=auth,
            base_ids=base_ids,
            target_day=since_day,
            window_days=window_days,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities_since,
        )
        if base.empty:
            raise RuntimeError("Ingen baseline-observasjoner funnet for valgt dato (selv med fallback).")

        base = base.rename(
            columns={"value": "value_since", "referenceTime": "referenceTime_since", "unit": "unit_since"}
        )
        latest = latest.rename(
            columns={"value": "value_latest", "referenceTime": "referenceTime_latest", "unit": "unit_latest"}
        )

        df = meta.merge(
            latest[["baseId", "sourceId", "value_latest", "referenceTime_latest"]],
            on="baseId",
            how="left",
        ).merge(
            base[["baseId", "value_since", "referenceTime_since", "diff_hours"]],
            on="baseId",
            how="left",
        )

        df["value_latest"] = pd.to_numeric(df["value_latest"], errors="coerce")
        df["value_since"] = pd.to_numeric(df["value_since"], errors="coerce")
        df["delta_cm"] = df["value_latest"] - df["value_since"]

        df["value"] = df["delta_cm"]
        df["unit"] = "cm"
        df["since_day"] = since_day.isoformat()

        df = df.dropna(subset=["lat", "lon", "value_latest", "value_since", "value"]).reset_index(drop=True)
        return df, now


def build_snow_df_for_day(
    date_str: Optional[str] = None,
    *,
    window_days: int = 1,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "",
    bbox_coords: Optional[Tuple[float, float, float, float]] = None,
) -> tuple[pd.DataFrame, _date]:
    """
    Day-mode uten /availableTimeSeries og uten /sources:
    - stasjoner fra parquet (has_snow) + bbox
    - hent obs i dag og fallback-vindu
    - velg nærmeste per baseId
    """
    auth = _env_auth()

    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        day = _date.today()

    meta = _stations_meta_for_bbox(bbox_coords)
    if meta.empty:
        raise RuntimeError("Fant ingen has_snow-stasjoner i utsnittet (parquet).")

    base_ids = meta["baseId"].astype(str).tolist()

    with requests.Session() as sess:
        obs = fetch_snow_for_baseids_with_fallback(
            sess,
            auth=auth,
            base_ids=base_ids,
            target_day=day,
            window_days=window_days,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

    if obs.empty:
        raise RuntimeError("Ingen observasjoner funnet (selv med fallback).")

    df = obs.merge(meta, on="baseId", how="left").drop(columns=["baseId"])
    df = df.dropna(subset=["lat", "lon", "value"]).reset_index(drop=True)
    return df, day


# ======================================================================
#  Public: bygg HTML
# ======================================================================

def build_snow_map_html(
    date_str: Optional[str] = None,
    *,
    mode: str = "latest",
    region: Optional[str] = None,
    bbox: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    window_days: int = 1,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "",
    cluster: bool = True,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 0.0,
    # endring
    change: bool = False,
    since: Optional[str] = None,   # "døgn" | "3døgn" | "YYYY-MM-DD" | "year"
) -> str:
    bbox_coords = _parse_bbox(bbox)
    if bbox_coords is None:
        key = (region or "south").strip().lower()
        bbox_coords = REGION_BBOX.get(key, REGION_BBOX["south"])

    if change:
        today = datetime.now(timezone.utc).date()
        if since is None or str(since).strip() == "":
            since_key = (date_str or today.isoformat()).strip().lower()
        else:
            since_key = str(since).strip().lower()

        if since_key in {"dogn", "døgn", "24h", "1d", "lastday", "sistedogn", "siste_dogn"}:
            since_day = today - timedelta(days=1)
            since_label = "siste døgn"
        elif since_key in {"3dogn", "3døgn", "72h", "3d", "last3days", "siste3dogn", "siste_3_dogn"}:
            since_day = today - timedelta(days=3)
            since_label = "siste 3 døgn"
        elif since_key in {"year", "iår", "iar", "thisyear"}:
            since_day = _date(today.year, 1, 1)
            since_label = "i år"
        else:
            since_day = datetime.strptime(since_key, "%Y-%m-%d").date()
            since_label = f"siden {since_day.isoformat()}"

        df, _now_dt = build_snow_df_change_since(
            since_day=since_day,
            window_days=window_days,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities_latest=qualities or "0,1,2,3,4",
            qualities_since=qualities,
            bbox_coords=bbox_coords,
        )
        df["since_label"] = since_label
    else:
        if mode not in {"latest", "day"}:
            mode = "latest"

        if mode == "latest":
            df, _now_dt = build_snow_df_latest_fast_south_first(
                timeout=timeout,
                batch_size=batch_size,
                qualities=qualities or "0,1,2,3,4",
                bbox_coords=bbox_coords,
            )
        else:
            df, _day = build_snow_df_for_day(
                date_str=date_str,
                window_days=window_days,
                timeout=timeout,
                batch_size=batch_size,
                limit=limit,
                qualities=qualities,
                bbox_coords=bbox_coords,
            )

    zoom_i: Optional[int] = None
    clat_f: Optional[float] = None
    clon_f: Optional[float] = None
    try:
        if z is not None and str(z).strip() != "":
            zoom_i = int(float(str(z)))
    except Exception:
        zoom_i = None
    try:
        if clat is not None and str(clat).strip() != "":
            clat_f = float(str(clat))
        if clon is not None and str(clon).strip() != "":
            clon_f = float(str(clon))
    except Exception:
        clat_f, clon_f = None, None

    return make_map(
        df,
        out_html=None,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_cm=heat_clip_cm,
        zoom_start=zoom_i,
        center_lat=clat_f,
        center_lon=clon_f,
        bbox_coords=bbox_coords,
    )


# ======================================================================
#  CLI
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Snødybde-kart fra Frost, med stasjonsliste fra parquet (has_snow). "
            "Mode 'latest' = siste verdi. Mode 'day' = gitt dato med fallback ±N dager. "
            "Ny: --change for endring (latest - baseline)."
        )
    )
    ap.add_argument("--mode", default="day", choices=["latest", "day"])
    ap.add_argument("--date", default=_date.today().isoformat(), help="YYYY-MM-DD (for day-mode)")
    ap.add_argument("--window-days", type=int, default=1, help="Fallback ±N dager (day/change)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--batch-size", type=int, default=80)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--qualities", default="", help="f.eks. '0,1,2,3,4'")

    ap.add_argument("--bbox", default="", help="bbox: 'south,west,north,east'")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-html", default="")

    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--show-heatmap", action="store_true")
    ap.add_argument("--heat-radius", type=int, default=25)
    ap.add_argument("--heat-blur", type=int, default=18)
    ap.add_argument("--heat-clip-cm", type=float, default=200.0)

    ap.add_argument("--change", action="store_true")
    ap.add_argument(
        "--since",
        default="",
        help="Baseline for --change: 'døgn' | '3døgn' | 'YYYY-MM-DD' | 'year'. Hvis tom brukes --date.",
    )

    args = ap.parse_args()
    bbox_coords = _parse_bbox(args.bbox)
    bbox_use = bbox_coords if bbox_coords is not None else REGION_BBOX["south"]

    if args.change:
        today = datetime.now(timezone.utc).date()
        sk = (args.since.strip().lower() if args.since.strip() else args.date.strip().lower())

        if sk in {"dogn", "døgn", "24h", "1d", "lastday", "sistedogn", "siste_dogn"}:
            since_day = today - timedelta(days=1)
            label = "døgn"
        elif sk in {"3dogn", "3døgn", "72h", "3d", "last3days", "siste3dogn", "siste_3_dogn"}:
            since_day = today - timedelta(days=3)
            label = "3døgn"
        elif sk in {"year", "iår", "iar", "thisyear"}:
            since_day = _date(today.year, 1, 1)
            label = "year"
        else:
            since_day = datetime.strptime(sk, "%Y-%m-%d").date()
            label = since_day.isoformat()

        df, now_dt = build_snow_df_change_since(
            since_day=since_day,
            window_days=args.window_days,
            timeout=args.timeout,
            batch_size=args.batch_size,
            limit=args.limit,
            qualities_latest=args.qualities or "0,1,2,3,4",
            qualities_since=args.qualities,
            bbox_coords=bbox_use,
        )
        df["since_label"] = label
        out_html = args.out_html or f"snow_map_change_{label}.html"
        day = now_dt.date()
        mode_txt = "change"
    else:
        if args.mode == "latest":
            df, now_dt = build_snow_df_latest_fast_south_first(
                timeout=args.timeout,
                batch_size=args.batch_size,
                qualities=args.qualities or "0,1,2,3,4",
                bbox_coords=bbox_use,
            )
            day = now_dt.date()
        else:
            df, day = build_snow_df_for_day(
                date_str=args.date,
                window_days=args.window_days,
                timeout=args.timeout,
                batch_size=args.batch_size,
                limit=args.limit,
                qualities=args.qualities,
                bbox_coords=bbox_coords,
            )
        out_html = args.out_html or f"snow_map_{args.mode}_{args.date}.html"
        mode_txt = args.mode

    if args.out_csv:
        df.to_csv(args.out_csv, index=False)

    make_map(
        df,
        out_html=out_html,
        cluster=not args.no_cluster,
        heatmap_show=args.show_heatmap,
        heat_radius=args.heat_radius,
        heat_blur=args.heat_blur,
        heat_clip_cm=args.heat_clip_cm,
        bbox_coords=bbox_use if (mode_txt in {"latest", "change"}) else bbox_coords,
    )

    print(f"Mode: {mode_txt}")
    print(f"Dato: {day}")
    if "sourceId" in df.columns:
        print(f"Stasjoner: {df['sourceId'].nunique()}")
    else:
        print(f"Rader: {len(df)}")
    if args.out_csv:
        print(f"Skrev CSV:  {args.out_csv}")
    print(f"Skrev kart: {out_html}")


if __name__ == "__main__":
    main()
