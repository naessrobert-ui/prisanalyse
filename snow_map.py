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
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# --- .env loading (robust på Windows/PyCharm) ----------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

ELEMENT_SNOW = "surface_snow_thickness"  # snødybde (cm)

# ----------------------------------------------------------------------
#  Enkle caches i minne for å unngå unødvendige kall i samme prosess
# ----------------------------------------------------------------------
# (dato, window_days) -> liste med sourceId
_SOURCE_UNIVERSE_CACHE: dict[tuple[_date, int], list[str]] = {}

# baseId -> metadata-dict (id, name, shortName, country, lat, lon)
_META_CACHE: dict[str, dict[str, Any]] = {}


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
    """
    GET med retries (429/5xx). Returnerer JSON (inkl. ErrorResponse ved 404).
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

        if r.status_code == 404:
            # "No data found" kan være normalt
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
    """
    Robust paginering for Frost v0:
    - Følg offset/itemsPerPage/totalItemCount.
    (Mange endepunkter har ikke 'next'.)
    """
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
    """
    Parse bbox-streng 'south,west,north,east' til tuple.
    Returnerer None hvis bbox er tom/ugyldig.
    """
    if not bbox:
        return None
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
        if len(parts) != 4:
            return None
        south, west, north, east = parts
        # enkel sanity-check
        if south > north or west > east:
            return None
        return south, west, north, east
    except Exception:
        return None


def _filter_meta_by_bbox(
    meta: pd.DataFrame,
    bbox_coords: Optional[Tuple[float, float, float, float]],
) -> pd.DataFrame:
    """Filtrer metadata-DF på bbox, hvis satt."""
    if bbox_coords is None or meta.empty:
        return meta
    south, west, north, east = bbox_coords
    m = meta.copy()
    m["lat"] = pd.to_numeric(m["lat"], errors="coerce")
    m["lon"] = pd.to_numeric(m["lon"], errors="coerce")
    return m[
        (m["lat"].between(south, north))
        & (m["lon"].between(west, east))
    ].copy()

REGION_BBOX: dict[str, Tuple[float, float, float, float]] = {
    # south, west, north, east
    "south": (57.0, 4.0, 62.5, 12.5),
    "mid": (62.0, 4.0, 66.7, 16.5),
    "north": (66.3, 10.0, 71.5, 31.5),
    # hele Norge-ish (kan justeres)
    "all": (57.0, 4.0, 71.5, 31.5),
}

# ======================================================================
#  Hente stasjoner og observasjoner
# ======================================================================

def list_sources_with_snow_in_referencetime(
    session: requests.Session,
    *,
    auth: FrostAuth,
    referencetime: str,
    timeout: int,
) -> list[str]:
    """
    Finn stasjoner som har tilgjengelig tidsserie for snødybde i referencetime-intervallet.
    NB: Ikke filtrer på timeoffsets/levels her for snødybde.
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    params: dict[str, str | int] = {
        "referencetime": referencetime,
        "elements": ELEMENT_SNOW,
        # mindre payload – vi trenger bare sourceId
        "fields": "sourceId",
    }

    source_ids: set[str] = set()
    for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
        if page.get("@type") == "ErrorResponse":
            continue
        for item in page.get("data", []):
            sid = item.get("sourceId")
            if sid and isinstance(sid, str):
                # Begrens til norske stasjoner (SNxxxxx) for å kutte støy
                if sid.startswith("SN"):
                    source_ids.add(sid)

    return sorted(source_ids)


def list_sources_for_day_window(
    session: requests.Session,
    *,
    auth: FrostAuth,
    day: _date,
    window_days: int,
    timeout: int,
    limit: int = 1000,
) -> list[str]:
    """
    Finn stasjoner med snødybde-serie i [day-window, day+window+1).

    Resultatet caches per (dato, window_days) i _SOURCE_UNIVERSE_CACHE for
    å slippe nye /availableTimeSeries-kall for samme dag.
    """
    cache_key = (day, int(window_days))
    if cache_key in _SOURCE_UNIVERSE_CACHE:
        return _SOURCE_UNIVERSE_CACHE[cache_key]

    start = day - timedelta(days=window_days)
    end = day + timedelta(days=window_days + 1)
    referencetime = f"{start.isoformat()}/{end.isoformat()}"

    universe = list_sources_with_snow_in_referencetime(
        session,
        auth=auth,
        referencetime=referencetime,
        timeout=timeout,
    )

    _SOURCE_UNIVERSE_CACHE[cache_key] = universe
    return universe


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
    """
    Hent observasjoner for sources i et interval.
    Returnerer lang DF med sourceId/referenceTime/value...
    """
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


def choose_nearest_per_station(
    df: pd.DataFrame,
    *,
    target_day: _date,
) -> pd.DataFrame:
    """
    Velg nærmeste observasjon til target_day (00:00 UTC)
    per sourceId (IKKE baseId).
    """
    if df.empty:
        return df

    target_dt = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    target_ts = pd.Timestamp(target_dt)

    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["value"])

    d["abs_diff_s"] = (d["referenceTime"] - target_ts).abs().dt.total_seconds()

    d = (
        d.sort_values(["sourceId", "abs_diff_s", "referenceTime"], ascending=[True, True, False])
        .groupby(["sourceId"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    d["diff_hours"] = (d["referenceTime"] - target_ts).dt.total_seconds() / 3600.0
    return d.drop(columns=["abs_diff_s"])


def choose_latest_per_station(df: pd.DataFrame) -> pd.DataFrame:
    """
    For 'latest'-modus: velg siste observasjon per sourceId (største referenceTime).
    """
    if df.empty:
        return df

    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["value"])

    d = (
        d.sort_values(["sourceId", "referenceTime"], ascending=[True, False])
        .groupby("sourceId", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return d


def get_sources_metadata(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    timeout: int,
    batch_size: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    /sources støtter ids=... (ikke sources=...).
    observations/sourceId kan være SNxxxx:0 -> vi spør /sources med SNxxxx.
    Returnerer DF med samme sourceId-format som i observations.

    For å spare tid caches metadata per baseId i _META_CACHE.
    """
    path = "/sources/v0.jsonld"
    mapping = {sid: base_source_id(sid) for sid in sources}
    base_ids = sorted(set(mapping.values()))

    rows: list[dict[str, Any]] = []

    # Hent bare metadata for baseId vi ikke allerede har
    missing: list[str] = [bid for bid in base_ids if bid not in _META_CACHE]

    for batch in chunked(missing, batch_size):
        if not batch:
            break
        params: dict[str, str | int] = {
            "ids": ",".join(batch),
            "fields": "id,name,shortName,country,geometry",
        }
        for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                continue
            for item in page.get("data", []):
                geom = item.get("geometry") or {}
                coords = geom.get("coordinates")  # [lon, lat]
                lon = lat = None
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]
                meta_rec = {
                    "baseId": item.get("id"),
                    "name": item.get("name"),
                    "shortName": item.get("shortName"),
                    "country": item.get("country"),
                    "lat": lat,
                    "lon": lon,
                }
                if meta_rec["baseId"]:
                    _META_CACHE[meta_rec["baseId"]] = meta_rec

    # Bygg DF ut fra cache for alle base_ids vi trenger
    for bid in base_ids:
        rec = _META_CACHE.get(bid)
        if rec:
            rows.append(rec)

    meta = pd.DataFrame(rows, columns=["baseId", "name", "shortName", "country", "lat", "lon"])
    link = pd.DataFrame({"sourceId": list(mapping.keys()), "baseId": list(mapping.values())})
    return link.merge(meta, on="baseId", how="left").drop(columns=["baseId"])


# ======================================================================
#  Kartbygging
# ======================================================================

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



def _heat_params_for_zoom(z: int | None) -> tuple[int, int]:
    """Return (radius, blur) for HeatMap tuned by zoom."""
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
    # legacy/tunables (can be overridden by zoom-aware defaults)
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 0.0,
    # view controls
    zoom_start: Optional[int] = None,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    bbox_coords: Optional[Tuple[float, float, float, float]] = None,
) -> str:
    """
    Lager folium-kart fra df (snødybde).

    - heat_clip_cm: hvis 0, beregnes automatisk (98-persentil) for *aktuelt utsnitt/region*.
    - zoom_start/center_*: brukes hvis sendt inn (for å bevare view ved reload).
    - bbox_coords: hvis sendt inn, brukes til fit_bounds (god UX ved regionvalg).
    """
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

    # ---- View: bruk clat/clon/z hvis sendt inn, ellers midtpunkt av punktene
    if center_lat is None:
        center_lat = float(d["lat"].mean())
    if center_lon is None:
        center_lon = float(d["lon"].mean())
    if zoom_start is None:
        zoom_start = 5

    m = folium.Map(location=[center_lat, center_lon], zoom_start=int(zoom_start), tiles="OpenStreetMap")

    # Fit bounds hvis vi har bbox (gir bedre “region-hopp”)
    if bbox_coords is not None:
        south, west, north, east = bbox_coords
        try:
            m.fit_bounds([[south, west], [north, east]])
        except Exception:
            pass

    # ---- Heatmap: “visuelt og pent”, vektet av snødybde (ikke bare tetthet)
    # Per-region/utsnitt scaling: beregn klipp (vmax) fra data i dette utsnittet (robust mot outliers)
    vals = d["value"].clip(lower=0.0)
    if heat_clip_cm and heat_clip_cm > 0:
        clip_cm = float(heat_clip_cm)
    else:
        q = float(vals.quantile(0.98)) if len(vals) else 1.0
        clip_cm = max(q, 10.0)  # minst 10 cm så ikke alt blir “rødt” på lite snø
    clipped = vals.clip(upper=clip_cm)

    # Normaliser til 0..1 og bruk en mild gamma for bedre kontrast
    weights = (clipped / clip_cm).clip(0.0, 1.0) ** 0.7

    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]

    # Snø-intuitiv gradient (kald blå -> hvit -> lilla/rød)
    SNOW_GRADIENT = {
        0.00: "#08306b",
        0.20: "#2171b5",
        0.40: "#41b6c4",
        0.55: "#ffffff",
        0.70: "#c7a9ff",
        0.85: "#7a3db8",
        1.00: "#d73027",
    }

    # Zoom-aware radius/blur (overstyr legacy-verdier hvis z er satt)
    zr, zb = _heat_params_for_zoom(int(zoom_start) if zoom_start is not None else None)
    heat_radius = int(zr) if zr else int(heat_radius)
    heat_blur = int(zb) if zb else int(heat_blur)

    heat_layer = folium.FeatureGroup(name="Snødybde (interpolert)", show=heatmap_show)
    HeatMap(
        heat_data,
        radius=heat_radius,
        blur=heat_blur,
        min_opacity=0.20,
        gradient=SNOW_GRADIENT,
        max_zoom=10,
    ).add_to(heat_layer)
    heat_layer.add_to(m)

    # ---- Punktmarkører + cluster med MEDIAN (cm)
    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)

    if cluster:
        icon_create_function = """
        function(cluster) {
          var children = cluster.getAllChildMarkers();
          var vals = [];
          for (var i=0; i<children.length; i++){
            var v = children[i].options && children[i].options.snow;
            if (v !== undefined && v !== null && !isNaN(v)) vals.push(v);
          }
          if (vals.length === 0){
            return L.divIcon({html: '<div><span>?</span></div>', className: 'marker-cluster marker-cluster-small', iconSize: new L.Point(40, 40)});
          }
          vals.sort(function(a,b){return a-b;});
          var mid = Math.floor(vals.length/2);
          var med = (vals.length % 2) ? vals[mid] : (vals[mid-1]+vals[mid])/2;
          var cm = Math.round(med);

          // Farge etter nivå
          var cls = 'marker-cluster-small';
          if (cm >= 150) cls = 'marker-cluster-large';
          else if (cm >= 70) cls = 'marker-cluster-medium';

          var html = '<div><span>' + cm + '</span><div style="font-size:10px; line-height:10px; margin-top:-2px; opacity:.85">cm</div></div>';
          return L.divIcon({ html: html, className: 'marker-cluster ' + cls, iconSize: new L.Point(46, 46) });
        }
        """
        layer_for_markers = MarkerCluster(icon_create_function=icon_create_function).add_to(points_layer)
    else:
        layer_for_markers = points_layer

    for _, r in d.iterrows():
        cm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r.get("sourceId"))
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
            # <-- brukes av cluster-funksjonen
            snow=cm,
        ).add_to(layer_for_markers)

    # ---- Overlay: KPI + tabell (topp/bunn) + knapp "Oppdater utsnitt"
    try:
        from branca.element import MacroElement, Template

        med = float(vals.median()) if len(vals) else 0.0
        mean = float(vals.mean()) if len(vals) else 0.0
        mx = float(vals.max()) if len(vals) else 0.0

        tbl = d.copy().sort_values("value", ascending=False)
        top20 = tbl.head(20)
        bot20 = tbl.tail(20).sort_values("value", ascending=True)

        def _rows(dfsub: pd.DataFrame) -> str:
            rows = []
            for i, rr in enumerate(dfsub.itertuples(index=False), start=1):
                nm = getattr(rr, "name", None) or getattr(rr, "shortName", None) or getattr(rr, "sourceId", "")
                sid = getattr(rr, "sourceId", "")
                v = float(getattr(rr, "value"))
                rows.append(
                    "<tr>"
                    f"<td style='padding:6px 8px; color:#64748b; width:26px;'>{i}</td>"
                    f"<td style='padding:6px 8px;'><div style='font-weight:800'>{nm}</div>"
                    f"<div style='color:#64748b; font-size:12px;'>{sid}</div></td>"
                    f"<td style='padding:6px 8px; text-align:right; font-weight:900;'>{v:.0f} cm</td>"
                    "</tr>"
                )
            return "".join(rows)

        rows_top = _rows(top20)
        rows_bot = _rows(bot20)

        overlay = f"""
        <div id="snow-panel"
         style="position: fixed; left: 16px; bottom: 16px; z-index: 9999;
         background: rgba(255,255,255,0.96); backdrop-filter: blur(6px);
         border-radius: 14px; box-shadow: 0 18px 45px rgba(15,23,42,.18);
         width: 380px; max-width: calc(100vw - 32px); overflow: hidden; font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;">
          <div style="padding:10px 12px; display:flex; align-items:center; justify-content:space-between; gap:10px;">
            <div style="font-weight:900;">Snø i utsnittet ({len(d)} stasjoner)</div>
            <div style="display:flex; gap:8px;">
              <button id="snow-toggle" style="border:none; background:#e2e8f0; padding:6px 10px; border-radius:999px; cursor:pointer; font-weight:800;">Vis/skjul</button>
              <button id="snow-refresh" style="border:none; background:#2563eb; color:white; padding:6px 10px; border-radius:999px; cursor:pointer; font-weight:900;">Oppdater utsnitt</button>
            </div>
          </div>

          <div id="snow-body" style="border-top:1px solid #e2e8f0;">
            <div style="padding:10px 12px; display:flex; gap:10px; flex-wrap:wrap; color:#0f172a;">
              <div style="background:#f1f5f9; padding:6px 10px; border-radius:999px;"><b>Maks</b> {mx:.0f} cm</div>
              <div style="background:#f1f5f9; padding:6px 10px; border-radius:999px;"><b>Median</b> {med:.0f} cm</div>
              <div style="background:#f1f5f9; padding:6px 10px; border-radius:999px;"><b>Snitt</b> {mean:.0f} cm</div>
              <div style="background:#f1f5f9; padding:6px 10px; border-radius:999px;"><b>Skala</b> ~P98={clip_cm:.0f} cm</div>
            </div>

            <div style="padding:0 12px 10px;">
              <select id="snow-table-mode" style="width:100%; padding:7px 10px; border-radius:12px; border:1px solid #d1d5db; font-weight:800;">
                <option value="top" selected>Mest snø (topp 20)</option>
                <option value="bottom">Minst snø (topp 20)</option>
              </select>
            </div>

            <div style="max-height: 320px; overflow:auto; border-top:1px solid #e2e8f0;">
              <table style="width:100%; border-collapse:collapse; font-size:13px;">
                <thead>
                  <tr style="position:sticky; top:0; background:white;">
                    <th style="text-align:left; padding:6px 8px; color:#64748b;">#</th>
                    <th style="text-align:left; padding:6px 8px; color:#64748b;">Stasjon</th>
                    <th style="text-align:right; padding:6px 8px; color:#64748b;">cm</th>
                  </tr>
                </thead>
                <tbody id="snow-table-rows">
                  {rows_top}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <script>
          (function() {{
            function getMap() {{
              for (var k in window) {{
                if (k.startsWith('map_') && window[k] && window[k].getBounds) return window[k];
              }}
              return null;
            }}

            var btnToggle = document.getElementById('snow-toggle');
            var body = document.getElementById('snow-body');
            btnToggle.addEventListener('click', function() {{
              body.style.display = (body.style.display === 'none') ? 'block' : 'none';
            }});

            var sel = document.getElementById('snow-table-mode');
            var rowsTop = {json.dumps(rows_top)};
            var rowsBot = {json.dumps(rows_bot)};
            sel.addEventListener('change', function() {{
              document.getElementById('snow-table-rows').innerHTML = (sel.value === 'bottom') ? rowsBot : rowsTop;
            }});

            var btnRefresh = document.getElementById('snow-refresh');
            btnRefresh.addEventListener('click', function() {{
              var map = getMap();
              if (!map) return;

              var b = map.getBounds();
              var sw = b.getSouthWest();
              var ne = b.getNorthEast();
              var bbox = [sw.lat.toFixed(5), sw.lng.toFixed(5), ne.lat.toFixed(5), ne.lng.toFixed(5)].join(',');
              var z = map.getZoom();
              var c = map.getCenter();

              var u = new URL(window.location.href);
              u.searchParams.set('bbox', bbox);
              u.searchParams.set('z', String(z));
              u.searchParams.set('clat', c.lat.toFixed(5));
              u.searchParams.set('clon', c.lng.toFixed(5));
              window.location.href = u.toString();
            }});
          }})();
        </script>
        """

        macro = MacroElement()
        macro._template = Template(overlay)
        m.get_root().add_child(macro)
    except Exception:
        pass

    folium.LayerControl().add_to(m)

    html_str = m.get_root().render()

    if out_html:
        m.save(out_html)

    return html_str


# ======================================================================
#  Bygg DF for "latest" og "day"
# ======================================================================


def bbox_to_wkt_polygon(bbox: Tuple[float, float, float, float]) -> str:
    """
    Lag WKT POLYGON fra bbox (south, west, north, east).
    WKT bruker (lon lat).
    """
    south, west, north, east = bbox
    return (
        f"POLYGON(({west} {south}, {east} {south}, {east} {north}, "
        f"{west} {north}, {west} {south}))"
    )


def list_sources_in_bbox(
    session: requests.Session,
    *,
    auth: FrostAuth,
    bbox: Tuple[float, float, float, float],
    timeout: int,
) -> pd.DataFrame:
    """
    Hent stasjoner (SensorSystem) innenfor bbox via /sources med geometry=POLYGON(WKT).
    /sources støtter IKKE 'limit' (hos deg), så vi henter uten limit og gjør best-effort paginering
    via offset dersom API-et tillater det.
    """
    path = "/sources/v0.jsonld"
    wkt = bbox_to_wkt_polygon(bbox)

    base_params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "geometry": wkt,
        "fields": "id,name,shortName,country,geometry",
    }

    rows: list[dict[str, Any]] = []

    # Første side
    first = frost_get_json(session, path, base_params, auth=auth, timeout=timeout)
    if first.get("@type") == "ErrorResponse":
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "lat", "lon"])

    def _consume(page: dict[str, Any]) -> None:
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

    _consume(first)

    # Best-effort paging: prøv offset hvis det finnes flere sider
    try:
        total = int(first.get("totalItemCount", 0))
        offset = int(first.get("offset", 0))
        per_page = int(first.get("itemsPerPage", 0))
    except Exception:
        total = offset = per_page = 0

    if total > 0 and per_page > 0:
        next_offset = offset + per_page
        while next_offset < total:
            p = dict(base_params)
            p["offset"] = next_offset  # noen Frost-oppsett støtter dette selv om det ikke står i help-lista
            page = frost_get_json(session, path, p, auth=auth, timeout=timeout)

            # Hvis offset ikke støttes hos deg vil dette typisk bli ErrorResponse/400 – da stopper vi.
            if page.get("@type") == "ErrorResponse":
                break

            _consume(page)

            try:
                offset = int(page.get("offset", next_offset))
                per_page = int(page.get("itemsPerPage", per_page))
                total = int(page.get("totalItemCount", total))
            except Exception:
                break
            next_offset = offset + per_page

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["baseId"]).reset_index(drop=True)
    return df


def fetch_latest_snow_for_sources(
    session: requests.Session,
    *,
    auth: "FrostAuth",
    source_base_ids: list[str],
    timeout: int,
    batch_size: int,
    qualities: str = "0,1,2,3,4",
) -> pd.DataFrame:
    """
    Hent siste snødybde per stasjon med referencetime=latest&limit=1.
    """
    if not source_base_ids:
        return pd.DataFrame()

    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    for batch in chunked(source_base_ids, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": "latest",
            "elements": ELEMENT_SNOW,
            "limit": 1,  # siste per (source, element)
            # disse to reduserer “duplikate” tidsserier (sensor/offset/levels)
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
    df = df.dropna(subset=["referenceTime", "value"])
    return df


def build_snow_df_latest_fast_south_first(
    *,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    qualities: str = "0,1,2,3,4",
    # Sør-Norge-ish default bbox (kan justeres):
    bbox_coords: Tuple[float, float, float, float] = (57.0, 4.0, 62.5, 12.5),
) -> tuple[pd.DataFrame, datetime]:
    """
    Rask oppstart:
      1) hent stasjoner i bbox via /sources (ingen availableTimeSeries-scan)
      2) hent 'latest' snødybde for disse (limit=1)
    """
    auth = _env_auth()
    now = datetime.now(timezone.utc)

    with requests.Session() as sess:
        meta = list_sources_in_bbox(sess, auth=auth, bbox=bbox_coords, timeout=timeout)

        if meta.empty:
            raise RuntimeError("Fant ingen stasjoner i valgt bbox.")

        base_ids = meta["baseId"].astype(str).tolist()

        obs = fetch_latest_snow_for_sources(
            sess,
            auth=auth,
            source_base_ids=base_ids,
            timeout=timeout,
            batch_size=batch_size,
            qualities=qualities,
        )

    if obs.empty:
        raise RuntimeError("Ingen 'latest' snøobservasjoner i området (prøv større bbox eller uten kvalitetsfilter).")

    # obs.sourceId kan være "SN12345:0" mens meta.baseId er "SN12345"
    obs["baseId"] = obs["sourceId"].astype(str).map(base_source_id)
    df = obs.merge(meta, left_on="baseId", right_on="baseId", how="left").drop(columns=["baseId"])
    return df, now

def build_snow_df_for_day(
    date_str: Optional[str] = None,
    *,
    # mindre default-vindu for bedre ytelse (var 2 tidligere)
    window_days: int = 1,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "",
    bbox_coords: Optional[Tuple[float, float, float, float]] = None,
) -> tuple[pd.DataFrame, _date]:
    """
    Lager data-frame med snødybde for valgt dag, med fallback ±window_days.
    Hvis bbox_coords er satt, begrenser vi universet til stasjoner i kartutsnittet.
    date_str: 'YYYY-MM-DD', eller None = i dag.
    Returnerer (df, day).
    """
    auth = _env_auth()

    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        day = _date.today()

    day_start = day
    day_end = day + timedelta(days=1)
    day_rt = f"{day_start.isoformat()}/{day_end.isoformat()}"

    with requests.Session() as sess:
        # 1) Finn univers av stasjoner i ±window_days (hele Norge)
        universe = list_sources_for_day_window(
            sess,
            auth=auth,
            day=day,
            window_days=window_days,
            timeout=timeout,
        )

        if not universe:
            raise RuntimeError(
                "Fant ingen stasjoner med snødybde-serie i valgt vindu. Prøv større window_days."
            )

        # 2) Metadata for universet
        meta_all = get_sources_metadata(
            sess,
            auth=auth,
            sources=universe,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
        )
        if meta_all.empty:
            raise RuntimeError("Fant stasjoner, men ingen koordinater fra /sources.")

        # 3) Begrens til stasjoner i kartutsnitt (hvis bbox)
        meta_filtered = _filter_meta_by_bbox(meta_all, bbox_coords)
        if meta_filtered.empty:
            raise RuntimeError("Ingen stasjoner innenfor valgt kartutsnitt.")

        # Bare disse vil vi gjøre observasjons-oppslag på:
        sources_in_view = meta_filtered["sourceId"].astype(str).unique().tolist()

        # 4) Hent observasjoner for valgt dag
        df_day = fetch_observations_interval(
            sess,
            auth=auth,
            sources=sources_in_view,
            referencetime=day_rt,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

        have = set(df_day["sourceId"].unique()) if not df_day.empty else set()
        missing = [s for s in sources_in_view if s not in have]

        # 5) Fallback ±window_days for de som mangler (hvis window_days > 0)
        df_fb = pd.DataFrame()
        if missing and window_days > 0:
            fb_start = day - timedelta(days=window_days)
            fb_end = day + timedelta(days=window_days + 1)
            fb_rt = f"{fb_start.isoformat()}/{fb_end.isoformat()}"
            df_fb = fetch_observations_interval(
                sess,
                auth=auth,
                sources=missing,
                referencetime=fb_rt,
                timeout=timeout,
                batch_size=batch_size,
                limit=limit,
                qualities=qualities,
            )

    df_all = pd.concat([df_day, df_fb], ignore_index=True)
    obs = choose_nearest_per_station(df_all, target_day=day)

    if obs.empty:
        raise RuntimeError("Ingen observasjoner funnet selv med fallback.")

    df = obs.merge(meta_filtered, on="sourceId", how="left")
    return df, day


# ======================================================================
#  Public: bygg HTML for snøkart
#  NB: tar nå 'mode' og optional bbox/z/clat/clon (for kompatibilitet)
# ======================================================================

def build_snow_map_html(
    date_str: Optional[str] = None,
    *,
    mode: str = "latest",             # "latest" eller "day"
    region: Optional[str] = None,     # "south" | "mid" | "north" | "all"
    bbox: Optional[str] = None,       # "south,west,north,east"
    z: Optional[str] = None,          # ignoreres (kompat)
    clat: Optional[str] = None,       # ignoreres
    clon: Optional[str] = None,       # ignoreres
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
) -> str:
    """
    Bygger snøkart for valgt modus og returnerer HTML.

    - mode="latest": bruker /sources+geometry (bbox) + /observations referencetime=latest&limit=1
      (rask oppstart, ingen availableTimeSeries-scan)

    - mode="day": bruker original day-fallback (availableTimeSeries -> metadata -> obs -> fallback)
      og kan begrenses av bbox (kartutsnitt) hvis sendt inn.
    """
    
    # 1) Bestem bbox: prioritet bbox-param, ellers region, ellers default sør
    bbox_coords = _parse_bbox(bbox)
    if bbox_coords is None:
        key = (region or "south").strip().lower()
        bbox_coords = REGION_BBOX.get(key, REGION_BBOX["south"])

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
        # day-mode: bruk din eksisterende funksjon (den fungerer), men begrens med bbox
        df, _day = build_snow_df_for_day(
            date_str=date_str,
            window_days=window_days,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
            bbox_coords=bbox_coords,
        )

    
    # View-parametere fra querystring (brukes for å beholde zoom/pan ved reload)
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

    html_str = make_map(
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
    return html_str


# ======================================================================
#  CLI-main (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Snødybde-kart fra Frost. "
            "Mode 'latest' = siste døgn. Mode 'day' = gitt dato med fallback ±N dager."
        )
    )
    ap.add_argument(
        "--mode",
        default="day",
        choices=["latest", "day"],
        help="latest = siste døgn (hurtig), day = kalenderdato + fallback",
    )
    ap.add_argument(
        "--date",
        help="Mål-dato (UTC) YYYY-MM-DD (brukes kun i mode=day, default = i dag)",
        default=_date.today().isoformat(),
    )
    ap.add_argument(
        "--window-days",
        type=int,
        default=1,
        help="Fallback-vindu ±N dager (kun for mode=day). 0 = ingen fallback.",
    )
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--batch-size", type=int, default=80)
    ap.add_argument("--limit", type=int, default=1000, help="Per-side limit for Frost (paging)")
    ap.add_argument("--qualities", default="", help="Valgfritt filter, f.eks. '0,1,2,3,4'. Tom = ingen filter.")

    ap.add_argument("--bbox", default="", help="Optional bbox: 'south,west,north,east'.")
    ap.add_argument("--out-csv", default="", help="CSV-ut (tom = ingen fil)")
    ap.add_argument("--out-html", default="", help="HTML-kart (tom = auto-navn)")

    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--show-heatmap", action="store_true")
    ap.add_argument("--heat-radius", type=int, default=25)
    ap.add_argument("--heat-blur", type=int, default=18)
    ap.add_argument("--heat-clip-cm", type=float, default=200.0)

    args = ap.parse_args()
    bbox_coords = _parse_bbox(args.bbox)

    if args.mode == "latest":
        if bbox_coords is not None:
            df, now_dt = build_snow_df_latest_fast_south_first(
                timeout=args.timeout,
                batch_size=args.batch_size,
                qualities=args.qualities or "0,1,2,3,4",
                bbox_coords=bbox_coords,
            )
        else:
            df, now_dt = build_snow_df_latest_fast_south_first(
                timeout=args.timeout,
                batch_size=args.batch_size,
                qualities=args.qualities or "0,1,2,3,4",
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

    if args.out_csv:
        df.to_csv(args.out_csv, index=False)

    _html_map = make_map(
        df,
        out_html=out_html,
        cluster=not args.no_cluster,
        heatmap_show=args.show_heatmap,
        heat_radius=args.heat_radius,
        heat_blur=args.heat_blur,
        heat_clip_cm=args.heat_clip_cm,
    )

    print(f"Mode: {args.mode}")
    print(f"Dato (for day-mode): {day}")
    print(f"Stasjoner (i kart): {df['sourceId'].nunique()}")
    if args.out_csv:
        print(f"Skrev CSV:  {args.out_csv}")
    print(f"Skrev kart: {out_html}")


if __name__ == "__main__":
    main()
