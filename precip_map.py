#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import threading
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

# --- .env loading (robust on Windows/PyCharm) ----------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

# Elementer:
# - Døgn (kalenderdøgn): ferdig døgnsum (mm) der tilgjengelig
ELEMENT_PRECIP_DAY = "sum(precipitation_amount P1D)"
# - Time: brukes for rullerende siste 24 timer (summerer 24 timeverdier)
ELEMENT_PRECIP_HOURLY = "sum(precipitation_amount PT1H)"

UNIT_MM = "mm"

Mode = Literal["last24h", "day", "mtd", "ytd"]

# ------------------------------------------------------------------
# Minimal in-memory cache (kun for last24h)
# ------------------------------------------------------------------
_LAST24H_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_LOCK = threading.Lock()
LAST24H_TTL_SECONDS = 300  # 5 minutter


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
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env + python-dotenv).")
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
    """Robust paginering via offset/itemsPerPage/totalItemCount."""
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
#  Kilder + metadata
# ======================================================================

def list_sources_with_element_in_referencetime(
    session: requests.Session,
    *,
    auth: FrostAuth,
    referencetime: str,
    elements: str,
    timeout: int,
) -> list[str]:
    """
    Finn stasjoner som har tilgjengelig tidsserie for gitt element i referencetime.
    timeoffsets=default og levels=default reduserer duplikater.
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    params: dict[str, str | int] = {
        "referencetime": referencetime,
        "elements": elements,
        "timeoffsets": "default",
        "levels": "default",
    }

    source_ids: set[str] = set()
    for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
        if page.get("@type") == "ErrorResponse":
            continue
        for item in page.get("data", []):
            sid = item.get("sourceId")
            if sid:
                source_ids.add(sid)

    return sorted(source_ids)


def get_sources_metadata(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    timeout: int,
    batch_size: int,
) -> pd.DataFrame:
    path = "/sources/v0.jsonld"
    mapping = {sid: base_source_id(sid) for sid in sources}
    base_ids = sorted(set(mapping.values()))

    rows: list[dict[str, Any]] = []
    for batch in chunked(base_ids, batch_size):
        params: dict[str, str | int] = {
            "ids": ",".join(batch),
            "fields": "id,name,shortName,country,geometry",
        }
        for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                continue
            for item in page.get("data", []):
                geom = item.get("geometry") or {}
                coords = geom.get("coordinates")
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

    meta = pd.DataFrame(rows, columns=["baseId", "name", "shortName", "country", "lat", "lon"])
    link = pd.DataFrame({"sourceId": list(mapping.keys()), "baseId": list(mapping.values())})
    return link.merge(meta, on="baseId", how="left").drop(columns=["baseId"])


# ======================================================================
#  Observasjoner
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
    Returnerer lang DF med: sourceId, referenceTime, elementId, value, unit, qualityCode
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
        .agg(value=("value", "sum"), n=("value", "size"), rt_max=("referenceTime", "max"))
        .reset_index(drop=True)
    )
    out.rename(columns={"n": count_col}, inplace=True)
    out["unit"] = UNIT_MM
    out["referenceTime"] = out["rt_max"]
    out.drop(columns=["rt_max"], inplace=True)
    return out


# ======================================================================
#  Kart
# ======================================================================

def _color_mm(mm: float) -> str:
    if mm >= 100:
        return "darkred"
    if mm >= 50:
        return "red"
    if mm >= 20:
        return "orange"
    if mm >= 5:
        return "green"
    if mm >= 1:
        return "cadetblue"
    return "lightgray"


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
) -> str:
    if df.empty:
        raise RuntimeError("Ingen data å plotte (df er tom).")

    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")

    # Heatmap (vektet)
    clipped = d["value"].clip(lower=0, upper=heat_clip_mm)
    weights = (clipped / heat_clip_mm) ** 0.5
    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name=f"Heatmap – {title}", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=8).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    for _, r in d.iterrows():
        mm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or UNIT_MM
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"

        extra = ""
        if "n_hours" in d.columns and pd.notna(r.get("n_hours")):
            extra = f"<br>Timer i sum: {int(r['n_hours'])}"
        elif "n_days" in d.columns and pd.notna(r.get("n_days")):
            extra = f"<br>Dager i sum: {int(r['n_days'])}"

        html = f"{name}<br>{title}: <b>{mm:.1f} {unit}</b><br>Tid: {t_str}{extra}"

        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=5,
            color=_color_mm(mm),
            fill=True,
            fill_opacity=0.85,
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=360),
        ).add_to(layer_for_markers)

    folium.LayerControl().add_to(m)
    html_str = m.get_root().render()

    if out_html:
        m.save(out_html)
    return html_str

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
    # POLYGON((lon lat, lon lat, ...))
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


def downsample_spatial_best_quality(
    df: pd.DataFrame,
    *,
    w: float,
    s: float,
    e: float,
    n: float,
    max_points: int = 1200,
    keep_top_n: int = 50,
) -> pd.DataFrame:
    """
    Hvis df har <= max_points -> returner df.
    Ellers: velg 1 pr gridcelle, prioritert på:
      1) qualityCode (lavest best)
      2) referenceTime (nyest best)
    I tillegg: behold alltid topp keep_top_n på høyeste value.
    """
    if df.empty or len(df) <= max_points:
        return df

    d = df.copy()
    d["qualityCode"] = pd.to_numeric(d.get("qualityCode"), errors="coerce")
    # NaN quality -> sett "dårlig" høyt tall
    d["qualityCode"] = d["qualityCode"].fillna(999999)

    # Gridstørrelse: sett slik at vi ender omtrent på max_points
    width = max(e - w, 1e-9)
    height = max(n - s, 1e-9)
    target_side = max(int(max_points ** 0.5), 10)

    cell_lon = width / target_side
    cell_lat = height / target_side

    # Minste cellestørrelse for å unngå ekstremt små ruter
    cell_lon = max(cell_lon, 0.02)
    cell_lat = max(cell_lat, 0.02)

    d["ix"] = ((d["lon"] - w) / cell_lon).astype(int)
    d["iy"] = ((d["lat"] - s) / cell_lat).astype(int)

    # Velg beste pr celle
    d_sorted = d.sort_values(
        ["iy", "ix", "qualityCode", "referenceTime"],
        ascending=[True, True, True, False],
    )
    grid_pick = (
        d_sorted.groupby(["iy", "ix"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    # Behold alltid topp N på nedbør (for å ikke miste “hendelser”)
    top_pick = d.sort_values("value", ascending=False).head(keep_top_n)

    out = pd.concat([grid_pick, top_pick], ignore_index=True)
    out = out.drop_duplicates(subset=["sourceId"], keep="first")

    # Hvis fortsatt for mange: trim (men behold beste kvalitet først)
    if len(out) > max_points:
        out = out.sort_values(["qualityCode", "referenceTime"], ascending=[True, False]).head(max_points)

    return out.reset_index(drop=True)


def make_empty_map_with_button(
    *,
    title: str,
    mode: str,
    date_str: str,
) -> str:
    """
    Raskt “tomt kart” (ingen datakall) + knapp som henter bbox og reload’er kartet i iframe.
    """
    # Grovt senter Norge
    m = folium.Map(location=[64.5, 11.0], zoom_start=5, tiles="OpenStreetMap")

    # Knapp + JS: henter Leaflet bounds og reloader samme endpoint med bbox
    # Folium-kartet har en map-variabel i JS; vi finner den ved å lete etter første globale med .getBounds.
    button_html = f"""
    <div style="
      position: fixed; top: 12px; left: 12px; z-index: 9999;
      background: rgba(255,255,255,.95); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 10px 30px rgba(15,23,42,.18);
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      max-width: 320px;
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
        // Folium lager en Leaflet-map i global scope; vi prøver å finne den.
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

        const qs = new URLSearchParams();
        qs.set('mode', '{mode}');
        if ('{date_str}') qs.set('date', '{date_str}');
        qs.set('bbox', bbox);

        window.location.href = '/ver/nedbor-kart?' + qs.toString();
      }});
    </script>
    """

    folium.Element(button_html).add_to(m.get_root().html)
    return m.get_root().render()

# ======================================================================
#  Bygg DF + HTML for gitt dato/mode
# ======================================================================

def build_precip_df(
    date_str: Optional[str] = None,
    *,
    mode: Mode = "day",
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
) -> tuple[pd.DataFrame, _date, str]:
    """
    Lager DF for nedbør:
      - last24h: rullerende siste 24 timer (summerer timeverdier)
      - day: kalenderdøgn (valgt dato)
      - mtd: sum fra månedstart til dato (inkl)
      - ytd: sum fra årstart til dato (inkl)

    Returnerer (df, day, title)
    """
    auth = _env_auth()
    day = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else _date.today()

    if mode == "last24h":
        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(hours=24)
        referencetime = f"{start_dt.isoformat()}/{now.isoformat()}"
        elements = ELEMENT_PRECIP_HOURLY
        title = "Nedbør siste 24 timer (rullerende)"
    elif mode == "day":
        start = day
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = "Nedbør kalenderdøgn (valgt dato)"
    elif mode == "mtd":
        start = _date(day.year, day.month, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = f"Nedbør hittil i måneden ({start.isoformat()} → {day.isoformat()})"
    elif mode == "ytd":
        start = _date(day.year, 1, 1)
        end = day + timedelta(days=1)
        referencetime = f"{start.isoformat()}/{end.isoformat()}"
        elements = ELEMENT_PRECIP_DAY
        title = f"Nedbør hittil i året ({start.isoformat()} → {day.isoformat()})"
    else:
        raise ValueError(f"Ukjent mode: {mode}")

    with requests.Session() as sess:
        sources = list_sources_with_element_in_referencetime(
            sess, auth=auth, referencetime=referencetime, elements=elements, timeout=timeout
        )
        if not sources:
            raise RuntimeError("Fant ingen stasjoner med nedbør-tidsserie i valgt intervall.")

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
            raise RuntimeError("Fikk ingen observasjoner i intervallet (selv om tidsserie finnes).")

        if mode == "last24h":
            out = aggregate_sum_per_station(obs, count_col="n_hours")
        elif mode == "day":
            picked = pick_day_value_per_station(obs, day=day)
            out = picked[["sourceId", "referenceTime", "value", "unit", "qualityCode"]].copy()
        elif mode in {"mtd", "ytd"}:
            out = aggregate_sum_per_station(obs, count_col="n_days")
        else:
            raise ValueError(f"Ukjent mode: {mode}")

        meta = get_sources_metadata(
            sess,
            auth=auth,
            sources=list(out["sourceId"].unique()),
            timeout=timeout,
            batch_size=batch_size,
        )

    df = out.merge(meta, on="sourceId", how="left")
    return df, day, title


def build_precip_map_html(
    date_str: Optional[str] = None,
    *,
    mode: Mode = "day",
    bbox: Optional[str] = None,
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
    """
    Uten bbox: returner raskt tomt kart med knapp.
    Med bbox: hent kun data for synlig område og bygg kartet på nytt.
    """

    # Dato default for UI (brukes for day/mtd/ytd)
    day_str = date_str or _date.today().isoformat()

    # 1) Ingen bbox -> superraskt kart uten datakall
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

    # 2) bbox finnes -> hent kun for området
    w, s, e, n = _parse_bbox(bbox)
    auth = _env_auth()

    # Finn referencetime + element basert på mode (samme logikk som i build_precip_df)
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
        # 2a) Hent stasjoner i bbox (metadata) – billig
        src_meta = fetch_sources_in_bbox(sess, auth=auth, w=w, s=s, e=e, n=n, timeout=timeout)
        if src_meta.empty:
            return make_empty_map_with_button(title=f"{title} (ingen stasjoner i området)", mode=str(mode), date_str=day_str)

        # Observations kan ofte ta baseId direkte. Vi bruker baseId-liste.
        sources = src_meta["baseId"].astype(str).tolist()

        # 2b) Hent observasjoner KUN for disse stasjonene
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
        # Ingen data i perioden i akkurat dette området
        return make_empty_map_with_button(title=f"{title} (ingen data i perioden)", mode=str(mode), date_str=day_str)

    # 2c) Aggreger (last24h/mtd/ytd = sum; day = pick)
    if mode == "day":
        picked = pick_day_value_per_station(obs, day=day)
        out = picked[["sourceId", "referenceTime", "value", "unit", "qualityCode"]].copy()
    else:
        out = aggregate_sum_per_station(obs, count_col=sum_count_col)

    # 2d) Merge inn lat/lon/name fra src_meta
    # obs.sourceId kan komme tilbake som "SNxxxx:0" selv om vi spurte på baseId,
    # så vi lager baseId kolonne for trygg merge.
    out["baseId"] = out["sourceId"].astype(str).map(base_source_id)
    meta = src_meta.rename(columns={"baseId": "baseId"})
    merged = out.merge(meta, on="baseId", how="left").drop(columns=["baseId"])

    merged = merged.dropna(subset=["lat", "lon", "value"])
    if merged.empty:
        return make_empty_map_with_button(title=f"{title} (ingen plottbare punkter)", mode=str(mode), date_str=day_str)

    # 2e) Nedskalering hvis veldig mange punkter (best kvalitet)
    merged = downsample_spatial_best_quality(
        merged,
        w=w, s=s, e=e, n=n,
        max_points=1200,
        keep_top_n=50,
    )

    # Oppdatert-stempel for last24h
    if mode == "last24h":
        updated = datetime.now(timezone.utc).strftime("%H:%M UTC")
        title = f"{title}<br><small>Oppdatert ca. {updated}</small>"

    # 2f) Bygg kart med punkter
    return make_map(
        merged,
        title=title,
        out_html=None,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_mm=heat_clip_mm,
    )


# ======================================================================
#  CLI (valgfritt)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Nedbør-kart fra Frost (last24h/day/MTD/YTD).")
    ap.add_argument("--date", default=_date.today().isoformat(), help="YYYY-MM-DD (UTC) (ignoreres for last24h)")
    ap.add_argument("--mode", default="last24h", choices=["last24h", "day", "mtd", "ytd"])
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--batch-size", type=int, default=80)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--qualities", default="0,1,2,3,4")

    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-html", default="")

    args = ap.parse_args()

    df, day, title = build_precip_df(
        date_str=args.date,
        mode=args.mode,  # type: ignore[arg-type]
        timeout=args.timeout,
        batch_size=args.batch_size,
        limit=args.limit,
        qualities=args.qualities,
    )

    out_csv = args.out_csv or f"precip_{args.mode}_{args.date}.csv"
    out_html = args.out_html or f"precip_map_{args.mode}_{args.date}.html"

    df.to_csv(out_csv, index=False)
    _ = make_map(df, title=title, out_html=out_html)

    print(f"Dato: {day} | Mode: {args.mode}")
    print(f"Stasjoner (i kart): {df['sourceId'].nunique()}")
    print(f"Skrev CSV:  {out_csv}")
    print(f"Skrev kart: {out_html}")


if __name__ == "__main__":
    main()
