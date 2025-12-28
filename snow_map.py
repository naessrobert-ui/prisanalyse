#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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

# --- .env loading (robust on Windows/PyCharm) ----------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

ELEMENT_SNOW = "surface_snow_thickness"  # snødybde (cm)


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
    (Per nå spør vi hele universet; fylke-filtrering gjøres senere via metadata.)
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    params: dict[str, str | int] = {
        "referencetime": referencetime,
        "elements": ELEMENT_SNOW,
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


def list_sources_for_day_window(
    session: requests.Session,
    *,
    auth: FrostAuth,
    day: _date,
    window_days: int,
    timeout: int,
    limit: int = 1000,
) -> list[str]:
    """Finn stasjoner med snødybde-serie i [day-window, day+window+1)."""
    start = day - timedelta(days=window_days)
    end = day + timedelta(days=window_days + 1)
    referencetime = f"{start.isoformat()}/{end.isoformat()}"
    return list_sources_with_snow_in_referencetime(session, auth=auth, referencetime=referencetime, timeout=timeout)


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
    Returnerer DF med samme sourceId-format som i observations,
    og inkluderer county for fylke-filtrering.
    """
    path = "/sources/v0.jsonld"
    mapping = {sid: base_source_id(sid) for sid in sources}
    base_ids = sorted(set(mapping.values()))

    rows: list[dict[str, Any]] = []
    for batch in chunked(base_ids, batch_size):
        params: dict[str, str | int] = {
            "ids": ",".join(batch),
            "fields": "id,name,shortName,country,county,geometry",
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
                rows.append(
                    {
                        "baseId": item.get("id"),
                        "name": item.get("name"),
                        "shortName": item.get("shortName"),
                        "country": item.get("country"),
                        "county": item.get("county"),  # kan være None
                        "lat": lat,
                        "lon": lon,
                    }
                )

    meta = pd.DataFrame(rows, columns=["baseId", "name", "shortName", "country", "county", "lat", "lon"])
    link = pd.DataFrame({"sourceId": list(mapping.keys()), "baseId": list(mapping.values())})
    return link.merge(meta, on="baseId", how="left").drop(columns=["baseId"])


# ======================================================================
#  Fylke-filtrering og topp-10-hjelpere
# ======================================================================

def filter_df_by_county(df: pd.DataFrame, county: str) -> pd.DataFrame:
    """
    Filtrer på fylke (case-insensitivt). Bruker kolonnen 'county' fra /sources.
    Hvis 'county' ikke finnes, eller alle er NaN, returneres tom DF.
    """
    if "county" not in df.columns:
        return df.iloc[0:0].copy()

    county = (county or "").strip().lower()
    if not county:
        return df

    s = df["county"].astype(str).str.lower()
    mask = s == county
    return df.loc[mask].copy()


# ======================================================================
#  Fylke-filtrering og topp-10-hjelpere
# ======================================================================

def filter_df_by_county(df: pd.DataFrame, county: str) -> pd.DataFrame:
    """
    Filtrer på fylke (case-insensitivt). Bruker kolonnen 'county' fra /sources.
    Hvis 'county' ikke finnes, eller alle er NaN, returneres tom DF.
    """
    if "county" not in df.columns:
        return df.iloc[0:0].copy()

    county = (county or "").strip().lower()
    if not county:
        return df

    s = df["county"].astype(str).str.lower()
    mask = s == county
    return df.loc[mask].copy()


def build_top_panel_html(df: pd.DataFrame, region_label: str, n: int = 10) -> str:
    """
    Lager et lite overlay-panel med topp N-stasjoner.
    - Tabellen er liten, uten dato-kolonne (kun #, stasjon, fylke, snødybde).
    - Rader er klikkbare og zoomer kartet inn på stasjonen.
    - En knapp "Topp 10 i utsnitt" beregner topp 10 for gjeldende kartutsnitt.
    """
    if df.empty:
        return ""

    d = df.copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d = d.dropna(subset=["value", "lat", "lon"])

    # Sikre felter
    if "county" not in d.columns:
        d["county"] = ""
    d["station"] = d["name"].fillna(d["shortName"]).fillna(d["sourceId"])
    d["referenceTime"] = pd.to_datetime(d["referenceTime"], errors="coerce", utc=True)
    d["referenceTime"] = d["referenceTime"].dt.strftime("%Y-%m-%d %H:%M UTC")
    if "unit" not in d.columns:
        d["unit"] = "cm"

    # Full datastruktur til JS (for "topp 10 i utsnitt")
    records = d[["station", "county", "value", "unit", "referenceTime", "lat", "lon"]]
    data_json = json.dumps(records.to_dict(orient="records"), ensure_ascii=False)

    # Initial topp N til tabellen
    top = records.sort_values("value", ascending=False).head(n).reset_index(drop=True)
    title_text = f"Topp {len(top)} snøstasjoner – {region_label}"

    parts: list[str] = []

    # Litt styling: lite, hvitt panel oppe til høyre
    parts.append(
        "<style>"
        "#snow-top10-wrapper{"
        " position:absolute;"
        " top:12px;"
        " right:12px;"
        " background:white;"
        " padding:8px 10px;"
        " border-radius:8px;"
        " box-shadow:0 2px 6px rgba(0,0,0,0.25);"
        " max-width:360px;"
        " font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"
        " font-size:12px;"
        " z-index:9999;"
        "}"
        "#snow-top10-wrapper h3,#snow-top10-wrapper div.snow-top10-title{"
        " margin:0 0 4px 0;"
        " font-size:13px;"
        " font-weight:600;"
        "}"
        "#snow-top10-wrapper button{"
        " margin:4px 0 6px 0;"
        " padding:3px 6px;"
        " font-size:11px;"
        " border-radius:6px;"
        " border:1px solid #1d4ed8;"
        " background:#2563eb;"
        " color:white;"
        " cursor:pointer;"
        "}"
        "#snow-top10-wrapper button:hover{background:#1d4ed8;}"
        "#snow-top10-wrapper table{"
        " width:100%;"
        " border-collapse:collapse;"
        "}"
        "#snow-top10-wrapper th,#snow-top10-wrapper td{"
        " padding:2px 4px;"
        " border-bottom:1px solid #e5e7eb;"
        " text-align:left;"
        "}"
        "#snow-top10-wrapper th{"
        " font-weight:600;"
        " font-size:11px;"
        "}"
        "#snow-top10-wrapper tr.snow-top10-row:hover{"
        " background:#eff6ff;"
        "}"
        "</style>"
    )

    # Selve panelet + tabell
    parts.append(
        "<div id='snow-top10-wrapper' class='snow-top10-panel'>"
        f"<div id='snow-top10-title' class='snow-top10-title'>{title_text}</div>"
        "<button id='snow-top10-viewport' type='button'>Topp 10 i utsnitt</button>"
        "<table id='snow-top10-table'>"
        "<thead>"
        "<tr>"
        "<th>#</th>"
        "<th>Stasjon</th>"
        "<th>Fylke</th>"
        "<th>Snø (cm)</th>"
        "</tr>"
        "</thead>"
        "<tbody>"
    )

    for idx, r in top.iterrows():
        rank = idx + 1
        parts.append(
            "<tr class='snow-top10-row' "
            f"data-lat='{r['lat']}' data-lon='{r['lon']}'>"
            f"<td>{rank}</td>"
            f"<td>{r['station']}</td>"
            f"<td>{r['county']}</td>"
            f"<td>{float(r['value']):.0f}</td>"
            "</tr>"
        )

    parts.append("</tbody></table></div>")  # close table + wrapper

    # All data som JSON – brukes for "topp 10 i utsnitt"
    parts.append(
        "<script id='snow-all-data' type='application/json'>"
        + data_json +
        "</script>"
    )

    # JS: finn Leaflet-kartet, flytt panelet inn i kart-diven,
    # zoom ved klikk på rad, og regn ut topp 10 i gjeldende utsnitt.
    js = (
        "(function(){"
        "  var dataEl=document.getElementById('snow-all-data');"
        "  if(!dataEl) return;"
        "  var snowData=JSON.parse(dataEl.textContent||dataEl.innerText||'[]');"
        "  var mapObj=null;"
        "  if(window.L){"
        "    for(var k in window){"
        "      try{"
        "        if(window[k] instanceof L.Map){ mapObj=window[k]; break; }"
        "      }catch(e){}"
        "    }"
        "  }"
        "  var panel=document.getElementById('snow-top10-wrapper');"
        "  var titleEl=document.getElementById('snow-top10-title');"
        "  var mapEl=document.querySelector('.folium-map');"
        "  if(panel && mapEl){"
        "    mapEl.style.position='relative';"
        "    mapEl.appendChild(panel);"
        "  }"
        "  function attachRowHandlers(){"
        "    var rows=document.querySelectorAll('#snow-top10-table tbody tr');"
        "    rows.forEach(function(row){"
        "      row.style.cursor='pointer';"
        "      row.addEventListener('click',function(){"
        "        var lat=parseFloat(this.getAttribute('data-lat'));"
        "        var lon=parseFloat(this.getAttribute('data-lon'));"
        "        if(mapObj && !isNaN(lat) && !isNaN(lon)){"
        "          mapObj.setView([lat,lon],10);"
        "        }"
        "      });"
        "    });"
        "  }"
        "  attachRowHandlers();"
        "  var btn=document.getElementById('snow-top10-viewport');"
        f"  var defaultTitle='{title_text.replace(\"'\",\"\\\\'\")}';"
        "  if(btn && mapObj){"
        "    btn.addEventListener('click',function(){"
        "      var b=mapObj.getBounds();"
        "      var inside=snowData.filter(function(p){"
        "        return p.lat>=b.getSouth() && p.lat<=b.getNorth() && "
        "               p.lon>=b.getWest() && p.lon<=b.getEast();"
        "      });"
        "      inside.sort(function(a,b){return b.value-a.value;});"
        f"      var top=inside.slice(0,{n});"
        "      var tbody=document.querySelector('#snow-top10-table tbody');"
        "      if(!tbody) return;"
        "      tbody.innerHTML='';"
        "      top.forEach(function(p,idx){"
        "        var tr=document.createElement('tr');"
        "        tr.className='snow-top10-row';"
        "        tr.setAttribute('data-lat',p.lat);"
        "        tr.setAttribute('data-lon',p.lon);"
        "        tr.innerHTML="
        "          '<td>'+(idx+1)+'</td>' +"
        "          '<td>'+p.station+'</td>' +"
        "          '<td>'+(p.county||'')+'</td>' +"
        "          '<td>'+Math.round(p.value)+'</td>';"
        "        tbody.appendChild(tr);"
        "      });"
        "      if(titleEl){"
        f"        titleEl.textContent='Topp {n} snøstasjoner – utsnitt';"
        "      }"
        "      attachRowHandlers();"
        "    });"
        "  }"
        "})();"
    )
    parts.append("<script>" + js + "</script>")

    return "".join(parts)


def inject_html_before_body_end(page_html: str, extra_html: str) -> str:
    """
    Stikk inn extra_html rett før </body>.
    Hvis </body> ikke finnes, appendes bare på slutten.
    """
    if not extra_html:
        return page_html
    marker = "</body>"
    lower = page_html.lower()
    idx = lower.rfind(marker)
    if idx == -1:
        return page_html + "\n" + extra_html
    return page_html[:idx] + extra_html + "\n" + page_html[idx:]

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


def make_map(
    df: pd.DataFrame,
    *,
    out_html: Optional[str] = None,
    cluster: bool,
    heatmap_show: bool,
    heat_radius: int,
    heat_blur: int,
    heat_clip_cm: float,
    zoom_to_data: bool = False,
) -> str:
    """
    Lager folium-kart fra df.
    out_html: hvis satt, lagres kartet til denne filen.
    zoom_to_data: hvis True, zoomer kartet inn på min/max lat/lon i df
                  (perfekt for fylke-visning).
    Returnerer alltid HTML-strengen for kartet (for embedding i web-app).
    """
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

    # Heatmap
    clipped = d["value"].clip(lower=0, upper=heat_clip_cm)
    weights = (clipped / heat_clip_cm) ** 0.5
    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name="Heatmap snødybde", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=8).add_to(heat_layer)
    heat_layer.add_to(m)

    # Punktmarkører
    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    for _, r in d.iterrows():
        cm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
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
        ).add_to(layer_for_markers)

    if zoom_to_data:
        lat_min, lat_max = float(d["lat"].min()), float(d["lat"].max())
        lon_min, lon_max = float(d["lon"].min()), float(d["lon"].max())
        m.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]])

    folium.LayerControl().add_to(m)

    html_str = m.get_root().render()

    if out_html:
        m.save(out_html)

    return html_str


# ======================================================================
#  Bygg DF + HTML for gitt dato (brukes både av CLI og Flask)
# ======================================================================

def build_snow_df_for_day(
    date_str: Optional[str] = None,
    *,
    window_days: int = 2,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "",
) -> tuple[pd.DataFrame, _date]:
    """
    Lager data-frame med snødybde for valgt dag, med fallback ±window_days.
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
        universe = list_sources_for_day_window(
            sess, auth=auth, day=day, window_days=window_days, timeout=timeout
        )

        if not universe:
            raise RuntimeError("Fant ingen stasjoner med snødybde-serie i valgt vindu. Prøv større window_days.")

        # Trinn 1: kun valgt dag
        df_day = fetch_observations_interval(
            sess,
            auth=auth,
            sources=universe,
            referencetime=day_rt,
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
            qualities=qualities,
        )

        have = set(df_day["sourceId"].unique()) if not df_day.empty else set()
        missing = [s for s in universe if s not in have]

        # Trinn 2: fallback ±window_days for de som mangler
        df_fb = pd.DataFrame()
        if missing:
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

        meta = get_sources_metadata(
            sess,
            auth=auth,
            sources=list(obs["sourceId"].unique()),
            timeout=timeout,
            batch_size=batch_size,
            limit=limit,
        )

    df = obs.merge(meta, on="sourceId", how="left")
    return df, day


def build_snow_map_html(
    date_str: Optional[str] = None,
    *,
    county: Optional[str] = None,
    top_n: int = 10,
    window_days: int = 2,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "",
    cluster: bool = True,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_cm: float = 200.0,
) -> str:
    """
    Bygger snøkart for valgt dato og returnerer HTML-strengen.
    - Hvis county er satt (f.eks. 'Vestland'), filtreres dataene til dette fylket
      og kartet zoomes inn på disse stasjonene.
    - I tillegg embeddes en HTML-tabell med topp N stasjoner (mest snø).
    date_str: 'YYYY-MM-DD', eller None = i dag (siste mulige dato med fallback).
    """
    df, _day = build_snow_df_for_day(
        date_str=date_str,
        window_days=window_days,
        timeout=timeout,
        batch_size=batch_size,
        limit=limit,
        qualities=qualities,
    )

    region_label = "Norge"
    df_used = df

    if county:
        filtered = filter_df_by_county(df, county)
        if not filtered.empty:
            df_used = filtered
            region_label = county
        # Hvis filtrering gir tomt resultat, behold hele Norge,
        # men region_label får fortsatt være "Norge".

    html_map = make_map(
        df_used,
        out_html=None,
        cluster=cluster,
        heatmap_show=show_heatmap,
        heat_radius=heat_radius,
        heat_blur=heat_blur,
        heat_clip_cm=heat_clip_cm,
        zoom_to_data=bool(county),  # zoom inn når vi viser ett fylke
    )

    # Bygg topp-10-tabell og stikk den inn før </body>
    # Bygg topp-10-panel (overlay) og stikk det inn før </body>
    top_panel_html = build_top_panel_html(df_used, region_label=region_label, n=top_n)
    combined_html = inject_html_before_body_end(html_map, top_panel_html)
    return combined_html



# ======================================================================
#  CLI-main (valgfritt, men kjekt å beholde)
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Snødybde-kart fra Frost for valgt dato, med fallback ±N dager kun for "
            "stasjoner uten data. Kan filtrere på fylke og viser topp 10-stasjoner."
        )
    )
    ap.add_argument(
        "--date",
        help="Mål-dato (UTC) YYYY-MM-DD (default = i dag)",
        default=_date.today().isoformat(),
    )
    ap.add_argument("--window-days", type=int, default=2, help="Fallback-vindu ±N dager (kun for manglende stasjoner)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--batch-size", type=int, default=80)
    ap.add_argument("--limit", type=int, default=1000, help="Per-side limit for Frost (paging)")
    ap.add_argument("--qualities", default="", help="Valgfritt filter, f.eks. '0,1,2,3,4'. Tom = ingen filter.")

    ap.add_argument("--out-csv", default="", help="CSV-ut (tom = auto-navn)")
    ap.add_argument("--out-html", default="", help="HTML-kart (tom = auto-navn)")

    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--show-heatmap", action="store_true")
    ap.add_argument("--heat-radius", type=int, default=25)
    ap.add_argument("--heat-blur", type=int, default=18)
    ap.add_argument("--heat-clip-cm", type=float, default=200.0)

    ap.add_argument(
        "--county",
        "--fylke",
        dest="county",
        default="",
        help="Filtrer på fylke (case-insensitivt), f.eks. 'Vestland'. Tom = hele Norge.",
    )

    args = ap.parse_args()

    df, day = build_snow_df_for_day(
        date_str=args.date,
        window_days=args.window_days,
        timeout=args.timeout,
        batch_size=args.batch_size,
        limit=args.limit,
        qualities=args.qualities,
    )

    county = (args.county or "").strip()
    if not county:
        # Spør interaktivt hvis ikke gitt
        try:
            county = input("Oppgi fylke (f.eks. 'Vestland', enter = alle): ").strip()
        except EOFError:
            county = ""

    region_label = "Norge"
    df_used = df

    if county:
        filtered = filter_df_by_county(df, county)
        if not filtered.empty:
            df_used = filtered
            region_label = county
        else:
            print(f"Advarsel: fant ingen stasjoner med fylke='{county}'. Viser hele Norge i stedet.")

    out_csv = args.out_csv or f"snow_{args.date}_smartfallback_pm{args.window_days}d.csv"
    out_html = args.out_html or f"snow_map_{args.date}_smartfallback_pm{args.window_days}d.html"

    df_used.to_csv(out_csv, index=False)

    html_map = make_map(
        df_used,
        out_html=None,  # vi legger på topp-10 først, så lagrer manuelt
        cluster=not args.no_cluster,
        heatmap_show=args.show_heatmap,
        heat_radius=args.heat_radius,
        heat_blur=args.heat_blur,
        heat_clip_cm=args.heat_clip_cm,
        zoom_to_data=bool(county),
    )

    # Legg på topp-10 i HTML-en
    top_html = top_n_html_table(df_used, region_label=region_label, n=10)
    combined_html = inject_html_before_body_end(html_map, top_html)

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(combined_html)

    print(f"Dato: {day}")
    print(f"Stasjoner (i valgt region): {df_used['sourceId'].nunique()}")
    print(f"Region: {region_label}")
    print(f"Skrev CSV:  {out_csv}")
    print(f"Skrev kart: {out_html}")

    # Print topp-10 i terminalen
    top_df = compute_top_n_df(df_used, n=10)
    if not top_df.empty:
        print("\nTopp 10 stasjoner (mest snø):")
        print(top_df.to_string(index=False))


if __name__ == "__main__":
    main()
