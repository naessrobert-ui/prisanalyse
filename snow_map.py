#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# --- .env loading -------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 25
ELEMENT_SNOW = "surface_snow_thickness"


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID.")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(session: requests.Session, path: str, params: Optional[dict[str, Any]] = None, *, auth: FrostAuth,
                   retries: int = 3, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    url = f"{FROST_BASE}{path}"
    for _ in range(retries):
        try:
            r = session.get(url, params=params, auth=(auth.client_id, auth.client_secret), timeout=timeout)
            if r.status_code == 200: return r.json()
        except:
            continue
    return {}


# --- Rask datathenting ---
def fetch_latest_snow_data(timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    auth = _env_auth()
    params = {
        "sources": "@all",
        "elements": ELEMENT_SNOW,
        "latest": 1,
        "fields": "sourceId,referenceTime,value,geometry,name,county,unit"
    }
    with requests.Session() as sess:
        data = frost_get_json(sess, "/observations/v0.jsonld", params, auth=auth, timeout=timeout)
        if "data" not in data: return pd.DataFrame()
        rows = []
        for item in data["data"]:
            geom = item.get("geometry", {})
            coords = geom.get("coordinates", [None, None])
            for obs in item.get("observations", []):
                rows.append({
                    "sourceId": item.get("sourceId"),
                    "name": item.get("name"),
                    "county": item.get("county", "Ukjent"),
                    "referenceTime": obs.get("referenceTime"),
                    "value": obs.get("value"),
                    "unit": obs.get("unit", "cm"),
                    "lat": coords[1], "lon": coords[0]
                })
        df = pd.DataFrame(rows).dropna(subset=["lat", "lon", "value"])
        if not df.empty: df["referenceTime"] = pd.to_datetime(df["referenceTime"], utc=True)
        return df


# --- Kartlogikk ---
def _color_cm(cm: float) -> str:
    if cm >= 150: return "darkblue"
    if cm >= 75:  return "blue"
    if cm >= 30:  return "cadetblue"
    if cm >= 10:  return "green"
    if cm >= 1:   return "orange"
    return "lightgray"


def build_top_panel_html(df: pd.DataFrame, region_label: str, n: int = 10) -> str:
    if df.empty: return ""
    top_df = df.sort_values("value", ascending=False).head(n)
    data_json = json.dumps(df[["name", "county", "value", "lat", "lon"]].to_dict(orient="records"), ensure_ascii=False)

    parts = [
        "<style>#snow-top10-wrapper{position:absolute;top:12px;right:12px;background:white;padding:8px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.3);max-width:300px;font-family:sans-serif;font-size:11px;z-index:9999;}"
        ".snow-title{font-weight:bold;margin-bottom:5px;} table{width:100%;border-collapse:collapse;} td,th{text-align:left;padding:2px;border-bottom:1px solid #eee;} tr:hover{background:#f0f7ff;cursor:pointer;}</style>",
        f"<div id='snow-top10-wrapper'><div class='snow-title'>Topp stasjoner - {region_label}</div>"
        "<button style='width:100%;margin-bottom:5px' id='btn-viewport'>Topp 10 i utsnitt</button>"
        "<table id='snow-table'><thead><tr><th>#</th><th>Stasjon</th><th>cm</th></tr></thead><tbody>"
    ]
    for i, (_, r) in enumerate(top_df.iterrows()):
        parts.append(
            f"<tr class='snow-row' data-lat='{r['lat']}' data-lon='{r['lon']}'><td>{i + 1}</td><td>{r['name']}</td><td>{r['value']:.0f}</td></tr>")
    parts.append("</tbody></table></div>")
    parts.append(f"<script id='snow-all-data' type='application/json'>{data_json}</script>")
    parts.append("""<script>
    window.addEventListener('load', function(){
        var snowData = JSON.parse(document.getElementById('snow-all-data').textContent);
        var mapObj = null; 
        if(window.L){ for(var k in window){ if(window[k] instanceof L.Map){ mapObj=window[k]; break; }}}
        function attach(){ document.querySelectorAll('.snow-row').forEach(row => { row.onclick = () => { mapObj.setView([row.dataset.lat, row.dataset.lon], 11); }; }); }
        attach();
        document.getElementById('btn-viewport').onclick = () => {
            var b = mapObj.getBounds();
            var inside = snowData.filter(p => b.contains([p.lat, p.lon])).sort((a,b) => b.value - a.value).slice(0,10);
            document.querySelector('#snow-table tbody').innerHTML = inside.map((p,i) => `<tr class='snow-row' data-lat='${p.lat}' data-lon='${p.lon}'><td>${i+1}</td><td>${p.name}</td><td>${Math.round(p.value)}</td></tr>`).join('');
            attach();
        };
    });</script>""")
    return "".join(parts)


def inject_html_before_body_end(page_html: str, extra_html: str) -> str:
    return page_html.replace("</body>", f"{extra_html}</body>") if "</body>" in page_html else page_html + extra_html


# --- HOVEDFUNKSJON FOR EKSTERN IMPORT (Flask/ver_routes) ---
def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    """Denne funksjonen brukes av Flask-appen din."""
    df = fetch_latest_snow_data()
    if df.empty: return "<h1>Ingen snødata tilgjengelig akkurat nå</h1>"

    region = "Norge"
    if county:
        mask = df["county"].astype(str).str.lower() == county.strip().lower()
        if not df[mask].empty:
            df = df[mask]
            region = county

    m = folium.Map(location=[65, 13], zoom_start=5)
    HeatMap([[r["lat"], r["lon"], (r["value"] / 200) ** 0.5] for _, r in df.iterrows()], radius=20, blur=15).add_to(m)

    marker_cluster = MarkerCluster().add_to(m)
    for _, r in df.iterrows():
        txt = f"{r['name']}<br>Snø: <b>{r['value']:.0f} cm</b>"
        folium.CircleMarker([r["lat"], r["lon"]], radius=6, color=_color_cm(r["value"]), fill=True, popup=txt).add_to(
            marker_cluster)

    if county: m.fit_bounds([[df.lat.min(), df.lon.min()], [df.lat.max(), df.lon.max()]])

    map_html = m.get_root().render()
    return inject_html_before_body_end(map_html, build_top_panel_html(df, region))


if __name__ == "__main__":
    print("Lagrer test-kart...")
    with open("test_map.html", "w", encoding="utf-8") as f:
        f.write(build_snow_map_html())