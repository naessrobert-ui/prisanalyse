#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv

# Last miljøvariabler
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 30
ELEMENT_SNOW = "surface_snow_thickness"


def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        print("FEIL: FROST_CLIENT_ID mangler i miljøvariabler!")
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def fetch_latest_snow_data(timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    auth = _env_auth()
    if not auth: return pd.DataFrame()

    # Vi definerer et tidsvindu (siste 14 dager) for å være sikre på at vi finner 'latest'
    now = datetime.now(timezone.utc)
    then = now - timedelta(days=14)
    ref_time = f"{then.strftime('%Y-%m-%dT%H:%M:%SZ')}/{now.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    params = {
        "sources": "@all",
        "elements": ELEMENT_SNOW,
        "referencetime": ref_time,
        "latest": 1,
        "fields": "sourceId,referenceTime,value,geometry,name,county,unit"
    }

    print(f"Henter snødata fra Frost med referencetime: {ref_time}...")

    try:
        r = requests.get(
            f"{FROST_BASE}/observations/v0.jsonld",
            params=params,
            auth=auth,
            timeout=timeout
        )

        if r.status_code != 200:
            print(f"Frost API feil: {r.status_code} - {r.text[:200]}")
            return pd.DataFrame()

        data = r.json().get("data", [])
        if not data:
            print("Frost returnerte 200 OK, men listen 'data' er tom.")
            return pd.DataFrame()

        rows = []
        for item in data:
            sid = item.get("sourceId")
            geom = item.get("geometry", {})
            coords = geom.get("coordinates", [None, None])

            for obs in item.get("observations", []):
                rows.append({
                    "sourceId": sid,
                    "name": item.get("name", sid),
                    "county": item.get("county", "Ukjent"),
                    "referenceTime": obs.get("referenceTime"),
                    "value": obs.get("value"),
                    "unit": obs.get("unit", "cm"),
                    "lat": coords[1],
                    "lon": coords[0]
                })

        df = pd.DataFrame(rows).dropna(subset=["lat", "lon", "value"])
        print(f"Suksess! Fant {len(df)} stasjoner med snødata.")
        return df

    except Exception as e:
        print(f"Nettverksfeil eller uventet feil: {e}")
        return pd.DataFrame()


def _color_cm(cm: float) -> str:
    if cm >= 150: return "darkblue"
    if cm >= 75:  return "blue"
    if cm >= 30:  return "cadetblue"
    if cm >= 10:  return "green"
    if cm >= 1:   return "orange"
    return "lightgray"


def build_top_panel_html(df: pd.DataFrame, region_label: str) -> str:
    if df.empty: return ""
    top_df = df.sort_values("value", ascending=False).head(10)
    data_json = json.dumps(df[["name", "county", "value", "lat", "lon"]].to_dict(orient="records"), ensure_ascii=False)

    html = f"""
    <style>
        #snow-top10-wrapper {{ position:absolute; top:10px; right:10px; background:white; padding:10px; border-radius:8px; 
        box-shadow:0 2px 10px rgba(0,0,0,0.2); z-index:1000; font-family: sans-serif; font-size: 12px; width: 250px; }}
        .snow-row:hover {{ background: #f0f0f0; cursor: pointer; }}
    </style>
    <div id="snow-top10-wrapper">
        <b>Topp snødybde - {region_label}</b>
        <table style="width:100%; margin-top:5px; border-collapse:collapse;">
    """
    for i, (_, r) in enumerate(top_df.iterrows()):
        html += f'<tr class="snow-row" onclick="window.mapObj.setView([{r["lat"]}, {r["lon"]}], 12)"><td>{i + 1}.</td><td>{r["name"]}</td><td style="text-align:right"><b>{int(r["value"])} cm</b></td></tr>'

    html += f"""
        </table>
    </div>
    <script id="snow-all-data" type="application/json">{data_json}</script>
    """
    return html


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    df = fetch_latest_snow_data()

    if df.empty:
        return "<div style='padding:20px;'><h3>Ingen snødata tilgjengelig akkurat nå</h3><p>Prøv igjen om litt eller sjekk loggene på Render.</p></div>"

    region = "Norge"
    if county:
        mask = df["county"].astype(str).str.lower() == county.strip().lower()
        if not df[mask].empty:
            df = df[mask]
            region = county

    m = folium.Map(location=[65, 13], zoom_start=5, tiles="cartodbpositron")

    # Legg map-objektet på window slik at JS-panelet kan nå det
    m.get_root().script.add_child(folium.Element("window.mapObj = null;"))

    HeatMap([[r["lat"], r["lon"], min(r["value"] / 150, 1.0)] for _, r in df.iterrows()], radius=15, blur=10).add_to(m)

    marker_cluster = MarkerCluster(name="Stasjoner").add_to(m)
    for _, r in df.iterrows():
        txt = f"<b>{r['name']}</b><br>Snødybde: {int(r['value'])} {r['unit']}<br><small>{r['referenceTime']}</small>"
        folium.CircleMarker(
            [r["lat"], r["lon"]],
            radius=7,
            color="black",
            weight=1,
            fill=True,
            fill_color=_color_cm(r["value"]),
            fill_opacity=0.7,
            popup=folium.Popup(txt, max_width=200)
        ).add_to(marker_cluster)

    if county:
        m.fit_bounds([[df.lat.min(), df.lon.min()], [df.lat.max(), df.lon.max()]])

    map_html = m.get_root().render()
    # Litt "hack" for å lagre map-referansen i window
    map_html = map_html.replace("var map_", "window.mapObj = L.map('map_", 1).replace("L.map(", "(", 1)

    return map_html + build_top_panel_html(df, region)