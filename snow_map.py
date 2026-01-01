#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

FROST_BASE = "https://frost.met.no"
ELEMENT_SNOW = "surface_snow_thickness"


def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        print("FEIL: FROST_CLIENT_ID er ikke satt.")
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def fetch_latest_snow_data() -> pd.DataFrame:
    auth = _env_auth()
    if not auth: return pd.DataFrame()

    # 1. Hent alle nyeste observasjoner for snø
    obs_params = {"sources": "@all", "elements": ELEMENT_SNOW, "latest": 1}
    print("Henter observasjoner fra Frost...")

    try:
        r = requests.get(f"{FROST_BASE}/observations/v0.jsonld", params=obs_params, auth=auth, timeout=30)
        if r.status_code != 200:
            print(f"Frost feil {r.status_code}: {r.text[:200]}")
            return pd.DataFrame()

        data = r.json().get("data", [])
        obs_list = []
        for item in data:
            sid = item.get("sourceId")
            for obs in item.get("observations", []):
                obs_list.append({
                    "sourceId": sid,
                    "baseId": sid.split(':')[0],
                    "value": obs.get("value"),
                    "time": obs.get("referenceTime")
                })

        if not obs_list: return pd.DataFrame()
        obs_df = pd.DataFrame(obs_list)

        # 2. Hent metadata i bolker (chunks) for å unngå for lange URL-er
        unique_ids = list(obs_df["baseId"].unique())
        meta_list = []
        chunk_size = 50  # Sikker størrelse for API-kall

        print(f"Henter metadata for {len(unique_ids)} stasjoner...")
        for i in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[i:i + chunk_size]
            m_params = {"ids": ",".join(chunk), "fields": "id,name,county,geometry"}
            rm = requests.get(f"{FROST_BASE}/sources/v0.jsonld", params=m_params, auth=auth, timeout=20)

            if rm.status_code == 200:
                for m in rm.json().get("data", []):
                    coords = m.get("geometry", {}).get("coordinates", [None, None])
                    meta_list.append({
                        "baseId": m.get("id"),
                        "name": m.get("name"),
                        "county": m.get("county"),
                        "lon": coords[0], "lat": coords[1]
                    })

        if not meta_list: return pd.DataFrame()

        # 3. Slå sammen dataene
        final_df = obs_df.merge(pd.DataFrame(meta_list), on="baseId", how="inner")
        return final_df.dropna(subset=["lat", "lon"])

    except Exception as e:
        print(f"Kritisk feil: {e}")
        return pd.DataFrame()


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    df = fetch_latest_snow_data()

    if df.empty:
        return "<div style='padding:20px;'><h3>Ingen snødata tilgjengelig</h3><p>Sjekk Frost-tilkoblingen.</p></div>"

    if county:
        df = df[df["county"].astype(str).str.lower() == county.strip().lower()]

    m = folium.Map(location=[65, 13], zoom_start=5, tiles="cartodbpositron")

    # Legg til varmekart for oversikt
    HeatMap([[r["lat"], r["lon"], min(r["value"] / 150, 1)] for _, r in df.iterrows()], radius=15).add_to(m)

    cluster = MarkerCluster(name="Stasjoner").add_to(m)
    for _, r in df.iterrows():
        # Fargekode basert på mengde
        color = "blue" if r["value"] > 100 else "cadetblue" if r["value"] > 30 else "orange"

        folium.CircleMarker(
            [r["lat"], r["lon"]],
            radius=6,
            color=color,
            fill=True,
            popup=f"<b>{r['name']}</b><br>Snø: {int(r['value'])} cm",
            tooltip=f"{r['name']}: {int(r['value'])} cm"
        ).add_to(cluster)

    return m.get_root().render()