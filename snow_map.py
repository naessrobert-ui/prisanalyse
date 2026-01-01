#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

FROST_BASE = "https://frost.met.no"
ELEMENT_SNOW = "surface_snow_thickness"


def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def fetch_latest_snow_data() -> pd.DataFrame:
    auth = _env_auth()
    if not auth:
        print("FROST_CLIENT_ID mangler.")
        return pd.DataFrame()

    # 1. Hent observasjoner (uten fields-parameteren som forårsaket 400-feil)
    obs_url = f"{FROST_BASE}/observations/v0.jsonld"
    obs_params = {
        "sources": "@all",
        "elements": ELEMENT_SNOW,
        "latest": 1
    }

    print("Henter observasjoner fra Frost...")
    try:
        r = requests.get(obs_url, params=obs_params, auth=auth, timeout=30)
        if r.status_code != 200:
            print(f"Frost Obs feil: {r.status_code} - {r.text[:200]}")
            return pd.DataFrame()

        data = r.json().get("data", [])
        obs_rows = []
        source_ids = []

        for item in data:
            sid = item.get("sourceId")
            source_ids.append(sid)
            for obs in item.get("observations", []):
                obs_rows.append({
                    "sourceId": sid,
                    "referenceTime": obs.get("referenceTime"),
                    "value": obs.get("value"),
                    "unit": obs.get("unit")
                })

        if not obs_rows:
            return pd.DataFrame()

        obs_df = pd.DataFrame(obs_rows)

        # 2. Hent metadata for de kildene vi fant
        # Vi splitter i SNxxxx (fjerner :0) for å treffe /sources
        base_ids = ",".join(list(set([sid.split(':')[0] for sid in source_ids[:500]])))  # Begrenser for sikkerhet

        meta_url = f"{FROST_BASE}/sources/v0.jsonld"
        meta_params = {"ids": base_ids, "fields": "id,name,county,geometry"}

        print("Henter metadata for stasjoner...")
        rm = requests.get(meta_url, params=meta_params, auth=auth, timeout=30)

        if rm.status_code == 200:
            meta_data = rm.json().get("data", [])
            meta_rows = []
            for m in meta_data:
                coords = m.get("geometry", {}).get("coordinates", [None, None])
                meta_rows.append({
                    "baseId": m.get("id"),
                    "name": m.get("name"),
                    "county": m.get("county"),
                    "lon": coords[0],
                    "lat": coords[1]
                })
            meta_df = pd.DataFrame(meta_rows)

            # Koble sammen (vi lager en link-kolonne)
            obs_df['baseId'] = obs_df['sourceId'].str.split(':').str[0]
            final_df = obs_df.merge(meta_df, on="baseId", how="inner")
            return final_df

    except Exception as e:
        print(f"Feil under henting: {e}")

    return pd.DataFrame()


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    df = fetch_latest_snow_data()

    if df.empty:
        return "<div style='padding:20px;'><h3>Ingen snødata tilgjengelig</h3><p>API-forespørselen returnerte ingen treff.</p></div>"

    if county:
        df = df[df["county"].astype(str).str.lower() == county.strip().lower()]

    m = folium.Map(location=[65, 13], zoom_start=5, tiles="cartodbpositron")
    marker_cluster = MarkerCluster().add_to(m)

    for _, r in df.iterrows():
        txt = f"<b>{r['name']}</b><br>Snø: {int(r['value'])} cm"
        folium.CircleMarker(
            [r["lat"], r["lon"]],
            radius=6,
            color="blue" if r['value'] > 50 else "orange",
            fill=True,
            popup=txt
        ).add_to(marker_cluster)

    return m.get_root().render()