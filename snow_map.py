#!/usr/bin/env python3
import os
import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

FROST_BASE = "https://frost.met.no"
ELEMENT_SNOW = "surface_snow_thickness"


def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    auth = _env_auth()
    if not auth:
        return "<h1>FROST_CLIENT_ID mangler</h1>"

    # 1. Bestem tidsintervall (bruker i dag som standard)
    if not date_str:
        target_dt = datetime.now(timezone.utc)
    else:
        target_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Vi henter data for de siste 2 dagene for å være sikre på å få treff
    start_time = (target_dt - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = target_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    referencetime = f"{start_time}/{end_time}"

    try:
        # 2. Hent observasjoner
        # Vi bruker et standard-kall som vi vet fungerer
        params = {
            "sources": "@all",
            "elements": ELEMENT_SNOW,
            "referencetime": referencetime,
            "latest": 1  # Henter kun den nyeste målingen per stasjon
        }

        r = requests.get(f"{FROST_BASE}/observations/v0.jsonld", params=params, auth=auth, timeout=30)
        if r.status_code != 200:
            return f"<h1>Feil fra Frost API ({r.status_code})</h1>"

        data = r.json().get('data', [])
        rows = []
        source_ids = []

        for item in data:
            sid = item['sourceId']
            source_ids.append(sid.split(':')[0])
            for obs in item['observations']:
                rows.append({
                    "sourceId": sid,
                    "baseId": sid.split(':')[0],
                    "value": obs['value'],
                    "time": obs['referenceTime']
                })

        if not rows:
            return "<h1>Ingen snødata funnet for valgt periode</h1>"

        obs_df = pd.DataFrame(rows)

        # 3. Hent metadata (Navn og koordinater) i mindre bolker
        meta_rows = []
        unique_ids = list(set(source_ids))

        # Vi henter metadata for 50 stasjoner om gangen for stabilitet
        for i in range(0, len(unique_ids), 50):
            chunk = unique_ids[i:i + 50]
            m_params = {"ids": ",".join(chunk), "fields": "id,name,county,geometry"}
            mr = requests.get(f"{FROST_BASE}/sources/v0.jsonld", params=m_params, auth=auth, timeout=20)
            if mr.status_code == 200:
                for m in mr.json().get('data', []):
                    coords = m.get('geometry', {}).get('coordinates', [None, None])
                    meta_rows.append({
                        "baseId": m['id'],
                        "name": m.get('name', m['id']),
                        "county": m.get('county', 'Ukjent'),
                        "lat": coords[1],
                        "lon": coords[0]
                    })

        meta_df = pd.DataFrame(meta_rows)
        df = obs_df.merge(meta_df, on="baseId", how="inner").dropna(subset=['lat', 'lon'])

        if county:
            df = df[df['county'].astype(str).str.lower() == county.strip().lower()]

        # 4. Bygg kartet
        m = folium.Map(location=[65, 13], zoom_start=5, tiles="OpenStreetMap")

        # Varmekart for visuell effekt
        HeatMap([[r['lat'], r['lon'], min(r['value'] / 150, 1)] for _, r in df.iterrows()], radius=15).add_to(m)

        cluster = MarkerCluster(name="Snøstasjoner").add_to(m)
        for _, r in df.iterrows():
            folium.CircleMarker(
                [r['lat'], r['lon']],
                radius=6,
                color="blue" if r['value'] > 50 else "orange",
                fill=True,
                popup=f"<b>{r['name']}</b><br>Snø: {int(r['value'])} cm",
                tooltip=f"{r['name']}: {int(r['value'])} cm"
            ).add_to(cluster)

        # 5. Legg til Topp 10-panelet (valgfritt, men praktisk)
        top_10 = df.sort_values('value', ascending=False).head(10)
        top_html = "<div style='position:fixed; top:10px; right:10px; width:220px; background:white; padding:10px; border:2px solid grey; z-index:9999; font-size:12px; border-radius:5px;'>"
        top_html += "<b>Topp 10 snødybde</b><br><table style='width:100%'>"
        for _, r in top_10.iterrows():
            top_html += f"<tr><td>{r['name']}</td><td style='text-align:right'><b>{int(r['value'])} cm</b></td></tr>"
        top_html += "</table></div>"

        # Kombiner kart og panel
        map_rendered = m.get_root().render()
        return map_rendered.replace("</body>", f"{top_html}</body>")

    except Exception as e:
        return f"<h1>Det oppstod en uventet feil</h1><p>{str(e)}</p>"