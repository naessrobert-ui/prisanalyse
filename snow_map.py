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
        return "<h1>FROST_CLIENT_ID mangler i miljøvariabler</h1>"

    # 1. Tidsstyring - ser 2 dager tilbake for å sikre at vi finner data
    if not date_str:
        target_dt = datetime.now(timezone.utc)
    else:
        try:
            target_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except:
            target_dt = datetime.now(timezone.utc)

    start_time = (target_dt - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = target_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    referencetime = f"{start_time}/{end_time}"

    try:
        # 2. Hent observasjoner (Trygg metode)
        params = {
            "sources": "@all",
            "elements": ELEMENT_SNOW,
            "referencetime": referencetime,
            "latest": 1
        }

        r = requests.get(f"{FROST_BASE}/observations/v0.jsonld", params=params, auth=auth, timeout=30)
        if r.status_code != 200:
            return f"<h1>Kunne ikke hente data (Frost feil {r.status_code})</h1>"

        data = r.json().get('data', [])
        obs_rows = []
        source_ids = []

        for item in data:
            sid = item['sourceId']
            source_ids.append(sid.split(':')[0])
            for obs in item['observations']:
                obs_rows.append({
                    "sourceId": sid,
                    "baseId": sid.split(':')[0],
                    "value": obs['value'],
                    "time": obs['referenceTime']
                })

        if not obs_rows:
            return "<h1>Ingen snødata funnet for denne perioden</h1>"

        obs_df = pd.DataFrame(obs_rows)

        # 3. Hent metadata i bolker av 50 (for å unngå URL-begrensninger)
        meta_rows = []
        unique_ids = list(set(source_ids))

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

        df = obs_df.merge(pd.DataFrame(meta_rows), on="baseId", how="inner").dropna(subset=['lat', 'lon'])

        if county:
            df = df[df['county'].astype(str).str.lower() == county.strip().lower()]

        # 4. Generer kartet
        m = folium.Map(location=[65, 13], zoom_start=5)

        # Varmekart-lag
        HeatMap([[r['lat'], r['lon'], min(r['value'] / 200, 1)] for _, r in df.iterrows()], radius=15).add_to(m)

        # Markører
        cluster = MarkerCluster(name="Snømålinger").add_to(m)
        for _, r in df.iterrows():
            # Farge: Blå for mye snø (>70cm), Grønn for middels (>20cm), Oransje for lite
            color = "darkblue" if r['value'] > 100 else "blue" if r['value'] > 50 else "green" if r[
                                                                                                      'value'] > 10 else "orange"

            folium.CircleMarker(
                [r['lat'], r['lon']],
                radius=7,
                color=color,
                fill=True,
                fill_opacity=0.7,
                popup=f"<b>{r['name']}</b><br>Snødybde: {int(r['value'])} cm",
                tooltip=f"{r['name']}: {int(r['value'])} cm"
            ).add_to(cluster)

        # 5. Bygg Topp 10-listen som HTML-overlay
        top_10 = df.sort_values('value', ascending=False).head(10)
        top_html = """
        <div style="position: fixed; top: 10px; right: 10px; width: 240px; 
                    background: rgba(255,255,255,0.9); padding: 10px; border: 2px solid #ccc; 
                    z-index: 9999; font-family: sans-serif; font-size: 12px; border-radius: 8px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.2);">
            <b style="font-size: 14px;">Topp 10 snødybde</b><hr style="margin: 5px 0;">
            <table style="width: 100%; border-collapse: collapse;">
        """
        for i, (_, r) in enumerate(top_10.iterrows()):
            top_html += f"<tr><td>{i + 1}. {r['name']}</td><td style='text-align:right;'><b>{int(r['value'])} cm</b></td></tr>"

        top_html += "</table></div>"

        # Injiser listen i kart-HTML-en
        map_rendered = m.get_root().render()
        return map_rendered.replace("</body>", f"{top_html}</body>")

    except Exception as e:
        print(f"Feil: {e}")
        return f"<h1>Systemfeil</h1><p>{str(e)}</p>"