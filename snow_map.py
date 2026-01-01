#!/usr/bin/env python3
import os
import requests
import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
from datetime import datetime, timedelta, date as _date
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Last miljøvariabler
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()


def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    auth = _env_auth()
    if not auth:
        return "<h1>Manglende API-nøkkel (FROST_CLIENT_ID)</h1>"

    # 1. Dato-håndtering
    # Frost feil 400 skjer ofte pga feil datoformat. Vi sikrer ISO-format her.
    try:
        if date_str:
            target_dt = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            target_dt = datetime.now()
    except:
        target_dt = datetime.now()

    # Vi henter data for de siste 24 timene - dette er raskest og gir færrest feil
    start_time = (target_dt - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    end_time = target_dt.strftime("%Y-%m-%dT%H:%M:%S")
    referencetime = f"{start_time}/{end_time}"

    session = requests.Session()

    try:
        # 2. Hent snø-observasjoner (raskeste metode)
        obs_url = "https://frost.met.no/observations/v0.jsonld"
        params = {
            "sources": "@all",
            "elements": "surface_snow_thickness",
            "referencetime": referencetime,
            "latest": 1
        }

        r = session.get(obs_url, params=params, auth=auth, timeout=25)

        if r.status_code == 400:
            return f"<h1>Frost feil 400</h1><p>API-et forsto ikke forespørselen. Prøv en annen dato.</p>"
        if r.status_code != 200:
            return f"<h1>Kunne ikke hente data (Frost feil {r.status_code})</h1>"

        data = r.json().get('data', [])
        if not data:
            return "<h1>Ingen snødata tilgjengelig for valgt tidspunkt</h1>"

        # 3. Hent Metadata for alle stasjoner (for å få koordinater og fylke)
        meta_url = "https://frost.met.no/sources/v0.jsonld"
        meta_params = {"fields": "id,name,geometry,county"}
        mr = session.get(meta_url, params=meta_params, auth=auth, timeout=25)

        stasjoner = {}
        if mr.status_code == 200:
            for s in mr.json().get('data', []):
                coords = s.get('geometry', {}).get('coordinates', [None, None])
                stasjoner[s['id']] = {
                    "name": s.get('name', s['id']),
                    "lat": coords[1],
                    "lon": coords[0],
                    "county": s.get('county', 'Ukjent')
                }

        # 4. Koble sammen
        rows = []
        for item in data:
            base_id = item['sourceId'].split(':')[0]
            if base_id in stasjoner:
                s = stasjoner[base_id]
                if s['lat'] is not None:
                    val = item['observations'][0]['value']
                    rows.append({
                        "name": s['name'],
                        "lat": s['lat'],
                        "lon": s['lon'],
                        "value": val,
                        "county": s['county']
                    })

        df = pd.DataFrame(rows)
        if county:
            df = df[df["county"].astype(str).str.lower() == county.lower()]

        if df.empty:
            return "<h1>Ingen data funnet for valgt område</h1>"

        # 5. Bygg kartet
        m = folium.Map(location=[65, 13], zoom_start=5)

        # Varmekart (Heatmap)
        heat_data = [[r['lat'], r['lon'], min(r['value'] / 100, 1)] for _, r in df.iterrows()]
        HeatMap(heat_data, radius=15, blur=10).add_to(m)

        # Cluster for stasjoner
        cluster = MarkerCluster().add_to(m)
        for _, r in df.iterrows():
            folium.CircleMarker(
                [r['lat'], r['lon']],
                radius=6,
                color="blue" if r['value'] > 40 else "green",
                fill=True,
                popup=f"<b>{r['name']}</b><br>Snø: {int(r['value'])} cm"
            ).add_to(cluster)

        # 6. Topp 10 Liste
        top_10 = df.sort_values("value", ascending=False).head(10)
        top_html = """
        <div style="position: fixed; top: 10px; right: 10px; width: 220px; 
                    background: white; padding: 10px; border: 2px solid #ccc; 
                    z-index: 9999; font-size: 12px; border-radius: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.2);">
            <b>Topp 10 snødybde</b><hr>
            <table style="width: 100%;">
        """
        for i, (_, r) in enumerate(top_10.iterrows()):
            top_html += f"<tr><td>{i + 1}. {r['name']}</td><td style='text-align:right'><b>{int(r['value'])} cm</b></td></tr>"
        top_html += "</table></div>"

        return m.get_root().render().replace("</body>", f"{top_html}</body>")

    except Exception as e:
        return f"<h1>Systemfeil</h1><p>{str(e)}</p>"