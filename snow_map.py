#!/usr/bin/env python3
import os
import requests
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))


def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    client_id = os.getenv("FROST_CLIENT_ID")
    if not client_id:
        return "<h1>Filingen mangler FROST_CLIENT_ID</h1>"

    # 1. Enkelt tidsvindu (Siste 24 timer)
    # Vi dropper avansert datovalg for et øyeblikk for å sikre at det virker
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(hours=24)
    referencetime = f"{yesterday.strftime('%Y-%m-%dT%H:%M')}/{now.strftime('%Y-%m-%dT%H:%M')}"

    # 2. Hent rådata (Kun observasjoner og kilde-ID)
    endpoint = "https://frost.met.no/observations/v0.jsonld"
    params = {
        "sources": "@all",
        "elements": "surface_snow_thickness",
        "referencetime": referencetime,
        "latest": 1
    }

    try:
        r = requests.get(endpoint, params=params, auth=(client_id, ""), timeout=20)

        if r.status_code != 200:
            return f"<h1>Frost API feil {r.status_code}</h1><p>Prøv å sjekke om API-nøkkelen din er aktiv.</p>"

        data = r.json().get('data', [])
        if not data:
            return "<h1>Ingen snødata funnet det siste døgnet</h1>"

        # 3. Hent metadata for stasjonene (Navn og posisjon)
        # Vi gjør dette i ett enkelt kall for alle stasjoner i Norge for å unngå krasj
        meta_url = "https://frost.met.no/sources/v0.jsonld"
        meta_params = {"types": "SensorStation", "country": "Norge", "fields": "id,name,geometry,county"}
        mr = requests.get(meta_url, params=meta_params, auth=(client_id, ""), timeout=20)

        stasjoner = {}
        if mr.status_code == 200:
            for s in mr.json().get('data', []):
                coords = s.get('geometry', {}).get('coordinates', [0, 0])
                stasjoner[s['id']] = {
                    "name": s.get('name', 'Ukjent'),
                    "lat": coords[1],
                    "lon": coords[0],
                    "county": s.get('county', 'Ukjent')
                }

        # 4. Kombiner data manuelt
        map_data = []
        for item in data:
            sid = item['sourceId'].split(':')[0]
            if sid in stasjoner:
                val = item['observations'][0]['value']
                s_info = stasjoner[sid]
                map_data.append({
                    "name": s_info['name'],
                    "lat": s_info['lat'],
                    "lon": s_info['lon'],
                    "value": val,
                    "county": s_info['county']
                })

        df = pd.DataFrame(map_data)
        if df.empty:
            return "<h1>Fant observasjoner, men mangler posisjonsdata</h1>"

        # 5. Lag kartet
        m = folium.Map(location=[65, 13], zoom_start=5)
        marker_cluster = MarkerCluster().add_to(m)

        for _, r in df.iterrows():
            folium.CircleMarker(
                location=[r['lat'], r['lon']],
                radius=8,
                popup=f"<b>{r['name']}</b>: {int(r['value'])} cm",
                color="blue" if r['value'] > 20 else "orange",
                fill=True
            ).add_to(marker_cluster)

        # 6. Topp 10 Liste
        top_10 = df.sort_values('value', ascending=False).head(10)
        list_html = '<div style="position:fixed; top:10px; right:10px; z-index:9999; background:white; padding:10px; border:2px solid black; border-radius:5px; font-family:sans-serif;">'
        list_html += '<b>Topp 10 snødybde</b><br><table>'
        for _, row in top_10.iterrows():
            list_html += f"<tr><td>{row['name']}</td><td style='padding-left:10px;'><b>{int(row['value'])} cm</b></td></tr>"
        list_html += '</table></div>'

        return m.get_root().render().replace("</body>", f"{list_html}</body>")

    except Exception as e:
        return f"<h1>Det skjedde en teknisk feil</h1><p>{str(e)}</p>"