#!/usr/bin/env python3
from __future__ import annotations
import os
import json
import time
import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster
from datetime import datetime, timedelta, timezone, date as _date
from pathlib import Path
from typing import Any, Iterator, Optional
from dotenv import load_dotenv

# --- Miljøvariabler ---
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
ELEMENT_SNOW = "surface_snow_thickness"


# --- Autentisering (Nøyaktig som din fungerende kode) ---
def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


# --- Hjelpefunksjoner for API (Paginering og Retries) ---
def frost_get_json(session, path, params, auth, timeout=20):
    url = f"{FROST_BASE}{path}"
    for _ in range(3):  # Enkel retry
        r = session.get(url, params=params, auth=auth, timeout=timeout)
        if r.status_code == 200: return r.json()
        if r.status_code == 404: return {"data": []}
        time.sleep(1)
    return {"data": []}


def iter_pages(session, path, params, auth):
    p = params.copy()
    first = frost_get_json(session, path, p, auth)
    yield first

    total = int(first.get("totalItemCount", 0))
    per_page = int(first.get("itemsPerPage", 0))
    if total <= 0 or per_page <= 0: return

    offset = per_page
    while offset < total:
        p["offset"] = offset
        page = frost_get_json(session, path, p, auth)
        yield page
        offset += per_page


# --- Datahenting ---
def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    auth = _env_auth()
    if not auth: return "<h1>Systemfeil: API-nøkkel mangler</h1>"

    # Dato-håndtering
    if date_str:
        target_day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_day = _date.today()

    start = target_day - timedelta(days=2)
    end = target_day + timedelta(days=1)
    ref_time = f"{start.isoformat()}/{end.isoformat()}"

    rows = []
    with requests.Session() as sess:
        # 1. Finn tilgjengelige kilder
        ts_params = {"referencetime": ref_time, "elements": ELEMENT_SNOW}
        source_ids = set()
        for page in iter_pages(sess, "/observations/availableTimeSeries/v0.jsonld", ts_params, auth):
            for item in page.get("data", []):
                if item.get("sourceId"): source_ids.add(item["sourceId"])

        if not source_ids:
            return "<h1>Ingen snødata funnet for denne perioden</h1>"

        # 2. Hent observasjoner i bolker
        sources_list = list(source_ids)
        for i in range(0, len(sources_list), 80):
            batch = sources_list[i:i + 80]
            obs_params = {"sources": ",".join(batch), "referencetime": ref_time, "elements": ELEMENT_SNOW, "latest": 1}
            page = frost_get_json(sess, "/observations/v0.jsonld", obs_params, auth)
            for item in page.get("data", []):
                sid = item["sourceId"]
                for obs in item.get("observations", []):
                    rows.append({"sourceId": sid, "baseId": sid.split(':')[0], "value": obs["value"],
                                 "time": obs["referenceTime"]})

        # 3. Hent Metadata
        unique_bases = list(set(r["baseId"] for r in rows))
        meta_map = {}
        for i in range(0, len(unique_bases), 50):
            m_params = {"ids": ",".join(unique_bases[i:i + 50]), "fields": "id,name,county,geometry"}
            m_page = frost_get_json(sess, "/sources/v0.jsonld", m_params, auth)
            for m in m_page.get("data", []):
                coords = m.get("geometry", {}).get("coordinates", [0, 0])
                meta_map[m["id"]] = {"name": m.get("name"), "county": m.get("county"), "lat": coords[1],
                                     "lon": coords[0]}

    # Kombiner og filtrer
    final_data = []
    for r in rows:
        if r["baseId"] in meta_map:
            m = meta_map[r["baseId"]]
            final_data.append({**r, **m})

    df = pd.DataFrame(final_data).dropna(subset=["lat", "lon"])
    if county:
        df = df[df["county"].astype(str).str.lower() == county.lower()]

    # --- Bygg Kart ---
    m = folium.Map(location=[65, 13], zoom_start=5, tiles="OpenStreetMap")
    HeatMap([[r["lat"], r["lon"], min(r["value"] / 200, 1)] for _, r in df.iterrows()], radius=15).add_to(m)

    cluster = MarkerCluster().add_to(m)
    for _, r in df.iterrows():
        folium.CircleMarker([r["lat"], r["lon"]], radius=6, color="blue" if r["value"] > 50 else "orange",
                            fill=True, popup=f"{r['name']}: {int(r['value'])} cm").add_to(cluster)

    # --- Topp 10 Panel ---
    top_10 = df.sort_values("value", ascending=False).head(10)
    top_html = """
    <div style="position: fixed; top: 10px; right: 10px; width: 220px; background: white; 
                padding: 10px; border: 2px solid #ccc; z-index: 9999; font-size: 12px; border-radius: 8px;">
        <b>Topp 10 snødybde</b><hr>
        <table style="width: 100%;">
    """
    for i, (_, r) in enumerate(top_10.iterrows()):
        top_html += f"<tr><td>{i + 1}. {r['name']}</td><td style='text-align:right'><b>{int(r['value'])} cm</b></td></tr>"
    top_html += "</table></div>"

    return m.get_root().render().replace("</body>", f"{top_html}</body>")