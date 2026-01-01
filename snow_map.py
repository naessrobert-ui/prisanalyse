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

# --- Last miljøvariabler ---
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
ELEMENT_SNOW = "surface_snow_thickness"


# ======================================================================
# API Hjelpefunksjoner (Fra ditt fungerende skript)
# ======================================================================

def _env_auth():
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        return None
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(session, path, params, auth, timeout=20) -> dict[str, Any]:
    """Henter JSON med enkel retry-logikk."""
    url = f"{FROST_BASE}{path}"
    cid, secret = auth
    for attempt in range(3):
        try:
            r = session.get(url, params=params, auth=(cid, secret), timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return {"data": [], "@type": "ErrorResponse"}
            time.sleep(1 * (attempt + 1))
        except Exception:
            time.sleep(1)
    return {"data": []}


def iter_pages(session, path, params, auth) -> Iterator[dict[str, Any]]:
    """Håndterer paginering i Frost API."""
    p = params.copy()
    first = frost_get_json(session, path, p, auth)
    yield first

    total = int(first.get("totalItemCount", 0))
    per_page = int(first.get("itemsPerPage", 0))
    if total <= 0 or per_page <= 0:
        return

    offset = per_page
    while offset < total:
        p["offset"] = offset
        page = frost_get_json(session, path, p, auth)
        yield page
        offset += per_page


# ======================================================================
# Hovedfunksjon for kartbygging
# ======================================================================

def build_snow_map_html(date_str: Optional[str] = None, county: Optional[str] = None, **kwargs) -> str:
    auth = _env_auth()
    if not auth:
        return "<h1>Filingen mangler FROST_CLIENT_ID</h1>"

    # 1. Dato-håndtering (Sjekker 2 dager bakover for best dekning)
    if date_str:
        try:
            target_day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            target_day = _date.today()
    else:
        target_day = _date.today()

    start = target_day - timedelta(days=2)
    end = target_day + timedelta(days=1)
    ref_time = f"{start.isoformat()}/{end.isoformat()}"

    rows = []
    with requests.Session() as sess:
        # 2. Finn tilgjengelige tidsserier for snødybde
        ts_params = {"referencetime": ref_time, "elements": ELEMENT_SNOW}
        source_ids = set()

        for page in iter_pages(sess, "/observations/availableTimeSeries/v0.jsonld", ts_params, auth):
            for item in page.get("data", []):
                sid = item.get("sourceId")
                if sid:
                    source_ids.add(sid)

        if not source_ids:
            return "<h1>Ingen snødata funnet for valgt periode</h1>"

        # 3. Hent de nyeste observasjonene i bolker (Batching)
        sources_list = list(source_ids)
        for i in range(0, len(sources_list), 80):
            batch = sources_list[i:i + 80]
            obs_params = {
                "sources": ",".join(batch),
                "referencetime": ref_time,
                "elements": ELEMENT_SNOW,
                "latest": 1
            }
            obs_page = frost_get_json(sess, "/observations/v0.jsonld", obs_params, auth)
            for item in obs_page.get("data", []):
                sid = item.get("sourceId")
                for obs in item.get("observations", []):
                    rows.append({
                        "sourceId": sid,
                        "baseId": sid.split(':')[0],
                        "value": obs["value"],
                        "time": obs["referenceTime"]
                    })

        # 4. Hent metadata for stasjonene (Posisjon og Navn)
        unique_bases = list(set(r["baseId"] for r in rows))
        meta_map = {}
        for i in range(0, len(unique_bases), 50):
            chunk = unique_bases[i:i + 50]
            m_params = {"ids": ",".join(chunk), "fields": "id,name,county,geometry"}
            m_page = frost_get_json(sess, "/sources/v0.jsonld", m_params, auth)
            for m in m_page.get("data", []):
                coords = m.get("geometry", {}).get("coordinates", [None, None])
                if coords[0] is not None:
                    meta_map[m["id"]] = {
                        "name": m.get("name", m["id"]),
                        "county": m.get("county", "Ukjent"),
                        "lat": coords[1],
                        "lon": coords[0]
                    }

    # 5. Slå sammen observasjoner og metadata
    final_data = []
    for r in rows:
        m_info = meta_map.get(r["baseId"])
        if m_info:
            final_data.append({
                "name": m_info["name"],
                "county": m_info["county"],
                "lat": m_info["lat"],
                "lon": m_info["lon"],
                "value": r["value"]
            })

    if not final_data:
        return "<h1>Kunne ikke koble snømålinger til kartposisjoner</h1>"

    df = pd.DataFrame(final_data)

    # Filtrer på fylke hvis valgt
    if county:
        df = df[df["county"].astype(str).str.lower() == county.lower()]
        if df.empty:
            return f"<h1>Ingen data funnet for {county}</h1>"

    # 6. Bygg Folium-kartet
    m = folium.Map(location=[65, 13], zoom_start=5, tiles="OpenStreetMap")

    # Varmekart-lag
    heat_data = [[r["lat"], r["lon"], min(r["value"] / 200, 1)] for _, r in df.iterrows()]
    HeatMap(heat_data, radius=15, blur=10).add_to(m)

    # Markør-cluster
    cluster = MarkerCluster(name="Snøstasjoner").add_to(m)
    for _, r in df.iterrows():
        # Fargekode: Blå (>50cm), Grønn (>10cm), Oransje (<10cm)
        color = "darkblue" if r["value"] > 100 else "blue" if r["value"] > 50 else "green" if r[
                                                                                                  "value"] > 10 else "orange"
        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=6,
            color=color,
            fill=True,
            fill_opacity=0.7,
            popup=f"<b>{r['name']}</b><br>Snødybde: {int(r['value'])} cm",
            tooltip=f"{r['name']}: {int(r['value'])} cm"
        ).add_to(cluster)

    # 7. Topp 10 Panel (Overlay HTML)
    top_10 = df.sort_values("value", ascending=False).head(10)
    top_html = """
    <div style="position: fixed; top: 10px; right: 10px; width: 230px; 
                background: rgba(255,255,255,0.95); padding: 12px; border: 1px solid #999; 
                z-index: 9999; font-family: Arial, sans-serif; font-size: 12px; 
                border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <b style="font-size: 13px;">Topp 10 snødybde</b><hr style="margin: 8px 0; border: 0; border-top: 1px solid #eee;">
        <table style="width: 100%; border-collapse: collapse;">
    """
    for i, (_, r) in enumerate(top_10.iterrows()):
        top_html += f"<tr><td style='padding:2px 0;'>{i + 1}. {r['name']}</td><td style='text-align:right;'><b>{int(r['value'])} cm</b></td></tr>"
    top_html += "</table></div>"

    # Injiser panelet og returner
    return m.get_root().render().replace("</body>", f"{top_html}</body>")