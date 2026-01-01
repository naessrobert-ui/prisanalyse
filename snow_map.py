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
ELEMENT_SNOW = "surface_snow_thickness"  # snødybde (cm)


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env + python-dotenv).")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(
        session: requests.Session,
        path: str,
        params: Optional[dict[str, Any]] = None,
        *,
        auth: FrostAuth,
        retries: int = 3,
        timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    url = f"{FROST_BASE}{path}"
    backoff = 1.0
    for _attempt in range(1, retries + 1):
        r = session.get(
            url,
            params=params,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
        if r.status_code == 200: return r.json()
        if r.status_code == 404: return r.json()
        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue
        raise RuntimeError(f"Frost-feil {r.status_code} for {url}: {r.text[:200]}")
    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk.")


# ======================================================================
#  Hente data lynraskt
# ======================================================================

def fetch_latest_snow_data(timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Henter de nyeste snødybdene for alle stasjoner i ett enkelt kall.
    Dette erstatter de gamle 'list_sources', 'metadata' og 'fallback' funksjonene.
    """
    auth = _env_auth()
    path = "/observations/v0.jsonld"

    # Vi ber om alt i ett kall: @all kilder, siste verdi (latest=1)
    # Vi inkluderer fields for å få metadata (navn, fylke, koordinater) direkte.
    params = {
        "sources": "@all",
        "elements": ELEMENT_SNOW,
        "latest": 1,
        "fields": "sourceId,referenceTime,value,geometry,name,county,unit"
    }

    rows = []
    with requests.Session() as sess:
        data = frost_get_json(sess, path, params, auth=auth, timeout=timeout)

        if "data" not in data or data.get("@type") == "ErrorResponse":
            return pd.DataFrame()

        for item in data["data"]:
            sid = item.get("sourceId")
            name = item.get("name", sid)
            county = item.get("county", "Ukjent")
            geom = item.get("geometry", {})
            coords = geom.get("coordinates", [None, None])  # [lon, lat]

            for obs in item.get("observations", []):
                rows.append({
                    "sourceId": sid,
                    "name": name,
                    "county": county,
                    "referenceTime": obs.get("referenceTime"),
                    "value": obs.get("value"),
                    "unit": obs.get("unit", "cm"),
                    "lat": coords[1],
                    "lon": coords[0]
                })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["referenceTime"] = pd.to_datetime(df["referenceTime"], utc=True)
        # Rens bort rader uten koordinater eller verdi
        df = df.dropna(subset=["lat", "lon", "value"])
    return df


# ======================================================================
#  Hjelpefunksjoner for filtrering og kart
# ======================================================================

def filter_df_by_county(df: pd.DataFrame, county: str) -> pd.DataFrame:
    if df.empty or not county: return df
    mask = df["county"].astype(str).str.lower() == county.strip().lower()
    return df.loc[mask].copy()


def _color_cm(cm: float) -> str:
    if cm >= 150: return "darkblue"
    if cm >= 75:  return "blue"
    if cm >= 30:  return "cadetblue"
    if cm >= 10:  return "green"
    if cm >= 1:   return "orange"
    return "lightgray"


def build_top_panel_html(df: pd.DataFrame, region_label: str, n: int = 10) -> str:
    if df.empty: return ""
    d = df.sort_values("value", ascending=False).head(n).copy()

    # Forbered data til JS
    records = df[["name", "county", "value", "unit", "lat", "lon"]].copy()
    records["station"] = records["name"]
    data_json = json.dumps(records.to_dict(orient="records"), ensure_ascii=False)

    parts = [
        "<style>#snow-top10-wrapper{position:absolute;top:12px;right:12px;background:white;padding:8px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.3);max-width:300px;font-family:sans-serif;font-size:11px;z-index:9999;}"
        ".snow-title{font-weight:bold;margin-bottom:5px;} table{width:100%;border-collapse:collapse;} td,th{text-align:left;padding:2px;border-bottom:1px solid #eee;} tr:hover{background:#f0f7ff;cursor:pointer;}</style>",
        "<div id='snow-top10-wrapper'><div class='snow-title'>Topp stasjoner - " + region_label + "</div>"
                                                                                                  "<button style='width:100%;margin-bottom:5px' id='btn-viewport'>Topp 10 i utsnitt</button>"
                                                                                                  "<table id='snow-table'><thead><tr><th>#</th><th>Stasjon</th><th>cm</th></tr></thead><tbody>"
    ]

    for i, (_, r) in enumerate(d.iterrows()):
        parts.append(
            f"<tr class='snow-row' data-lat='{r['lat']}' data-lon='{r['lon']}'><td>{i + 1}</td><td>{r['name']}</td><td>{r['value']:.0f}</td></tr>")

    parts.append("</tbody></table></div>")
    parts.append(f"<script id='snow-all-data' type='application/json'>{data_json}</script>")

    js = """
    (function(){
      window.addEventListener('load', function(){
        var snowData = JSON.parse(document.getElementById('snow-all-data').textContent);
        var mapObj = null; 
        if(window.L){ for(var k in window){ if(window[k] instanceof L.Map){ mapObj=window[k]; break; }}}
        function attach(){
          document.querySelectorAll('.snow-row').forEach(row => {
            row.onclick = () => { mapObj.setView([row.dataset.lat, row.dataset.lon], 11); };
          });
        }
        attach();
        document.getElementById('btn-viewport').onclick = () => {
          var b = mapObj.getBounds();
          var inside = snowData.filter(p => b.contains([p.lat, p.lon])).sort((a,b) => b.value - a.value).slice(0,10);
          var html = inside.map((p,i) => `<tr class='snow-row' data-lat='${p.lat}' data-lon='${p.lon}'><td>${i+1}</td><td>${p.station}</td><td>${Math.round(p.value)}</td></tr>`).join('');
          document.querySelector('#snow-table tbody').innerHTML = html; attach();
        };
      });
    })();
    """
    parts.append(f"<script>{js}</script>")
    return "".join(parts)


def inject_html_before_body_end(page_html: str, extra_html: str) -> str:
    return page_html.replace("</body>", f"{extra_html}</body>") if "</body>" in page_html else page_html + extra_html


# ======================================================================
#  Hovedfunksjoner for kartbyging
# ======================================================================

def make_map(df: pd.DataFrame, cluster: bool, show_heatmap: bool, zoom_to_data: bool) -> str:
    m = folium.Map(location=[65, 13], zoom_start=5, tiles="OpenStreetMap")

    if show_heatmap:
        heat_data = [[r["lat"], r["lon"], (r["value"] / 200) ** 0.5] for _, r in df.iterrows()]
        HeatMap(heat_data, radius=20, blur=15).add_to(folium.FeatureGroup(name="Varmekart").add_to(m))

    marker_layer = MarkerCluster().add_to(m) if cluster else m
    for _, r in df.iterrows():
        popup_txt = f"{r['name']}<br>Snødybde: <b>{r['value']:.0f} {r['unit']}</b><br>Tid: {r['referenceTime'].strftime('%d.%m %H:%M')}"
        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=6,
            color=_color_cm(r["value"]),
            fill=True,
            tooltip=r["name"],
            popup=folium.Popup(popup_txt, max_width=200)
        ).add_to(marker_layer)

    if zoom_to_data:
        m.fit_bounds([[df.lat.min(), df.lon.min()], [df.lat.max(), df.lon.max()]])

    folium.LayerControl().add_to(m)
    return m.get_root().render()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="Fylke-filter", default="")
    parser.add_argument("--no-cluster", action="store_true")
    args = parser.parse_args()

    print("Henter siste snødata fra Frost (dette tar nå kun få sekunder)...")
    start_time = time.time()

    df = fetch_latest_snow_data()
    if df.empty:
        print("Fant ingen data.")
        return

    region = "Norge"
    df_used = df
    if args.county:
        filtered = filter_df_by_county(df, args.county)
        if not filtered.empty:
            df_used = filtered
            region = args.county

    html_map = make_map(df_used, cluster=not args.no_cluster, show_heatmap=True, zoom_to_data=bool(args.county))
    top_panel = build_top_panel_html(df_used, region)
    final_html = inject_html_before_body_end(html_map, top_panel)

    out_file = "snow_map_latest.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(final_html)

    print(f"Ferdig på {time.time() - start_time:.1f} sekunder!")
    print(f"Antall stasjoner vist: {len(df_used)}")
    print(f"Kart lagret til: {out_file}")


if __name__ == "__main__":
    main()