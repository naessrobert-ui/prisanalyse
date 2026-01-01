#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

import folium
from folium.plugins import HeatMap, MarkerCluster

# --- .env loading ---
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20
ELEMENT_SNOW = "surface_snow_thickness"


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID.")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(session: requests.Session, path: str, params: Optional[dict] = None, *, auth: FrostAuth,
                   retries: int = 6, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    url = f"{FROST_BASE}{path}"
    backoff = 1.0
    for _attempt in range(1, retries + 1):
        r = session.get(url, params=params, auth=(auth.client_id, auth.client_secret), timeout=timeout)
        if r.status_code == 200: return r.json()
        if r.status_code == 404: return r.json()
        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue
        break
    return {"data": []}


def iter_pages(session: requests.Session, path: str, params: dict, *, auth: FrostAuth, timeout: int) -> Iterator[
    dict[str, Any]]:
    p = params.copy()
    first = frost_get_json(session, path, p, auth=auth, timeout=timeout)
    yield first
    if first.get("@type") == "ErrorResponse": return
    try:
        total, offset, per_page = int(first.get("totalItemCount", 0)), int(first.get("offset", 0)), int(
            first.get("itemsPerPage", 0))
    except:
        return
    if total <= 0 or per_page <= 0: return
    next_offset = offset + per_page
    while next_offset < total:
        p2 = params.copy();
        p2["offset"] = next_offset
        page = frost_get_json(session, path, p2, auth=auth, timeout=timeout)
        yield page
        next_offset += per_page


def chunked(xs: list[str], n: int):
    for i in range(0, len(xs), n): yield xs[i: i + n]


def base_source_id(source_id: str) -> str:
    return source_id.split(":")[0]


def list_sources_for_day_window(session, *, auth, day, window_days, timeout) -> list[str]:
    start = day - timedelta(days=window_days)
    end = day + timedelta(days=window_days + 1)
    params = {"referencetime": f"{start.isoformat()}/{end.isoformat()}", "elements": ELEMENT_SNOW}
    source_ids = set()
    for page in iter_pages(session, "/observations/availableTimeSeries/v0.jsonld", params, auth=auth, timeout=timeout):
        for item in page.get("data", []):
            if item.get("sourceId"): source_ids.add(item["sourceId"])
    return sorted(source_ids)


def fetch_observations_interval(session, *, auth, sources, referencetime, timeout, batch_size, limit=1000,
                                qualities="") -> pd.DataFrame:
    rows = []
    for batch in chunked(sources, batch_size):
        params = {"sources": ",".join(batch), "referencetime": referencetime, "elements": ELEMENT_SNOW, "limit": limit}
        if qualities: params["qualities"] = qualities
        for page in iter_pages(session, "/observations/v0.jsonld", params, auth=auth, timeout=timeout):
            for item in page.get("data", []):
                sid, item_rt = item.get("sourceId"), item.get("referenceTime")
                for obs in item.get("observations", []):
                    rows.append({"sourceId": sid, "referenceTime": obs.get("referenceTime") or item_rt,
                                 "value": obs.get("value"), "unit": obs.get("unit")})
    df = pd.DataFrame(rows)
    if not df.empty: df["referenceTime"] = pd.to_datetime(df["referenceTime"], errors="coerce", utc=True)
    return df


def choose_nearest_per_station(df: pd.DataFrame, *, target_day: _date) -> pd.DataFrame:
    if df.empty: return df
    target_ts = pd.Timestamp(datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc))
    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["value"])
    d["abs_diff_s"] = (d["referenceTime"] - target_ts).abs().dt.total_seconds()
    return d.sort_values(["sourceId", "abs_diff_s"]).groupby("sourceId").head(1).reset_index(drop=True)


def get_sources_metadata(session, *, auth, sources, timeout, batch_size) -> pd.DataFrame:
    mapping = {sid: base_source_id(sid) for sid in sources}
    base_ids = sorted(set(mapping.values()))
    rows = []
    for batch in chunked(base_ids, batch_size):
        params = {"ids": ",".join(batch), "fields": "id,name,county,geometry"}
        for page in iter_pages(session, "/sources/v0.jsonld", params, auth=auth, timeout=timeout):
            for item in page.get("data", []):
                coords = item.get("geometry", {}).get("coordinates", [None, None])
                rows.append(
                    {"baseId": item.get("id"), "name": item.get("name"), "county": item.get("county"), "lat": coords[1],
                     "lon": coords[0]})
    meta = pd.DataFrame(rows)
    link = pd.DataFrame({"sourceId": list(mapping.keys()), "baseId": list(mapping.values())})
    return link.merge(meta, on="baseId", how="left").drop(columns=["baseId"])


def filter_df_by_county(df: pd.DataFrame, county: str) -> pd.DataFrame:
    if "county" not in df.columns or not county: return df
    mask = df["county"].astype(str).str.lower() == county.strip().lower()
    return df.loc[mask].copy()


def build_top_panel_html(df: pd.DataFrame, region_label: str, n: int = 10) -> str:
    if df.empty: return ""
    d = df.dropna(subset=["value", "lat", "lon"]).copy()
    d["station"] = d["name"].fillna(d["sourceId"])
    top = d.sort_values("value", ascending=False).head(n)

    parts = [
        "<style>#snow-top10-wrapper{position:absolute;top:12px;right:12px;background:white;padding:10px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.3);z-index:9999;font-family:sans-serif;font-size:12px;min-width:200px;}</style>"]
    parts.append(f"<div id='snow-top10-wrapper'><b>Topp {len(top)} - {region_label}</b><hr><table style='width:100%'>")
    for idx, r in top.reset_index(drop=True).iterrows():
        parts.append(
            f"<tr><td>{idx + 1}. {r['station']}</td><td style='text-align:right'><b>{int(r['value'])} cm</b></td></tr>")
    parts.append("</table></div>")
    return "".join(parts)


def inject_html_before_body_end(page_html: str, extra_html: str) -> str:
    return page_html.replace("</body>", f"{extra_html}</body>") if "</body>" in page_html else page_html + extra_html


def make_map(df, cluster, heatmap_show, heat_radius, heat_blur, heat_clip_cm, zoom_to_data=False) -> str:
    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    m = folium.Map(location=[d["lat"].mean(), d["lon"].mean()], zoom_start=5)

    if heatmap_show:
        HeatMap([[r["lat"], r["lon"], min(r["value"] / heat_clip_cm, 1)] for _, r in d.iterrows()], radius=heat_radius,
                blur=heat_blur).add_to(m)

    target = MarkerCluster().add_to(m) if cluster else m
    for _, r in d.iterrows():
        folium.CircleMarker([r["lat"], r["lon"]], radius=5, color="blue" if r["value"] > 50 else "green", fill=True,
                            popup=f"{r['name']}: {int(r['value'])} cm").add_to(target)

    if zoom_to_data: m.fit_bounds([[d.lat.min(), d.lon.min()], [d.lat.max(), d.lon.max()]])
    return m.get_root().render()


def build_snow_df_for_day(date_str=None, window_days=2) -> tuple[pd.DataFrame, _date]:
    auth = _env_auth()
    day = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else _date.today()
    with requests.Session() as sess:
        universe = list_sources_for_day_window(sess, auth=auth, day=day, window_days=window_days, timeout=20)
        df_all = fetch_observations_interval(sess, auth=auth, sources=universe,
                                             referencetime=f"{(day - timedelta(days=window_days)).isoformat()}/{(day + timedelta(days=1)).isoformat()}",
                                             timeout=20, batch_size=80)
        obs = choose_nearest_per_station(df_all, target_day=day)
        meta = get_sources_metadata(sess, auth=auth, sources=list(obs["sourceId"].unique()), timeout=20, batch_size=80)
        return obs.merge(meta, on="sourceId", how="left"), day


def build_snow_map_html(date_str=None, county=None, **kwargs) -> str:
    df, _ = build_snow_df_for_day(date_str)
    region = "Norge"
    if county:
        filtered = filter_df_by_county(df, county)
        if not filtered.empty: df, region = filtered, county

    html = make_map(df, cluster=True, heatmap_show=True, heat_radius=25, heat_blur=18, heat_clip_cm=200,
                    zoom_to_data=bool(county))
    return inject_html_before_body_end(html, build_top_panel_html(df, region))


if __name__ == "__main__":
    # For CLI bruk
    df, d = build_snow_df_for_day()
    print(df.sort_values("value", ascending=False).head(10))