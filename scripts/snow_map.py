#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

# --- .env loading (robust on Windows/PyCharm) ----------------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 20

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
    params: Optional[dict[str, str | int]] = None,
    *,
    auth: FrostAuth,
    retries: int = 6,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """GET med retries (429/5xx). Returnerer JSON (inkl. ErrorResponse ved 404)."""
    url = f"{FROST_BASE}{path}"
    backoff = 1.0

    for _attempt in range(1, retries + 1):
        r = session.get(
            url,
            params=params or None,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 404:
            # "No data found" kan være normalt
            return r.json()

        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Frost-feil {r.status_code} for {url}: {detail}")

    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk for {url}")


def iter_pages(
    session: requests.Session,
    path: str,
    params: dict[str, str | int],
    *,
    auth: FrostAuth,
    timeout: int,
) -> Iterator[dict[str, Any]]:
    """
    Robust paginering for Frost v0:
    - Følg offset/itemsPerPage/totalItemCount.
    (Mange endepunkter har ikke 'next'.)
    """
    p = params.copy()
    first = frost_get_json(session, path, p, auth=auth, timeout=timeout)
    yield first

    if first.get("@type") == "ErrorResponse":
        return

    try:
        total = int(first.get("totalItemCount", 0))
        offset = int(first.get("offset", 0))
        per_page = int(first.get("itemsPerPage", 0))
    except Exception:
        return

    if total <= 0 or per_page <= 0:
        return

    next_offset = offset + per_page
    while next_offset < total:
        p2 = params.copy()
        p2["offset"] = next_offset
        page = frost_get_json(session, path, p2, auth=auth, timeout=timeout)
        yield page

        if page.get("@type") == "ErrorResponse":
            return

        try:
            offset = int(page.get("offset", next_offset))
            per_page = int(page.get("itemsPerPage", per_page))
            total = int(page.get("totalItemCount", total))
        except Exception:
            return

        next_offset = offset + per_page


def chunked(xs: list[str], n: int) -> Iterator[list[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def base_source_id(source_id: str) -> str:
    return source_id.split(":")[0]


def list_sources_with_snow_in_referencetime(
    session: requests.Session,
    *,
    auth: FrostAuth,
    referencetime: str,
    timeout: int,
) -> list[str]:
    """
    Finn stasjoner som har tilgjengelig tidsserie for snødybde i referencetime-intervallet.
    NB: Ikke filtrer på timeoffsets/levels her for snødybde.
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    params: dict[str, str | int] = {
        "referencetime": referencetime,
        "elements": ELEMENT_SNOW,
    }

    source_ids: set[str] = set()
    for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
        if page.get("@type") == "ErrorResponse":
            continue
        for item in page.get("data", []):
            sid = item.get("sourceId")
            if sid:
                source_ids.add(sid)

    return sorted(source_ids)


def list_sources_for_day_window(
    session: requests.Session,
    *,
    auth: FrostAuth,
    day: _date,
    window_days: int,
    timeout: int,
    limit: int = 1000,
) -> list[str]:
    """Finn stasjoner med snødybde-serie i [day-window, day+window+1)."""
    start = day - timedelta(days=window_days)
    end = day + timedelta(days=window_days + 1)
    referencetime = f"{start.isoformat()}/{end.isoformat()}"
    return list_sources_with_snow_in_referencetime(session, auth=auth, referencetime=referencetime, timeout=timeout)


def fetch_observations_interval(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    referencetime: str,
    timeout: int,
    batch_size: int,
    limit: int = 1000,
    qualities: str = "",
) -> pd.DataFrame:
    """
    Hent observasjoner for sources i et interval.
    Returnerer lang DF med sourceId/referenceTime/value...
    """
    path = "/observations/v0.jsonld"
    rows: list[dict[str, Any]] = []

    for batch in chunked(sources, batch_size):
        params: dict[str, str | int] = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": ELEMENT_SNOW,
            "limit": limit,
        }
        if qualities:
            params["qualities"] = qualities

        for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                continue

            for item in page.get("data", []):
                sid = item.get("sourceId")
                item_rt = item.get("referenceTime")
                for obs in item.get("observations", []):
                    rows.append(
                        {
                            "sourceId": sid,
                            "referenceTime": obs.get("referenceTime") or item_rt,
                            "elementId": obs.get("elementId"),
                            "value": obs.get("value"),
                            "unit": obs.get("unit"),
                            "qualityCode": obs.get("qualityCode"),
                        }
                    )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["referenceTime"] = pd.to_datetime(df["referenceTime"], errors="coerce", utc=True)
    return df


def choose_nearest_per_station(
    df: pd.DataFrame,
    *,
    target_day: _date,
) -> pd.DataFrame:
    """
    Velg nærmeste observasjon til target_day (00:00 UTC)
    per sourceId (IKKE baseId).
    """
    if df.empty:
        return df

    target_dt = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    target_ts = pd.Timestamp(target_dt)

    d = df.dropna(subset=["referenceTime", "value"]).copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["value"])

    d["abs_diff_s"] = (d["referenceTime"] - target_ts).abs().dt.total_seconds()

    d = (
        d.sort_values(["sourceId", "abs_diff_s", "referenceTime"], ascending=[True, True, False])
        .groupby(["sourceId"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    d["diff_hours"] = (d["referenceTime"] - target_ts).dt.total_seconds() / 3600.0
    return d.drop(columns=["abs_diff_s"])


def get_sources_metadata(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: list[str],
    timeout: int,
    batch_size: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    /sources støtter ids=... (ikke sources=...).
    observations/sourceId kan være SNxxxx:0 -> vi spør /sources med SNxxxx.
    Returnerer DF med samme sourceId-format som i observations.
    """
    path = "/sources/v0.jsonld"
    mapping = {sid: base_source_id(sid) for sid in sources}
    base_ids = sorted(set(mapping.values()))

    rows: list[dict[str, Any]] = []
    for batch in chunked(base_ids, batch_size):
        params: dict[str, str | int] = {
            "ids": ",".join(batch),
            "fields": "id,name,shortName,country,geometry",
        }
        for page in iter_pages(session, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                continue
            for item in page.get("data", []):
                geom = item.get("geometry") or {}
                coords = geom.get("coordinates")
                lon = lat = None
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]
                rows.append(
                    {
                        "baseId": item.get("id"),
                        "name": item.get("name"),
                        "shortName": item.get("shortName"),
                        "country": item.get("country"),
                        "lat": lat,
                        "lon": lon,
                    }
                )

    meta = pd.DataFrame(rows, columns=["baseId", "name", "shortName", "country", "lat", "lon"])
    link = pd.DataFrame({"sourceId": list(mapping.keys()), "baseId": list(mapping.values())})
    return link.merge(meta, on="baseId", how="left").drop(columns=["baseId"])


def _color_cm(cm: float) -> str:
    if cm >= 150:
        return "darkblue"
    if cm >= 75:
        return "blue"
    if cm >= 30:
        return "cadetblue"
    if cm >= 10:
        return "green"
    if cm >= 1:
        return "orange"
    return "lightgray"


def make_map(
    df: pd.DataFrame,
    *,
    out_html: str,
    cluster: bool,
    heatmap_show: bool,
    heat_radius: int,
    heat_blur: int,
    heat_clip_cm: float,
) -> None:
    if df.empty:
        raise RuntimeError("Ingen data å plotte (df er tom).")

    d = df.dropna(subset=["lat", "lon", "value"]).copy()
    if d.empty:
        raise RuntimeError("Har data, men ingen rader med både koordinater og verdi.")

    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["lat", "lon", "value"])

    center_lat = float(d["lat"].mean())
    center_lon = float(d["lon"].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap")

    clipped = d["value"].clip(lower=0, upper=heat_clip_cm)
    weights = (clipped / heat_clip_cm) ** 0.5
    heat_data = [[float(lat), float(lon), float(w)] for lat, lon, w in zip(d["lat"], d["lon"], weights)]
    heat_layer = folium.FeatureGroup(name="Heatmap snødybde", show=heatmap_show)
    HeatMap(heat_data, radius=heat_radius, blur=heat_blur, min_opacity=0.2, max_zoom=8).add_to(heat_layer)
    heat_layer.add_to(m)

    points_layer = folium.FeatureGroup(name="Stasjoner (hover)", show=True)
    points_layer.add_to(m)
    layer_for_markers = MarkerCluster().add_to(points_layer) if cluster else points_layer

    for _, r in d.iterrows():
        cm = float(r["value"])
        name = (r.get("name") or r.get("shortName") or r["sourceId"])
        unit = r.get("unit") or "cm"
        t = r.get("referenceTime")
        t_str = pd.to_datetime(t).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "ukjent tid"

        diff_part = ""
        if "diff_hours" in d.columns and pd.notna(r.get("diff_hours")):
            diff_part = f"<br>Avvik: {float(r['diff_hours']):+.1f} timer"

        html = f"{name}<br>Snødybde: <b>{cm:.0f} {unit}</b><br>Tid: {t_str}{diff_part}"

        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=5,
            color=_color_cm(cm),
            fill=True,
            fill_opacity=0.85,
            tooltip=folium.Tooltip(html, sticky=True),
            popup=folium.Popup(html, max_width=320),
        ).add_to(layer_for_markers)

    folium.LayerControl().add_to(m)
    m.save(out_html)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Snødybde-kart fra Frost for valgt dato. Raskt + fallback ±N dager kun for stasjoner uten data."
    )
    ap.add_argument("--date", required=True, help="Mål-dato (UTC) YYYY-MM-DD")
    ap.add_argument("--window-days", type=int, default=2, help="Fallback-vindu ±N dager (kun for manglende stasjoner)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--batch-size", type=int, default=80)
    ap.add_argument("--limit", type=int, default=1000, help="Per-side limit for Frost (paging)")
    ap.add_argument("--qualities", default="", help="Valgfritt filter, f.eks. '0,1,2,3,4'. Tom = ingen filter.")

    ap.add_argument("--out-csv", default="", help="CSV-ut (tom = auto-navn)")
    ap.add_argument("--out-html", default="", help="HTML-kart (tom = auto-navn)")

    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--show-heatmap", action="store_true")
    ap.add_argument("--heat-radius", type=int, default=25)
    ap.add_argument("--heat-blur", type=int, default=18)
    ap.add_argument("--heat-clip-cm", type=float, default=200.0)

    args = ap.parse_args()
    auth = _env_auth()

    day = datetime.strptime(args.date, "%Y-%m-%d").date()
    day_start = day
    day_end = day + timedelta(days=1)
    day_rt = f"{day_start.isoformat()}/{day_end.isoformat()}"

    # Finn stasjoner i vinduet (så vi har komplett "univers")
    # Bruk samme window som fallback, ellers blir missing-lista misvisende.
    with requests.Session() as sess:
        universe = list_sources_for_day_window(
            sess, auth=auth, day=day, window_days=args.window_days, timeout=args.timeout
        )

        if not universe:
            raise RuntimeError("Fant ingen stasjoner med snødybde-serie i valgt vindu. Prøv større --window-days.")

        # Trinn 1: hent kun dagen (rask)
        df_day = fetch_observations_interval(
            sess,
            auth=auth,
            sources=universe,
            referencetime=day_rt,
            timeout=args.timeout,
            batch_size=args.batch_size,
            limit=args.limit,
            qualities=args.qualities,
        )

        have = set(df_day["sourceId"].unique()) if not df_day.empty else set()
        missing = [s for s in universe if s not in have]

        # Trinn 2: fallback kun for manglende
        df_fb = pd.DataFrame()
        if missing:
            fb_start = day - timedelta(days=args.window_days)
            fb_end = day + timedelta(days=args.window_days + 1)
            fb_rt = f"{fb_start.isoformat()}/{fb_end.isoformat()}"
            df_fb = fetch_observations_interval(
                sess,
                auth=auth,
                sources=missing,
                referencetime=fb_rt,
                timeout=args.timeout,
                batch_size=args.batch_size,
                limit=args.limit,
                qualities=args.qualities,
            )

        df_all = pd.concat([df_day, df_fb], ignore_index=True)
        obs = choose_nearest_per_station(df_all, target_day=day)

        if obs.empty:
            raise RuntimeError("Ingen observasjoner funnet selv med fallback. Prøv større --window-days eller annen dato.")

        meta = get_sources_metadata(
            sess,
            auth=auth,
            sources=list(obs["sourceId"].unique()),
            timeout=args.timeout,
            batch_size=args.batch_size,
            limit=args.limit,
        )

    out_csv = args.out_csv or f"snow_{args.date}_smartfallback_pm{args.window_days}d.csv"
    out_html = args.out_html or f"snow_map_{args.date}_smartfallback_pm{args.window_days}d.html"

    df = obs.merge(meta, on="sourceId", how="left")
    df.to_csv(out_csv, index=False)

    make_map(
        df,
        out_html=out_html,
        cluster=not args.no_cluster,
        heatmap_show=args.show_heatmap,
        heat_radius=args.heat_radius,
        heat_blur=args.heat_blur,
        heat_clip_cm=args.heat_clip_cm,
    )

    print(f"Universe-stasjoner i vindu: {len(universe)}")
    print(f"Treff på valgt dag: {len(set(have))}")
    print(f"Fallback-stasjoner spurt: {len(missing)}")
    print(f"Stasjoner (i kart): {df['sourceId'].nunique()}")
    print(f"Skrev CSV:  {out_csv}")
    print(f"Skrev kart: {out_html}")


if __name__ == "__main__":
    main()
