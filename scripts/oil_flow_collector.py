#!/usr/bin/env python3
"""Collect TankerMap crude flows and publish compact S3 datasets for the web app."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import os
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3
import folium

API = "https://tankermap.com/api"
USER_AGENT = "PrisanalyseOilFlow/1.0 (low-frequency analytical use)"
BUCKET = os.getenv("OIL_TRACKER_S3_BUCKET") or os.getenv("S3_BUCKET_NAME") or "prisanalyse-data"
PREFIX = (os.getenv("OIL_TRACKER_S3_PREFIX") or "oil-tanker-data").strip("/")
REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"
BARRELS_PER_TONNE = 7.33
CARGO_SHARE_OF_DWT = 0.92

DESTINATION_ALIASES = {
    "China": ("CHINA", "ZHOUSHAN", "NINGBO", "QINGDAO", "RIZHAO", "DALIAN", "TIANJIN", "ZHAJIANG", "CAOFEIDIAN"),
    "India": ("INDIA", "SIKKA", "JAMNAGAR", "VADINAR", "MUNDRA", "PARADIP", "VISAKHAPATNAM", "MANGALORE", "CHENNAI"),
    "Japan": ("JAPAN", "CHIBA", "KASHIMA", "YOKOHAMA", "KAWASAKI", "MIZUSHIMA", "YOKKAICHI", "KIIRE"),
    "South Korea": ("KOREA", "ULSAN", "YEOSU", "DAESAN", "ONSAN", "INCHEON"),
    "USA": ("USA", "US GULF", "HOUSTON", "CORPUS CHRISTI", "LOOP", "NEW ORLEANS", "PORT ARTHUR", "BEAUMONT"),
    "Europe": ("ROTTERDAM", "EUROPOORT", "ANTWERP", "LE HAVRE", "FOS", "TRIESTE", "AUGUSTA", "MILFORD HAVEN", "WILHELMSHAVEN", "GDANSK"),
}
DESTINATION_COLORS = {
    "China": "#d73027",
    "India": "#f28e2b",
    "Japan": "#b07aa1",
    "South Korea": "#e15759",
    "Europe": "#4e79a7",
    "USA": "#59a14f",
    "Unclassified": "#8a8f98",
}
DESTINATION_LABELS = {
    "China": "Kina",
    "India": "India",
    "Japan": "Japan",
    "South Korea": "Sør-Korea",
    "Europe": "Europa",
    "USA": "USA",
    "Unclassified": "Annen / ukjent destinasjon",
}
FLOW_FIELDS = ["collected_at", "destination_region", "vessel_count", "loaded_count", "estimated_barrels", "total_dwt"]


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def api_get(path: str, retries: int = 3):
    req = urllib.request.Request(API + path, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt + 1 == retries:
                raise
            time.sleep(2**attempt)


def destination_region(vessel: dict) -> str | None:
    destination = str(vessel.get("destination") or "").upper().strip()
    for region, aliases in DESTINATION_ALIASES.items():
        if any(alias in destination for alias in aliases):
            return region
    return None


def estimate_barrels(vessel: dict) -> float:
    return float(vessel.get("deadweight") or 0) * CARGO_SHARE_OF_DWT * BARRELS_PER_TONNE


def aggregate(vessels: list[dict], collected_at: str) -> list[dict]:
    rows = {
        region: {"collected_at": collected_at, "destination_region": region, "vessel_count": 0,
                 "loaded_count": 0, "estimated_barrels": 0.0, "total_dwt": 0.0}
        for region in (*DESTINATION_ALIASES, "Unclassified")
    }
    for vessel in vessels:
        row = rows[destination_region(vessel) or "Unclassified"]
        row["vessel_count"] += 1
        row["total_dwt"] += float(vessel.get("deadweight") or 0)
        if vessel.get("cargo_state") == "loaded":
            row["loaded_count"] += 1
            row["estimated_barrels"] += estimate_barrels(vessel)
    return list(rows.values())


def s3_client():
    return boto3.client("s3", region_name=REGION)


def read_history(s3) -> list[dict]:
    key = f"{PREFIX}/aggregates/flow_history.csv"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        return []
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return []
        raise
    return list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8-sig"))))


def write_history(s3, existing: list[dict], new_rows: list[dict]) -> int:
    combined = {(r["collected_at"], r["destination_region"]): r for r in existing}
    combined.update({(r["collected_at"], r["destination_region"]): r for r in new_rows})
    ordered = [combined[key] for key in sorted(combined)]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=FLOW_FIELDS)
    writer.writeheader()
    writer.writerows(ordered)
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/aggregates/flow_history.csv",
                  Body=output.getvalue().encode("utf-8"), ContentType="text/csv")
    return len(ordered)


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 6371.0088 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def nearest_china_port(vessel: dict, ports: list[dict]) -> str | None:
    if vessel.get("latitude") is None or vessel.get("longitude") is None:
        return None
    matches = []
    for port in ports:
        distance = haversine_km(float(vessel["latitude"]), float(vessel["longitude"]), float(port["lat"]), float(port["lon"]))
        if distance <= max(float(port.get("radius_km") or 5), 5) + 2:
            matches.append((distance, port["slug"]))
    return min(matches)[1] if matches else None


def build_map(vessels: list[dict], ports: list[dict], output: Path) -> dict:
    m = folium.Map(location=[25, 105], zoom_start=3, tiles="CartoDB positron", control_scale=True,
                   prefer_canvas=True)
    layers = {
        region: folium.FeatureGroup(name=DESTINATION_LABELS[region], show=True).add_to(m)
        for region in DESTINATION_COLORS
    }
    counts = {DESTINATION_LABELS[region]: 0 for region in DESTINATION_COLORS}
    counts.update({"Lastet": 0, "Ballast / ukjent": 0, "Ved kinesisk oljeterminal": 0})
    for vessel in vessels:
        lat, lon = vessel.get("latitude"), vessel.get("longitude")
        if lat is None or lon is None:
            continue
        at_port = nearest_china_port(vessel, ports)
        region = destination_region(vessel) or ("China" if at_port else "Unclassified")
        color = DESTINATION_COLORS[region]
        loaded = vessel.get("cargo_state") == "loaded"
        capacity = estimate_barrels(vessel)
        radius = 3.5 + 5.5 * math.sqrt(min(capacity, 2_200_000) / 2_200_000) if capacity else 3.5
        counts[DESTINATION_LABELS[region]] += 1
        counts["Lastet" if loaded else "Ballast / ukjent"] += 1
        if at_port:
            counts["Ved kinesisk oljeterminal"] += 1
        volume_label = "Estimert oljevolum" if loaded else "Estimert kapasitet (last ikke bekreftet)"
        popup = "<br>".join((f"<b>{vessel.get('name') or 'Ukjent navn'}</b>", f"IMO: {vessel.get('imo')}",
                             f"DWT: {float(vessel.get('deadweight') or 0):,.0f}",
                             f"{volume_label}: {capacity/1e6:.2f} mill. fat",
                             f"Lastestatus: {vessel.get('cargo_state') or 'ukjent'}",
                             f"Destinasjonsregion: {DESTINATION_LABELS[region]}",
                             f"Destinasjon: {vessel.get('destination') or '-'}",
                             f"Fart: {float(vessel.get('speed_knots') or 0):.1f} knop",
                             f"Ved kinesisk terminal: {at_port}" if at_port else ""))
        folium.CircleMarker(
            [lat, lon], radius=radius, color=color, weight=1.5 if loaded else 1,
            fill=True, fill_color=color, fill_opacity=.78 if loaded else .08,
            opacity=.95 if loaded else .5, popup=folium.Popup(popup, max_width=360),
            tooltip=(f"{vessel.get('name') or 'Ukjent'} – {DESTINATION_LABELS[region]} – "
                     f"{capacity/1e6:.2f} mill. fat"),
        ).add_to(layers[region])
    port_layer = folium.FeatureGroup(name="Kinesiske oljeterminaler", show=True).add_to(m)
    for port in ports:
        folium.Marker([port["lat"], port["lon"]], tooltip=port["name"], icon=folium.Icon(color="darkgreen", icon="industry", prefix="fa")).add_to(port_layer)
    folium.LayerControl(collapsed=False).add_to(m)
    colors = "".join(
        f'<div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
        f'background:{color};margin-right:6px"></span>{DESTINATION_LABELS[region]}</div>'
        for region, color in DESTINATION_COLORS.items()
    )
    legend = f'''<div style="position:fixed;bottom:24px;left:12px;z-index:9999;background:white;
        padding:10px 12px;border:1px solid #aaa;border-radius:4px;font:12px/1.45 Arial;box-shadow:0 1px 4px #999">
        <b>Råoljetankere</b><br>Farge = destinasjonsregion<br>{colors}
        <div style="margin-top:7px"><span style="display:inline-block;width:10px;height:10px;border-radius:50%;
        background:#555;margin-right:6px"></span>Fylt = lastet</div>
        <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:1px solid #555;
        margin-right:6px"></span>Hul = ballast / ukjent</div>
        <div style="margin-top:7px">Størrelse = estimert kapasitet</div></div>'''
    m.get_root().html.add_child(folium.Element(legend))
    m.save(str(output))
    return counts


def main() -> int:
    collected_at = utcnow().isoformat()
    vessels = [v for v in api_get("/vessels/live") if "crude" in str(v.get("vessel_type", "")).lower() and v.get("imo")]
    all_ports = api_get("/ports")
    ports = [p for p in all_ports if p.get("country") == "China" and p.get("commodity") == "oil" and p.get("direction") == "import"]
    rows = aggregate(vessels, collected_at)
    relevant = [v for v in vessels if destination_region(v) or nearest_china_port(v, ports)]
    s3 = s3_client()

    stamp = utcnow()
    intraday_key = f"{PREFIX}/aggregates/intraday/year={stamp:%Y}/month={stamp:%m}/day={stamp:%d}/flows_{stamp:%Y%m%dT%H%M%SZ}.json"
    s3.put_object(Bucket=BUCKET, Key=intraday_key,
                  Body=json.dumps({"collected_at": collected_at, "regions": rows}).encode("utf-8"), ContentType="application/json")
    history_count = write_history(s3, read_history(s3), rows)
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/vessels/latest.json",
                  Body=json.dumps({"collected_at": collected_at, "vessels": relevant}).encode("utf-8"), ContentType="application/json")

    with tempfile.TemporaryDirectory() as tmp:
        map_path = Path(tmp) / "latest.html"
        counts = build_map(vessels, ports, map_path)
        s3.upload_file(str(map_path), BUCKET, f"{PREFIX}/maps/latest.html", ExtraArgs={"ContentType": "text/html"})

    china = next(row for row in rows if row["destination_region"] == "China")
    print(f"Published {len(vessels)} tankers, {history_count} aggregate rows and map to s3://{BUCKET}/{PREFIX}/")
    print(f"China-bound loaded: {china['loaded_count']} ({china['estimated_barrels']/1e6:.1f}m barrels). Map: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
