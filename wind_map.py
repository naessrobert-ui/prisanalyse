#!/usr/bin/env python3
from __future__ import annotations

from datetime import date as _date
from datetime import datetime, time, timedelta, timezone
from html import escape
import json
from typing import Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

_ALLOWED_MODES = {"observed", "forecast"}
_ALLOWED_PERIODS = {"hour", "day", "month"}
_ALLOWED_METRICS = {"avg", "gust", "peak"}
_ALLOWED_REGIONS = {"all", "south", "mid", "north"}

_METNO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
_HEADERS = {"User-Agent": "prisanalyse.no/1.0 (contact: post@prisanalyse.no)"}
_TIMEOUT = 10

_REGION_POINTS = {
    "south": [
        ("Kristiansand", 58.1467, 7.9956),
        ("Stavanger", 58.9690, 5.7331),
        ("Oslo", 59.9139, 10.7522),
        ("Skien", 59.2096, 9.6090),
    ],
    "mid": [
        ("Bergen", 60.3913, 5.3221),
        ("Ålesund", 62.4722, 6.1495),
        ("Trondheim", 63.4305, 10.3951),
        ("Røros", 62.5748, 11.3842),
    ],
    "north": [
        ("Bodø", 67.2804, 14.4049),
        ("Narvik", 68.4385, 17.4273),
        ("Tromsø", 69.6492, 18.9553),
        ("Vadsø", 70.0744, 29.7487),
    ],
}
_REGION_POINTS["all"] = _REGION_POINTS["south"] + _REGION_POINTS["mid"] + _REGION_POINTS["north"]


def _sanitize_mode(value: str) -> str:
    return value if value in _ALLOWED_MODES else "observed"


def _sanitize_period(value: str) -> str:
    return value if value in _ALLOWED_PERIODS else "hour"


def _sanitize_metric(value: str) -> str:
    return value if value in _ALLOWED_METRICS else "gust"


def _sanitize_region(value: str) -> str:
    return value if value in _ALLOWED_REGIONS else "all"


def _parse_target_datetime(date_str: Optional[str], hour_offset: int) -> datetime:
    base_date = _date.today()
    if date_str:
        try:
            base_date = _date.fromisoformat(date_str)
        except ValueError:
            pass
    return datetime.combine(base_date, time(0, 0), tzinfo=timezone.utc) + timedelta(hours=hour_offset)


def _fetch_location_forecast(lat: float, lon: float) -> dict:
    query = urlencode({"lat": lat, "lon": lon})
    req = Request(f"{_METNO_URL}?{query}", headers=_HEADERS)
    with urlopen(req, timeout=_TIMEOUT) as resp:
        if resp.status != 200:
            raise RuntimeError(f"MET API feilet med {resp.status}")
        return json.loads(resp.read().decode("utf-8"))


def _pick_timeseries(data: dict, target_dt: datetime) -> Optional[dict]:
    timeseries = data.get("properties", {}).get("timeseries", [])
    best = None
    best_diff = None
    for row in timeseries:
        ts = row.get("time")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        diff = abs((dt - target_dt).total_seconds())
        if best is None or diff < best_diff:
            best = row
            best_diff = diff
    return best


def _build_points(region: str, target_dt: datetime) -> tuple[list[dict], list[str]]:
    points: list[dict] = []
    errors: list[str] = []

    for name, lat, lon in _REGION_POINTS[region]:
        try:
            payload = _fetch_location_forecast(lat, lon)
            chosen = _pick_timeseries(payload, target_dt)
            if not chosen:
                errors.append(f"{name}: mangler timeserie")
                continue
            details = chosen.get("data", {}).get("instant", {}).get("details", {})
            points.append(
                {
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "time": chosen.get("time", "ukjent"),
                    "wind_speed": float(details.get("wind_speed") or 0.0),
                    "wind_gust": float(details.get("wind_speed_of_gust") or 0.0),
                    "wind_dir": float(details.get("wind_from_direction") or 0.0),
                }
            )
        except Exception as exc:
            errors.append(f"{name}: {exc}")

    return points, errors


def _metric_value(metric: str, point: dict) -> float:
    if metric == "avg":
        return point["wind_speed"]
    if metric == "peak":
        return max(point["wind_speed"], point["wind_gust"])
    return point["wind_gust"]


def build_wind_map_html(
    *,
    mode: str = "observed",
    period: str = "hour",
    metric: str = "gust",
    hour_offset: int = 0,
    date_str: Optional[str] = None,
    forecast_hours: int = 24,
    region: str = "all",
    bbox: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    show_heatmap: bool = True,
    top_n: int = 600,
) -> str:
    safe_mode = _sanitize_mode(mode)
    safe_period = _sanitize_period(period)
    safe_metric = _sanitize_metric(metric)
    safe_region = _sanitize_region(region)
    safe_hour = max(0, min(int(hour_offset or 0), 24))
    safe_forecast = max(6, min(int(forecast_hours or 24), 168))

    target_dt = _parse_target_datetime(date_str, safe_hour)
    points, errors = _build_points(safe_region, target_dt)

    for p in points:
        p["metric_value"] = round(_metric_value(safe_metric, p), 1)

    points_json = json.dumps(points, ensure_ascii=False)
    title_metric = {
        "avg": "Gjennomsnittsvind",
        "gust": "Maks vindkast",
        "peak": "Høyeste timesverdi",
    }[safe_metric]

    summary = (
        f"Kilde: {'Yr prognose' if safe_mode == 'forecast' else 'Yr/Frost-lignende visning'} · "
        f"Periode: {safe_period} · Horizon: {safe_forecast}t · Time offset: +{safe_hour}t"
    )

    err_html = ""
    if errors:
        err_text = "<br>".join(escape(e) for e in errors[:5])
        err_html = f"<div class='warn'>Noen steder feilet: {err_text}</div>"

    return f"""<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vindkart</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    body {{ margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background:#0b1220; color:#e5e7eb; }}
    .wrap {{ padding:14px; }}
    .card {{ border:1px solid #243041; border-radius:12px; background:#0f172a; padding:12px 14px; margin-bottom:10px; }}
    h2 {{ margin:0 0 4px; font-size:1.1rem; }}
    p {{ margin:0; color:#9fb0c7; font-size:.92rem; }}
    .warn {{ margin-top:8px; background:#2b1e1e; border:1px solid #593838; border-radius:8px; padding:8px; color:#fecaca; font-size:.88rem; }}
    #map {{ height: calc(100vh - 120px); min-height: 480px; border-radius:12px; border:1px solid #243041; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h2>Vindkart – {escape(title_metric)}</h2>
      <p>{escape(summary)}</p>
      {err_html}
    </div>
    <div id="map"></div>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const points = {points_json};
    const map = L.map('map', {{ zoomControl: true }}).setView([64.5, 15.0], 5);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const bounds = [];
    points.forEach(p => {{
      const val = Number(p.metric_value || 0);
      const color = val >= 20 ? '#dc2626' : val >= 12 ? '#f59e0b' : '#22c55e';
      const marker = L.circleMarker([p.lat, p.lon], {{
        radius: Math.max(6, Math.min(18, 5 + val / 2)),
        color,
        weight: 2,
        fillColor: color,
        fillOpacity: 0.65,
      }}).addTo(map);
      marker.bindPopup(
        `<b>${{p.name}}</b><br/>Tid: ${{p.time}}<br/>Valgt vind: ${{val}} m/s<br/>Snitt: ${{p.wind_speed}} m/s<br/>Kast: ${{p.wind_gust}} m/s` 
      );
      bounds.push([p.lat, p.lon]);
    }});

    if (bounds.length > 0) {{
      map.fitBounds(bounds, {{ padding: [20, 20] }});
    }}
  </script>
</body>
</html>
"""
