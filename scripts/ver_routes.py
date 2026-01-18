from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Blueprint, Response, jsonify, render_template, request

from snow_map import build_snow_map_html
from precip_map import build_precip_map_html
from sunshine_map import build_sunshine_map_html
from temp_map import build_min_temp_map_html

try:
    from zoneinfo import ZoneInfo  # py3.9+

    OSLO = ZoneInfo("Europe/Oslo")
except Exception:
    OSLO = timezone.utc


# ✅ Blueprint heter "ver" og URL-prefix er /ver
ver = Blueprint("ver", __name__, url_prefix="/ver")


# =========================
# SKILØYPER (Kvamskogen)
# =========================
# Dette er "loype backend"-en: kilden som faktisk server MVT-segmenter.
# Du trenger ikke konfigurere noe – vi kaller api.loyper.net direkte.
UPSTREAM_SEGMENTS = "https://api.loyper.net/segments/{z}/{x}/{y}"
_loyper_session = requests.Session()

# Kvamskogen (sentrum for sampling i stats)
KVAM_LAT = 60.37834747146485
KVAM_LNG = 5.979590206513535
KVAM_LOCATION_ID = "kvamskogen"


def _latlng_to_tile(lat: float, lng: float, z: int) -> Tuple[int, int]:
    n = 2**z
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def _parse_last_update(s: Any) -> Optional[datetime]:
    """Forventet format: 'YYYY-MM-DD HH:MM:SS' (tolkes som Europe/Oslo)."""
    if not s:
        return None
    try:
        local_dt = datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").replace(tzinfo=OSLO)
        return local_dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass
class _CacheEntry:
    expires_at: float
    payload: dict


_STATS_CACHE: Dict[tuple, _CacheEntry] = {}


# =========================
# HUB / MENY
# =========================
@ver.get("/")
def ver_hub() -> str:
    """Hub for vær-appene.

    Prøver å bruke eksisterende template (ver_analyse.html) hvis du har den.
    Hvis ikke, brukes en enkel fallback.
    """
    try:
        return render_template("ver_analyse.html")
    except Exception:
        return """
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Vær – prisanalyse.no</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background:#f5f7fb; }
      .page { max-width: 1100px; margin: 32px auto; padding: 0 16px; }
      h1 { margin: 0 0 14px; }
      .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 16px; }
      .card { background: white; border-radius: 18px; padding: 18px 20px; box-shadow: 0 18px 45px rgba(15,23,42,.08); }
      .card h2 { margin:0 0 6px; }
      .muted { color:#475569; margin: 0 0 12px; }
      .btn { display:inline-block; padding: 8px 14px; border-radius: 999px; background:#2563eb; color:#fff; text-decoration:none; font-weight:700; }
      @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <div class="page">
      <h1>Vær</h1>
      <div class="grid">
        <div class="card">
          <h2>Snømengde</h2>
          <p class="muted">Snødybde fra Frost. Zoom/pan og hent for utsnitt.</p>
          <a class="btn" href="/ver/sno">Åpne</a>
        </div>
        <div class="card">
          <h2>Nedbør</h2>
          <p class="muted">Siste 24 timer (rullerende) + dag / MTD / YTD.</p>
          <a class="btn" href="/ver/nedbor">Åpne</a>
        </div>
        <div class="card">
          <h2>Solskinn</h2>
          <p class="muted">Siste 24 timer (rullerende) + dag / MTD / YTD.</p>
          <a class="btn" href="/ver/solskinn">Åpne</a>
        </div>
        <div class="card">
          <h2>Min temperatur siste døgn</h2>
          <p class="muted">Velg fylke og se nyeste døgn-min (P1D) per stasjon.</p>
          <a class="btn" href="/ver/min-temp">Åpne</a>
        </div>
        <div class="card">
          <h2>Skiløyper – Kvamskogen</h2>
          <p class="muted">Sanntids løypestatus (preparering) med alder-farger.</p>
          <a class="btn" href="/ver/skiloyper-kvamskogen">Åpne</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""


# =========================
# SKILØYPER (Kvamskogen)
# =========================
@ver.get("/skiloyper-kvamskogen")
def skiloyper_kvamskogen_page():
    """Kart-side.

    Forventet plassering:
      templates/ver/loypekart_kvamskogen.html
    """
    # prøv først under ver/...
    try:
        return render_template("ver/loypekart_kvamskogen.html")
    except Exception:
        # fallback: hvis du la den rett i templates/
        return render_template("loypekart_kvamskogen.html")


@ver.get("/skiloyper-kvamskogen/tiles/segments/<int:z>/<int:x>/<int:y>.pbf")
def skiloyper_kvamskogen_tile(z: int, x: int, y: int):
    """Proxy for MVT tiles slik at alt går under prisanalyse.no (samme origin)."""
    url = UPSTREAM_SEGMENTS.format(z=z, x=x, y=y)
    r = _loyper_session.get(url, timeout=20)

    if r.status_code not in (200, 204):
        return Response("Upstream error", status=r.status_code)

    resp = Response(r.content, status=r.status_code, mimetype="application/x-protobuf")
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp


@ver.get("/skiloyper-kvamskogen/stats")
def skiloyper_kvamskogen_stats():
    """Stats til dashboardet.

    Denne ruten *kan* bruke `mapbox-vector-tile` for å dekode MVT og telle.
    For at Render-deploy ikke skal feile ved import, prøver vi å importere inne i funksjonen.

    Hvis du vil ha full stats: legg dette i requirements.txt:
      mapbox-vector-tile
    """

    # params
    z = int(request.args.get("z", 13))
    radius = int(request.args.get("radius", 2))
    fresh_hours = int(request.args.get("fresh_hours", 12))
    cache_seconds = int(request.args.get("cache_seconds", 60))

    z = max(0, min(19, z))
    radius = max(0, min(6, radius))
    fresh_hours = max(1, min(72, fresh_hours))
    cache_seconds = max(0, min(600, cache_seconds))

    cache_key = ("kvamskogen", z, radius, fresh_hours)
    now_ts = time.time()
    if cache_seconds > 0:
        hit = _STATS_CACHE.get(cache_key)
        if hit and hit.expires_at > now_ts:
            return jsonify(hit.payload)

    # optional dependency
    try:
        from mapbox_vector_tile import decode as mvt_decode  # type: ignore
    except Exception:
        payload = {
            "location_id": KVAM_LOCATION_ID,
            "fresh_hours": fresh_hours,
            "sample": {"z": z, "radius": radius, "tiles": (2 * radius + 1) ** 2},
            "counts": {
                "segments_total": None,
                "segments_active": None,
                "segments_freshly_groomed": None,
            },
            "updates": {"latest_update_utc": None, "latest_update_local": None, "newest_segment": None},
            "note": "Installer 'mapbox-vector-tile' i requirements.txt for å få full stats i dashboardet.",
        }
        return jsonify(payload)

    center_x, center_y = _latlng_to_tile(KVAM_LAT, KVAM_LNG, z)
    now_utc = datetime.now(timezone.utc)
    fresh_seconds = fresh_hours * 3600

    seen = set()  # (id, track_id)
    total = 0
    active = 0
    freshly_groomed = 0

    newest_dt_utc: Optional[datetime] = None
    newest_dt_local: Optional[datetime] = None
    newest_seg_id: Optional[Any] = None
    newest_track_id: Optional[Any] = None
    newest_age_seconds: Optional[float] = None

    for dx in range(-radius, radius + 1):
        for dy in range(-radius, radius + 1):
            x = center_x + dx
            y = center_y + dy

            url = UPSTREAM_SEGMENTS.format(z=z, x=x, y=y)
            r = _loyper_session.get(url, timeout=20)
            if r.status_code == 204:
                continue
            if r.status_code != 200:
                continue

            try:
                tile = mvt_decode(r.content)
            except Exception:
                continue

            layer = tile.get("segments")
            if not layer:
                continue
            for f in layer.get("features", []) or []:
                props: Dict[str, Any] = f.get("properties", {}) or {}
                if str(props.get("location_id", "")) != KVAM_LOCATION_ID:
                    continue

                seg_id = props.get("id")
                track_id = props.get("track_id")
                key = (seg_id, track_id)
                if key in seen:
                    continue
                seen.add(key)

                total += 1
                is_active = bool(props.get("is_active"))
                if is_active:
                    active += 1

                last_dt_utc = _parse_last_update(props.get("last_update"))
                if last_dt_utc:
                    if newest_dt_utc is None or last_dt_utc > newest_dt_utc:
                        newest_dt_utc = last_dt_utc
                        newest_dt_local = last_dt_utc.astimezone(OSLO)
                        newest_seg_id = seg_id
                        newest_track_id = track_id
                        newest_age_seconds = (now_utc - last_dt_utc).total_seconds()

                    # "nylig preparert": aktiv + ikke open_not_groomed + innen fresh_hours
                    if is_active and (not bool(props.get("open_not_groomed"))):
                        age = (now_utc - last_dt_utc).total_seconds()
                        if age <= fresh_seconds:
                            freshly_groomed += 1

    payload = {
        "location_id": KVAM_LOCATION_ID,
        "fresh_hours": fresh_hours,
        "sample": {"z": z, "radius": radius, "tiles": (2 * radius + 1) ** 2},
        "counts": {
            "segments_total": total,
            "segments_active": active,
            "segments_freshly_groomed": freshly_groomed,
        },
        "updates": {
            "latest_update_utc": newest_dt_utc.isoformat() if newest_dt_utc else None,
            "latest_update_local": newest_dt_local.isoformat() if newest_dt_local else None,
            "newest_segment": {
                "id": newest_seg_id,
                "track_id": newest_track_id,
                "age_seconds": newest_age_seconds,
            }
            if newest_dt_utc
            else None,
        },
    }

    if cache_seconds > 0:
        _STATS_CACHE[cache_key] = _CacheEntry(expires_at=now_ts + cache_seconds, payload=payload)
    return jsonify(payload)


# =========================
# MIN TEMP
# =========================
@ver.get("/min-temp")
def min_temp_index():
    # Hvis du har en template; hvis ikke kan du peke kortet direkte til /min-temp-kart
    try:
        return render_template("ver/min_temp_index.html")
    except Exception:
        return Response("Min temp: template mangler (ver/min_temp_index.html)", mimetype="text/plain")


@ver.get("/min-temp-kart")
def min_temp_map():
    county = request.args.get("county") or None
    temp = request.args.get("temp", "min")
    period = request.args.get("period", "last")
    date_str = request.args.get("date")
    month_str = request.args.get("month")
    year_str = request.args.get("year")

    html = build_min_temp_map_html(
        county=county,
        temp=temp,
        period=period,
        date_str=date_str,
        month_str=month_str,
        year_str=year_str,
        timeout=20,
        batch_size=80,
        limit=1000,
        qualities="0,1,2,3,4",
    )
    return Response(html, mimetype="text/html; charset=utf-8")


# =========================
# SNØ
# =========================
@ver.get("/sno")
def sno_index() -> str:
    # Enkel index: direkte til kart (du har trolig en penere template fra før)
    today_str = _date.today().isoformat()
    return f"""<!doctype html>
<html lang=\"no\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Snømengde</title></head>
<body style=\"margin:0\">
<iframe src=\"/ver/sno-kart?date={today_str}\" style=\"border:0;width:100vw;height:100vh\"></iframe>
</body></html>"""


@ver.get("/sno-kart")
def sno_kart() -> str:
    date_str = request.args.get("date")
    bbox = request.args.get("bbox")
    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")
    return build_snow_map_html(
        date_str=date_str,
        bbox=bbox,
        z=z,
        clat=clat,
        clon=clon,
    )


# =========================
# NEDBØR
# =========================
@ver.get("/nedbor")
def nedbor_index() -> str:
    today_str = _date.today().isoformat()
    return f"""<!doctype html>
<html lang=\"no\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Nedbør</title></head>
<body style=\"margin:0\">
<iframe src=\"/ver/nedbor-kart?mode=last24h&date={today_str}\" style=\"border:0;width:100vw;height:100vh\"></iframe>
</body></html>"""


@ver.get("/nedbor-kart")
def nedbor_kart() -> str:
    date_str = request.args.get("date")
    mode = request.args.get("mode", "last24h")
    bbox = request.args.get("bbox")
    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")
    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"
    return build_precip_map_html(
        date_str=date_str,
        mode=mode,  # type: ignore[arg-type]
        bbox=bbox,
        z=z,
        clat=clat,
        clon=clon,
    )


# =========================
# SOLSKINN
# =========================
@ver.get("/solskinn")
def solskinn_index() -> str:
    today_str = _date.today().isoformat()
    return f"""<!doctype html>
<html lang=\"no\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Solskinn</title></head>
<body style=\"margin:0\">
<iframe src=\"/ver/solskinn-kart?mode=last24h&date={today_str}\" style=\"border:0;width:100vw;height:100vh\"></iframe>
</body></html>"""


@ver.get("/solskinn-kart")
def solskinn_kart() -> str:
    date_str = request.args.get("date")
    mode = request.args.get("mode", "last24h")
    bbox = request.args.get("bbox")
    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")
    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"
    return build_sunshine_map_html(
        date_str=date_str,
        mode=mode,  # type: ignore[arg-type]
        bbox=bbox,
        z=z,
        clat=clat,
        clon=clon,
        show_heatmap=True,
    )
