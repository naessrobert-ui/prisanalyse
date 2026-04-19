from __future__ import annotations

import math
import os
import json
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests
from flask import Blueprint, request, render_template, Response, jsonify
from mapbox_vector_tile import decode as mvt_decode
from metno_locationforecast import Place

try:
    from zoneinfo import ZoneInfo  # py3.9+
    OSLO = ZoneInfo("Europe/Oslo")
except Exception:
    OSLO = timezone.utc  # fallback

from snow_map import build_snow_map_html
from precip_map import build_precip_county_map_html
from sunshine_map import build_sunshine_map_html
from temp_map import build_min_temp_map_html
from wind_map import build_wind_map_html
from ver_station_db import load_station_db
from yr_forecast import fetch_precip_forecast, fetch_temp_forecast
from sunshine_forecast import fetch_sunshine_forecast

# Snøprognose-logikk fra snow_increase.py
from snow_increase import (
    STASJONER,
    _env_auth,
    hent_intervaller,
    hent_snødybde_frost,
    simuler_snøprognose,
)

# Legg snow_increase.py i scripts/-mappen (eller samme mappe som ver_routes.py)

# ✅ Blueprint heter "ver" og URL-prefix er /ver
ver = Blueprint("ver", __name__, url_prefix="/ver")


# =========================
# SKILØYPER (Kvamskogen)
# =========================
UPSTREAM_SEGMENTS = "https://api.loyper.net/segments/{z}/{x}/{y}"
_loyper_session = requests.Session()

KVAM_LAT = 60.37834747146485
KVAM_LNG = 5.979590206513535
KVAM_LOCATION_ID = "kvamskogen"


def _latlng_to_tile(lat: float, lng: float, z: int) -> Tuple[int, int]:
    n = 2 ** z
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def _parse_last_update(s: Any) -> Optional[datetime]:
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
@ver.route("/")
def ver_hub() -> str:
    return """<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Vær og snø – prisanalyse.no</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#0a0f1e;color:#e2e8f0;font-family:system-ui,-apple-system,sans-serif;line-height:1.5;min-height:100vh;}

/* Nav */
.topnav{padding:14px 28px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #1e293b;}
.topnav-left{font-size:13px;color:#64748b;}
.topnav-left a{color:#64748b;text-decoration:none;}
.topnav-left a:hover{color:#e2e8f0;}
.topnav-brand{font-weight:800;font-size:15px;color:#e2e8f0;}
.topnav-brand span{color:#3b82f6;}
.topnav-dato{font-size:12px;color:#475569;}

/* Hero */
.hero{
  padding:52px 28px 44px;text-align:center;
  background:radial-gradient(ellipse 80% 55% at 50% 0%,rgba(59,130,246,.12) 0%,transparent 70%);
  border-bottom:1px solid #1e293b;
}
.hero-badge{
  display:inline-flex;align-items:center;gap:6px;
  background:rgba(59,130,246,.1);border:1px solid rgba(59,130,246,.22);
  border-radius:20px;padding:4px 14px;font-size:11px;font-weight:700;
  color:#60a5fa;margin-bottom:18px;letter-spacing:.05em;text-transform:uppercase;
}
.hero h1{font-size:clamp(26px,4.5vw,44px);font-weight:800;letter-spacing:-.4px;line-height:1.15;margin-bottom:12px;}
.hero h1 em{font-style:normal;color:#3b82f6;}
.hero-sub{font-size:15px;color:#64748b;max-width:500px;margin:0 auto;}

/* Stats */
.stats{display:flex;justify-content:center;border-bottom:1px solid #1e293b;flex-wrap:wrap;}
.stat{padding:16px 28px;text-align:center;border-right:1px solid #1e293b;}
.stat:last-child{border-right:none;}
.stat-val{font-size:20px;font-weight:800;color:#3b82f6;}
.stat-lbl{font-size:10px;color:#475569;text-transform:uppercase;letter-spacing:.06em;margin-top:2px;}

/* Main */
.main{max-width:1080px;margin:0 auto;padding:36px 20px 64px;}

/* Seksjoner */
.seksjon{margin-bottom:36px;}
.seksjon-tittel{
  font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;
  color:#475569;margin-bottom:14px;display:flex;align-items:center;gap:10px;
}
.seksjon-tittel::after{content:'';flex:1;height:1px;background:#1e293b;}

/* Kort-grid */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:14px;}
.grid-wide{grid-template-columns:repeat(auto-fill,minmax(340px,1fr));}

/* Kort */
.kort{
  background:#111827;border:1px solid #1e293b;border-radius:16px;
  padding:20px 22px;text-decoration:none;color:inherit;
  display:flex;flex-direction:column;gap:8px;
  transition:border-color .15s,background .15s,box-shadow .15s;
  cursor:pointer;
}
.kort:hover{border-color:#3b82f6;background:#141e33;box-shadow:0 0 0 1px rgba(59,130,246,.15),0 8px 24px rgba(0,0,0,.3);}
.kort-topp{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;}
.kort-ikon{font-size:26px;line-height:1;flex-shrink:0;}
.kort-badge{
  font-size:10px;font-weight:700;padding:3px 9px;border-radius:20px;
  text-transform:uppercase;letter-spacing:.05em;flex-shrink:0;
}
.b-blå{background:rgba(59,130,246,.15);color:#60a5fa;}
.b-grønn{background:rgba(34,197,94,.12);color:#4ade80;}
.b-gul{background:rgba(245,158,11,.12);color:#fbbf24;}
.b-rød{background:rgba(239,68,68,.12);color:#f87171;}
.b-lilla{background:rgba(139,92,246,.12);color:#a78bfa;}
.b-cyan{background:rgba(6,182,212,.12);color:#22d3ee;}
.b-hvit{background:rgba(148,163,184,.1);color:#94a3b8;}
.kort-tittel{font-size:15px;font-weight:700;color:#f1f5f9;margin-top:2px;}
.kort-tekst{font-size:12px;color:#64748b;line-height:1.6;flex:1;}
.kort-lenke{
  display:inline-flex;align-items:center;gap:5px;margin-top:4px;
  font-size:12px;font-weight:600;color:#3b82f6;
}
.kort-lenke::after{content:'→';}

/* Fremhevet kort (nyhet/anbefalt) */
.kort-featured{
  background:linear-gradient(135deg,#111e3a 0%,#0f1829 100%);
  border-color:rgba(59,130,246,.35);
}
.kort-featured:hover{border-color:#3b82f6;}

@media(max-width:600px){
  .hero{padding:36px 16px 30px;}
  .stat{padding:14px 18px;}
  .main{padding:24px 14px 48px;}
}
</style>
</head>
<body>

<nav class="topnav">
  <div class="topnav-left">
    <a href="/">prisanalyse.no</a> › Vær og snø
  </div>
  <div class="topnav-brand">pris<span>analyse</span>.no</div>
  <div class="topnav-dato" id="nav-dato"></div>
</nav>

<div class="hero">
  <div class="hero-badge">🌨️ Sanntidsdata fra Frost &amp; Yr</div>
  <h1>Norge i <em>sanntid</em> —<br>vær, snø og sol</h1>
  <div class="hero-sub">Snødybde, nedbør, solskinn og temperaturer fra hele landet — pluss skiturplanlegging for Kvamskogen og resten av Norge.</div>
</div>

<div class="stats">
  <div class="stat"><div class="stat-val">442</div><div class="stat-lbl">Snøstasjoner</div></div>
  <div class="stat"><div class="stat-val">Frost</div><div class="stat-lbl">Datakilde</div></div>
  <div class="stat"><div class="stat-val">Yr</div><div class="stat-lbl">Prognose</div></div>
  <div class="stat"><div class="stat-val">8 dager</div><div class="stat-lbl">Fremtidsutsikt</div></div>
</div>

<div class="main">

  <!-- Skitur og snø -->
  <div class="seksjon">
    <div class="seksjon-tittel">⛷️ Skitur og snøforhold</div>
    <div class="grid grid-wide">

      <a class="kort kort-featured" href="/snø/">
        <div class="kort-topp">
          <div class="kort-ikon">🏔️</div>
          <span class="kort-badge b-blå">Nytt</span>
        </div>
        <div class="kort-tittel">Snødashboard – Norge</div>
        <div class="kort-tekst">Finn de beste skiforholdene i hele Norge. Sanntids snødybde, prognose 8 dager frem og skituranbefaling for hver dag. Klikk «Finn snø nær meg» for å starte.</div>
        <div class="kort-lenke">Åpne dashboard</div>
      </a>

      <a class="kort kort-featured" href="/kvamskogen/">
        <div class="kort-topp">
          <div class="kort-ikon">🎿</div>
          <span class="kort-badge b-grønn">Kvamskogen</span>
        </div>
        <div class="kort-tittel">Kvamskogen – Snø og vær</div>
        <div class="kort-tekst">Detaljert snø- og værstatus for Kvamskogen. Historikk, prognose, skituranbefaling og løypestatus direkte fra Frost og Yr.</div>
        <div class="kort-lenke">Åpne Kvamskogen</div>
      </a>

      <a class="kort" href="/ver/varsel-kvamskogen">
        <div class="kort-topp">
          <div class="kort-ikon">📊</div>
          <span class="kort-badge b-lilla">Prognose</span>
        </div>
        <div class="kort-tittel">Snøprognoser</div>
        <div class="kort-tekst">Time for time prognose basert på Yr-varsel og observert snødybde. Velg blant alle norske snøstasjoner.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort" href="/ver/skiloyper-kvamskogen">
        <div class="kort-topp">
          <div class="kort-ikon">🗺️</div>
          <span class="kort-badge b-cyan">Kart</span>
        </div>
        <div class="kort-tittel">Skiløyper – Kvamskogen</div>
        <div class="kort-tekst">Sanntids løypestatus med alder-farger og egne markeringer. Se hvilke løyper som er preparert.</div>
        <div class="kort-lenke">Åpne kart</div>
      </a>

    </div>
  </div>

  <!-- Nedbør og temperatur -->
  <div class="seksjon">
    <div class="seksjon-tittel">🌡️ Nedbør og temperatur</div>
    <div class="grid">

      <a class="kort" href="/ver/sno">
        <div class="kort-topp">
          <div class="kort-ikon">❄️</div>
          <span class="kort-badge b-blå">Kart</span>
        </div>
        <div class="kort-tittel">Snømengde</div>
        <div class="kort-tekst">Snødybde fra Frost. Zoom og pan for å hente data for ønsket utsnitt av Norge.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort" href="/ver/nedbor">
        <div class="kort-topp">
          <div class="kort-ikon">🌧️</div>
          <span class="kort-badge b-grønn">24t</span>
        </div>
        <div class="kort-tittel">Nedbør</div>
        <div class="kort-tekst">Siste 24 timer (rullerende) pluss dag-, måneds- og årsakkumulering per stasjon.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort" href="/ver/solskinn">
        <div class="kort-topp">
          <div class="kort-ikon">☀️</div>
          <span class="kort-badge b-gul">Dagslys</span>
        </div>
        <div class="kort-tittel">Solskinn</div>
        <div class="kort-tekst">Soltimer siste 24 timer (rullerende) pluss dag-, måneds- og årsakkumulering.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort" href="/ver/min-temp">
        <div class="kort-topp">
          <div class="kort-ikon">🌡️</div>
          <span class="kort-badge b-rød">Temp</span>
        </div>
        <div class="kort-tittel">Min temperatur siste døgn</div>
        <div class="kort-tekst">Velg fylke og se nyeste døgn-minimum (P1D) per stasjon. Perfekt for å finne frostpunkter.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort" href="/ver/vind">
        <div class="kort-topp">
          <div class="kort-ikon">💨</div>
          <span class="kort-badge b-cyan">Vind</span>
        </div>
        <div class="kort-tittel">Vind og vindkast</div>
        <div class="kort-tekst">Se høyeste og gjennomsnittlig vind i kartet, eller bytt til forventet vind (Yr) for neste 24 timer.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort kort-featured" href="/ver/klima-toppliste">
        <div class="kort-topp">
          <div class="kort-ikon">🏆</div>
          <span class="kort-badge b-lilla">Auto</span>
        </div>
        <div class="kort-tittel">Klima-topplister</div>
        <div class="kort-tekst">Mest/minst sol, nedbør, vind og høy temperatur for i år, siste måned, siste døgn og forventet neste døgn/syv døgn.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

      <a class="kort kort-featured" href="/ver/aktivt-varsel">
        <div class="kort-topp">
          <div class="kort-ikon">🏃</div>
          <span class="kort-badge b-blå">Ny</span>
        </div>
        <div class="kort-tittel">Aktivitetsvarsel for alle steder</div>
        <div class="kort-tekst">Velg nærmeste punkt eller søk sted. Få detaljert 24t-varsel, kvalitetsvurdering og vinduer for fint vær de neste dagene.</div>
        <div class="kort-lenke">Åpne</div>
      </a>

    </div>
  </div>

</div>

<script>
(function(){
  const el = document.getElementById('nav-dato');
  if (el) {
    el.textContent = new Date().toLocaleDateString('no-NO',{weekday:'long',day:'numeric',month:'long'});
  }
})();
</script>
</body>
</html>"""


# =========================
# SNØPROGNOSE (Kvamskogen)
# =========================

def _vær_type(temp: float, nedbør: float) -> dict:
    """Enkel klassifisering basert på temp og nedbør."""
    if nedbør < 0.1:
        return {"icon": "☀️", "label": "Oppholdsvær"} if temp > 2 else {"icon": "❄️", "label": "Tørt og kaldt"}
    if temp <= -1.0:
        if nedbør >= 3.0:
            return {"icon": "🌨️", "label": "Kraftig snø"}
        return {"icon": "❄️", "label": "Snø"}
    if temp <= 1.5:
        return {"icon": "🌨️", "label": "Sludd/snø"}
    if nedbør >= 5.0:
        return {"icon": "🌧️", "label": "Kraftig regn"}
    return {"icon": "🌧️", "label": "Regn"}



def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _pick_first_value(obj: Any, *names: str) -> Any:
    """Finn første ikke-tomme felt, både direkte og ett nivå ned i vanlige beholdere."""
    if obj is None:
        return None

    containers = [obj]

    def _add_subcontainers(container: Any) -> None:
        for subname in ("details", "data", "instant", "values", "forecast", "weather", "properties"):
            sub = None
            if isinstance(container, dict):
                sub = container.get(subname)
            else:
                sub = getattr(container, subname, None)
            if sub is not None:
                containers.append(sub)

    _add_subcontainers(obj)
    # Ett ekstra nivå ned gir støtte for f.eks. {"instant": {"details": {...}}}
    for container in list(containers):
        _add_subcontainers(container)

    for container in containers:
        if isinstance(container, dict):
            for name in names:
                if name in container and not _is_missing_value(container[name]):
                    return container[name]
        else:
            for name in names:
                if hasattr(container, name):
                    value = getattr(container, name)
                    if not _is_missing_value(value):
                        return value
    return None


def _safe_float(value: Any) -> Optional[float]:
    if _is_missing_value(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_time_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        ts = value
    else:
        try:
            ts = pd.to_datetime(value, utc=True)
        except Exception:
            try:
                txt = str(value).strip()
                if txt.endswith("Z"):
                    txt = txt[:-1] + "+00:00"
                ts = pd.to_datetime(txt, utc=True)
            except Exception:
                return str(value)
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.isoformat()


def _build_raw_interval_lookup(intervaller: Any) -> dict[str, Any]:
    lookup: dict[str, Any] = {}
    for iv in intervaller or []:
        start_value = _pick_first_value(
            iv,
            "start", "from", "time", "referenceTime", "valid_from", "start_time",
            "fra", "tid",
        )
        key = _normalize_time_key(start_value)
        if key and key not in lookup:
            lookup[key] = iv
    return lookup


def _row_pick_first(rad: pd.Series, *names: str) -> Any:
    for name in names:
        if name in rad.index:
            value = rad.get(name)
            if not _is_missing_value(value):
                return value
    return None


def _hent_prognose_data(
    stasjon_navn: str,
    *,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    moh: Optional[float] = None,
    frost_id: Optional[str] = None,
) -> dict[str, Any]:
    """Henter og beregner snøprognose. Returnerer dict klar for JSON."""
    config = STASJONER.get(stasjon_navn)
    if not config:
        if lat is None or lon is None or not frost_id:
            return {
                "error": (
                    "Ukjent stasjon. Velg en foreslått snøstasjon, eller oppgi lat/lon + frost_id."
                ),
                "stasjoner": list(STASJONER.keys()),
            }
        place = (
            Place(stasjon_navn, float(lat), float(lon), int(moh))
            if (moh is not None and float(moh) > 0)
            else Place(stasjon_navn, float(lat), float(lon))
        )
        config = {
            "place": place,
            "frost_id": frost_id,
        }

    auth = _env_auth()
    session = requests.Session()

    # 1) Observert snødybde
    snødybde_cm = hent_snødybde_frost(config["frost_id"], session=session, auth=auth)
    if snødybde_cm is None:
        snødybde_cm = 0.0

    # 2) YR-varsel
    intervaller = hent_intervaller(config["place"])
    raw_interval_lookup = _build_raw_interval_lookup(intervaller)

    # 3) Simuler
    df = simuler_snøprognose(intervaller, snødybde_cm).copy()

    # 3b) Berik dataframe med vindkast og vindretning fra rå YR-intervaller
    if "vind_kast_ms" not in df.columns:
        df["vind_kast_ms"] = None
    if "vindretning_grader" not in df.columns:
        df["vindretning_grader"] = None

    for idx, rad in df.iterrows():
        gust = _safe_float(_row_pick_first(
            rad,
            "vind_kast_ms", "vindkast_ms", "vind_kast", "vindkast",
            "wind_speed_of_gust", "wind_speed_of_gust_ms", "wind_gust_ms",
            "wind_gust", "gust_ms", "gust",
        ))
        direction = _safe_float(_row_pick_first(
            rad,
            "vindretning_grader", "vindretning", "vindretning_deg",
            "wind_from_direction_deg", "wind_from_direction",
            "wind_direction_deg", "wind_direction", "direction_deg",
        ))

        if gust is None or direction is None:
            raw_iv = raw_interval_lookup.get(_normalize_time_key(rad.get("start")))
            if raw_iv is not None:
                if gust is None:
                    gust = _safe_float(_pick_first_value(
                        raw_iv,
                        "vind_kast_ms", "vindkast_ms", "vind_kast", "vindkast",
                        "wind_speed_of_gust", "wind_speed_of_gust_ms",
                        "wind_gust_ms", "wind_gust", "gust_ms", "gust",
                    ))
                if direction is None:
                    direction = _safe_float(_pick_first_value(
                        raw_iv,
                        "vindretning_grader", "vindretning", "vindretning_deg",
                        "wind_from_direction_deg", "wind_from_direction",
                        "wind_direction_deg", "wind_direction", "direction_deg",
                    ))

        if gust is not None:
            df.at[idx, "vind_kast_ms"] = gust
        if direction is not None:
            df.at[idx, "vindretning_grader"] = direction

    # 4) Bygg intervall-data med vær-type
    intervall_data = []
    for _, rad in df.iterrows():
        vær = _vær_type(rad["temperatur_c"], rad["nedbør_mm"])
        gust = _safe_float(_row_pick_first(
            rad,
            "vind_kast_ms", "vindkast_ms", "vind_kast", "vindkast",
            "wind_speed_of_gust", "wind_speed_of_gust_ms", "wind_gust_ms",
            "wind_gust", "gust_ms", "gust",
        ))
        direction = _safe_float(_row_pick_first(
            rad,
            "vindretning_grader", "vindretning", "vindretning_deg",
            "wind_from_direction_deg", "wind_from_direction",
            "wind_direction_deg", "wind_direction", "direction_deg",
        ))
        intervall_data.append({
            "start":        rad["start"].isoformat() if hasattr(rad["start"], "isoformat") else str(rad["start"]),
            "slutt":        rad["slutt"].isoformat() if hasattr(rad["slutt"], "isoformat") else str(rad["slutt"]),
            "timer":        rad["timer"],
            "temperatur_c": rad["temperatur_c"],
            "nedbør_mm":    rad["nedbør_mm"],
            "nedbør_min_mm": round(float(rad.get("nedbør_min_mm")), 1) if pd.notna(rad.get("nedbør_min_mm")) else None,
            "nedbør_maks_mm": round(float(rad.get("nedbør_maks_mm")), 1) if pd.notna(rad.get("nedbør_maks_mm")) else None,
            "nedbør_sannsynlighet_pct": int(round(float(rad.get("nedbør_sannsynlighet_pct")))) if pd.notna(rad.get("nedbør_sannsynlighet_pct")) else None,
            "ny_snø_mm":    rad["ny_snø_mm"],
            "smelting_mm":  rad["smelting_mm"],
            "netto_mm":     rad["netto_mm"],
            "snødybde_cm":  rad["snødybde_cm"],
            "snøfaktor":    rad["snøfaktor"],
            "vind_ms":      round(float(rad.get("vind_ms")), 1) if pd.notna(rad.get("vind_ms")) else None,
            "vind_kast_ms": round(float(gust), 1) if gust is not None else None,
            "vindretning_grader": int(round(float(direction))) if direction is not None else None,
            "vær_ikon":     vær["icon"],
            "vær_label":    vær["label"],
        })

    # 5) Daglig sammendrag
    df2 = df.copy()
    df2["dato"] = pd.to_datetime(df2["start"]).dt.date
    daglig = (
        df2.groupby("dato")
        .agg(
            min_temp_c=("temperatur_c", "min"),
            maks_temp_c=("temperatur_c", "max"),
            total_nedbør_mm=("nedbør_mm", "sum"),
            total_ny_snø_mm=("ny_snø_mm", "sum"),
            total_smelting_mm=("smelting_mm", "sum"),
            snødybde_slutt_cm=("snødybde_cm", "last"),
            vind_ms_snitt=("vind_ms", "mean"),
            vind_ms_maks=("vind_ms", "max"),
            vind_kast_ms_maks=("vind_kast_ms", "max"),
        )
        .round(1)
    )

    daglig_data = []
    for dato, rad in daglig.iterrows():
        dag_df = df2[df2["dato"] == dato]
        snø_timer = ((dag_df["temperatur_c"] <= 1.5) & (dag_df["nedbør_mm"] > 0.1)).sum()
        regn_timer = ((dag_df["temperatur_c"] > 1.5) & (dag_df["nedbør_mm"] > 0.1)).sum()
        if snø_timer > regn_timer and snø_timer > 0:
            vær = {"icon": "❄️", "label": "Snø"}
        elif regn_timer > 0:
            vær = {"icon": "🌧️", "label": "Regn"}
        else:
            vær = {"icon": "☀️", "label": "Oppholdsvær"} if rad["maks_temp_c"] > 0 else {"icon": "❄️", "label": "Kaldt"}

        daglig_data.append({
            "dato":              str(dato),
            "min_temp_c":        rad["min_temp_c"],
            "maks_temp_c":       rad["maks_temp_c"],
            "total_nedbør_mm":   rad["total_nedbør_mm"],
            "total_ny_snø_mm":   rad["total_ny_snø_mm"],
            "total_smelting_mm": rad["total_smelting_mm"],
            "snødybde_slutt_cm": rad["snødybde_slutt_cm"],
            "vind_ms_snitt":     rad["vind_ms_snitt"],
            "vind_ms_maks":      rad["vind_ms_maks"],
            "vind_kast_ms_maks": rad.get("vind_kast_ms_maks"),
            "vær_ikon":          vær["icon"],
            "vær_label":         vær["label"],
        })

    # 6) Nærprognose (neste 1t / 3t / 24t)
    def _snødybde_ved_timer(timer_frem: float) -> float:
        if df.empty:
            return float(snødybde_cm)

        kumulativ_tid = df["timer"].cumsum()
        treff = df[kumulativ_tid >= timer_frem]
        if treff.empty:
            return float(df["snødybde_cm"].iloc[-1])
        return float(treff.iloc[0]["snødybde_cm"])

    snø_1t = _snødybde_ved_timer(1.0)
    snø_3t = _snødybde_ved_timer(3.0)
    snø_24t = _snødybde_ved_timer(24.0)

    # 7) Sammendrag-tall (neste 48t)
    df_48 = df.head(48) if len(df) > 48 else df
    sammendrag = {
        "start_snødybde_cm": snødybde_cm,
        "slutt_snødybde_cm": float(df["snødybde_cm"].iloc[-1]) if not df.empty else snødybde_cm,
        "endring_cm":        round(float(df["snødybde_cm"].iloc[-1]) - snødybde_cm, 1) if not df.empty else 0,
        "snø_neste_time_cm": round(snø_1t, 1),
        "snø_neste_3t_cm":   round(snø_3t, 1),
        "snø_neste_døgn_cm": round(snø_24t, 1),
        "endring_neste_time_cm": round(snø_1t - snødybde_cm, 1),
        "endring_neste_3t_cm":   round(snø_3t - snødybde_cm, 1),
        "endring_neste_døgn_cm": round(snø_24t - snødybde_cm, 1),
        "total_nedbør_mm":   round(float(df_48["nedbør_mm"].sum()), 1),
        "total_ny_snø_mm":   round(float(df_48["ny_snø_mm"].sum()), 1),
        "total_ny_snø_cm":   round(float(df_48["ny_snø_mm"].sum()) / 10, 1),
        "total_smelting_mm": round(float(df_48["smelting_mm"].sum()), 1),
        "temperatur_nå_c":   round(float(df["temperatur_c"].iloc[0]), 1) if not df.empty else None,
        "min_temp_c":        round(float(df_48["temperatur_c"].min()), 1),
        "maks_temp_c":       round(float(df_48["temperatur_c"].max()), 1),
    }

    place = config["place"]
    return {
        "stasjon":     stasjon_navn,
        "lat":         place.coordinates["latitude"],
        "lon":         place.coordinates["longitude"],
        "moh":         place.coordinates["altitude"],
        "frost_id":    config["frost_id"],
        "hentet":      datetime.now().isoformat(timespec="seconds"),
        "sammendrag":  sammendrag,
        "intervaller": intervall_data,
        "daglig":      daglig_data,
    }


def _finn_snøstasjoner(query: str = "", limit: int = 30) -> list[dict[str, Any]]:
    """Søk i lokal Frost-stasjonsdatabase etter stasjoner som rapporterer snø."""
    items: list[dict[str, Any]] = []

    # 1) Håndplukkede standardvalg først
    for navn, conf in STASJONER.items():
        place = conf["place"]
        items.append({
            "label": navn,
            "frost_id": conf["frost_id"],
            "lat": place.coordinates["latitude"],
            "lon": place.coordinates["longitude"],
            "moh": place.coordinates["altitude"],
            "source": "preset",
        })

    # 2) Fyll på fra lokal stasjons-DB
    try:
        df = load_station_db()
        if not df.empty:
            d = df[df["has_snow"] == True].copy()
            if query:
                q = query.strip().lower()
                if q:
                    d = d[d["name"].astype(str).str.lower().str.contains(q, na=False)]
            d = d.head(limit)
            for _, rad in d.iterrows():
                frost_id = str(rad.get("baseId", "")).strip()
                if not frost_id:
                    continue
                items.append({
                    "label": str(rad.get("name") or frost_id),
                    "frost_id": frost_id,
                    "lat": float(rad.get("lat")) if pd.notna(rad.get("lat")) else None,
                    "lon": float(rad.get("lon")) if pd.notna(rad.get("lon")) else None,
                    "moh": None,
                    "source": "frost_db",
                })
    except Exception:
        pass

    # dedup på frost_id
    unike: dict[str, dict[str, Any]] = {}
    for it in items:
        fid = str(it.get("frost_id") or "")
        if fid and fid not in unike:
            unike[fid] = it

    out = list(unike.values())
    out.sort(key=lambda x: x["label"].lower())
    return out[:limit]


@ver.get("/api/snostasjoner")
def api_snostasjoner():
    q = request.args.get("q", "").strip()
    limit = min(max(int(request.args.get("limit", 30)), 1), 100)
    return jsonify({"items": _finn_snøstasjoner(query=q, limit=limit)})


@ver.get("/api/snovarsel")
def api_snovarsel():
    stasjon = request.args.get("stasjon", "Kvamskogen")
    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)
    moh = request.args.get("moh", type=float)
    frost_id = request.args.get("frost_id")
    try:
        data = _hent_prognose_data(stasjon, lat=lat, lon=lon, moh=moh, frost_id=frost_id)
        return jsonify(data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@ver.get("/varsel-kvamskogen")
def varsel_kvamskogen():
    return Response(_VARSEL_HTML, mimetype="text/html; charset=utf-8")


# HTML for snøvarsel-siden (self-contained)
_VARSEL_HTML = r'''<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Snøvarsel – Kvamskogen</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800&family=Playfair+Display:wght@700;900&display=swap" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#080e1f; --surface:rgba(15,23,48,.85); --surface2:rgba(22,34,62,.7);
  --border:rgba(100,130,200,.1); --ink:#dce4f5; --muted:#7b8db5;
  --accent:#5eead4; --accent2:#818cf8; --snow-c:#a5d8ff; --rain-c:#74c0fc;
  --warm-c:#ffa94d; --cold-c:#66d9e8; --neg-c:#ff6b6b; --pos-c:#69db7c;
  --serif:'Playfair Display',Georgia,serif; --sans:'Outfit',system-ui,sans-serif;
  --shadow:0 8px 40px rgba(0,0,0,.5);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.55;min-height:100vh;overflow-x:hidden;}
.page{position:relative;z-index:1;max-width:1060px;margin:0 auto;padding:20px 14px 50px}
.back{display:inline-block;color:var(--accent);text-decoration:none;font-size:13px;font-weight:600;margin-bottom:10px;opacity:.8}
.back:hover{opacity:1;text-decoration:underline}
.hero{text-align:center;padding:18px 0 8px}
.hero h1{font-family:var(--serif);font-weight:900;font-size:clamp(26px,5vw,40px);background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;letter-spacing:-.02em;}
.hero .sub{color:var(--muted);font-size:13px;margin-top:4px}
.loc-bar{display:flex;justify-content:center;gap:8px;flex-wrap:wrap;margin:14px 0 6px;align-items:center;}
.loc-bar select,.loc-bar input{background:var(--surface2);color:var(--ink);border:1px solid var(--border);border-radius:10px;padding:8px 12px;font-size:13px;font-family:var(--sans);}
.loc-bar button{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#0a0f1f;border:none;border-radius:10px;padding:8px 18px;font-weight:700;cursor:pointer;font-family:var(--sans);font-size:13px;}
.status{text-align:center;color:var(--muted);font-size:12px;min-height:18px;margin:4px 0 14px}
.status.err{color:var(--neg-c)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:18px;margin-bottom:14px;box-shadow:var(--shadow);backdrop-filter:blur(12px);}
.card h2{font-family:var(--serif);font-size:18px;font-weight:700;margin-bottom:12px;color:var(--snow-c);}
.sg{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:14px}
.sb{background:var(--surface2);border-radius:12px;padding:12px 8px;text-align:center;}
.sb .v{font-size:22px;font-weight:800;line-height:1.1}
.sb .l{font-size:10px;color:var(--muted);margin-top:4px;text-transform:uppercase;letter-spacing:.04em}
.sb .m{font-size:9px;color:var(--muted);margin-top:3px;opacity:.85}
.sb.snow .v{color:var(--snow-c)} .sb.rain .v{color:var(--rain-c)}
.sb.warm .v{color:var(--warm-c)} .sb.cold .v{color:var(--cold-c)}
.sb.pos .v{color:var(--pos-c)} .sb.neg .v{color:var(--neg-c)}
.cw{position:relative;height:220px}
.hs{overflow-x:auto;padding-bottom:8px}
.ht{display:flex;gap:0}
.hc{min-width:56px;padding:8px 4px;text-align:center;border-right:1px solid var(--border);position:relative;}
.hc.night{background:rgba(8,14,31,.4)}
.hc .t{font-size:11px;color:var(--muted);font-weight:700}
.hc .wi{font-size:20px;margin:4px 0}
.hc .tp{font-size:12px;font-weight:700}
.hc .pr{font-size:10px;color:var(--rain-c);min-height:14px}
.hc .sn{font-size:10px;color:var(--snow-c);min-height:14px}
.hc .sd{font-size:10px;color:var(--muted);margin-top:2px}
.hc .wd{font-size:10px;color:var(--accent);font-weight:700}
.day-sep{position:absolute;top:-18px;left:0;right:0;font-size:9px;color:var(--accent);font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.dg{display:grid;grid-template-columns:repeat(auto-fill,minmax(125px,1fr));gap:8px}
.dc{background:var(--surface2);border-radius:12px;padding:12px 10px;text-align:center}
.dc .dn{font-size:11px;color:var(--accent);font-weight:700;margin-bottom:4px}
.dc .di{font-size:24px;margin:4px 0}
.dc .dt{font-size:13px;font-weight:700} .dc .hi{color:var(--warm-c)} .dc .lo{color:var(--cold-c)}
.dc .dp{font-size:11px;color:var(--rain-c);min-height:16px}
.dc .ds{font-size:11px;color:var(--snow-c);min-height:16px}
.dc .dd{font-size:11px;color:var(--muted);margin-top:4px}
@keyframes spin{to{transform:rotate(360deg)}}
.spinner{width:28px;height:28px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;margin:0 auto}
@media(max-width:600px){.sg{grid-template-columns:repeat(2,1fr)}.dg{grid-template-columns:repeat(auto-fill,minmax(100px,1fr))}}
</style>
</head>
<body>
<div class="page">
  <a class="back" href="/ver/">← Tilbake</a>
  <div class="hero">
    <h1>❄️ Snøvarsel</h1>
    <p class="sub">Prognose basert på YR-varsel + observert snødybde fra <a href="https://frost.met.no" target="_blank">Frost</a></p>
  </div>
  <div class="loc-bar">
    <input id="stasjon-query" placeholder="Søk sted / stasjonsnavn"/>
    <button onclick="loadStations()">Søk</button>
    <select id="stasjon-sel"></select>
    <button onclick="load()">Hent prognose</button>
  </div>
  <div class="status" id="status"></div>
  <div id="cards" style="display:none">
    <div class="card" id="c-sum">
      <h2>📊 Sammendrag</h2>
      <div class="sg" id="sum-grid"></div>
    </div>
    <div class="card" id="c-snow">
      <h2>🏔 Snødybde-prognose</h2>
      <div class="cw"><canvas id="chart-snow"></canvas></div>
    </div>
    <div class="card" id="c-tw">
      <h2>🌡 Temperatur & nedbør</h2>
      <div class="cw"><canvas id="chart-tw"></canvas></div>
    </div>
    <div class="card" id="c-hourly">
      <h2>🕐 Time for time</h2>
      <div class="hs" id="hourly-scroll"></div>
    </div>
    <div class="card" id="c-daily">
      <h2>📅 Videre utvikling dag for dag</h2>
      <div class="dg" id="daily-grid"></div>
    </div>
  </div>
</div>
<script>
const MONTHS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
const DAYS=['søn','man','tir','ons','tor','fre','lør'];
const statusEl=document.getElementById('status');
const cardsEl=document.getElementById('cards');
let snowChart=null, twChart=null;

function fmtDH(s){const d=new Date(s);return d.getDate()+'.'+MONTHS[d.getMonth()]+' '+String(d.getHours()).padStart(2,'0')+'h'}
function fmtH(s){const d=new Date(s);return String(d.getHours()).padStart(2,'0')+':00'}
function fmtWeekdayHour(s){const d=new Date(s);return DAYS[d.getDay()]+' '+fmtH(s)}

async function loadStations(){
  const q=(document.getElementById('stasjon-query').value||'').trim();
  const sel=document.getElementById('stasjon-sel');
  try{
    const r=await fetch('/ver/api/snostasjoner?q='+encodeURIComponent(q));
    const data=await r.json();
    const items=data.items||[];
    if(!items.length){
      sel.innerHTML='<option value="">Ingen stasjoner funnet</option>';
      return;
    }
    const defaultIdx=Math.max(0, items.findIndex(it=>it.frost_id==='SN50310' || String(it.label||'').toLowerCase().includes('kvamskogen')));
    sel.innerHTML=items.map((it,idx)=>`<option value="${it.label}" data-frost="${it.frost_id}" data-lat="${it.lat ?? ''}" data-lon="${it.lon ?? ''}" data-moh="${it.moh ?? 0}" ${idx===defaultIdx?'selected':''}>${it.label} (${it.frost_id})</option>`).join('');
  }catch(e){
    sel.innerHTML='<option value="Kvamskogen" data-frost="SN50310" data-lat="60.3983" data-lon="5.9728" data-moh="500" selected>Kvamskogen (SN50310)</option>';
  }
}

async function load(){
  const sel=document.getElementById('stasjon-sel');
  const opt=sel.options[sel.selectedIndex];
  if(!opt){return;}
  const st=opt.value;
  const frostId=opt.dataset.frost || '';
  const lat=opt.dataset.lat || '';
  const lon=opt.dataset.lon || '';
  const moh=opt.dataset.moh || '';
  statusEl.innerHTML='<div class="spinner"></div>';
  statusEl.className='status';
  cardsEl.style.display='none';
  try{
    const qs=new URLSearchParams({stasjon:st,frost_id:frostId,lat,lon,moh}).toString();
    const r=await fetch('/ver/api/snovarsel?'+qs);
    const d=await r.json();
    if(d.error){throw new Error(d.error);}
    render(d);
    cardsEl.style.display='';
    statusEl.innerHTML='Hentet '+new Date(d.hentet).toLocaleString('no-NO');
  }catch(e){
    statusEl.textContent='Feil: '+e.message;
    statusEl.className='status err';
  }
}

function render(d){renderSummary(d);renderSnowChart(d);renderTempChart(d);renderHourly(d);renderDaily(d);}

function renderSummary(d){
  const s=d.sammendrag;
  const endring=s.endring_cm;
  const fmtDelta=(v)=>`${v>=0?'+':''}${v}`;
  const cls=(v)=>v>=0?'pos':'neg';
  const tempNow = s.temperatur_nå_c ?? 0;
  document.getElementById('sum-grid').innerHTML=`
    <div class="sb ${cls(s.endring_neste_time_cm)}"><div class="v">${fmtDelta(s.endring_neste_time_cm)} cm</div><div class="l">Endring neste time</div><div class="m">Snødybde: ${s.snø_neste_time_cm} cm</div></div>
    <div class="sb ${cls(s.endring_neste_3t_cm)}"><div class="v">${fmtDelta(s.endring_neste_3t_cm)} cm</div><div class="l">Endring neste 3 timer</div><div class="m">Snødybde: ${s.snø_neste_3t_cm} cm</div></div>
    <div class="sb ${cls(s.endring_neste_døgn_cm)}"><div class="v">${fmtDelta(s.endring_neste_døgn_cm)} cm</div><div class="l">Endring neste døgn</div><div class="m">Snødybde: ${s.snø_neste_døgn_cm} cm</div></div>
    <div class="sb snow"><div class="v">${s.start_snødybde_cm}</div><div class="l">Snødybde nå (cm)</div></div>
    <div class="sb ${endring>=0?'pos':'neg'}"><div class="v">${endring>=0?'+':''}${endring}</div><div class="l">Endring (cm)</div></div>
    <div class="sb snow"><div class="v">${s.slutt_snødybde_cm.toFixed(1)}</div><div class="l">Prognose slutt (cm)</div></div>
    <div class="sb rain"><div class="v">${s.total_nedbør_mm}</div><div class="l">Nedbør 48t (mm)</div></div>
    <div class="sb snow"><div class="v">${s.total_ny_snø_cm.toFixed(1)}</div><div class="l">Ny snø 48t (cm)</div></div>
    <div class="sb warm"><div class="v">${s.maks_temp_c}°</div><div class="l">Maks temp</div></div>
    <div class="sb ${tempNow<=0?'cold':'warm'}"><div class="v">${tempNow>0?'+':''}${tempNow}°</div><div class="l">Temperatur nå</div></div>
    <div class="sb cold"><div class="v">${s.min_temp_c}°</div><div class="l">Min temp</div></div>
    <div class="sb neg"><div class="v">${s.total_smelting_mm}</div><div class="l">Smelting 48t (mm)</div></div>`;
}

function renderSnowChart(d){
  const iv=d.intervaller;
  const ctx=document.getElementById('chart-snow').getContext('2d');
  if(snowChart) snowChart.destroy();
  snowChart=new Chart(ctx,{type:'line',data:{labels:iv.map(x=>fmtDH(x.start)),datasets:[
    {label:'Snødybde (cm)',data:iv.map(x=>x.snødybde_cm),borderColor:'#a5d8ff',backgroundColor:'rgba(165,216,255,.1)',fill:true,tension:.3,borderWidth:2.5,pointRadius:0,pointHoverRadius:4,yAxisID:'y'},
    {label:'Netto snø/smelting (mm)',data:iv.map(x=>x.netto_mm),type:'bar',backgroundColor:iv.map(x=>x.netto_mm>=0?'rgba(116,192,252,.55)':'rgba(255,107,107,.45)'),borderRadius:2,yAxisID:'y1'}
  ]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
    plugins:{legend:{labels:{color:'#7b8db5',font:{family:'Outfit',size:11}}},tooltip:{backgroundColor:'rgba(15,23,48,.92)',titleFont:{family:'Outfit'},bodyFont:{family:'Outfit'}}},
    scales:{x:{ticks:{color:'#7b8db5',font:{size:9,family:'Outfit'},maxRotation:45,autoSkip:true,maxTicksLimit:18},grid:{color:'rgba(100,130,200,.08)'}},
      y:{position:'left',title:{display:true,text:'Snødybde (cm)',color:'#a5d8ff',font:{family:'Outfit',size:11}},ticks:{color:'#a5d8ff',font:{size:10,family:'Outfit'}},grid:{color:'rgba(100,130,200,.06)'}},
      y1:{position:'right',title:{display:true,text:'Netto mm',color:'#7b8db5',font:{family:'Outfit',size:11}},ticks:{color:'#7b8db5',font:{size:10,family:'Outfit'}},grid:{drawOnChartArea:false}}}}});
}

function renderTempChart(d){
  const iv=d.intervaller;
  const ctx=document.getElementById('chart-tw').getContext('2d');
  if(twChart) twChart.destroy();
  twChart=new Chart(ctx,{type:'line',data:{labels:iv.map(x=>fmtDH(x.start)),datasets:[
    {label:'Temperatur (°C)',data:iv.map(x=>x.temperatur_c),borderColor:'#4dabf7',segment:{borderColor:(ctx)=>ctx.p0.parsed.y<=0&&ctx.p1.parsed.y<=0?'#4dabf7':'#ff6b6b'},backgroundColor:'transparent',tension:.3,borderWidth:2,pointRadius:0,pointHoverRadius:4,yAxisID:'y'},
    {label:'Nedbør (mm)',data:iv.map(x=>x.nedbør_mm),type:'bar',backgroundColor:iv.map(x=>x.temperatur_c<=0?'rgba(116,192,252,.55)':'rgba(255,107,107,.5)'),borderRadius:2,yAxisID:'y1'}
  ]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
    plugins:{legend:{labels:{color:'#7b8db5',font:{family:'Outfit',size:11}}},tooltip:{backgroundColor:'rgba(15,23,48,.92)',titleFont:{family:'Outfit'},bodyFont:{family:'Outfit'}}},
    scales:{x:{ticks:{color:'#7b8db5',font:{size:9,family:'Outfit'},maxRotation:45,autoSkip:true,maxTicksLimit:18},grid:{color:'rgba(100,130,200,.08)'}},
      y:{position:'left',title:{display:true,text:'°C',color:'#7dd3fc',font:{family:'Outfit',size:11}},ticks:{color:'#7dd3fc',font:{size:10,family:'Outfit'}},grid:{color:'rgba(100,130,200,.06)'}},
      y1:{position:'right',title:{display:true,text:'Nedbør (mm)',color:'#74c0fc',font:{family:'Outfit',size:11}},ticks:{color:'#74c0fc',font:{size:10,family:'Outfit'}},grid:{drawOnChartArea:false},min:0}}}});
}

function renderHourly(d){
  const el=document.getElementById('hourly-scroll');
  let html='<div class="ht">';
  let prevDate='';
  for(const iv of d.intervaller){
    if(iv.timer>1) continue;
    const dt=new Date(iv.start);
    const h=dt.getHours();
    const isNight=h<7||h>=22;
    const dateStr=dt.getDate()+'. '+MONTHS[dt.getMonth()];
    let daySep='';
    if(dateStr!==prevDate){daySep=`<div class="day-sep">${DAYS[dt.getDay()]} ${dateStr}</div>`;prevDate=dateStr;}
    html+=`<div class="hc${isNight?' night':''}">
      ${daySep}
      <div class="wd">${fmtWeekdayHour(iv.start)}</div>
      <div class="wi">${iv.vær_ikon}</div>
      <div class="tp" style="color:${iv.temperatur_c<=0?'var(--cold-c)':'var(--warm-c)'}">${iv.temperatur_c>0?'+':''}${iv.temperatur_c}°</div>
      <div class="pr">${iv.nedbør_mm>0?iv.nedbør_mm+'mm':''}</div>
      <div class="sn">${iv.ny_snø_mm>0?'❄ '+(iv.ny_snø_mm/10).toFixed(1)+'cm':''}</div>
      <div class="pr">${iv.vind_ms!==null?'💨 '+iv.vind_ms+' m/s':''}</div>
      <div class="sd">${iv.snødybde_cm}cm</div>
    </div>`;}
  el.innerHTML=html+'</div>';
}

function renderDaily(d){
  let html='';
  for(const dag of d.daglig){
    const dt=new Date(dag.dato+'T12:00:00');
    const dayName=DAYS[dt.getDay()]+' '+dt.getDate()+'.'+MONTHS[dt.getMonth()];
    html+=`<div class="dc">
      <div class="dn">${dayName}</div>
      <div class="di">${dag.vær_ikon}</div>
      <div class="dt"><span class="hi">${dag.maks_temp_c>0?'+':''}${dag.maks_temp_c}°</span> / <span class="lo">${dag.min_temp_c}°</span></div>
      <div class="dp">${dag.total_nedbør_mm>0?'💧 '+dag.total_nedbør_mm+'mm':''}</div>
      <div class="ds">${dag.total_ny_snø_mm>0?'❄ +'+(dag.total_ny_snø_mm/10).toFixed(1)+'cm':''}</div>
      <div class="dp">${dag.vind_ms_snitt!==null?'💨 '+dag.vind_ms_snitt+' m/s':''}</div>
      <div class="dd">Snø: ${dag.snødybde_slutt_cm}cm</div>
    </div>`;}
  document.getElementById('daily-grid').innerHTML=html;
}

// Last automatisk ved oppstart
loadStations().then(()=>load());
</script>
</body>
</html>
'''


# =========================
# SKILØYPER
# =========================
@ver.get("/skiloyper-kvamskogen")
def skiloyper_kvamskogen_index():
    return render_template("ver/loypekart_kvamskogen.html")


@ver.get("/skiloyper-kvamskogen/tiles/segments/<int:z>/<int:x>/<int:y>.pbf")
def skiloyper_kvamskogen_tile(z: int, x: int, y: int):
    url = UPSTREAM_SEGMENTS.format(z=z, x=x, y=y)
    r = _loyper_session.get(url, timeout=15)
    status = r.status_code
    if status not in (200, 204):
        return Response("Upstream error", status=status)
    resp = Response(r.content, status=status, mimetype="application/x-protobuf")
    resp.headers["Cache-Control"] = "public, max-age=60"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


def fetch_loyper_stats(
    z: int = 13,
    radius: int = 2,
    fresh_hours: int = 12,
    cache_seconds: int = 60,
) -> dict:
    """
    Kjernelogikk for løypestatus – ingen Flask-avhengighet.
    Kan kalles direkte fra andre moduler (f.eks. kvamskogen_routes.py)
    uten å være inne i en request-kontekst.
    """
    z = max(0, min(19, z)); radius = max(0, min(6, radius))
    fresh_hours = max(1, min(72, fresh_hours)); cache_seconds = max(0, min(600, cache_seconds))

    cache_key = ("kvamskogen", z, radius, fresh_hours)
    now_ts = time.time()
    if cache_seconds > 0:
        hit = _STATS_CACHE.get(cache_key)
        if hit and hit.expires_at > now_ts:
            return hit.payload

    center_x, center_y = _latlng_to_tile(KVAM_LAT, KVAM_LNG, z)
    now_utc = datetime.now(timezone.utc)
    fresh_seconds = fresh_hours * 3600
    seen = set()
    total = active = freshly_groomed = with_last_update = missing_last_update = 0
    newest_dt_utc = newest_dt_local = newest_seg_id = newest_track_id = newest_age_seconds = None

    for dx in range(-radius, radius + 1):
        for dy in range(-radius, radius + 1):
            url = UPSTREAM_SEGMENTS.format(z=z, x=center_x+dx, y=center_y+dy)
            r = _loyper_session.get(url, timeout=15)
            if r.status_code not in (200,): continue
            try: tile = mvt_decode(r.content)
            except Exception: continue
            layer = tile.get("segments")
            if not layer: continue
            for f in layer.get("features", []):
                props = f.get("properties", {}) or {}
                if str(props.get("location_id", "")) != KVAM_LOCATION_ID: continue
                key = (props.get("id"), props.get("track_id"))
                if key in seen: continue
                seen.add(key); total += 1
                is_active = bool(props.get("is_active"))
                if is_active: active += 1
                last_dt_utc = _parse_last_update(props.get("last_update"))
                if last_dt_utc:
                    with_last_update += 1
                    if newest_dt_utc is None or last_dt_utc > newest_dt_utc:
                        newest_dt_utc = last_dt_utc
                        newest_dt_local = last_dt_utc.astimezone(OSLO)
                        newest_seg_id = props.get("id")
                        newest_track_id = props.get("track_id")
                        newest_age_seconds = (now_utc - last_dt_utc).total_seconds()
                    if is_active and not bool(props.get("open_not_groomed")):
                        if (now_utc - last_dt_utc).total_seconds() <= fresh_seconds:
                            freshly_groomed += 1
                else:
                    missing_last_update += 1

    payload = {
        "location_id": KVAM_LOCATION_ID, "fresh_hours": fresh_hours,
        "sample": {"z": z, "radius": radius, "tiles": (2*radius+1)**2},
        "counts": {"segments_total": total, "segments_active": active, "segments_freshly_groomed": freshly_groomed},
        "updates": {
            "latest_update_utc": newest_dt_utc.isoformat() if newest_dt_utc else None,
            "latest_update_local": newest_dt_local.isoformat() if newest_dt_local else None,
            "newest_segment": {"id": newest_seg_id, "track_id": newest_track_id, "age_seconds": newest_age_seconds},
            "coverage": {"with_last_update": with_last_update, "missing_last_update": missing_last_update},
        },
    }
    if cache_seconds > 0:
        _STATS_CACHE[cache_key] = _CacheEntry(expires_at=now_ts + cache_seconds, payload=payload)
    return payload


@ver.get("/skiloyper-kvamskogen/stats")
def skiloyper_kvamskogen_stats():
    z            = int(request.args.get("z", 13))
    radius       = int(request.args.get("radius", 2))
    fresh_hours  = int(request.args.get("fresh_hours", 12))
    cache_seconds = int(request.args.get("cache_seconds", 60))
    return jsonify(fetch_loyper_stats(z, radius, fresh_hours, cache_seconds))


# =========================
# MIN TEMP
# =========================
@ver.get("/min-temp")
def min_temp_index():
    return render_template("ver/min_temp_index.html")


@ver.get("/min-temp-kart")
def min_temp_map():
    html = build_min_temp_map_html(
        county=request.args.get("county") or None,
        temp=request.args.get("temp", "min"),
        period=request.args.get("period", "last"),
        date_str=request.args.get("date"),
        month_str=request.args.get("month"),
        year_str=request.args.get("year"),
        timeout=20, batch_size=80, limit=1000, qualities="0,1,2,3,4",
    )
    return Response(html, mimetype="text/html; charset=utf-8")


# =========================
# SNØ
# =========================
@ver.route("/sno")
def sno_index() -> str:
    today_str = _date.today().isoformat()
    default_bbox = "57.0,4.0,62.5,12.5"
    default_z = "5"; default_clat = "60.5"; default_clon = "8.5"
    return f"""<!doctype html>
<html lang="no"><head><meta charset="utf-8"/><title>Snømengde i Norge</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
:root{{--bg:#f5f7fb;--card:#fff;--ink:#0f172a;--muted:#475569;--border:#e2e8f0;--shadow:0 18px 45px rgba(15,23,42,.10);--blue:#2563eb;--blue2:#1d4ed8;--pill:#eef2ff;}}
body{{margin:0;font-family:system-ui,sans-serif;background:var(--bg);color:var(--ink);}}
.page{{max-width:1240px;margin:28px auto;padding:0 16px 28px;}}
.card{{background:var(--card);border-radius:18px;padding:16px 18px;box-shadow:var(--shadow);border:1px solid rgba(226,232,240,.8);margin-bottom:14px;}}
.hdr{{display:flex;gap:12px;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;}}
h1{{margin:0;font-size:22px;}} .sub{{margin:6px 0 0;color:var(--muted);font-size:13px;}}
.controls{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;}}
.field{{display:flex;flex-direction:column;gap:6px;min-width:180px;}}
.field label{{font-size:12px;color:var(--muted);font-weight:700;}}
select,input[type="date"]{{padding:9px 10px;border-radius:12px;border:1px solid var(--border);background:white;outline:none;font-size:14px;}}
.seg{{display:flex;gap:6px;padding:6px;border:1px solid var(--border);border-radius:14px;background:#fff;}}
.seg button{{border:none;border-radius:12px;padding:8px 12px;font-weight:800;cursor:pointer;background:transparent;color:var(--muted);}}
.seg button.active{{background:var(--pill);color:var(--blue2);}}
.actions{{display:flex;gap:10px;align-items:center;margin-left:auto;}}
.btn{{padding:10px 14px;border-radius:999px;border:none;background:var(--blue);color:white;font-weight:900;cursor:pointer;}}
.ghost{{padding:10px 14px;border-radius:999px;border:1px solid var(--border);background:white;color:var(--ink);font-weight:900;cursor:pointer;}}
.toggle{{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:14px;border:1px solid var(--border);background:#fff;}}
.toggle .tlabel{{font-weight:900;}}
.switch{{position:relative;width:44px;height:26px;background:#e2e8f0;border-radius:999px;cursor:pointer;flex:0 0 auto;}}
.switch::after{{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;background:white;border-radius:999px;box-shadow:0 8px 16px rgba(15,23,42,.15);transition:transform .18s ease;}}
.switch.on{{background:rgba(22,163,74,.25);}} .switch.on::after{{transform:translateX(18px);background:#16a34a;}}
.panel{{margin-top:10px;padding:12px;border-radius:16px;border:1px solid var(--border);display:none;}}
.panel.show{{display:block;}} .panel .row{{display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap;}}
.hint{{font-size:12px;color:var(--muted);margin:8px 0 0;}}
#map-frame{{width:100%;height:80vh;border:none;border-radius:18px;background:#e5e7eb;box-shadow:var(--shadow);}}
</style></head><body>
<div class="page">
  <div class="card">
    <div class="hdr">
      <div><h1>Snømengde</h1><p class="sub">Standard er <b>siste oppdaterte snødybde</b>. Slå på <b>Endring</b> for "nå minus baseline".</p></div>
      <div class="actions">
        <div class="seg"><button type="button" id="tab-latest" class="active">Latest</button><button type="button" id="tab-day">Dato</button></div>
        <button type="button" id="btn-reset" class="ghost">Reset utsnitt</button>
        <button type="button" id="btn-go" class="btn">Vis</button>
      </div>
    </div>
    <div class="controls">
      <div class="field"><label>Region</label>
        <select id="region-select"><option value="south" selected>Sør</option><option value="mid">Midt</option><option value="north">Nord</option><option value="all">Hele landet</option></select>
      </div>
      <div class="field"><label>Dato (kun Dato-modus)</label><input type="date" id="date-input" value="{today_str}" max="{today_str}"></div>
      <div class="toggle"><div class="switch" id="change-switch" role="switch" aria-checked="false" tabindex="0"></div>
        <div><div class="tlabel">Endring</div><div style="font-size:12px;color:var(--muted);font-weight:700;">Nå minus baseline</div></div>
      </div>
    </div>
    <div class="panel" id="change-panel">
      <div class="row">
        <div class="field"><label>Baseline</label>
          <select id="since-select"><option value="døgn" selected>Siste døgn</option><option value="3døgn">Siste 3 døgn</option><option value="year">I år</option><option value="date">Valgt dato</option></select>
        </div>
        <div class="field" id="since-date-field" style="display:none;"><label>Siden dato</label><input type="date" id="since-date" value="{today_str}" max="{today_str}"></div>
      </div>
      <p class="hint">Tips: Endring beregnes kun for stasjoner i nåværende utsnitt.</p>
    </div>
  </div>
  <iframe id="map-frame" src="/ver/snomengde-kart?mode=latest&region=south&bbox={default_bbox}&z={default_z}&clat={default_clat}&clon={default_clon}" loading="lazy"></iframe>
</div>
<script>
const STORE_KEY="snow_view_v2";
const frame=document.getElementById("map-frame");
const tabLatest=document.getElementById("tab-latest"),tabDay=document.getElementById("tab-day");
const regionSelect=document.getElementById("region-select"),dateInput=document.getElementById("date-input");
const changeSwitch=document.getElementById("change-switch"),changePanel=document.getElementById("change-panel");
const sinceSelect=document.getElementById("since-select"),sinceDateField=document.getElementById("since-date-field"),sinceDate=document.getElementById("since-date");
let mode="latest",changeOn=false;
const RDEFS={{south:{{bbox:"57.0,4.0,62.5,12.5",z:"5",clat:"60.5",clon:"8.5"}},mid:{{bbox:"62.0,4.0,66.7,16.5",z:"5",clat:"64.5",clon:"10.5"}},north:{{bbox:"66.3,10.0,71.5,31.5",z:"4",clat:"68.8",clon:"19.0"}},all:{{bbox:"57.0,4.0,71.5,31.5",z:"4",clat:"64.0",clon:"14.0"}}}};
function readSaved(){{try{{const r=sessionStorage.getItem(STORE_KEY);return r?JSON.parse(r):null;}}catch(e){{return null;}}}}
function saveFU(){{try{{const u=new URL(frame.contentWindow.location.href);const b=u.searchParams.get("bbox");if(!b)return;sessionStorage.setItem(STORE_KEY,JSON.stringify({{bbox:b,z:u.searchParams.get("z")||"",clat:u.searchParams.get("clat")||"",clon:u.searchParams.get("clon")||""}}));}}catch(e){{}}}}
frame.addEventListener("load",saveFU);
function setMode(m){{mode=m;tabLatest.classList.toggle("active",m==="latest");tabDay.classList.toggle("active",m==="day");dateInput.disabled=m!=="day";dateInput.style.opacity=m==="day"?"1":"0.55";}}
function setChange(on){{changeOn=on;changeSwitch.classList.toggle("on",on);changeSwitch.setAttribute("aria-checked",on?"true":"false");changePanel.classList.toggle("show",on);}}
sinceSelect.addEventListener("change",()=>{{sinceDateField.style.display=sinceSelect.value==="date"?"block":"none";}});
function buildUrl(reset=false){{
  const reg=regionSelect.value||"south",d=dateInput.value||"{today_str}";
  const qs=new URLSearchParams();qs.set("region",reg);
  if(changeOn){{qs.set("mode","latest");qs.set("change","1");qs.set("since",sinceSelect.value==="date"?(sinceDate.value||"{today_str}"):sinceSelect.value);}}
  else{{qs.set("mode",mode);if(mode==="day")qs.set("date",d);}}
  const def=RDEFS[reg]||RDEFS.south;
  if(reset){{qs.set("bbox",def.bbox);qs.set("z",def.z);qs.set("clat",def.clat);qs.set("clon",def.clon);}}
  else{{const s=readSaved();if(s){{if(s.bbox)qs.set("bbox",s.bbox);if(s.z)qs.set("z",s.z);if(s.clat)qs.set("clat",s.clat);if(s.clon)qs.set("clon",s.clon);}}else{{qs.set("bbox",def.bbox);qs.set("z",def.z);qs.set("clat",def.clat);qs.set("clon",def.clon);}}}}
  return "/ver/snomengde-kart?"+qs.toString();
}}
function go(r=false){{frame.src=buildUrl(r);}}
tabLatest.addEventListener("click",()=>{{setMode("latest");go();}});
tabDay.addEventListener("click",()=>{{setMode("day");go();}});
document.getElementById("btn-go").addEventListener("click",()=>go());
document.getElementById("btn-reset").addEventListener("click",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}go(true);}});
regionSelect.addEventListener("change",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}go(true);}});
changeSwitch.addEventListener("click",()=>{{setChange(!changeOn);go();}});
changeSwitch.addEventListener("keydown",(e)=>{{if(e.key==="Enter"||e.key===" "){{e.preventDefault();setChange(!changeOn);go();}}}});
setMode("latest");setChange(false);
</script></body></html>"""


@ver.route("/snomengde-kart")
def snomengde_kart():
    mode = request.args.get("mode", "latest")
    if mode not in {"latest", "day"}: mode = "latest"
    html = build_snow_map_html(
        date_str=request.args.get("date"), mode=mode,
        bbox=request.args.get("bbox"), region=request.args.get("region"),
        z=request.args.get("z"), clat=request.args.get("clat"), clon=request.args.get("clon"),
        show_heatmap=True,
        change=(request.args.get("change","") or "").strip().lower() in {"1","true","yes","on"},
        since=request.args.get("since") or "",
        timeout=20, qualities="0,1,2,3,4", window_days=2,
    )
    return Response(html, mimetype="text/html; charset=utf-8")


# =========================
# NEDBØR
# =========================
@ver.route("/nedbor")
def nedbor_index():
    return Response(build_precip_county_map_html(county=None, mode="last24h", top_n=50, rank="max"), mimetype="text/html; charset=utf-8")


@ver.route("/nedbor-kart")
def nedbor_kart() -> Response:
    mode = request.args.get("mode", "last24h")
    if mode not in {"last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"}: mode = "last24h"
    rank = request.args.get("rank", "max")
    if rank not in {"max", "min"}: rank = "max"
    return Response(build_precip_county_map_html(
        county=request.args.get("county") or None, mode=mode,
        date_str=request.args.get("date"), top_n=request.args.get("top", "50"), rank=rank,
        timeout=20, batch_size=80, limit=1000, qualities="0,1,2,3,4", show_heatmap=True,
    ), mimetype="text/html; charset=utf-8")


# =========================
# SOLSKINN
# =========================
@ver.route("/solskinn")
def solskinn_index() -> str:
    today_str = _date.today().isoformat()
    return f"""<!doctype html>
<html lang="no"><head><meta charset="utf-8"/><title>Solskinn i Norge</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
body{{margin:0;font-family:system-ui,sans-serif;background:#f5f7fb;}}
.page{{max-width:1200px;margin:32px auto;padding:0 16px 32px;}}
.card{{background:white;border-radius:16px;padding:18px 22px;box-shadow:0 18px 45px rgba(15,23,42,.08);margin-bottom:16px;}}
.card h1{{margin:0 0 8px;}}
.controls{{display:flex;flex-wrap:wrap;gap:10px;margin-top:10px;align-items:center;}}
.controls input,.controls select{{padding:6px 10px;border-radius:10px;border:1px solid #d1d5db;}}
.controls button{{padding:7px 14px;border-radius:999px;border:none;background:#2563eb;color:white;cursor:pointer;}}
#map-frame{{width:100%;height:80vh;border:none;border-radius:16px;box-shadow:0 18px 45px rgba(15,23,42,.10);background:#e5e7eb;}}
</style></head><body>
<div class="page">
  <div class="card">
    <h1>Solskinn i Norge</h1>
    <p style="margin:0;color:#475569;">Standard er rullerende siste 24 timer.</p>
    <form id="controls-form" class="controls">
      <label>Dato:</label><input type="date" id="date-input" value="{today_str}" max="{today_str}">
      <label>Periode:</label>
      <select id="mode-select"><option value="last24h" selected>Siste 24 timer</option><option value="day">Kalenderdøgn</option><option value="mtd">Hittil i måneden</option><option value="ytd">Hittil i året</option><option disabled>── Prognose (Yr) ──</option><option value="next24h">Neste 24 timer</option><option value="next48h">Neste 48 timer</option><option value="next7d">Neste 7 dager</option></select>
      <button type="submit">Vis</button>
    </form>
  </div>
  <iframe id="map-frame" src="/ver/solskinn-kart?mode=last24h" loading="lazy"></iframe>
</div>
<script>
const STORE_KEY="precip_view_v1";
const frame=document.getElementById("map-frame");
const dateInput=document.getElementById("date-input"),modeSelect=document.getElementById("mode-select");
function readSaved(){{try{{const r=sessionStorage.getItem(STORE_KEY);return r?JSON.parse(r):null;}}catch(e){{return null;}}}}
function buildUrl(){{const qs=new URLSearchParams();qs.set("mode",modeSelect.value||"last24h");qs.set("date",dateInput.value||"{today_str}");const s=readSaved();if(s){{if(s.bbox)qs.set("bbox",s.bbox);if(s.z)qs.set("z",s.z);if(s.clat)qs.set("clat",s.clat);if(s.clon)qs.set("clon",s.clon);}}return"/ver/solskinn-kart?"+qs.toString();}}
function saveFU(){{try{{const u=new URL(frame.contentWindow.location.href);const b=u.searchParams.get("bbox");if(!b)return;sessionStorage.setItem(STORE_KEY,JSON.stringify({{bbox:b,z:u.searchParams.get("z")||"",clat:u.searchParams.get("clat")||"",clon:u.searchParams.get("clon")||""}}));}}catch(e){{}}}}
frame.addEventListener("load",saveFU);
document.getElementById("controls-form").addEventListener("submit",e=>{{e.preventDefault();showLoader();frame.src=buildUrl();}});
const FORECAST_MODES=new Set(["next24h","next48h","next7d"]);
function toggleDateInput(){{const isFc=FORECAST_MODES.has(modeSelect.value);dateInput.style.display=isFc?"none":"";dateInput.previousElementSibling.style.display=isFc?"none":"";}}
modeSelect.addEventListener("change",()=>{{toggleDateInput();frame.src=buildUrl();}});
toggleDateInput();
</script></body></html>"""


@ver.route("/solskinn-kart")
def solskinn_kart() -> str:
    mode = request.args.get("mode", "last24h")
    if mode not in {"last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"}: mode = "last24h"
    return build_sunshine_map_html(
        date_str=request.args.get("date"), mode=mode,
        bbox=request.args.get("bbox"),
        z=request.args.get("z"), clat=request.args.get("clat"), clon=request.args.get("clon"),
        show_heatmap=True,
    )


# =========================
# VIND
# =========================
@ver.route("/vind")
def vind_index() -> str:
    today_str = _date.today().isoformat()
    return f"""<!doctype html>
<html lang="no"><head><meta charset="utf-8"/><title>Vind i Norge</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
body{{margin:0;font-family:system-ui,sans-serif;background:#f5f7fb;}}
.page{{max-width:1200px;margin:32px auto;padding:0 16px 32px;}}
.card{{background:white;border-radius:16px;padding:18px 22px;box-shadow:0 18px 45px rgba(15,23,42,.08);margin-bottom:16px;}}
.card h1{{margin:0 0 8px;}}
.controls{{display:flex;flex-wrap:wrap;gap:10px;margin-top:10px;align-items:center;}}
.controls input,.controls select{{padding:6px 10px;border-radius:10px;border:1px solid #d1d5db;}}
.controls button{{padding:7px 14px;border-radius:999px;border:none;background:#2563eb;color:white;cursor:pointer;}}
#map-frame{{width:100%;height:80vh;border:none;border-radius:16px;box-shadow:0 18px 45px rgba(15,23,42,.10);background:#e5e7eb;}}
</style></head><body>
<div class="page">
  <div class="card">
    <h1>Vind i Norge</h1>
    <p style="margin:0;color:#475569;">Velg observerte vinddata (Frost) eller forventet vind neste 24 timer (Yr).</p>
    <form id="controls-form" class="controls">
      <label>Dato:</label><input type="date" id="date-input" value="{today_str}" max="{today_str}">
      <label>Kilde:</label>
      <select id="mode-select">
        <option value="observed" selected>Observerte data</option>
        <option value="forecast">Forventet vind</option>
      </select>
      <label>Region:</label>
      <select id="region-select">
        <option value="all" selected>Hele landet</option>
        <option value="south">Sør</option>
        <option value="mid">Midt</option>
        <option value="north">Nord</option>
      </select>
      <label>Periode:</label>
      <select id="period-select">
        <option value="hour" selected>Per time (siste 24t)</option>
        <option value="day">Per dag</option>
        <option value="month">Per måned</option>
      </select>
      <label>Værmelding:</label>
      <select id="forecast-hours">
        <option value="24" selected>I dag / neste 24t</option>
        <option value="72">Neste 3 døgn</option>
        <option value="168">Neste uke</option>
      </select>
      <label>Måling:</label>
      <select id="metric-select">
        <option value="avg">Gjennomsnittsvind</option>
        <option value="gust" selected>Maks vindkast</option>
      </select>
      <button type="submit">Vis</button>
      <button type="button" id="btn-reset">Reset kart</button>
    </form>
  </div>

  <details style="margin-top:12px;border:0.5px solid var(--color-border-tertiary,#e5e7eb);border-radius:8px;padding:8px 12px;">
    <summary style="cursor:pointer;font-size:13px;font-weight:500;color:var(--color-text-secondary,#6b7280);user-select:none;">
      &#128168; Hva betyr vindstyrken? (Beaufort-skalaen)
    </summary>
    <table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px;">
      <thead><tr style="border-bottom:1px solid var(--color-border-tertiary,#e5e7eb);">
        <th style="text-align:left;padding:4px 6px;font-weight:500;color:var(--color-text-secondary,#6b7280);">Bf</th>
        <th style="text-align:left;padding:4px 6px;font-weight:500;color:var(--color-text-secondary,#6b7280);">m/s</th>
        <th style="text-align:left;padding:4px 6px;font-weight:500;color:var(--color-text-secondary,#6b7280);">Betegnelse</th>
        <th style="text-align:left;padding:4px 6px;font-weight:500;color:var(--color-text-secondary,#6b7280);display:none" class="bf-desc">Beskrivelse</th>
      </tr></thead>
      <tbody>
        <tr><td style="padding:3px 6px;color:#888">0</td><td style="padding:3px 6px;font-family:monospace">0–0.2</td><td style="padding:3px 6px">Stille</td></tr>
        <tr><td style="padding:3px 6px;color:#639922">1–3</td><td style="padding:3px 6px;font-family:monospace">0.3–5.4</td><td style="padding:3px 6px">Svak vind</td></tr>
        <tr><td style="padding:3px 6px;color:#639922">4</td><td style="padding:3px 6px;font-family:monospace">5.5–7.9</td><td style="padding:3px 6px">Laber bris</td></tr>
        <tr style="background:var(--color-background-secondary,#f9fafb)"><td style="padding:3px 6px;color:#BA7517">5</td><td style="padding:3px 6px;font-family:monospace">8–10.7</td><td style="padding:3px 6px">Frisk bris &#8212; sm&#229;tr&#230;r svaier</td></tr>
        <tr><td style="padding:3px 6px;color:#EF9F27">6</td><td style="padding:3px 6px;font-family:monospace">10.8–13.8</td><td style="padding:3px 6px">Liten kuling &#8212; store greiner beveger seg</td></tr>
        <tr style="background:var(--color-background-secondary,#f9fafb)"><td style="padding:3px 6px;color:#D85A30">7</td><td style="padding:3px 6px;font-family:monospace">13.9–17.1</td><td style="padding:3px 6px">Stiv kuling &#8212; tungt &#229; g&#229; mot vinden</td></tr>
        <tr><td style="padding:3px 6px;color:#993C1D">8</td><td style="padding:3px 6px;font-family:monospace">17.2–20.7</td><td style="padding:3px 6px">Sterk kuling &#8212; nesten umulig &#229; g&#229; mot vinden</td></tr>
        <tr style="background:var(--color-background-secondary,#f9fafb)"><td style="padding:3px 6px;color:#A32D2D">9</td><td style="padding:3px 6px;font-family:monospace">20.8–24.4</td><td style="padding:3px 6px">Liten storm &#8212; takstein kan l&#248;sne</td></tr>
        <tr><td style="padding:3px 6px;color:#791F1F">10</td><td style="padding:3px 6px;font-family:monospace">24.5–28.4</td><td style="padding:3px 6px">Full storm &#8212; tr&#230;r rykkes opp</td></tr>
        <tr style="background:var(--color-background-secondary,#f9fafb)"><td style="padding:3px 6px;color:#501313">11</td><td style="padding:3px 6px;font-family:monospace">28.5–32.6</td><td style="padding:3px 6px">Sterk storm &#8212; store &#248;deleggelser</td></tr>
        <tr><td style="padding:3px 6px;font-weight:500;color:#2C2C2A">12</td><td style="padding:3px 6px;font-family:monospace">32.7+</td><td style="padding:3px 6px;font-weight:500">Orkan &#8212; katastrofeskader</td></tr>
      </tbody>
    </table>
    <p style="font-size:11px;color:var(--color-text-secondary,#6b7280);margin-top:6px;">
      Kast er typisk 30&#8211;50% h&#248;yere enn middelvindhastigheten.
    </p>
  </details>
  <div style="position:relative;">
    <div style="padding:4px 0 6px;color:#64748b;font-size:12px;">
    💡 Klikk på en stasjon for time-for-time vær- og vinddata
  </div>
  <div id="map-loader" style="position:absolute;inset:0;z-index:10;background:rgba(15,23,42,0.85);display:flex;flex-direction:column;align-items:center;justify-content:center;border-radius:12px;min-height:400px;">
      <div style="width:48px;height:48px;border:5px solid #334155;border-top-color:#93c5fd;border-radius:50%;animation:spin 0.9s linear infinite;"></div>
      <div id="loader-text" style="color:#93c5fd;margin-top:16px;font-size:14px;font-family:system-ui;">Henter vinddata...</div>
    </div>
    <iframe id="map-frame" src="/ver/vind-kart?mode=observed&period=hour&metric=gust" height="700" style="width:100%;border:none;border-radius:12px;" onload="document.getElementById('map-loader').style.display='none';"></iframe>
  </div>
  <style>@keyframes spin{{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>
</div>
<script>
const STORE_KEY="wind_view_v1";
const frame=document.getElementById("map-frame");
const dateInput=document.getElementById("date-input");
const modeSelect=document.getElementById("mode-select");
const regionSelect=document.getElementById("region-select");
const periodSelect=document.getElementById("period-select");
const forecastHoursSelect=document.getElementById("forecast-hours");
const metricSelect=document.getElementById("metric-select");
function readSaved(){{try{{const r=sessionStorage.getItem(STORE_KEY);return r?JSON.parse(r):null;}}catch(e){{return null;}}}}
function saveFU(){{try{{const u=new URL(frame.contentWindow.location.href);const b=u.searchParams.get("bbox");if(!b)return;sessionStorage.setItem(STORE_KEY,JSON.stringify({{bbox:b,z:u.searchParams.get("z")||"",clat:u.searchParams.get("clat")||"",clon:u.searchParams.get("clon")||""}}));}}catch(e){{}}}}
function showLoader(){{
  var el=document.getElementById('map-loader');
  var txt=document.getElementById('loader-text');
  if(el) el.style.display='flex';
  if(txt) txt.textContent='Henter vinddata...';
}}
function buildUrl(){{
  const qs=new URLSearchParams();
  qs.set("mode",modeSelect.value||"observed");
  qs.set("region",regionSelect.value||"all");
  qs.set("period",periodSelect.value||"hour");
  qs.set("forecast_hours",forecastHoursSelect.value||"24");
  qs.set("metric",metricSelect.value||"avg");
  qs.set("date",dateInput.value||"{today_str}");
  const s=readSaved();
  if(s){{if(s.bbox)qs.set("bbox",s.bbox);if(s.z)qs.set("z",s.z);if(s.clat)qs.set("clat",s.clat);if(s.clon)qs.set("clon",s.clon);}}
  return "/ver/vind-kart?"+qs.toString();
}}
frame.addEventListener("load",saveFU);
modeSelect.addEventListener("change",()=>{{
  const fc = modeSelect.value === "forecast";
  periodSelect.disabled = fc;
  forecastHoursSelect.disabled = !fc;

  showLoader(); frame.src=buildUrl();
}});
regionSelect.addEventListener("change",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}frame.src=buildUrl();}});
periodSelect.addEventListener("change",()=>{{showLoader();frame.src=buildUrl();}});
forecastHoursSelect.addEventListener("change",()=>{{showLoader();frame.src=buildUrl();}});
metricSelect.addEventListener("change",()=>{{showLoader();frame.src=buildUrl();}});
document.getElementById("controls-form").addEventListener("submit",e=>{{e.preventDefault();showLoader();frame.src=buildUrl();}});
document.getElementById("btn-reset").addEventListener("click",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}frame.src="/ver/vind-kart?mode="+(modeSelect.value||"observed")+"&region="+(regionSelect.value||"all")+"&period="+(periodSelect.value||"hour")+"&forecast_hours="+(forecastHoursSelect.value||"24")+"&metric="+(metricSelect.value||"avg")+"&date="+(dateInput.value||"{today_str}");}});
periodSelect.disabled = false; forecastHoursSelect.disabled = true;
</script></body></html>"""




# ── /ver/vind-popup ──────────────────────────────────────────────────────────
_VIND_POPUP_CACHE: dict = {}

def _vindretning_txt_vp(deg: float) -> str:
    d = ['N','NØ','Ø','SØ','S','SV','V','NV']
    return d[int((deg + 22.5) / 45) % 8]

def _hent_frost_timesdata(station_id: str) -> list:
    import os, requests as _req
    from datetime import datetime, timedelta, timezone
    from collections import defaultdict
    auth = (os.getenv('FROST_CLIENT_ID',''), os.getenv('FROST_CLIENT_SECRET',''))
    now  = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=24)
    rt = f"{start.strftime('%Y-%m-%dT%H:00:00Z')}/{now.strftime('%Y-%m-%dT%H:00:00Z')}"
    elements = 'wind_speed,max(wind_speed_of_gust PT1H),wind_from_direction,air_temperature,sum(precipitation_amount PT1H)'
    try:
        r = _req.get(
            'https://frost.met.no/observations/v0.jsonld',
            params={'sources': station_id, 'referencetime': rt, 'elements': elements, 'limit': 1000},
            auth=auth, headers={'Accept': 'application/json'}, timeout=20,
        )
        if r.status_code != 200:
            return []
    except Exception:
        return []
    by_time: dict = defaultdict(dict)
    for item in r.json().get('data', []) or []:
        t = item.get('referenceTime','')[:16]
        for obs in item.get('observations', []) or []:
            eid = obs.get('elementId','')
            val = obs.get('value')
            if val is None: continue
            if eid == 'wind_speed': by_time[t]['vind'] = round(float(val),1)
            elif eid == 'max(wind_speed_of_gust PT1H)': by_time[t]['kast'] = round(float(val),1)
            elif eid == 'wind_from_direction': by_time[t]['retning'] = _vindretning_txt_vp(float(val))
            elif eid == 'air_temperature': by_time[t]['temp'] = round(float(val),1)
            elif eid == 'sum(precipitation_amount PT1H)': by_time[t]['nedbor'] = round(float(val),1)
    rows = []
    for t in sorted(by_time.keys()):
        d = by_time[t]
        rows.append({'tid': t[11:16], 'dato': t[:10], 'vind': d.get('vind'),
                     'kast': d.get('kast'), 'retning': d.get('retning',''),
                     'retning_grader': None, 'temp': d.get('temp'), 'nedbor': d.get('nedbor', 0.0),
                     'nedbor_prob': None})
    return rows

def _hent_yr_timesdata(lat: float, lon: float, hours: int = 48) -> list:
    import requests as _req
    from datetime import datetime, timedelta, timezone
    import pandas as pd
    try:
        r = _req.get(
            'https://api.met.no/weatherapi/locationforecast/2.0/complete',
            params={'lat': f'{lat:.4f}', 'lon': f'{lon:.4f}'},
            headers={'User-Agent': 'prisanalyse.no/1.0 kontakt@prisanalyse.no'},
            timeout=20,
        )
        if r.status_code != 200:
            return []
    except Exception:
        return []
    ts = r.json()['properties']['timeseries']
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=hours)
    rows = []
    for it in ts:
        t = pd.to_datetime(it.get('time'), utc=True, errors='coerce')
        if pd.isna(t): continue
        t_py = t.to_pydatetime()
        if t_py < now or t_py > horizon: continue
        inst = ((it.get('data') or {}).get('instant') or {}).get('details') or {}
        n1d  = ((it.get('data') or {}).get('next_1_hours') or {}).get('details') or {}
        ws = inst.get('wind_speed')
        wg = inst.get('wind_speed_of_gust')
        wd = inst.get('wind_from_direction')
        tmp = inst.get('air_temperature')
        prec = n1d.get('precipitation_amount')
        prob = n1d.get('probability_of_precipitation')
        rows.append({
            'tid': t_py.astimezone().strftime('%H:%M'),
            'dato': t_py.astimezone().strftime('%a %d.%m'),
            'vind': round(ws,1) if ws is not None else None,
            'kast': round(wg,1) if wg is not None else None,
            'retning': _vindretning_txt_vp(wd) if wd is not None else '',
            'retning_grader': round(wd) if wd is not None else None,
            'temp': round(tmp,1) if tmp is not None else None,
            'nedbor': round(prec,1) if prec is not None else 0.0,
            'nedbor_prob': round(prob) if prob is not None else None,
        })
    return rows

def _vind_verdict(fc_rows: list, obs_rows: list) -> str:
    rows = fc_rows if fc_rows else obs_rows
    if not rows:
        return ''
    vind_vals  = [r.get('vind') or 0 for r in rows]
    kast_vals  = [r.get('kast') or 0 for r in rows]
    nedbor_sum = sum(r.get('nedbor') or 0 for r in rows)
    max_vind   = max(vind_vals) if vind_vals else 0
    max_kast   = max(kast_vals) if kast_vals else 0

    if max_kast >= 32.7 or max_vind >= 28.5:
        vind_badge = ('<span style="background:#7f1d1d;color:#fca5a5;padding:2px 8px;border-radius:4px;font-weight:700">' +
                      '&#128308; Orkan/storm</span>')
    elif max_kast >= 24.5 or max_vind >= 20.8:
        vind_badge = ('<span style="background:#7c2d12;color:#fdba74;padding:2px 8px;border-radius:4px;font-weight:700">' +
                      '&#128308; Sterk kuling</span>')
    elif max_kast >= 17.2 or max_vind >= 13.9:
        vind_badge = ('<span style="background:#713f12;color:#fde68a;padding:2px 8px;border-radius:4px;font-weight:700">' +
                      '&#128993; Kuling</span>')
    elif max_vind >= 8.0:
        vind_badge = ('<span style="background:#1c3d5a;color:#93c5fd;padding:2px 8px;border-radius:4px;font-weight:700">' +
                      '&#128994; Frisk bris</span>')
    else:
        vind_badge = ('<span style="background:#14532d;color:#86efac;padding:2px 8px;border-radius:4px;font-weight:700">' +
                      '&#128994; Svak vind</span>')

    if nedbor_sum >= 20:
        neb_badge = ('&nbsp;<span style="background:#1e3a5f;color:#60a5fa;padding:2px 8px;border-radius:4px;font-weight:700">' +
                     '&#127783; Kraftig neb&#248;r</span>')
    elif nedbor_sum >= 5:
        neb_badge = ('&nbsp;<span style="background:#1e3a5f;color:#93c5fd;padding:2px 8px;border-radius:4px;font-weight:700">' +
                     '&#127783; Moderat neb&#248;r</span>')
    else:
        neb_badge = ''

    maks_txt = f'Maks {max_vind:.0f}'
    if max_kast > max_vind:
        maks_txt += f' (kast {max_kast:.0f}) m/s'
    else:
        maks_txt += ' m/s'

    return (
        '<div style="padding:6px 12px 8px;display:flex;flex-wrap:wrap;gap:4px;align-items:center">' +
        vind_badge + neb_badge +
        f'<span style="color:#475569;font-size:10px;margin-left:6px">{maks_txt}</span>' +
        '</div>'
    )


def _build_popup_html(name: str, station_id: str, obs_rows: list, fc_rows: list, hours: int) -> str:
    max_vind_obs = max((r.get('vind') or 0) for r in obs_rows) or 1
    max_vind_fc  = max((r.get('vind') or 0) for r in fc_rows) or 1

    def _vc(v, mx):
        if v is None: return '#64748b'
        ratio = v / mx
        if ratio < 0.4: return '#22c55e'
        if ratio < 0.7: return '#f59e0b'
        return '#ef4444'

    def _tc(t):
        if t is None: return '#94a3b8'
        if t < -5: return '#93c5fd'
        if t < 0:  return '#bfdbfe'
        if t < 5:  return '#fde68a'
        return '#fca5a5'

    def _make_tbody(rows, mx):
        prev_dato = ''
        tbody = ''
        for r in rows:
            dato_header = ''
            dato = r.get('dato','')
            if dato != prev_dato:
                prev_dato = dato
                dato_header = f'<tr><td colspan="6" style="background:#1e293b;color:#93c5fd;font-weight:700;padding:4px 8px;font-size:11px">{dato}</td></tr>'
            v = r.get('vind')
            k = r.get('kast')
            t = r.get('temp')
            n = r.get('nedbor') or 0
            p = r.get('nedbor_prob')
            ret = r.get('retning','')
            rdeg = r.get('retning_grader')
            pil = f'<span style="display:inline-block;transform:rotate({rdeg}deg)">&#8593;</span>' if rdeg is not None else ''
            neb = f'<span style="color:#60a5fa">{n:.1f}</span>' if n > 0 else '<span style="color:#334155">&#8212;</span>'
            if n > 0 and p: neb += f' <span style="color:#475569;font-size:10px">{p}%</span>'
            tbody += (
                dato_header +
                f'<tr>'
                f'<td style="padding:3px 6px;color:#94a3b8;white-space:nowrap">{r.get("tid","")}</td>'
                f'<td style="padding:3px 6px;color:{_tc(t)};font-weight:600">{f"{t:+.1f}" + chr(176) if t is not None else "&#8212;"}</td>'
                f'<td style="padding:3px 6px;color:{_vc(v,mx)};font-weight:700">{v if v is not None else "&#8212;"}</td>'
                f'<td style="padding:3px 6px;color:#64748b">{f"({k})" if k else ""}</td>'
                f'<td style="padding:3px 6px;color:#475569;font-size:11px;white-space:nowrap">{pil} {ret}</td>'
                f'<td style="padding:3px 6px">{neb}</td>'
                f'</tr>'
            )
        return tbody or '<tr><td colspan="6" style="color:#475569;padding:12px">Ingen data</td></tr>'

    obs_tbody = _make_tbody(obs_rows, max_vind_obs)
    fc_tbody  = _make_tbody(fc_rows,  max_vind_fc)

    thead = ('<thead><tr>'
             '<th>Tid</th><th>Temp</th><th>Vind</th><th>Kast</th><th>Retning</th><th>Neb&#248;r</th>'
             '</tr></thead>')

    css = (
        'body{margin:0;background:#0f172a;color:#e2e8f0;font:12px/1.4 system-ui}'
        'h2{margin:0;padding:8px 12px 2px;font-size:13px;color:#f1f5f9}'
        '.sub{padding:0 12px 6px;color:#475569;font-size:10px}'
        '.tabs{display:flex;border-bottom:1px solid #1e293b;}'
        '.tab{padding:6px 14px;cursor:pointer;color:#64748b;font-size:11px;font-weight:600;border-bottom:2px solid transparent}'
        '.tab.active{color:#93c5fd;border-bottom-color:#93c5fd}'
        '.panel{display:none}.panel.active{display:block}'
        'table{width:100%;border-collapse:collapse}'
        'th{padding:4px 6px;text-align:left;color:#475569;font-size:10px;border-bottom:1px solid #1e293b}'
        'tr:hover td{background:#ffffff08}'
        '.leg{padding:5px 12px;color:#334155;font-size:10px;border-top:1px solid #1e293b}'
    )

    js = (
        'function showTab(id){'
        'document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));'
        'document.querySelectorAll(".panel").forEach(p=>p.classList.remove("active"));'
        'document.getElementById("tab-"+id).classList.add("active");'
        'document.getElementById("panel-"+id).classList.add("active");'
        '}'
    )

    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<style>{css}</style></head><body>'
        f'<h2>&#128168; {name}</h2>'
        f'<div class="sub">{station_id}</div>'
        f'{_vind_verdict(fc_rows, obs_rows)}'
        f'<div class="tabs">'
        f'<div class="tab active" id="tab-fc" onclick="showTab(\'fc\')">&#128302; Prognose ({hours}t)</div>'
        f'<div class="tab" id="tab-obs" onclick="showTab(\'obs\')">&#128200; Observert (24t)</div>'
        f'</div>'
        f'<div class="panel active" id="panel-fc">'
        f'<table>{thead}<tbody>{fc_tbody}</tbody></table>'
        f'</div>'
        f'<div class="panel" id="panel-obs">'
        f'<table>{thead}<tbody>{obs_tbody}</tbody></table>'
        f'</div>'
        f'<div class="leg">Vind/kast i m/s &middot; Neb&#248;r mm/t</div>'
        f'<script>{js}</script>'
        f'</body></html>'
    )


@ver.route('/vind-popup')
def vind_popup():
    import time as _time
    station_id = request.args.get('id', '')
    mode       = request.args.get('mode', 'observed')
    lat        = request.args.get('lat', type=float)
    lon        = request.args.get('lon', type=float)
    name       = request.args.get('name', station_id)
    hours      = request.args.get('hours', 48, type=int)
    cache_key  = (station_id, mode, hours)
    cached     = _VIND_POPUP_CACHE.get(cache_key)
    if cached and cached[0] > _time.time():
        from flask import Response as _Resp
        return _Resp(cached[1], mimetype='text/html; charset=utf-8')
    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=2) as ex:
        fut_obs = ex.submit(_hent_frost_timesdata, station_id) if station_id else None
        fut_fc  = ex.submit(_hent_yr_timesdata, lat, lon, hours) if (lat is not None and lon is not None) else None
        obs_rows = fut_obs.result() if fut_obs else []
        fc_rows  = fut_fc.result()  if fut_fc  else []
    # Filter obs to whole hours only
    obs_rows = [r for r in obs_rows if r.get('tid','').endswith(':00')]
    html = _build_popup_html(name, station_id, obs_rows, fc_rows, hours)
    _VIND_POPUP_CACHE[cache_key] = (_time.time() + 1800, html)
    from flask import Response as _Resp
    return _Resp(html, mimetype='text/html; charset=utf-8')





@ver.route("/vind-kart")
def vind_kart() -> str:
    mode = request.args.get("mode", "observed")
    period = request.args.get("period", "hour")
    metric = request.args.get("metric", "avg")
    forecast_hours = int(request.args.get("forecast_hours", "24") or "24")
    region = request.args.get("region", "all")
    return build_wind_map_html(
        mode=mode if mode in {"observed", "forecast"} else "observed",
        period=period if period in {"hour", "day", "month"} else "hour",
        metric=metric if metric in {"avg", "gust"} else "avg",
        date_str=request.args.get("date"),
        forecast_hours=max(6, min(forecast_hours, 168)),
        region=region if region in {"all", "south", "mid", "north"} else "all",
        bbox=request.args.get("bbox"),
        z=request.args.get("z"),
        clat=request.args.get("clat"),
        clon=request.args.get("clon"),
        show_heatmap=True,
        top_n=600,
    )


def _vindretning_txt_general(deg: Optional[float]) -> str:
    if deg is None:
        return "Ukjent"
    dirs = ["N", "NØ", "Ø", "SØ", "S", "SV", "V", "NV"]
    return dirs[int((float(deg) + 22.5) // 45) % 8]


def _hent_yr_komplett(lat: float, lon: float) -> list[dict[str, Any]]:
    r = requests.get(
        "https://api.met.no/weatherapi/locationforecast/2.0/complete",
        params={"lat": f"{lat:.4f}", "lon": f"{lon:.4f}"},
        headers={"User-Agent": "prisanalyse.no/1.0 kontakt@prisanalyse.no"},
        timeout=20,
    )
    r.raise_for_status()
    return ((r.json() or {}).get("properties") or {}).get("timeseries") or []


def _til_aktivitetstype(temp: float, rain: float, wind: float, gust: float, hour_local: Optional[int] = None) -> str:
    if hour_local is not None and (hour_local >= 23 or hour_local <= 5):
        if rain > 2.0 or gust >= 15:
            return "Natt: krevende forhold ute"
        return "Natt: rolig tur passer bedre enn løpetur"
    if rain > 2.0 or gust >= 15:
        return "Utfordrende utevær"
    if temp >= 19 and rain < 0.2 and wind <= 6:
        return "Svært bra for bading/sol"
    if 10 <= temp <= 22 and rain < 0.6 and wind <= 10:
        return "Bra for løping og tur"
    if temp < 3 and rain > 0:
        return "Kaldt og vått"
    return "Greit utevær"


def _lag_kvalitetsvurdering(temp: float, rain: float, wind: float, gust: float) -> dict[str, str]:
    if rain <= 0.2 and wind <= 6 and gust <= 10:
        return {"score": "5/5", "label": "Meget bra", "reason": "Lite nedbør og behagelig vind."}
    if rain <= 0.8 and gust <= 13:
        return {"score": "4/5", "label": "Bra", "reason": "Brukbare forhold med begrenset nedbør/vind."}
    if rain <= 1.8 and gust <= 17:
        return {"score": "3/5", "label": "Middels", "reason": "Noe regn eller vind trekker ned komforten."}
    if rain <= 3.0 and gust <= 22:
        return {"score": "2/5", "label": "Svakt", "reason": "Mye vind eller nedbør i perioder."}
    return {"score": "1/5", "label": "Dårlig", "reason": "Kraftig vind og/eller nedbør."}


@ver.get("/api/sted-sok")
def api_sted_sok():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"items": []})

    # Hurtig-fallback for vanlige norske byer hvis tredjeparts geokoding feiler/er treg.
    by_fallback = {
        "oslo": (59.91387, 10.75225, "Oslo, Norge"),
        "bergen": (60.39299, 5.32415, "Bergen, Norge"),
        "trondheim": (63.4305, 10.3951, "Trondheim, Norge"),
        "stavanger": (58.97, 5.7331, "Stavanger, Norge"),
        "kristiansand": (58.1467, 7.9956, "Kristiansand, Norge"),
        "tromsø": (69.6492, 18.9553, "Tromsø, Norge"),
        "tromso": (69.6492, 18.9553, "Tromsø, Norge"),
    }

    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "jsonv2", "addressdetails": 1, "limit": 8, "accept-language": "no"},
            headers={"User-Agent": "prisanalyse.no/1.0 kontakt@prisanalyse.no"},
            timeout=8,
        )
        r.raise_for_status()
        raw_items = r.json() or []
    except Exception:
        raw_items = []

    items = []
    for it in raw_items:
        try:
            items.append(
                {
                    "name": it.get("display_name"),
                    "lat": float(it.get("lat")),
                    "lon": float(it.get("lon")),
                }
            )
        except Exception:
            continue

    if not items:
        q_lower = q.lower()
        for key, (lat, lon, label) in by_fallback.items():
            if q_lower in key or key in q_lower:
                items.append({"name": label, "lat": lat, "lon": lon})

    return jsonify({"items": items})


def _reverse_geocode_label(lat: float, lon: float) -> Optional[str]:
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "jsonv2", "accept-language": "no"},
            headers={"User-Agent": "prisanalyse.no/1.0 kontakt@prisanalyse.no"},
            timeout=6,
        )
        r.raise_for_status()
        payload = r.json() or {}
        address = payload.get("address") or {}

        locality = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("hamlet")
        )
        country = address.get("country")
        if locality and country:
            return f"{locality}, {country}"
        if locality:
            return str(locality)
        display_name = payload.get("display_name")
        if display_name:
            return str(display_name).split(",")[0].strip()
    except Exception:
        return None
    return None


@ver.get("/api/aktivt-varsel")
def api_aktivt_varsel():
    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)
    sted = (request.args.get("sted") or "Valgt sted").strip()
    if lat is None or lon is None:
        return jsonify({"error": "lat/lon mangler"}), 400

    if sted.lower() in {"min posisjon", "my location", "current location"}:
        resolved_sted = _reverse_geocode_label(lat, lon)
        if resolved_sted:
            sted = resolved_sted

    ts = _hent_yr_komplett(lat, lon)
    now = datetime.now(timezone.utc)
    # `now` er allerede timezone-aware (UTC), så vi må ikke sende `tz=` på nytt.
    now_local = pd.Timestamp(now).tz_convert(OSLO)
    day_start_local = now_local.normalize()
    day_end_local = day_start_local + pd.Timedelta(days=1)
    day_start_utc = day_start_local.tz_convert("UTC").to_pydatetime()
    day_end_utc = day_end_local.tz_convert("UTC").to_pydatetime()
    horizon_24 = now + pd.Timedelta(hours=24)
    horizon_7d = now + pd.Timedelta(days=7)

    rows_24 = []   # brukes til KPI + forward-only beregninger (fremdeles neste 24t)
    rows_day = []  # dagens døgn (00:00-23:59 lokal tid), inkl historikk/prognose
    rows_7d = []   # -4t til +7d (alt vi har)
    for it in ts:
        t = pd.to_datetime(it.get("time"), utc=True, errors="coerce")
        if pd.isna(t):
            continue
        t_py = t.to_pydatetime()
        if t_py < day_start_utc or t_py > horizon_7d:
            continue
        data = it.get("data") or {}
        inst = (data.get("instant") or {}).get("details") or {}
        n1 = (data.get("next_1_hours") or {}).get("details") or {}
        rec = {
            "time": t_py.isoformat(),
            "is_history": t_py < now,
            "temp": float(inst.get("air_temperature")) if inst.get("air_temperature") is not None else None,
            "wind": float(inst.get("wind_speed")) if inst.get("wind_speed") is not None else 0.0,
            "gust": float(inst.get("wind_speed_of_gust")) if inst.get("wind_speed_of_gust") is not None else 0.0,
            "wind_deg": float(inst.get("wind_from_direction")) if inst.get("wind_from_direction") is not None else None,
            "cloud": float(inst.get("cloud_area_fraction")) if inst.get("cloud_area_fraction") is not None else None,
            "rain": float(n1.get("precipitation_amount")) if n1.get("precipitation_amount") is not None else 0.0,
            "rain_min": float(n1.get("precipitation_amount_min")) if n1.get("precipitation_amount_min") is not None else None,
            "rain_max": float(n1.get("precipitation_amount_max")) if n1.get("precipitation_amount_max") is not None else None,
            "rain_prob": float(n1.get("probability_of_precipitation")) if n1.get("probability_of_precipitation") is not None else None,
            "symbol": ((data.get("next_1_hours") or {}).get("summary") or {}).get("symbol_code", "cloudy"),
        }
        rows_7d.append(rec)
        if day_start_utc <= t_py < day_end_utc:
            rows_day.append(rec)
        if now <= t_py <= horizon_24:
            rows_24.append(rec)

    if not rows_24 and not rows_day:
        return jsonify({"error": "Fant ingen prognosedata"}), 502

    # KPI/visning tar utgangspunkt i dagens døgn (lokal tid)
    if rows_day:
        df_day = pd.DataFrame(rows_day)
    else:
        df_day = pd.DataFrame(rows_24)

    future_now = [r for r in rows_day if not r.get("is_history")] or rows_24
    df_now = pd.DataFrame(future_now) if future_now else df_day
    temp_now = float(df_now["temp"].dropna().iloc[0]) if not df_now["temp"].dropna().empty else 0.0
    rain_day = float(df_day["rain"].sum())
    max_wind = float(df_day["wind"].max())
    max_gust = float(df_day["gust"].max())
    quality = _lag_kvalitetsvurdering(temp_now, rain_day, max_wind, max_gust)

    best_windows = []
    current = None
    for rec in rows_7d:
        if rec.get("is_history"):
            continue
        ok = rec["rain"] <= 0.3 and rec["wind"] <= 8 and (rec["gust"] or 0) <= 12 and (rec["temp"] or -99) >= 8
        t_local = pd.to_datetime(rec["time"], utc=True).tz_convert(OSLO)
        if ok:
            if current is None:
                current = {"start": t_local, "end": t_local, "score": 0.0, "hours": 0}
            current["end"] = t_local
            current["hours"] += 1
            current["score"] += (max(0.0, 14 - rec["wind"]) + max(0.0, 0.5 - rec["rain"]) * 10 + max(0.0, (rec["temp"] or 8) - 8))
        elif current:
            if current["hours"] >= 2:
                best_windows.append(current)
            current = None
    if current and current["hours"] >= 2:
        best_windows.append(current)

    best_windows.sort(key=lambda x: (x["score"], x["hours"]), reverse=True)
    windows = []
    for w in best_windows[:6]:
        windows.append(
            {
                "start": w["start"].isoformat(),
                "end": (w["end"] + pd.Timedelta(hours=1)).isoformat(),
                "hours": w["hours"],
            }
        )

    # Bygg et komplett lokalt døgn (00:00–23:59) med timespunkter.
    # Hvis API mangler noen timer, fyller vi hull med tomme punkter slik at grafen alltid viser hele døgnet.
    rows_day_by_hour: dict[pd.Timestamp, dict[str, Any]] = {}
    for rec in rows_day:
        t_local = pd.to_datetime(rec["time"], utc=True).tz_convert(OSLO).floor("h")
        rows_day_by_hour[t_local] = rec

    full_day_slots = pd.date_range(start=day_start_local, end=day_end_local, inclusive="left", freq="h")

    hourly = []
    for slot_local in full_day_slots:
        rec = rows_day_by_hour.get(slot_local)
        is_history_slot = bool(slot_local.tz_convert("UTC").to_pydatetime() < now)

        if rec is None:
            hourly.append(
                {
                    "time": slot_local.isoformat(),
                    "is_history": is_history_slot,
                    "temp": None,
                    "rain": None,
                    "rain_min": None,
                    "rain_max": None,
                    "rain_prob": None,
                    "wind": None,
                    "gust": None,
                    "wind_dir": "Ukjent",
                    "cloud": None,
                    "activity": "Mangler historikk",
                    "symbol": "unknown",
                }
            )
            continue

        t_local = pd.to_datetime(rec["time"], utc=True).tz_convert(OSLO)
        hourly.append(
            {
                "time": t_local.isoformat(),
                "is_history": rec.get("is_history", False),
                "temp": round(float(rec["temp"]), 1) if rec["temp"] is not None else None,
                "rain": round(float(rec["rain"]), 1) if rec.get("rain") is not None else None,
                "rain_min": round(float(rec["rain_min"]), 1) if rec["rain_min"] is not None else None,
                "rain_max": round(float(rec["rain_max"]), 1) if rec["rain_max"] is not None else None,
                "rain_prob": int(round(rec["rain_prob"])) if rec["rain_prob"] is not None else None,
                "wind": round(float(rec["wind"]), 1) if rec.get("wind") is not None else None,
                "gust": round(float(rec["gust"]), 1) if rec.get("gust") is not None else None,
                "wind_dir": _vindretning_txt_general(rec["wind_deg"]),
                "cloud": round(float(rec["cloud"]), 0) if rec.get("cloud") is not None else None,
                "activity": _til_aktivitetstype(
                    rec["temp"] or 0.0,
                    rec["rain"] or 0.0,
                    rec["wind"] or 0.0,
                    rec["gust"] or 0.0,
                    t_local.hour,
                ),
                "symbol": rec["symbol"],
            }
        )

    # Hjelper: klassifiser en time som sol, skyet, eller nedbør basert på symbol
    def _sol_kategori(symbol: str, rain: float) -> str:
        s = (symbol or "").lower()
        if rain >= 0.2:
            return "rain"
        if "clearsky" in s or "fair" in s:
            return "sun"
        if "partlycloudy" in s:
            return "partly"
        return "cloudy"

    # Beste 6-timers blokk for en gitt liste av timesdata (kun dagtimer 06-22)
    def _beste_6t_blokk(hrs: list[dict]) -> Optional[dict]:
        # Score-funksjon per time: belønner lite regn, moderat temperatur, lite vind, lite skydekke
        def _score(h: dict) -> float:
            t = h.get("temp") or 0.0
            r = h.get("rain") or 0.0
            w = h.get("wind") or 0.0
            g = h.get("gust") or 0.0
            c = h.get("cloud") or 50.0
            # Ideell temp = 15-20°C, straff for kulde og varme
            temp_pts = max(0.0, 10 - abs(t - 17))
            rain_pts = max(0.0, 5 - r * 3)
            wind_pts = max(0.0, 8 - w) + max(0.0, 12 - g) * 0.5
            sun_pts = max(0.0, (100 - c) / 20)  # 0-5 poeng for sol
            return temp_pts + rain_pts + wind_pts + sun_pts

        # Filtrer dagtimer (06-22 lokal tid)
        dag = []
        for h in hrs:
            t_local = pd.to_datetime(h["time"], utc=True).tz_convert(OSLO)
            if 6 <= t_local.hour <= 22:
                dag.append({**h, "_local_hour": t_local.hour, "_local_iso": t_local.isoformat()})

        if len(dag) < 6:
            return None

        scores = [_score(h) for h in dag]
        best_sum = -1e9
        best_idx = 0
        for i in range(len(dag) - 5):
            s = sum(scores[i:i + 6])
            if s > best_sum:
                best_sum = s
                best_idx = i

        block = dag[best_idx:best_idx + 6]
        start_t = pd.to_datetime(block[0]["_local_iso"])
        end_t = pd.to_datetime(block[-1]["_local_iso"]) + pd.Timedelta(hours=1)
        return {
            "start": start_t.isoformat(),
            "end": end_t.isoformat(),
            "start_hour": block[0]["_local_hour"],
            "end_hour": (block[-1]["_local_hour"] + 1) % 24,
            "avg_temp": round(sum((h.get("temp") or 0) for h in block) / 6, 1),
            "total_rain": round(sum((h.get("rain") or 0) for h in block), 1),
            "max_wind": round(max((h.get("wind") or 0) for h in block), 1),
            "max_gust": round(max((h.get("gust") or 0) for h in block), 1),
        }

    daily = {}
    for rec in rows_7d:
        if rec.get("is_history"):
            continue  # daglig-aggregat skal kun bruke prognose fremover
        t_local = pd.to_datetime(rec["time"], utc=True).tz_convert(OSLO)
        key = t_local.date().isoformat()
        daily.setdefault(key, []).append(rec)

    daily_out = []
    for d, vals in sorted(daily.items()):
        temps = [v["temp"] for v in vals if v["temp"] is not None]
        rains = [v["rain"] for v in vals]
        winds = [v["wind"] for v in vals]
        gusts = [v["gust"] for v in vals]

        # Soltimer: tell timer i dagtid (06-22) som er "sun" eller "partly"
        sol_timer = 0
        sky_kategorier = {"sun": 0, "partly": 0, "cloudy": 0, "rain": 0}
        for v in vals:
            t_local = pd.to_datetime(v["time"], utc=True).tz_convert(OSLO)
            if 6 <= t_local.hour <= 21:
                kat = _sol_kategori(v.get("symbol", ""), v.get("rain", 0.0))
                sky_kategorier[kat] += 1
                if kat == "sun":
                    sol_timer += 1.0
                elif kat == "partly":
                    sol_timer += 0.5  # delvis skyet regnes som halv soltime

        # Bygg hourly-detaljer for denne dagen (brukes i expand-panel)
        day_hours = []
        for v in vals:
            t_local = pd.to_datetime(v["time"], utc=True).tz_convert(OSLO)
            day_hours.append({
                "time": t_local.isoformat(),
                "temp": round(float(v["temp"]), 1) if v["temp"] is not None else None,
                "rain": round(float(v["rain"]), 1),
                "rain_prob": int(round(v["rain_prob"])) if v["rain_prob"] is not None else None,
                "wind": round(float(v["wind"]), 1),
                "gust": round(float(v["gust"]), 1),
                "wind_deg": float(v["wind_deg"]) if v.get("wind_deg") is not None else None,
                "wind_dir": _vindretning_txt_general(v.get("wind_deg")),
                "cloud": round(float(v["cloud"]), 0) if v.get("cloud") is not None else None,
                "activity": _til_aktivitetstype(
                    v.get("temp") or 0.0,
                    v.get("rain") or 0.0,
                    v.get("wind") or 0.0,
                    v.get("gust") or 0.0,
                    t_local.hour,
                ),
                "symbol": v.get("symbol", ""),
            })

        daily_out.append(
            {
                "date": d,
                "temp_min": round(min(temps), 1) if temps else None,
                "temp_max": round(max(temps), 1) if temps else None,
                "rain_total": round(sum(rains), 1),
                "wind_max": round(max(winds), 1) if winds else None,
                "gust_max": round(max(gusts), 1) if gusts else None,
                "sun_hours": round(sol_timer, 1),
                "sky_mix": sky_kategorier,
                "hours": day_hours,
                "best_6h": _beste_6t_blokk(day_hours),
            }
        )

    return jsonify(
        {
            "sted": sted,
            "coords": {"lat": lat, "lon": lon},
            "quality": quality,
            "summary": {
                "temp_now": round(temp_now, 1),
                "temp_min_24h": round(float(df_day["temp"].min()), 1),
                "temp_max_24h": round(float(df_day["temp"].max()), 1),
                "rain_24h": round(rain_day, 1),
                "max_wind_24h": round(max_wind, 1),
                "max_gust_24h": round(max_gust, 1),
            },
            "hourly": hourly,
            "daily": daily_out[:8],
            "fine_windows": windows,
            "hentet": datetime.now().isoformat(timespec="seconds"),
        }
    )


@ver.get("/aktivt-varsel")
def aktivt_varsel() -> Response:
    return Response(_AKTIVT_VARSEL_HTML, mimetype="text/html; charset=utf-8")


_AKTIVT_VARSEL_HTML = r"""<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Værvarsel for aktiviteter – prisanalyse.no</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#f6f7f9;
  --surface:#ffffff;
  --border:#e5e7eb;
  --border-strong:#d1d5db;
  --text:#111827;
  --text-2:#4b5563;
  --text-3:#9ca3af;
  --accent:#2563eb;
  --accent-dark:#1e40af;
  --good:#16a34a;
  --good-bg:#dcfce7;
  --good-bg-light:#f0fdf4;
  --warn:#d97706;
  --warn-bg:#fef3c7;
  --bad:#dc2626;
  --bad-bg:#fee2e2;
  --night:#64748b;
  --night-bg:#e2e8f0;
  --temp:#ea580c;
  --rain:#2563eb;
  --rain-light:#93c5fd;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;line-height:1.5;font-size:14px}
.wrap{max-width:1100px;margin:0 auto;padding:20px 16px 48px}

/* ---------- SØKEKORT ---------- */
.search-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:16px}
.search-card h1{font-size:20px;font-weight:600;margin:0 0 4px;display:flex;align-items:center;gap:8px}
.search-card .sub{color:var(--text-2);margin:0 0 14px;font-size:13px}
.search-row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.search-row input{flex:1;min-width:220px;padding:9px 12px;border:1px solid var(--border-strong);border-radius:8px;font-size:14px;background:var(--surface);color:var(--text)}
.search-row input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px rgba(37,99,235,.15)}
.btn{padding:9px 14px;border-radius:8px;border:none;cursor:pointer;font-weight:500;font-size:14px;transition:opacity .15s}
.btn-primary{background:var(--accent);color:#fff}
.btn-primary:hover{opacity:.9}
.btn-secondary{background:var(--text);color:#fff}
.btn-secondary:hover{opacity:.9}
#status{margin-top:10px;font-size:13px;color:var(--text-2)}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.chip{background:#eff6ff;color:var(--accent-dark);padding:6px 10px;border-radius:6px;font-size:12px;border:1px solid #dbeafe;cursor:pointer}
.chip:hover{background:#dbeafe}

/* ---------- RESULTAT-SEKSJON ---------- */
#results{display:none}

/* Header */
.result-header{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap}
.result-header .place-label{font-size:11px;color:var(--text-3);text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.result-header .place-name{font-size:22px;font-weight:600;margin-top:2px}
.result-header .place-coords{font-size:12px;color:var(--text-2);margin-top:2px}
.result-header .updated{font-size:11px;color:var(--text-3);margin-top:6px}
.verdict-banner{padding:10px 16px;border-radius:10px;display:flex;align-items:center;gap:10px;min-width:220px}
.verdict-banner.good{background:var(--good-bg);color:var(--good)}
.verdict-banner.warn{background:var(--warn-bg);color:var(--warn)}
.verdict-banner.bad{background:var(--bad-bg);color:var(--bad)}
.verdict-banner .verdict-icon{font-size:24px}
.verdict-banner .verdict-label{font-size:10px;text-transform:uppercase;letter-spacing:.05em;opacity:.7;font-weight:600}
.verdict-banner .verdict-text{font-size:15px;font-weight:600}

/* Kvalitet-badge */
.quality{font-size:12px;color:var(--text-2);margin-bottom:14px;display:flex;align-items:center;gap:8px}
.quality-chip{background:#f3f4f6;color:var(--text);padding:3px 9px;border-radius:6px;font-weight:500;border:1px solid var(--border)}

/* KPI-bånd */
.kpi-band{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 18px;margin-bottom:14px;display:grid;grid-template-columns:repeat(5,1fr);gap:0}
.kpi-cell{padding:0 14px;border-right:1px solid var(--border)}
.kpi-cell:first-child{padding-left:0}
.kpi-cell:last-child{border-right:none;padding-right:0}
.kpi-lbl{font-size:10px;color:var(--text-2);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.kpi-val{font-size:22px;font-weight:600;margin-top:2px}
.kpi-val small{font-size:12px;color:var(--text-2);font-weight:400}

/* Graf-kort */
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:14px}
.chart-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px;flex-wrap:wrap;gap:8px}
.chart-header h3{margin:0;font-size:15px;font-weight:600}
.chart-legend{font-size:11px;color:var(--text-2);display:flex;gap:12px;flex-wrap:wrap}
.chart-legend span{display:inline-flex;align-items:center;gap:5px}
.chart-legend .sw{width:10px;height:10px;border-radius:2px;display:inline-block}

/* Verdict-bånd over grafen */
.verdict-strip{display:flex;height:16px;border-radius:4px;overflow:hidden;margin-bottom:2px}
.verdict-strip .seg{flex:1;position:relative}
.verdict-strip .seg.sun{background:#86efac}
.verdict-strip .seg.partly{background:#fde68a}
.verdict-strip .seg.cloudy{background:#cbd5e1}
.verdict-strip .seg.rain{background:#93c5fd}
.verdict-strip .seg.night{background:#e5e7eb}

.chart-box{position:relative;width:100%;height:260px}

/* Beste vinduer */
.windows-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:14px}
.windows-card h3{margin:0 0 10px;font-size:15px;font-weight:600}
.window-row{display:grid;grid-template-columns:60px 130px 1fr 55px;gap:12px;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px}
.window-row:last-child{border-bottom:none}
.window-row .w-day{font-weight:600;text-transform:capitalize}
.window-row .w-range{color:var(--text-2)}
.window-row .w-bar{background:var(--good-bg-light);height:8px;border-radius:4px;overflow:hidden;border:1px solid #bbf7d0}
.window-row .w-bar .w-fill{height:100%;background:var(--good)}
.window-row .w-hours{text-align:right;color:var(--text-2);font-size:12px}

/* 7-dagers strip */
.daily-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:14px}
.daily-card h3{margin:0 0 10px;font-size:15px;font-weight:600}
.daily-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px}
.daily-cell{background:#f9fafb;border:1px solid var(--border);border-radius:8px;padding:10px 12px;cursor:pointer;transition:all .15s;position:relative}
.daily-cell:hover{border-color:var(--accent);background:#f3f4f6}
.daily-cell.active{border-color:var(--accent);background:#eff6ff;box-shadow:0 0 0 1px var(--accent)}
.daily-cell .d-day{font-size:12px;color:var(--text-2);text-transform:capitalize;font-weight:500}
.daily-cell .d-icon-temp{display:flex;align-items:center;gap:6px;margin:4px 0 2px}
.daily-cell .d-icon{font-size:20px}
.daily-cell .d-temp{font-size:18px;font-weight:600}
.daily-cell .d-meta{font-size:11px;color:var(--text-2);line-height:1.45}
.daily-cell .d-sun{font-size:11px;color:var(--warn);font-weight:500;margin-top:3px;display:flex;align-items:center;gap:3px}
.daily-cell .d-arrow{position:absolute;top:10px;right:10px;font-size:10px;color:var(--text-3);transition:transform .2s}
.daily-cell.active .d-arrow{transform:rotate(180deg);color:var(--accent)}

/* Detalj-panel for valgt dag */
.day-detail{display:none;margin-top:12px;padding:16px;background:#f9fafb;border:1px solid var(--border);border-radius:10px}
.day-detail.show{display:block}
.day-detail-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px;flex-wrap:wrap;gap:8px}
.day-detail-header h4{margin:0;font-size:15px;font-weight:600}
.day-detail-close{background:none;border:none;color:var(--text-2);font-size:13px;cursor:pointer;padding:2px 6px;border-radius:4px}
.day-detail-close:hover{background:#e5e7eb}
.day-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:14px}
.day-stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.day-stat .ds-lbl{font-size:10px;color:var(--text-2);text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.day-stat .ds-val{font-size:17px;font-weight:600;margin-top:2px}
.day-stat .ds-val small{font-size:11px;color:var(--text-2);font-weight:400}
.best6-banner{background:#dcfce7;border:1px solid #86efac;color:#15803d;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;display:flex;align-items:center;gap:8px}
.best6-banner strong{font-weight:600}
.day-chart-box{position:relative;width:100%;height:200px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px}
.day-symbol-strip{display:grid;grid-template-columns:repeat(auto-fill,minmax(64px,1fr));gap:8px;margin:10px 0 12px}
.sym-chip{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;padding:8px 6px;border:1px solid var(--border);border-radius:10px;background:#fff}
.sym-chip .t{font-size:11px;color:var(--text-2);font-weight:500}
.sym-chip .wx{font-size:24px;line-height:1}
.sym-chip .w{font-size:11px;color:var(--text-2);display:flex;align-items:center;gap:4px}
.sym-chip .dir{font-size:13px;color:#334155}

/* Timesoversikt-tabell (kompakt) */
.hourly-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:14px}
.hourly-card h3{margin:0 0 10px;font-size:15px;font-weight:600}
.hourly-table{width:100%;border-collapse:collapse;font-size:12.5px}
.hourly-table th{text-align:left;padding:6px 8px;color:var(--text-2);font-weight:500;text-transform:uppercase;font-size:10.5px;letter-spacing:.04em;border-bottom:1px solid var(--border)}
.hourly-table td{padding:6px 8px;border-bottom:1px solid #f3f4f6}
.hourly-table tr:last-child td{border-bottom:none}
.hourly-table tr.row-good td{background:rgba(134,239,172,.12)}
.hourly-table tr.row-night td{background:rgba(203,213,225,.14)}
.hourly-table .time-cell{white-space:nowrap;font-weight:500}
.hourly-table .num{text-align:right;font-variant-numeric:tabular-nums}

/* Kollapsbar oversikts-seksjon */
.overview-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:0;margin-bottom:14px;overflow:hidden}
.overview-summary{padding:12px 18px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-size:13px;color:var(--text-2);user-select:none}
.overview-summary:hover{background:#f9fafb}
.overview-summary .arrow{transition:transform .2s}
.overview-card[open] .overview-summary .arrow{transform:rotate(90deg)}
.overview-body{padding:0 18px 16px}
.overview-table{width:100%;border-collapse:collapse;font-size:13px}
.overview-table th,.overview-table td{padding:8px 6px;border-bottom:1px solid var(--border);text-align:left}
.overview-table th{color:var(--text-2);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
.overview-table td.temp{text-align:right;white-space:nowrap;font-weight:600}
.overview-table td.day{text-align:center;white-space:nowrap}
.risk{padding:3px 8px;border-radius:6px;font-size:11px;font-weight:600;display:inline-flex;align-items:center;gap:4px}
.risk.ok{background:var(--good-bg);color:var(--good)}
.risk.mid{background:var(--warn-bg);color:var(--warn)}
.risk.high{background:var(--bad-bg);color:var(--bad)}
.muted{color:var(--text-3)}

@media(max-width:700px){
  .kpi-band{grid-template-columns:repeat(2,1fr);gap:10px}
  .kpi-cell{border-right:none;padding:0}
  .kpi-cell:nth-child(odd){border-right:1px solid var(--border);padding-right:10px}
  .kpi-cell:nth-child(even){padding-left:10px}
  .kpi-val{font-size:18px}
  .window-row{grid-template-columns:48px 1fr 40px;grid-template-rows:auto auto;gap:6px 10px}
  .window-row .w-range{grid-column:2;grid-row:1}
  .window-row .w-bar{grid-column:2;grid-row:2}
  .window-row .w-hours{grid-column:3;grid-row:1/3;align-self:center}
  .hourly-table th:nth-child(4),.hourly-table td:nth-child(4){display:none}
  .hourly-table th:nth-child(6),.hourly-table td:nth-child(6){display:none}
  .result-header{flex-direction:column}
  .verdict-banner{width:100%}
}
</style>
</head>
<body>
<div class="wrap">

  <!-- Søke-kort -->
  <div class="search-card">
    <h1>🌤️ Værvarsel for aktiviteter</h1>
    <p class="sub">Bruk posisjon eller søk sted. Grafen viser døgnet i dag (00:00–23:59), med historikk fram til nå og prognose videre i døgnet.</p>
    <div class="search-row">
      <button class="btn btn-primary" id="geoBtn">📍 Bruk min posisjon</button>
      <input id="placeInput" placeholder="Søk sted, f.eks. Bergen sentrum"/>
      <button class="btn btn-secondary" id="searchBtn">Søk</button>
    </div>
    <div id="status"></div>
    <div id="suggestions" class="chips"></div>
  </div>

  <!-- Resultater (skjult til data er lastet) -->
  <div id="results">

    <!-- Header med verdict -->
    <div class="result-header">
      <div>
        <div class="place-label">Posisjon</div>
        <div class="place-name" id="placeName">—</div>
        <div class="place-coords" id="placeCoords"></div>
        <div class="updated" id="updated"></div>
      </div>
      <div class="verdict-banner" id="verdictBanner">
        <div class="verdict-icon" id="verdictIcon">☀️</div>
        <div>
          <div class="verdict-label">Verdikt i dag</div>
          <div class="verdict-text" id="verdictText">—</div>
        </div>
      </div>
    </div>

    <div class="quality" id="quality"></div>

    <!-- KPI-bånd -->
    <div class="kpi-band" id="kpiBand"></div>

    <!-- Hovedgraf: 24 timer -->
    <div class="chart-card">
      <div class="chart-header">
        <h3>I dag: 00:00–23:59</h3>
        <div class="chart-legend">
          <span><span class="sw" style="background:#86efac"></span>Sol</span>
          <span><span class="sw" style="background:#fde68a"></span>Lettskyet</span>
          <span><span class="sw" style="background:#cbd5e1"></span>Overskyet</span>
          <span><span class="sw" style="background:#93c5fd"></span>Nedbør</span>
          <span><span class="sw" style="background:var(--temp)"></span>Temperatur</span>
          <span><span class="sw" style="background:var(--rain)"></span>Nedbør (forventet)</span>
          <span><span class="sw" style="background:var(--rain-light)"></span>Usikkerhet (maks)</span>
          <span><span class="sw" style="background:#e5e7eb;border:1px dashed #64748b"></span>Vind (stiplet linje)</span>
        </div>
      </div>
      <div class="verdict-strip" id="verdictStrip" title="Værtype per time"></div>
      <div class="chart-box">
        <canvas id="mainChart" role="img" aria-label="Kombinert graf over temperatur, nedbør og vind for dagens døgn."></canvas>
      </div>
    </div>

    <!-- Beste vinduer -->
    <div class="windows-card" id="windowsCard" style="display:none">
      <h3>Beste vinduer med fint vær (2+ timer)</h3>
      <div id="windowsList"></div>
    </div>

    <!-- 7-dagers strip -->
    <div class="daily-card" id="dailyCard" style="display:none">
      <h3>Kommende dager</h3>
      <div class="daily-grid" id="dailyGrid"></div>
      <div class="day-detail" id="dayDetail">
        <div class="day-detail-header">
          <h4 id="dayDetailTitle">—</h4>
          <button class="day-detail-close" id="dayDetailClose">✕ Lukk</button>
        </div>
        <div class="best6-banner" id="best6Banner" style="display:none"></div>
        <div class="day-stats" id="dayStats"></div>
        <div class="day-symbol-strip" id="daySymbols"></div>
        <div class="day-chart-box">
          <canvas id="dayChart" role="img" aria-label="Timesdetaljer for valgt dag"></canvas>
        </div>
      </div>
    </div>

    <!-- Detaljert timesoversikt (beholdt som tabell, mer kompakt) -->
    <div class="hourly-card" id="hourlyCard" style="display:none">
      <h3 id="hourlyTitle">Detaljert timesoversikt</h3>
      <div style="overflow:auto">
        <table class="hourly-table">
          <thead><tr>
            <th>Tid</th>
            <th class="num">Temp</th>
            <th class="num">Nedbør</th>
            <th class="num">Sjanse</th>
            <th class="num">Vind/kast</th>
            <th>Retning</th>
            <th>Vurdering</th>
          </tr></thead>
          <tbody id="hourlyBody"></tbody>
        </table>
      </div>
    </div>

  </div>

  <!-- Rask oversikt (kollapsbar, nederst) -->
  <details class="overview-card">
    <summary class="overview-summary">
      <span>🗺️ Rask oversikt – 5 norske byer</span>
      <span class="arrow">▶</span>
    </summary>
    <div class="overview-body">
      <table class="overview-table">
        <thead>
          <tr>
            <th>Sted</th>
            <th>Nå</th>
            <th>I dag</th>
            <th>I morgen</th>
            <th>+2d</th>
            <th>+3d</th>
            <th>Vurdering</th>
          </tr>
        </thead>
        <tbody id="overviewBody">
          <tr><td colspan="7" class="muted">Laster oversikt …</td></tr>
        </tbody>
      </table>
    </div>
  </details>

</div>

<script>
// ============================================================
// Hjelpefunksjoner
// ============================================================
const statusEl=document.getElementById('status');
const suggestionsEl=document.getElementById('suggestions');
const placeInput=document.getElementById('placeInput');
const overviewBody=document.getElementById('overviewBody');
const overviewPlaces=[
  {name:'Bergen',lat:60.39299,lon:5.32415},
  {name:'Oslo',lat:59.91387,lon:10.75225},
  {name:'Trondheim',lat:63.4305,lon:10.3951},
  {name:'Kristiansand',lat:58.1467,lon:7.9956},
  {name:'Tromsø',lat:69.6492,lon:18.9553},
];

function fmtLocal(iso,opt={hour:'2-digit',minute:'2-digit'}){return new Date(iso).toLocaleString('no-NO',opt);}

function weatherEmoji(symbol){
  const s=String(symbol||'').toLowerCase();
  if(s.includes('thunder')) return '⛈️';
  if(s.includes('sleet')) return '🌨️';
  if(s.includes('snow')) return '❄️';
  if(s.includes('rainshowers')||s.includes('heavyrain')||s.includes('rain')) return '🌧️';
  if(s.includes('fog')) return '🌫️';
  if(s.includes('partlycloudy')) return '⛅';
  if(s.includes('cloudy')) return '☁️';
  if(s.includes('clearsky')||s.includes('fair')) return '☀️';
  return '🌤️';
}

function windArrow(deg){
  if(deg==null || Number.isNaN(Number(deg))) return '•';
  const dirs=['↑','↗','→','↘','↓','↙','←','↖'];
  const idx=Math.round((Number(deg)%360)/45)%8;
  return dirs[idx];
}

function windStrengthIcon(speed){
  const s=Number(speed||0);
  if(s>=12) return '●';
  if(s>=7) return '◍';
  return '◌';
}

function setStatus(t){statusEl.textContent=t||'';}

function weatherStripBucket(hour){
  const h=new Date(hour.time).getHours();
  if(h<6 || h>=22) return 'night';
  const rain=Number(hour.rain||0);
  const s=String(hour.symbol||'').toLowerCase();
  if(rain>=0.2 || s.includes('rain') || s.includes('sleet') || s.includes('snow')) return 'rain';
  if(s.includes('clearsky') || s.includes('fair')) return 'sun';
  if(s.includes('partlycloudy')) return 'partly';
  return 'cloudy';
}

function weatherStripLabel(bucket){
  if(bucket==='sun') return 'Sol';
  if(bucket==='partly') return 'Lettskyet';
  if(bucket==='cloudy') return 'Overskyet';
  if(bucket==='rain') return 'Nedbør';
  return 'Natt';
}

// Kategoriser en enkelttimes "activity"-tekst til verdict-bøtte
function verdictBucket(activity, timeIso){
  const a=String(activity||'').toLowerCase();
  const h=new Date(timeIso).getHours();
  if(h<6 || h>=22) return 'night';
  if(a.includes('bra for')||a.includes('fint')||a.includes('flott')) return 'good';
  if(a.includes('krevende')||a.includes('storm')||a.includes('kraftig')||a.includes('unngå')) return 'bad';
  return 'ok';
}

function overallVerdict(hourly){
  // Bare prognose-timer (ikke historikk) og bare dagtimer (6-22)
  const day=hourly.filter(h=>{
    if(h.is_history) return false;
    const hr=new Date(h.time).getHours();
    return hr>=6 && hr<22;
  });
  const buckets={good:0,ok:0,bad:0};
  day.forEach(h=>{
    const b=verdictBucket(h.activity,h.time);
    if(b==='good') buckets.good++;
    else if(b==='bad') buckets.bad++;
    else if(b==='ok') buckets.ok++;
  });
  const total=buckets.good+buckets.ok+buckets.bad || 1;
  if(buckets.bad/total>0.3) return {cls:'bad',icon:'⚠️',text:'Krevende forhold – vurder inne'};
  if(buckets.good/total>0.4) return {cls:'good',icon:'☀️',text:'Bra for uteaktivitet'};
  return {cls:'warn',icon:'⛅',text:'Greit utevær – følg med'};
}

function dayCell(d){
  if(!d) return '–';
  const icon=(d.gust_max ?? 0) >= 15 ? '🌬️' : (d.rain_total ?? 0) >= 1.5 ? '🌧️' : '☀️';
  const maxTemp=d.temp_max ?? '–';
  return `${icon} ${maxTemp}°`;
}
function riskTag(d){
  const s=d.summary||{};
  if((s.max_gust_24h ?? 0) >= 16 || (s.rain_24h ?? 0) >= 12) return '<span class="risk high">🔴 Krevende</span>';
  if((s.max_gust_24h ?? 0) >= 11 || (s.rain_24h ?? 0) >= 5) return '<span class="risk mid">🟡 Følg med</span>';
  return '<span class="risk ok">🟢 Bra</span>';
}

// ============================================================
// Hovedrendering
// ============================================================
let mainChart=null;

async function loadForecast(lat,lon,name){
  setStatus('Henter varsel …');
  try{
    const r=await fetch(`/ver/api/aktivt-varsel?lat=${lat}&lon=${lon}&sted=${encodeURIComponent(name||'Valgt sted')}`);
    const d=await r.json();
    if(!r.ok){setStatus(d.error||'Feil');return;}
    renderData(d);
    cachePlace(d.coords?.lat ?? lat, d.coords?.lon ?? lon, d.sted || name || 'Valgt sted');
    setStatus('');
  }catch(e){
    setStatus('Kunne ikke hente varsel.');
  }
}

function renderData(d){
  document.getElementById('results').style.display='block';

  // Header
  document.getElementById('placeName').textContent=d.sted;
  document.getElementById('placeCoords').textContent=`${d.coords.lat.toFixed(4)}° N, ${d.coords.lon.toFixed(4)}° Ø`;
  document.getElementById('updated').textContent=`Oppdatert ${new Date(d.hentet).toLocaleString('no-NO',{hour:'2-digit',minute:'2-digit',day:'2-digit',month:'2-digit'})}`;

  // Verdict-banner
  const v=overallVerdict(d.hourly||[]);
  const banner=document.getElementById('verdictBanner');
  banner.className='verdict-banner '+v.cls;
  document.getElementById('verdictIcon').textContent=v.icon;
  document.getElementById('verdictText').textContent=v.text;

  // Kvalitet
  document.getElementById('quality').innerHTML=`<span class="quality-chip">Kvalitet ${d.quality.score}/5 · ${d.quality.label}</span><span>${d.quality.reason||''}</span>`;

  // KPI-bånd
  const s=d.summary;
  document.getElementById('kpiBand').innerHTML=`
    <div class="kpi-cell"><div class="kpi-lbl">Nåtemp</div><div class="kpi-val">${s.temp_now}°</div></div>
    <div class="kpi-cell"><div class="kpi-lbl">Min/maks 24t</div><div class="kpi-val">${s.temp_min_24h}° / ${s.temp_max_24h}°</div></div>
    <div class="kpi-cell"><div class="kpi-lbl">Nedbør 24t</div><div class="kpi-val">${s.rain_24h}<small> mm</small></div></div>
    <div class="kpi-cell"><div class="kpi-lbl">Maks vind</div><div class="kpi-val">${s.max_wind_24h}<small> m/s</small></div></div>
    <div class="kpi-cell"><div class="kpi-lbl">Maks kast</div><div class="kpi-val">${s.max_gust_24h}<small> m/s</small></div></div>
  `;

  // hourly inneholder døgnet i dag (00-23) med historikk + prognose.
  const hourly=(d.hourly||[]);
  const hourlyForward=hourly.filter(h=>!h.is_history);
  const strip=document.getElementById('verdictStrip');
  strip.innerHTML=hourly.map(h=>{
    const b=weatherStripBucket(h);
    const label=`kl ${new Date(h.time).getHours()}:00 – ${weatherStripLabel(b)}`;
    return `<div class="seg ${b}" title="${label}"></div>`;
  }).join('');

  drawMainChart(hourly);

  // Beste vinduer (rangert liste)
  const win=d.fine_windows||[];
  const winCard=document.getElementById('windowsCard');
  if(win.length){
    winCard.style.display='block';
    const maxHours=Math.max(...win.map(w=>w.hours),1);
    document.getElementById('windowsList').innerHTML=win.map(w=>{
      const day=new Date(w.start).toLocaleDateString('no-NO',{weekday:'short'});
      const range=`${fmtLocal(w.start,{hour:'2-digit',minute:'2-digit'})}–${fmtLocal(w.end,{hour:'2-digit',minute:'2-digit'})}`;
      const pct=Math.round(w.hours/maxHours*100);
      return `<div class="window-row">
        <div class="w-day">${day}</div>
        <div class="w-range">${range}</div>
        <div class="w-bar"><div class="w-fill" style="width:${pct}%"></div></div>
        <div class="w-hours">${w.hours} t</div>
      </div>`;
    }).join('');
  }else{
    winCard.style.display='block';
    document.getElementById('windowsList').innerHTML='<div class="muted" style="padding:8px 0">Ingen tydelige finværsvinduer funnet enda.</div>';
  }

  // 7-dagers strip
  const daily=d.daily||[];
  document.getElementById('dailyCard').style.display='block';
  // Lukk evt. åpen detalj-panel ved ny data-lasting
  document.getElementById('dayDetail').classList.remove('show');
  document.querySelectorAll('.daily-cell.active').forEach(el=>el.classList.remove('active'));

  document.getElementById('dailyGrid').innerHTML=daily.map((x,idx)=>{
    const day=new Date(x.date).toLocaleDateString('no-NO',{weekday:'short',day:'numeric',month:'short'});
    let icon='☀️';
    const sun=x.sun_hours ?? 0;
    if((x.gust_max??0)>=15) icon='🌬️';
    else if((x.rain_total??0)>=1.5) icon='🌧️';
    else if(sun<2) icon='☁️';
    else if(sun<5) icon='⛅';
    else icon='☀️';
    const sunStr=sun>0?`☀️ ${sun} soltimer`:`☁️ ingen sol`;
    return `<div class="daily-cell" data-day-idx="${idx}">
      <span class="d-arrow">▼</span>
      <div class="d-day">${day}</div>
      <div class="d-icon-temp"><span class="d-icon">${icon}</span><span class="d-temp">${x.temp_max??'–'}°</span></div>
      <div class="d-meta">${x.temp_min??'–'}° · ${x.rain_total} mm</div>
      <div class="d-sun">${sunStr}</div>
    </div>`;
  }).join('');

  // Klikk-handlere for daglig-celler
  document.querySelectorAll('.daily-cell').forEach(cell=>{
    cell.addEventListener('click',()=>{
      const idx=Number(cell.dataset.dayIdx);
      const wasActive=cell.classList.contains('active');
      document.querySelectorAll('.daily-cell.active').forEach(el=>el.classList.remove('active'));
      if(wasActive){
        document.getElementById('dayDetail').classList.remove('show');
        return;
      }
      cell.classList.add('active');
      showDayDetail(daily[idx]);
      const dayLabel=new Date(daily[idx].date).toLocaleDateString('no-NO',{weekday:'long',day:'numeric',month:'short'});
      renderHourlyTable(daily[idx].hours||[], dayLabel);
    });
  });

  // Timesoversikt (kompakt tabell, beholdt for de som vil ha detaljer)
  document.getElementById('hourlyCard').style.display='block';
  const defaultDay=daily[0];
  if(defaultDay){
    const label=new Date(defaultDay.date).toLocaleDateString('no-NO',{weekday:'long',day:'numeric',month:'short'});
    renderHourlyTable(defaultDay.hours||[], label);
  }else{
    renderHourlyTable(hourlyForward, 'neste døgn');
  }
}

function renderHourlyTable(hours,titleLabel){
  document.getElementById('hourlyTitle').textContent=`Detaljert timesoversikt – ${titleLabel}`;
  document.getElementById('hourlyBody').innerHTML=(hours||[]).map(h=>{
    const b=verdictBucket(h.activity,h.time);
    const rowClass=b==='good'?'row-good':(b==='night'?'row-night':'');
    const rainRange=(h.rain_min!=null&&h.rain_max!=null)?` <span class="muted">(${h.rain_min}–${h.rain_max})</span>`:'';
    const windBadge=`${windStrengthIcon(h.wind)} ${h.wind} m/s <span class="muted">(kast ${h.gust})</span>`;
    return `<tr class="${rowClass}">
      <td class="time-cell">${weatherEmoji(h.symbol)} ${fmtLocal(h.time,{weekday:'short',hour:'2-digit',minute:'2-digit'})}</td>
      <td class="num">${h.temp??'–'}°</td>
      <td class="num">${h.rain ?? 0} mm${rainRange}</td>
      <td class="num">${h.rain_prob??'–'}%</td>
      <td class="num">${windBadge}</td>
      <td>${windArrow(h.wind_deg)} ${h.wind_dir||'–'}</td>
      <td>${h.activity||''}</td>
    </tr>`;
  }).join('');
}

// ============================================================
// Dag-detaljer (expand-panel)
// ============================================================
let dayChart=null;

function showDayDetail(day){
  const panel=document.getElementById('dayDetail');
  const title=new Date(day.date).toLocaleDateString('no-NO',{weekday:'long',day:'numeric',month:'long'});
  document.getElementById('dayDetailTitle').textContent=title;

  // Statistikk-kort
  document.getElementById('dayStats').innerHTML=`
    <div class="day-stat"><div class="ds-lbl">Temperatur</div><div class="ds-val">${day.temp_min}° – ${day.temp_max}°</div></div>
    <div class="day-stat"><div class="ds-lbl">Nedbør totalt</div><div class="ds-val">${day.rain_total}<small> mm</small></div></div>
    <div class="day-stat"><div class="ds-lbl">Soltimer</div><div class="ds-val">${day.sun_hours ?? 0}<small> t</small></div></div>
    <div class="day-stat"><div class="ds-lbl">Maks vind</div><div class="ds-val">${day.wind_max ?? '–'}<small> m/s</small></div></div>
    <div class="day-stat"><div class="ds-lbl">Maks kast</div><div class="ds-val">${day.gust_max ?? '–'}<small> m/s</small></div></div>
  `;

  const sym=(day.hours||[]).map(h=>{
    const hour=String(new Date(h.time).getHours()).padStart(2,'0');
    return `<div class="sym-chip" title="kl. ${hour}:00 · ${h.wind} m/s (${h.gust} kast)">
      <div class="t">${hour}</div>
      <div class="wx">${weatherEmoji(h.symbol)}</div>
      <div class="w"><span class="dir">${windArrow(h.wind_deg)}</span><span>${h.wind ?? '–'} m/s</span></div>
    </div>`;
  }).join('');
  document.getElementById('daySymbols').innerHTML=sym;

  // Beste 6-timers blokk
  const b6=day.best_6h;
  const banner=document.getElementById('best6Banner');
  if(b6){
    const sh=String(b6.start_hour).padStart(2,'0');
    const eh=String(b6.end_hour).padStart(2,'0');
    banner.innerHTML=`🎯 <strong>Beste 6-timers vindu:</strong> kl. ${sh}:00–${eh}:00 · snitt ${b6.avg_temp}°, ${b6.total_rain} mm nedbør, vind opp til ${b6.max_wind} m/s`;
    banner.style.display='flex';
  }else{
    banner.style.display='none';
  }

  // Timesgraf for dagen
  drawDayChart(day.hours||[], b6);

  panel.classList.add('show');
  panel.scrollIntoView({behavior:'smooth',block:'nearest'});
}

function drawDayChart(hours,b6){
  const labels=hours.map(h=>String(new Date(h.time).getHours()).padStart(2,'0'));
  const temp=hours.map(h=>h.temp);
  const rain=hours.map(h=>h.rain ?? 0);
  const wind=hours.map(h=>h.wind ?? 0);

  // Custom plugin: marker beste 6t-blokk med grønn bakgrunn
  let startIdx=-1;
  if(b6){
    startIdx=hours.findIndex(h=>new Date(h.time).getHours()===b6.start_hour);
  }
  const best6Plugin={
    id:'best6Box',
    beforeDatasetsDraw(chart){
      if(startIdx<0) return;
      const xScale=chart.scales.x;
      const yArea=chart.chartArea;
      const x1=xScale.getPixelForValue(startIdx)-((xScale.getPixelForValue(1)-xScale.getPixelForValue(0))/2);
      const x2=xScale.getPixelForValue(startIdx+5)+((xScale.getPixelForValue(1)-xScale.getPixelForValue(0))/2);
      const ctx=chart.ctx;
      ctx.save();
      ctx.fillStyle='rgba(134,239,172,0.25)';
      ctx.fillRect(x1, yArea.top, x2-x1, yArea.bottom-yArea.top);
      ctx.restore();
    }
  };

  const ctx=document.getElementById('dayChart').getContext('2d');
  if(dayChart) dayChart.destroy();

  dayChart=new Chart(ctx,{
    plugins:[best6Plugin],
    data:{
      labels:labels,
      datasets:[
        {
          type:'bar',
          label:'Nedbør',
          data:rain,
          backgroundColor:'rgba(37,99,235,0.75)',
          yAxisID:'yRain',
          order:2,
          barPercentage:0.9,
          categoryPercentage:0.9
        },
        {
          type:'line',
          label:'Temperatur',
          data:temp,
          borderColor:'rgba(234,88,12,1)',
          backgroundColor:'rgba(234,88,12,0.08)',
          borderWidth:2,
          tension:0.35,
          fill:true,
          pointRadius:0,
          pointHoverRadius:3,
          yAxisID:'yTemp',
          order:1
        },
        {
          type:'line',
          label:'Vind',
          data:wind,
          borderColor:'rgba(2,132,199,0.9)',
          backgroundColor:'rgba(2,132,199,0.05)',
          borderWidth:1.8,
          tension:0.25,
          fill:false,
          pointRadius:0,
          pointHoverRadius:3,
          yAxisID:'yWind',
          order:0
        }
      ]
    },
    options:{
      responsive:true,
      maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:false},
        tooltip:{
          callbacks:{
            title:(items)=>`kl. ${items[0].label}:00`,
            label:(ctx)=>{
              if(ctx.dataset.label==='Temperatur') return `Temp: ${ctx.parsed.y?.toFixed(1)}°`;
              if(ctx.dataset.label==='Nedbør') return `Regn: ${ctx.parsed.y?.toFixed(1)} mm`;
              if(ctx.dataset.label==='Vind') return `Vind: ${ctx.parsed.y?.toFixed(1)} m/s`;
              return '';
            }
          }
        }
      },
      scales:{
        x:{grid:{display:false},ticks:{maxRotation:0,autoSkip:true,maxTicksLimit:12,font:{size:10}}},
        yTemp:{position:'left',grid:{color:'rgba(0,0,0,0.05)'},ticks:{font:{size:10},callback:(v)=>v+'°'}},
        yRain:{position:'right',offset:true,grid:{display:false},min:0,suggestedMax:2,ticks:{font:{size:10}}},
        yWind:{position:'right',grid:{display:false},min:0,suggestedMax:12,ticks:{font:{size:10},callback:(v)=>v+' m/s'}}
      }
    }
  });
}

document.getElementById('dayDetailClose').addEventListener('click',()=>{
  document.getElementById('dayDetail').classList.remove('show');
  document.querySelectorAll('.daily-cell.active').forEach(el=>el.classList.remove('active'));
});

// ============================================================
// Hovedgraf (dagens døgn)
// ============================================================
function drawMainChart(hourly){
  const labels=hourly.map(h=>{
    const dt=new Date(h.time);
    return dt.getHours().toString().padStart(2,'0');
  });
  // Finn indeks for "nå" – siste historiske time + 0.5 (eller første prognose)
  let nowIdx=-1;
  for(let i=0;i<hourly.length;i++){
    if(!hourly[i].is_history){ nowIdx=i; break; }
  }
  if(nowIdx<0) nowIdx=hourly.length;

  // Data-arrays: splitt temp i historikk vs prognose (to datasets for visuelt skille)
  const tempHist=hourly.map((h,i)=>h.is_history ? h.temp : null);
  const tempFcst=hourly.map((h,i)=>{
    if(h.is_history) return null;
    // Inkluder siste historiske punkt for å koble linjene
    return h.temp;
  });
  // Knytt linjene sammen: duplisér overgangs-punktet
  if(nowIdx>0 && nowIdx<hourly.length){
    tempHist[nowIdx]=hourly[nowIdx-1].temp;
  }

  const rainExp=hourly.map(h=>h.rain ?? 0);
  const rainUnc=hourly.map(h=>{
    const mx=h.rain_max ?? h.rain ?? 0;
    const exp=h.rain ?? 0;
    return Math.max(0, mx - exp);
  });
  const wind=hourly.map(h=>h.wind ?? 0);

  // Custom plugin: tegn vertikal "nå"-linje
  const nowLinePlugin={
    id:'nowLine',
    afterDraw(chart){
      if(nowIdx<=0 || nowIdx>=hourly.length) return;
      const xScale=chart.scales.x;
      const yArea=chart.chartArea;
      // Plasser linjen mellom nowIdx-1 og nowIdx (dvs. ved grensen)
      const x=xScale.getPixelForValue(nowIdx)-((xScale.getPixelForValue(nowIdx)-xScale.getPixelForValue(nowIdx-1))/2);
      const ctx=chart.ctx;
      ctx.save();
      ctx.strokeStyle='rgba(220,38,38,0.65)';
      ctx.lineWidth=1.5;
      ctx.setLineDash([4,3]);
      ctx.beginPath();
      ctx.moveTo(x,yArea.top);
      ctx.lineTo(x,yArea.bottom);
      ctx.stroke();
      // "NÅ"-label
      ctx.setLineDash([]);
      ctx.fillStyle='rgba(220,38,38,0.85)';
      ctx.font='600 10px -apple-system, sans-serif';
      ctx.textAlign='center';
      ctx.fillText('NÅ', x, yArea.top+10);
      ctx.restore();
    }
  };

  const ctx=document.getElementById('mainChart').getContext('2d');
  if(mainChart) mainChart.destroy();

  mainChart=new Chart(ctx,{
    plugins:[nowLinePlugin],
    data:{
      labels:labels,
      datasets:[
        {
          type:'bar',
          label:'Usikkerhet (maks)',
          data:rainUnc,
          backgroundColor:(ctx)=>ctx.dataIndex<nowIdx?'rgba(147,197,253,0.3)':'rgba(147,197,253,0.6)',
          yAxisID:'yRain',
          stack:'precip',
          order:3,
          barPercentage:1.0,
          categoryPercentage:0.85
        },
        {
          type:'bar',
          label:'Nedbør (forventet)',
          data:rainExp,
          backgroundColor:(ctx)=>ctx.dataIndex<nowIdx?'rgba(37,99,235,0.45)':'rgba(37,99,235,0.85)',
          yAxisID:'yRain',
          stack:'precip',
          order:2,
          barPercentage:1.0,
          categoryPercentage:0.85
        },
        {
          type:'line',
          label:'Vind (m/s)',
          data:wind,
          borderColor:'rgba(100,116,139,0.8)',
          borderDash:[3,3],
          borderWidth:1.5,
          pointRadius:0,
          pointHoverRadius:3,
          tension:0.3,
          fill:false,
          yAxisID:'yWind',
          order:1,
          spanGaps:false
        },
        {
          type:'line',
          label:'Temperatur (historikk)',
          data:tempHist,
          borderColor:'rgba(234,88,12,0.45)',
          backgroundColor:'rgba(234,88,12,0.04)',
          borderWidth:2,
          borderDash:[2,3],
          tension:0.35,
          fill:true,
          pointRadius:0,
          pointHoverRadius:4,
          yAxisID:'yTemp',
          order:0,
          spanGaps:false
        },
        {
          type:'line',
          label:'Temperatur',
          data:tempFcst,
          borderColor:'rgba(234,88,12,1)',
          backgroundColor:'rgba(234,88,12,0.08)',
          borderWidth:2.5,
          tension:0.35,
          fill:true,
          pointRadius:0,
          pointHoverRadius:4,
          yAxisID:'yTemp',
          order:0,
          spanGaps:false
        }
      ]
    },
    options:{
      responsive:true,
      maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:false},
        tooltip:{
          filter:(item)=>item.dataset.label!=='Temperatur (historikk)' || item.raw!=null,
          callbacks:{
            title:(items)=>{
              const i=items[0].dataIndex;
              const prefix=i<nowIdx?'(tidligere) ':'';
              return `${prefix}kl. ${items[0].label}:00`;
            },
            label:(ctx)=>{
              if(ctx.dataset.label==='Temperatur'||ctx.dataset.label==='Temperatur (historikk)'){
                if(ctx.parsed.y==null) return null;
                return `Temp: ${ctx.parsed.y?.toFixed(1)}°`;
              }
              if(ctx.dataset.label==='Nedbør (forventet)') return `Regn: ${ctx.parsed.y?.toFixed(1)} mm`;
              if(ctx.dataset.label==='Usikkerhet (maks)'){
                const tot=ctx.parsed.y+(ctx.chart.data.datasets[1].data[ctx.dataIndex]||0);
                return `Maks regn: ${tot.toFixed(1)} mm`;
              }
              if(ctx.dataset.label==='Vind (m/s)') return `Vind: ${ctx.parsed.y?.toFixed(1)} m/s`;
              return null;
            }
          }
        }
      },
      scales:{
        x:{
          grid:{display:false},
          ticks:{maxRotation:0,autoSkip:true,maxTicksLimit:12,font:{size:11}}
        },
        yTemp:{
          position:'left',
          title:{display:true,text:'°C',font:{size:11}},
          grid:{color:'rgba(0,0,0,0.06)'},
          ticks:{font:{size:11},callback:(v)=>v+'°'}
        },
        yRain:{
          position:'right',
          title:{display:true,text:'mm',font:{size:11}},
          grid:{display:false},
          min:0,
          suggestedMax:2,
          ticks:{font:{size:11}}
        },
        yWind:{
          position:'right',
          offset:true,
          title:{display:true,text:'m/s',font:{size:11}},
          grid:{display:false},
          min:0,
          suggestedMax:12,
          ticks:{font:{size:11}}
        }
      }
    }
  });
}

// ============================================================
// Kollapsbar "Rask oversikt" – laster først når man åpner
// ============================================================
let overviewLoaded=false;
document.querySelector('.overview-card').addEventListener('toggle',(e)=>{
  if(e.target.open && !overviewLoaded){
    overviewLoaded=true;
    loadOverview();
  }
});

async function loadOverview(){
  overviewBody.innerHTML='<tr><td colspan="7" class="muted">Laster oversikt …</td></tr>';
  const rows=await Promise.all(overviewPlaces.map(async(p)=>{
    try{
      const r=await fetch(`/ver/api/aktivt-varsel?lat=${p.lat}&lon=${p.lon}&sted=${encodeURIComponent(p.name)}`);
      const d=await r.json();
      if(!r.ok) throw new Error(d.error||'Feil');
      return {ok:true,p,d};
    }catch(_){
      return {ok:false,p};
    }
  }));
  overviewBody.innerHTML=rows.map(x=>{
    if(!x.ok){
      return `<tr><td>${x.p.name}</td><td colspan="5" class="muted">Kunne ikke hente varsel</td><td><span class="risk mid">⚪ Ukjent</span></td></tr>`;
    }
    const d=x.d;
    const daily=d.daily||[];
    return `<tr>
      <td><strong>${d.sted}</strong></td>
      <td class="temp">${d.summary.temp_now}°C</td>
      <td class="day">${dayCell(daily[0])}</td>
      <td class="day">${dayCell(daily[1])}</td>
      <td class="day">${dayCell(daily[2])}</td>
      <td class="day">${dayCell(daily[3])}</td>
      <td>${riskTag(d)}</td>
    </tr>`;
  }).join('');
}

// ============================================================
// Søk + geolokasjon
// ============================================================
const PLACE_CACHE_KEY='aktivt_varsel_last_place_v1';
const BERGEN_DEFAULT={name:'Bergen, Norge',lat:60.39299,lon:5.32415};

function cachePlace(lat,lon,name){
  try{
    localStorage.setItem(PLACE_CACHE_KEY,JSON.stringify({lat:Number(lat),lon:Number(lon),name:String(name||'Valgt sted')}));
  }catch(_){}
}

function loadCachedPlace(){
  try{
    const raw=localStorage.getItem(PLACE_CACHE_KEY);
    if(!raw) return null;
    const p=JSON.parse(raw);
    if(typeof p?.lat!=='number' || typeof p?.lon!=='number') return null;
    return {lat:p.lat,lon:p.lon,name:p.name||'Valgt sted'};
  }catch(_){
    return null;
  }
}

async function searchPlace(){
  const q=placeInput.value.trim();
  if(q.length<2){setStatus('Skriv minst 2 tegn for stedsøk.');return;}
  setStatus('Søker sted …');
  try{
    const r=await fetch(`/ver/api/sted-sok?q=${encodeURIComponent(q)}`);
    const d=await r.json();
    if(!r.ok){throw new Error(d.error||'Stedsøk feilet');}
    const items=d.items||[];
    suggestionsEl.innerHTML=items.map(it=>`<button class="chip" data-lat="${it.lat}" data-lon="${it.lon}" data-name="${it.name.replace(/"/g,'&quot;')}">${it.name}</button>`).join('');
    if(!items.length){setStatus('Fant ingen treff.');}
    else{
      setStatus('');
      // Velg første treff automatisk når det kun er ett resultat.
      if(items.length===1){
        const it=items[0];
        suggestionsEl.innerHTML='';
        loadForecast(Number(it.lat),Number(it.lon),it.name);
      }
    }
  }catch(_){
    setStatus('Kunne ikke søke opp sted akkurat nå.');
  }
}

document.getElementById('searchBtn').addEventListener('click',searchPlace);
placeInput.addEventListener('keydown',(e)=>{if(e.key==='Enter') searchPlace();});
suggestionsEl.addEventListener('click',(e)=>{
  const b=e.target.closest('button[data-lat]'); if(!b) return;
  suggestionsEl.innerHTML='';
  loadForecast(Number(b.dataset.lat),Number(b.dataset.lon),b.dataset.name);
});
document.getElementById('geoBtn').addEventListener('click',()=>{
  if(!navigator.geolocation){setStatus('Nettleseren støtter ikke posisjon.'); return;}
  setStatus('Henter posisjon …');
  navigator.geolocation.getCurrentPosition(
    (pos)=>loadForecast(pos.coords.latitude,pos.coords.longitude,'Min posisjon'),
    ()=>setStatus('Kunne ikke hente posisjon.')
  );
});

// Auto-hent posisjon ved sidelast
window.addEventListener('load',()=>{
  const cached=loadCachedPlace();
  if(cached){
    loadForecast(cached.lat,cached.lon,cached.name);
    return;
  }
  loadForecast(BERGEN_DEFAULT.lat,BERGEN_DEFAULT.lon,BERGEN_DEFAULT.name);
});
</script>
</body>
</html>"""

# =========================
# Klima-topplister (precomputet cache)
# =========================
_LEADERBOARD_LOCK = threading.Lock()
_LEADERBOARD_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "weather_topplister_cache.json"
_LEADERBOARD_TTL_SECONDS = int(os.getenv("WEATHER_LEADERBOARD_TTL_SECONDS", "3600"))
_LEADERBOARD_STATION_LIMIT = int(os.getenv("WEATHER_LEADERBOARD_STATION_LIMIT", "220"))
_LEADERBOARD_TOP_N = int(os.getenv("WEATHER_LEADERBOARD_TOP_N", "5"))


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _frost_observations_aggregate(
    session: requests.Session,
    station_ids: list[str],
    *,
    element: str,
    referencetime: str,
    timeout: int = 25,
    batch_size: int = 60,
) -> pd.DataFrame:
    auth = _env_auth()
    rows: list[dict[str, Any]] = []
    for i in range(0, len(station_ids), batch_size):
        batch = station_ids[i: i + batch_size]
        params = {
            "sources": ",".join(batch),
            "referencetime": referencetime,
            "elements": element,
            "timeoffsets": "default",
            "levels": "default",
            "limit": 1000,
            "qualities": "0,1,2,3,4",
        }
        try:
            resp = session.get(
                "https://frost.met.no/observations/v0.jsonld",
                params=params,
                auth=(auth.client_id, auth.client_secret),
                headers={"Accept": "application/json"},
                timeout=timeout,
            )
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except Exception:
            continue

        for item in payload.get("data", []) or []:
            source_id = str(item.get("sourceId") or "").split(":")[0]
            for obs in item.get("observations", []) or []:
                val = _as_float(obs.get("value"))
                if val is None:
                    continue
                rows.append(
                    {
                        "sourceId": source_id,
                        "value": val,
                        "referenceTime": obs.get("referenceTime") or item.get("referenceTime"),
                    }
                )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["referenceTime"] = pd.to_datetime(out["referenceTime"], errors="coerce", utc=True)
    out = out.dropna(subset=["sourceId", "value"]).copy()
    return out.sort_values(["sourceId", "referenceTime"]).groupby("sourceId", as_index=False).tail(1)


def _join_station_meta(df: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep_cols = [c for c in ["baseId", "name", "county", "lat", "lon"] if c in stations.columns]
    m = stations[keep_cols].copy().rename(columns={"baseId": "sourceId"})
    out = df.merge(m, on="sourceId", how="left")
    if "lat" not in out.columns:
        if "lat_y" in out.columns:
            out["lat"] = out["lat_y"]
        elif "lat_x" in out.columns:
            out["lat"] = out["lat_x"]
    if "lon" not in out.columns:
        if "lon_y" in out.columns:
            out["lon"] = out["lon_y"]
        elif "lon_x" in out.columns:
            out["lon"] = out["lon_x"]
    return out.dropna(subset=["lat", "lon"]).copy()


def _rank_rows(df: pd.DataFrame, *, top_n: int, asc: bool) -> list[dict[str, Any]]:
    if df.empty:
        return []
    ranked = df.sort_values("value", ascending=asc).head(top_n).copy()
    rows: list[dict[str, Any]] = []
    for _, row in ranked.iterrows():
        rows.append(
            {
                "station_id": str(row.get("sourceId") or ""),
                "name": str(row.get("name") or row.get("sourceId") or "Ukjent"),
                "county": str(row.get("county") or ""),
                "lat": round(float(row.get("lat")), 5),
                "lon": round(float(row.get("lon")), 5),
                "value": round(float(row.get("value")), 2),
            }
        )
    return rows


def _fetch_forecast_wind(stations: pd.DataFrame, *, hours: int) -> pd.DataFrame:
    if stations.empty:
        return pd.DataFrame()

    start_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = start_utc + pd.Timedelta(hours=hours)

    def _one_station(row: dict[str, Any]) -> Optional[dict[str, Any]]:
        lat = _as_float(row.get("lat"))
        lon = _as_float(row.get("lon"))
        if lat is None or lon is None:
            return None
        try:
            r = requests.get(
                "https://api.met.no/weatherapi/locationforecast/2.0/complete",
                params={"lat": round(lat, 4), "lon": round(lon, 4)},
                headers={"User-Agent": "prisanalyse.no/1.0 kontakt@prisanalyse.no"},
                timeout=12,
            )
            if r.status_code != 200:
                return None
            ts = r.json().get("properties", {}).get("timeseries", [])
            vals: list[float] = []
            for item in ts:
                try:
                    t = datetime.fromisoformat(str(item.get("time", "")).replace("Z", "+00:00"))
                except Exception:
                    continue
                if t < start_utc or t >= end_utc:
                    continue
                inst = item.get("data", {}).get("instant", {}).get("details", {})
                w = _as_float(inst.get("wind_speed"))
                if w is not None:
                    vals.append(w)
            if not vals:
                return None
            return {
                "sourceId": str(row.get("baseId") or ""),
                "value": max(vals),
            }
        except Exception:
            return None

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(_one_station, row) for row in stations.to_dict("records")]
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                rows.append(res)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _build_weather_topplister_payload() -> dict[str, Any]:
    stations = load_station_db().copy()
    if stations.empty:
        raise RuntimeError("Stasjonsdatabase er tom")

    stations = stations.dropna(subset=["baseId", "lat", "lon"]).copy()
    stations = stations.head(max(50, _LEADERBOARD_STATION_LIMIT))
    station_ids = stations["baseId"].astype(str).tolist()

    with requests.Session() as session:
        # Historiske aggregater fra Frost
        sun_day = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(duration_of_sunshine P1D)", referencetime="latest"),
            stations,
        )
        sun_month = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(duration_of_sunshine P1M)", referencetime="latest"),
            stations,
        )
        sun_year = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(duration_of_sunshine P1Y)", referencetime="latest"),
            stations,
        )

        rain_day = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(precipitation_amount P1D)", referencetime="latest"),
            stations,
        )
        rain_month = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(precipitation_amount P1M)", referencetime="latest"),
            stations,
        )
        rain_year = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="sum(precipitation_amount P1Y)", referencetime="latest"),
            stations,
        )

        temp_day = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(air_temperature P1D)", referencetime="latest"),
            stations,
        )
        temp_month = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(air_temperature P1M)", referencetime="latest"),
            stations,
        )
        temp_year = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(air_temperature P1Y)", referencetime="latest"),
            stations,
        )

        wind_day = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(wind_speed P1D)", referencetime="latest"),
            stations,
        )
        wind_month = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(wind_speed P1M)", referencetime="latest"),
            stations,
        )
        wind_year = _join_station_meta(
            _frost_observations_aggregate(session, station_ids, element="max(wind_speed P1Y)", referencetime="latest"),
            stations,
        )

    # Prognoser fra Yr
    fc_sun_24 = fetch_sunshine_forecast(stations, mode="next24h")
    fc_sun_7d = fetch_sunshine_forecast(stations, mode="next7d")
    fc_rain_24 = fetch_precip_forecast(stations, mode="next24h")
    fc_rain_7d = fetch_precip_forecast(stations, mode="next7d")
    fc_temp_24 = fetch_temp_forecast(stations, mode="next24h", temp_kind="max")
    fc_temp_7d = fetch_temp_forecast(stations, mode="next7d", temp_kind="max")
    fc_wind_24 = _fetch_forecast_wind(stations, hours=24)
    fc_wind_7d = _fetch_forecast_wind(stations, hours=168)

    fc_sun_24 = _join_station_meta(fc_sun_24.rename(columns={"sun_hours": "value"}), stations)
    fc_sun_7d = _join_station_meta(fc_sun_7d.rename(columns={"sun_hours": "value"}), stations)
    fc_rain_24 = _join_station_meta(fc_rain_24, stations)
    fc_rain_7d = _join_station_meta(fc_rain_7d, stations)
    fc_temp_24 = _join_station_meta(fc_temp_24, stations)
    fc_temp_7d = _join_station_meta(fc_temp_7d, stations)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": _LEADERBOARD_TTL_SECONDS,
        "station_count": int(len(stations)),
        "top_n": _LEADERBOARD_TOP_N,
        "metrics": {
            "sun": {
                "unit": "timer",
                "best": {
                    "year": _rank_rows(sun_year, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "month": _rank_rows(sun_month, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "day": _rank_rows(sun_day, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next24h": _rank_rows(fc_sun_24, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next7d": _rank_rows(fc_sun_7d, top_n=_LEADERBOARD_TOP_N, asc=False),
                },
            },
            "precip": {
                "unit": "mm",
                "max": {
                    "year": _rank_rows(rain_year, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "month": _rank_rows(rain_month, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "day": _rank_rows(rain_day, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next24h": _rank_rows(fc_rain_24, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next7d": _rank_rows(fc_rain_7d, top_n=_LEADERBOARD_TOP_N, asc=False),
                },
                "min": {
                    "year": _rank_rows(rain_year, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "month": _rank_rows(rain_month, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "day": _rank_rows(rain_day, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next24h": _rank_rows(fc_rain_24, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next7d": _rank_rows(fc_rain_7d, top_n=_LEADERBOARD_TOP_N, asc=True),
                },
            },
            "temperature_max": {
                "unit": "°C",
                "min": {
                    "year": _rank_rows(temp_year, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "month": _rank_rows(temp_month, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "day": _rank_rows(temp_day, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next24h": _rank_rows(fc_temp_24, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next7d": _rank_rows(fc_temp_7d, top_n=_LEADERBOARD_TOP_N, asc=True),
                },
                "max": {
                    "year": _rank_rows(temp_year, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "month": _rank_rows(temp_month, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "day": _rank_rows(temp_day, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next24h": _rank_rows(fc_temp_24, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next7d": _rank_rows(fc_temp_7d, top_n=_LEADERBOARD_TOP_N, asc=False),
                },
            },
            "wind": {
                "unit": "m/s",
                "max": {
                    "year": _rank_rows(wind_year, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "month": _rank_rows(wind_month, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "day": _rank_rows(wind_day, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next24h": _rank_rows(fc_wind_24, top_n=_LEADERBOARD_TOP_N, asc=False),
                    "next7d": _rank_rows(fc_wind_7d, top_n=_LEADERBOARD_TOP_N, asc=False),
                },
                "min": {
                    "year": _rank_rows(wind_year, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "month": _rank_rows(wind_month, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "day": _rank_rows(wind_day, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next24h": _rank_rows(fc_wind_24, top_n=_LEADERBOARD_TOP_N, asc=True),
                    "next7d": _rank_rows(fc_wind_7d, top_n=_LEADERBOARD_TOP_N, asc=True),
                },
            },
        },
    }
    return payload


def _cache_is_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) < ttl_seconds


def _read_cached_topplister() -> Optional[dict[str, Any]]:
    try:
        if not _LEADERBOARD_CACHE_PATH.exists():
            return None
        with _LEADERBOARD_CACHE_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _save_cached_topplister(payload: dict[str, Any]) -> None:
    _LEADERBOARD_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LEADERBOARD_CACHE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)


def _get_or_build_topplister(force: bool = False) -> dict[str, Any]:
    with _LEADERBOARD_LOCK:
        if not force and _cache_is_fresh(_LEADERBOARD_CACHE_PATH, _LEADERBOARD_TTL_SECONDS):
            cached = _read_cached_topplister()
            if cached:
                return cached

        payload = _build_weather_topplister_payload()
        _save_cached_topplister(payload)
        return payload


@ver.route("/api/klima-topplister")
def klima_topplister_api() -> Response:
    force = request.args.get("force", "").strip().lower() in {"1", "true", "yes"}
    try:
        payload = _get_or_build_topplister(force=force)
        payload["cache_file"] = str(_LEADERBOARD_CACHE_PATH)
        payload["cache_fresh"] = _cache_is_fresh(_LEADERBOARD_CACHE_PATH, _LEADERBOARD_TTL_SECONDS)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@ver.route("/klima-toppliste")
def klima_toppliste_page() -> str:
    return """<!doctype html>
<html lang=\"no\"><head>
<meta charset=\"utf-8\"/>
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
<title>Klima-topplister</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;background:#0b1220;color:#e2e8f0;margin:0;padding:0}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
a{color:#93c5fd}
.muted{color:#94a3b8;font-size:13px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.card{background:#0f172a;border:1px solid #1e293b;border-radius:14px;padding:16px}
.card h2{margin:0 0 10px;font-size:18px}
.card h3{margin:12px 0 8px;font-size:13px;color:#93c5fd;text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px 6px;border-bottom:1px solid #1e293b;text-align:left}
tr:last-child td{border-bottom:none}
.k{font-size:12px;color:#64748b;text-transform:uppercase}
@media(max-width:980px){.grid{grid-template-columns:1fr}}
</style></head>
<body>
<div class=\"wrap\">
  <p><a href=\"/ver/\">← Tilbake til værhub</a></p>
  <h1>Klima-topplister</h1>
  <p class=\"muted\">Preberegnes automatisk (standard: hver time) og lagres i filcache, slik at siden åpner raskt.</p>
  <p id=\"meta\" class=\"muted\">Laster data …</p>
  <div class=\"grid\" id=\"grid\"></div>
</div>
<script>
function fmt(v){return (v??'–');}
function placeCell(r){
  if(!r) return '–';
  const q = encodeURIComponent(`${r.lat},${r.lon}`);
  return `<a href=\"/ver/aktivt-varsel?lat=${r.lat}&lon=${r.lon}&sted=${encodeURIComponent(r.name)}\">${r.name}</a><div class=\"muted\">${r.county||''}</div><div class=\"muted\"><a target=\"_blank\" href=\"https://www.google.com/maps/search/?api=1&query=${q}\">Kart</a></div>`;
}
function table(title, periods, unit){
  const labels={year:'I år',month:'Siste måned',day:'Siste døgn',next24h:'Neste døgn',next7d:'Neste 7 døgn'};
  let html = `<h3>${title}</h3><table><thead><tr><th>Periode</th><th>Sted</th><th>Verdi (${unit})</th></tr></thead><tbody>`;
  for(const key of ['year','month','day','next24h','next7d']){
    const row=(periods[key]||[])[0];
    html += `<tr><td class=\"k\">${labels[key]}</td><td>${placeCell(row)}</td><td>${fmt(row?.value)}</td></tr>`;
  }
  return html + '</tbody></table>';
}
fetch('/ver/api/klima-topplister').then(r=>r.json()).then(d=>{
  if(d.error){document.getElementById('meta').textContent='Feil: '+d.error;return;}
  document.getElementById('meta').textContent=`Oppdatert: ${new Date(d.generated_at).toLocaleString('nb-NO')} · stasjoner: ${d.station_count} · topp ${d.top_n}`;
  const m=d.metrics||{};
  const cards=[];
  cards.push(`<div class=\"card\"><h2>☀️ Sol</h2>${table('Mest sol', m.sun?.best||{}, m.sun?.unit||'timer')}</div>`);
  cards.push(`<div class=\"card\"><h2>🌧️ Nedbør</h2>${table('Mest nedbør', m.precip?.max||{}, m.precip?.unit||'mm')}${table('Minst nedbør', m.precip?.min||{}, m.precip?.unit||'mm')}</div>`);
  cards.push(`<div class=\"card\"><h2>🌡️ Høy temperatur</h2>${table('Mest varme', m.temperature_max?.max||{}, m.temperature_max?.unit||'°C')}${table('Lav høy temperatur', m.temperature_max?.min||{}, m.temperature_max?.unit||'°C')}</div>`);
  cards.push(`<div class=\"card\"><h2>💨 Vind</h2>${table('Mest vind', m.wind?.max||{}, m.wind?.unit||'m/s')}${table('Minst vind', m.wind?.min||{}, m.wind?.unit||'m/s')}</div>`);
  document.getElementById('grid').innerHTML=cards.join('');
}).catch(err=>{document.getElementById('meta').textContent='Kunne ikke laste data: '+err;});
</script>
</body></html>"""
