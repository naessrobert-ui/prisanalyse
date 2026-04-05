from __future__ import annotations

import math
import time
import traceback
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timezone
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
    if mode not in {"last24h", "day", "mtd", "ytd"}: mode = "last24h"
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
      <select id="mode-select"><option value="last24h" selected>Siste 24 timer</option><option value="day">Kalenderdøgn</option><option value="mtd">Hittil i måneden</option><option value="ytd">Hittil i året</option></select>
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
document.getElementById("controls-form").addEventListener("submit",e=>{{e.preventDefault();frame.src=buildUrl();}});
modeSelect.addEventListener("change",()=>{{frame.src=buildUrl();}});
</script></body></html>"""


@ver.route("/solskinn-kart")
def solskinn_kart() -> str:
    mode = request.args.get("mode", "last24h")
    if mode not in {"last24h", "day", "mtd", "ytd"}: mode = "last24h"
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
	        <option value="peak">Høyeste timesverdi</option>
	        <option value="gust" selected>Maks vindkast</option>
      </select>
      <button type="submit">Vis</button>
      <button type="button" id="btn-reset">Reset kart</button>
    </form>
  </div>
	  <iframe id="map-frame" src="/ver/vind-kart?mode=observed&period=hour&metric=gust" height="700"></iframe>
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
function buildUrl(){{
  const qs=new URLSearchParams();
  qs.set("mode",modeSelect.value||"observed");
  qs.set("region",regionSelect.value||"all");
  qs.set("period",periodSelect.value||"hour");
  qs.set("forecast_hours",forecastHoursSelect.value||"24");
	  qs.set("metric",metricSelect.value||"gust");
  qs.set("date",dateInput.value||"{today_str}");
  const s=readSaved();
  if(s){{if(s.bbox)qs.set("bbox",s.bbox);if(s.z)qs.set("z",s.z);if(s.clat)qs.set("clat",s.clat);if(s.clon)qs.set("clon",s.clon);}}
  return "/ver/vind-kart?"+qs.toString();
}}
frame.addEventListener("load",saveFU);
modeSelect.addEventListener("change",()=>{{const fc = modeSelect.value === "forecast";periodSelect.disabled = fc;forecastHoursSelect.disabled = !fc; frame.src=buildUrl();}});
regionSelect.addEventListener("change",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}frame.src=buildUrl();}});
periodSelect.addEventListener("change",()=>{{frame.src=buildUrl();}});
forecastHoursSelect.addEventListener("change",()=>{{frame.src=buildUrl();}});
metricSelect.addEventListener("change",()=>{{frame.src=buildUrl();}});
document.getElementById("controls-form").addEventListener("submit",e=>{{e.preventDefault();frame.src=buildUrl();}});
	document.getElementById("btn-reset").addEventListener("click",()=>{{try{{sessionStorage.removeItem(STORE_KEY);}}catch(e){{}}frame.src="/ver/vind-kart?mode="+(modeSelect.value||"observed")+"&region="+(regionSelect.value||"all")+"&period="+(periodSelect.value||"hour")+"&forecast_hours="+(forecastHoursSelect.value||"24")+"&metric="+(metricSelect.value||"gust")+"&date="+(dateInput.value||"{today_str}");}});
periodSelect.disabled = false; forecastHoursSelect.disabled = true;
</script></body></html>"""


@ver.route("/vind-kart")
def vind_kart() -> str:
    mode = request.args.get("mode", "observed")
    period = request.args.get("period", "hour")
    metric = request.args.get("metric", "gust")
    forecast_hours = int(request.args.get("forecast_hours", "24") or "24")
    region = request.args.get("region", "all")
    return build_wind_map_html(
        mode=mode if mode in {"observed", "forecast"} else "observed",
        period=period if period in {"hour", "day", "month"} else "hour",
        metric=metric if metric in {"avg", "gust", "peak"} else "gust",
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
