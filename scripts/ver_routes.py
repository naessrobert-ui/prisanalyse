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

try:
    from zoneinfo import ZoneInfo  # py3.9+
    OSLO = ZoneInfo("Europe/Oslo")
except Exception:
    OSLO = timezone.utc  # fallback

from snow_map import build_snow_map_html
from precip_map import build_precip_county_map_html
from sunshine_map import build_sunshine_map_html
from temp_map import build_min_temp_map_html

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
    return """
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Vær – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background:#f5f7fb; }
      .page { max-width: 1100px; margin: 32px auto; padding: 0 16px; }
      h1 { margin: 0 0 14px; }
      .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 16px; }
      .card {
        background: white; border-radius: 18px; padding: 18px 20px;
        box-shadow: 0 18px 45px rgba(15,23,42,.08);
      }
      .card h2 { margin:0 0 6px; }
      .muted { color:#475569; margin: 0 0 12px; }
      .btn {
        display:inline-block; padding: 8px 14px; border-radius: 999px;
        background:#2563eb; color:#fff; text-decoration:none; font-weight:700;
      }
      .btn-green  { background:#16a34a; }
      .btn-amber  { background:#f59e0b; }
      .btn-red    { background:#ef4444; }
      .btn-indigo { background:#4f46e5; }
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
          <a class="btn btn-green" href="/ver/nedbor">Åpne</a>
        </div>

        <div class="card">
          <h2>Solskinn</h2>
          <p class="muted">Siste 24 timer (rullerende) + dag / MTD / YTD.</p>
          <a class="btn btn-amber" href="/ver/solskinn">Åpne</a>
        </div>

        <div class="card">
          <h2>Min temperatur siste døgn</h2>
          <p class="muted">Velg fylke og se nyeste døgn-min (P1D) per stasjon.</p>
          <a class="btn btn-red" href="/ver/min-temp">Åpne</a>
        </div>

        <div class="card">
          <h2>Skiløyper – Kvamskogen</h2>
          <p class="muted">Sanntids løypestatus (preparering) med alder-farger og egne markeringer.</p>
          <a class="btn" href="/ver/skiloyper-kvamskogen">Åpne</a>
        </div>

        <div class="card">
          <h2>Snøprognoser</h2>
          <p class="muted">Så mye snø kommer det? Prognose time for time basert på YR-varsel og observert snødybde.</p>
          <a class="btn btn-indigo" href="/ver/varsel-kvamskogen">Åpne</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""


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


def _hent_prognose_data(stasjon_navn: str) -> dict[str, Any]:
    """Henter og beregner snøprognose. Returnerer dict klar for JSON."""
    config = STASJONER.get(stasjon_navn)
    if not config:
        return {"error": f"Ukjent stasjon: {stasjon_navn}", "stasjoner": list(STASJONER.keys())}

    auth = _env_auth()
    session = requests.Session()

    # 1) Observert snødybde
    snødybde_cm = hent_snødybde_frost(config["frost_id"], session=session, auth=auth)
    if snødybde_cm is None:
        snødybde_cm = 0.0

    # 2) YR-varsel
    intervaller = hent_intervaller(config["place"])

    # 3) Simuler
    df = simuler_snøprognose(intervaller, snødybde_cm)

    # 4) Bygg intervall-data med vær-type
    intervall_data = []
    for _, rad in df.iterrows():
        vær = _vær_type(rad["temperatur_c"], rad["nedbør_mm"])
        intervall_data.append({
            "start":        rad["start"].isoformat() if hasattr(rad["start"], "isoformat") else str(rad["start"]),
            "slutt":        rad["slutt"].isoformat() if hasattr(rad["slutt"], "isoformat") else str(rad["slutt"]),
            "timer":        rad["timer"],
            "temperatur_c": rad["temperatur_c"],
            "nedbør_mm":    rad["nedbør_mm"],
            "ny_snø_mm":    rad["ny_snø_mm"],
            "smelting_mm":  rad["smelting_mm"],
            "netto_mm":     rad["netto_mm"],
            "snødybde_cm":  rad["snødybde_cm"],
            "snøfaktor":    rad["snøfaktor"],
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
            "vær_ikon":          vær["icon"],
            "vær_label":         vær["label"],
        })

    # 6) Sammendrag-tall (neste 48t)
    df_48 = df.head(48) if len(df) > 48 else df
    sammendrag = {
        "start_snødybde_cm": snødybde_cm,
        "slutt_snødybde_cm": float(df["snødybde_cm"].iloc[-1]) if not df.empty else snødybde_cm,
        "endring_cm":        round(float(df["snødybde_cm"].iloc[-1]) - snødybde_cm, 1) if not df.empty else 0,
        "total_nedbør_mm":   round(float(df_48["nedbør_mm"].sum()), 1),
        "total_ny_snø_mm":   round(float(df_48["ny_snø_mm"].sum()), 1),
        "total_smelting_mm": round(float(df_48["smelting_mm"].sum()), 1),
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


@ver.get("/api/snovarsel")
def api_snovarsel():
    stasjon = request.args.get("stasjon", "Kvamskogen")
    try:
        data = _hent_prognose_data(stasjon)
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
.loc-bar select{background:var(--surface2);color:var(--ink);border:1px solid var(--border);border-radius:10px;padding:8px 12px;font-size:13px;font-family:var(--sans);}
.loc-bar button{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#0a0f1f;border:none;border-radius:10px;padding:8px 18px;font-weight:700;cursor:pointer;font-family:var(--sans);font-size:13px;}
.status{text-align:center;color:var(--muted);font-size:12px;min-height:18px;margin:4px 0 14px}
.status.err{color:var(--neg-c)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:18px;margin-bottom:14px;box-shadow:var(--shadow);backdrop-filter:blur(12px);}
.card h2{font-family:var(--serif);font-size:18px;font-weight:700;margin-bottom:12px;color:var(--snow-c);}
.sg{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:14px}
.sb{background:var(--surface2);border-radius:12px;padding:12px 8px;text-align:center;}
.sb .v{font-size:22px;font-weight:800;line-height:1.1}
.sb .l{font-size:10px;color:var(--muted);margin-top:4px;text-transform:uppercase;letter-spacing:.04em}
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
    <select id="stasjon-sel">
      <option value="Kvamskogen" selected>Kvamskogen</option>
    </select>
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
      <h2>📅 Daglig oversikt</h2>
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

async function load(){
  const st=document.getElementById('stasjon-sel').value;
  statusEl.innerHTML='<div class="spinner"></div>';
  statusEl.className='status';
  cardsEl.style.display='none';
  try{
    const r=await fetch('/ver/api/snovarsel?stasjon='+encodeURIComponent(st));
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
  document.getElementById('sum-grid').innerHTML=`
    <div class="sb snow"><div class="v">${s.start_snødybde_cm}</div><div class="l">Snødybde nå (cm)</div></div>
    <div class="sb ${endring>=0?'pos':'neg'}"><div class="v">${endring>=0?'+':''}${endring}</div><div class="l">Endring (cm)</div></div>
    <div class="sb snow"><div class="v">${s.slutt_snødybde_cm.toFixed(1)}</div><div class="l">Prognose slutt (cm)</div></div>
    <div class="sb rain"><div class="v">${s.total_nedbør_mm}</div><div class="l">Nedbør 48t (mm)</div></div>
    <div class="sb snow"><div class="v">${s.total_ny_snø_mm}</div><div class="l">Ny snø 48t (mm)</div></div>
    <div class="sb cold"><div class="v">${s.min_temp_c}°</div><div class="l">Min temp</div></div>
    <div class="sb warm"><div class="v">${s.maks_temp_c}°</div><div class="l">Maks temp</div></div>
    <div class="sb neg"><div class="v">${s.total_smelting_mm}</div><div class="l">Smelting 48t (mm)</div></div>`;
}

function renderSnowChart(d){
  const iv=d.intervaller;
  const ctx=document.getElementById('chart-snow').getContext('2d');
  if(snowChart) snowChart.destroy();
  snowChart=new Chart(ctx,{type:'line',data:{labels:iv.map(x=>fmtDH(x.start)),datasets:[
    {label:'Snødybde (cm)',data:iv.map(x=>x.snødybde_cm),borderColor:'#a5d8ff',backgroundColor:'rgba(165,216,255,.1)',fill:true,tension:.3,borderWidth:2.5,pointRadius:0,pointHoverRadius:4,yAxisID:'y'},
    {label:'Netto snø/smelting (mm)',data:iv.map(x=>x.netto_mm),type:'bar',backgroundColor:iv.map(x=>x.netto_mm>=0?'rgba(105,219,124,.5)':'rgba(255,107,107,.45)'),borderRadius:2,yAxisID:'y1'}
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
    {label:'Temperatur (°C)',data:iv.map(x=>x.temperatur_c),borderColor:'#ffa94d',backgroundColor:'transparent',tension:.3,borderWidth:2,pointRadius:0,pointHoverRadius:4,yAxisID:'y'},
    {label:'Nedbør (mm)',data:iv.map(x=>x.nedbør_mm),type:'bar',backgroundColor:iv.map(x=>x.temperatur_c<=1.5?'rgba(165,216,255,.55)':'rgba(116,192,252,.45)'),borderRadius:2,yAxisID:'y1'}
  ]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
    plugins:{legend:{labels:{color:'#7b8db5',font:{family:'Outfit',size:11}}},tooltip:{backgroundColor:'rgba(15,23,48,.92)',titleFont:{family:'Outfit'},bodyFont:{family:'Outfit'}}},
    scales:{x:{ticks:{color:'#7b8db5',font:{size:9,family:'Outfit'},maxRotation:45,autoSkip:true,maxTicksLimit:18},grid:{color:'rgba(100,130,200,.08)'}},
      y:{position:'left',title:{display:true,text:'°C',color:'#ffa94d',font:{family:'Outfit',size:11}},ticks:{color:'#ffa94d',font:{size:10,family:'Outfit'}},grid:{color:'rgba(100,130,200,.06)'}},
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
      <div class="t">${fmtH(iv.start)}</div>
      <div class="wi">${iv.vær_ikon}</div>
      <div class="tp" style="color:${iv.temperatur_c<=0?'var(--cold-c)':'var(--warm-c)'}">${iv.temperatur_c>0?'+':''}${iv.temperatur_c}°</div>
      <div class="pr">${iv.nedbør_mm>0?iv.nedbør_mm+'mm':''}</div>
      <div class="sn">${iv.ny_snø_mm>0?'❄ '+iv.ny_snø_mm+'mm':''}</div>
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
      <div class="ds">${dag.total_ny_snø_mm>0?'❄ +'+dag.total_ny_snø_mm+'mm':''}</div>
      <div class="dd">Snø: ${dag.snødybde_slutt_cm}cm</div>
    </div>`;}
  document.getElementById('daily-grid').innerHTML=html;
}

// Last automatisk ved oppstart
load();
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


@ver.get("/skiloyper-kvamskogen/stats")
def skiloyper_kvamskogen_stats():
    z = int(request.args.get("z", 13))
    radius = int(request.args.get("radius", 2))
    fresh_hours = int(request.args.get("fresh_hours", 12))
    cache_seconds = int(request.args.get("cache_seconds", 60))
    z = max(0, min(19, z)); radius = max(0, min(6, radius))
    fresh_hours = max(1, min(72, fresh_hours)); cache_seconds = max(0, min(600, cache_seconds))

    cache_key = ("kvamskogen", z, radius, fresh_hours)
    now_ts = time.time()
    if cache_seconds > 0:
        hit = _STATS_CACHE.get(cache_key)
        if hit and hit.expires_at > now_ts:
            return jsonify(hit.payload)

    center_x, center_y = _latlng_to_tile(KVAM_LAT, KVAM_LNG, z)
    now_utc = datetime.now(timezone.utc)
    fresh_seconds = fresh_hours * 3600
    seen = set(); total = active = freshly_groomed = with_last_update = missing_last_update = 0
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
                        newest_dt_utc = last_dt_utc; newest_dt_local = last_dt_utc.astimezone(OSLO)
                        newest_seg_id = props.get("id"); newest_track_id = props.get("track_id")
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
    return jsonify(payload)


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