# -*- coding: utf-8 -*-
"""
Kvamskogen forside – prisanalyse.no/kvamskogen
"""

from __future__ import annotations

import os
import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from flask import Blueprint, Response, jsonify, request
from dotenv import load_dotenv

load_dotenv()

kvamskogen_bp = Blueprint("kvamskogen", __name__, url_prefix="/kvamskogen")

LOCAL_TZ = ZoneInfo("Europe/Oslo")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL   = "claude-sonnet-4-20250514"

_SYSTEM_PROMPT = """
Du er en lokal ekspert på Kvamskogen som skriver korte, presise værmeldinger
til en norsk hytteeier. Basert på rådataene du får, skriv:

1. verdict (maks 12 ord): Én setning som beskriver situasjonen. Eks: "Kald natt
   med snø – ideelt skiføre tidlig i morgen." Bruk norske uttrykk.

2. detail (maks 35 ord): En forklaring av snøtypen og hva det betyr i praksis.
   Nevn løypestatus hvis relevant. Vær konkret.

3. snow_quality: Et av disse: "Utmerket" | "Godt" | "Moderat" | "Dårlig"

4. badge_color: "green" | "amber" | "red" basert på snow_quality.

5. icon: Velg ett emoji som passer: ⛷️ 🎿 ☀️ 🌨️ 🌧️ 🌫️ 🥶 💧

Svar KUN med gyldig JSON (ingen markdown, ingen forklaring):
{"verdict":"...","detail":"...","snow_quality":"...","badge_color":"...","icon":"..."}

Regler for snow_quality:
- Utmerket: ny løssnø, temp -5 til -15C, løyper preparert siste 12t
- Godt:     temp under 0C, snø siste 3 døgn, løyper aktive
- Moderat:  mildt (0-3C), våt/klissete snø, eller løyper ikke preparert
- Dårlig:   regn, smelting, over 3C, ingen ny snø på lenge
""".strip()

FROST_BASE_URL = "https://frost.met.no"
FROST_SOURCE   = "SN50310"
FROST_TIMEOUT  = 20
FROST_RETRIES  = 4

FROST_ELEMENTS = {
    "temperature":   "air_temperature,max(air_temperature PT1H),min(air_temperature PT1H)",
    "precipitation": "sum(precipitation_amount PT1H)",
    "wind_speed":    "wind_speed",
}


def _frost_session() -> requests.Session:
    s = requests.Session()
    s.auth = (os.getenv("FROST_CLIENT_ID", ""), os.getenv("FROST_CLIENT_SECRET", ""))
    s.headers.update({"Accept": "application/json"})
    return s


def _frost_get(session: requests.Session, path: str, params: dict) -> dict:
    url = f"{FROST_BASE_URL}{path}"
    for attempt in range(1, FROST_RETRIES + 1):
        r = session.get(url, params=params, timeout=FROST_TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(30, 2 ** (attempt - 1)))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Frost ga opp etter {FROST_RETRIES} forsok")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_value(element_id: str, value: Any) -> Any:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return value
    if "precipitation_amount" in element_id and n == -1:
        return 0.0
    return n


def hent_historikk(hours: int = 24) -> list:
    end_utc   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_utc = end_utc - timedelta(hours=hours)
    session   = _frost_session()

    all_elements = "air_temperature,max(air_temperature PT1H),min(air_temperature PT1H),sum(precipitation_amount PT1H),wind_speed"

    payload = _frost_get(session, "/observations/v0.jsonld", {
        "sources":       FROST_SOURCE,
        "referencetime": f"{_iso_z(start_utc)}/{_iso_z(end_utc)}",
        "elements":      all_elements,
        "timeoffsets":   "default",
        "levels":        "default",
        "qualities":     "0,1,2,3,4",
    })

    def _classify(eid: str):
        if eid in ("air_temperature", "max(air_temperature PT1H)", "min(air_temperature PT1H)"):
            return "temperature"
        if "precipitation_amount" in eid:
            return "precipitation"
        if eid in ("wind_speed", "mean(wind_speed PT1H)"):
            return "wind_speed"
        return None

    rows: dict = {}
    for item in payload.get("data", []):
        ref = item.get("referenceTime")
        if not ref:
            continue
        ref_local = pd.to_datetime(ref, utc=True).tz_convert(LOCAL_TZ).isoformat()
        if ref_local not in rows:
            rows[ref_local] = {"time": ref_local}
        for obs in item.get("observations", []):
            eid  = str(obs.get("elementId", ""))
            name = _classify(eid)
            if name and name not in rows[ref_local]:
                rows[ref_local][name] = _clean_value(eid, obs.get("value"))

    return sorted(rows.values(), key=lambda x: x["time"])


def _ai_tolkning(sno_data: dict, loyper_data: dict) -> dict:
    if not ANTHROPIC_API_KEY:
        return _fallback_tolkning(sno_data, loyper_data)

    s    = sno_data.get("sammendrag", {})
    dag0 = (sno_data.get("daglig") or [{}])[0]

    payload_str = json.dumps({
        "temp_na_c":            s.get("temperatur_nå_c"),
        "snodybde_cm":          s.get("start_snødybde_cm"),
        "ny_sno_48t_cm":        s.get("total_ny_snø_cm"),
        "smelting_48t_mm":      s.get("total_smelting_mm"),
        "maks_temp_48t_c":      s.get("maks_temp_c"),
        "min_temp_48t_c":       s.get("min_temp_c"),
        "ver_i_dag":            dag0.get("vær_label"),
        "loyper_preparert":     loyper_data.get("freshly_groomed", 0),
        "sist_preparert_timer": round(loyper_data.get("newest_age_seconds", 0) / 3600, 1)
                                if loyper_data.get("newest_age_seconds") else None,
    }, ensure_ascii=False)

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": ANTHROPIC_MODEL, "max_tokens": 256,
                  "system": _SYSTEM_PROMPT,
                  "messages": [{"role": "user", "content": payload_str}]},
            timeout=15,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception:
        traceback.print_exc()
        return _fallback_tolkning(sno_data, loyper_data)


def _fallback_tolkning(sno_data: dict, loyper_data: dict) -> dict:
    s         = sno_data.get("sammendrag", {})
    temp      = s.get("temperatur_nå_c") or 0
    dybde     = s.get("start_snødybde_cm") or 0
    ny_sno    = s.get("total_ny_snø_cm") or 0
    preparert = loyper_data.get("freshly_groomed", 0)

    if temp <= -3 and ny_sno > 3 and preparert > 0:
        return {"verdict": "Kald natt med fersk sno – ideelt skiføre",
                "detail": f"Temperaturen er {temp}C og {ny_sno:.1f} cm ny sno. Loyper preparert.",
                "snow_quality": "Utmerket", "badge_color": "green", "icon": "⛷️"}
    elif temp <= 0 and dybde > 10:
        return {"verdict": "Kaldt og greit – brukbart skiføre",
                "detail": f"Snodybden er {dybde} cm og temp under null.",
                "snow_quality": "Godt", "badge_color": "green", "icon": "🎿"}
    elif 0 < temp <= 3:
        return {"verdict": "Mildt ver – snoen er vat og tung",
                "detail": f"Med {temp}C blir snoen klissete. Bruk morgenoekten.",
                "snow_quality": "Moderat", "badge_color": "amber", "icon": "🌨️"}
    else:
        return {"verdict": "Smelting og mildt – darlig skiføre",
                "detail": f"Temperaturen er {temp}C. Snoen smelter.",
                "snow_quality": "Dårlig", "badge_color": "red", "icon": "💧"}


def _hent_sno() -> dict:
    try:
        r = requests.get("http://localhost:5000/ver/api/snovarsel",
                         params={"stasjon": "Kvamskogen"}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def _hent_loyper() -> dict:
    try:
        r = requests.get("http://localhost:5000/ver/skiloyper-kvamskogen/stats",
                         params={"z": 13, "radius": 2, "fresh_hours": 12}, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@kvamskogen_bp.get("/")
def forside():
    return Response(_FORSIDE_HTML, mimetype="text/html; charset=utf-8")


@kvamskogen_bp.get("/api/status")
def api_status():
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        fut_sno    = ex.submit(_hent_sno)
        fut_loyper = ex.submit(_hent_loyper)
        sno_data    = fut_sno.result()
        loyper_data = fut_loyper.result()

    tolkning = _ai_tolkning(sno_data, loyper_data)
    s      = sno_data.get("sammendrag", {})
    daglig = sno_data.get("daglig", [])

    return jsonify({
        "hentet":   datetime.now().isoformat(timespec="seconds"),
        "tolkning": tolkning,
        "sno": {
            "dybde_cm":          s.get("start_snødybde_cm"),
            "endring_1t_cm":     s.get("endring_neste_time_cm"),
            "endring_3t_cm":     s.get("endring_neste_3t_cm"),
            "endring_24t_cm":    s.get("endring_neste_døgn_cm"),
            "ny_sno_48t_cm":     s.get("total_ny_snø_cm"),
            "smelting_48t_mm":   s.get("total_smelting_mm"),
            "temp_na_c":         s.get("temperatur_nå_c"),
            "min_temp_c":        s.get("min_temp_c"),
            "maks_temp_c":       s.get("maks_temp_c"),
            "prognose_slutt_cm": s.get("slutt_snødybde_cm"),
        },
        "loyper": {
            "totalt":          loyper_data.get("total", 0),
            "aktive":          loyper_data.get("active", 0),
            "preparert":       loyper_data.get("freshly_groomed", 0),
            "sist_prep_timer": round(loyper_data.get("newest_age_seconds", 0) / 3600, 1)
                               if loyper_data.get("newest_age_seconds") else None,
            "last_update":     loyper_data.get("newest_dt_local"),
        },
        "daglig": [
            {
                "dato":        d.get("dato"),
                "min_temp_c":  d.get("min_temp_c"),
                "maks_temp_c": d.get("maks_temp_c"),
                "ny_sno_cm":   round(d.get("total_ny_snø_mm", 0) / 10, 1),
                "smelting_mm": d.get("total_smelting_mm"),
                "snodybde_cm": d.get("snødybde_slutt_cm"),
                "ver_ikon":    d.get("vær_ikon"),
                "ver_label":   d.get("vær_label"),
                "vind_ms":     d.get("vind_ms_snitt"),
            }
            for d in daglig[:8]
        ],
    })


@kvamskogen_bp.get("/api/historikk")
def api_historikk():
    try:
        hours = max(1, min(72, int(request.args.get("hours", 24))))
        data  = hent_historikk(hours)
        return jsonify({"ok": True, "data": data, "antall": len(data)})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

_FORSIDE_HTML = r"""<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Kvamskogen - i dag</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#f5f7fb;--surface:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;--hint:#94a3b8;--green:#15803d;--green-bg:#f0fdf4;--green-bd:#bbf7d0;--amber:#92400e;--amber-bg:#fffbeb;--amber-bd:#fde68a;--red:#991b1b;--red-bg:#fef2f2;--red-bd:#fecaca;--blue:#1d4ed8;--radius:14px;--shadow:0 2px 12px rgba(15,23,42,.07);}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.55;}
a{color:var(--blue);text-decoration:none;}a:hover{text-decoration:underline;}
.page{max-width:860px;margin:0 auto;padding:20px 16px 48px;}
.nav{font-size:13px;color:var(--muted);margin-bottom:18px;}.nav a{color:var(--muted);}
.hero{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:22px 24px 18px;box-shadow:var(--shadow);margin-bottom:14px;}
.hero-top{display:flex;align-items:flex-start;gap:14px;}
.hero-icon{font-size:38px;line-height:1;flex-shrink:0;margin-top:2px;}
.hero-text{flex:1;}
.hero-verdict{font-size:20px;font-weight:600;line-height:1.3;margin-bottom:6px;}
.hero-detail{font-size:14px;color:var(--muted);line-height:1.5;}
.hero-badge{display:inline-block;margin-top:12px;font-size:12px;font-weight:600;padding:3px 12px;border-radius:20px;border:1px solid;}
.badge-green{background:var(--green-bg);color:var(--green);border-color:var(--green-bd);}
.badge-amber{background:var(--amber-bg);color:var(--amber);border-color:var(--amber-bd);}
.badge-red{background:var(--red-bg);color:var(--red);border-color:var(--red-bd);}
.section-label{font-size:11px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--hint);margin:18px 0 8px;}
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;}
@media(max-width:540px){.metric-grid{grid-template-columns:repeat(2,1fr);}}
.metric{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px 14px;box-shadow:var(--shadow);}
.metric-lbl{font-size:11px;color:var(--hint);margin-bottom:4px;}
.metric-val{font-size:20px;font-weight:600;}
.metric-sub{font-size:11px;color:var(--muted);margin-top:3px;}
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;box-shadow:var(--shadow);}
.chart-hours{display:flex;gap:6px;margin-bottom:10px;}
.chart-hours button{font-size:11px;padding:3px 10px;border-radius:20px;border:1px solid var(--border);cursor:pointer;background:var(--bg);color:var(--muted);}
.chart-hours button.active{background:#0f172a;color:#fff;border-color:#0f172a;}
.chart-legend{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px;}
.leg{display:flex;align-items:center;gap:5px;font-size:11px;color:var(--muted);}
.leg-line{display:inline-block;width:18px;height:2px;border-radius:2px;}
.leg-bar{display:inline-block;width:10px;height:10px;border-radius:2px;}
.chart-wrap{position:relative;}
.chart-msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:13px;color:var(--hint);}
.loyper-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;box-shadow:var(--shadow);}
.loyper-row{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);font-size:13px;}
.loyper-row:last-child{border-bottom:none;}
.loyper-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;}
.dot-green{background:#16a34a;}.dot-amber{background:#d97706;}.dot-gray{background:#94a3b8;}
.loyper-meta{font-size:11px;color:var(--hint);margin-top:8px;}
.forecast-strip{display:flex;gap:8px;overflow-x:auto;padding-bottom:4px;}
.fday{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;min-width:76px;text-align:center;flex-shrink:0;box-shadow:var(--shadow);}
.fday-name{font-size:10px;color:var(--hint);margin-bottom:4px;}
.fday-icon{font-size:20px;margin:2px 0;}
.fday-temp{font-size:12px;font-weight:600;}
.fday-snow{font-size:10px;color:#0284c7;margin-top:3px;min-height:14px;}
.fday-depth{font-size:10px;color:var(--hint);margin-top:2px;}
.links-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;}
@media(max-width:480px){.links-grid{grid-template-columns:repeat(2,1fr);}}
.link-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px 14px;box-shadow:var(--shadow);text-align:center;color:var(--text);display:flex;flex-direction:column;align-items:center;gap:6px;font-size:13px;}
.link-card:hover{border-color:var(--blue);background:#eff6ff;}
.link-card-icon{font-size:22px;}
.footer{margin-top:20px;font-size:11px;color:var(--hint);text-align:right;}
.spinner{display:inline-block;width:18px;height:18px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;}
@keyframes spin{to{transform:rotate(360deg)}}
.loading-hero{padding:32px;text-align:center;color:var(--muted);font-size:14px;}
</style>
</head>
<body>
<div class="page">
  <nav class="nav"><a href="/">prisanalyse.no</a> &rsaquo; Kvamskogen</nav>
  <div class="hero" id="hero"><div class="loading-hero"><span class="spinner"></span> Henter vaerstatus&hellip;</div></div>
  <div class="section-label">Snøstatus</div>
  <div class="metric-grid">
    <div class="metric"><div class="metric-lbl">Snødybde nå</div><div class="metric-val" id="m-dybde">–</div><div class="metric-sub" id="m-dybde-sub"></div></div>
    <div class="metric"><div class="metric-lbl">Endring neste time</div><div class="metric-val" id="m-1t">–</div><div class="metric-sub">prognose</div></div>
    <div class="metric"><div class="metric-lbl">Endring neste 3t</div><div class="metric-val" id="m-3t">–</div><div class="metric-sub">prognose</div></div>
    <div class="metric"><div class="metric-lbl">Temperatur nå</div><div class="metric-val" id="m-temp">–</div><div class="metric-sub" id="m-temp-sub"></div></div>
  </div>
  <div class="section-label">Siste døgn – observert (Frost/SN50310)</div>
  <div class="chart-card">
    <div class="chart-hours">
      <button class="active" onclick="setHours(12,this)">12t</button>
      <button onclick="setHours(24,this)">24t</button>
      <button onclick="setHours(48,this)">48t</button>
    </div>
    <div class="chart-legend">
      <span class="leg"><span class="leg-line" style="background:#3b82f6"></span>Temp (°C)</span>
      <span class="leg"><span class="leg-bar" style="background:#ef4444;opacity:.7"></span>Nedbør varm (mm)</span>
      <span class="leg"><span class="leg-bar" style="background:#60a5fa;opacity:.7"></span>Nedbør kald (mm)</span>
      <span class="leg"><span class="leg-line" style="background:#a78bfa"></span>Vind (m/s)</span>
    </div>
    <div class="chart-wrap" style="height:240px">
      <canvas id="hist-chart"></canvas>
      <div class="chart-msg" id="chart-msg"><span class="spinner"></span></div>
    </div>
  </div>
  <div class="section-label">Løypestatus</div>
  <div class="loyper-card">
    <div class="loyper-row"><span><span class="loyper-dot dot-green"></span>Nylig preparert (&le;12t)</span><span id="l-preparert" style="font-weight:600">–</span></div>
    <div class="loyper-row"><span><span class="loyper-dot dot-amber"></span>Aktive, ikke preparert</span><span id="l-aktive">–</span></div>
    <div class="loyper-row"><span><span class="loyper-dot dot-gray"></span>Totalt registrert</span><span id="l-totalt">–</span></div>
    <div class="loyper-meta" id="l-meta"></div>
  </div>
  <div class="section-label">Snøprognose – kommende dager</div>
  <div class="forecast-strip" id="forecast"></div>
  <div class="section-label">Verktøy</div>
  <div class="links-grid">
    <a class="link-card" href="/ver/varsel-kvamskogen"><span class="link-card-icon">❄️</span>Snøvarsel (detaljert)</a>
    <a class="link-card" href="/ver/skiloyper-kvamskogen"><span class="link-card-icon">🗺️</span>Løypekart</a>
    <a class="link-card" href="/ver/sno"><span class="link-card-icon">📊</span>Snøkart Norge</a>
    <a class="link-card" href="/ver/nedbor"><span class="link-card-icon">🌧️</span>Nedbørskart</a>
    <a class="link-card" href="/ver/solskinn"><span class="link-card-icon">☀️</span>Solskinnkart</a>
    <a class="link-card" href="/ver/"><span class="link-card-icon">🌦️</span>Alle værverktøy</a>
  </div>
  <div class="footer" id="footer"></div>
</div>
<script>
const DAYS=['søn','man','tir','ons','tor','fre','lør'];
const MONTHS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
let histData=[],currentHours=12,histChart=null;

function fmtTemp(v){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1)+'°C';}
function fmtDelta(v,u='cm'){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1)+' '+u;}

async function init(){
  try{const d=await(await fetch('/kvamskogen/api/status')).json();renderStatus(d);}
  catch(e){document.getElementById('hero').innerHTML='<div class="loading-hero" style="color:#991b1b">Kunne ikke hente data.</div>';}
}

function renderStatus(d){
  const t=d.tolkning||{},s=d.sno||{},lp=d.loyper||{};
  const bc={'green':'badge-green','amber':'badge-amber','red':'badge-red'}[t.badge_color]||'badge-amber';
  document.getElementById('hero').innerHTML=`<div class="hero-top"><div class="hero-icon">${t.icon||'🏔️'}</div><div class="hero-text"><div class="hero-verdict">${t.verdict||'Kvamskogen'}</div><div class="hero-detail">${t.detail||''}</div><span class="hero-badge ${bc}">${t.snow_quality||'Ukjent'} skiføre</span></div></div>`;
  document.getElementById('m-dybde').textContent=s.dybde_cm!=null?s.dybde_cm+' cm':'–';
  document.getElementById('m-dybde-sub').textContent=s.ny_sno_48t_cm!=null?'+'+s.ny_sno_48t_cm+' cm siste 48t':'';
  document.getElementById('m-1t').textContent=fmtDelta(s.endring_1t_cm);
  document.getElementById('m-3t').textContent=fmtDelta(s.endring_3t_cm);
  document.getElementById('m-temp').textContent=fmtTemp(s.temp_na_c);
  document.getElementById('m-temp-sub').textContent=s.min_temp_c!=null?`min ${fmtTemp(s.min_temp_c)} / maks ${fmtTemp(s.maks_temp_c)}`:'';
  const prepEl=document.getElementById('l-preparert');
  prepEl.textContent=lp.preparert!=null?lp.preparert+' segmenter':'–';
  prepEl.style.color=lp.preparert>0?'#15803d':'#64748b';
  document.getElementById('l-aktive').textContent=lp.aktive!=null?lp.aktive+' segmenter':'–';
  document.getElementById('l-totalt').textContent=lp.totalt!=null?lp.totalt+' segmenter':'–';
  if(lp.sist_prep_timer!=null)document.getElementById('l-meta').textContent=`Sist preparert: ${lp.sist_prep_timer} t siden · ${lp.last_update||''}`;
  document.getElementById('forecast').innerHTML=(d.daglig||[]).map(dag=>{
    const dt=new Date(dag.dato+'T12:00:00');
    const navn=DAYS[dt.getDay()]+' '+dt.getDate()+'.'+MONTHS[dt.getMonth()];
    return`<div class="fday"><div class="fday-name">${navn}</div><div class="fday-icon">${dag.ver_ikon||'–'}</div><div class="fday-temp"><span style="color:#ef4444">${dag.maks_temp_c>0?'+':''}${dag.maks_temp_c}°</span> / <span style="color:#0284c7">${dag.min_temp_c}°</span></div><div class="fday-snow">${dag.ny_sno_cm>0?'❄ +'+dag.ny_sno_cm+'cm':''}</div><div class="fday-depth">${dag.snodybde_cm!=null?dag.snodybde_cm+' cm':''}</div></div>`;
  }).join('');
  const ts=new Date(d.hentet);
  document.getElementById('footer').textContent=`Oppdatert: ${ts.toLocaleString('no-NO')} · Data: Yr, Frost, loyper.net`;
}

async function loadHistorikk(){
  const msg=document.getElementById('chart-msg');
  msg.innerHTML='<span class="spinner"></span>';msg.style.display='flex';
  try{
    const d=await(await fetch(`/kvamskogen/api/historikk?hours=${currentHours}`)).json();
    if(d.ok&&d.data.length){histData=d.data;renderChart();msg.style.display='none';}
    else{msg.textContent='Ingen data';msg.style.display='flex';}
  }catch(e){msg.textContent='Feil ved lasting';msg.style.display='flex';}
}

function renderChart(){
  if(!histData.length)return;
  // Filtrer til kun hele timer (der temperatur finnes)
  const hourly=histData.filter(x=>x.temperature!=null);
  if(!hourly.length)return;
  const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
  const labels=hourly.map(x=>{const dt=new Date(x.time);const h=dt.getHours();if(h===0)return dt.getDate()+'.'+MS[dt.getMonth()]+' 00:00';return String(h).padStart(2,'0')+':00';});
  const temps=hourly.map(x=>parseFloat(x.temperature));
  const precip=hourly.map(x=>x.precipitation!=null?parseFloat(x.precipitation):null);
  const precipColors=hourly.map(x=>{const t=parseFloat(x.temperature);return t<=0?'rgba(96,165,250,0.75)':'rgba(239,68,68,0.7)';});
  const wind=hourly.map(x=>x.wind_speed!=null?parseFloat(x.wind_speed):null);
  const ctx=document.getElementById('hist-chart').getContext('2d');
  if(histChart)histChart.destroy();
  histChart=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør (mm)',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yPrecip',order:3},
      {type:'line',label:'Temperatur (°C)',data:temps,borderWidth:2,pointRadius:0,pointHoverRadius:4,tension:0.3,fill:false,yAxisID:'yTemp',order:1,segment:{borderColor:ctx=>ctx.p0.parsed.y<=0?'rgba(59,130,246,1)':'rgba(239,68,68,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind (m/s)',data:wind,borderColor:'rgba(167,139,250,0.8)',backgroundColor:'transparent',borderWidth:1.5,borderDash:[3,3],pointRadius:0,pointHoverRadius:3,tension:0.3,fill:false,yAxisID:'yWind',order:2},
    ]},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:false},tooltip:{backgroundColor:'rgba(15,23,42,.92)',callbacks:{label:c=>{if(c.parsed.y==null)return null;if(c.dataset.label.includes('Temp'))return`Temp: ${c.parsed.y>0?'+':''}${c.parsed.y.toFixed(1)}°C`;if(c.dataset.label.includes('Nedbør'))return`Nedbør: ${c.parsed.y.toFixed(1)} mm`;if(c.dataset.label.includes('Vind'))return`Vind: ${c.parsed.y.toFixed(1)} m/s`;}}}},
      scales:{
        x:{ticks:{color:'#94a3b8',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:14},grid:{color:'rgba(0,0,0,.04)'}},
        yTemp:{position:'left',ticks:{color:'#94a3b8',font:{size:10},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(0,0,0,.04)'},afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yPrecip:{position:'right',min:0,suggestedMax:1,ticks:{color:'#94a3b8',font:{size:10},callback:v=>v+' mm'},grid:{drawOnChartArea:false}},
        yWind:{display:false,min:0}
      }
    }
  });
}

function setHours(h,btn){
  currentHours=h;
  document.querySelectorAll('.chart-hours button').forEach(el=>el.classList.remove('active'));
  if(btn)btn.classList.add('active');
  loadHistorikk();
}

init();
loadHistorikk();
setInterval(init,10*60*1000);
</script>
</body>
</html>
"""
