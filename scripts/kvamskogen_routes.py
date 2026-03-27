# -*- coding: utf-8 -*-
"""
Kvamskogen forside – prisanalyse.no/kvamskogen

Registrer i app.py:
    from kvamskogen_routes import kvamskogen_bp
    app.register_blueprint(kvamskogen_bp)

Avhenger av:
  - /ver/api/snovarsel?stasjon=Kvamskogen  (fra snow_increase.py / ver_routes.py)
  - /ver/skiloyper-kvamskogen/stats        (fra ver_routes.py)
  - Anthropic API (ANTHROPIC_API_KEY i .env)
"""

from __future__ import annotations

import os
import json
import traceback
from datetime import datetime

import requests
from flask import Blueprint, Response, jsonify, request
from dotenv import load_dotenv

load_dotenv()

kvamskogen_bp = Blueprint("kvamskogen", __name__, url_prefix="/kvamskogen")

# ── Anthropic ────────────────────────────────────────────────────────────────

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
- Utmerket: ny løssnø, temp -5 til -15°C, løyper preparert siste 12t
- Godt:     temp under 0°C, snø siste 3 døgn, løyper aktive
- Moderat:  mildt (0–3°C), våt/klissete snø, eller løyper ikke preparert
- Dårlig:   regn, smelting, over 3°C, ingen ny snø på lenge
""".strip()


def _ai_tolkning(sno_data: dict, loyper_data: dict) -> dict:
    """Kaller Claude API og returnerer tolknings-dict."""
    if not ANTHROPIC_API_KEY:
        return _fallback_tolkning(sno_data, loyper_data)

    s = sno_data.get("sammendrag", {})
    daglig = sno_data.get("daglig", [{}])
    dag0   = daglig[0] if daglig else {}

    payload_str = json.dumps({
        "temp_nå_c":          s.get("temperatur_nå_c"),
        "snødybde_cm":        s.get("start_snødybde_cm"),
        "endring_siste_døgn_cm": s.get("endring_neste_døgn_cm"),
        "ny_snø_48t_cm":      s.get("total_ny_snø_cm"),
        "smelting_48t_mm":    s.get("total_smelting_mm"),
        "maks_temp_48t_c":    s.get("maks_temp_c"),
        "min_temp_48t_c":     s.get("min_temp_c"),
        "vær_i_dag":          dag0.get("vær_label"),
        "løyper_totalt":      loyper_data.get("total", 0),
        "løyper_aktive":      loyper_data.get("active", 0),
        "løyper_preparert":   loyper_data.get("freshly_groomed", 0),
        "sist_preparert_timer": round(loyper_data.get("newest_age_seconds", 0) / 3600, 1)
                                if loyper_data.get("newest_age_seconds") else None,
    }, ensure_ascii=False)

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      ANTHROPIC_MODEL,
                "max_tokens": 256,
                "system":     _SYSTEM_PROMPT,
                "messages":   [{"role": "user", "content": payload_str}],
            },
            timeout=15,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        # Strip markdown-blokker om modellen glemmer seg
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception:
        traceback.print_exc()
        return _fallback_tolkning(sno_data, loyper_data)


def _fallback_tolkning(sno_data: dict, loyper_data: dict) -> dict:
    """Regelbasert fallback hvis AI-kall feiler."""
    s    = sno_data.get("sammendrag", {})
    temp = s.get("temperatur_nå_c") or 0
    dybde = s.get("start_snødybde_cm") or 0
    ny_sno = s.get("total_ny_snø_cm") or 0
    preparert = loyper_data.get("freshly_groomed", 0)

    if temp <= -3 and ny_sno > 3 and preparert > 0:
        return {
            "verdict": "Kald natt med fersk snø – ideelt skiføre",
            "detail":  f"Temperaturen er {temp}°C og det har kommet {ny_sno:.1f} cm ny snø. Løypene er nylig preparert.",
            "snow_quality": "Utmerket", "badge_color": "green", "icon": "⛷️",
        }
    elif temp <= 0 and dybde > 10:
        return {
            "verdict": "Kaldt og greit – brukbart skiføre",
            "detail":  f"Snødybden er {dybde} cm og temperaturen holder seg under null.",
            "snow_quality": "Godt", "badge_color": "green", "icon": "🎿",
        }
    elif 0 < temp <= 3:
        return {
            "verdict": "Mildt vær – snøen er våt og tung",
            "detail":  f"Med {temp}°C blir snøen klissete. Bruk heller morgenøkten.",
            "snow_quality": "Moderat", "badge_color": "amber", "icon": "🌨️",
        }
    else:
        return {
            "verdict": "Smelting og mildt – dårlig skiføre",
            "detail":  f"Temperaturen er {temp}°C. Snøen smelter raskt.",
            "snow_quality": "Dårlig", "badge_color": "red", "icon": "💧",
        }


# ── Interne API-kall (server-side proxy) ────────────────────────────────────

def _hent_sno() -> dict:
    """Henter snødata fra vår egen /ver/api/snovarsel."""
    try:
        r = requests.get(
            "http://localhost:5000/ver/api/snovarsel",
            params={"stasjon": "Kvamskogen"},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def _hent_loyper() -> dict:
    """Henter løypedata fra vår egen /ver/skiloyper-kvamskogen/stats."""
    try:
        r = requests.get(
            "http://localhost:5000/ver/skiloyper-kvamskogen/stats",
            params={"z": 13, "radius": 2, "fresh_hours": 12},
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


# ── Routes ───────────────────────────────────────────────────────────────────

@kvamskogen_bp.get("/")
def forside():
    """Server HTML-forsiden (all logikk er klient-side JS mot /kvamskogen/api/status)."""
    return Response(_FORSIDE_HTML, mimetype="text/html; charset=utf-8")


@kvamskogen_bp.get("/api/status")
def api_status():
    """
    Aggregerer snø + løyper + AI-tolkning til én JSON-respons.
    Klienten kaller dette endepunktet ved oppstart.
    """
    sno_data    = _hent_sno()
    loyper_data = _hent_loyper()
    tolkning    = _ai_tolkning(sno_data, loyper_data)

    s  = sno_data.get("sammendrag", {})
    daglig = sno_data.get("daglig", [])

    return jsonify({
        "hentet":    datetime.now().isoformat(timespec="seconds"),
        "tolkning":  tolkning,
        "sno": {
            "dybde_cm":      s.get("start_snødybde_cm"),
            "endring_1t_cm": s.get("endring_neste_time_cm"),
            "endring_3t_cm": s.get("endring_neste_3t_cm"),
            "endring_24t_cm":s.get("endring_neste_døgn_cm"),
            "ny_sno_48t_cm": s.get("total_ny_snø_cm"),
            "smelting_48t_mm": s.get("total_smelting_mm"),
            "temp_nå_c":     s.get("temperatur_nå_c"),
            "min_temp_c":    s.get("min_temp_c"),
            "maks_temp_c":   s.get("maks_temp_c"),
            "prognose_slutt_cm": s.get("slutt_snødybde_cm"),
        },
        "loyper": {
            "totalt":      loyper_data.get("total", 0),
            "aktive":      loyper_data.get("active", 0),
            "preparert":   loyper_data.get("freshly_groomed", 0),
            "sist_prep_timer": round(loyper_data.get("newest_age_seconds", 0) / 3600, 1)
                               if loyper_data.get("newest_age_seconds") else None,
            "last_update": loyper_data.get("newest_dt_local"),
        },
        "daglig": [
            {
                "dato":         d.get("dato"),
                "min_temp_c":   d.get("min_temp_c"),
                "maks_temp_c":  d.get("maks_temp_c"),
                "ny_sno_cm":    round(d.get("total_ny_snø_mm", 0) / 10, 1),
                "smelting_mm":  d.get("total_smelting_mm"),
                "snødybde_cm":  d.get("snødybde_slutt_cm"),
                "vær_ikon":     d.get("vær_ikon"),
                "vær_label":    d.get("vær_label"),
                "vind_ms":      d.get("vind_ms_snitt"),
            }
            for d in daglig[:8]
        ],
    })


# ── HTML ─────────────────────────────────────────────────────────────────────

_FORSIDE_HTML = r"""<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Kvamskogen – i dag</title>
<style>
:root{
  --bg:#f5f7fb; --surface:#fff; --border:#e2e8f0;
  --text:#0f172a; --muted:#64748b; --hint:#94a3b8;
  --green:#15803d; --green-bg:#f0fdf4; --green-bd:#bbf7d0;
  --amber:#92400e; --amber-bg:#fffbeb; --amber-bd:#fde68a;
  --red:#991b1b;   --red-bg:#fef2f2;   --red-bd:#fecaca;
  --blue:#1d4ed8;
  --radius:14px; --shadow:0 2px 12px rgba(15,23,42,.07);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.55;min-height:100vh;}
a{color:var(--blue);text-decoration:none;}
a:hover{text-decoration:underline;}

.page{max-width:860px;margin:0 auto;padding:20px 16px 48px;}
.nav{font-size:13px;color:var(--muted);margin-bottom:18px;}
.nav a{color:var(--muted);}

/* ─── HERO ─── */
.hero{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
      padding:22px 24px 18px;box-shadow:var(--shadow);margin-bottom:14px;}
.hero-top{display:flex;align-items:flex-start;gap:14px;}
.hero-icon{font-size:38px;line-height:1;flex-shrink:0;margin-top:2px;}
.hero-text{flex:1;}
.hero-verdict{font-size:20px;font-weight:600;color:var(--text);line-height:1.3;margin-bottom:6px;}
.hero-detail{font-size:14px;color:var(--muted);line-height:1.5;}
.hero-badge{display:inline-block;margin-top:12px;font-size:12px;font-weight:600;
            padding:3px 12px;border-radius:20px;border:1px solid;}
.badge-green{background:var(--green-bg);color:var(--green);border-color:var(--green-bd);}
.badge-amber{background:var(--amber-bg);color:var(--amber);border-color:var(--amber-bd);}
.badge-red  {background:var(--red-bg);  color:var(--red);  border-color:var(--red-bd);}

/* ─── SEKSJONSOVERSKRIFT ─── */
.section-label{font-size:11px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;
               color:var(--hint);margin:18px 0 8px;}

/* ─── METRIKK-GRID ─── */
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;}
@media(max-width:540px){.metric-grid{grid-template-columns:repeat(2,1fr);}}
.metric{background:var(--surface);border:1px solid var(--border);border-radius:10px;
        padding:12px 14px;box-shadow:var(--shadow);}
.metric-lbl{font-size:11px;color:var(--hint);margin-bottom:4px;}
.metric-val{font-size:20px;font-weight:600;color:var(--text);}
.metric-sub{font-size:11px;color:var(--muted);margin-top:3px;}

/* ─── LØYPER ─── */
.loyper-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
             padding:16px 18px;box-shadow:var(--shadow);}
.loyper-row{display:flex;justify-content:space-between;align-items:center;
            padding:7px 0;border-bottom:1px solid var(--border);font-size:13px;}
.loyper-row:last-child{border-bottom:none;}
.loyper-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;flex-shrink:0;}
.dot-green{background:#16a34a;} .dot-amber{background:#d97706;} .dot-gray{background:#94a3b8;}
.loyper-meta{font-size:11px;color:var(--hint);margin-top:8px;}

/* ─── PROGNOSE-STRIP ─── */
.forecast-strip{display:flex;gap:8px;overflow-x:auto;padding-bottom:4px;}
.forecast-strip::-webkit-scrollbar{height:4px;}
.forecast-strip::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px;}
.fday{background:var(--surface);border:1px solid var(--border);border-radius:10px;
      padding:10px 12px;min-width:76px;text-align:center;flex-shrink:0;box-shadow:var(--shadow);}
.fday-name{font-size:10px;color:var(--hint);margin-bottom:4px;}
.fday-icon{font-size:20px;margin:2px 0;}
.fday-temp{font-size:12px;font-weight:600;}
.fday-snow{font-size:10px;color:#0284c7;margin-top:3px;min-height:14px;}
.fday-depth{font-size:10px;color:var(--hint);margin-top:2px;}

/* ─── LENKER ─── */
.links-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;}
@media(max-width:480px){.links-grid{grid-template-columns:repeat(2,1fr);}}
.link-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;
           padding:12px 14px;box-shadow:var(--shadow);text-align:center;color:var(--text);
           display:flex;flex-direction:column;align-items:center;gap:6px;font-size:13px;}
.link-card:hover{border-color:var(--blue);background:#eff6ff;}
.link-card-icon{font-size:22px;}

/* ─── FOOTER ─── */
.footer{margin-top:20px;font-size:11px;color:var(--hint);text-align:right;}

/* ─── SPINNER ─── */
.spinner{display:inline-block;width:18px;height:18px;border:2px solid var(--border);
         border-top-color:var(--blue);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;}
@keyframes spin{to{transform:rotate(360deg)}}
.loading-hero{padding:32px;text-align:center;color:var(--muted);font-size:14px;}
</style>
</head>
<body>
<div class="page">
  <nav class="nav"><a href="/">prisanalyse.no</a> › Kvamskogen</nav>

  <!-- HERO -->
  <div class="hero" id="hero">
    <div class="loading-hero"><span class="spinner"></span> Henter værstatus…</div>
  </div>

  <!-- SNØ -->
  <div class="section-label">Snøstatus</div>
  <div class="metric-grid" id="sno-grid">
    <div class="metric"><div class="metric-lbl">Snødybde nå</div><div class="metric-val" id="m-dybde">–</div><div class="metric-sub" id="m-dybde-sub"></div></div>
    <div class="metric"><div class="metric-lbl">Endring siste time</div><div class="metric-val" id="m-1t">–</div><div class="metric-sub">neste time</div></div>
    <div class="metric"><div class="metric-lbl">Endring siste 3t</div><div class="metric-val" id="m-3t">–</div><div class="metric-sub">neste 3 timer</div></div>
    <div class="metric"><div class="metric-lbl">Temperatur nå</div><div class="metric-val" id="m-temp">–</div><div class="metric-sub" id="m-temp-sub"></div></div>
  </div>

  <!-- LØYPER -->
  <div class="section-label">Løypestatus</div>
  <div class="loyper-card">
    <div class="loyper-row">
      <span><span class="loyper-dot dot-green"></span>Nylig preparert (&le;12t)</span>
      <span id="l-preparert" style="font-weight:600">–</span>
    </div>
    <div class="loyper-row">
      <span><span class="loyper-dot dot-amber"></span>Aktive, ikke preparert</span>
      <span id="l-aktive">–</span>
    </div>
    <div class="loyper-row">
      <span><span class="loyper-dot dot-gray"></span>Totalt registrert</span>
      <span id="l-totalt">–</span>
    </div>
    <div class="loyper-meta" id="l-meta"></div>
  </div>

  <!-- PROGNOSE -->
  <div class="section-label">Snøprognose – kommende dager</div>
  <div class="forecast-strip" id="forecast"></div>

  <!-- LENKER -->
  <div class="section-label">Verktøy</div>
  <div class="links-grid">
    <a class="link-card" href="/ver/varsel-kvamskogen">
      <span class="link-card-icon">❄️</span>Snøvarsel (detaljert)
    </a>
    <a class="link-card" href="/ver/skiloyper-kvamskogen">
      <span class="link-card-icon">🗺️</span>Løypekart
    </a>
    <a class="link-card" href="/ver/sno">
      <span class="link-card-icon">📊</span>Snøkart Norge
    </a>
    <a class="link-card" href="/ver/nedbor">
      <span class="link-card-icon">🌧️</span>Nedbørskart
    </a>
    <a class="link-card" href="/ver/solskinn">
      <span class="link-card-icon">☀️</span>Solskinnkart
    </a>
    <a class="link-card" href="/ver/">
      <span class="link-card-icon">🌦️</span>Alle værverktøy
    </a>
  </div>

  <div class="footer" id="footer"></div>
</div>

<script>
const DAYS=['søn','man','tir','ons','tor','fre','lør'];
const MONTHS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];

function fmt(v, suffix='', decimals=1){
  if(v===null||v===undefined) return '–';
  const n = parseFloat(v);
  if(isNaN(n)) return '–';
  const s = decimals===0 ? Math.round(n).toString() : n.toFixed(decimals);
  return (n>0&&suffix!='°C'?'+':'')+s+suffix;
}

function fmtTemp(v){
  if(v===null||v===undefined) return '–';
  const n=parseFloat(v);
  return (n>0?'+':'')+n.toFixed(1)+'°C';
}

function fmtDelta(v, unit='cm'){
  if(v===null||v===undefined) return '–';
  const n=parseFloat(v);
  return (n>0?'+':'')+n.toFixed(1)+' '+unit;
}

async function init(){
  try{
    const r = await fetch('/kvamskogen/api/status');
    const d = await r.json();
    render(d);
  }catch(e){
    document.getElementById('hero').innerHTML =
      '<div class="loading-hero" style="color:#991b1b">Kunne ikke hente data. Prøv igjen om litt.</div>';
  }
}

function render(d){
  const t  = d.tolkning || {};
  const s  = d.sno || {};
  const lp = d.loyper || {};

  // ── Hero
  const badgeClass = {'green':'badge-green','amber':'badge-amber','red':'badge-red'}[t.badge_color]||'badge-amber';
  document.getElementById('hero').innerHTML = `
    <div class="hero-top">
      <div class="hero-icon">${t.icon||'🏔️'}</div>
      <div class="hero-text">
        <div class="hero-verdict">${t.verdict||'Kvamskogen'}</div>
        <div class="hero-detail">${t.detail||''}</div>
        <span class="hero-badge ${badgeClass}">${t.snow_quality||'Ukjent'} skiføre</span>
      </div>
    </div>`;

  // ── Snø-metrikker
  document.getElementById('m-dybde').textContent     = s.dybde_cm!=null ? s.dybde_cm+' cm' : '–';
  document.getElementById('m-dybde-sub').textContent = s.ny_sno_48t_cm!=null ? '+'+s.ny_sno_48t_cm+' cm siste 48t' : '';
  document.getElementById('m-1t').textContent        = fmtDelta(s.endring_1t_cm);
  document.getElementById('m-3t').textContent        = fmtDelta(s.endring_3t_cm);
  document.getElementById('m-temp').textContent      = fmtTemp(s.temp_nå_c);
  document.getElementById('m-temp-sub').textContent  = s.min_temp_c!=null ? `min ${fmtTemp(s.min_temp_c)} / maks ${fmtTemp(s.maks_temp_c)}` : '';

  // ── Løyper
  const prepEl = document.getElementById('l-preparert');
  prepEl.textContent = lp.preparert != null ? lp.preparert+' segmenter' : '–';
  prepEl.style.color = lp.preparert > 0 ? '#15803d' : '#64748b';
  document.getElementById('l-aktive').textContent  = lp.aktive  != null ? lp.aktive+' segmenter'  : '–';
  document.getElementById('l-totalt').textContent  = lp.totalt  != null ? lp.totalt+' segmenter'  : '–';
  if(lp.sist_prep_timer!=null){
    document.getElementById('l-meta').textContent =
      `Sist preparert: ${lp.sist_prep_timer} t siden · ${lp.last_update||''}`;
  }

  // ── Prognose-strip
  const strip = document.getElementById('forecast');
  strip.innerHTML = (d.daglig||[]).map(dag => {
    const dt     = new Date(dag.dato+'T12:00:00');
    const navn   = DAYS[dt.getDay()]+' '+dt.getDate()+'.'+MONTHS[dt.getMonth()];
    const nySnø  = dag.ny_sno_cm > 0 ? `❄ +${dag.ny_sno_cm}cm` : '';
    const tempStr= `<span style="color:#ef4444">${dag.maks_temp_c>0?'+':''}${dag.maks_temp_c}°</span>`+
                   ` / <span style="color:#0284c7">${dag.min_temp_c}°</span>`;
    return `<div class="fday">
      <div class="fday-name">${navn}</div>
      <div class="fday-icon">${dag.vær_ikon||'–'}</div>
      <div class="fday-temp">${tempStr}</div>
      <div class="fday-snow">${nySnø}</div>
      <div class="fday-depth">${dag.snødybde_cm!=null?dag.snødybde_cm+' cm':''}</div>
    </div>`;
  }).join('');

  // ── Footer
  const ts = new Date(d.hentet);
  document.getElementById('footer').textContent =
    `Oppdatert: ${ts.toLocaleString('no-NO')} · Data: Yr, Frost, loyper.net`;
}

init();
// Auto-refresh hvert 10. minutt
setInterval(init, 10*60*1000);
</script>
</body>
</html>
"""
