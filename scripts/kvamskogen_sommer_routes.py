# -*- coding: utf-8 -*-
"""Kvamskogen sommermodul – turvær uten ski/snø/løyper."""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from flask import Blueprint, jsonify

try:
    from scripts.ver_routes import _hent_prognose_data
except ImportError:
    from ver_routes import _hent_prognose_data

kvamskogen_sommer_bp = Blueprint("kvamskogen_sommer", __name__, url_prefix="/kvamskogen-sommer")


def _is_sun(icon: str) -> bool:
    return any(s in (icon or "") for s in ["☀", "🌤", "⛅"])


def _analyser_dager(intervaller: list[dict]) -> list[dict]:
    per_dag = defaultdict(list)
    for iv in intervaller:
        d = (iv.get("start") or "")[:10]
        if d:
            per_dag[d].append(iv)

    dager = []
    for d in sorted(per_dag.keys())[:7]:
        ivs = per_dag[d]
        regn_timer = sum(1 for iv in ivs if float(iv.get("nedbor_mm") or 0) >= 0.2)
        sol_timer = sum(1 for iv in ivs if _is_sun(iv.get("ver_ikon") or ""))
        kraftig_vind_timer = sum(1 for iv in ivs if float(iv.get("vind_ms") or 0) >= 10)
        maks_temp = max([float(iv.get("temperatur_c") or 0) for iv in ivs] or [0])

        er_fin_dag = regn_timer == 0 and sol_timer >= 5 and kraftig_vind_timer == 0
        score = 5 if er_fin_dag else (4 if regn_timer == 0 and kraftig_vind_timer <= 1 else (3 if regn_timer <= 2 else 2))
        dager.append({
            "dato": d,
            "ukedag": ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"][date.fromisoformat(d).weekday()],
            "regn_timer": regn_timer,
            "sol_timer": sol_timer,
            "kraftig_vind_timer": kraftig_vind_timer,
            "maks_temp": round(maks_temp, 1),
            "score": score,
            "fin_turdag": er_fin_dag,
        })
    return dager


def _overskrift(dager: list[dict]) -> str:
    if not dager:
        return "Ingen turprognose tilgjengelig nå"
    topp = next((d for d in dager if d["fin_turdag"]), None)
    if topp:
        return f"{topp['ukedag'].capitalize()} blir en fin turdag: tørt og minst 5 timer sol"
    ok = next((d for d in dager if d["score"] >= 4), None)
    if ok:
        return f"{ok['ukedag'].capitalize()} ser best ut for tur i terrenget"
    return "Ustabilt turvær – se etter korte opphold uten regn og vind"


@kvamskogen_sommer_bp.get("/")
def index():
    return """
<!DOCTYPE html><html lang='no'><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
<title>Kvamskogen sommer</title><script src='https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js'></script>
<style>
:root{--bg:#f5f7fb;--surface:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;--hint:#94a3b8;--blue:#1d4ed8;--radius:14px;--shadow:0 2px 12px rgba(15,23,42,.07)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,sans-serif}
.page{max-width:1120px;margin:0 auto;padding:20px 16px 48px}.hero,.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;box-shadow:var(--shadow);margin-bottom:14px}
.row{display:grid;gap:10px}.day{border:1px solid #dbeafe;background:#f8fbff;border-radius:10px;padding:12px}.good{background:#ecfdf5;border-color:#a7f3d0}.ok{background:#eff6ff;border-color:#bfdbfe}
.section{font-size:11px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--hint);margin:18px 0 8px}
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.metric{background:#fff;border:1px solid var(--border);border-radius:10px;padding:12px}.m1{font-size:11px;color:var(--hint)}.m2{font-size:22px;font-weight:700}
@media(max-width:650px){.metric-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}
</style></head>
<body><div class='page'>
<div class='hero'><h1 style='margin:0'>🥾 Kvamskogen sommer – turvær</h1><p style='color:var(--muted)'>Samme visning som vintermodulen, men uten snø/løyper.</p><h2 id='overskrift'>Laster turanalyse…</h2></div>
<div class='section'>Værprognose – kommende timer</div><div class='card'><canvas id='fchart' height='110'></canvas></div>
<div class='section'>Turstatus</div><div class='metric-grid'>
<div class='metric'><div class='m1'>Beste dag</div><div class='m2' id='m-best'>–</div></div>
<div class='metric'><div class='m1'>Fine turdager</div><div class='m2' id='m-fine'>–</div></div>
<div class='metric'><div class='m1'>Maks temp (7d)</div><div class='m2' id='m-temp'>–</div></div>
<div class='metric'><div class='m1'>Regntimer i dag</div><div class='m2' id='m-regn'>–</div></div></div>
<div class='section'>Dag-for-dag</div><div class='card'><div id='dager' class='row'></div></div>
</div>
<script>
let chart=null;function mkChart(iv){const labels=iv.map(x=>new Date(x.start).toLocaleTimeString('no-NO',{hour:'2-digit',minute:'2-digit'}));
const t=iv.map(x=>x.temperatur_c??null),p=iv.map(x=>x.nedbor_mm??null),w=iv.map(x=>x.vind_ms??null);const c=document.getElementById('fchart').getContext('2d');if(chart)chart.destroy();
chart=new Chart(c,{data:{labels,datasets:[{type:'line',label:'Temp',data:t,borderColor:'#3b82f6',pointRadius:0,tension:.3,yAxisID:'y'},{type:'bar',label:'Regn',data:p,backgroundColor:'rgba(239,68,68,.5)',yAxisID:'y1'},{type:'line',label:'Vind',data:w,borderColor:'#8b5cf6',pointRadius:0,borderDash:[4,4],yAxisID:'y'}]},options:{plugins:{legend:{display:true}},scales:{y:{position:'left'},y1:{position:'right',grid:{drawOnChartArea:false},min:0}}}})}
async function load(){const r=await fetch('/kvamskogen-sommer/api/status');const data=await r.json();document.getElementById('overskrift').textContent=data.overskrift||'Ingen data';
const d=data.dager||[];document.getElementById('m-best').textContent=d[0]?.ukedag||'–';document.getElementById('m-fine').textContent=d.filter(x=>x.fin_turdag).length;document.getElementById('m-temp').textContent=(Math.max(...d.map(x=>x.maks_temp||-999))||'–')+'°C';document.getElementById('m-regn').textContent=d[0]?.regn_timer+' t';
const root=document.getElementById('dager');root.innerHTML='';d.forEach(x=>{const el=document.createElement('div');el.className=x.fin_turdag?'day good':(x.score>=4?'day ok':'day');el.innerHTML=`<b>${x.ukedag} ${x.dato}</b><br>Regn: ${x.regn_timer} t · Sol: ${x.sol_timer} t · Kraftig vind: ${x.kraftig_vind_timer} t · Maks temp: ${x.maks_temp}°C`;root.appendChild(el);});
mkChart((data.intervaller||[]).slice(0,24));}
load();
</script></body></html>
    """


@kvamskogen_sommer_bp.get("/api/status")
def status():
    try:
        data = _hent_prognose_data("Kvamskogen sommer", lat=60.397, lon=5.963, frost_id="SN50310")
        intervaller = data.get("intervaller", [])
        dager = _analyser_dager(intervaller)
        return jsonify({
            "ok": True,
            "overskrift": _overskrift(dager),
            "dager": dager,
            "intervaller": intervaller,
            "kriterier": "Fin dag = ingen regn og minst 5 timer sol, uten kraftig vind",
        })
    except Exception as exc:
        return jsonify({"ok": False, "overskrift": "Midlertidig feil i turmodulen", "dager": [], "error": str(exc)}), 200
