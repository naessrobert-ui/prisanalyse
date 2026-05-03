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
    <html><head><title>Kvamskogen sommer</title>
    <meta name='viewport' content='width=device-width, initial-scale=1'/>
    <style>
      body{font-family:Inter,Arial,sans-serif;background:#f3f4f6;padding:18px;color:#111827}
      .card{max-width:900px;margin:0 auto 14px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px}
      .muted{color:#6b7280}.row{display:grid;grid-template-columns:1fr;gap:10px}
      .day{border:1px solid #e5e7eb;border-radius:10px;padding:12px}
      .good{background:#ecfdf5;border-color:#a7f3d0}.ok{background:#eff6ff;border-color:#bfdbfe}
    </style></head>
    <body>
      <div class='card'>
        <h1 style='margin:0'>🥾 Kvamskogen sommer – turvær</h1>
        <p class='muted'>Fokus på tur i terrenget: opphold, lite vind og mye sol.</p>
        <h2 id='overskrift' style='margin:8px 0 0'>Laster turanalyse…</h2>
      </div>
      <div class='card'>
        <div id='dager' class='row'></div>
      </div>
      <script>
        async function load(){
          const r = await fetch('/kvamskogen-sommer/api/status');
          const data = await r.json();
          document.getElementById('overskrift').textContent = data.overskrift || 'Ingen data';
          const root = document.getElementById('dager');
          root.innerHTML='';
          (data.dager||[]).forEach(d=>{
            const cls = d.fin_turdag ? 'day good' : (d.score>=4 ? 'day ok' : 'day');
            const el=document.createElement('div');
            el.className=cls;
            el.innerHTML = `<b>${d.ukedag} ${d.dato}</b><br>
              Regn: ${d.regn_timer} t · Sol: ${d.sol_timer} t · Kraftig vind: ${d.kraftig_vind_timer} t · Maks temp: ${d.maks_temp}°C`;
            root.appendChild(el);
          });
        }
        load().catch(()=>{document.getElementById('overskrift').textContent='Kunne ikke laste turdata akkurat nå';});
      </script>
    </body></html>
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
            "kriterier": "Fin dag = ingen regn og minst 5 timer sol, uten kraftig vind",
        })
    except Exception as exc:
        return jsonify({"ok": False, "overskrift": "Midlertidig feil i turmodulen", "dager": [], "error": str(exc)}), 200
