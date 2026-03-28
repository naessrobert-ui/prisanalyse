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
ANTHROPIC_MODEL   = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """
Du er en lokal værvakt på Kvamskogen som skriver engasjerende meldinger til en hytteeier.
Skriv naturlig, muntlig norsk bokmål – varm og personlig tone.

Svar KUN med gyldig JSON (ingen markdown):
{"verdict":"...","detail":"...","snow_quality":"...","badge_color":"...","icon":"..."}

1. verdict (maks 10 ord): Situasjonen akkurat nå. Gjerne entusiastisk hvis forholdene er gode.

2. detail (100-200 ord) i 2-3 avsnitt:
   - Første avsnitt: Forholdene nå – snøtype, temperatur, hva det betyr for skiopplevelsen.
   - Andre avsnitt: Løypestatus – preparert? Nylig kjørt? Hva kan man forvente?
   - Tredje avsnitt: Fremtidsutsikter – analyser ALLE dagene i prognosen, ikke bare de nærmeste.
     Se etter gode vinduer selv om det er dårlige dager imellom.
     Drømmedagen = nysnø dagen før + oppholdsvær/sol + vind under 4 m/s + kald natt (min under -1°C).
     Nevn KONKRET hvilke dager som ser bra ut med ukedag og hva som gjør dem gode.
     Eks: "Etter regnværet onsdag snur det – torsdag og fredag har -3°C om natten, sol og lite vind."
     Ikke stopp analysen ved første dårlige dag – se hele uka.

3. snow_quality: "Utmerket" | "Godt" | "Moderat" | "Dårlig"
   - Utmerket: nysnø + sol/oppholdsvær + lite vind + under 0°C
   - Godt: under 0°C, snø siste 3 døgn, løyper preparert
   - Moderat: 0–3°C, våt snø, eller løyper ikke preparert
   - Dårlig: regn, over 3°C, kraftig vind, smelting

4. badge_color: "green" | "amber" | "red"
5. icon: ⛷️ 🎿 ☀️ 🌨️ 🌧️ 🌫️ 🥶 💧
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
        "loyper_preparert":     loyper_data.get("counts", {}).get("segments_freshly_groomed", 0),
        "sist_preparert_timer": round(loyper_data.get("updates", {}).get("newest_segment", {}).get("age_seconds", 0) / 3600, 1)
                                if loyper_data.get("updates", {}).get("newest_segment", {}).get("age_seconds") else None,
        "prognose_neste_dager": [
            {
                "dato":       d.get("dato"),
                "ukedag":     ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][
                                  __import__("datetime").date.fromisoformat(d.get("dato","2000-01-01")).weekday()
                              ] if d.get("dato") else "",
                "min_c":      d.get("min_temp_c"),
                "maks_c":     d.get("maks_temp_c"),
                "ny_sno_cm":  round(d.get("total_ny_snø_mm", 0) / 10, 1),
                "vind_ms":    d.get("vind_ms_snitt"),
                "ver":        d.get("vær_label"),
            }
            for d in sno_data.get("daglig", [])[:8]
        ],
    }, ensure_ascii=False)

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": ANTHROPIC_MODEL, "max_tokens": 1024,
                  "system": _SYSTEM_PROMPT,
                  "messages": [{"role": "user", "content": payload_str}]},
            timeout=45,
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
        return {"verdict": "Kald natt med fersk snø – ideelt skiføre",
                "detail": f"Temperaturen er {temp}°C og det har kommet {ny_sno:.1f} cm ny snø. Løypene er nylig preparert.",
                "snow_quality": "Utmerket", "badge_color": "green", "icon": "⛷️"}
    elif temp <= -3 and ny_sno > 3:
        return {"verdict": "Fersk snø og kaldt – godt føre",
                "detail": f"{ny_sno:.1f} cm ny snø og {temp}°C. Løypene er ikke nylig preparert.",
                "snow_quality": "Godt", "badge_color": "green", "icon": "🎿"}
    elif temp <= 0 and dybde > 10:
        return {"verdict": "Kaldt og stabilt – brukbart skiføre",
                "detail": f"Snødybden er {dybde} cm og temperaturen holder seg under null.",
                "snow_quality": "Godt", "badge_color": "green", "icon": "🎿"}
    elif 0 < temp <= 3:
        return {"verdict": "Mildt vær – snøen er våt og tung",
                "detail": f"Med {temp}°C blir snøen klissete. Bruk heller morgenøkten.",
                "snow_quality": "Moderat", "badge_color": "amber", "icon": "🌨️"}
    else:
        return {"verdict": "Smelting og mildt – dårlig skiføre",
                "detail": f"Temperaturen er {temp}°C. Snøen smelter raskt.",
                "snow_quality": "Dårlig", "badge_color": "red", "icon": "💧"}


# ── API-kall ──────────────────────────────────────────────────────────────────
# Lokalt: localhost:5000, på Render: prisanalyse.no
_IS_RENDER = os.getenv("RENDER") == "true"
_BASE = "https://prisanalyse.no" if _IS_RENDER else "http://localhost:5000"


def _hent_sno() -> dict:
    try:
        r = requests.get(f"{_BASE}/ver/api/snovarsel",
                         params={"stasjon": "Kvamskogen"}, timeout=45)
        r.raise_for_status()
        return r.json()
    except Exception:
        traceback.print_exc()
        return {}


def _hent_loyper() -> dict:
    try:
        r = requests.get(f"{_BASE}/ver/skiloyper-kvamskogen/stats",
                         params={"z": 13, "radius": 2, "fresh_hours": 12}, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        traceback.print_exc()
        return {}


@kvamskogen_bp.get("/")
def forside():
    return Response(_FORSIDE_HTML, mimetype="text/html; charset=utf-8")


_STATUS_CACHE: dict = {}
_STATUS_CACHE_TTL = 15 * 60
_STATUS_REFRESHING = False


def _refresh_cache():
    """Kjøres i bakgrunnen – oppdaterer cache uten å blokkere requester."""
    global _STATUS_REFRESHING
    if _STATUS_REFRESHING:
        return
    _STATUS_REFRESHING = True
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut_sno    = ex.submit(_hent_sno)
            fut_loyper = ex.submit(_hent_loyper)
            sno_data    = fut_sno.result(timeout=60)
            loyper_data = fut_loyper.result(timeout=25)

        tolkning = _ai_tolkning(sno_data, loyper_data)
        s      = sno_data.get("sammendrag", {})
        daglig = sno_data.get("daglig", [])

        payload = _build_payload(tolkning, sno_data, loyper_data, s, daglig)
        _STATUS_CACHE["status"] = {
            "expires_at": time.time() + _STATUS_CACHE_TTL,
            "payload": payload,
        }
    except Exception:
        traceback.print_exc()
    finally:
        _STATUS_REFRESHING = False


def _build_payload(tolkning, sno_data, loyper_data, s, daglig):
    import datetime as _dt
    return {
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
            "totalt":          loyper_data.get("counts", {}).get("segments_total", 0),
            "aktive":          loyper_data.get("counts", {}).get("segments_active", 0),
            "preparert":       loyper_data.get("counts", {}).get("segments_freshly_groomed", 0),
            "sist_prep_timer": round(loyper_data.get("updates", {}).get("newest_segment", {}).get("age_seconds", 0) / 3600, 1)
                               if loyper_data.get("updates", {}).get("newest_segment", {}).get("age_seconds") else None,
            "last_update":     loyper_data.get("updates", {}).get("latest_update_local"),
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
        "intervaller": [
            {
                "start":        iv.get("start"),
                "temperatur_c": iv.get("temperatur_c"),
                "nedbor_mm":    iv.get("nedbør_mm"),
                "vind_ms":      iv.get("vind_ms"),
                "ver_ikon":     iv.get("vær_ikon"),
                "timer":        iv.get("timer"),
            }
            for iv in sno_data.get("intervaller", [])
        ],
    }


@kvamskogen_bp.get("/api/status")
def api_status():
    import threading

    now_ts = time.time()
    hit = _STATUS_CACHE.get("status")

    if hit and hit["expires_at"] > now_ts:
        # Cache er gyldig – returner umiddelbart
        return jsonify(hit["payload"])

    if hit:
        # Cache er utløpt – returner gammel data og oppdater i bakgrunnen
        threading.Thread(target=_refresh_cache, daemon=True).start()
        return jsonify(hit["payload"])

    # Ingen cache – første gang, må vente
    _refresh_cache()
    hit = _STATUS_CACHE.get("status")
    if hit:
        return jsonify(hit["payload"])
    return jsonify({"hentet": datetime.now().isoformat(), "tolkning": _fallback_tolkning({}, {}),
                    "sno": {}, "loyper": {}, "daglig": [], "intervaller": []})


_HISTORIKK_CACHE: dict = {}
_HISTORIKK_CACHE_TTL = 15 * 60


@kvamskogen_bp.get("/api/historikk")
def api_historikk():
    try:
        hours = max(1, min(72, int(request.args.get("hours", 24))))
        now_ts = time.time()
        hit = _HISTORIKK_CACHE.get(hours)
        if hit and hit["expires_at"] > now_ts:
            return jsonify(hit["payload"])
        data = hent_historikk(hours)
        payload = {"ok": True, "data": data, "antall": len(data)}
        _HISTORIKK_CACHE[hours] = {"expires_at": now_ts + _HISTORIKK_CACHE_TTL, "payload": payload}
        return jsonify(payload)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Soloppgang/solnedgang ─────────────────────────────────────────────────────

def _sol_tider(dato_str: str) -> tuple:
    """Beregn soloppgang og solnedgang for Kvamskogen (60.4°N, 5.97°E)."""
    import math
    from datetime import date
    lat = math.radians(60.4)
    dato = date.fromisoformat(dato_str)
    dag_nr = dato.timetuple().tm_yday
    decl = math.radians(23.45 * math.sin(math.radians(360/365*(dag_nr-81))))
    cos_ha = (math.sin(math.radians(-0.833)) - math.sin(lat)*math.sin(decl)) / (math.cos(lat)*math.cos(decl))
    if abs(cos_ha) > 1:
        return None, None
    ha = math.degrees(math.acos(cos_ha))
    tz_offset = 2 if 3 <= dato.month <= 10 else 1
    noon = 12 - 5.97/15 + tz_offset
    def fmt(h):
        hh = int(h); mm = int((h-hh)*60)
        return f"{hh:02d}:{mm:02d}"
    return fmt(noon - ha/15), fmt(noon + ha/15)


def _analyser_dag(dato: str, intervaller: list, soloppgang: str, solnedgang: str) -> dict:
    """Analyser én dag og returner skianbefaling via AI."""
    if not ANTHROPIC_API_KEY:
        return _fallback_dag(dato, intervaller, soloppgang, solnedgang)

    prompt = f"""Analyser dette for skianbefalinger på Kvamskogen.
Dato: {dato}, Soloppgang: {soloppgang}, Solnedgang: {solnedgang}

Timedata (start, nedbør_mm, vind_ms, vær):
{json.dumps([{"t": iv["start"][11:16], "nedbor": iv.get("nedbor_mm",0), "vind": iv.get("vind_ms",0), "ver": iv.get("ver_ikon",""), "temp": iv.get("temperatur_c",0)} for iv in intervaller if iv.get("timer",1)<=1], ensure_ascii=False)}

Ranger forholdene og finn beste tidsvindu mellom soloppgang og solnedgang.

REGLER:
- Regn = dårlig (unngå)
- Lett snøvær = greit
- Vind > 8 m/s = ubehagelig
- Sol + lite vind = perfekt
- Mørkt = ikke aktuelt (utenfor soloppgang-solnedgang)

Svar KUN med JSON:
{{"score": 1-5, "kort": "En linje, maks 12 ord", "beste_tid": "f.eks. 10:00-14:00 eller null", "detalj": "2-3 setninger med konkret anbefaling og begrunnelse"}}

Score: 5=perfekt, 4=bra, 3=greit, 2=dårlig, 1=ikke gå ut"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": ANTHROPIC_MODEL, "max_tokens": 256,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        return json.loads(text)
    except Exception:
        return _fallback_dag(dato, intervaller, soloppgang, solnedgang)


def _fallback_dag(dato: str, intervaller: list, soloppgang: str, solnedgang: str) -> dict:
    """Smartere regelbasert analyse – ser på perioder og overganger."""

    def t_til_min(t):
        if not t or ':' not in t: return 0
        h, m = t.split(':'); return int(h)*60+int(m)

    sol_opp_min = t_til_min(soloppgang)
    sol_ned_min = t_til_min(solnedgang)

    def iv_min(iv):
        return t_til_min((iv.get("start") or "")[11:16])

    def iv_type(iv):
        nb  = iv.get("nedbor_mm") or 0
        temp = iv.get("temperatur_c") or 0
        if nb < 0.15: return "tørt"
        if temp > 1.5: return "regn"
        if 0 < temp <= 1.5: return "sludd"
        return "snø"

    def iv_label(iv):
        nb   = iv.get("nedbor_mm") or 0
        vind = iv.get("vind_ms") or 0
        t    = iv_type(iv)
        ikon = iv.get("ver_ikon") or ""
        sol  = any(s in ikon for s in ["☀","🌤","⛅"])
        if t == "tørt":
            if sol:   return "sol"
            if vind > 6: return "oppholdsvær, vind"
            return "oppholdsvær"
        if t == "regn":
            return "kraftig regn" if nb > 2 else "lett regn"
        if t == "sludd": return "sludd"
        return "moderat snøvær" if nb > 1 else "lett snøvær"

    # Kun timer i dagslys
    dagslys = [iv for iv in intervaller
               if sol_opp_min <= iv_min(iv) <= sol_ned_min] or intervaller

    # Grupper sammenhengende intervaller med samme label
    def finn_perioder(ivs):
        if not ivs: return []
        perioder, start_iv, prev = [], ivs[0], iv_label(ivs[0])
        for iv in ivs[1:]:
            curr = iv_label(iv)
            if curr != prev:
                perioder.append((start_iv, iv, prev))
                start_iv, prev = iv, curr
        perioder.append((start_iv, ivs[-1], prev))
        return perioder

    perioder = finn_perioder(dagslys)

    # Beskriv perioder
    deler = []
    for start_iv, slutt_iv, label in perioder:
        st = (start_iv.get("start") or "")[11:16]
        sl = (slutt_iv.get("start") or "")[11:16]
        if st == sl:
            deler.append(f"{st}: {label}")
        else:
            deler.append(f"{st}–{sl}: {label}")

    # Score
    n = max(len(dagslys), 1)
    regn_n = sum(1 for iv in dagslys if iv_type(iv) == "regn")
    sol_n  = sum(1 for iv in dagslys if "sol" in iv_label(iv))
    vind_snitt = sum(iv.get("vind_ms") or 0 for iv in dagslys) / n

    if regn_n / n > 0.6:   score = 1
    elif regn_n / n > 0.3: score = 2
    elif sol_n / n > 0.4 and vind_snitt < 5: score = 5
    elif sol_n / n > 0.2 and vind_snitt < 6: score = 4
    else:                   score = 3

    score_tekst = {5:"Strålende dag – perfekt skitur!",4:"Fin dag med gode forhold",
                   3:"Greit skiføre",2:"En del regn, noen pauser",1:"Mye regn – bli hjemme"}

    # Finn beste vindu
    gode = [iv for iv in intervaller
            if iv_type(iv) != "regn" and (iv.get("vind_ms") or 0) < 8
            and sol_opp_min <= iv_min(iv) <= sol_ned_min]
    beste_tid = None
    if gode:
        s = (gode[0].get("start") or "")[11:16]
        e = (gode[-1].get("start") or "")[11:16]
        if s != e: beste_tid = f"{s}–{e}"

    detalj = ". ".join(deler) + "."
    if vind_snitt > 7: detalj += " Merk: sterk vind hele dagen."
    elif sol_n / n > 0.4: detalj += " 🌟"

    # Legg til konkret anbefaling
    if score == 5:
        detalj += f" Bare å stikke ut – hele dagen fra {soloppgang} er fin!"
    elif score == 4:
        detalj += f" Anbefaler {beste_tid or soloppgang+'–'+solnedgang}."
    elif score == 3 and beste_tid:
        detalj += f" Det ser lovende ut mellom {beste_tid} – ellers grått."
    elif score == 2 and beste_tid:
        detalj += f" Har du lyst, bruk vinduet {beste_tid} – men forvent vått."
    elif score == 1:
        detalj += " Bli heller hjemme i dag."

    return {"score": score, "kort": score_tekst[score],
            "beste_tid": beste_tid or f"{soloppgang}–{solnedgang}",
            "detalj": detalj}


_SKITUR_AI_CACHE: dict = {}
_SKITUR_AI_CACHE_TTL = 15 * 60

_SKITUR_AI_PROMPT = """Du er en lokal turguide på Kvamskogen. Analyser timedata og gi korte, konkrete turanbefalinger på norsk bokmål.

For hver dag:
1. Se på HELE dagen time for time – ikke bare snittverdier
2. Finn når vinden tiltar eller avtar (viktig å si kl XX blir det sterkere vind)
3. Finn perioden med minst nedbør mellom soloppgang og solnedgang
4. Skill mellom regn (temp>1.5°C), sludd (0-1.5°C) og snø (<0°C)
5. Skriv "detalj" som én konkret setning som oppsummerer dagen – dette er det viktigste feltet
6. Skriv "kort" som en 4-6 ords komprimering av detalj (ikke en separat vurdering)

Eksempel på god detalj: "Rolig morgen med lett snø til kl 10, deretter kraftig vind 8–10 m/s og tett snøfall fra 13."
Eksempel på tilhørende kort: "Rolig morgen, vind fra 13"

Svar KUN med gyldig JSON-array (ingen markdown):
[{"dato":"YYYY-MM-DD","score":1-5,"kort":"4-6 ord fra detalj","beste_tid":"HH:MM–HH:MM eller null","detalj":"1 konkret setning med timing"}]

Score: 5=sol+lite vind hele dagen, 4=bra forhold, 3=variabelt/greit, 2=mye nedbør men pauser, 1=bli hjemme
Beste tidsvindu: perioden med minst nedbør OG akseptabel vind (<8 m/s) mellom soloppgang og solnedgang."""


@kvamskogen_bp.get("/api/skitur-ai")
def api_skitur_ai():
    """AI-analyse av alle skidager i ett kall."""
    now_ts = time.time()
    hit = _SKITUR_AI_CACHE.get("skitur_ai")
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    if not ANTHROPIC_API_KEY:
        return jsonify({"ok": False, "dager": []})

    status_hit = _STATUS_CACHE.get("status")
    if not status_hit:
        return jsonify({"ok": False, "dager": []})

    intervaller_raw = status_hit["payload"].get("intervaller", [])
    if not intervaller_raw:
        return jsonify({"ok": False, "dager": []})

    from collections import defaultdict
    import datetime as _dt

    per_dag: dict = defaultdict(list)
    for iv in intervaller_raw:
        dato = (iv.get("start") or "")[:10]
        if dato:
            per_dag[dato].append(iv)

    # Bygg kompakt dagsoversikt for AI
    dager_payload = []
    for dato in sorted(per_dag.keys())[:7]:
        sol_opp, sol_ned = _sol_tider(dato)
        dt = _dt.date.fromisoformat(dato)
        ukedag = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][dt.weekday()]
        ivs = per_dag[dato]
        dager_payload.append({
            "dato": dato,
            "ukedag": ukedag,
            "soloppgang": sol_opp,
            "solnedgang": sol_ned,
            "timer": [
                {
                    "t": (iv.get("start") or "")[11:16],
                    "nb": round(iv.get("nedbor_mm") or 0, 1),
                    "temp": round(iv.get("temperatur_c") or 0, 1),
                    "vind": round(iv.get("vind_ms") or 0, 1),
                    "ikon": iv.get("ver_ikon") or ""
                }
                for iv in ivs
            ]
        })

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "system": _SKITUR_AI_PROMPT,
                "messages": [{"role": "user", "content": json.dumps(dager_payload, ensure_ascii=False)}]
            },
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        ai_dager = json.loads(text)

        # Legg til ukedag og sol-tider
        for dag in ai_dager:
            dato = dag.get("dato", "")
            if dato:
                dt = _dt.date.fromisoformat(dato)
                dag["ukedag"] = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][dt.weekday()]
                dag["soloppgang"], dag["solnedgang"] = _sol_tider(dato)

        payload = {"ok": True, "dager": ai_dager}
        _SKITUR_AI_CACHE["skitur_ai"] = {"expires_at": now_ts + _SKITUR_AI_CACHE_TTL, "payload": payload}
        return jsonify(payload)

    except Exception:
        traceback.print_exc()
        return jsonify({"ok": False, "dager": []})


@kvamskogen_bp.get("/api/skitur")
def api_skitur():
    now_ts = time.time()
    hit = _SKITUR_CACHE.get("skitur")
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    # Gjenbruk status-cache
    status_hit = _STATUS_CACHE.get("status")
    if not status_hit:
        return jsonify({"ok": False, "dager": []})

    intervaller_raw = status_hit["payload"].get("intervaller", [])
    if not intervaller_raw:
        return jsonify({"ok": False, "dager": []})

    from collections import defaultdict
    per_dag: dict = defaultdict(list)
    for iv in intervaller_raw:
        dato = (iv.get("start") or "")[:10]
        if dato:
            per_dag[dato].append(iv)

    dager = []
    for dato in sorted(per_dag.keys())[:8]:
        ivs = per_dag[dato]
        sol_opp, sol_ned = _sol_tider(dato)
        analyse = _fallback_dag(dato, ivs, sol_opp or "07:00", sol_ned or "20:00")
        from datetime import date as _date
        dt = _date.fromisoformat(dato)
        ukedag = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][dt.weekday()]
        dager.append({
            "dato": dato, "ukedag": ukedag,
            "soloppgang": sol_opp, "solnedgang": sol_ned,
            "score": analyse.get("score", 3),
            "kort": analyse.get("kort", ""),
            "beste_tid": analyse.get("beste_tid"),
            "detalj": analyse.get("detalj", ""),
        })

    payload = {"ok": True, "dager": dager}
    _SKITUR_CACHE["skitur"] = {"expires_at": now_ts + _SKITUR_CACHE_TTL, "payload": payload}
    return jsonify(payload)


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
.hero-detail{font-size:14px;color:var(--muted);line-height:1.6;}
.hero-detail-short{display:block;}
.hero-detail-full{display:none;margin-top:8px;}
.hero-detail-full.open{display:block;}
.les-mer{font-size:12px;color:var(--blue);cursor:pointer;border:none;background:none;padding:4px 0;text-decoration:underline;}
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
.ski-day{border-radius:14px;margin-bottom:10px;box-shadow:var(--shadow);overflow:hidden;}
.ski-day-header{display:flex;align-items:center;gap:0;cursor:pointer;user-select:none;border-radius:14px;overflow:hidden;}
.ski-day-left{width:88px;min-height:80px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;flex-shrink:0;padding:12px 8px;}
.ski-day-left .score-5{background:linear-gradient(135deg,#fbbf24,#f59e0b);}
.ski-day-left .score-4{background:linear-gradient(135deg,#34d399,#10b981);}
.ski-day-left .score-3{background:linear-gradient(135deg,#93c5fd,#60a5fa);}
.ski-day-left .score-2{background:linear-gradient(135deg,#94a3b8,#64748b);}
.ski-day-left .score-1{background:linear-gradient(135deg,#fca5a5,#ef4444);}
.ski-day-icon-big{font-size:32px;line-height:1;}
.ski-day-name-top{font-size:11px;font-weight:700;color:rgba(255,255,255,0.9);text-transform:uppercase;letter-spacing:0.5px;}
.ski-day-right{flex:1;background:var(--surface);padding:12px 16px;border-left:none;min-height:80px;display:flex;flex-direction:column;justify-content:center;gap:4px;}
.ski-day-right:hover{background:#f8fafc;}
.ski-day-title{font-size:15px;font-weight:600;color:var(--text);}
.ski-day-kort{font-size:13px;color:var(--muted);}
.ski-day-stats-inline{font-size:11px;color:var(--hint);font-weight:400;margin-left:6px;letter-spacing:0.2px;}
.ski-day-stats{font-size:11px;color:var(--hint);margin-top:3px;letter-spacing:0.2px;}
.ski-day-meta{display:flex;align-items:center;gap:10px;margin-top:4px;}
.ski-beste-tid{font-size:12px;font-weight:600;color:var(--blue);}
.ski-arrow{font-size:12px;color:var(--hint);margin-left:auto;}
.ski-day-detail{padding:0 16px 12px 100px;}
.ski-day-detail-text{font-size:13px;color:var(--muted);line-height:1.6;}
.ski-day-sol{font-size:11px;color:var(--hint);margin-top:4px;}
.ski-day-chart-section{background:#0f172a;border-radius:0 0 14px 14px;}
.ski-day-chart-wrap{padding:0 12px 12px;height:180px;position:relative;}
.ski-day-chart-msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#475569;font-size:12px;}
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
  <div class="hero" id="hero">
    <div class="hero-top">
      <div class="hero-icon">🏔️</div>
      <div class="hero-text">
        <div class="hero-verdict">Kvamskogen</div>
        <div class="hero-detail"><span class="hero-detail-short">Henter værstatus…</span></div>
        <span class="hero-badge badge-amber">Laster…</span>
      </div>
    </div>
  </div>

  <div class="section-label">Værprognose – kommende timer</div>
  <div style="background:#0f172a;border-radius:var(--radius);padding:16px 18px;margin-bottom:4px;">
    <div class="chart-hours" style="margin-bottom:8px;">
      <button class="active" onclick="setFcast(12,this)" style="background:#334155;color:#e2e8f0;border-color:#334155;">12t</button>
      <button onclick="setFcast(24,this)" style="background:#1e293b;color:#94a3b8;border-color:#1e293b;">24t</button>
      <button onclick="setFcast(48,this)" style="background:#1e293b;color:#94a3b8;border-color:#1e293b;">48t</button>
      <button onclick="setFcast(168,this)" style="background:#1e293b;color:#94a3b8;border-color:#1e293b;">7 dager</button>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:8px;">
      <span class="leg"><span class="leg-line" style="background:#4dabf7"></span><span style="color:#7b8db5;font-size:11px;">Temp (°C)</span></span>
      <span class="leg"><span class="leg-bar" style="background:rgba(255,107,107,0.6)"></span><span style="color:#7b8db5;font-size:11px;">Nedbør varm</span></span>
      <span class="leg"><span class="leg-bar" style="background:rgba(116,192,252,0.6)"></span><span style="color:#7b8db5;font-size:11px;">Nedbør kald</span></span>
      <span class="leg"><span class="leg-line" style="background:#a78bfa"></span><span style="color:#7b8db5;font-size:11px;">Vind (m/s)</span></span>
    </div>
    <div id="fcast-icons" style="display:flex;overflow-x:auto;gap:0;margin-bottom:6px;min-height:28px;"></div>
    <div style="position:relative;height:200px;">
      <canvas id="fcast-chart"></canvas>
      <div id="fcast-msg" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#7b8db5;font-size:13px;"><span class="spinner"></span></div>
    </div>
  </div>

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
  <div class="loyper-card" id="loyper-card">
    <div style="font-size:15px;font-weight:600;color:var(--text);margin-bottom:4px;" id="l-main">–</div>
    <div style="font-size:13px;color:var(--muted);" id="l-sub"></div>
    <div style="font-size:12px;color:var(--hint);margin-top:4px;" id="l-sno-siden-prep"></div>
    <div style="font-size:13px;font-weight:500;margin-top:8px;padding-top:8px;border-top:1px solid var(--border);" id="l-vurdering"></div>
    <div style="margin-top:10px;padding-top:8px;border-top:1px solid var(--border);">
      <a href="/ver/skiloyper-kvamskogen" style="font-size:13px;color:var(--blue);">🗺️ Se løypekartet →</a>
    </div>
  </div>
  <div class="section-label">Når kan jeg gå på tur?</div>
  <div style="font-size:12px;color:var(--hint);margin-bottom:10px;">Klikk på en dag for timedetaljer</div>
  <div id="skitur-list"><div style="color:var(--hint);font-size:13px;padding:8px 0"><span class="spinner"></span> Laster skidager…</div></div>
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
let fcastData=[],fcastHours=12,fcastChart=null;
let histData=[],currentHours=12,histChart=null;

function setFcast(h,btn){
  fcastHours=h;
  document.querySelectorAll('[onclick^="setFcast"]').forEach(el=>{
    el.style.background='#1e293b';el.style.color='#94a3b8';
  });
  if(btn){btn.style.background='#334155';btn.style.color='#e2e8f0';}
  renderFcast();
}

function renderFcast(){
  if(!fcastData.length)return;
  const msg=document.getElementById('fcast-msg');
  const now=new Date();
  const cutoff=new Date(now.getTime()+fcastHours*3600*1000);
  let data=fcastData.filter(x=>new Date(x.start)>=new Date(now.getTime()-3600*1000)&&new Date(x.start)<=cutoff);
  if(fcastHours>48) data=data.filter(x=>x.timer>=6);
  if(!data.length)return;

  const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];

  // Vær-ikoner rad
  const iconsEl=document.getElementById('fcast-icons');
  if(iconsEl){
    const step=Math.max(1,Math.floor(data.length/16));
    iconsEl.innerHTML=data.filter((_,i)=>i%step===0).map(x=>{
      const dt=new Date(x.start);
      const lbl=String(dt.getHours()).padStart(2,'0')+':00';
      const ikon=x.ver_ikon||'';
      return`<div style="flex:1;min-width:32px;text-align:center;"><div style="font-size:14px;">${ikon}</div><div style="font-size:9px;color:#475569;">${lbl}</div></div>`;
    }).join('');
  }

  const labels=data.map(x=>{
    const dt=new Date(x.start);const h=dt.getHours();
    if(h===0||fcastHours>24)return dt.getDate()+'.'+MS[dt.getMonth()]+(h===0?'':' '+String(h).padStart(2,'0'));
    return String(h).padStart(2,'0')+':00';
  });
  const temps=data.map(x=>x.temperatur_c!=null?parseFloat(x.temperatur_c):null);
  const precip=data.map(x=>x.nedbor_mm!=null?parseFloat(x.nedbor_mm):null);
  const precipColors=data.map(x=>{
    const t=x.temperatur_c!=null?parseFloat(x.temperatur_c):1;
    return t<=0?'rgba(116,192,252,0.6)':'rgba(255,107,107,0.6)';
  });
  const wind=data.map(x=>x.vind_ms!=null?parseFloat(x.vind_ms):null);

  const ctx=document.getElementById('fcast-chart').getContext('2d');
  if(fcastChart)fcastChart.destroy();
  if(msg)msg.style.display='none';

  fcastChart=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør (mm)',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yP',order:3},
      {type:'line',label:'Temperatur (°C)',data:temps,borderWidth:2,pointRadius:0,pointHoverRadius:4,tension:0.3,fill:false,yAxisID:'yT',order:1,
        segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind (m/s)',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroundColor:'transparent',
        borderWidth:1.5,borderDash:[3,3],pointRadius:0,pointHoverRadius:3,tension:0.3,fill:false,yAxisID:'yW',order:2},
    ]},
    options:{
      responsive:true,maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:false},
        tooltip:{backgroundColor:'rgba(8,14,31,.95)',titleColor:'#dce4f5',bodyColor:'#7b8db5',
          callbacks:{label:c=>{
            if(c.parsed.y==null)return null;
            if(c.dataset.label.includes('Temp'))return`Temp: ${c.parsed.y>0?'+':''}${c.parsed.y.toFixed(1)}°C`;
            if(c.dataset.label.includes('Vind'))return`Vind: ${c.parsed.y.toFixed(1)} m/s`;
            return`Nedbør: ${c.parsed.y.toFixed(1)} mm`;
          }}}
      },
      scales:{
        x:{ticks:{color:'#7b8db5',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:14},grid:{color:'rgba(100,130,200,.06)'}},
        yT:{position:'left',ticks:{color:'#7b8db5',font:{size:10},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(100,130,200,.06)'},
          afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:10},callback:v=>v+' mm'},grid:{drawOnChartArea:false}},
        yW:{display:false,min:0},
      }
    }
  });
}



// ── Skitur-anbefalinger (bygges fra status-data, ingen ekstra kall) ──
const SCORE_EMOJI = {5:'☀️', 4:'🌤️', 3:'🌨️', 2:'🌧️', 1:'🌧️'};
const UKEDAGER = ['søndag','mandag','tirsdag','onsdag','torsdag','fredag','lørdag'];

function solTider(datoStr) {
  // Enkel soloppgang/solnedgang for Kvamskogen (60.4°N)
  const dato = new Date(datoStr+'T12:00:00');
  const dag = Math.floor((dato - new Date(dato.getFullYear(),0,0)) / 86400000);
  const decl = 23.45 * Math.sin((360/365*(dag-81)) * Math.PI/180) * Math.PI/180;
  const lat = 60.4 * Math.PI/180;
  const cosHA = (Math.sin(-0.833*Math.PI/180) - Math.sin(lat)*Math.sin(decl)) / (Math.cos(lat)*Math.cos(decl));
  if(Math.abs(cosHA)>1) return ['--:--','--:--'];
  const ha = Math.acos(cosHA) * 180/Math.PI;
  const tz = dato.getMonth()>=2 && dato.getMonth()<=9 ? 2 : 1;
  const noon = 12 - 5.97/15 + tz;
  const fmt = h => { const hh=Math.floor(h); const mm=Math.round((h-hh)*60); return String(hh).padStart(2,'0')+':'+String(mm).padStart(2,'0'); };
  return [fmt(noon-ha/15), fmt(noon+ha/15)];
}

function analyserDag(dato, ivs, solOpp, solNed) {
  function tTilMin(t){ if(!t||!t.includes(':'))return 0; const[h,m]=t.split(':');return+h*60+ +m; }
  const solOppMin=tTilMin(solOpp), solNedMin=tTilMin(solNed);
  function ivMin(iv){ return tTilMin((iv.start||'').substring(11,16)); }

  function ivType(iv){
    const nb=iv.nedbor_mm||0, temp=iv.temperatur_c||0;
    if(nb<0.15) return 'tørt';
    if(temp>1.5) return 'regn';
    if(temp>0)   return 'sludd';
    return 'snø';
  }
  function ivLabel(iv){
    const nb=iv.nedbor_mm||0, vind=iv.vind_ms||0, t=ivType(iv);
    const ikon=iv.ver_ikon||'';
    const sol=['☀','🌤','⛅'].some(s=>ikon.includes(s));
    if(t==='tørt') return sol?'sol':(vind>6?'oppholdsvær, vind':'oppholdsvær');
    if(t==='regn') return nb>2?'kraftig regn':'lett regn';
    if(t==='sludd') return 'sludd';
    return nb>1?'moderat snøvær':'lett snøvær';
  }

  // Kun dagslys
  const dagslys=ivs.filter(iv=>ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin);
  const base=dagslys.length?dagslys:ivs;
  const n=Math.max(base.length,1);

  // Grupper sammenhengende perioder
  const perioder=[];
  if(base.length){
    let startIv=base[0], prevLabel=ivLabel(base[0]);
    for(let i=1;i<base.length;i++){
      const curr=ivLabel(base[i]);
      if(curr!==prevLabel){
        perioder.push({start:(startIv.start||'').substring(11,16), slutt:(base[i].start||'').substring(11,16), label:prevLabel});
        startIv=base[i]; prevLabel=curr;
      }
    }
    perioder.push({start:(startIv.start||'').substring(11,16), slutt:(base[base.length-1].start||'').substring(11,16), label:prevLabel});
  }

  // Finn beste vindu (ikke regn, vind<8, i dagslys)
  const gode=ivs.filter(iv=>ivType(iv)!=='regn'&&(iv.vind_ms||0)<8&&ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin);
  let besteTid=null;
  if(gode.length>1){
    const s=(gode[0].start||'').substring(11,16);
    const e=(gode[gode.length-1].start||'').substring(11,16);
    if(s!==e) besteTid=`${s}–${e}`;
  }

  // Score
  const regnN=base.filter(iv=>ivType(iv)==='regn').length;
  const solN=base.filter(iv=>ivLabel(iv)==='sol').length;
  const vindSnitt=base.reduce((a,iv)=>a+(iv.vind_ms||0),0)/n;
  let score;
  if(regnN/n>0.6) score=1;
  else if(regnN/n>0.3) score=2;
  else if(solN/n>0.4&&vindSnitt<5) score=5;
  else if(solN/n>0.2&&vindSnitt<6) score=4;
  else score=3;

  const kortTekst={5:'Strålende dag – perfekt skitur!',4:'Fin dag med gode forhold',3:'Greit skiføre',2:'En del regn, noen pauser',1:'Mye regn – bli hjemme'};

  // Finn lavest nedbør-vindu i dagslys (2+ timer sammenhengende)
  let lavNedbørVindu=null;
  if(dagslys.length>1){
    let bestSnitt=999, bestS=null, bestE=null;
    for(let i=0;i<dagslys.length-1;i++){
      for(let j=i+1;j<dagslys.length;j++){
        const utsnitt=dagslys.slice(i,j+1);
        const snitt=utsnitt.reduce((a,iv)=>a+(iv.nedbor_mm||0),0)/utsnitt.length;
        if(snitt<bestSnitt&&utsnitt.length>=2){
          bestSnitt=snitt;
          bestS=(dagslys[i].start||'').substring(11,16);
          bestE=(dagslys[j].start||'').substring(11,16);
        }
      }
    }
    if(bestS&&bestE&&bestS!==bestE) lavNedbørVindu=`${bestS}–${bestE}`;
  }

  // Kortfattet konklusjon – ikke lang periodebeskrivelse
  let detalj='';
  if(score===5){
    detalj=`Sol og lite vind hele dagen. Bare å stikke ut fra ${solOpp}! 🌟`;
  } else if(score===4){
    detalj=`Gode forhold med sol i perioder.${besteTid?' Anbefalt tid: '+besteTid+'.':''}`;
  } else if(score===3){
    const vindKomm=vindSnitt>7?' Merk: en del vind.':'';
    if(lavNedbørVindu) detalj=`Variabelt vær, men minst nedbør mellom ${lavNedbørVindu}. Da er det best å være ute.${vindKomm}`;
    else detalj=`Greit skiføre gjennom dagen.${vindKomm}${besteTid?' Beste tid: '+besteTid+'.':''}`;
  } else if(score===2){
    if(besteTid) detalj=`En del regn, men vinduet ${besteTid} kan utnyttes hvis du vil ut.`;
    else detalj='Mye nedbør – vanskelig å finne gode vinduer.';
  } else {
    detalj='Det regner mesteparten av dagen. Bli heller hjemme.';
  }

  return {score, kort:kortTekst[score], besteTid:besteTid||(solOpp+'–'+solNed), detalj};
}

function dagStats(ivs, solOpp, solNed) {
  function tTilMin(t){ if(!t||!t.includes(':'))return 0; const[h,m]=t.split(':');return+h*60+ +m; }
  const solOppMin=tTilMin(solOpp), solNedMin=tTilMin(solNed);
  function ivMin(iv){ return tTilMin((iv.start||'').substring(11,16)); }

  let regnMm=0, snoMm=0, vindSum=0, vindMaks=0, solTimer=0;
  let n=0;
  for(const iv of ivs){
    const nb=iv.nedbor_mm||0, temp=iv.temperatur_c||0, vind=iv.vind_ms||0;
    const ikon=iv.ver_ikon||'';
    const t=iv.timer||1;
    if(temp>1.5) regnMm+=nb; else snoMm+=nb;
    vindSum+=vind; n++;
    if(vind>vindMaks) vindMaks=vind;
    if(['☀','🌤','⛅'].some(s=>ikon.includes(s))&&ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin) solTimer+=t;
  }
  const vindSnitt=n?vindSum/n:0;
  return {
    regnMm: Math.round(regnMm*10)/10,
    snoMm:  Math.round(snoMm*10)/10,
    totMm:  Math.round((regnMm+snoMm)*10)/10,
    vindSnitt: Math.round(vindSnitt*10)/10,
    vindMaks:  Math.round(vindMaks*10)/10,
    solTimer:  Math.round(solTimer),
  };
}


function buildSkitur(intervaller, daglig) {
  window._skiIntervaller = intervaller;
  const el = document.getElementById('skitur-list');
  if(!el) return;

  if(!intervaller||!intervaller.length){
    setTimeout(()=>{
      fetch('/kvamskogen/api/status').then(r=>r.json()).then(d=>{
        if(d.intervaller&&d.intervaller.length) buildSkitur(d.intervaller, d.daglig||[]);
      }).catch(()=>{});
    }, 3000);
    return;
  }

  // Vis regelbasert umiddelbart
  renderSkiturLokal(intervaller, el);

  // Oppdater stille med AI i bakgrunnen
  fetch('/kvamskogen/api/skitur-ai')
    .then(r=>r.json())
    .then(d=>{
      if(d.ok && d.dager && d.dager.length){
        renderSkiturDager(d.dager, el, intervaller);
      }
    })
    .catch(()=>{});
}

function renderSkiturLokal(intervaller, el) {
  const perDag={};
  intervaller.forEach(iv=>{
    const dato=(iv.start||'').substring(0,10);
    if(dato){if(!perDag[dato])perDag[dato]=[];perDag[dato].push(iv);}
  });
  const datoer=Object.keys(perDag).sort().slice(0,8);
  if(!datoer.length){el.innerHTML='';return;}

  const dager=datoer.map(dato=>{
    const[solOpp,solNed]=solTider(dato);
    const a=analyserDag(dato,perDag[dato],solOpp,solNed);
    const dt=new Date(dato+'T12:00:00');
    return{dato,ukedag:UKEDAGER[dt.getDay()],soloppgang:solOpp,solnedgang:solNed,...a,beste_tid:a.besteTid};
  });
  renderSkiturDager(dager, el, intervaller);
}
function renderSkiturDager(dager, el, intervaller) {
  if(!dager||!dager.length){el.innerHTML='';return;}

  // Bygg per-dag intervall-lookup
  const perDag={};
  (intervaller||[]).forEach(iv=>{
    const dato=(iv.start||'').substring(0,10);
    if(dato){if(!perDag[dato])perDag[dato]=[];perDag[dato].push(iv);}
  });

  el.innerHTML = dager.map((dag,i) => {
    const dato=dag.dato, solOpp=dag.soloppgang||'07:00', solNed=dag.solnedgang||'20:00';
    const score=dag.score||3, kort=dag.kort||'', detalj=dag.detalj||'';
    const besteTid=dag.beste_tid||dag.besteTid||null;
    const ukedag=dag.ukedag||'';
    const dagStr=ukedag.charAt(0).toUpperCase()+ukedag.slice(1);
    const datoStr=(dato||'').slice(5).replace('-','.');
    const emoji=SCORE_EMOJI[score]||'🌨️';
    const tidHtml=besteTid?`<span class="ski-beste-tid">⏰ ${besteTid}</span>`:'';

    // Daglig statistikk
    const ivs=perDag[dato]||[];
    const st=ivs.length?dagStats(ivs,solOpp,solNed):null;
    let statsHtml='';
    if(st){
      const nbParts=[];
      if(st.snoMm>0) nbParts.push(`❄️ ${st.snoMm}mm`);
      if(st.regnMm>0) nbParts.push(`🌧️ ${st.regnMm}mm`);
      const nbStr=nbParts.length?nbParts.join(' '):'Tørt';
      const vindStr=`💨 ${st.vindSnitt}/${st.vindMaks} m/s`;
      const solStr=st.solTimer>0?`☀️ ${st.solTimer}t`:'';
      statsHtml=`<div class="ski-day-stats">${nbStr} &nbsp;${vindStr}${solStr?' &nbsp;'+solStr:''}</div>`;
    }
    return `<div class="ski-day">
      <div class="ski-day-header" onclick="toggleSkiDay(${i})">
        <div class="ski-day-left score-${score}">
          <div class="ski-day-icon-big">${emoji}</div>
          <div class="ski-day-name-top">${dagStr}</div>
        </div>
        <div class="ski-day-right">
          <div class="ski-day-title">${dagStr} ${datoStr}${st?` <span class="ski-day-stats-inline">${st.snoMm>0?'❄️ '+st.snoMm+'mm':''}${st.regnMm>0?(st.snoMm>0?' · ':'')+'🌧️ '+st.regnMm+'mm':''} · 💨 ${st.vindSnitt}/${st.vindMaks}m/s${st.solTimer>0?' · ☀️ '+st.solTimer+'t':''}</span>`:''}</div>
          <div class="ski-day-kort">${kort}</div>
          <div class="ski-day-kort" style="margin-top:4px;color:var(--text);opacity:0.75;">${detalj}</div>
          <div class="ski-day-meta">
            ${tidHtml}
            <span class="ski-arrow" id="ski-arrow-${i}">▾ Graf</span>
          </div>
        </div>
      </div>
      <div class="ski-day-detail" id="ski-detail-${i}" data-dato="${dato}" data-rendered="0" style="display:block;">
        <div class="ski-day-chart-section" id="ski-chart-section-${i}" style="display:none;">
          <div class="ski-day-chart-wrap">
            <canvas id="ski-chart-${i}"></canvas>
            <div class="ski-day-chart-msg" id="ski-chart-msg-${i}"><span class="spinner"></span></div>
          </div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function toggleSkiDay(i){
  const chartSection=document.getElementById('ski-chart-section-'+i);
  const arrow=document.getElementById('ski-arrow-'+i);
  const detail=document.getElementById('ski-detail-'+i);
  if(!chartSection) return;
  const open=chartSection.style.display==='none';
  chartSection.style.display=open?'block':'none';
  if(arrow) arrow.textContent=open?'▴ Graf':'▾ Graf';
  if(open && detail && detail.dataset.rendered==='0'){
    detail.dataset.rendered='1';
    renderSkiChart(i, detail.dataset.dato);
  }
}

const _skiCharts={};
function renderSkiChart(i, dato){
  const msg=document.getElementById('ski-chart-msg-'+i);
  const ctx=document.getElementById('ski-chart-'+i);
  if(!ctx) return;

  // Filtrer intervaller for denne datoen
  const ivs=(window._skiIntervaller||[]).filter(iv=>(iv.start||'').startsWith(dato));
  if(!ivs.length){ if(msg) msg.textContent='Ingen timedata'; return; }
  if(msg) msg.style.display='none';

  const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
  const labels=ivs.map(iv=>{
    const dt=new Date(iv.start); return String(dt.getHours()).padStart(2,'0')+':00';
  });
  const temps=ivs.map(iv=>iv.temperatur_c!=null?parseFloat(iv.temperatur_c):null);
  const precip=ivs.map(iv=>iv.nedbor_mm!=null?parseFloat(iv.nedbor_mm):null);
  const precipColors=ivs.map(iv=>(iv.temperatur_c||0)<=0?'rgba(116,192,252,0.7)':'rgba(255,107,107,0.7)');
  const wind=ivs.map(iv=>iv.vind_ms!=null?parseFloat(iv.vind_ms):null);

  if(_skiCharts[i]) _skiCharts[i].destroy();
  _skiCharts[i]=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yP',order:3},
      {type:'line',label:'Temp',data:temps,borderWidth:2,pointRadius:0,tension:0.3,fill:false,yAxisID:'yT',order:1,
        segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroundColor:'transparent',
        borderWidth:1.5,borderDash:[3,3],pointRadius:0,tension:0.3,fill:false,yAxisID:'yW',order:2},
    ]},
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'rgba(8,14,31,.95)',titleColor:'#dce4f5',bodyColor:'#7b8db5',
        callbacks:{label:c=>{
          if(c.parsed.y==null)return null;
          if(c.dataset.label==='Temp')return`Temp: ${c.parsed.y>0?'+':''}${c.parsed.y.toFixed(1)}°C`;
          if(c.dataset.label==='Vind')return`Vind: ${c.parsed.y.toFixed(1)} m/s`;
          return`Nedbør: ${c.parsed.y.toFixed(1)} mm`;
        }}}},
      scales:{
        x:{ticks:{color:'#7b8db5',font:{size:9},maxRotation:0},grid:{color:'rgba(100,130,200,.06)'}},
        yT:{position:'left',ticks:{color:'#7b8db5',font:{size:9},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(100,130,200,.06)'},afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:9}},grid:{drawOnChartArea:false}},
        yW:{display:false,min:0},
      }
    }
  });
}



function toggleDetail(btn){
  const full=document.getElementById('hero-full');
  const open=full.classList.toggle('open');
  btn.textContent=open?'Les mindre ▴':'Les mer ▾';
}

function fmtTemp(v){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1)+'°C';}
function fmtDelta(v,u='cm'){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1)+' '+u;}

function init(){
  fetch('/kvamskogen/api/status')
    .then(r=>r.json())
    .then(d=>{
      renderStatus(d);
      if(!d.sno||d.sno.dybde_cm==null){
        setTimeout(init, 3000);
      } else {
        // Data er klart – sett opp periodisk refresh
        setTimeout(init, 15*60*1000);
      }
    })
    .catch(()=>setTimeout(init, 5000));
}

function renderStatus(d){
  try{
  const t=d.tolkning||{},s=d.sno||{},lp=d.loyper||{};
  const bc={'green':'badge-green','amber':'badge-amber','red':'badge-red'}[t.badge_color]||'badge-amber';
  const fullDetail = (t.detail||'').replace(/\n/g,'<br>');
  // Første avsnitt som kortversjon
  const firstPara = (t.detail||'').split('\n').filter(x=>x.trim())[0] || '';
  const hasMore = (t.detail||'').split('\n').filter(x=>x.trim()).length > 1;
  const detailHtml = hasMore
    ? `<span class="hero-detail-short">${firstPara}</span>
       <span class="hero-detail-full" id="hero-full">${fullDetail}</span>
       <button class="les-mer" onclick="toggleDetail(this)">Les mer ▾</button>`
    : `<span class="hero-detail-short">${fullDetail}</span>`;

  document.getElementById('hero').innerHTML=`<div class="hero-top"><div class="hero-icon">${t.icon||'🏔️'}</div><div class="hero-text"><div class="hero-verdict">${t.verdict||'Kvamskogen'}</div><div class="hero-detail">${detailHtml}</div><span class="hero-badge ${bc}">${t.snow_quality||'Ukjent'} skiføre</span></div></div>`;
  document.getElementById('m-dybde').textContent=s.dybde_cm!=null?s.dybde_cm+' cm':'–';
  document.getElementById('m-dybde-sub').textContent=s.ny_sno_48t_cm!=null?'+'+s.ny_sno_48t_cm+' cm siste 48t':'';
  document.getElementById('m-1t').textContent=fmtDelta(s.endring_1t_cm);
  document.getElementById('m-3t').textContent=fmtDelta(s.endring_3t_cm);
  document.getElementById('m-temp').textContent=fmtTemp(s.temp_na_c);
  document.getElementById('m-temp-sub').textContent=s.min_temp_c!=null?`min ${fmtTemp(s.min_temp_c)} / maks ${fmtTemp(s.maks_temp_c)}`:'';
  // Løypestatus
  const mainEl=document.getElementById('l-main');
  const subEl=document.getElementById('l-sub');
  const snoSidenEl=document.getElementById('l-sno-siden-prep');
  if(mainEl&&lp.preparert!=null){
    const prepPct=lp.totalt>0?Math.round(lp.preparert/lp.totalt*100):0;
    const sistTimer=lp.sist_prep_timer!=null?lp.sist_prep_timer:null;

    // Formatér tid siden prep
    function fmtTid(t){
      if(t==null) return 'ukjent';
      const min=Math.round(t*60);
      if(min<60) return `${min} minutter siden`;
      if(t<2) return '1 time siden';
      if(t<5) return `${Math.round(t*2)/2} timer siden`.replace('.',',');
      if(t<48) return `${Math.round(t)} timer siden`;
      return `${Math.round(t/24)} dager siden`;
    }

    mainEl.textContent=`🎿 Løypene ble kjørt ${fmtTid(sistTimer)}`;
    mainEl.style.color=sistTimer!=null&&sistTimer<6?'var(--green)':sistTimer<24?'var(--text)':'var(--muted)';
    if(subEl) subEl.textContent=`${lp.preparert} av ${lp.totalt} segmenter preparert (${prepPct}%)`;

    // Snø siden prep – vis i cm
    let snoSidenMm=0;
    if(sistTimer!=null&&d.intervaller&&d.intervaller.length){
      const prepTidspunkt=new Date(Date.now()-sistTimer*3600*1000);
      snoSidenMm=d.intervaller
        .filter(iv=>new Date(iv.start)>=prepTidspunkt)
        .reduce((sum,iv)=>sum+((iv.temperatur_c||0)<=1.5?(iv.nedbor_mm||0):0),0);
    }
    const snoSidenCm=Math.round(snoSidenMm/10*10)/10;
    if(snoSidenEl&&sistTimer!=null){
      snoSidenEl.textContent=snoSidenCm>0?`❄️ ca. ${snoSidenCm} cm ny snø siden preparering`:'Ingen snøfall siden siste preparering';
    }

    // Regelbasert kvalitetsvurdering med værsymboler
    const vurdEl=document.getElementById('l-vurdering');
    if(vurdEl&&sistTimer!=null){
      const temp=s.temp_na_c||0;
      let vurd='', vurdFarge='var(--muted)';
      if(sistTimer<2&&snoSidenCm<1){
        vurd='☀️ Nypreparert og tørt – løypene er i topp stand'; vurdFarge='var(--green)';
      } else if(snoSidenCm>=5&&temp<=0){
        vurd=`🌨️ ${snoSidenCm} cm kaldsnø siden prep – perfekt underlag`; vurdFarge='var(--green)';
      } else if(snoSidenCm>=2&&temp<=1.5){
        vurd=`🌨️ ${snoSidenCm} cm ny snø siden prep – godt underlag`; vurdFarge='var(--green)';
      } else if(snoSidenCm>=1&&temp<=1.5){
        vurd=`🌨️ Litt nysnø siden prep – passe bra`; vurdFarge='var(--text)';
      } else if(sistTimer<8&&temp<=1){
        vurd='☀️ Relativt fersk preparering og kaldt – bra forhold'; vurdFarge='var(--text)';
      } else if(temp>2&&sistTimer>6){
        vurd='🌧️ Mildt vær – snøen er trolig tung og klissete'; vurdFarge='var(--amber)';
      } else if(sistTimer>24){
        vurd='🌫️ Over ett døgn siden prep – løypene kan være slitt'; vurdFarge='var(--amber)';
      } else if(snoSidenMm>2&&temp>1.5){
        vurd='🌧️ Regn siden prep – løypene er trolig våte'; vurdFarge='var(--amber)';
      } else {
        vurd='⛅ Greit preparert – akseptable forhold'; vurdFarge='var(--text)';
      }
      vurdEl.textContent=vurd;
      vurdEl.style.color=vurdFarge;
    }
  }
  const forecastEl=document.getElementById('forecast');
  if(forecastEl) forecastEl.innerHTML=(d.daglig||[]).map(dag=>{
    const dt=new Date(dag.dato+'T12:00:00');
    const navn=DAYS[dt.getDay()]+' '+dt.getDate()+'.'+MONTHS[dt.getMonth()];
    return`<div class="fday"><div class="fday-name">${navn}</div><div class="fday-icon">${dag.ver_ikon||'–'}</div><div class="fday-temp"><span style="color:#ef4444">${dag.maks_temp_c>0?'+':''}${dag.maks_temp_c}°</span> / <span style="color:#0284c7">${dag.min_temp_c}°</span></div><div class="fday-snow">${dag.ny_sno_cm>0?'❄ +'+dag.ny_sno_cm+'cm':''}</div><div class="fday-depth">${dag.snodybde_cm!=null?dag.snodybde_cm+' cm':''}</div></div>`;
  }).join('');
  const ts=new Date(d.hentet);
  document.getElementById('footer').textContent=`Oppdatert: ${ts.toLocaleString('no-NO')} · Data: Yr, Frost, loyper.net`;
  if(d.intervaller&&d.intervaller.length){fcastData=d.intervaller;renderFcast();}
  // Bygg skitur-anbefalinger fra intervaller
  if(d.intervaller&&d.intervaller.length) buildSkitur(d.intervaller, d.daglig||[]);
  }catch(e){console.error('renderStatus feil:', e);}
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
</script>
</body>
</html>
"""
