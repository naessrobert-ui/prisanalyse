# -*- coding: utf-8 -*-
"""
Kvamskogen sommer/turvær – prisanalyse.no/kvamskogen-sommer
Speiler kvamskogen_routes.py, men uten løypelogikk og uten skiføre.
Fokus: turvær. Sol + lite vind = best. Regn med 3t opphold = fin luke.
Mye nedbør + vind = "crazy turvær".
"""
from __future__ import annotations
import os
import json
import time
import socket
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo
import pandas as pd
import requests
from urllib3.util import connection as urllib3_connection
from flask import Blueprint, Response, jsonify, request


# Direkteimport – unngår HTTP self-call som timer ut på Render
try:
       from scripts.ver_routes import _hent_prognose_data as _ver_hent_prognose_data
       _VER_DIREKTE = True
except ImportError:
       try:
              from ver_routes import _hent_prognose_data as _ver_hent_prognose_data
              _VER_DIREKTE = True
       except ImportError:
              _VER_DIREKTE = False


from dotenv import load_dotenv
load_dotenv()


kvamskogen_sommer_bp = Blueprint("kvamskogen_sommer", __name__, url_prefix="/kvamskogen-somme
LOCAL_TZ = ZoneInfo("Europe/Oslo")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL         = "claude-haiku-4-5"


# ── System-prompt for hovedtolkning (turvær, ikke skiføre) ──────────────────
_SYSTEM_PROMPT = """
Du er en lokal turvakt på Kvamskogen som skriver engasjerende meldinger til en hytteeier
om turvær – ikke om snø eller skiføre. Skriv naturlig, muntlig norsk bokmål – varm og personl


Du får to ferdig beregnede felt som du skal bruke ORDRETT – ikke omformuler dem:
- "verdict_ferdig" → bruk dette som "verdict"-feltet i JSON-svaret.
- "fremover_tekst_ferdig" → bruk dette som tredje avsnitt i "detail"-feltet.


Bruk kamerabilder aktivt – de avslører ting tall ikke kan: sikt, lys, om bakken er tørr/våt,


VIKTIG: Ikke kommenter snø, ski, løyper eller skiføre. Dette er et sommer-/vår-/høst-modul.
Når du beskriver forhold, fokuser på sol, vind, regn, sikt, temperatur og terreng.


Svar KUN med gyldig JSON (ingen markdown):
{"verdict":"...","detail":"...","tur_quality":"...","badge_color":"...","icon":"..."}


detail i 3 korte avsnitt (60-100 ord totalt):
    - Første avsnitt: Forholdene nå – beskriv hva bildene viser (sikt, lys, tørr/våt bakke, ak
        Bruk temperaturen for å si noe om hvor varmt eller kaldt det føles.
    - Andre avsnitt: Hva slags tur passer akkurat nå – kort og konkret (lang fjelltur,
        skogsrunde, korte luker mellom bygene, eller "crazy turvær" for de som vil ha det røft).
    - Tredje avsnitt: Kopier inn "fremover_tekst_ferdig" ORDRETT. Ikke legg til, ikke trekk fr


tur_quality: "Strålende" | "Bra" | "Moderat" | "Crazy"
badge_color: "green" | "amber" | "red"
icon:
""".strip()


# ── Kamera-URLer (bilde-lenker beholdes som i snø-versjonen) ─────────────────
KAMERA_URLS = {
     "vegvesen":    "https://kamera.atlas.vegvesen.no/api/images/1229056_1",
     "furedalen": "https://furedalen-webcamera.no/bunn/bilde2.jpg",
     "eikedalen": "https://www.eikedalen.no/wc3/image00001.jpg",
}


# ── Kamera-AI: turvær-fokus, ikke løyper ─────────────────────────────────────
_KAMERA_SYSTEM = """
Du analyserer webkamerabilder fra Kvamskogen-området med fokus på TURVÆR (sommer/vår/høst).
Ikke kommenter løyper, ski eller snøforhold.


Svar KUN med gyldig JSON (ingen markdown):
{"nedbor_type":"...","sikt":"...","bakke":"...","sammendrag":"...","ikon":"..."}


1. nedbor_type: "regn" | "tørt" | "duskregn" | "tåkeregn" | "ukjent"
2. sikt: "god" | "tåke" | "dårlig" | "ukjent"
3. bakke: "tørr" | "våt" | "sølete" | "ikke synlig"
4. sammendrag: Én setning (maks 15 ord) om det du ser – fokus på turforhold.
5. ikon: Ett emoji som oppsummerer forholdene (                        ).
""".strip()


# ── Tving IPv4 for nettverkskall (samme som snø-versjonen) ──────────────────
_ORIG_CREATE_CONNECTION = urllib3_connection.create_connection
def _ipv4_create_connection(address, *args, **kwargs):
    """Variant av urllib3.create_connection som kun resolver til IPv4."""
    host, port = address
    infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    err = None
    for family, socktype, proto, _canon, sa in infos:
           sock = None
           try:
                  sock = socket.socket(family, socktype, proto)
                  timeout = kwargs.get("timeout")
                  if timeout is not None and timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                      sock.settimeout(timeout)
                  source = kwargs.get("source_address")
                  if source:
                      sock.bind(source)
                  sock.connect(sa)
                  return sock
           except OSError as e:
                  err = e
                  if sock is not None:
                      sock.close()
    if err is not None:
           raise err
    raise OSError("getaddrinfo returned empty list")


@contextmanager
def _force_ipv4_dns_resolution():
    urllib3_connection.create_connection = _ipv4_create_connection
    try:
           yield
    finally:
           urllib3_connection.create_connection = _ORIG_CREATE_CONNECTION


def _download_image_with_ipv4_fallback(session: requests.Session, url: str, timeout: int = 15
    try:
           return session.get(url, timeout=timeout)
    except requests.RequestException:
           with _force_ipv4_dns_resolution():
                  return session.get(url, timeout=timeout)


def _hent_og_analyser_ett_kamera(navn: str, url: str) -> dict:
    import base64
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (prisanalyse.no weather dashboard)"})
    try:
           r = _download_image_with_ipv4_fallback(session, url, timeout=8)
           if r.status_code != 200:
               return {"url": url, "feil": True}
           b64 = base64.b64encode(r.content).decode()
           ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
           if ct not in ("image/jpeg", "image/png", "image/gif", "image/webp"):
               ct = "image/jpeg"
           resp = requests.post(
               "https://api.anthropic.com/v1/messages",
               headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
               },
               json={
                    "model": "claude-haiku-4-5",
                    "max_tokens": 200,
                    "system": _KAMERA_SYSTEM,
                    "messages": [{
                          "role": "user",
                          "content": [
                               {"type": "image", "source": {"type": "base64", "media_type": ct, "dat
                               {"type": "text", "text": "Analyser dette kamerabildet fra Kvamskogen
                          ],
                    }],
               },
               timeout=20,
           )
           resp.raise_for_status()
           text = resp.json()["content"][0]["text"].strip()
           if text.startswith("```"):
               text = text.split("```")[1]
               if text.startswith("json"):
                    text = text[4:]
           data = json.loads(text)
           data["url"] = url
           data["hentet"] = datetime.now().isoformat(timespec="seconds")
           return data
    except Exception as exc:
           print(f"[kvamskogen-sommer] Kamerahenting feilet for {navn} ({url}): {exc}")
           return {"url": url, "feil": True}


def _analyser_kamera() -> dict:
    """Henter kamerabilder parallelt og analyserer dem med Claude Vision."""
    if not ANTHROPIC_API_KEY:
        return {}
    import concurrent.futures
    resultater: dict = {navn: {"url": url, "feil": True} for navn, url in KAMERA_URLS.items()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(KAMERA_URLS)) as ex:
        futs = {ex.submit(_hent_og_analyser_ett_kamera, navn, url): navn
                   for navn, url in KAMERA_URLS.items()}
        try:
               for fut in concurrent.futures.as_completed(futs, timeout=35):
                   navn = futs[fut]
                   try:
                          resultater[navn] = fut.result()
                   except Exception as exc:
                          print(f"[kvamskogen-sommer] Kamera-future feilet for {navn}: {exc}")
        except concurrent.futures.TimeoutError:
               print("[kvamskogen-sommer] Kameraanalyse: as_completed timeout – bruker det vi ha
    return resultater


# ── Frost-historikk (samme som snø-versjonen) ───────────────────────────────
FROST_BASE_URL = "https://frost.met.no"
FROST_SOURCE      = "SN50310"
FROST_TIMEOUT     = 20
FROST_RETRIES     = 4
FROST_ELEMENTS = {
    "temperature":         "air_temperature,max(air_temperature PT1H),min(air_temperature PT1H)",
    "precipitation": "sum(precipitation_amount PT1H)",
    "wind_speed":          "wind_speed",
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
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "
def _clean_value(element_id: str, value: Any) -> Any:
    try:
           n = float(value)
    except (TypeError, ValueError):
           return value
    if "precipitation_amount" in element_id and n == -1:
           return 0.0
    return n


def hent_historikk(hours: int = 24) -> list:
    end_utc      = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_utc = end_utc - timedelta(hours=hours)
    session = _frost_session()
    all_elements = "air_temperature,max(air_temperature PT1H),min(air_temperature PT1H),sum(p
    payload = _frost_get(session, "/observations/v0.jsonld", {
           "sources":         FROST_SOURCE,
           "referencetime": f"{_iso_z(start_utc)}/{_iso_z(end_utc)}",
           "elements":        all_elements,
           "timeoffsets":     "default",
           "levels":          "default",
           "qualities":       "0,1,2,3,4",
    })
    def _classify(eid: str):
           if eid in ("air_temperature", "max(air_temperature PT1H)", "min(air_temperature PT1H)
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
               eid = str(obs.get("elementId", ""))
               name = _classify(eid)
               if name and name not in rows[ref_local]:
                   rows[ref_local][name] = _clean_value(eid, obs.get("value"))
    return sorted(rows.values(), key=lambda x: x["time"])


# ── Bygg "fremover"-tekst (3. avsnitt i detail) ─────────────────────────────
def _bygg_fremover_tekst(tur_dager: list[dict], today_iso: str) -> str:
    """Bygg tredje avsnitt i detail regelbasert – AI skal bruke dette ordrett.
    Fokus: sol/lite vind = topp, mye nedbør+vind = crazy turvær."""
    fremtidige = [d for d in tur_dager if d.get("dato", "") >= today_iso]
    if not fremtidige:
        return "Ingen turdager i prognosen."


    # Finn beste turdag (score 4-5: sol, lite vind, lite/ingen regn).
    forste_topp = next((
        d for d in fremtidige
        if d.get("score", 3) >= 4
    ), None)


    # Finn første "crazy turvær"-dag (mye nedbør og mye vind).
    forste_crazy = next((
        d for d in fremtidige
        if d.get("score", 3) == 0         # 0 brukes som signal for crazy
    ), None)


    # Finn første dag med fin "regn-luke" (regndag, men ≥3t opphold midt på).
    forste_luke = next((
        d for d in fremtidige
        if d.get("score", 3) == 3 and d.get("har_oppholdsluke")
    ), None)


    def _dagnavn(d: dict) -> str:
        dato = d.get("dato", "")
        if dato == today_iso:
               return "I dag"
        try:
               from datetime import date as _date
               delta = (_date.fromisoformat(dato) - _date.fromisoformat(today_iso)).days
               if delta == 1:
                      return "I morgen"
        except Exception:
               pass
        return d.get("ukedag", dato).capitalize()


    def _topp_tekst(score: int) -> str:
        return {5: "strålende", 4: "bra"}.get(score, "bra")


    if forste_topp:
        navn = _dagnavn(forste_topp)
        score = forste_topp.get("score", 4)
        sol_t = (forste_topp.get("features") or {}).get("sol_timer", 0)
        if sol_t and sol_t >= 5:
               return f"{navn} ser {_topp_tekst(score)} ut for tur – mye sol og lite vind."
        return f"{navn} ser {_topp_tekst(score)} ut for tur."
    elif forste_luke:
        navn = _dagnavn(forste_luke)
        luke = forste_luke.get("opphold_vindu") or ""
        if luke:
               return f"{navn} byr på en fin oppholdsluke {luke} – grei tid for en runde."
        return f"{navn} har en oppholdsluke som passer for en runde."
    elif forste_crazy:
        navn = _dagnavn(forste_crazy)
        return f"{navn} blir crazy turvær – mye nedbør og kraftig vind. Dagen for de tøffeste
    else:
        return "Ingen utpregede turdager i nærmeste fremtid – mest variabelt vær."


# ── Hovedtolkning (AI + regelbasert verdict for turvær) ─────────────────────
def _ai_tolkning(ver_data: dict, kamera_data: dict | None = None) -> dict:
    if not ANTHROPIC_API_KEY:
        return _fallback_tolkning(ver_data)
    s       = ver_data.get("sammendrag", {})
    dag0 = (ver_data.get("daglig") or [{}])[0]
    tur_dager = _compute_local_tur_days(ver_data.get("intervaller", []), limit=5)


    now_local = datetime.now(LOCAL_TZ)
    now_min = now_local.hour * 60 + now_local.minute
    today_iso = now_local.date().isoformat()


    def _er_fremdeles_relevant(dag: dict) -> bool:
        """Returner True hvis vinduet har minst 90 min igjen, eller er en fremtidig dag."""
        dato = dag.get("dato", "")
        if dato > today_iso:
               return True
        if dato < today_iso:
               return False
        beste_tid = dag.get("beste_tid") or ""
        if "–" not in beste_tid:
               return False
        _, slutt = beste_tid.split("–", 1)
        slutt_min = _t_til_min(slutt.strip())
        return (slutt_min - now_min) >= 90


    neste_topp = next(
        (d for d in tur_dager if d.get("score", 0) >= 4 and d.get("beste_tid") and _er_fremde
        next((d for d in tur_dager if d.get("score", 0) >= 3 and d.get("beste_tid") and _er_f
    )


    if neste_topp:
        er_i_dag = neste_topp.get("dato") == today_iso
        dag_label = "I dag" if er_i_dag else neste_topp['ukedag'].capitalize()
    score = neste_topp.get("score", 3)
    kvalitet = {5: "strålende dag", 4: "bra dag", 3: "brukbar dag med oppholdsluke",
                2: "marginal dag", 1: "regndag", 0: "crazy turvær"}.get(score, "brukbar d
    neste_topp_dag = (
        f"{dag_label} {neste_topp['dato']}: "
        f"beste tid {neste_topp['beste_tid']} "
        f"– {kvalitet} (score {score}/5, '{neste_topp.get('kort', '')}')"
    )
else:
    neste_topp_dag = "Ingen gode turvinduer gjenstår i dag eller de neste dagene"


# ── Regelbasert verdict – styrer hovedoverskriften ────────────────────
def _bygg_verdict(neste: dict | None, now_h: int, today: str) -> str:
    """Bestem overskrift basert på tidspunkt og neste gode turdag."""
    if neste is None:
        # Sjekk om det finnes en crazy-dag i prognosen (signal: score == 0)
        crazy = next((d for d in tur_dager if d.get("score") == 0), None)
        if crazy and crazy.get("dato") != today:
            return f"Crazy turvær venter – {crazy.get('ukedag','').lower()}"
        return "Ingen utpregede turdager de neste dagene"


    er_i_dag = neste.get("dato") == today
    score = neste.get("score", 3)
    ukedag = neste.get("ukedag", "").capitalize()
    beste_tid = neste.get("beste_tid") or ""
    tid_tekst = _fmt_beste_tid_tekst(beste_tid) or "hele dagen"
    features = neste.get("features") or {}
    sol_timer = float(features.get("sol_timer") or 0)
    snitt_vind = float(features.get("snitt_vind") or 0)
    regn_andel = float(features.get("regn_andel") or 0)
    klar_turdag = sol_timer >= 5 and snitt_vind < 4 and regn_andel < 0.10


    # Etter kl 14: fokuser på neste dag, ikke i dag
    if now_h >= 14:
        if er_i_dag:
            slutt_min = _t_til_min(beste_tid.split("–")[-1]) if "–" in beste_tid else 0
            if slutt_min - now_h * 60 >= 60:
                return f"Siste sjanse for tur i dag – {tid_tekst}"
            neste_fremtidig = next(
                (d for d in tur_dager if d.get("dato", "") > today and d.get("score", 0)
                None
            )
            if neste_fremtidig:
                nf_score = neste_fremtidig.get("score", 3)
                nf_dag = neste_fremtidig.get("ukedag", "").capitalize()
                if nf_score >= 5:
                       return f"Hold ut – {nf_dag.lower()} blir en strålende turdag"
               elif nf_score >= 4:
                      return f"Grått nå, men {nf_dag.lower()} lover bra turvær"
               return f"Neste turmulighet: {nf_dag.lower()}"
        return "Dagens luke er snart over"
    else:
        er_i_morgen = False
        try:
               import datetime as _dt
               neste_dato = _dt.date.fromisoformat(neste.get("dato", ""))
               er_i_morgen = (neste_dato - now_local.date()).days == 1
        except Exception:
               pass
        if score >= 5 and er_i_morgen:
               return "Hold ut – i morgen blir strålende turdag"
        elif score >= 5:
               return f"Hold ut – {ukedag.lower()} blir strålende for tur"
        elif score >= 4 and er_i_morgen:
               return "Grått nå, men i morgen ser bra ut for tur"
        elif score >= 4:
               return f"Grått nå, men {ukedag.lower()} lover bra turvær"
        return f"Neste brukbare turdag: {ukedag.lower()}"
else:
    # Før kl 14: vurder forholdene nå vs fremover
    if er_i_dag and score >= 5:
        if klar_turdag:
               return "Strålende turdag – sol, lite vind, ingen regn"
        return f"Strålende turdag – best {tid_tekst}"
    elif er_i_dag and score >= 4:
        return f"Bra turforhold i dag – {tid_tekst}"
    elif er_i_dag and score == 3:
        return f"Fin oppholdsluke – best {tid_tekst}"
    elif er_i_dag and score == 0:
        return "Crazy turvær i dag – kraftig vind og mye regn"
    elif score >= 5:
        er_i_morgen = False
        try:
               import datetime as _dt
               neste_dato = _dt.date.fromisoformat(neste.get("dato", ""))
               er_i_morgen = (neste_dato - now_local.date()).days == 1
        except Exception:
               pass
        if er_i_morgen:
               return "Regnvær i dag – strålende turvær i morgen"
        return f"Variabelt nå, men {ukedag.lower()} blir strålende"
    elif score >= 4:
        return f"Variabelt nå – {ukedag.lower()} ser bedre ut for tur"
    return "Variabelt vær – se etter tørre turluker"
ferdig_verdict = _bygg_verdict(neste_topp, now_local.hour, today_iso)
fremover_tekst_ferdig = _bygg_fremover_tekst(tur_dager, today_iso)


# Kompakt per-dag-oversikt – gir AI fasit slik at den ikke kan rose en regndag.
tur_dager_oversikt = [
       {
           "dato":         d["dato"],
           "ukedag":       d["ukedag"],
           "score":        d["score"],
           "kort":         d.get("kort", ""),
           "beste_tid": d.get("beste_tid"),
       }
       for d in tur_dager
]


payload_str = json.dumps({
       "verdict_ferdig":             ferdig_verdict,
       "fremover_tekst_ferdig": fremover_tekst_ferdig,
       "tidspunkt_na":              now_local.strftime("%A %H:%M").replace("Monday","mandag").rep
       "temp_na_c":                 s.get("temperatur_nå_c"),
       "maks_temp_24t_c":           s.get("maks_temp_c"),
       "min_temp_24t_c":            s.get("min_temp_c"),
       "ver_i_dag":                 dag0.get("vær_label"),
       "neste_topp_dag":            neste_topp_dag,
       "tur_dager_oversikt":        tur_dager_oversikt,
}, ensure_ascii=False)


img_session = None
user_content: list = [{"type": "text", "text": payload_str}]
try:
       import base64
       import gc
       if kamera_data:
           img_session = requests.Session()
           img_session.headers.update({"User-Agent": "Mozilla/5.0 (prisanalyse.no)"})
           kamera_labels = {
               "vegvesen":       "Vegvesen-kamera (veien til Kvamskogen)",
               "furedalen": "Furedalen",
               "eikedalen": "Eikedalen",
           }
           for navn, label in kamera_labels.items():
               url = KAMERA_URLS.get(navn)
               if not url:
                      continue
               try:
                      img_r = img_session.get(url, timeout=12)
                          if img_r.status_code != 200:
                                 continue
                          ct = img_r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip(
                          if ct not in ("image/jpeg", "image/png", "image/gif", "image/webp"):
                                 ct = "image/jpeg"
                          b64 = base64.b64encode(img_r.content).decode()
                          img_r.close()
                          user_content.append({"type": "text", "text": f"Kamerabilde: {label}"})
                          user_content.append({"type": "image",
                                                     "source": {"type": "base64", "media_type": ct, "data
                          del b64
                      except Exception:
                          pass
        r = requests.post(
               "https://api.anthropic.com/v1/messages",
               headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                           "content-type": "application/json"},
               json={"model": ANTHROPIC_MODEL, "max_tokens": 500,
                        "system": _SYSTEM_PROMPT,
                        "messages": [{"role": "user", "content": user_content}]},
               timeout=60,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if text.startswith("```"):
               text = text.split("```")[1]
               if text.startswith("json"):
                      text = text[4:]
        result = json.loads(text)
        # Overstyr alltid verdict med den regelbaserte Python-verdien
        result["verdict"] = ferdig_verdict
        return result
    except Exception:
        traceback.print_exc()
        return _fallback_tolkning(ver_data)
    finally:
        if img_session is not None:
               img_session.close()
        try:
               del user_content
               import gc as _gc
               _gc.collect()
        except Exception:
               pass


def _fallback_tolkning(ver_data: dict) -> dict:
    """Regelbasert fallback – uten skiføre/snø-kommentarer."""
    s              = ver_data.get("sammendrag", {})
    temp           = s.get("temperatur_nå_c") or 0
    maks           = s.get("maks_temp_c") or temp
    intervaller = ver_data.get("intervaller", []) or []
    nedbor_24t = sum(float(iv.get("nedbør_mm") or 0) for iv in intervaller[:24])
    maks_vind        = max((float(iv.get("vind_ms") or 0) for iv in intervaller[:24]), default=0)


    if nedbor_24t >= 8 and maks_vind >= 12:
           return {"verdict": "Crazy turvær – mye nedbør og kraftig vind",
                      "detail":     f"Det er meldt {nedbor_24t:.0f} mm nedbør og vind opp mot {maks_vi
                                    f"Dette er dagen for de tøffeste turforholdene.",
                      "tur_quality": "Crazy", "badge_color": "red", "icon": "      "}
    if nedbor_24t < 1 and maks_vind < 4 and maks >= 12:
           return {"verdict": "Strålende turvær – sol og lite vind",
                      "detail":     f"Lite eller ingen nedbør neste døgn, vind under {maks_vind:.0f} m
                                    f"Perfekt for en lengre tur.",
                      "tur_quality": "Strålende", "badge_color": "green", "icon": "          "}
    if nedbor_24t < 2 and maks_vind < 8:
           return {"verdict": "Bra turvær – stort sett opphold",
                      "detail":     f"Lite nedbør og moderat vind. Greie forhold for tur.",
                      "tur_quality": "Bra", "badge_color": "green", "icon": "      "}
    if nedbor_24t < 6:
           return {"verdict": "Variabelt – se etter oppholdsluker",
                      "detail":     f"Det er meldt rundt {nedbor_24t:.0f} mm nedbør neste døgn. "
                                    f"Sjekk timeprognosen for å finne en luke.",
                      "tur_quality": "Moderat", "badge_color": "amber", "icon": "       "}
    return {"verdict": "Mye nedbør – kort tur eller utsett",
                  "detail":   f"Rundt {nedbor_24t:.0f} mm nedbør neste døgn. Lengre tur er ikke å an
                  "tur_quality": "Moderat", "badge_color": "amber", "icon": "      "}


# ── Datahenting (kun vær – ingen løyper) ────────────────────────────────────
_IS_RENDER = os.getenv("RENDER") == "true"
_BASE = "https://prisanalyse.no" if _IS_RENDER else "http://localhost:5000"


def _hent_ver() -> dict:
    """Henter timeprognose for Kvamskogen. Direkteimport hvis mulig, ellers HTTP."""
    if _VER_DIREKTE:
           try:
                  return _ver_hent_prognose_data("Kvamskogen")
           except Exception:
                  traceback.print_exc()
                  return {}
    try:
           r = requests.get(f"{_BASE}/ver/api/snovarsel",
                                  params={"stasjon": "Kvamskogen"}, timeout=45)
           r.raise_for_status()
           return r.json()
    except Exception:
           traceback.print_exc()
           return {}


@kvamskogen_sommer_bp.get("/")
def forside():
    return Response(_FORSIDE_HTML, mimetype="text/html; charset=utf-8")


# ── Cache ────────────────────────────────────────────────────────────────────
_STATUS_CACHE: dict = {}
_STATUS_CACHE_TTL = 30 * 60
_STATUS_REFRESHING = False


def _refresh_cache():
    """Kjøres i bakgrunnen – oppdaterer cache uten å blokkere requester."""
    global _STATUS_REFRESHING
    if _STATUS_REFRESHING:
           return
    _STATUS_REFRESHING = True
    try:
           import concurrent.futures
           ex = concurrent.futures.ThreadPoolExecutor(max_workers=2)
           try:
                  fut_ver       = ex.submit(_hent_ver)
                  fut_kamera = ex.submit(_analyser_kamera)


                  def _safe(fut, timeout, label):
                      try:
                             return fut.result(timeout=timeout)
                      except Exception as exc:
                             print(f"[kvamskogen-sommer] {label} feilet/timed out ({exc}) – fortsetter
                             return {}


                  ver_data       = _safe(fut_ver,    60, "ver")
                  kamera_data = _safe(fut_kamera, 45, "kamera")
           finally:
                  ex.shutdown(wait=False)


           tolkning = _ai_tolkning(ver_data, kamera_data)
           s         = ver_data.get("sammendrag", {})
           daglig = ver_data.get("daglig", [])
           payload = _build_payload(tolkning, ver_data, s, daglig, kamera_data)
           _STATUS_CACHE["status"] = {
                  "expires_at": time.time() + _STATUS_CACHE_TTL,
                  "payload":       payload,
           }
    except Exception:
        traceback.print_exc()
    finally:
        _STATUS_REFRESHING = False


def _build_payload(tolkning, ver_data, s, daglig, kamera_data=None):
    tur_dager_raw = _compute_local_tur_days(ver_data.get("intervaller", []), limit=6)
    tur_summary = _build_tur_summary_from_days(tur_dager_raw) if tur_dager_raw else {
        "today": None, "tomorrow": None, "next_days": [], "hero_text": "", "future_text": ""
    }
    return {
        "hentet":      datetime.now().isoformat(timespec="seconds"),
        "tolkning": tolkning,
        "ver": {
               "temp_na_c":        s.get("temperatur_nå_c"),
               "min_temp_c":       s.get("min_temp_c"),
               "maks_temp_c":      s.get("maks_temp_c"),
        },
        "daglig": [
               {
                   "dato":           d.get("dato"),
                   "min_temp_c":     d.get("min_temp_c"),
                   "maks_temp_c": d.get("maks_temp_c"),
                   "nedbor_mm":      d.get("total_nedbør_mm") or d.get("total_smelting_mm"),
                   "ver_ikon":       d.get("vær_ikon"),
                   "ver_label":      d.get("vær_label"),
                   "vind_ms":        d.get("vind_ms_snitt"),
               }
               for d in daglig[:8]
        ],
        "intervaller": [
               {
                   "start":           iv.get("start"),
                   "temperatur_c": iv.get("temperatur_c"),
                   "nedbor_mm":       iv.get("nedbør_mm"),
                   "nedbor_min_mm": iv.get("nedbør_min_mm"),
                   "nedbor_maks_mm": iv.get("nedbør_maks_mm"),
                   "nedbor_sannsynlighet_pct": iv.get("nedbør_sannsynlighet_pct"),
                   "vind_ms":         iv.get("vind_ms"),
                   "vind_kast_ms": iv.get("vind_kast_ms"),
                   "vindretning_grader": iv.get("vindretning_grader"),
                   "ver_ikon":        iv.get("vær_ikon"),
                   "timer":           iv.get("timer"),
               }
               for iv in ver_data.get("intervaller", [])
        ],
        "kamera": kamera_data or {},
        "tur_summary": tur_summary,
    }


@kvamskogen_sommer_bp.get("/api/status")
def api_status():
    import threading
    now_ts = time.time()
    hit = _STATUS_CACHE.get("status")
    if hit and hit["expires_at"] > now_ts:
           return jsonify(hit["payload"])
    if hit:
           threading.Thread(target=_refresh_cache, daemon=True).start()
           return jsonify(hit["payload"])
    threading.Thread(target=_refresh_cache, daemon=True).start()
    return jsonify({"hentet": datetime.now().isoformat(),
                       "tolkning": _fallback_tolkning({}),
                       "ver": {}, "daglig": [], "intervaller": [], "laster": True})


_HISTORIKK_CACHE: dict = {}
_HISTORIKK_CACHE_TTL = 30 * 60


@kvamskogen_sommer_bp.get("/api/historikk")
def api_historikk():
    try:
           hours = max(1, min(72, int(request.args.get("hours", 24))))
           now_ts = time.time()
           hit = _HISTORIKK_CACHE.get(hours)
           if hit and hit["expires_at"] > now_ts:
               return jsonify(hit["payload"])
           data = hent_historikk(hours)
           payload = {"ok": True, "data": data, "antall": len(data)}
           _HISTORIKK_CACHE[hours] = {"expires_at": now_ts + _HISTORIKK_CACHE_TTL, "payload": pa
           expired = [k for k, v in _HISTORIKK_CACHE.items() if v["expires_at"] <= now_ts]
           for k in expired:
               del _HISTORIKK_CACHE[k]
           return jsonify(payload)
    except Exception as e:
           traceback.print_exc()
           return jsonify({"ok": False, "error": str(e)}), 500


# ── Soloppgang/solnedgang (samme som snø-versjonen) ─────────────────────────
def _sol_tider(dato_str: str) -> tuple:
    """Beregn soloppgang og solnedgang for Kvamskogen (60.4°N, 5.97°E)."""
    import math
    from datetime import date
    lat = math.radians(60.4)
    dato = date.fromisoformat(dato_str)
    dag_nr = dato.timetuple().tm_yday
    decl = math.radians(23.45 * math.sin(math.radians(360/365*(dag_nr-81))))
    cos_ha = (math.sin(math.radians(-0.833)) - math.sin(lat)*math.sin(decl)) / (math.cos(lat)
    if abs(cos_ha) > 1:
           return None, None
    ha = math.degrees(math.acos(cos_ha))
    tz_offset = 2 if 3 <= dato.month <= 10 else 1
    noon = 12 - 5.97/15 + tz_offset
    def fmt(h):
           hh = int(h); mm = int((h-hh)*60)
           return f"{hh:02d}:{mm:02d}"
    return fmt(noon - ha/15), fmt(noon + ha/15)


# ── Hjelpefunksjoner for intervaller ────────────────────────────────────────
@dataclass
class TurWindow:
    start: str
    end: str
    hours: float
    score: float
    kind: str


def _fmt_min(mm: int) -> str:
    mm = max(0, int(round(mm)))
    h = mm // 60
    m = mm % 60
    return f"{h:02d}:{m:02d}"


def _t_til_min(t: str | None) -> int:
    if not t or ":" not in t:
           return 0
    h, m = t.split(":")
    return int(h) * 60 + int(m)


def _iv_min(iv: dict) -> int:
    return _t_til_min((iv.get("start") or "")[11:16])


def _iv_varighet_min(iv: dict) -> int:
    try:
           return max(60, int(round(float(iv.get("timer") or 1) * 60)))
    except Exception:
           return 60


def _iv_slutt_min(iv: dict) -> int:
    return _iv_min(iv) + _iv_varighet_min(iv)


def _iv_tid(iv: dict) -> str:
    return (iv.get("start") or "")[11:16]
def _overlapp_min(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start))


def _iv_har_nedbor(iv: dict) -> bool:
    return float(iv.get("nedbor_mm") or 0) >= 0.15


def _iv_er_sol(iv: dict) -> bool:
    ikon = iv.get("ver_ikon") or ""
    return any(s in ikon for s in ["☀", "   ", "   "])


def _iv_er_brukbar_tur(iv: dict) -> bool:
    """En time er brukbar for tur hvis det er tørt og vinden under 10 m/s."""
    return not _iv_har_nedbor(iv) and float(iv.get("vind_ms") or 0) < 10


def _iv_er_strålende(iv: dict) -> bool:
    """Strålende tur-time: tørt, sol, vind under 4 m/s."""
    return (
        not _iv_har_nedbor(iv)
        and float(iv.get("vind_ms") or 0) < 4
        and _iv_er_sol(iv)
    )


def _score_iv_for_tur(iv: dict) -> float:
    """Score per time for turvær.
    Premier sol + lite vind tungt. Straff regn og sterk vind."""
    nb = float(iv.get("nedbor_mm") or 0)
    vind = float(iv.get("vind_ms") or 0)
    score = 0.0
    if nb >= 0.5:
        score -= 8
    elif nb >= 0.15:
        score -= 3
    else:
        score += 4
    if vind < 4:
        score += 5
    elif vind < 6:
        score += 3
    elif vind < 8:
        score += 1
    elif vind < 12:
        score -= 4
    else:
        score -= 8
    if _iv_er_sol(iv):
        score += 3
    score -= min(nb, 5) * 1.0
    return score


def _finn_sammenhengende_oppholds_luker(
    intervaller: list[dict],
    start_min: int,
    end_min: int,
    min_hours: int = 2,
) -> list[TurWindow]:
    """Finn alle sammenhengende blokker med opphold (ingen nedbør) og moderat vind."""
    min_minutes = int(min_hours * 60)
    kandidater: list[tuple[dict, int]] = []
    for iv in intervaller:
        overlap = _overlapp_min(_iv_min(iv), _iv_slutt_min(iv), start_min, end_min)
        if overlap > 0:
            kandidater.append((iv, overlap))
    blokker: list[list[tuple[dict, int]]] = []
    gjeldende: list[tuple[dict, int]] = []
    for iv, overlap in kandidater:
        if _iv_er_brukbar_tur(iv):
            gjeldende.append((iv, overlap))
        else:
            if sum(m for _, m in gjeldende) >= min_minutes:
                   blokker.append(gjeldende)
            gjeldende = []
    if sum(m for _, m in gjeldende) >= min_minutes:
        blokker.append(gjeldende)
    vinduer: list[TurWindow] = []
    for blokk in blokker:
        total_minutes = sum(m for _, m in blokk)
        total_score = sum(_score_iv_for_tur(iv) * m for iv, m in blokk)
        score = total_score / max(total_minutes, 1)
        if all(_iv_er_strålende(iv) for iv, _ in blokk):
            kind = "strålende"
        elif all(not _iv_har_nedbor(iv) for iv, _ in blokk):
            kind = "god"
        else:
            kind = "brukbar"
        start_actual = max(_iv_min(blokk[0][0]), start_min)
        end_actual = min(start_actual + total_minutes, end_min)
        start_rounded = round(start_actual / 60) * 60
        end_rounded = round(end_actual / 60) * 60
        if end_rounded <= start_rounded:
            end_rounded = start_rounded + 60
        vinduer.append(TurWindow(
            start=_fmt_min(start_rounded),
            end=_fmt_min(end_rounded),
             hours=round(total_minutes / 60, 1),
             score=round(score, 2),
             kind=kind,
        ))
    return vinduer


def _finn_beste_tur_luke(
    intervaller: list[dict],
    soloppgang: str,
    solnedgang: str,
) -> TurWindow | None:
    """Finn beste tur-luke i dagslys. Premier sol + lite vind."""
    sol_opp_min = _t_til_min(soloppgang)
    sol_ned_min = _t_til_min(solnedgang)
    dagslys = _finn_sammenhengende_oppholds_luker(intervaller, sol_opp_min, sol_ned_min, min_
    if dagslys:
        return max(dagslys, key=lambda x: (x.kind == "strålende", x.score, x.hours))
    return None


def _finn_3t_oppholds_luke(
    intervaller: list[dict],
    soloppgang: str,
    solnedgang: str,
) -> TurWindow | None:
    """For regndager: finn fineste sammenhengende ≥3t opphold midt på dagen."""
    sol_opp_min = _t_til_min(soloppgang)
    sol_ned_min = _t_til_min(solnedgang)
    luker = _finn_sammenhengende_oppholds_luker(intervaller, sol_opp_min, sol_ned_min, min_ho
    if not luker:
        return None
    # Velg den lengste sammenhengende oppholdsluken
    return max(luker, key=lambda x: (x.hours, x.score))


# ── Bygg dag-features for tur ───────────────────────────────────────────────
def _bygg_dag_features(dato: str, intervaller: list[dict], soloppgang: str, solnedgang: str)
    sol_opp_min = _t_til_min(soloppgang)
    sol_ned_min = _t_til_min(solnedgang)
    dagslys = [iv for iv in intervaller
                  if _overlapp_min(_iv_min(iv), _iv_slutt_min(iv), sol_opp_min, sol_ned_min) > 0


    total_minutes = 0
    regn_minutes = 0
    brukbare_minutes = 0
    strålende_minutes = 0
    sol_minutes = 0
    vind_sum = 0.0
    nedbor_sum = 0.0
vindtall = []
for iv in dagslys:
    mins = _overlapp_min(_iv_min(iv), _iv_slutt_min(iv), sol_opp_min, sol_ned_min) or _iv
    total_minutes += mins
    vindtall.append(float(iv.get("vind_ms") or 0))
    vind_sum += float(iv.get("vind_ms") or 0) * mins
    nedbor_sum += float(iv.get("nedbor_mm") or 0)
    if _iv_har_nedbor(iv):
           regn_minutes += mins
    if _iv_er_brukbar_tur(iv):
           brukbare_minutes += mins
    if _iv_er_strålende(iv):
           strålende_minutes += mins
    if _iv_er_sol(iv) and float(iv.get("timer") or 1) <= 1:
           sol_minutes += mins


beste = _finn_beste_tur_luke(intervaller, soloppgang, solnedgang)
luke_3t = _finn_3t_oppholds_luke(intervaller, soloppgang, solnedgang)


return {
    "dato": dato,
    "soloppgang": soloppgang,
    "solnedgang": solnedgang,
    "dagslys_timer": round(total_minutes / 60, 1),
    "regn_timer": round(regn_minutes / 60, 1),
    "regn_andel": round(regn_minutes / max(total_minutes, 1), 2),
    "brukbare_timer": round(brukbare_minutes / 60, 1),
    "strålende_timer": round(strålende_minutes / 60, 1),
    "maks_vind": round(max(vindtall) if vindtall else 0, 1),
    "snitt_vind": round(vind_sum / max(total_minutes, 1), 1),
    "sum_nedbor": round(nedbor_sum, 1),
    "sol_timer": round(sol_minutes / 60, 1),
    "grov_prognose": bool(dagslys) and all(float(iv.get("timer") or 1) >= 6 for iv in dag
    "beste_luke": (
           {
               "fra": beste.start, "til": beste.end,
               "timer": beste.hours, "score": beste.score, "kvalitet": beste.kind,
           } if beste else None
    ),
    "luke_3t": (
           {
               "fra": luke_3t.start, "til": luke_3t.end,
               "timer": luke_3t.hours,
           } if luke_3t else None
    ),
}
# ── Format-hjelpere ─────────────────────────────────────────────────────────
def _fmt_time_natural(hhmm: str | None) -> str:
    if not hhmm or ":" not in hhmm:
           return hhmm or ""
    h, m = hhmm.split(":", 1)
    try:
           h_i = int(h); m_i = int(m)
    except ValueError:
           return hhmm
    if m_i == 0:
           return str(h_i)
    return f"{h_i}:{m_i:02d}"


def _fmt_range_natural(range_text: str | None) -> str:
    if not range_text or "–" not in range_text:
           return range_text or ""
    start, end = range_text.split("–", 1)
    return f"mellom {_fmt_time_natural(start)} og {_fmt_time_natural(end)}"


def _fmt_beste_tid_tekst(beste_tid: str | None,
                               soloppgang: str | None = None,
                               solnedgang: str | None = None) -> str | None:
    """Returner lesbar tidstekst, eller None hvis vinduet dekker hele dagen."""
    if not beste_tid or "–" not in beste_tid:
           return None
    start_s, end_s = beste_tid.split("–", 1)
    start_min = _t_til_min(start_s.strip())
    end_min      = _t_til_min(end_s.strip())
    varighet_min = end_min - start_min
    if soloppgang and solnedgang:
           sol_opp_min = _t_til_min(soloppgang)
           sol_ned_min = _t_til_min(solnedgang)
           dagslys_min = max(sol_ned_min - sol_opp_min, 1)
           andel = varighet_min / dagslys_min
           if andel >= 0.85 or varighet_min >= 8 * 60:
               return None
    elif varighet_min >= 8 * 60:
           return None
    start_h = round(start_min / 60)
    end_h      = round(end_min / 60)
    if start_h == end_h:
           end_h += 1
    return f"mellom {start_h} og {end_h}"


# ── Tekstgenerator for hver dag ─────────────────────────────────────────────
def _lag_dagtekst(features: dict) -> tuple[int, str, str | None, str, dict]:
    """Returner (score, kort, beste_tid, detalj, ekstra_flags) for en turdag.
Score-skala (sommer/turvær):
  5 = strålende turdag (≥5t sol, vind <4 m/s, ingen regn)
  4 = bra turdag (lite regn, vind <8 m/s, en del sol)
  3 = brukbar regndag MED ≥3t sammenhengende oppholdsluke
  2 = marginal (kort luke eller mye vind)
  1 = regndag uten god luke
  0 = "crazy turvær" – mye nedbør + sterk vind


Returnerer også et flagg-dict som inneholder bl.a. "har_oppholdsluke"
og "opphold_vindu" for bruk i fremover-tekst.
"""
flags = {"har_oppholdsluke": False, "opphold_vindu": None}
beste = features.get("beste_luke")
luke_3t = features.get("luke_3t")
regn_andel = features.get("regn_andel", 0)
maks_vind = features.get("maks_vind", 0)
snitt_vind = features.get("snitt_vind", 0)
sol_timer = features.get("sol_timer", 0)
sum_nedbor = features.get("sum_nedbor", 0)


# ── 0: Crazy turvær: mye nedbør + sterk vind ──────────────────────────
if sum_nedbor >= 8 and (maks_vind >= 12 or snitt_vind >= 9):
      return 0, "Crazy turvær – kraftig regn og vind", None, (
          f"Det er meldt {sum_nedbor:.0f} mm nedbør og vind opp mot {maks_vind:.0f} m/s. "
          f"Dette er dagen for de tøffeste turforholdene – hvis du har lyst på det, "
          f"er det her du finner skikkelig motvind og våtvær."
      ), flags


sol_opp = features.get("soloppgang")
sol_ned = features.get("solnedgang")


# ── 1: Regndag uten luke ────────────────────────────────────────────────
if not beste and not luke_3t:
      return 1, "Regndag – ingen god luke", None, (
          "Regn dominerer hele dagen, og det finnes ingen sammenhengende oppholdsluke "
          "på minst to timer. Dårlig dag for tur."
      ), flags


# ── 3: Regndag MED ≥3t oppholdsluke ─────────────────────────────────────
# Hvis hovedvinduet "beste" er marginalt (regn dominerer), men det finnes en
# konkret 3t-luke i dagslys, fremhev luken som turmulighet.
if regn_andel > 0.35 and luke_3t:
      beste_tid = f'{luke_3t["fra"]}–{luke_3t["til"]}'
      tid_tekst = _fmt_beste_tid_tekst(beste_tid, sol_opp, sol_ned)
      # Hvis luken dekker (nesten) hele dagslyset, oppgrader til score 4
      if tid_tekst is None and luke_3t["timer"] >= 6:
        flags["har_oppholdsluke"] = True
        flags["opphold_vindu"] = "store deler av dagen"
        return 4, "Bra dag – fint det meste av dagen", beste_tid, (
               f"Stort sett opphold gjennom dagen. Det er et lite tidsvindu med regn, "
               f"men {luke_3t['timer']:.0f} sammenhengende timer er tørre."
        ), flags
    tekst_inn = tid_tekst or "i en lang luke midt på dagen"
    flags["har_oppholdsluke"] = True
    flags["opphold_vindu"] = tekst_inn
    return 3, f"Fin oppholdsluke {tekst_inn}", beste_tid, (
        f"Regn ellers, men det er meldt minst {luke_3t['timer']:.0f} sammenhengende timer
        f"uten nedbør {tekst_inn}. Fin tid for en runde."
    ), flags


if regn_andel > 0.35 and not luke_3t:
    # Regndag uten 3t-luke (kanskje en kort 2t-luke)
    if beste:
        beste_tid = f'{beste["fra"]}–{beste["til"]}'
        tid_tekst = _fmt_beste_tid_tekst(beste_tid, sol_opp, sol_ned) or "midt på dagen"
        return 2, f"Regndag – kort luke {tid_tekst}", beste_tid, (
               f"Regn dominerer dagen. Det finnes en kort luke {tid_tekst}, men den er "
               f"ikke lang nok for en skikkelig tur."
        ), flags
    return 1, "Regndag – ikke anbefalt", None, (
        "Regn det meste av dagslyset. Ikke en dag for lengre tur."
    ), flags


# ── 5: Strålende turdag ─────────────────────────────────────────────────
if (beste and beste["kvalitet"] == "strålende"
        and beste["timer"] >= 4
        and sol_timer >= 5
        and snitt_vind < 4
        and regn_andel < 0.05):
    score = 5
# ── 4: Bra turdag (litt yr i randene tolereres når luken er lang) ──────
elif (beste and beste["kvalitet"] in ("strålende", "god")
        and beste["timer"] >= 3
        and snitt_vind < 8
        and regn_andel < 0.30):
    score = 4
else:
    score = 3


beste_tid = f'{beste["fra"]}–{beste["til"]}' if beste else None
tid_tekst = _fmt_beste_tid_tekst(beste_tid, sol_opp, sol_ned) if beste_tid else None


if score == 5:
        if tid_tekst:
               kort = f"Strålende {tid_tekst}"
               detalj = f"Strålende turvær – mye sol, lite vind og opphold {tid_tekst}. Perfekt
        else:
               kort = "Strålende dag – sol hele dagen"
               detalj = "Sol, opphold og lite vind fra morgen til kveld. Perfekt turdag."
    elif score == 4:
        if tid_tekst:
               kort = f"Bra {tid_tekst}"
               detalj = f"Bra turforhold {tid_tekst}. Stort sett opphold, vinden holder seg og d
        else:
               kort = "Bra dag – fint hele dagen"
               detalj = "Bra turvær gjennom hele dagen. Lite nedbør og moderat vind."
    else:
        if tid_tekst:
               kort = f"Greit {tid_tekst}"
               detalj = f"Brukbart turvær {tid_tekst}. Vær forberedt på variabelt vær ellers."
        else:
               kort = "Brukbart – variabelt"
               detalj = "Dagen har perioder med brukbare forhold, men forvent litt variabelt vær


    grov = features.get("grov_prognose", False)
    if grov and score >= 3:
        detalj += " Prognosen er grovere denne dagen, så tidsvinduet bør tolkes litt romslig.
    if maks_vind >= 10 and score > 0:
        detalj += f" Merk at vinden kan komme opp i {maks_vind:.0f} m/s utenfor det anbefalte


    return score, kort, beste_tid, detalj, flags


def _fallback_dag(dato: str, intervaller: list, soloppgang: str, solnedgang: str) -> dict:
    """Regelbasert dag-analyse (uten AI)."""
    features = _bygg_dag_features(dato, intervaller, soloppgang, solnedgang)
    score, kort, beste_tid, detalj, flags = _lag_dagtekst(features)
    return {
        "score": score,
        "kort": kort,
        "beste_tid": beste_tid,
        "detalj": detalj,
        "features": features,
        "har_oppholdsluke": flags["har_oppholdsluke"],
        "opphold_vindu": flags["opphold_vindu"],
    }


# ── AI-tolkning per dag (valgfritt – brukes av /api/tur-ai) ─────────────────
_TUR_AI_CACHE: dict = {}
_TUR_AI_CACHE_TTL = 30 * 60
_TUR_AI_PROMPT = """Du er en lokal turguide for Kvamskogen (sommer/vår/høst – ikke ski).
Oppgaven er å formulere NÅR det er best å gå på tur, basert på analysen du får.


Du får:
- dato og ukedag
- soloppgang og solnedgang
- ferdig beregnet analysefelt
- timepunkter for kontroll


VIKTIGE REGLER:
1. Bruk feltet "beste_luke" som fasit hvis det finnes.
2. Ikke finn opp et tidsvindu som ikke finnes i dataene.
3. Hvis "beste_luke" er null OG det heller ikke finnes en ≥3t oppholdsluke,
   skal "beste_tid" være null og teksten skal si tydelig at det ikke er en god turdag.
4. Sol + lite vind (under 4 m/s) = perfekt.
5. ≥3 timer sammenhengende uten nedbør = en fin luke for tur, selv på en regnvær-dag.
6. Mye nedbør (≥8 mm) kombinert med sterk vind (≥12 m/s) = "crazy turvær" – sett score 0.
7. Hold teksten kort, konkret og nyttig. Ikke nevn ski, snø eller løyper.


Svar KUN med gyldig JSON-array:
[{"dato":"YYYY-MM-DD","score":0-5,"kort":"maks 6 ord","beste_tid":"HH:MM–HH:MM eller null","d


Scoring:
5 = strålende turdag (sol + lite vind)
4 = bra turdag
3 = brukbar oppholdsluke ≥3t på regnværsdag
2 = kort eller marginal luke
1 = regndag uten luke
0 = crazy turvær (mye nedbør + sterk vind)"""


def _coerce_ai_day(local_day: dict, ai_day: dict | None) -> dict:
    result = dict(local_day)
    if not ai_day:
           return result
    beste_tid = ai_day.get("beste_tid")
    if beste_tid in ("null", "", "ingen", "None"):
           beste_tid = None
    if local_day.get("beste_tid") is None:
           beste_tid = None
    else:
           beste_tid = local_day.get("beste_tid")
    result["beste_tid"] = beste_tid
    try:
           ai_score = int(ai_day.get("score"))
    except Exception:
           ai_score = local_day.get("score", 3)
    if beste_tid is None and ai_score >= 2:
           ai_score = 1
    result["score"] = max(0, min(5, ai_score))
    if ai_day.get("kort"):
        result["kort"] = str(ai_day["kort"])[:80]
    if ai_day.get("detalj"):
        result["detalj"] = str(ai_day["detalj"])[:240]
    return result


def _compute_local_tur_days(intervaller_raw: list[dict], limit: int = 8) -> list[dict]:
    """Beregn lokale turdag-analyser for hver dato i prognosen."""
    from collections import defaultdict
    import datetime as _dt
    per_dag: dict = defaultdict(list)
    for iv in intervaller_raw:
        dato = (iv.get("start") or "")[:10]
        if dato:
             per_dag[dato].append(iv)
    dager: list[dict] = []
    for dato in sorted(per_dag.keys())[:limit]:
        sol_opp, sol_ned = _sol_tider(dato)
        sol_opp = sol_opp or "07:00"
        sol_ned = sol_ned or "20:00"
        ivs = per_dag[dato]
        analyse = _fallback_dag(dato, ivs, sol_opp, sol_ned)
        dt = _dt.date.fromisoformat(dato)
        ukedag = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][dt.weekda
        dager.append({
             "dato":               dato,
             "ukedag":             ukedag,
             "soloppgang":         sol_opp,
             "solnedgang":         sol_ned,
             "score":              analyse.get("score", 3),
             "kort":               analyse.get("kort", ""),
             "beste_tid":          analyse.get("beste_tid"),
             "detalj":             analyse.get("detalj", ""),
             "features":           analyse.get("features", {}),
             "har_oppholdsluke":   analyse.get("har_oppholdsluke", False),
             "opphold_vindu":      analyse.get("opphold_vindu"),
             "intervaller":        ivs,
        })
    return dager


# ── Bygg hero-sammendrag (i dag + i morgen + fremover) ─────────────────────
def _build_tur_summary_from_days(dager: list[dict]) -> dict:
    def _first_sentence(text: str) -> str:
        text = (text or "").strip()
        if not text:
             return ""
    head = text.split(".")[0].strip()
    return head or text


def _parse_range(range_text: str | None) -> tuple[int | None, int | None]:
    if not range_text or "–" not in range_text:
        return None, None
    start, end = range_text.split("–", 1)
    return _t_til_min(start), _t_til_min(end)


def _time_to_phrase(prefix: str, end_min: int | None) -> str:
    if end_min is None:
        return prefix
    hh = f"{end_min // 60:02d}:{end_min % 60:02d}"
    return f"{prefix} {_fmt_time_natural(hh)}"


def _today_urgent_line(dag: dict | None) -> str | None:
    if not dag:
        return None
    beste_tid = dag.get("beste_tid")
    score = dag.get("score", 3)
    if score == 0:
        return "I dag er det crazy turvær – kraftig regn og vind."
    if not beste_tid:
        return "I dag er det ingen god turluke."
    now_local = datetime.now(LOCAL_TZ)
    now_min = now_local.hour * 60 + now_local.minute
    start_min, end_min = _parse_range(beste_tid)
    today_date = dag.get("dato")
    is_today = False
    if today_date:
        try:
               is_today = datetime.now(LOCAL_TZ).date().isoformat() == today_date
        except Exception:
               is_today = False
    if not is_today:
        return None
    # Hvis vinduet dekker hele dagen (≥8t), gi en mer generell tekst
    sol_opp = dag.get("soloppgang")
    sol_ned = dag.get("solnedgang")
    tid_tekst = _fmt_beste_tid_tekst(beste_tid, sol_opp, sol_ned)
    if start_min is None or end_min is None:
        return (f"I dag er det fint hele dagen." if tid_tekst is None
                   else f"I dag er det best {tid_tekst}.")
    if now_min < start_min:
        return (f"I dag er det fint hele dagen." if tid_tekst is None
                   else f"I dag er det best {tid_tekst}.")
    if start_min <= now_min <= end_min:
        remaining = end_min - now_min
        if tid_tekst is None and remaining > 240:
               return "Nå er det fint turvær – holder ut hele dagen."
        if remaining <= 90:
               return _time_to_phrase("Nå haster det – best fram til", end_min) + "."
        if remaining <= 240:
               return _time_to_phrase("Nå er det fint å være ute – best fram til", end_min)
        return _time_to_phrase("Nå er det fint turvær – holder til", end_min) + "."
    return "Den beste turluken i dag var tidligere."


def _short_desc(dag: dict | None, fallback: str) -> str:
    if not dag:
        return fallback
    detalj = _first_sentence(dag.get("detalj") or "")
    if detalj:
        return detalj.rstrip(". ")
    kort = (dag.get("kort") or "").strip()
    if kort:
        return kort.rstrip(". ")
    return fallback


def _tomorrow_line(dag: dict | None, fallback: str) -> str | None:
    if not dag:
        return None
    beste_tid = dag.get("beste_tid")
    score = dag.get("score", 3)
    kort = (dag.get("kort") or "").strip().rstrip(".")
    tid_tekst = _fmt_beste_tid_tekst(beste_tid)
    if score == 0:
        return "I morgen blir det crazy turvær."
    if score >= 5:
        if tid_tekst:
               return f"I morgen blir strålende turdag – best {tid_tekst}."
        else:
               return f"I morgen blir strålende turdag – {kort.lower() if kort else 'sol og
    elif score >= 4:
        if tid_tekst:
               return f"I morgen ser bra ut, best {tid_tekst}."
        else:
               return "I morgen ser bra ut – fint turvær hele dagen."
    elif score == 3 and beste_tid:
        if tid_tekst:
               return f"I morgen finnes en oppholdsluke {tid_tekst}."
        else:
               return "I morgen finnes en grei oppholdsluke."
    elif beste_tid:
        if tid_tekst:
               return f"I morgen er det begrenset – kort luke {tid_tekst}."
        return "I morgen er forholdene marginale."
    return f"I morgen: {_short_desc(dag, fallback)}."


def _pick_best_days(candidates: list[dict], max_items: int = 2) -> list[dict]:
    ranked = sorted(
        [d for d in candidates if d and d.get("score") is not None],
        key=lambda d: (d.get("score", 0), bool(d.get("beste_tid")), -(len(d.get("detalj")
        reverse=True,
    )
    preferred = [d for d in ranked if d.get("score", 0) >= 4]
    source = preferred if preferred else [d for d in ranked if d.get("score", 0) >= 3]
    out = []
    seen = set()
    for d in source:
        key = d.get("dato")
        if key and key not in seen:
               out.append(d)
               seen.add(key)
        if len(out) >= max_items:
               break
    return out


def _pick_crazy_day(candidates: list[dict]) -> dict | None:
    crazy = [d for d in candidates if d and d.get("score") == 0]
    return crazy[0] if crazy else None


today    = dager[0] if len(dager) > 0 else None
tomorrow = dager[1] if len(dager) > 1 else None
upcoming = dager[1:6] if len(dager) > 1 else []


lines = []
today_line = _today_urgent_line(today)
if not today_line and today:
    beste_tid = today.get("beste_tid")
    today_line = (f"I dag er det best {_fmt_range_natural(beste_tid)}."
                       if beste_tid else "I dag er det ingen god turluke.")
tomorrow_line = _tomorrow_line(tomorrow, "ingen god turluke")
if today_line:
    lines.append(today_line)
if tomorrow_line:
    lines.append(tomorrow_line)


best_days = _pick_best_days(upcoming, max_items=2)
crazy_day = _pick_crazy_day(upcoming)
future_parts = []
if best_days:
    names = []
    for d in best_days:
           name = (d.get("ukedag") or "").capitalize() or (d.get("dato") or "")
           bt = d.get("beste_tid")
           score = d.get("score", 3)
           tid_tekst = _fmt_beste_tid_tekst(bt)
           if score >= 5:
               names.append(f"{name.lower()} (strålende{', best ' + tid_tekst if tid_tekst e
           elif score >= 4:
               names.append(f"{name.lower()} ({tid_tekst})" if tid_tekst else f"{name.lower(
           elif tid_tekst:
               names.append(f"{name.lower()} (oppholdsluke {tid_tekst})")
           else:
               names.append(name.lower())
    if len(names) == 1:
           future_parts.append(f"Fremover ser {names[0]} best ut")
    else:
           future_parts.append(f"Fremover ser {names[0]} og {names[1]} best ut")
if crazy_day:
    name = (crazy_day.get("ukedag") or "").capitalize() or (crazy_day.get("dato") or "")
    future_parts.append(f"crazy turvær på {name.lower()}")


future_text = ". ".join(p.rstrip(". ") for p in future_parts).strip()
if future_text:
    future_text += "."
    lines.append(future_text)


return {
    "today": {
           "dato":         today.get("dato"),
           "score":        today.get("score"),
           "kort":         today.get("kort"),
           "beste_tid": today.get("beste_tid"),
           "detalj":       today.get("detalj"),
    } if today else None,
    "tomorrow": {
           "dato":         tomorrow.get("dato"),
           "score":        tomorrow.get("score"),
           "kort":         tomorrow.get("kort"),
           "beste_tid": tomorrow.get("beste_tid"),
           "detalj":       tomorrow.get("detalj"),
    } if tomorrow else None,
    "next_days": [
           {
               "dato":         d.get("dato"),
               "ukedag":       d.get("ukedag"),
               "score":        d.get("score"),
                  "beste_tid": d.get("beste_tid"),
                  "detalj":       d.get("detalj"),
             }
             for d in upcoming
        ],
        "future_text": future_text,
        "hero_text":       " ".join(lines).strip(),
    }


# ── /api/tur-ai og /api/tur ────────────────────────────────────────────────
@kvamskogen_sommer_bp.get("/api/tur-ai")
def api_tur_ai():
    """AI-analyse av alle turdager i ett kall. Koden velger vindu, AI formulerer."""
    now_ts = time.time()
    hit = _TUR_AI_CACHE.get("tur_ai")
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])
    status_hit = _STATUS_CACHE.get("status")
    if not status_hit:
        return jsonify({"ok": False, "dager": []})
    intervaller_raw = status_hit["payload"].get("intervaller", [])
    if not intervaller_raw:
        return jsonify({"ok": False, "dager": []})
    lokale_dager_raw = _compute_local_tur_days(intervaller_raw, limit=7)
    lokale_dager = [{k: v for k, v in dag.items()
                         if k not in ("features", "intervaller")}
                       for dag in lokale_dager_raw]
    dager_payload = []
    for dag in lokale_dager_raw:
        dager_payload.append({
             "dato":           dag["dato"],
             "ukedag":         dag["ukedag"],
             "soloppgang": dag["soloppgang"],
             "solnedgang": dag["solnedgang"],
             "analyse":        dag.get("features", {}),
             "timer": [
                  {
                       "t":      (iv.get("start") or "")[11:16],
                       "nb":     round(iv.get("nedbor_mm") or 0, 1),
                       "temp": round(iv.get("temperatur_c") or 0, 1),
                       "vind": round(iv.get("vind_ms") or 0, 1),
                       "ikon": iv.get("ver_ikon") or "",
                  }
                  for iv in dag.get("intervaller", [])
             ],
        })
    if not ANTHROPIC_API_KEY:
           payload = {"ok": True, "dager": lokale_dager}
           _TUR_AI_CACHE["tur_ai"] = {"expires_at": now_ts + _TUR_AI_CACHE_TTL, "payload": paylo
           return jsonify(payload)
    try:
           r = requests.post(
               "https://api.anthropic.com/v1/messages",
               headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"},
               json={
                    "model":      "claude-haiku-4-5",
                    "max_tokens": 1024,
                    "system":     _TUR_AI_PROMPT,
                    "messages": [{"role": "user", "content": json.dumps(dager_payload, ensure_asc
               },
               timeout=30,
           )
           r.raise_for_status()
           text = r.json()["content"][0]["text"].strip()
           if "```" in text:
               text = text.split("```")[1]
               if text.startswith("json"):
                    text = text[4:]
           ai_dager = json.loads(text)
           ai_map = {d.get("dato"): d for d in ai_dager if isinstance(d, dict)}
           dager = [_coerce_ai_day(lokal, ai_map.get(lokal["dato"])) for lokal in lokale_dager]
           payload = {"ok": True, "dager": dager}
           _TUR_AI_CACHE["tur_ai"] = {"expires_at": now_ts + _TUR_AI_CACHE_TTL, "payload": paylo
           return jsonify(payload)
    except Exception:
           traceback.print_exc()
           return jsonify({"ok": True, "dager": lokale_dager})


_TUR_CACHE: dict = {}
_TUR_CACHE_TTL = 30 * 60


@kvamskogen_sommer_bp.get("/api/tur")
def api_tur():
    """Rask, regelbasert turdag-analyse uten AI."""
    now_ts = time.time()
    hit = _TUR_CACHE.get("tur")
    if hit and hit["expires_at"] > now_ts:
           return jsonify(hit["payload"])
    status_hit = _STATUS_CACHE.get("status")
    if not status_hit:
           return jsonify({"ok": False, "dager": []})
    intervaller_raw = status_hit["payload"].get("intervaller", [])
    if not intervaller_raw:
          return jsonify({"ok": False, "dager": []})
    dager_raw = _compute_local_tur_days(intervaller_raw, limit=8)
    dager = [{k: v for k, v in dag.items() if k not in ("features", "intervaller")}
               for dag in dager_raw]
    payload = {"ok": True, "dager": dager}
    _TUR_CACHE["tur"] = {"expires_at": now_ts + _TUR_CACHE_TTL, "payload": payload}
    return jsonify(payload)
_FORSIDE_HTML = r"""<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Kvamskogen turvær - i dag</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#f5f7fb;--surface:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;--hint:#94a
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sa
a{color:var(--blue);text-decoration:none;}a:hover{text-decoration:underline;}
.page{max-width:1120px;margin:0 auto;padding:20px 16px 48px;}
.nav{font-size:13px;color:var(--muted);margin-bottom:18px;}.nav a{color:var(--muted);}
.hero{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);pa
.hero-top{display:flex;align-items:flex-start;gap:14px;}
.hero-icon{font-size:38px;line-height:1;flex-shrink:0;margin-top:2px;}
.hero-text{flex:1;}
.hero-verdict{font-size:20px;font-weight:600;line-height:1.3;margin-bottom:6px;}
.hero-detail{font-size:14px;color:var(--muted);line-height:1.6;}
.hero-detail-short{display:block;}
.hero-detail-full{display:none;margin-top:8px;}
.hero-detail-full.open{display:block;}
.les-mer{font-size:12px;color:var(--blue);cursor:pointer;border:none;background:none;padding:
.hero-ski-summary{margin-top:12px;padding:10px 12px;border-radius:12px;background:#eff6ff;col
.hero-badge{display:inline-block;margin-top:12px;font-size:12px;font-weight:600;padding:3px 1
.badge-green{background:var(--green-bg);color:var(--green);border-color:var(--green-bd);}
.badge-amber{background:var(--amber-bg);color:var(--amber);border-color:var(--amber-bd);}
.badge-red{background:var(--red-bg);color:var(--red);border-color:var(--red-bd);}
.section-label{font-size:11px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;c
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;}
@media(max-width:540px){.metric-grid{grid-template-columns:repeat(2,1fr);}}
.metric{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1
.metric-lbl{font-size:11px;color:var(--hint);margin-bottom:4px;}
.metric-val{font-size:20px;font-weight:600;}
.metric-sub{font-size:11px;color:var(--muted);margin-top:3px;}
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radi
.chart-hours{display:flex;gap:6px;margin-bottom:10px;}
.chart-hours button{font-size:11px;padding:3px 10px;border-radius:20px;border:1px solid var(-
.chart-hours button.active{background:#0f172a;color:#fff;border-color:#0f172a;}
.kamera-seksjon{margin:2rem 0 1rem;}
.kamera-seksjon h2{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.1
.kamera-seksjon h2::after{content:'';flex:1;height:1px;background:#e2e8f0;}
.kamera-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:1rem;
.kamera-kort{background:#fff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;box-
.kamera-kort img{width:100%;height:180px;object-fit:cover;display:block;background:#f1f5f9;}
.kamera-info{padding:.75rem 1rem .9rem;}
.kamera-tittel{font-size:.8rem;font-weight:700;color:#0f172a;margin-bottom:.4rem;}
.kamera-ai{font-size:.78rem;color:#334155;line-height:1.5;}
.kamera-ai .ai-linje{display:flex;gap:.4rem;align-items:center;margin-bottom:.2rem;}
.kamera-ai .ai-label{font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing
.kamera-badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:.68rem;font-w
.kb-sno{background:#dbeafe;color:#1d4ed8;}.kb-regn{background:#fee2e2;color:#b91c1c;}
.kb-sludd{background:#fef9c3;color:#92400e;}.kb-tort{background:#dcfce7;color:#15803d;}
.kb-god{background:#dcfce7;color:#15803d;}.kb-taake{background:#f1f5f9;color:#475569;}
.kb-darlig{background:#fee2e2;color:#b91c1c;}.kb-ukjent{background:#f1f5f9;color:#94a3b8;}
.kamera-sammendrag{font-size:.78rem;color:#475569;margin-top:.4rem;font-style:italic;}
.kamera-tid{font-size:.65rem;color:#94a3b8;margin-top:.3rem;}
.kamera-spinner{height:180px;display:flex;align-items:center;justify-content:center;backgroun
.chart-legend{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px;}
.leg{display:flex;align-items:center;gap:5px;font-size:11px;color:var(--muted);}
.leg-line{display:inline-block;width:18px;height:2px;border-radius:2px;}
.leg-bar{display:inline-block;width:10px;height:10px;border-radius:2px;}
.chart-wrap{position:relative;}
.chart-msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;f
.loyper-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--rad
.loyper-row{display:flex;justify-content:space-between;align-items:center;padding:7px 0;borde
.loyper-row:last-child{border-bottom:none;}
.loyper-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;}
.dot-green{background:#16a34a;}.dot-amber{background:#d97706;}.dot-gray{background:#94a3b8;}
.loyper-meta{font-size:11px;color:var(--hint);margin-top:8px;}
.forecast-strip{display:flex;gap:8px;overflow-x:auto;padding-bottom:4px;}
.fday{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10p
.fday-name{font-size:10px;color:var(--hint);margin-bottom:4px;}
.fday-icon{font-size:20px;margin:2px 0;}
.fday-temp{font-size:12px;font-weight:600;}
.fday-snow{font-size:10px;color:#0284c7;margin-top:3px;min-height:14px;}
.fday-depth{font-size:10px;color:var(--hint);margin-top:2px;}
.ski-day{border-radius:14px;margin-bottom:10px;box-shadow:var(--shadow);overflow:hidden;}
.ski-day-header{display:flex;align-items:center;gap:0;cursor:pointer;user-select:none;border-
.ski-day-left{width:88px;min-height:80px;display:flex;flex-direction:column;align-items:cente
.ski-day-left .score-5{background:linear-gradient(135deg,#fbbf24,#f59e0b);}
.ski-day-left .score-4{background:linear-gradient(135deg,#34d399,#10b981);}
.ski-day-left .score-3{background:linear-gradient(135deg,#93c5fd,#60a5fa);}
.ski-day-left .score-2{background:linear-gradient(135deg,#94a3b8,#64748b);}
.ski-day-left .score-1{background:linear-gradient(135deg,#fca5a5,#ef4444);}
.ski-day-left .score-0{background:linear-gradient(135deg,#7c3aed,#4c1d95);}
.ski-day-icon-big{font-size:32px;line-height:1;}
.ski-day-name-top{font-size:11px;font-weight:700;color:rgba(255,255,255,0.9);text-transform:u
.ski-day-right{flex:1;background:var(--surface);padding:12px 16px;border-left:none;min-height
.ski-day-right:hover{background:#f8fafc;}
.ski-day-title{font-size:15px;font-weight:600;color:var(--text);}
.ski-day-kort{font-size:13px;color:var(--muted);}
.ski-day-stats-inline{font-size:11px;color:var(--hint);font-weight:400;margin-left:6px;letter
.ski-day-stats{font-size:11px;color:var(--hint);margin-top:3px;letter-spacing:0.2px;}
.ski-day-meta{display:flex;align-items:center;gap:10px;margin-top:4px;}
.ski-beste-tid{font-size:12px;font-weight:600;color:var(--blue);}
.ski-arrow{font-size:12px;color:var(--hint);margin-left:auto;}
.ski-day-detail{padding:0 16px 12px 100px;}
.ski-day-detail-text{font-size:13px;color:var(--muted);line-height:1.6;}
.ski-day-sol{font-size:11px;color:var(--hint);margin-top:4px;}
.ski-day-chart-section{background:#0f172a;border-radius:0 0 14px 14px;}
.ski-day-chart-wrap{padding:0 12px 12px;height:180px;position:relative;}
.ski-day-chart-msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:
.ski-day-table-wrap{background:#f8fafc;border-top:1px solid #e2e8f0;padding:12px 14px 14px;ov
.ski-day-table-meta{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px;}
.ski-day-meta-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-rad
.ski-day-meta-chip.best{background:#f0fdf4;color:#166534;border-color:#bbf7d0;}
.ski-day-meta-chip.wind{background:#faf5ff;color:#6b21a8;border-color:#e9d5ff;}
.ski-day-meta-chip.precip{background:#eff6ff;color:#1d4ed8;border-color:#bfdbfe;}
.ski-day-meta-chip.cold{background:#ecfeff;color:#155e75;border-color:#a5f3fc;}
.ski-day-meta-chip.warm{background:#fff7ed;color:#c2410c;border-color:#fed7aa;}
.ski-day-table{width:100%;border-collapse:collapse;font-size:12px;color:#1e293b;min-width:980
.ski-day-table th,.ski-day-table td{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-ali
.ski-day-table th{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.3px;b
.ski-day-table th:first-child,.ski-day-table td:first-child{position:sticky;left:0;background
.ski-day-table thead th:first-child{z-index:4;}
.ski-day-table tr:hover td:first-child{background:#f8fafc;}
.ski-day-table-scrollhint{display:none;font-size:11px;color:#64748b;margin:0 0 8px 0;}
.ski-day-table tr:hover td{background:#f8fafc;}
.ski-day-row-best td{background:#f0fdf4;}
.ski-day-row-warn td{background:#fffaf0;}
.ski-day-row-bad td{background:#fef2f2;}
.ski-cell-temp-cold{color:#0f766e;font-weight:700;}
.ski-cell-temp-warm{color:#c2410c;font-weight:700;}
.ski-cell-nb-warn{color:#1d4ed8;font-weight:700;}
.ski-cell-nb-bad{color:#1d4ed8;font-weight:800;}
.ski-cell-type-rain{color:#b91c1c;font-weight:700;}
.ski-cell-type-snow{color:#1d4ed8;font-weight:700;}
.ski-cell-type-dry{color:#166534;font-weight:700;}
.ski-cell-wind-warn{color:#7c3aed;font-weight:700;}
.ski-cell-wind-bad{color:#6b21a8;font-weight:800;}
.ski-vurdering{white-space:normal;min-width:220px;}
.ski-vurdering-pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:10px;f
.ski-vurdering-good{background:#dcfce7;color:#166534;border-color:#bbf7d0;}
.ski-vurdering-warn{background:#fef3c7;color:#92400e;border-color:#fde68a;}
.ski-vurdering-bad{background:#fee2e2;color:#991b1b;border-color:#fecaca;}
.ski-vurdering-cold{background:#cffafe;color:#155e75;border-color:#a5f3fc;}
.ski-vurdering-warm{background:#ffedd5;color:#9a3412;border-color:#fed7aa;}
.ski-vurdering-note{display:block;font-size:11px;color:#475569;line-height:1.4;white-space:no
.ski-day-symbols{display:flex;gap:6px;flex-wrap:wrap;margin-top:2px;}
.ski-day-symbol{font-size:10px;color:#334155;background:#e2e8f0;padding:2px 6px;border-radius
@media(max-width:900px){
    .ski-day-table{min-width:860px;font-size:11px;}
    .ski-day-table th,.ski-day-table td{padding:6px 8px;}
}
@media(max-width:700px){
    .ski-day-detail{padding:0 0 12px 0;}
    .ski-day-table-wrap{margin:0 -12px;padding:10px 12px 12px;}
    .ski-day-table-scrollhint{display:block;}
    .ski-day-table{min-width:900px;font-size:11px;}
    .ski-day-table th,.ski-day-table td{padding:6px 7px;}
    .ski-day-table-meta{gap:6px;}
    .ski-vurdering{min-width:150px;}
    .ski-vurdering-note{display:none;}
}
@media(max-width:480px){
    .ski-day-table{min-width:860px;font-size:10.5px;}
    .ski-day-table th,.ski-day-table td{padding:6px 6px;}
    .ski-day-table-meta{padding-right:8px;}
}
.links-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;}
@media(max-width:480px){.links-grid{grid-template-columns:repeat(2,1fr);}}
.link-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;paddin
.link-card:hover{border-color:var(--blue);background:#eff6ff;}
.link-card-icon{font-size:22px;}
.footer{margin-top:20px;font-size:11px;color:var(--hint);text-align:right;}
.spinner{display:inline-block;width:18px;height:18px;border:2px solid var(--border);border-to
@keyframes spin{to{transform:rotate(360deg)}}
.loading-hero{padding:32px;text-align:center;color:var(--muted);font-size:14px;}
</style>
</head>
<body>
<div class="page">
    <nav class="nav"><a href="/">prisanalyse.no</a> &rsaquo; Kvamskogen turvær</nav>
    <div class="hero" id="hero">
      <div class="hero-top">
         <div class="hero-icon">    </div>
         <div class="hero-text">
           <div class="hero-verdict">Kvamskogen</div>
           <div class="hero-detail"><span class="hero-detail-short">Henter værstatus…</span></di
         <span class="hero-badge badge-amber">Laster…</span>
    </div>
  </div>
</div>
<div class="section-label">Værprognose – kommende timer</div>
<div style="background:#0f172a;border-radius:var(--radius);padding:16px 18px;margin-bottom:
  <div class="chart-hours" style="margin-bottom:8px;">
    <button class="active" onclick="setFcast(12,this)" style="background:#334155;color:#e2e
    <button onclick="setFcast(24,this)" style="background:#1e293b;color:#94a3b8;border-colo
    <button onclick="setFcast(48,this)" style="background:#1e293b;color:#94a3b8;border-colo
    <button onclick="setFcast(168,this)" style="background:#1e293b;color:#94a3b8;border-col
  </div>
  <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:8px;">
    <span class="leg"><span class="leg-line" style="background:#4dabf7"></span><span style=
    <span class="leg"><span class="leg-bar" style="background:rgba(255,107,107,0.6)"></span
    <span class="leg"><span class="leg-bar" style="background:rgba(116,192,252,0.6)"></span
    <span class="leg"><span class="leg-line" style="background:#a78bfa"></span><span style=
  </div>
  <div id="fcast-icons" style="display:flex;overflow-x:auto;gap:0;margin-bottom:6px;min-hei
  <div style="position:relative;height:200px;">
    <canvas id="fcast-chart"></canvas>
    <div id="fcast-msg" style="position:absolute;inset:0;display:flex;align-items:center;ju
  </div>
</div>
<div class="section-label">Værstatus</div>
<div class="metric-grid">
  <div class="metric"><div class="metric-lbl">Temperatur nå</div><div class="metric-val" id
  <div class="metric"><div class="metric-lbl">Maks i dag</div><div class="metric-val" id="m
  <div class="metric"><div class="metric-lbl">Min i dag</div><div class="metric-val" id="m-
  <div class="metric"><div class="metric-lbl">Vind nå</div><div class="metric-val" id="m-vi
</div>
  <div class="metric"><div class="metric-lbl">Endring neste time</div><div class="metric-va
  <div class="metric"><div class="metric-lbl">Endring neste 3t</div><div class="metric-val"
  <div class="metric"><div class="metric-lbl">Temperatur nå</div><div class="metric-val" id
</div>
<div class="section-label">Slik var været siste døgn (Frost/SN50310)</div>
<div class="chart-card">
  <div class="chart-hours">
    <button class="active" onclick="setHours(12,this)">12t</button>
    <button onclick="setHours(24,this)">24t</button>
    <button onclick="setHours(48,this)">48t</button>
  </div>
  <div class="chart-legend">
    <span class="leg"><span class="leg-line" style="background:#3b82f6"></span>Temp (°C)</s
    <span class="leg"><span class="leg-bar" style="background:#ef4444;opacity:.7"></span>Re
    <span class="leg"><span class="leg-bar" style="background:#60a5fa;opacity:.7"></span>Sn
    <span class="leg"><span class="leg-line" style="background:#a78bfa"></span>Vind (m/s)</
      </div>
      <div class="chart-wrap" style="height:240px">
          <canvas id="hist-chart"></canvas>
          <div class="chart-msg" id="chart-msg"><span class="spinner"></span></div>
      </div>
    </div>
    <div class="section-label">Beste tidspunkt for tur</div>
    <div style="font-size:12px;color:var(--hint);margin-bottom:10px;">Klikk på en dag for tidsv
    <div id="tur-list"><div style="color:var(--hint);font-size:13px;padding:8px 0"><span class=
    <section class="kamera-seksjon">
      <h2>     Webkameraer</h2>
      <div class="kamera-grid">
          <div class="kamera-kort" id="kamera-vegvesen"><div class="kamera-spinner">Laster…</div>
          <div class="kamera-kort" id="kamera-furedalen"><div class="kamera-spinner">Laster…</div
          <div class="kamera-kort" id="kamera-eikedalen"><div class="kamera-spinner">Laster…</div
      </div>
    </section>
    <div class="section-label">Verktøy</div>
    <div class="links-grid">
      <a class="link-card" href="/ver/varsel-kvamskogen"><span class="link-card-icon">     </span>
      <a class="link-card" href="/ver/solskinn"><span class="link-card-icon">     </span>Soltimer-
      <a class="link-card" href="/ver/nedbor"><span class="link-card-icon">     </span>Nedbørskart
      <a class="link-card" href="/ver/"><span class="link-card-icon">    </span>Alle værverktøy</
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
    let data=fcastData.filter(x=>new Date(x.start)>=new Date(now.getTime()-3600*1000)&&new Date
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
      return`<div style="flex:1;min-width:32px;text-align:center;"><div style="font-size:14px
    }).join('');
}
const labels=data.map(x=>{
    const dt=new Date(x.start);const h=dt.getHours();
    if(h===0||fcastHours>24)return dt.getDate()+'.'+MS[dt.getMonth()]+(h===0?'':' '+String(h)
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
      {type:'bar',label:'Nedbør (mm)',data:precip,backgroundColor:precipColors,borderRadius:2
      {type:'line',label:'Temperatur (°C)',data:temps,borderWidth:2,pointRadius:0,pointHoverR
          segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},
      {type:'line',label:'Vind (m/s)',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroun
          borderWidth:1.5,borderDash:[3,3],pointRadius:0,pointHoverRadius:3,tension:0.3,fill:fa
    ]},
    options:{
      responsive:true,maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
          legend:{display:false},
          tooltip:{backgroundColor:'rgba(8,14,31,.95)',titleColor:'#dce4f5',bodyColor:'#7b8db5'
            callbacks:{label:c=>{
              if(c.parsed.y==null)return null;
              if(c.dataset.label.includes('Temp'))return`Temp: ${c.parsed.y>0?'+':''}${c.parsed
              if(c.dataset.label.includes('Vind'))return`Vind: ${c.parsed.y.toFixed(1)} m/s`;
              return`Nedbør: ${c.parsed.y.toFixed(1)} mm`;
            }}}
            },
            scales:{
                 x:{ticks:{color:'#7b8db5',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:1
                 yT:{position:'left',ticks:{color:'#7b8db5',font:{size:10},callback:v=>fmtAxisTemp(v)}
                   afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
                 yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:10},callb
                 yW:{display:false,min:0},
            }
        }
    });
}
// ── Skitur-anbefalinger (bygges fra status-data, ingen ekstra kall) ──
const SCORE_EMOJI = {5:'           ', 4:'    ', 3:'   ', 2:'   ', 1:'   ', 0:'   '};
const UKEDAGER = ['søndag','mandag','tirsdag','onsdag','torsdag','fredag','lørdag'];
function solTider(datoStr) {
    // Enkel soloppgang/solnedgang for Kvamskogen (60.4°N)
    const dato = new Date(datoStr+'T12:00:00');
    const dag = Math.floor((dato - new Date(dato.getFullYear(),0,0)) / 86400000);
    const decl = 23.45 * Math.sin((360/365*(dag-81)) * Math.PI/180) * Math.PI/180;
    const lat = 60.4 * Math.PI/180;
    const cosHA = (Math.sin(-0.833*Math.PI/180) - Math.sin(lat)*Math.sin(decl)) / (Math.cos(lat
    if(Math.abs(cosHA)>1) return ['--:--','--:--'];
    const ha = Math.acos(cosHA) * 180/Math.PI;
    const tz = dato.getMonth()>=2 && dato.getMonth()<=9 ? 2 : 1;
    const noon = 12 - 5.97/15 + tz;
    const fmt = h => { const hh=Math.floor(h); const mm=Math.round((h-hh)*60); return String(hh
    return [fmt(noon-ha/15), fmt(noon+ha/15)];
}
function analyserDag(dato, ivs, solOpp, solNed) {
    function tTilMin(t){ if(!t||!t.includes(':'))return 0; const[h,m]=t.split(':'); return (+h)
    function fmtMin(v){ const h=Math.floor(v/60), m=v%60; return String(h).padStart(2,'0')+':'+
    function ivMin(iv){ return tTilMin((iv.start||'').substring(11,16)); }
    function ivDur(iv){ return Math.max(60, Math.round((iv.timer||1)*60)); }
    function ivEnd(iv){ return ivMin(iv)+ivDur(iv); }
    function overlap(aStart,aEnd,bStart,bEnd){ return Math.max(0, Math.min(aEnd,bEnd)-Math.max(
    const solOppMin=tTilMin(solOpp), solNedMin=tTilMin(solNed);
    const prefStart=Math.max(solOppMin, 9*60), prefEnd=Math.min(solNedMin, 16*60);
    function ivType(iv){
        const nb=iv.nedbor_mm||0, temp=iv.temperatur_c||0;
        if(nb<0.15) return 'tørt';
        if(temp>1.5) return 'regn';
        if(temp>0) return 'sludd';
        return 'snø';
    }
    function ivSunny(iv){ const ikon=iv.ver_ikon||''; return ['☀','              ','   '].some(s=>ikon.inclu
    function ivUsable(iv){ return ivType(iv)!=='regn' && (iv.vind_ms||0)<8; }
    function ivPerfect(iv){ return ivType(iv)==='tørt' && (iv.vind_ms||0)<4 && ivSunny(iv); }
function ivScore(iv){
    const nb=iv.nedbor_mm||0, vind=iv.vind_ms||0;
    let s=0;
    if(ivType(iv)==='regn') s-=12;
    else if(ivType(iv)==='sludd') s-=2;
    else if(ivType(iv)==='snø') s+=1.5;
    else s+=4;
    if(vind<4) s+=4;
    else if(vind<6) s+=2;
    else if(vind>=8) s-=6;
    if(ivSunny(iv)) s+=2;
    s-=Math.min(nb,5)*0.75;
    return s;
}
const dagslys=ivs.filter(iv=>overlap(ivMin(iv), ivEnd(iv), solOppMin, solNedMin)>0);
const base=dagslys.length?dagslys:ivs;
function finnLuker(startMin,endMin){
    const kandidater=base
        .map(iv=>({iv, overlap: overlap(ivMin(iv), ivEnd(iv), startMin, endMin)}))
        .filter(x=>x.overlap>0);
    const blokker=[];
    let gjeldende=[];
    for(const item of kandidater){
        if(ivUsable(item.iv)) gjeldende.push(item);
        else {
            const minutter=gjeldende.reduce((a,x)=>a+x.overlap,0);
            if(minutter>=120) blokker.push(gjeldende);
            gjeldende=[];
        }
    }
    const minutter=gjeldende.reduce((a,x)=>a+x.overlap,0);
    if(minutter>=120) blokker.push(gjeldende);
    return blokker.map(blokk=>{
        const totalMin=blokk.reduce((a,x)=>a+x.overlap,0);
        const score=blokk.reduce((a,x)=>a+ivScore(x.iv)*x.overlap,0)/Math.max(totalMin,1);
        const allPerfect=blokk.every(x=>ivPerfect(x.iv));
        const allDry=blokk.every(x=>ivType(x.iv)==='tørt');
        const kind=allPerfect?'perfekt':(allDry?'god':'brukbar');
        const startActual=Math.max(ivMin(blokk[0].iv), startMin);
        const endActual=Math.min(startActual+totalMin, endMin);
        return {fra:fmtMin(startActual), til:fmtMin(endActual), timer:Math.round(totalMin/6)/10
    });
}
let luker=finnLuker(prefStart,prefEnd);
if(!luker.length) luker=finnLuker(solOppMin,solNedMin);
const beste=luker.length ? luker.sort((a,b)=> (Number(b.kvalitet==='perfekt')-Number(a.kval
let totalMin=0, regnMin=0, solMin=0, vindWeighted=0;
    for(const iv of base){
        const mins=overlap(ivMin(iv), ivEnd(iv), solOppMin, solNedMin) || ivDur(iv);
        totalMin+=mins;
        if(ivType(iv)==='regn') regnMin+=mins;
        if(ivSunny(iv)) solMin+=mins;
        vindWeighted+=(iv.vind_ms||0)*mins;
    }
    const regnAndel=regnMin/Math.max(totalMin,1);
    const solAndel=solMin/Math.max(totalMin,1);
    const vindSnitt=vindWeighted/Math.max(totalMin,1);
    let score=1, besteTid=null, kort='Ingen god turluke', detalj='Ingen sammenhengende luke på
    if(beste){
        besteTid=`${beste.fra}–${beste.til}`;
        if(beste.kvalitet==='perfekt' && beste.timer>=3) score=5;
        else if((beste.kvalitet==='perfekt'||beste.kvalitet==='god') && beste.timer>=2) score=4;
        else if(regnAndel>0.35) score=2;
        else score=3;
        if(score===5){
            kort=`Best mellom ${besteTid}`;
            detalj=`Dette er dagens beste turluke: ${besteTid}. Opphold, solgløtt og lite vind gir
        } else if(score===4){
            kort=`Finest mellom ${besteTid}`;
            detalj=`Beste tidspunkt for tur er ${besteTid}. Det ser stort sett opphold ut, og vinde
        } else if(score===3){
            kort=`Greit mellom ${besteTid}`;
            detalj=`Det finnes en brukbar luke for tur mellom ${besteTid}. Forvent mer grått vær, s
        } else {
            kort=`Kort luke ${besteTid}`;
            detalj=`Det er mest urolig vær denne dagen, men ${besteTid} er den minst dårlige luken.
        }
    }
    const grov=base.length>0 && base.every(iv=>(iv.timer||1)>=6);
    if(grov && score>=3) detalj += ' Prognosen er grovere denne dagen, så tidsvinduet bør tolke
    if(vindSnitt>=8 && score>1) detalj += ' Merk at vinden blir kraftig utenfor det anbefalte t
    return {score, kort, besteTid, detalj};
}
function fmtVindPilFraGrader(deg){
    if(deg==null || Number.isNaN(Number(deg))) return '';
    const dirs=['↑','↗','→','↘','↓','↙','←','↖'];
    const idx=Math.round((((Number(deg)%360)+360)%360)/45)%8;
    return dirs[idx];
}
function dagStats(ivs, solOpp, solNed) {
    function tTilMin(t){ if(!t||!t.includes(':'))return 0; const[h,m]=t.split(':');return+h*60+
    const solOppMin=tTilMin(solOpp), solNedMin=tTilMin(solNed);
    function ivMin(iv){ return tTilMin((iv.start||'').substring(11,16)); }
    let regnMm=0, snoMm=0, vindSum=0, vindMaks=0, solTimer=0;
let solCount=0, delvisCount=0, overskyetCount=0;
let totalTimer=0, harVindData=false, harKastData=false;
let retSin=0, retCos=0, retWeight=0;
for(const iv of ivs){
    const nb=Number(iv.nedbor_mm||0);
    const temp=Number(iv.temperatur_c||0);
    const vind=(iv.vind_ms!=null && !Number.isNaN(Number(iv.vind_ms))) ? Number(iv.vind_ms) :
    const kast=(iv.vind_kast_ms!=null && !Number.isNaN(Number(iv.vind_kast_ms))) ? Number(iv.
    const retning=(iv.vindretning_grader!=null && !Number.isNaN(Number(iv.vindretning_grader)
    const ikon=iv.ver_ikon||'';
    const t=Number(iv.timer||1);
    if(temp>1.5) regnMm+=nb; else snoMm+=nb;
    vindSum+=vind*t;
    totalTimer+=t;
    if(iv.vind_ms!=null) harVindData=true;
    if(kast!=null) harKastData=true;
    const vindTop=Math.max(vind, kast!=null?kast:0);
    if(vindTop>vindMaks) vindMaks=vindTop;
    if(retning!=null){
        const weight=Math.max(vindTop, 0.1)*t;
        const rad=retning*Math.PI/180;
        retSin+=Math.sin(rad)*weight;
        retCos+=Math.cos(rad)*weight;
        retWeight+=weight;
    }
    if(['☀','    ','   '].some(s=>ikon.includes(s))&&ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin&
    if(ikon.includes('☀')) solCount+=t;
    else if(ikon.includes('    ')||ikon.includes('    ')) delvisCount+=t;
    else if(ikon.includes('☁')||ikon.includes('      ')||ikon.includes('    ')) overskyetCount+=t;
}
const vindSnitt=totalTimer?vindSum/totalTimer:0;
const vindRetningGrader=retWeight?((Math.atan2(retSin, retCos)*180/Math.PI)+360)%360:null;
const vindPil=fmtVindPilFraGrader(vindRetningGrader);
const avrundetSnitt=harVindData?Math.round(vindSnitt):null;
const avrundetMaks=(harVindData||harKastData)?Math.round(vindMaks):null;
const vindKort=avrundetSnitt!=null
    ? (harKastData && avrundetMaks!=null ? `${avrundetSnitt}(${avrundetMaks})` : `${avrundetS
    : '–';
const symboler=[];
if(solCount>=Math.max(delvisCount,overskyetCount) && solCount>0) symboler.push('          Sol');
else if(overskyetCount>=Math.max(solCount,delvisCount) && overskyetCount>0) symboler.push('
else if(delvisCount>0) symboler.push('        Delvis skyet');
if(snoMm>0) symboler.push(`        Snø ${Math.round(snoMm*10)/10} mm`);
if(regnMm>0) symboler.push(`        Regn ${Math.round(regnMm*10)/10} mm`);
if(!symboler.length) symboler.push('        Variabelt');
return {
    regnMm: Math.round(regnMm*10)/10,
         snoMm:       Math.round(snoMm*10)/10,
         totMm:       Math.round((regnMm+snoMm)*10)/10,
         vindSnitt: Math.round(vindSnitt*10)/10,
         vindMaks:       Math.round(vindMaks*10)/10,
         vindKort,
         vindPil,
         vindRetningGrader: vindRetningGrader!=null ? Math.round(vindRetningGrader) : null,
         solTimer:       Math.round(solTimer),
         symboler,
    };
}
function buildTur(intervaller, daglig) {
    window._turIntervaller = intervaller;
    const el = document.getElementById('tur-list');
    if(!el) return;
    if(!intervaller||!intervaller.length){
         setTimeout(()=>{
              fetch('/kvamskogen-sommer/api/status').then(r=>r.json()).then(d=>{
                  if(d.intervaller&&d.intervaller.length) buildTur(d.intervaller, d.daglig||[]);
              }).catch(()=>{});
         }, 3000);
         return;
    }
    // Vis regelbasert umiddelbart
    renderTurLokal(intervaller, el);
    // Oppdater stille med AI i bakgrunnen
    fetch('/kvamskogen-sommer/api/tur-ai')
         .then(r=>r.json())
         .then(d=>{
              if(d.ok && d.dager && d.dager.length){
                  renderTurDager(d.dager, el, intervaller);
              }
         })
         .catch(()=>{});
}
function renderTurLokal(intervaller, el) {
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
      return{dato,ukedag:UKEDAGER[dt.getDay()],soloppgang:solOpp,solnedgang:solNed,...a,beste_t
    });
    renderTurDager(dager, el, intervaller);
}
function renderTurDager(dager, el, intervaller) {
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
      const datoStr=(()=>{const d=new Date((dato||'')+'T12:00:00');const MNORSK=['jan','feb','m
      const emoji=SCORE_EMOJI[score]||'       ';
      const tidHtml=besteTid?`<span class="ski-beste-tid">        ${besteTid}</span>`:'';
      // Daglig statistikk
      const ivs=perDag[dato]||[];
      const st=ivs.length?dagStats(ivs,solOpp,solNed):null;
      const symbolsHtml=(st&&st.symboler&&st.symboler.length)
          ? `<div class="ski-day-symbols">${st.symboler.map(s=>`<span class="ski-day-symbol">${s}
          : '';
      return `<div class="ski-day">
          <div class="ski-day-header" onclick="toggleSkiDay(${i})">
            <div class="ski-day-left score-${score}">
              <div class="ski-day-icon-big">${emoji}</div>
              <div class="ski-day-name-top">${dagStr}</div>
            </div>
            <div class="ski-day-right">
              <div class="ski-day-title">${dagStr} ${datoStr}${st?` <span class="ski-day-stats-in
              ${symbolsHtml}
              <div class="ski-day-kort">${kort}</div>
              <div class="ski-day-kort" style="margin-top:4px;color:var(--text);opacity:0.75;">${
              <div class="ski-day-meta">
                   ${tidHtml}
                   <span class="ski-arrow" id="ski-arrow-${i}">▾ Graf + tabell</span>
              </div>
            </div>
          </div>
          <div class="ski-day-detail" id="ski-detail-${i}" data-dato="${dato}" data-rendered="0"
            <div class="ski-day-chart-section" id="ski-chart-section-${i}" style="display:none;">
              <div class="ski-day-chart-wrap">
                   <canvas id="ski-chart-${i}"></canvas>
                   <div class="ski-day-chart-msg" id="ski-chart-msg-${i}"><span class="spinner"></sp
              </div>
              <div class="ski-day-table-wrap" id="ski-table-wrap-${i}"></div>
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
    if(arrow) arrow.textContent=open?'▴ Graf + tabell':'▾ Graf + tabell';
    if(open && detail && detail.dataset.rendered==='0'){
        detail.dataset.rendered='1';
        renderSkiDetails(i, detail.dataset.dato);
    }
}
const _skiCharts={};
function renderSkiDetails(i, dato){
    renderSkiChart(i, dato);
    renderSkiTable(i, dato);
}
function renderSkiChart(i, dato){
    const msg=document.getElementById('ski-chart-msg-'+i);
    const ctx=document.getElementById('ski-chart-'+i);
    if(!ctx) return;
    // Filtrer intervaller for denne datoen
    const ivs=(window._turIntervaller||[]).filter(iv=>(iv.start||'').startsWith(dato));
    if(!ivs.length){ if(msg) msg.textContent='Ingen timedata'; return; }
    if(msg) msg.style.display='none';
    const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
    const labels=ivs.map(iv=>{
        const dt=new Date(iv.start); return String(dt.getHours()).padStart(2,'0')+':00';
    });
    const temps=ivs.map(iv=>iv.temperatur_c!=null?parseFloat(iv.temperatur_c):null);
    const precip=ivs.map(iv=>iv.nedbor_mm!=null?parseFloat(iv.nedbor_mm):null);
    const precipColors=ivs.map(iv=>(iv.temperatur_c||0)<=0?'rgba(116,192,252,0.7)':'rgba(255,10
    const wind=ivs.map(iv=>iv.vind_ms!=null?parseFloat(iv.vind_ms):null);
    if(_skiCharts[i]) _skiCharts[i].destroy();
    _skiCharts[i]=new Chart(ctx,{
        data:{labels,datasets:[
          {type:'bar',label:'Nedbør',data:precip,backgroundColor:precipColors,borderRadius:2,yAxi
          {type:'line',label:'Temp',data:temps,borderWidth:2,pointRadius:0,tension:0.3,fill:false
              segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},
          {type:'line',label:'Vind',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroundColor
              borderWidth:1.5,borderDash:[3,3],pointRadius:0,tension:0.3,fill:false,yAxisID:'yW',or
      ]},
      options:{
          responsive:true,maintainAspectRatio:false,
          plugins:{legend:{display:false},tooltip:{backgroundColor:'rgba(8,14,31,.95)',titleColor
              callbacks:{label:c=>{
                if(c.parsed.y==null)return null;
                if(c.dataset.label==='Temp')return`Temp: ${c.parsed.y>0?'+':''}${c.parsed.y.toFixed
                if(c.dataset.label==='Vind')return`Vind: ${c.parsed.y.toFixed(1)} m/s`;
                return`Nedbør: ${c.parsed.y.toFixed(1)} mm`;
              }}}},
          scales:{
              x:{ticks:{color:'#7b8db5',font:{size:9},maxRotation:0},grid:{color:'rgba(100,130,200,
              yT:{position:'left',ticks:{color:'#7b8db5',font:{size:9},callback:v=>fmtAxisTemp(v)},
              yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:9}},grid:
              yW:{display:false,min:0},
          }
      }
    });
}
function intervalType(iv){
    const nb=Number(iv.nedbor_mm||0);
    const temp=Number(iv.temperatur_c||0);
    if(nb<0.15) return 'Tørt';
    if(temp>1.5) return 'Regn';
    if(temp>0) return 'Sludd';
    return 'Snø';
}
function intervalWindTop(iv){
    const vind=iv.vind_ms!=null && !Number.isNaN(Number(iv.vind_ms)) ? Number(iv.vind_ms) : 0;
    const kast=iv.vind_kast_ms!=null && !Number.isNaN(Number(iv.vind_kast_ms)) ? Number(iv.vind
    return Math.max(vind, kast!=null ? kast : 0);
}
function vurderIntervall(iv){
    const nb=Number(iv.nedbor_mm||0);
    const temp=Number(iv.temperatur_c||0);
    const vind=iv.vind_ms!=null && !Number.isNaN(Number(iv.vind_ms)) ? Number(iv.vind_ms) : 0;
    const kast=iv.vind_kast_ms!=null && !Number.isNaN(Number(iv.vind_kast_ms)) ? Number(iv.vind
    const vindTop=Math.max(vind, kast!=null ? kast : 0);
    const type=intervalType(iv);
    const icon=iv.ver_ikon||'';
    const hasSun=['☀','      ','   '].some(s=>icon.includes(s));
    let score=0;
    let tone='good';
let label='Brukbart';
let note='Rolige forhold for dette intervallet.';
if(type==='Regn'){
    score -= 10;
    tone='bad';
    label=nb>=1 ? 'Regn – dropp' : 'Vått føre';
    note=nb>=1 ? 'Regn i dette intervallet gjør turen våt og ubehagelig.' : 'Mild nedbør bety
} else if(type==='Sludd'){
    score -= 3;
    tone='warn';
    label='Sludd – variabelt';
    note='På plussiden og med nedbør blir underlaget fort tyngre.';
} else if(type==='Snø'){
    score += nb>0.2 ? 2 : 1;
    label='Snøvær';
    note=nb>=0.7 ? 'Lett nedbør, men ofte greit hvis vinden holder seg.' : 'Lett nedbør kan v
} else {
    score += 4;
    label='Tørt og rolig';
    note='Lite nedbør gir ofte de mest stabile forholdene.';
}
if(vindTop>=14){
    score -= 8;
    tone='bad';
    label='Svært vindutsatt';
    note='Kraftige kast gjør dette intervallet klart mer krevende.';
} else if(vindTop>=10){
    score -= 5;
    tone=tone==='bad' ? 'bad' : 'warn';
    label='Mye vind';
    note='Vinden blir godt merkbar og kan gjøre turen ubehagelig.';
} else if(vindTop>=8){
    score -= 3;
    tone=tone==='bad' ? 'bad' : 'warn';
    label='På grensen pga vind';
    note='Dette er omtrent grensen der vinden begynner å trekke kvaliteten tydelig ned.';
} else if(vindTop<4){
    score += 3;
    if(type==='Tørt'){
        label='Svært bra';
        note='Opphold og lite vind gjør dette til et av de beste intervallene.';
    }
} else if(vindTop<6){
    score += 1;
    if(type==='Tørt'){
        label='Bra nå';
        note='Rolig vind og opphold gir gode forhold.';
        }
    }
    if(nb>=2){
        score -= 4;
        tone='bad';
        if(type!=='Regn'){
            label='Mye nedbør';
            note='Dette er et av de våteste intervallene og blir mer krevende enn resten av dagen.'
        }
    } else if(nb>=0.8){
        score -= 2;
        if(tone==='good') tone='warn';
        if(type==='Tørt'){
            label='Mer nedbør';
            note='En del nedbør i dette intervallet trekker komforten ned.';
        }
    }
    if(temp<=-8 && type!=='Regn'){
        score += 1;
        if(tone==='good') tone='cold';
        label='Kaldt men fint';
        note='Kald luft kan kjennes skarpt i ansiktet, men er ellers fint å gå i.';
    } else if(temp>=4 && type==='Tørt'){
        score -= 2;
        if(tone==='good') tone='warm';
        label='Mildt og mykt';
        note='Milde temperaturer gir behagelige turforhold.';
    }
    if(hasSun && type==='Tørt' && vindTop<6){
        score += 2;
        tone='good';
        label='Best i dag';
        note='Gløtt av sol, opphold og lite vind gjør dette intervallet ekstra attraktivt.';
    }
    return {score, tone, label, note, type, vindTop, nb, temp};
}
function finnIntervallHoydepunkter(ivs){
    if(!ivs.length) return {};
    const vurderinger=ivs.map(vurderIntervall);
    let bestIdx=0, windIdx=0, precipIdx=0, coldIdx=0, warmIdx=0;
    for(let i=1;i<ivs.length;i++){
        if(vurderinger[i].score>vurderinger[bestIdx].score) bestIdx=i;
        if(vurderinger[i].vindTop>vurderinger[windIdx].vindTop) windIdx=i;
        if(vurderinger[i].nb>vurderinger[precipIdx].nb) precipIdx=i;
        if(vurderinger[i].temp<vurderinger[coldIdx].temp) coldIdx=i;
        if(vurderinger[i].temp>vurderinger[warmIdx].temp) warmIdx=i;
    }
    return {
         vurderinger,
         bestIdx,
         windIdx: vurderinger[windIdx].vindTop>=8 ? windIdx : null,
         precipIdx: vurderinger[precipIdx].nb>=0.5 ? precipIdx : null,
         coldIdx: vurderinger[coldIdx].temp<=-5 ? coldIdx : null,
         warmIdx: vurderinger[warmIdx].temp>=3 ? warmIdx : null,
    };
}
function formatVindKort(iv){
    const vindSnitt=iv.vind_ms!=null?Math.round(Number(iv.vind_ms)):null;
    const vindMaks=iv.vind_kast_ms!=null?Math.round(Number(iv.vind_kast_ms)):null;
    const vindPil=fmtVindPilFraGrader(iv.vindretning_grader);
    const vind=vindSnitt!=null?(vindMaks!=null?`${vindSnitt}(${vindMaks})`:`${vindSnitt}`):'–';
    return vindPil?`${vind} ${vindPil}`:vind;
}
function byggSkiMetaChips(ivs, hoydepunkter){
    const chips=[];
    if(!ivs.length || !hoydepunkter || !hoydepunkter.vurderinger) return chips;
    const {bestIdx, windIdx, precipIdx, coldIdx, warmIdx, vurderinger}=hoydepunkter;
    function hhmm(iv){
         const dt=new Date(iv.start);
         return String(dt.getHours()).padStart(2,'0')+':00';
    }
    if(bestIdx!=null){
         chips.push(`<span class="ski-day-meta-chip best">     Best ${hhmm(ivs[bestIdx])} · ${vurder
    }
    if(windIdx!=null){
         chips.push(`<span class="ski-day-meta-chip wind">     Mest vind ${hhmm(ivs[windIdx])} · ${f
    }
    if(precipIdx!=null){
         chips.push(`<span class="ski-day-meta-chip precip">     Mest nedbør ${hhmm(ivs[precipIdx])}
    }
    if(coldIdx!=null){
         chips.push(`<span class="ski-day-meta-chip cold">     Kaldest ${hhmm(ivs[coldIdx])} · ${Num
    }
    if(warmIdx!=null){
         chips.push(`<span class="ski-day-meta-chip warm">     Mildest ${hhmm(ivs[warmIdx])} · ${Num
    }
    return chips;
}
function renderSkiTable(i, dato){
    const el=document.getElementById('ski-table-wrap-'+i);
    if(!el) return;
    const ivs=(window._turIntervaller||[]).filter(iv=>(iv.start||'').startsWith(dato));
    if(!ivs.length){ el.innerHTML=''; return; }
    const hp=finnIntervallHoydepunkter(ivs);
    const vurderinger=hp.vurderinger||[];
    const metaChips=byggSkiMetaChips(ivs, hp);
    const rows=ivs.map((iv, idx)=>{
      const dt=new Date(iv.start);
      const hh=String(dt.getHours()).padStart(2,'0')+':00';
      const ikon=iv.ver_ikon||'–';
      const tempNum=iv.temperatur_c!=null?Number(iv.temperatur_c):null;
      const t=tempNum!=null?`${tempNum>0?'+':''}${tempNum.toFixed(1)}°C`:'–';
      const nbNum=iv.nedbor_mm!=null?Number(iv.nedbor_mm):0;
      const nb=`${nbNum.toFixed(1)} mm`;
      const nbMin=iv.nedbor_min_mm!=null?Number(iv.nedbor_min_mm).toFixed(1):null;
      const nbMaks=iv.nedbor_maks_mm!=null?Number(iv.nedbor_maks_mm).toFixed(1):null;
      const visRange=(nbMin!=null&&nbMaks!=null&&(Number(nbMin)>0||Number(nbMaks)>0));
      const nbCombined=visRange?`${nb} (${nbMin}/${nbMaks})`:nb;
      const nbProbRaw=iv.nedbor_sannsynlighet_pct!=null?Math.round(Number(iv.nedbor_sannsynligh
      const nbProb=(nbNum===0 && (nbProbRaw===0 || nbProbRaw==null))?'–':(nbProbRaw!=null?`${nb
      const vindMedRetning=formatVindKort(iv);
      const v=vurderinger[idx]||vurderIntervall(iv);
      const type=v.type;
      const rowClasses=[];
      if(idx===hp.bestIdx) rowClasses.push('ski-day-row-best');
      if(v.tone==='warn' && idx!==hp.bestIdx) rowClasses.push('ski-day-row-warn');
      if(v.tone==='bad') rowClasses.push('ski-day-row-bad');
      const tempClass=tempNum!=null?(tempNum<=-5?'ski-cell-temp-cold':(tempNum>=3?'ski-cell-tem
      const nbClass=nbNum>=1.5?'ski-cell-nb-bad':(nbNum>=0.5?'ski-cell-nb-warn':'');
      const vindTop=v.vindTop;
      const vindClass=vindTop>=10?'ski-cell-wind-bad':(vindTop>=8?'ski-cell-wind-warn':'');
      const typeClass=type==='Regn'?'ski-cell-type-rain':(type==='Snø'?'ski-cell-type-snow':(ty
      const tags=[];
      if(idx===hp.bestIdx) tags.push('<span class="ski-vurdering-pill ski-vurdering-good">Best
      if(hp.windIdx===idx) tags.push('<span class="ski-vurdering-pill ski-vurdering-warn">Mest
      if(hp.precipIdx===idx) tags.push('<span class="ski-vurdering-pill ski-vurdering-warn">Mes
      if(hp.coldIdx===idx) tags.push('<span class="ski-vurdering-pill ski-vurdering-cold">Kalde
      if(hp.warmIdx===idx) tags.push('<span class="ski-vurdering-pill ski-vurdering-warm">Milde
      const toneClass=v.tone==='bad'?'ski-vurdering-bad':(v.tone==='warn'?'ski-vurdering-warn':
      tags.push(`<span class="ski-vurdering-pill ${toneClass}">${v.label}</span>`);
      const durationText=(iv.timer!=null && Number(iv.timer)>1)?` <span style="color:#94a3b8;fo
      return `<tr class="${rowClasses.join(' ')}"><td>${hh}${durationText}</td><td>${ikon}</td>
    }).join('');
    const mobileHint = window.innerWidth <= 700 ? '<div class="ski-day-table-scrollhint">Dra si
    el.innerHTML=`${mobileHint}${metaChips.length?`<div class="ski-day-table-meta">${metaChips.
      <thead><tr><th>Tid</th><th>Symbol</th><th>Temp</th><th>Nedbør (min/maks)</th><th>Sjanse n
      <tbody>${rows}</tbody>
    </table>`;
}
function toggleDetail(btn){
    const full=document.getElementById('hero-full');
    const open=full.classList.toggle('open');
    btn.textContent=open?'Les mindre ▴':'Les mer ▾';
}
function fmtTemp(v){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1
function fmtDelta(v,u='cm'){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.t
function fmtAxisTemp(v){const n=Math.round(Number(v)*10)/10;if(Number.isNaN(n))return '';retu
function fmtAxisMm(v){const n=Math.round(Number(v)*10)/10;if(Number.isNaN(n))return '';return
function init(){
    fetch('/kvamskogen-sommer/api/status')
      .then(r=>r.json())
      .then(d=>{
           renderStatus(d);
           if(!d.ver||d.ver.temp_na_c==null){
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
    const t=d.tolkning||{},ski=d.tur_summary||{};
    const bc={'green':'badge-green','amber':'badge-amber','red':'badge-red'}[t.badge_color]||'b
    const fullDetail = (t.detail||'').replace(/\n/g,'<br>');
    // Split på dobbel linjeskift for avsnitt
    const paras = (t.detail||'').split(/\n\n+/).map(x=>x.trim()).filter(Boolean);
    const firstPara = paras[0] || '';
    const hasMore = paras.length > 1;
    const detailHtml = hasMore
      ? `<span class="hero-detail-short">${firstPara}</span>
               <span class="hero-detail-full" id="hero-full">${paras.slice(1).join('<br><br>')}</span
               <button class="les-mer" onclick="toggleDetail(this)">Les mer ▾</button>`
      : `<span class="hero-detail-short">${fullDetail}</span>`;
    const skiSummaryHtml = ski.hero_text ? `<div class="hero-ski-summary">${ski.hero_text}</div
    document.getElementById('hero').innerHTML=`<div class="hero-top"><div class="hero-icon">${t
    // Værstatus
    const ver=d.ver||{};
    const fmtVind=v=>v==null?'–':(Number(v).toFixed(1)+' m/s');
    document.getElementById('m-temp').textContent=fmtTemp(ver.temp_na_c);
    document.getElementById('m-temp-sub').textContent='nå';
    document.getElementById('m-maks').textContent=fmtTemp(ver.maks_temp_c);
    document.getElementById('m-min').textContent=fmtTemp(ver.min_temp_c);
    // Vind nå – hent fra første intervall i prognosen
    const ivNa=(d.intervaller||[])[0]||{};
    document.getElementById('m-vind').textContent=fmtVind(ivNa.vind_ms);
    const vindKast=ivNa.vind_kast_ms;
    document.getElementById('m-vind-sub').textContent=vindKast!=null?'kast '+Number(vindKast).t
    const forecastEl=document.getElementById('forecast');
    if(forecastEl) forecastEl.innerHTML=(d.daglig||[]).map(dag=>{
         const dt=new Date(dag.dato+'T12:00:00');
         const navn=DAYS[dt.getDay()]+' '+dt.getDate()+'. '+MONTHS[dt.getMonth()];
         return`<div class="fday"><div class="fday-name">${navn}</div><div class="fday-icon">${dag
    }).join('');
    const ts=new Date(d.hentet);
    document.getElementById('footer').textContent=`Oppdatert: ${ts.toLocaleString('no-NO')} · D
    if(d.intervaller&&d.intervaller.length){fcastData=d.intervaller;renderFcast();}
    // Bygg skitur-anbefalinger fra intervaller
    if(d.intervaller&&d.intervaller.length) buildTur(d.intervaller, d.daglig||[]);
    if(d.kamera) renderKamera(d.kamera);
    }catch(e){console.error('renderStatus feil:', e);}
}
function renderKamera(kamera){
    const KAMERA_META={
         vegvesen:      {tittel:'   Vegvesen – Kvamskogen', url:'https://kamera.atlas.vegvesen.no/api/
         furedalen: {tittel:'       Furedalen skitrekk',    url:'https://furedalen-webcamera.no/bunn/b
         eikedalen: {tittel:'       Eikedalen skisenter',    url:'https://www.eikedalen.no/wc3/image00
    };
    function badgeClass(type){
         const m={'snø':'kb-sno','regn':'kb-regn','sludd':'kb-sludd','tørt':'kb-tort',
                     'god':'kb-god','tåke':'kb-taake','dårlig':'kb-darlig'};
         return m[type]||'kb-ukjent';
    }
    for(const [navn,meta] of Object.entries(KAMERA_META)){
         const el=document.getElementById('kamera-'+navn);
         if(!el) continue;
         const data=kamera[navn]||{};
         const ts=data.hentet?new Date(data.hentet).toLocaleTimeString('no-NO',{hour:'2-digit',min
         // Legg til cache-buster på bildet så det alltid er ferskt
         const imgUrl=meta.url+'?t='+Date.now();
         let aiHtml='';
         if(!data.feil&&data.sammendrag){
             aiHtml=`
               <div class="kamera-ai">
                 <div class="ai-linje"><span class="ai-label">Nedbør</span><span class="kamera-badge
                 <div class="ai-linje"><span class="ai-label">Sikt</span><span class="kamera-badge $
                 <div class="ai-linje"><span class="ai-label">Bakke</span><span class="kamera-badge
                 <div class="kamera-sammendrag">"${data.sammendrag}"</div>
               </div>`;
         } else if(data.feil){
             aiHtml='<div class="kamera-ai" style="color:#94a3b8">AI-analyse ikke tilgjengelig</div>
         }
         el.innerHTML=`
            <img src="${imgUrl}" alt="${meta.tittel}" loading="lazy" onerror="this.style.display='n
            <div class="kamera-info">
                <div class="kamera-tittel">${meta.tittel}</div>
                ${aiHtml}
                ${ts?`<div class="kamera-tid">Analysert ${ts}</div>`:''}
            </div>`;
    }
}
async function loadHistorikk(){
    const msg=document.getElementById('chart-msg');
    msg.innerHTML='<span class="spinner"></span>';msg.style.display='flex';
    try{
        const d=await(await fetch(`/kvamskogen-sommer/api/historikk?hours=${currentHours}`)).json
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
    const labels=hourly.map(x=>{const dt=new Date(x.time);const h=dt.getHours();if(h===0)return
    const temps=hourly.map(x=>parseFloat(x.temperature));
    const precip=hourly.map(x=>x.precipitation!=null?parseFloat(x.precipitation):null);
    const precipColors=hourly.map(x=>{const t=parseFloat(x.temperature);return t<=0?'rgba(96,16
    const wind=hourly.map(x=>x.wind_speed!=null?parseFloat(x.wind_speed):null);
    const ctx=document.getElementById('hist-chart').getContext('2d');
    if(histChart)histChart.destroy();
    histChart=new Chart(ctx,{
        data:{labels,datasets:[
            {type:'bar',label:'Nedbør (mm)',data:precip,backgroundColor:precipColors,borderRadius:2
            {type:'line',label:'Temperatur (°C)',data:temps,borderWidth:2,pointRadius:0,pointHoverR
            {type:'line',label:'Vind (m/s)',data:wind,borderColor:'rgba(167,139,250,0.8)',backgroun
        ]},
        options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:fa
            plugins:{legend:{display:false},tooltip:{backgroundColor:'rgba(15,23,42,.92)',callbacks
            scales:{
                x:{ticks:{color:'#94a3b8',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:1
                yTemp:{position:'left',ticks:{color:'#94a3b8',font:{size:10},callback:v=>fmtAxisTemp(
                yPrecip:{position:'right',min:0,suggestedMax:1,ticks:{color:'#94a3b8',font:{size:10},
                yWind:{display:false,min:0}
            }
        }
    });
}
function setHours(h,btn){
    currentHours=h;
    document.querySelectorAll('.chart-hours button').forEach(el=>el.classList.remove('active'))
    if(btn)btn.classList.add('active');
    loadHistorikk();
}
init();
loadHistorikk();
</script>
</body>
</html>
"""
