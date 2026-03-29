"""
sno_routes.py – Generell snødashboard for norske værstajoner
Rute: /snø/                  → oversikt alle stasjoner
Rute: /snø/<stasjon_id>      → dashboard for én stasjon
"""
from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Optional

import requests
from flask import Blueprint, Response, jsonify, request

sno_bp = Blueprint("sno", __name__, url_prefix="/snø")

# ── Konfig ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL   = "claude-sonnet-4-6"
FROST_BASE_URL    = "https://frost.met.no"
FROST_TIMEOUT     = 20
_IS_RENDER        = os.getenv("RENDER") == "true"
_BASE             = "https://prisanalyse.no" if _IS_RENDER else "http://localhost:5000"

# ── Stasjonsdatabase ──────────────────────────────────────────────────────────
_STATION_DB: Optional[object] = None

def _load_stations():
    global _STATION_DB
    if _STATION_DB is not None:
        return _STATION_DB
    try:
        from ver_station_db import load_station_db
        df = load_station_db()
        snow = df[df["has_snow"] == True].copy()
        snow = snow.dropna(subset=["baseId", "lat", "lon"])
        _STATION_DB = snow
        return snow
    except Exception:
        traceback.print_exc()
        return None

def _get_stasjon(stasjon_id: str) -> Optional[dict]:
    """Hent én stasjon fra parquet."""
    df = _load_stations()
    if df is None:
        return None
    row = df[df["baseId"] == stasjon_id]
    if row.empty:
        return None
    r = row.iloc[0]
    return {
        "id":    str(r["baseId"]),
        "navn":  str(r.get("name", r["baseId"])),
        "kort":  str(r.get("shortName", r.get("name", r["baseId"]))),
        "lat":   float(r["lat"]),
        "lon":   float(r["lon"]),
        "fylke": str(r.get("county", "")),
    }

# ── Cache ─────────────────────────────────────────────────────────────────────
_STATUS_CACHE:    dict = {}
_HISTORIKK_CACHE: dict = {}
_CACHE_TTL = 15 * 60

# ── Norge-wide snødybde-cache (prefetchet ved oppstart) ───────────────────────
_NORGE_SNØ_CACHE: dict = {}          # stasjonId → float (cm)
_NORGE_SNØ_TS:    float = 0.0        # unix-timestamp for siste refresh
_NORGE_SNØ_TTL   = 30 * 60          # refresh hvert 30. min

def _hent_alle_snodybder_norge() -> dict:
    """Hent siste snødybde for alle has_snow-stasjoner i ett eller flere batch-kall."""
    client_id     = os.getenv("FROST_CLIENT_ID", "")
    client_secret = os.getenv("FROST_CLIENT_SECRET", "")
    if not client_id:
        return {}
    df = _load_stations()
    if df is None:
        return {}
    alle_ids = df["baseId"].tolist()
    resultat: dict = {}
    batch = 100
    for i in range(0, len(alle_ids), batch):
        chunk = alle_ids[i:i+batch]
        try:
            r = requests.get(
                f"{FROST_BASE_URL}/observations/v0.jsonld",
                auth=(client_id, client_secret),
                params={
                    "sources":       ",".join(chunk),
                    "referencetime": "latest",
                    "elements":      "surface_snow_thickness",
                    "limit":         500,
                },
                timeout=FROST_TIMEOUT,
            )
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sid = item.get("sourceId", "").split(":")[0]
                    for obs in item.get("observations", []):
                        if obs.get("elementId") == "surface_snow_thickness":
                            v = obs.get("value")
                            if v is not None:
                                resultat[sid] = float(v)
        except Exception:
            traceback.print_exc()
    return resultat

def _warmup_norge():
    """Kjør i bakgrunnstråd: hent snødybde for hele Norge og oppdater cache."""
    global _NORGE_SNØ_CACHE, _NORGE_SNØ_TS
    try:
        data = _hent_alle_snodybder_norge()
        if data:
            _NORGE_SNØ_CACHE = data
            _NORGE_SNØ_TS    = time.time()
    except Exception:
        traceback.print_exc()
    finally:
        import threading
        t = threading.Timer(_NORGE_SNØ_TTL, _warmup_norge)
        t.daemon = True
        t.start()

def start_warmup():
    """Kalles én gang ved app-oppstart fra app.py: sno_routes.start_warmup()"""
    import threading
    t = threading.Thread(target=_warmup_norge, daemon=True)
    t.start()

# ── Sol-beregning ─────────────────────────────────────────────────────────────
def _sol_tider(dato_str: str, lat: float, lon: float) -> tuple:
    import math
    from datetime import date
    lat_r = math.radians(lat)
    dato  = date.fromisoformat(dato_str)
    dag_nr = dato.timetuple().tm_yday
    decl = math.radians(23.45 * math.sin(math.radians(360/365*(dag_nr-81))))
    cos_ha = (math.sin(math.radians(-0.833)) - math.sin(lat_r)*math.sin(decl)) / (math.cos(lat_r)*math.cos(decl))
    if abs(cos_ha) > 1:
        return None, None
    ha = math.degrees(math.acos(cos_ha))
    tz_offset = 2 if 3 <= dato.month <= 10 else 1
    noon = 12 - lon/15 + tz_offset
    def fmt(h):
        hh = int(h); mm = int((h-hh)*60)
        return f"{hh:02d}:{mm:02d}"
    return fmt(noon - ha/15), fmt(noon + ha/15)

# ── Datahenting ───────────────────────────────────────────────────────────────
def _hent_snovarsel(lat: float, lon: float, frost_id: str) -> dict:
    """Hent snovarsel direkte fra ver_routes – ingen HTTP-kall."""
    try:
        try:
            from scripts.ver_routes import _hent_prognose_data
        except ImportError:
            from ver_routes import _hent_prognose_data
        return _hent_prognose_data("", lat=lat, lon=lon, frost_id=frost_id)
    except Exception:
        traceback.print_exc()
        return {}

def _hent_frost_historikk(stasjon_id: str, timer: int = 24) -> list:
    """Frost observasjoner for en stasjon."""
    try:
        client_id     = os.getenv("FROST_CLIENT_ID", "")
        client_secret = os.getenv("FROST_CLIENT_SECRET", "")
        if not client_id:
            return []
        from datetime import timedelta
        now   = datetime.now(timezone.utc)
        start = (now - timedelta(hours=timer)).isoformat().replace("+00:00", "Z")
        slutt = now.isoformat().replace("+00:00", "Z")
        r = requests.get(
            f"{FROST_BASE_URL}/observations/v0.jsonld",
            auth=(client_id, client_secret),
            params={
                "sources":       stasjon_id,
                "referencetime": f"{start}/{slutt}",
                "elements":      "surface_snow_thickness,air_temperature,sum(precipitation_amount PT1H),wind_speed",
                "limit":         500,
            },
            timeout=FROST_TIMEOUT,
        )
        if r.status_code != 200:
            return []
        data = r.json().get("data", [])
        rows = []
        for item in data:
            t = item.get("referenceTime", "")
            for obs in item.get("observations", []):
                rows.append({"tid": t, "element": obs.get("elementId"), "verdi": obs.get("value")})
        return rows
    except Exception:
        traceback.print_exc()
        return []

def _hent_frost_sno_na(stasjon_id: str) -> Optional[float]:
    """Siste snødybde fra Frost."""
    try:
        client_id     = os.getenv("FROST_CLIENT_ID", "")
        client_secret = os.getenv("FROST_CLIENT_SECRET", "")
        if not client_id:
            return None
        r = requests.get(
            f"{FROST_BASE_URL}/observations/v0.jsonld",
            auth=(client_id, client_secret),
            params={
                "sources":       stasjon_id,
                "referencetime": "latest",
                "elements":      "surface_snow_thickness",
                "limit":         1,
            },
            timeout=FROST_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        data = r.json().get("data", [])
        if not data:
            return None
        for obs in data[0].get("observations", []):
            if obs.get("elementId") == "surface_snow_thickness":
                return obs.get("value")
        return None
    except Exception:
        return None

# ── AI-tolkning ───────────────────────────────────────────────────────────────
_AI_PROMPT = """Du er en lokal værvakt som skriver engasjerende meldinger til en hytteeier.
Skriv naturlig, muntlig norsk bokmål.

Svar KUN med gyldig JSON (ingen markdown):
{"verdict":"...","detail":"...","snow_quality":"...","badge_color":"...","icon":"..."}

1. verdict (maks 10 ord): Situasjonen akkurat nå.
2. detail (100-200 ord) i 2-3 avsnitt: forholdene nå, fremtidsutsikter med beste dag.
3. snow_quality: "Utmerket"|"Godt"|"Moderat"|"Dårlig"
4. badge_color: "green"|"amber"|"red"
5. icon: ⛷️ 🎿 ☀️ 🌨️ 🌧️ 🌫️ 🥶 💧""".strip()

def _ai_tolkning(sno_data: dict, sno_na: Optional[float]) -> dict:
    if not ANTHROPIC_API_KEY or not sno_data:
        return {"verdict": "Henter data...", "detail": "", "snow_quality": "Ukjent",
                "badge_color": "amber", "icon": "🏔️"}
    try:
        s = sno_data.get("sammendrag", {})
        payload = {
            "snodybde_cm":    sno_na,
            "temp_na_c":      s.get("temperatur_nå_c"),
            "ny_sno_48t_cm":  s.get("total_ny_snø_cm"),
            "prognose_neste_dager": [
                {
                    "dato":      d.get("dato"),
                    "ukedag":    ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][
                                     __import__("datetime").date.fromisoformat(d.get("dato","2000-01-01")).weekday()
                                 ] if d.get("dato") else "",
                    "min_c":     d.get("min_temp_c"),
                    "maks_c":    d.get("maks_temp_c"),
                    "ny_sno_cm": round(d.get("total_ny_snø_mm", 0) / 10, 1),
                    "vind_ms":   d.get("vind_ms_snitt"),
                    "ver":       d.get("vær_label"),
                }
                for d in sno_data.get("daglig", [])[:7]
            ],
        }
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": ANTHROPIC_MODEL, "max_tokens": 1024,
                  "system": _AI_PROMPT,
                  "messages": [{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}]},
            timeout=45,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        return json.loads(text)
    except Exception:
        traceback.print_exc()
        return {"verdict": "Data hentet", "detail": "", "snow_quality": "Ukjent",
                "badge_color": "amber", "icon": "🏔️"}

# ── Bakgrunns-refresh ─────────────────────────────────────────────────────────
_REFRESHING: set = set()

def _refresh_status(stasjon_id: str, lat: float, lon: float):
    if stasjon_id in _REFRESHING:
        return
    _REFRESHING.add(stasjon_id)
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut_sno    = ex.submit(_hent_snovarsel, lat, lon, stasjon_id)
            fut_sno_na = ex.submit(_hent_frost_sno_na, stasjon_id)
            sno_data = fut_sno.result(timeout=60)
            sno_na   = fut_sno_na.result(timeout=20)

        s        = sno_data.get("sammendrag", {})

        # Bygg payload uten AI først – returner raskt
        payload = _build_payload({}, sno_data, sno_na, s)
        _STATUS_CACHE[stasjon_id] = {
            "expires_at": time.time() + _CACHE_TTL,
            "payload": payload,
        }

        # Kjør AI i bakgrunnen og oppdater cachen
        import threading
        def _ai_update():
            tolkning = _ai_tolkning(sno_data, sno_na or s.get("start_snødybde_cm"))
            payload_med_ai = _build_payload(tolkning, sno_data, sno_na, s)
            _STATUS_CACHE[stasjon_id] = {
                "expires_at": time.time() + _CACHE_TTL,
                "payload": payload_med_ai,
            }
        threading.Thread(target=_ai_update, daemon=True).start()

    except Exception:
        traceback.print_exc()
    finally:
        _REFRESHING.discard(stasjon_id)


def _build_payload(tolkning: dict, sno_data: dict, sno_na, s: dict) -> dict:
    daglig = sno_data.get("daglig", [])
    return {
        "hentet":   datetime.now().isoformat(timespec="seconds"),
        "tolkning": tolkning,
        "sno": {
            "dybde_cm":      sno_na or s.get("start_snødybde_cm"),
            "temp_na_c":     s.get("temperatur_nå_c"),
            "ny_sno_48t_cm": s.get("total_ny_snø_cm"),
            "min_temp_c":    s.get("min_temp_c"),
            "maks_temp_c":   s.get("maks_temp_c"),
            "endring_1t_cm": s.get("endring_neste_time_cm"),
            "endring_3t_cm": s.get("endring_neste_3t_cm"),
        },
        "daglig": [
            {
                "dato":       d.get("dato"),
                "min_temp_c": d.get("min_temp_c"),
                "maks_temp_c":d.get("maks_temp_c"),
                "ny_sno_cm":  round((d.get("total_ny_snø_mm") or 0) / 10, 1),
                "ver_ikon":   d.get("vær_ikon"),
                "ver_label":  d.get("vær_label"),
                "vind_ms":    d.get("vind_ms_snitt"),
                "snodybde_cm":d.get("snødybde_slutt_cm"),
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

# ── API-endepunkter ───────────────────────────────────────────────────────────

@sno_bp.get("/api/stasjoner")
def api_stasjoner():
    """Liste over alle stasjoner med snødata."""
    df = _load_stations()
    if df is None:
        return jsonify({"ok": False, "stasjoner": []})
    stasjoner = []
    for _, r in df.iterrows():
        stasjoner.append({
            "id":    str(r["baseId"]),
            "navn":  str(r.get("name", r["baseId"])),
            "kort":  str(r.get("shortName", r.get("name", r["baseId"]))),
            "lat":   float(r["lat"]),
            "lon":   float(r["lon"]),
            "fylke": str(r.get("county", "")),
        })
    return jsonify({"ok": True, "antall": len(stasjoner), "stasjoner": stasjoner})


@sno_bp.get("/api/norge_sno")
def api_norge_sno():
    """
    Returnerer hele Norge-cachen: stasjonId → snødybde_cm.
    Brukes av frontend for topplister. Serveres fra minne, lynraskt.
    Hvis cachen er tom (kald start), hentes data synkront.
    """
    global _NORGE_SNØ_CACHE, _NORGE_SNØ_TS
    now_ts = time.time()
    if not _NORGE_SNØ_CACHE or (now_ts - _NORGE_SNØ_TS) > _NORGE_SNØ_TTL * 2:
        # Kald start eller svært gammel cache – hent synkront (én gang)
        data = _hent_alle_snodybder_norge()
        if data:
            _NORGE_SNØ_CACHE = data
            _NORGE_SNØ_TS    = now_ts
    alder_min = int((now_ts - _NORGE_SNØ_TS) / 60) if _NORGE_SNØ_TS else None
    return jsonify({
        "ok":        True,
        "stasjoner": _NORGE_SNØ_CACHE,
        "antall":    len(_NORGE_SNØ_CACHE),
        "alder_min": alder_min,
    })


@sno_bp.get("/api/sno_endring")
def api_sno_endring():
    """
    Hent snødybde for et sett stasjoner 2 døgn tilbake og beregn endring.
    Query-param: ids=SN18700,SN18701,...  (kommaseparert, maks 150)
    Returnerer: {ok, endringer: {id: {na, for2d, diff}}, antall}
    Caches 30 min.
    """
    from datetime import timedelta

    client_id     = os.getenv("FROST_CLIENT_ID", "")
    client_secret = os.getenv("FROST_CLIENT_SECRET", "")
    if not client_id:
        return jsonify({"ok": False, "error": "Frost ikke konfigurert"}), 503

    ids_raw = request.args.get("ids", "")
    if not ids_raw:
        return jsonify({"ok": False, "error": "Mangler ids-parameter"}), 400

    ids = [i.strip() for i in ids_raw.split(",") if i.strip()][:150]
    if not ids:
        return jsonify({"ok": False, "error": "Ingen gyldige IDs"}), 400

    # Cache-nøkkel basert på sorterte IDs (dato inngår implisitt via TTL)
    cache_key = "endring_" + "_".join(sorted(ids)[:8]) + f"_{len(ids)}"
    now_ts = time.time()
    hit = _STATUS_CACHE.get(cache_key)
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    # Dagens verdier fra Norge-cachen (ingen ekstra Frost-kall)
    na_verdier = {sid: _NORGE_SNØ_CACHE[sid] for sid in ids if sid in _NORGE_SNØ_CACHE}

    # Dato 2 døgn tilbake (gir pålitelige observasjoner uavhengig av tidspunkt)
    for2d_dato = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    ref_start  = f"{for2d_dato}T00:00:00Z"
    ref_slutt  = f"{for2d_dato}T23:59:59Z"

    for2d_verdier: dict = {}
    try:
        r = requests.get(
            f"{FROST_BASE_URL}/observations/v0.jsonld",
            auth=(client_id, client_secret),
            params={
                "sources":       ",".join(ids),
                "referencetime": f"{ref_start}/{ref_slutt}",
                "elements":      "surface_snow_thickness",
                "timeoffsets":   "PT0H",          # kun 06:00-observasjonen
                "limit":         500,
            },
            timeout=FROST_TIMEOUT,
        )
        if r.status_code == 200:
            for item in r.json().get("data", []):
                sid = item.get("sourceId", "").split(":")[0]
                for obs in item.get("observations", []):
                    if obs.get("elementId") == "surface_snow_thickness":
                        v = obs.get("value")
                        if v is not None:
                            # Behold høyeste verdi for dagen (kan være flere obs)
                            if sid not in for2d_verdier or float(v) > for2d_verdier[sid]:
                                for2d_verdier[sid] = float(v)
    except Exception:
        traceback.print_exc()

    # Beregn endring
    endringer = {}
    for sid in ids:
        na  = na_verdier.get(sid)
        f2d = for2d_verdier.get(sid)
        if na is not None and f2d is not None:
            endringer[sid] = {
                "na":   na,
                "for2d": f2d,
                "diff":  round(na - f2d, 1),
            }

    payload = {
        "ok":        True,
        "endringer": endringer,
        "antall":    len(endringer),
        "for2d_dato": for2d_dato,
    }
    _STATUS_CACHE[cache_key] = {"expires_at": now_ts + 30 * 60, "payload": payload}
    return jsonify(payload)


@sno_bp.get("/api/snodybder")
def api_snodybder():
    """
    Hent siste snødybde for stasjoner innenfor et bbox eller innenfor radius fra et punkt.
    Query-params:
      bbox=south,west,north,east   ELLER
      lat=&lon=&km=                (radius-søk)
    Maks 150 stasjoner per kall. Resultat caches 30 min.
    """
    client_id     = os.getenv("FROST_CLIENT_ID", "")
    client_secret = os.getenv("FROST_CLIENT_SECRET", "")
    if not client_id:
        return jsonify({"ok": False, "error": "Frost ikke konfigurert"}), 503

    df = _load_stations()
    if df is None:
        return jsonify({"ok": False, "error": "Ingen stasjonsdata"}), 503

    # ── Finn relevante stasjoner ──────────────────────────────────────────────
    bbox_str = request.args.get("bbox", "")
    lat_s    = request.args.get("lat", "")
    lon_s    = request.args.get("lon", "")
    km_s     = request.args.get("km", "150")

    subset = df.copy()

    if bbox_str:
        try:
            s, w, n, e = [float(x) for x in bbox_str.split(",")]
            subset = subset[
                (subset["lat"] >= s) & (subset["lat"] <= n) &
                (subset["lon"] >= w) & (subset["lon"] <= e)
            ]
        except Exception:
            return jsonify({"ok": False, "error": "Ugyldig bbox"}), 400

    elif lat_s and lon_s:
        import math
        try:
            ulat = float(lat_s)
            ulon = float(lon_s)
            km   = min(float(km_s), 300)
        except Exception:
            return jsonify({"ok": False, "error": "Ugyldig lat/lon/km"}), 400

        def _hav(r):
            d2r = math.pi / 180
            dlat = (r["lat"] - ulat) * d2r
            dlon = (r["lon"] - ulon) * d2r
            a = math.sin(dlat/2)**2 + math.cos(ulat*d2r)*math.cos(r["lat"]*d2r)*math.sin(dlon/2)**2
            return 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        subset = subset.copy()
        subset["_dist"] = subset.apply(_hav, axis=1)
        subset = subset[subset["_dist"] <= km].sort_values("_dist")
    else:
        return jsonify({"ok": False, "error": "Oppgi bbox eller lat/lon/km"}), 400

    subset = subset.head(150)
    if subset.empty:
        return jsonify({"ok": True, "stasjoner": {}})

    # ── Cache-nøkkel ──────────────────────────────────────────────────────────
    ids = sorted(subset["baseId"].tolist())
    cache_key = "snodybder_" + "_".join(ids[:10]) + f"_{len(ids)}"
    now_ts = time.time()
    hit = _STATUS_CACHE.get(cache_key)
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    # ── Frost batch-kall ──────────────────────────────────────────────────────
    resultat = {}
    try:
        sources = ",".join(ids)
        r = requests.get(
            f"{FROST_BASE_URL}/observations/v0.jsonld",
            auth=(client_id, client_secret),
            params={
                "sources":       sources,
                "referencetime": "latest",
                "elements":      "surface_snow_thickness",
                "limit":         500,
            },
            timeout=FROST_TIMEOUT,
        )
        if r.status_code == 200:
            for item in r.json().get("data", []):
                sid  = item.get("sourceId", "").split(":")[0]
                for obs in item.get("observations", []):
                    if obs.get("elementId") == "surface_snow_thickness":
                        v = obs.get("value")
                        if v is not None:
                            resultat[sid] = float(v)
    except Exception:
        traceback.print_exc()

    payload = {"ok": True, "stasjoner": resultat, "antall": len(resultat)}
    _STATUS_CACHE[cache_key] = {"expires_at": now_ts + 30 * 60, "payload": payload}
    return jsonify(payload)


@sno_bp.get("/<stasjon_id>/api/status")
def api_status(stasjon_id: str):
    stasjon = _get_stasjon(stasjon_id)
    if not stasjon:
        return jsonify({"error": "Ukjent stasjon"}), 404

    import threading
    now_ts = time.time()
    hit = _STATUS_CACHE.get(stasjon_id)

    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    # Start bakgrunnsoppdatering alltid
    threading.Thread(
        target=_refresh_status,
        args=(stasjon_id, stasjon["lat"], stasjon["lon"]),
        daemon=True
    ).start()

    if hit:
        return jsonify(hit["payload"])

    # Ingen cache – returner tomt, JS poller hvert 3 sek
    return jsonify({"hentet": datetime.now().isoformat(), "tolkning": {},
                    "sno": {}, "daglig": [], "intervaller": []})


@sno_bp.get("/<stasjon_id>/api/historikk")
def api_historikk(stasjon_id: str):
    stasjon = _get_stasjon(stasjon_id)
    if not stasjon:
        return jsonify({"ok": False, "error": "Ukjent stasjon"}), 404

    timer = max(1, min(72, int(request.args.get("hours", 24))))
    cache_key = f"{stasjon_id}_{timer}"
    now_ts = time.time()
    hit = _HISTORIKK_CACHE.get(cache_key)
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    data = _hent_frost_historikk(stasjon_id, timer)
    payload = {"ok": True, "data": data, "antall": len(data)}
    _HISTORIKK_CACHE[cache_key] = {"expires_at": now_ts + _CACHE_TTL, "payload": payload}
    return jsonify(payload)


_SKITUR_AI_CACHE: dict = {}
_SKITUR_AI_CACHE_TTL = 120 * 60  # 2 timer, samme som resten

_SKITUR_AI_PROMPT = """Du er en lokal turguide som analyserer timedata og gir korte, konkrete turanbefalinger på norsk bokmål.

For hver dag:
1. Se på HELE dagen time for time
2. Finn perioden med minst nedbør mellom soloppgang og solnedgang
3. Skill mellom regn (temp>1.5°C), sludd (0-1.5°C) og snø (<0°C)
4. Skriv "detalj" som én konkret setning som oppsummerer dagen
5. Skriv "kort" som en 4-6 ords komprimering av detalj

Eksempel på god detalj: "Rolig morgen med lett snø til kl 10, deretter kraftig vind 8–10 m/s fra 13."
Tilhørende kort: "Rolig morgen, vind fra 13"

Svar KUN med gyldig JSON-array (ingen markdown):
[{"dato":"YYYY-MM-DD","score":1-5,"kort":"4-6 ord","beste_tid":"HH:MM–HH:MM eller null","detalj":"1 konkret setning med timing"}]

Score: 5=sol+lite vind hele dagen, 4=bra, 3=variabelt/greit, 2=mye nedbør men pauser, 1=bli hjemme""".strip()


@sno_bp.get("/<stasjon_id>/api/skitur-ai")
def api_skitur_ai(stasjon_id: str):
    """Alle skidager analysert av AI i ett kall – cachet 2 timer."""
    import datetime as _dt
    from collections import defaultdict

    now_ts = time.time()
    cache_key = f"skitur_ai_{stasjon_id}"
    hit = _SKITUR_AI_CACHE.get(cache_key)
    if hit and hit["expires_at"] > now_ts:
        return jsonify(hit["payload"])

    if not ANTHROPIC_API_KEY:
        return jsonify({"ok": False, "dager": []})

    status_hit = _STATUS_CACHE.get(stasjon_id)
    if not status_hit:
        return jsonify({"ok": False, "dager": []})

    intervaller_raw = status_hit["payload"].get("intervaller", [])
    if not intervaller_raw:
        return jsonify({"ok": False, "dager": []})

    stasjon = _get_stasjon(stasjon_id)
    lat = stasjon["lat"] if stasjon else 60.0
    lon = stasjon["lon"] if stasjon else 10.0

    per_dag: dict = defaultdict(list)
    for iv in intervaller_raw:
        dato = (iv.get("start") or "")[:10]
        if dato:
            per_dag[dato].append(iv)

    dager_payload = []
    for dato in sorted(per_dag.keys())[:7]:
        sol_opp, sol_ned = _sol_tider(dato, lat, lon)
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
                    "ikon": iv.get("ver_ikon") or "",
                }
                for iv in ivs
            ],
        })

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 800,
                "system": _SKITUR_AI_PROMPT,
                "messages": [{"role": "user", "content": json.dumps(dager_payload, ensure_ascii=False)}],
            },
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        ai_dager = json.loads(text)

        for dag in ai_dager:
            dato = dag.get("dato", "")
            if dato:
                dt = _dt.date.fromisoformat(dato)
                dag["ukedag"] = ["mandag","tirsdag","onsdag","torsdag","fredag","lørdag","søndag"][dt.weekday()]
                dag["soloppgang"], dag["solnedgang"] = _sol_tider(dato, lat, lon)

        payload = {"ok": True, "dager": ai_dager}
        _SKITUR_AI_CACHE[cache_key] = {"expires_at": now_ts + _SKITUR_AI_CACHE_TTL, "payload": payload}
        return jsonify(payload)

    except Exception:
        traceback.print_exc()
        return jsonify({"ok": False, "dager": []})


# ── HTML-sider ────────────────────────────────────────────────────────────────

@sno_bp.get("/")
def oversikt():
    """Oversiktsside med alle stasjoner."""
    html = _OVERSIKT_HTML
    return Response(html, mimetype="text/html; charset=utf-8")


@sno_bp.get("/<stasjon_id>")
@sno_bp.get("/<stasjon_id>/")
def stasjon_side(stasjon_id: str):
    stasjon = _get_stasjon(stasjon_id)
    if not stasjon:
        return f"<h1>Ukjent stasjon: {stasjon_id}</h1>", 404
    html = _STASJON_HTML.replace("__STASJON_ID__", stasjon_id)\
                        .replace("__STASJON_NAVN__", stasjon["navn"])\
                        .replace("__STASJON_FYLKE__", stasjon["fylke"])\
                        .replace("__LAT__", str(stasjon["lat"]))\
                        .replace("__LON__", str(stasjon["lon"]))
    return Response(html, mimetype="text/html; charset=utf-8")


# ── HTML-maler ────────────────────────────────────────────────────────────────

_OVERSIKT_HTML = """<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Snødashboard – finn de beste skiforholdene i Norge</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
/* ── Reset + base ── */
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#0a0f1e;color:#e2e8f0;font-family:system-ui,-apple-system,sans-serif;line-height:1.5;}
a{color:inherit;text-decoration:none;}

/* ── Tokens ── */
:root{
  --bg:#0a0f1e;--surface:#111827;--surface2:#1a2235;--border:#1e293b;
  --blue:#3b82f6;--blue-d:#1d4ed8;--blue-glow:rgba(59,130,246,.18);
  --green:#22c55e;--green-d:#15803d;--green-glow:rgba(34,197,94,.15);
  --muted:#64748b;--hint:#94a3b8;--text:#e2e8f0;
  --radius:14px;--shadow:0 4px 24px rgba(0,0,0,.4);
}

/* ── Nav ── */
.topnav{padding:14px 24px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);}
.topnav-brand{font-size:13px;color:var(--muted);}
.topnav-brand a{color:var(--hint);}
.topnav-brand span{color:var(--blue);font-weight:700;}

/* ── Hero ── */
.hero{
  padding:56px 24px 48px;
  text-align:center;
  background:radial-gradient(ellipse 80% 60% at 50% 0%, rgba(59,130,246,.13) 0%, transparent 70%);
  border-bottom:1px solid var(--border);
}
.hero-badge{
  display:inline-flex;align-items:center;gap:6px;
  background:rgba(59,130,246,.12);border:1px solid rgba(59,130,246,.25);
  border-radius:20px;padding:4px 14px;font-size:12px;font-weight:600;
  color:var(--blue);margin-bottom:20px;letter-spacing:.04em;text-transform:uppercase;
}
.hero h1{font-size:clamp(28px,5vw,48px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:14px;}
.hero h1 em{font-style:normal;color:var(--blue);}
.hero-sub{font-size:16px;color:var(--muted);max-width:520px;margin:0 auto 32px;}
.hero-actions{display:flex;flex-wrap:wrap;gap:12px;justify-content:center;}
.btn-primary{
  display:inline-flex;align-items:center;gap:8px;
  padding:12px 24px;background:var(--blue);color:#fff;
  border:none;border-radius:10px;font-size:15px;font-weight:700;
  cursor:pointer;transition:background .15s,box-shadow .15s;
  box-shadow:0 0 0 0 var(--blue-glow);
}
.btn-primary:hover{background:#2563eb;box-shadow:0 0 20px 4px var(--blue-glow);}
.btn-secondary{
  display:inline-flex;align-items:center;gap:8px;
  padding:12px 22px;background:transparent;color:var(--hint);
  border:1px solid var(--border);border-radius:10px;font-size:15px;font-weight:600;
  cursor:pointer;transition:border-color .15s,color .15s;
}
.btn-secondary:hover{border-color:var(--blue);color:var(--blue);}

/* ── Stats-strip ── */
.stats-strip{
  display:flex;justify-content:center;gap:0;
  border-bottom:1px solid var(--border);
}
.stat{
  padding:18px 32px;text-align:center;border-right:1px solid var(--border);
}
.stat:last-child{border-right:none;}
.stat-val{font-size:22px;font-weight:800;color:var(--blue);}
.stat-lbl{font-size:11px;color:var(--muted);margin-top:2px;text-transform:uppercase;letter-spacing:.05em;}

/* ── Main layout ── */
.main{max-width:1100px;margin:0 auto;padding:32px 20px 60px;}

/* ── Section headers ── */
.seksjon-header{display:flex;align-items:center;gap:10px;margin-bottom:16px;}
.seksjon-header h2{font-size:16px;font-weight:700;}
.seksjon-header .pill{
  font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
  padding:3px 9px;border-radius:20px;background:rgba(59,130,246,.12);color:var(--blue);
}
.seksjon-header .pill.green{background:rgba(34,197,94,.12);color:var(--green);}

/* ── Topplister grid ── */
.topp-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:32px;}
@media(max-width:640px){.topp-grid{grid-template-columns:1fr;}}

.topp-kort{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;}
.topp-kort-header{padding:14px 18px 10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;}
.topp-kort-header .ikon{font-size:18px;}
.topp-kort-header h3{font-size:13px;font-weight:700;flex:1;}
.topp-kort-header .oppdatert{font-size:11px;color:var(--muted);}

.topp-rad{
  display:flex;align-items:center;gap:10px;
  padding:10px 18px;border-bottom:1px solid rgba(255,255,255,.04);
  cursor:pointer;transition:background .12s;
}
.topp-rad:last-child{border-bottom:none;}
.topp-rad:hover{background:rgba(255,255,255,.04);}
.topp-nr{font-size:12px;font-weight:800;color:var(--muted);width:18px;text-align:center;flex-shrink:0;}
.topp-nr.gull{color:#f59e0b;}
.topp-nr.solv{color:#94a3b8;}
.topp-nr.bronse{color:#b45309;}
.topp-info{flex:1;min-width:0;}
.topp-navn{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.topp-fylke{font-size:11px;color:var(--muted);}
.topp-sno{
  font-size:14px;font-weight:800;flex-shrink:0;
  padding:3px 10px;border-radius:8px;
}
.topp-sno.stor{background:rgba(59,130,246,.15);color:#60a5fa;}
.topp-sno.middels{background:rgba(34,197,94,.12);color:var(--green);}
.topp-sno.liten{background:rgba(148,163,184,.1);color:var(--hint);}
.topp-pil{font-size:10px;color:var(--muted);flex-shrink:0;}
.topp-laster{padding:20px;text-align:center;color:var(--muted);font-size:13px;}

/* ── Søk-seksjon ── */
.sok-seksjon{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;margin-bottom:16px;}
.sok-grid{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;}
.f-group{display:flex;flex-direction:column;gap:5px;flex:1;min-width:130px;}
.f-label{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;}
input[type=text],input[type=number],select{
  padding:9px 12px;border:1px solid var(--border);border-radius:8px;
  font-size:13px;outline:none;background:var(--bg);color:var(--text);width:100%;
}
input:focus,select:focus{border-color:var(--blue);}

/* ── Kart ── */
#kart-seksjon{display:none;}
#map{height:520px;border-radius:var(--radius);border:1px solid var(--border);box-shadow:var(--shadow);background:#111827;}

/* ── Stasjonsliste ── */
#liste-seksjon{display:none;margin-top:20px;}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px;}
.card{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
  padding:14px 16px;cursor:pointer;transition:border-color .15s,background .15s;
}
.card:hover{border-color:var(--blue);background:var(--surface2);}
.card-navn{font-size:13px;font-weight:700;}
.card-info{font-size:11px;color:var(--muted);margin-top:3px;}
.card-dist{font-size:11px;color:var(--blue);font-weight:600;margin-top:4px;}
.card-sno{
  display:inline-block;padding:2px 9px;border-radius:20px;
  font-size:11px;font-weight:700;margin-top:6px;
  background:rgba(34,197,94,.12);color:var(--green);
}
.card-sno.ingen{background:rgba(148,163,184,.1);color:var(--muted);}

/* ── Popup ── */
.naer-kort{
  background:#1a2235;border:1px solid #1e293b;border-radius:12px;
  padding:12px 16px;cursor:pointer;transition:border-color .15s;min-width:160px;flex:1;max-width:220px;
}
.naer-kort:hover{border-color:#3b82f6;}
.naer-navn{font-size:13px;font-weight:700;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.naer-dist{font-size:11px;color:#3b82f6;font-weight:600;margin-top:2px;}
.naer-sno{font-size:18px;font-weight:800;margin-top:6px;color:#fff;}
.naer-fylke{font-size:10px;color:#64748b;margin-top:2px;}
.leaflet-popup-content{font-family:system-ui,sans-serif;font-size:13px;min-width:160px;}
.popup-navn{font-weight:700;font-size:14px;margin-bottom:4px;color:#0f172a;}
.popup-info{color:#64748b;font-size:12px;margin-bottom:8px;}
.popup-btn{
  display:block;text-align:center;padding:7px 0;background:#1d4ed8;color:#fff;
  border-radius:8px;font-size:13px;font-weight:600;
}

/* ── Hjelp ── */
.hjelp-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-top:32px;}
.hjelp-kort{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;}
.hjelp-ikon{font-size:22px;margin-bottom:8px;}
.hjelp-tittel{font-size:13px;font-weight:700;margin-bottom:4px;}
.hjelp-tekst{font-size:12px;color:var(--muted);line-height:1.6;}

/* ── Status ── */
#sno-status{display:none;text-align:center;font-size:12px;color:var(--muted);padding:6px 0;}
#sno-hint{text-align:center;padding:10px 0;font-size:12px;color:var(--muted);}

/* ── Spinner ── */
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;}
@keyframes spin{to{transform:rotate(360deg)}}

/* ── Responsive ── */
@media(max-width:520px){
  .stats-strip{flex-wrap:wrap;}
  .stat{flex:1;min-width:120px;}
  .hero{padding:40px 16px 32px;}
  #map{height:320px;}
}
</style>
</head>
<body>

<!-- Nav -->
<div class="topnav">
  <div class="topnav-brand"><a href="/">prisanalyse.no</a> › <span>Snødashboard</span></div>
  <div style="font-size:12px;color:var(--muted);" id="nav-dato"></div>
</div>

<!-- Hero -->
<div class="hero">
  <div class="hero-badge">❄️ Norge rundt</div>
  <h1>Når skal du på <em>skitur</em>?<br>Vi hjelper deg finne svaret.</h1>
  <div class="hero-sub">Sanntidsdata fra 442 norske fjellstasjoner. Finn stedene med mest snø — og best prognose.</div>
  <div class="hero-actions">
    <button class="btn-primary" id="pos-btn" onclick="hentPosisjon()">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3"/></svg>
      Finn snø nær meg
    </button>
    <button class="btn-secondary" onclick="visKart()">
      🗺 Åpne kartvisning
    </button>
  </div>
  <div style="font-size:11px;color:var(--muted);margin-top:12px;" id="pos-status"></div>
</div>

<!-- Nærmeste stasjoner (vises etter Finn meg) -->
<div id="naer-meg-seksjon" style="display:none;background:#111827;border-top:1px solid #1e293b;padding:20px 0;">
  <div style="max-width:1100px;margin:0 auto;padding:0 20px;">
    <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#64748b;margin-bottom:12px;">📍 Nærmeste stasjoner</div>
    <div id="naer-meg-grid" style="display:flex;gap:10px;flex-wrap:wrap;"></div>
  </div>
</div>

<!-- Stats -->
<div class="stats-strip">
  <div class="stat"><div class="stat-val">442</div><div class="stat-lbl">Stasjoner</div></div>
  <div class="stat"><div class="stat-val" id="stat-sno">–</div><div class="stat-lbl">Stasjoner med snø</div></div>
  <div class="stat"><div class="stat-val" id="stat-max">–</div><div class="stat-lbl">Maks snødybde (cm)</div></div>
  <div class="stat"><div class="stat-val" id="stat-okt">–</div><div class="stat-lbl">Mest økning (2d)</div></div>
</div>

<!-- Main -->
<div class="main">

  <!-- Topplister -->
  <div class="topp-grid">

    <div class="topp-kort">
      <div class="topp-kort-header">
        <span class="ikon">🏆</span>
        <h3>Mest snø akkurat nå</h3>
        <span class="oppdatert" id="oppdatert-tid"></span>
      </div>
      <div id="topp-mest" class="topp-laster"><span class="spinner"></span> Laster…</div>
    </div>

    <div class="topp-kort">
      <div class="topp-kort-header">
        <span class="ikon">📈</span>
        <h3>Mest økning siste 2 døgn</h3>
        <span class="pill green">48t · ≥30cm</span>
      </div>
      <div id="topp-okt" class="topp-laster"><span class="spinner"></span> Laster…</div>
    </div>

  </div>

  <!-- Søk + kart -->
  <div class="seksjon-header">
    <h2>Søk og utforsk</h2>
    <span class="pill">Kart</span>
  </div>

  <div class="sok-seksjon">
    <div class="sok-grid">
      <div class="f-group" style="flex:2;min-width:180px;">
        <span class="f-label">Søk etter stasjon eller sted</span>
        <input type="text" id="search" placeholder="f.eks. Kvamskogen, Geilo, Troms…" oninput="sokInput(this.value)"/>
      </div>
      <div class="f-group">
        <span class="f-label">Maks avstand (km)</span>
        <input type="number" id="f-dist" placeholder="Alle" min="1" max="2000" oninput="oppdaterVisning()"/>
      </div>
      <div class="f-group">
        <span class="f-label">Min snødybde (cm)</span>
        <input type="number" id="f-minsno" placeholder="0" min="0" max="500" oninput="oppdaterVisning()"/>
      </div>
      <div class="f-group">
        <span class="f-label">Snøutvikling</span>
        <select id="f-stiger" onchange="oppdaterVisning()">
          <option value="">Alle</option>
          <option value="ja">Kun stigende ↑</option>
        </select>
      </div>
    </div>
  </div>

  <!-- Kart (skjult til åpnet) -->
  <div id="kart-seksjon">
    <div id="sno-hint">🔍 Zoom inn (eller klikk <b>Finn snø nær meg</b>) for å hente snødybde</div>
    <div id="sno-status"></div>
    <div id="map"></div>
    <div style="text-align:right;margin-top:6px;">
      <button onclick="skjulKart()" style="font-size:12px;color:var(--muted);background:none;border:none;cursor:pointer;">Skjul kart ▴</button>
    </div>
  </div>

  <!-- Stasjonsliste -->
  <div id="liste-seksjon">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
      <span style="font-size:13px;font-weight:700;">Stasjoner i området <span id="count" style="font-weight:400;color:var(--muted);font-size:12px;"></span></span>
      <button onclick="skjulListe()" style="font-size:12px;color:var(--muted);background:none;border:none;cursor:pointer;">Skjul ▴</button>
    </div>
    <div class="grid" id="grid"></div>
  </div>

  <!-- Hjelp-seksjon -->
  <div class="hjelp-grid">
    <div class="hjelp-kort">
      <div class="hjelp-ikon">📍</div>
      <div class="hjelp-tittel">Finn snø nær deg</div>
      <div class="hjelp-tekst">Klikk "Finn snø nær meg" og appen henter snødybde for alle stasjoner i nærheten. Sorter etter avstand.</div>
    </div>
    <div class="hjelp-kort">
      <div class="hjelp-ikon">🗺</div>
      <div class="hjelp-tittel">Utforsk kartet</div>
      <div class="hjelp-tekst">Zoom inn på kartet for å se snødybde per stasjon. Størrelse og farge viser mengden. Klikk direkte for full prognose.</div>
    </div>
    <div class="hjelp-kort">
      <div class="hjelp-ikon">⛷️</div>
      <div class="hjelp-tittel">Planlegg skituren</div>
      <div class="hjelp-tekst">Stasjonssiden viser 8-dagers prognose, skituranbefaling dag for dag, og observert historikk fra Frost.</div>
    </div>
    <div class="hjelp-kort">
      <div class="hjelp-ikon">📈</div>
      <div class="hjelp-tittel">Stigende snø</div>
      <div class="hjelp-tekst">Bruk filteret "Kun stigende" for å finne steder der snødybden øker — ideelt for helgeplanlegging.</div>
    </div>
  </div>

</div><!-- /main -->

<script>
// ═══════════════════════════════════════════════════════════════
//  State
// ═══════════════════════════════════════════════════════════════
let alleStasjoner = [];
let userPos       = null;
let kart          = null;
let markerLag     = null;
let userMarker    = null;
let kartInit      = false;
let snøCache      = {};
let zoomTimer     = null;
let sisteHentetBbox = null;

// ── Dato i nav ────────────────────────────────────────────────
(function(){
  const el = document.getElementById('nav-dato');
  if(el){
    const n = new Date();
    el.textContent = n.toLocaleDateString('no-NO',{weekday:'long',day:'numeric',month:'long'});
  }
})();

// ═══════════════════════════════════════════════════════════════
//  Kart
// ═══════════════════════════════════════════════════════════════
function initKart() {
  if (kartInit) return;
  kartInit = true;
  kart = L.map('map', {center:[64.5,14.5], zoom:5, zoomControl:true, attributionControl:false});
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {maxZoom:18}).addTo(kart);
  markerLag = L.layerGroup().addTo(kart);
  kart.on('moveend zoomend', () => {
    clearTimeout(zoomTimer);
    zoomTimer = setTimeout(sjekkOgHentSnø, 600);
  });
}

function visKart() {
  const sek = document.getElementById('kart-seksjon');
  sek.style.display = 'block';
  initKart();
  setTimeout(() => kart.invalidateSize(), 50);
  sek.scrollIntoView({behavior:'smooth', block:'start'});
}

function skjulKart() {
  document.getElementById('kart-seksjon').style.display = 'none';
}

function skjulListe() {
  document.getElementById('liste-seksjon').style.display = 'none';
}

// ═══════════════════════════════════════════════════════════════
//  Stasjonsliste (lynrask, kun metadata)
// ═══════════════════════════════════════════════════════════════
async function lastStasjoner() {
  try {
    const r = await fetch('/sn%C3%B8/api/stasjoner');
    const d = await r.json();
    alleStasjoner = (d.stasjoner || []).map(s => ({...s, sno:null}));
  } catch(e) { console.error('Feil ved lasting av stasjoner', e); }
}

// ═══════════════════════════════════════════════════════════════
//  Topplister  (/api/snodybder for et stort bbox)
// ═══════════════════════════════════════════════════════════════
async function lastTopplister() {
  try {
    // Steg 1: Hent dagens snødybde fra server-cache (lynrask)
    const r = await fetch('/sn%C3%B8/api/norge_sno');
    const d = await r.json();
    if (!d.ok) return;

    Object.assign(snøCache, d.stasjoner);
    alleStasjoner.forEach(s => {
      if (snøCache[s.id] !== undefined) s.sno = snøCache[s.id];
    });

    // Statistikk-strip
    const medSno = alleStasjoner.filter(s => s.sno != null && s.sno > 0);
    const maks   = medSno.reduce((m,s) => Math.max(m, s.sno), 0);
    document.getElementById('stat-sno').textContent = medSno.length;
    document.getElementById('stat-max').textContent = maks > 0 ? Math.round(maks) : '–';

    // Topp 8 mest snø – vises umiddelbart
    const sortert = [...medSno].sort((a,b) => b.sno - a.sno).slice(0,8);
    document.getElementById('topp-mest').innerHTML = sortert.length
      ? sortert.map((s,i) => toppRad(s, i, s.sno, 'cm')).join('')
      : '<div class="topp-laster">Ingen data</div>';

    // Tidspunkt + alder på cache
    const now = new Date();
    const alder = d.alder_min != null ? ` · ${d.alder_min} min siden` : '';
    document.getElementById('oppdatert-tid').textContent =
      'kl. ' + String(now.getHours()).padStart(2,'0') + ':' + String(now.getMinutes()).padStart(2,'0') + alder;

    // Steg 2: Hent endring asynkront (kun for stasjoner med ≥ 30 cm)
    lastEndring(medSno);

  } catch(e) { console.error('Topplister feil:', e); }
}

async function lastEndring(medSno) {
  try {
    // Filtrer til stasjoner med ≥ 30 cm – typisk 50-100 stk
    const kandidater = medSno.filter(s => s.sno >= 30);
    if (!kandidater.length) {
      document.getElementById('topp-okt').innerHTML =
        '<div class="topp-laster" style="padding:16px 18px;">Ingen stasjoner med ≥ 30 cm akkurat nå</div>';
      return;
    }

    document.getElementById('topp-okt').innerHTML =
      '<div class="topp-laster"><span class="spinner"></span> Henter endring (2 døgn)…</div>';

    const ids = kandidater.map(s => s.id).join(',');
    const r2  = await fetch('/sn%C3%B8/api/sno_endring?ids=' + ids);
    const d2  = await r2.json();
    if (!d2.ok) return;

    // Bygg opp endring-map: stasjonId → diff
    const endringMap = {};
    for (const [sid, e] of Object.entries(d2.endringer)) {
      endringMap[sid] = e.diff;
    }

    // Sorter etter størst positiv endring, topp 8
    const medOkning = kandidater
      .filter(s => endringMap[s.id] != null && endringMap[s.id] > 0)
      .map(s => ({...s, diff: endringMap[s.id]}))
      .sort((a,b) => b.diff - a.diff)
      .slice(0, 8);

    // Oppdater stat-okt
    document.getElementById('stat-okt').textContent = medOkning.length > 0
      ? '+' + Math.round(medOkning[0].diff) + ' cm'
      : '–';

    document.getElementById('topp-okt').innerHTML = medOkning.length
      ? medOkning.map((s,i) => toppRadEndring(s, i)).join('')
      : '<div class="topp-laster" style="padding:16px 18px;color:var(--muted);font-size:12px;">Ingen stasjoner med økning siste 2 døgn</div>';

  } catch(e) {
    console.error('Endring feil:', e);
    document.getElementById('topp-okt').innerHTML =
      '<div class="topp-laster" style="padding:16px 18px;color:var(--muted);font-size:12px;">Kunne ikke hente endringsdata</div>';
  }
}

function toppRadEndring(s, i) {
  const nrKlass  = i === 0 ? 'gull' : i === 1 ? 'solv' : i === 2 ? 'bronse' : '';
  const snoKlass = s.diff >= 20 ? 'stor' : s.diff >= 5 ? 'middels' : 'liten';
  const kortNavn = (s.navn || '').split(' – ').pop();
  return `<div class="topp-rad" onclick="window.location='/sn%C3%B8/${s.id}'">
    <div class="topp-nr ${nrKlass}">${i+1}</div>
    <div class="topp-info">
      <div class="topp-navn">${kortNavn}</div>
      <div class="topp-fylke">${s.fylke || ''} · ${Math.round(s.sno)} cm nå</div>
    </div>
    <div class="topp-sno ${snoKlass}">+${Math.round(s.diff)} cm</div>
    <div class="topp-pil">›</div>
  </div>`;
}

function toppRad(s, i, verdi, enhet) {
  const nrKlass = i === 0 ? 'gull' : i === 1 ? 'solv' : i === 2 ? 'bronse' : '';
  const snoKlass = verdi >= 60 ? 'stor' : verdi >= 20 ? 'middels' : 'liten';
  const kortNavn = (s.navn || '').split(' – ').pop();
  return `<div class="topp-rad" onclick="window.location='/sn%C3%B8/${s.id}'">
    <div class="topp-nr ${nrKlass}">${i+1}</div>
    <div class="topp-info">
      <div class="topp-navn">${kortNavn}</div>
      <div class="topp-fylke">${s.fylke || ''}</div>
    </div>
    <div class="topp-sno ${snoKlass}">${Math.round(verdi)} ${enhet}</div>
    <div class="topp-pil">›</div>
  </div>`;
}

// ═══════════════════════════════════════════════════════════════
//  Snødybde for synlig kartutsnitt
// ═══════════════════════════════════════════════════════════════
async function sjekkOgHentSnø() {
  if (!kart) return;
  const zoom = kart.getZoom();
  const hint = document.getElementById('sno-hint');
  if (zoom < 7) { if(hint) hint.style.display='block'; return; }
  if(hint) hint.style.display = 'none';

  const b    = kart.getBounds();
  const bbox = [b.getSouth().toFixed(4),b.getWest().toFixed(4),b.getNorth().toFixed(4),b.getEast().toFixed(4)].join(',');
  if (bbox === sisteHentetBbox) return;
  sisteHentetBbox = bbox;

  visSnøStatus('Henter snødybde…', true);
  try {
    const r = await fetch('/sn%C3%B8/api/snodybder?bbox=' + bbox);
    const d = await r.json();
    if (d.ok) {
      Object.assign(snøCache, d.stasjoner);
      alleStasjoner.forEach(s => { if (snøCache[s.id] !== undefined) s.sno = snøCache[s.id]; });
      visSnøStatus('Oppdatert – ' + d.antall + ' stasjoner', false);
      oppdaterVisning();
    }
  } catch(e) { visSnøStatus('Feil ved henting', false); }
}

async function hentSnøForPosisjon(lat, lon, km) {
  visSnøStatus('Henter snødybde…', true);
  sisteHentetBbox = null;
  try {
    const r = await fetch('/sn%C3%B8/api/snodybder?lat='+lat+'&lon='+lon+'&km='+km);
    const d = await r.json();
    if (d.ok) {
      Object.assign(snøCache, d.stasjoner);
      alleStasjoner.forEach(s => { if (snøCache[s.id] !== undefined) s.sno = snøCache[s.id]; });
      visSnøStatus('Oppdatert – ' + d.antall + ' stasjoner', false);
      oppdaterVisning();
    }
  } catch(e) { visSnøStatus('Feil', false); }
}

function visSnøStatus(tekst, laster) {
  const el = document.getElementById('sno-status');
  if (!el) return;
  el.innerHTML = laster ? '<span class="spinner"></span> '+tekst : '✓ '+tekst;
  el.style.display = 'block';
  if (!laster) setTimeout(() => { el.style.display='none'; }, 4000);
}

// ═══════════════════════════════════════════════════════════════
//  Geolocation
// ═══════════════════════════════════════════════════════════════
function hentPosisjon() {
  const btn    = document.getElementById('pos-btn');
  const status = document.getElementById('pos-status');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner" style="border-top-color:#fff;border-color:rgba(255,255,255,.3)"></span> Finner posisjon…';
  if (!navigator.geolocation) {
    status.textContent = 'Ikke støttet av nettleseren.';
    btn.disabled = false;
    btn.innerHTML = '📍 Finn snø nær meg';
    return;
  }
  navigator.geolocation.getCurrentPosition(
    pos => {
      userPos = {lat: pos.coords.latitude, lon: pos.coords.longitude};
      btn.disabled = false;
      btn.innerHTML = '✓ Posisjon funnet';
      status.textContent = userPos.lat.toFixed(3)+'°N, '+userPos.lon.toFixed(3)+'°E';

      visKart();
      setTimeout(() => {
        if (userMarker) userMarker.remove();
        const myIcon = L.divIcon({
          html:'<div style="width:16px;height:16px;background:#3b82f6;border:3px solid #fff;border-radius:50%;box-shadow:0 0 0 4px rgba(59,130,246,.3)"></div>',
          iconSize:[16,16], iconAnchor:[8,8], className:''
        });
        userMarker = L.marker([userPos.lat, userPos.lon], {icon:myIcon}).addTo(kart);
        userMarker.bindPopup('<b>Din posisjon</b>').openPopup();
        kart.setView([userPos.lat, userPos.lon], 8, {animate:true});
        const km = parseFloat(document.getElementById('f-dist').value) || 100;
        hentSnøForPosisjon(userPos.lat, userPos.lon, km).then(() => {
          visNaerMeg(userPos.lat, userPos.lon);
        });
        oppdaterVisning();
      }, 100);
    },
    () => {
      btn.disabled = false;
      btn.innerHTML = '📍 Finn snø nær meg';
      status.textContent = 'Tilgang nektet – tillat posisjon i nettleseren.';
    },
    {timeout:10000, maximumAge:60000}
  );
}

// ═══════════════════════════════════════════════════════════════
//  Markør-ikoner
// ═══════════════════════════════════════════════════════════════
function snøFargeOgStr(sno) {
  if (sno === null) return {farge:'#334155', str:22, ring:'rgba(51,65,85,0.2)'};
  if (sno >= 100)   return {farge:'#1e3a5f', str:56, ring:'rgba(30,58,95,0.25)'};
  if (sno >= 60)    return {farge:'#1d4ed8', str:50, ring:'rgba(29,78,216,0.22)'};
  if (sno >= 30)    return {farge:'#0284c7', str:44, ring:'rgba(2,132,199,0.2)'};
  if (sno >= 10)    return {farge:'#15803d', str:38, ring:'rgba(21,128,61,0.2)'};
  if (sno > 0)      return {farge:'#65a30d', str:32, ring:'rgba(101,163,13,0.18)'};
  return                   {farge:'#475569', str:26, ring:'rgba(71,85,105,0.15)'};
}

function lagMarkerIkon(s) {
  const {farge, str, ring} = snøFargeOgStr(s.sno);
  const half = str / 2;
  if (s.sno === null) {
    return L.divIcon({
      html:`<div style="width:${str}px;height:${str}px;background:${farge};border:2px solid rgba(255,255,255,.2);border-radius:50%;box-shadow:0 1px 4px rgba(0,0,0,.4);"></div>`,
      className:'', iconSize:[str,str], iconAnchor:[half,half]
    });
  }
  const kortNavn = (s.kort || s.navn || '').split(' – ').pop().split(' - ').pop();
  const navnVis  = kortNavn.length > 12 ? kortNavn.slice(0,11)+'…' : kortNavn;
  const fs       = str >= 50 ? 11 : str >= 40 ? 10 : 9;
  return L.divIcon({
    html:`<div style="
      width:${str}px;height:${str}px;background:${farge};
      border:2.5px solid rgba(255,255,255,.85);border-radius:50%;
      box-shadow:0 2px 10px rgba(0,0,0,.5),0 0 0 5px ${ring};
      display:flex;flex-direction:column;align-items:center;justify-content:center;
      gap:1px;padding:3px;cursor:pointer;color:#fff;text-align:center;
    ">
      <div style="font-size:${fs+1}px;font-weight:800;line-height:1;">${Math.round(s.sno)}cm</div>
      <div style="font-size:${fs-1}px;font-weight:600;line-height:1.1;opacity:.88;">${navnVis}</div>
    </div>`,
    className:'', iconSize:[str,str], iconAnchor:[half,half]
  });
}

// ═══════════════════════════════════════════════════════════════
//  Filtrering + visning
// ═══════════════════════════════════════════════════════════════
function haversine(lat1,lon1,lat2,lon2) {
  const R=6371,d2r=Math.PI/180;
  const dLat=(lat2-lat1)*d2r,dLon=(lon2-lon1)*d2r;
  const a=Math.sin(dLat/2)**2+Math.cos(lat1*d2r)*Math.cos(lat2*d2r)*Math.sin(dLon/2)**2;
  return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}

function filtrerOgSorter() {
  const q      = document.getElementById('search').value.toLowerCase();
  const maxD   = parseFloat(document.getElementById('f-dist').value) || Infinity;
  const minSno = parseFloat(document.getElementById('f-minsno').value) || 0;
  const stiger = document.getElementById('f-stiger').value === 'ja';
  return alleStasjoner
    .map(s => ({...s, dist: userPos ? haversine(userPos.lat,userPos.lon,s.lat,s.lon) : null}))
    .filter(s => {
      if (q && !(s.navn+s.fylke+s.id).toLowerCase().includes(q)) return false;
      if (s.dist !== null && s.dist > maxD) return false;
      if (minSno > 0 && (s.sno == null || s.sno < minSno)) return false;
      if (stiger && !s.stiger) return false;
      return true;
    })
    .sort((a,b) => a.dist !== null && b.dist !== null ? a.dist-b.dist : a.navn.localeCompare(b.navn,'no'));
}

function oppdaterVisning() {
  const liste = filtrerOgSorter();

  // Kart
  if (kart && markerLag) {
    markerLag.clearLayers();
    liste.slice(0,300).forEach(s => {
      const m = L.marker([s.lat,s.lon], {icon:lagMarkerIkon(s)}).addTo(markerLag);
      m.on('click', () => { window.location.href='/sn%C3%B8/'+s.id; });
    });
  }

  // Liste (kun om vi har snødata)
  const harSnø = liste.some(s => s.sno !== null);
  const sek    = document.getElementById('liste-seksjon');
  if (sek) sek.style.display = harSnø ? 'block' : 'none';

  const g = document.getElementById('grid');
  const c = document.getElementById('count');
  const medData = liste.filter(s => s.sno !== null);
  if (c) c.textContent = '– ' + medData.length + ' stasjoner';
  if (g) g.innerHTML = medData.slice(0,80).map(s => {
    const snoHtml = `<span class="card-sno${s.sno<=0?' ingen':''}">${s.sno>30?'❄':s.sno>5?'🌨':'·'} ${Math.round(s.sno)} cm</span>`;
    const distHtml = s.dist!=null ? `<div class="card-dist">📍 ${Math.round(s.dist)} km</div>` : '';
    return `<a class="card" href="/sn%C3%B8/${s.id}">
      <div class="card-navn">${s.navn}</div>
      <div class="card-info">${s.fylke||''}</div>
      ${distHtml}${snoHtml}
    </a>`;
  }).join('');
}

// ═══════════════════════════════════════════════════════════════
//  Søk → trigger kartvisning
// ═══════════════════════════════════════════════════════════════
function sokInput(q) {
  oppdaterVisning();
  if (q.length >= 2) {
    visKart();
    // Finn første treff og sentrér kartet
    const treff = alleStasjoner.find(s =>
      (s.navn+s.fylke+s.id).toLowerCase().includes(q.toLowerCase())
    );
    if (treff && kart) {
      kart.setView([treff.lat, treff.lon], 9, {animate:true});
    }
  }
}

// ═══════════════════════════════════════════════════════════════
//  Nærmeste stasjoner under hero
// ═══════════════════════════════════════════════════════════════
function visNaerMeg(lat, lon) {
  function hav(s) {
    const R=6371,d2r=Math.PI/180;
    const dLat=(s.lat-lat)*d2r, dLon=(s.lon-lon)*d2r;
    const a=Math.sin(dLat/2)**2+Math.cos(lat*d2r)*Math.cos(s.lat*d2r)*Math.sin(dLon/2)**2;
    return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
  }
  const med = alleStasjoner
    .filter(s => s.sno !== null)
    .map(s => ({...s, dist: hav(s)}))
    .sort((a,b) => a.dist - b.dist)
    .slice(0, 5);

  const sek = document.getElementById('naer-meg-seksjon');
  const grid = document.getElementById('naer-meg-grid');
  if (!sek || !grid) return;

  if (!med.length) { sek.style.display='none'; return; }

  grid.innerHTML = med.map(s => {
    const snoFarge = s.sno >= 60 ? '#60a5fa' : s.sno >= 20 ? '#22c55e' : '#94a3b8';
    const kortNavn = (s.navn||'').split(' – ').pop();
    return `<div class="naer-kort" onclick="window.location='/sn%C3%B8/${s.id}'">
      <div class="naer-navn">${kortNavn}</div>
      <div class="naer-fylke">${s.fylke||''}</div>
      <div class="naer-dist">📍 ${Math.round(s.dist)} km</div>
      <div class="naer-sno" style="color:${snoFarge}">❄ ${Math.round(s.sno)} cm</div>
    </div>`;
  }).join('');
  sek.style.display = 'block';
}

// ═══════════════════════════════════════════════════════════════
//  Boot
// ═══════════════════════════════════════════════════════════════
lastStasjoner().then(lastTopplister);
</script>
</body>
</html>"""



_STASJON_HTML = """<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>__STASJON_NAVN__ – Snødashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#f5f7fb;--surface:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;--hint:#94a3b8;--green:#15803d;--green-bg:#f0fdf4;--amber:#92400e;--amber-bg:#fffbeb;--red:#991b1b;--red-bg:#fef2f2;--blue:#1d4ed8;--radius:14px;--shadow:0 2px 12px rgba(15,23,42,.07);}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,sans-serif;line-height:1.55;}
a{color:var(--blue);text-decoration:none;}
.page{max-width:860px;margin:0 auto;padding:20px 16px 48px;}
.nav{font-size:13px;color:var(--muted);margin-bottom:18px;}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px 24px;box-shadow:var(--shadow);margin-bottom:14px;}
.section-label{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--hint);margin:22px 0 10px;}
.hero-top{display:flex;align-items:flex-start;gap:14px;}
.hero-icon{font-size:38px;line-height:1;flex-shrink:0;}
.hero-text{flex:1;}
.hero-verdict{font-size:20px;font-weight:600;line-height:1.3;margin-bottom:6px;}
.hero-detail{font-size:14px;color:var(--muted);line-height:1.6;}
.hero-detail-short{display:block;}
.hero-detail-full{display:none;margin-top:8px;}
.hero-detail-full.open{display:block;}
.les-mer{font-size:12px;color:var(--blue);cursor:pointer;border:none;background:none;padding:4px 0;text-decoration:underline;}
.badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:600;margin-top:10px;}
.badge-green{background:var(--green-bg);color:var(--green);}
.badge-amber{background:var(--amber-bg);color:var(--amber);}
.badge-red{background:var(--red-bg);color:var(--red);}
.sno-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;}
.sno-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;box-shadow:var(--shadow);}
.sno-label{font-size:11px;color:var(--hint);margin-bottom:4px;}
.sno-val{font-size:22px;font-weight:700;}
.sno-sub{font-size:11px;color:var(--muted);margin-top:3px;}
.forecast-strip{display:flex;gap:8px;overflow-x:auto;padding-bottom:4px;}
.fday{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;min-width:76px;text-align:center;flex-shrink:0;box-shadow:var(--shadow);}
.fday-name{font-size:10px;color:var(--hint);margin-bottom:4px;}
.fday-icon{font-size:20px;margin:2px 0;}
.fday-temp{font-size:12px;font-weight:600;}
.fday-snow{font-size:10px;color:#0284c7;margin-top:3px;min-height:14px;}
.chart-hours{display:flex;gap:6px;margin-bottom:8px;}
.chart-hours button{padding:4px 10px;border:1px solid #334155;border-radius:6px;font-size:12px;cursor:pointer;}
.ski-day{border-radius:14px;margin-bottom:10px;box-shadow:var(--shadow);overflow:hidden;}
.ski-day-header{display:flex;align-items:center;cursor:pointer;user-select:none;border-radius:14px;overflow:hidden;}
.ski-day-left{width:80px;min-height:72px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;flex-shrink:0;padding:10px 6px;}
.score-5{background:linear-gradient(135deg,#fbbf24,#f59e0b);}
.score-4{background:linear-gradient(135deg,#34d399,#10b981);}
.score-3{background:linear-gradient(135deg,#93c5fd,#60a5fa);}
.score-2{background:linear-gradient(135deg,#94a3b8,#64748b);}
.score-1{background:linear-gradient(135deg,#fca5a5,#ef4444);}
.ski-day-icon-big{font-size:28px;line-height:1;}
.ski-day-name-top{font-size:10px;font-weight:700;color:rgba(255,255,255,0.9);text-transform:uppercase;}
.ski-day-right{flex:1;background:var(--surface);padding:10px 14px;min-height:72px;display:flex;flex-direction:column;justify-content:center;gap:3px;}
.ski-day-right:hover{background:#f8fafc;}
.ski-day-title{font-size:14px;font-weight:600;color:var(--text);}
.ski-day-kort{font-size:12px;color:var(--muted);}
.ski-day-detail-text{font-size:13px;color:var(--muted);line-height:1.6;padding:8px 14px 12px 94px;}
.ski-beste-tid{font-size:11px;font-weight:600;color:var(--blue);}
.ski-arrow{font-size:11px;color:var(--hint);margin-left:auto;}
.ski-day-chart-section{background:#0f172a;border-radius:0 0 14px 14px;}
.ski-day-chart-wrap{padding:0 12px 12px;height:160px;position:relative;}
.ski-day-chart-msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#475569;font-size:12px;}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #e2e8f0;border-top-color:#64748b;border-radius:50%;animation:spin .7s linear infinite;}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="page">
  <div class="nav"><a href="/">prisanalyse.no</a> › <a href="/snø/">Snødashboard</a> › __STASJON_NAVN__</div>

  <div class="card" id="hero">
    <div class="hero-top">
      <div class="hero-icon">🏔️</div>
      <div class="hero-text">
        <div class="hero-verdict">__STASJON_NAVN__</div>
        <div class="hero-detail"><span class="hero-detail-short">Henter værstatus…</span></div>
        <span class="badge badge-amber">Laster…</span>
      </div>
    </div>
  </div>

  <div class="section-label">Værprognose – kommende timer</div>
  <div style="background:#0f172a;border-radius:var(--radius);padding:16px 18px;margin-bottom:14px;">
    <div class="chart-hours">
      <button class="active" onclick="setFcast(12,this)" style="background:#334155;color:#e2e8f0;">12t</button>
      <button onclick="setFcast(24,this)" style="background:#1e293b;color:#94a3b8;">24t</button>
      <button onclick="setFcast(48,this)" style="background:#1e293b;color:#94a3b8;">48t</button>
      <button onclick="setFcast(168,this)" style="background:#1e293b;color:#94a3b8;">7 dager</button>
    </div>
    <div id="fcast-icons" style="display:flex;overflow-x:auto;gap:0;margin-bottom:6px;min-height:28px;"></div>
    <div style="position:relative;height:200px;">
      <canvas id="fcast-chart"></canvas>
      <div id="fcast-msg" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#7b8db5;font-size:13px;"><span class="spinner"></span></div>
    </div>
  </div>

  <div class="section-label">Snøstatus</div>
  <div class="sno-grid" style="margin-bottom:14px;">
    <div class="sno-card"><div class="sno-label">Snødybde nå</div><div class="sno-val" id="m-dybde">–</div><div class="sno-sub" id="m-dybde-sub"></div></div>
    <div class="sno-card"><div class="sno-label">Temperatur nå</div><div class="sno-val" id="m-temp">–</div><div class="sno-sub" id="m-temp-sub"></div></div>
    <div class="sno-card"><div class="sno-label">Ny snø 48t</div><div class="sno-val" id="m-nysno">–</div></div>
  </div>

  <div class="section-label">Siste døgn – observert (Frost)</div>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;margin-bottom:14px;box-shadow:var(--shadow);">
    <div class="chart-hours">
      <button class="hist-active" onclick="setHours(12,this)" style="background:#334155;color:#e2e8f0;border-color:#334155;">12t</button>
      <button onclick="setHours(24,this)" style="background:var(--bg);color:var(--muted);">24t</button>
      <button onclick="setHours(48,this)" style="background:var(--bg);color:var(--muted);">48t</button>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:8px;font-size:11px;color:var(--muted);">
      <span>— Temp (°C)</span><span style="color:#ef4444">■ Nedbør varm</span><span style="color:#93c5fd">■ Nedbør kald</span><span>— Vind (m/s)</span>
    </div>
    <div style="position:relative;height:200px;">
      <canvas id="hist-chart"></canvas>
      <div id="hist-msg" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:13px;"><span class="spinner"></span></div>
    </div>
  </div>

  <div class="section-label">Når kan jeg gå på tur?</div>
  <div style="font-size:12px;color:var(--hint);margin-bottom:10px;">Klikk på en dag for timedetaljer</div>
  <div id="skitur-list"><div style="color:var(--hint);font-size:13px;padding:8px 0;"><span class="spinner"></span> Laster skidager…</div></div>

  <div class="section-label">Snøprognose – kommende dager</div>
  <div class="forecast-strip" id="forecast" style="margin-bottom:14px;"></div>

  <div style="font-size:11px;color:var(--hint);margin-top:20px;text-align:center;" id="footer"></div>
</div>

<script>
const STASJON_ID='__STASJON_ID__';
const DAYS=['søn','man','tir','ons','tor','fre','lør'];
const MONTHS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
let fcastData=[],fcastHours=12,fcastChart=null;

function fmtTemp(v){if(v==null)return'–';const n=parseFloat(v);return(n>0?'+':'')+n.toFixed(1)+'°C';}

function toggleDetail(btn){
  const full=document.getElementById('hero-full');
  const open=full.classList.toggle('open');
  btn.textContent=open?'Les mindre ▴':'Les mer ▾';
}

function init(){
  fetch(`/sn%C3%B8/${STASJON_ID}/api/status`)
    .then(r=>r.json())
    .then(d=>{
      renderStatus(d);
      const harData = d.sno && d.sno.dybde_cm != null;
      const harAI   = d.tolkning && d.tolkning.verdict;
      if(!harData){
        setTimeout(init, 3000);       // Ingen data ennå – poll raskt
      } else if(!harAI){
        setTimeout(init, 5000);       // Data OK men AI ikke klar – poll litt
      } else {
        setTimeout(init, 15*60*1000); // Alt OK – refresh hvert 15 min
      }
    })
    .catch(()=>setTimeout(init, 5000));
}

function renderStatus(d){
  try{
    const t=d.tolkning||{},s=d.sno||{};
    const bc={'green':'badge-green','amber':'badge-amber','red':'badge-red'}[t.badge_color]||'badge-amber';
    const fullDetail=(t.detail||'').replace(/\\n/g,'<br>');
    const firstPara=(t.detail||'').split('\\n').filter(x=>x.trim())[0]||'';
    const hasMore=(t.detail||'').split('\\n').filter(x=>x.trim()).length>1;
    const detailHtml=hasMore
      ?`<span class="hero-detail-short">${firstPara}</span>
        <span class="hero-detail-full" id="hero-full">${fullDetail}</span>
        <button class="les-mer" onclick="toggleDetail(this)">Les mer ▾</button>`
      :`<span class="hero-detail-short">${fullDetail||'Henter data…'}</span>`;
    document.getElementById('hero').innerHTML=`
      <div class="hero-top">
        <div class="hero-icon">${t.icon||'🏔️'}</div>
        <div class="hero-text">
          <div class="hero-verdict">${t.verdict||'__STASJON_NAVN__'}</div>
          <div class="hero-detail">${detailHtml}</div>
          <span class="badge ${bc}">${t.snow_quality||'Ukjent'} skiføre</span>
        </div>
      </div>`;

    if(s.dybde_cm!=null) document.getElementById('m-dybde').textContent=s.dybde_cm+' cm';
    if(s.ny_sno_48t_cm!=null) document.getElementById('m-dybde-sub').textContent='+'+s.ny_sno_48t_cm+' cm siste 48t';
    document.getElementById('m-temp').textContent=fmtTemp(s.temp_na_c);
    if(s.min_temp_c!=null) document.getElementById('m-temp-sub').textContent=`min ${fmtTemp(s.min_temp_c)} / maks ${fmtTemp(s.maks_temp_c)}`;
    if(s.ny_sno_48t_cm!=null) document.getElementById('m-nysno').textContent=s.ny_sno_48t_cm+' cm';

    if(d.daglig&&d.daglig.length){
      document.getElementById('forecast').innerHTML=d.daglig.map(dag=>{
        const dt=new Date(dag.dato+'T12:00:00');
        const navn=DAYS[dt.getDay()]+' '+dt.getDate()+'.'+MONTHS[dt.getMonth()];
        return`<div class="fday">
          <div class="fday-name">${navn}</div>
          <div class="fday-icon">${dag.ver_ikon||'–'}</div>
          <div class="fday-temp"><span style="color:#ef4444">${dag.maks_temp_c>0?'+':''}${dag.maks_temp_c}°</span> / <span style="color:#0284c7">${dag.min_temp_c}°</span></div>
          <div class="fday-snow">${dag.ny_sno_cm>0?'❄ +'+dag.ny_sno_cm+'cm':''}</div>
        </div>`;
      }).join('');
    }

    if(d.intervaller&&d.intervaller.length){fcastData=d.intervaller;renderFcast();buildSkitur(d.intervaller);}

    const ts=new Date(d.hentet);
    document.getElementById('footer').textContent=`Oppdatert: ${ts.toLocaleString('no-NO')} · Data: Yr, Frost`;
  }catch(e){console.error('renderStatus feil:',e);}
}

function setFcast(h,btn){
  fcastHours=h;
  document.querySelectorAll('.chart-hours button').forEach(el=>{
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

  const iconsEl=document.getElementById('fcast-icons');
  if(iconsEl){
    const step=Math.max(1,Math.floor(data.length/16));
    iconsEl.innerHTML=data.filter((_,i)=>i%step===0).map(x=>{
      const dt=new Date(x.start);
      const lbl=String(dt.getHours()).padStart(2,'0')+':00';
      return`<div style="flex:1;min-width:32px;text-align:center;"><div style="font-size:14px;">${x.ver_ikon||''}</div><div style="font-size:9px;color:#475569;">${lbl}</div></div>`;
    }).join('');
  }

  const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
  const labels=data.map(x=>{
    const dt=new Date(x.start);const h=dt.getHours();
    if(h===0||fcastHours>24)return dt.getDate()+'.'+MS[dt.getMonth()]+(h===0?'':' '+String(h).padStart(2,'0'));
    return String(h).padStart(2,'0')+':00';
  });
  const temps=data.map(x=>x.temperatur_c!=null?parseFloat(x.temperatur_c):null);
  const precip=data.map(x=>x.nedbor_mm!=null?parseFloat(x.nedbor_mm):null);
  const precipColors=data.map(x=>(x.temperatur_c||0)<=0?'rgba(116,192,252,0.6)':'rgba(255,107,107,0.6)');
  const wind=data.map(x=>x.vind_ms!=null?parseFloat(x.vind_ms):null);

  const ctx=document.getElementById('fcast-chart').getContext('2d');
  if(fcastChart)fcastChart.destroy();
  if(msg)msg.style.display='none';

  fcastChart=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yP',order:3},
      {type:'line',label:'Temp',data:temps,borderWidth:2,pointRadius:0,tension:0.3,fill:false,yAxisID:'yT',order:1,
        segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroundColor:'transparent',
        borderWidth:1.5,borderDash:[3,3],pointRadius:0,tension:0.3,fill:false,yAxisID:'yW',order:2},
    ]},
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'rgba(8,14,31,.95)',titleColor:'#dce4f5',bodyColor:'#7b8db5'}},
      scales:{
        x:{ticks:{color:'#7b8db5',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:14},grid:{color:'rgba(100,130,200,.06)'}},
        yT:{position:'left',ticks:{color:'#7b8db5',font:{size:10},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(100,130,200,.06)'},afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:10}},grid:{drawOnChartArea:false}},
        yW:{display:false,min:0},
      }
    }
  });
}

// ── Historikk-graf ──
let histData=[],currentHours=12,histChart=null;

async function loadHistorikk(){
  const msg=document.getElementById('hist-msg');
  try{
    const d=await(await fetch(`/sn%C3%B8/${STASJON_ID}/api/historikk?hours=${currentHours}`)).json();
    if(!d.ok||!d.data.length){if(msg)msg.textContent='Ingen historikkdata';return;}
    histData=d.data; renderHistChart();
  }catch(e){if(msg)msg.textContent='Feil ved henting';}
}

function setHours(h,btn){
  currentHours=h;
  document.querySelectorAll('.chart-hours button').forEach(el=>{el.style.background='var(--bg)';el.style.color='var(--muted)';el.style.borderColor='var(--border)';});
  if(btn){btn.style.background='#334155';btn.style.color='#e2e8f0';btn.style.borderColor='#334155';}
  loadHistorikk();
}

function renderHistChart(){
  const msg=document.getElementById('hist-msg');
  const byTime={};
  histData.forEach(r=>{const t=r.tid;if(!byTime[t])byTime[t]={};byTime[t][r.element]=r.verdi;});
  const tider=Object.keys(byTime).sort();
  if(!tider.length)return;
  const MS=['jan','feb','mar','apr','mai','jun','jul','aug','sep','okt','nov','des'];
  const labels=tider.map(t=>{const d=new Date(t);const h=d.getHours();return h===0?d.getDate()+'.'+MS[d.getMonth()]+' 00:00':String(h).padStart(2,'0')+':00';});
  const temps=tider.map(t=>byTime[t]['air_temperature']??null);
  const precip=tider.map(t=>byTime[t]['sum(precipitation_amount PT1H)']??null);
  const wind=tider.map(t=>byTime[t]['wind_speed']??null);
  const precipColors=tider.map((t,i)=>(temps[i]??1)<=0?'rgba(116,192,252,0.6)':'rgba(255,107,107,0.6)');
  const ctx=document.getElementById('hist-chart').getContext('2d');
  if(histChart)histChart.destroy();
  if(msg)msg.style.display='none';
  histChart=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yP',order:2},
      {type:'line',label:'Temp',data:temps,borderWidth:2,pointRadius:0,tension:0.3,fill:false,yAxisID:'yT',order:1,
        segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind',data:wind,borderColor:'rgba(167,139,250,0.6)',backgroundColor:'transparent',borderWidth:1.5,borderDash:[3,3],pointRadius:0,tension:0.3,fill:false,yAxisID:'yW',order:3},
    ]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{ticks:{color:'#64748b',font:{size:10},maxRotation:30,autoSkip:true,maxTicksLimit:14},grid:{color:'rgba(0,0,0,.04)'}},
        yT:{position:'left',ticks:{color:'#64748b',font:{size:10},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(0,0,0,.04)'},afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#64748b',font:{size:10}},grid:{drawOnChartArea:false}},
        yW:{display:false,min:0},
      }
    }
  });
}

// ── Skitur-anbefalinger ──
const UKEDAGER=['søndag','mandag','tirsdag','onsdag','torsdag','fredag','lørdag'];
const SCORE_EMOJI={5:'☀️',4:'🌤️',3:'🌨️',2:'🌧️',1:'🌧️'};

function solTider(datoStr){
  const dato=new Date(datoStr+'T12:00:00');
  const dag=Math.floor((dato-new Date(dato.getFullYear(),0,0))/86400000);
  const lat=__LAT__*Math.PI/180;
  const decl=23.45*Math.sin((360/365*(dag-81))*Math.PI/180)*Math.PI/180;
  const cosHA=(Math.sin(-0.833*Math.PI/180)-Math.sin(lat)*Math.sin(decl))/(Math.cos(lat)*Math.cos(decl));
  if(Math.abs(cosHA)>1)return['--:--','--:--'];
  const ha=Math.acos(cosHA)*180/Math.PI;
  const tz=dato.getMonth()>=2&&dato.getMonth()<=9?2:1;
  const noon=12-__LON__/15+tz;
  const fmt=h=>{const hh=Math.floor(h);const mm=Math.round((h-hh)*60);return String(hh).padStart(2,'0')+':'+String(mm).padStart(2,'0');};
  return[fmt(noon-ha/15),fmt(noon+ha/15)];
}

function analyserDag(dato,ivs,solOpp,solNed){
  function tTilMin(t){if(!t||!t.includes(':'))return 0;const[h,m]=t.split(':');return+h*60+ +m;}
  const solOppMin=tTilMin(solOpp),solNedMin=tTilMin(solNed);
  function ivMin(iv){return tTilMin((iv.start||'').substring(11,16));}
  function ivType(iv){const nb=iv.nedbor_mm||0,temp=iv.temperatur_c||0;if(nb<0.15)return'tørt';if(temp>1.5)return'regn';if(temp>0)return'sludd';return'snø';}
  function ivLabel(iv){const nb=iv.nedbor_mm||0,vind=iv.vind_ms||0,t=ivType(iv),ikon=iv.ver_ikon||'';const sol=['☀','🌤','⛅'].some(s=>ikon.includes(s));if(t==='tørt')return sol?'sol':(vind>6?'oppholdsvær, vind':'oppholdsvær');if(t==='regn')return nb>2?'kraftig regn':'lett regn';if(t==='sludd')return'sludd';return nb>1?'moderat snøvær':'lett snøvær';}
  const dagslys=ivs.filter(iv=>ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin)||ivs;
  const base=dagslys.length?dagslys:ivs;const n=Math.max(base.length,1);
  const gode=ivs.filter(iv=>ivType(iv)!=='regn'&&(iv.vind_ms||0)<8&&ivMin(iv)>=solOppMin&&ivMin(iv)<=solNedMin);
  let besteTid=null;
  if(gode.length>1){const s=(gode[0].start||'').substring(11,16);const e=(gode[gode.length-1].start||'').substring(11,16);if(s!==e)besteTid=`${s}–${e}`;}
  const regnN=base.filter(iv=>ivType(iv)==='regn').length;
  const solN=base.filter(iv=>ivLabel(iv)==='sol').length;
  const vindSnitt=base.reduce((a,iv)=>a+(iv.vind_ms||0),0)/n;
  let score;
  if(regnN/n>0.6)score=1;else if(regnN/n>0.3)score=2;else if(solN/n>0.4&&vindSnitt<5)score=5;else if(solN/n>0.2&&vindSnitt<6)score=4;else score=3;
  const kortTekst={5:'Strålende dag – perfekt!',4:'Fin dag med gode forhold',3:'Greit skiføre',2:'En del regn, noen pauser',1:'Mye regn – bli hjemme'};
  const lavNedbørIvs=dagslys.filter(iv=>(iv.nedbor_mm||0)<0.3&&(iv.vind_ms||0)<8);
  let lavVindu=null;
  if(lavNedbørIvs.length>1){const s=(lavNedbørIvs[0].start||'').substring(11,16);const e=(lavNedbørIvs[lavNedbørIvs.length-1].start||'').substring(11,16);if(s!==e)lavVindu=`${s}–${e}`;}
  let detalj='';
  if(score===5)detalj=`Sol og lite vind hele dagen. Bare å stikke ut fra ${solOpp}! 🌟`;
  else if(score===4&&besteTid)detalj=`Gode forhold. Anbefalt tid: ${besteTid}.`;
  else if(score===3&&lavVindu)detalj=`Variabelt vær, men minst nedbør mellom ${lavVindu}.`;
  else if(score===2&&besteTid)detalj=`En del regn, men vinduet ${besteTid} kan utnyttes.`;
  else detalj='Det regner mesteparten av dagen. Bli heller hjemme.';
  return{score,kort:kortTekst[score],besteTid:besteTid||`${solOpp}–${solNed}`,detalj};
}

function buildSkitur(intervaller){
  window._skiIntervaller=intervaller;
  const el=document.getElementById('skitur-list');
  if(!el||!intervaller||!intervaller.length){if(el)el.innerHTML='';return;}

  // Vis regelbasert umiddelbart
  renderSkiturLokal(intervaller, el);

  // Oppdater stille med AI i bakgrunnen
  fetch(`/sn%C3%B8/${STASJON_ID}/api/skitur-ai`)
    .then(r=>r.json())
    .then(d=>{
      if(d.ok&&d.dager&&d.dager.length) renderSkiturDager(d.dager, el);
    })
    .catch(()=>{});
}

function renderSkiturLokal(intervaller, el){
  const perDag={};
  intervaller.forEach(iv=>{const dato=(iv.start||'').substring(0,10);if(dato){if(!perDag[dato])perDag[dato]=[];perDag[dato].push(iv);}});
  const datoer=Object.keys(perDag).sort().slice(0,8);
  if(!datoer.length){el.innerHTML='';return;}
  const dager=datoer.map(dato=>{
    const[solOpp,solNed]=solTider(dato);
    const a=analyserDag(dato,perDag[dato],solOpp,solNed);
    const dt=new Date(dato+'T12:00:00');
    return{dato,ukedag:UKEDAGER[dt.getDay()],soloppgang:solOpp,solnedgang:solNed,...a,beste_tid:a.besteTid};
  });
  renderSkiturDager(dager, el);
}

function renderSkiturDager(dager, el){
  if(!dager||!dager.length){el.innerHTML='';return;}
  el.innerHTML=dager.map((dag,i)=>{
    const dato=dag.dato;
    const score=dag.score||3, kort=dag.kort||'', detalj=dag.detalj||'';
    const besteTid=dag.beste_tid||dag.besteTid||null;
    const ukedag=dag.ukedag||'';
    const dagStr=ukedag.charAt(0).toUpperCase()+ukedag.slice(1);
    const datoStr=(dato||'').slice(5).replace('-','.');
    const emoji=SCORE_EMOJI[score]||'🌨️';
    const tidHtml=besteTid?`<span class="ski-beste-tid">⏰ ${besteTid}</span>`:'';
    return`<div class="ski-day">
      <div class="ski-day-header" onclick="toggleSkiDay(${i})">
        <div class="ski-day-left score-${score}"><div class="ski-day-icon-big">${emoji}</div><div class="ski-day-name-top">${dagStr}</div></div>
        <div class="ski-day-right">
          <div class="ski-day-title">${dagStr} ${datoStr}</div>
          <div class="ski-day-kort">${kort}</div>
          <div class="ski-day-kort" style="margin-top:3px;color:var(--hint);font-size:12px;">${detalj}</div>
          <div style="display:flex;align-items:center;gap:8px;margin-top:3px;">${tidHtml}<span class="ski-arrow" id="ski-arrow-${i}">▾ Graf</span></div>
        </div>
      </div>
      <div id="ski-chart-section-${i}" style="display:none;background:#0f172a;border-radius:0 0 14px 14px;">
        <div class="ski-day-chart-wrap"><canvas id="ski-chart-${i}"></canvas><div class="ski-day-chart-msg" id="ski-chart-msg-${i}"><span class="spinner"></span></div></div>
      </div>
    </div>`;
  }).join('');
}

function toggleSkiDay(i){
  const sec=document.getElementById('ski-chart-section-'+i);
  const arrow=document.getElementById('ski-arrow-'+i);
  const detail=document.getElementById('ski-detail-'+i);
  if(!sec)return;
  const open=sec.style.display==='none';
  sec.style.display=open?'block':'none';
  if(arrow)arrow.textContent=open?'▴ Graf':'▾ Graf';
  if(open&&(!detail||detail.dataset.rendered!=='1')){
    if(detail)detail.dataset.rendered='1';
    renderSkiChart(i,(window._skiIntervaller||[]).filter(iv=>(iv.start||'').startsWith(Object.keys((() =>{const p={};(window._skiIntervaller||[]).forEach(iv=>{const d=(iv.start||'').substring(0,10);if(d){if(!p[d])p[d]=[];p[d].push(iv);}});return p;})()).sort()[i])));
  }
}

const _skiCharts={};
function renderSkiChart(i,ivs){
  const msg=document.getElementById('ski-chart-msg-'+i);
  const ctx=document.getElementById('ski-chart-'+i);
  if(!ctx||!ivs||!ivs.length){if(msg)msg.textContent='Ingen data';return;}
  if(msg)msg.style.display='none';
  const labels=ivs.map(iv=>{const dt=new Date(iv.start);return String(dt.getHours()).padStart(2,'0')+':00';});
  const temps=ivs.map(iv=>iv.temperatur_c!=null?parseFloat(iv.temperatur_c):null);
  const precip=ivs.map(iv=>iv.nedbor_mm!=null?parseFloat(iv.nedbor_mm):null);
  const precipColors=ivs.map(iv=>(iv.temperatur_c||0)<=0?'rgba(116,192,252,0.6)':'rgba(255,107,107,0.6)');
  const wind=ivs.map(iv=>iv.vind_ms!=null?parseFloat(iv.vind_ms):null);
  if(_skiCharts[i])_skiCharts[i].destroy();
  _skiCharts[i]=new Chart(ctx,{
    data:{labels,datasets:[
      {type:'bar',label:'Nedbør',data:precip,backgroundColor:precipColors,borderRadius:2,yAxisID:'yP',order:3},
      {type:'line',label:'Temp',data:temps,borderWidth:2,pointRadius:0,tension:0.3,fill:false,yAxisID:'yT',order:1,segment:{borderColor:c=>c.p0.parsed.y<=0?'rgba(77,171,247,1)':'rgba(255,107,107,1)'},backgroundColor:'transparent'},
      {type:'line',label:'Vind',data:wind,borderColor:'rgba(167,139,250,0.7)',backgroundColor:'transparent',borderWidth:1.5,borderDash:[3,3],pointRadius:0,tension:0.3,fill:false,yAxisID:'yW',order:2},
    ]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},
      scales:{
        x:{ticks:{color:'#7b8db5',font:{size:9},maxRotation:0},grid:{color:'rgba(100,130,200,.06)'}},
        yT:{position:'left',ticks:{color:'#7b8db5',font:{size:9},callback:v=>(v>0?'+':'')+v+'°'},grid:{color:'rgba(100,130,200,.06)'},afterDataLimits(s){if(s.min>0)s.min=-1;if(s.max<0)s.max=1;}},
        yP:{position:'right',min:0,suggestedMax:1,ticks:{color:'#7b8db5',font:{size:9}},grid:{drawOnChartArea:false}},
        yW:{display:false,min:0},
      }
    }
  });
}

init();
loadHistorikk();
</script>
</body>
</html>"""
