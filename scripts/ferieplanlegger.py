"""Ferieplanlegger – korttid-modus.

Henter Yr-prognoser parallelt for alle destinasjoner i
:mod:`scripts.ferie_destinasjoner`, aggregerer per kalenderdag (lokal tid
per destinasjon, tilnærmet som UTC), og returnerer en rangert liste over
hvor det er forventet å være sol og varme de neste 7 dagene.

Resultatet caches i ~1 time for å respektere met.no sine TOS.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

from scripts.ferie_destinasjoner import DESTINASJONER, Destinasjon


# ---------- Konstanter ----------

YR_FORECAST_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
USER_AGENT = "prisanalyse.no/1.0 (kontakt@prisanalyse.no)"

DEFAULT_TIMEOUT = 12
MAX_WORKERS = 24
RETRY_WAIT = 2.0
PROGNOSE_DAGER = 10  # met.no Locationforecast leverer ~10 dager; siste dager er mindre presise

CACHE_TTL_S = 60 * 60  # 1 time


# ---------- Datatyper ----------

@dataclass
class Dagsprognose:
    """Aggregert prognose for én kalenderdag (UTC-basert)."""
    dato: date
    temp_max: Optional[float] = None
    temp_min: Optional[float] = None
    temp_mean: Optional[float] = None
    nedbor_mm: float = 0.0
    sky_mean_pct: Optional[float] = None
    symbol: str = ""           # dominant Yr-symbol for dagen
    treff: bool = False        # True hvis dagen oppfyller brukerens kriterier


@dataclass
class DestinasjonResultat:
    destinasjon: Destinasjon
    dager: list[Dagsprognose] = field(default_factory=list)
    treff_dager: int = 0
    score: float = 0.0
    beste_dag: Optional[Dagsprognose] = None
    feil: Optional[str] = None  # satt hvis prognose ikke kunne hentes


# ---------- Cache ----------

_FORECAST_CACHE: dict[tuple[float, float], tuple[float, list[dict]]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(lat: float, lon: float) -> Optional[list[dict]]:
    key = (round(lat, 3), round(lon, 3))
    with _CACHE_LOCK:
        entry = _FORECAST_CACHE.get(key)
        if not entry:
            return None
        expires_at, ts = entry
        if time.time() >= expires_at:
            _FORECAST_CACHE.pop(key, None)
            return None
        return ts


def _cache_put(lat: float, lon: float, ts: list[dict]) -> None:
    key = (round(lat, 3), round(lon, 3))
    with _CACHE_LOCK:
        _FORECAST_CACHE[key] = (time.time() + CACHE_TTL_S, ts)


# ---------- Henting fra Yr ----------

def _fetch_yr_timeseries(
    lat: float,
    lon: float,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[list[dict]]:
    cached = _cache_get(lat, lon)
    if cached is not None:
        return cached

    params = {"lat": round(lat, 4), "lon": round(lon, 4)}
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(3):
        try:
            r = session.get(YR_FORECAST_URL, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                ts = r.json().get("properties", {}).get("timeseries", []) or []
                _cache_put(lat, lon, ts)
                return ts
            if r.status_code == 429:
                time.sleep(RETRY_WAIT * (attempt + 1))
                continue
            if r.status_code >= 500:
                time.sleep(RETRY_WAIT)
                continue
            return None
        except requests.RequestException:
            time.sleep(1.0)
    return None


# ---------- Aggregering ----------

def _parse_time(item: dict) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(item["time"].replace("Z", "+00:00"))
    except (KeyError, ValueError, TypeError):
        return None


def _precip_for_block(item: dict) -> tuple[float, float]:
    """Returnerer (nedbør_mm, varighet_timer) for første tilgjengelige blokk."""
    data = item.get("data", {})
    for key, hours in (("next_1_hours", 1), ("next_6_hours", 6), ("next_12_hours", 12)):
        details = data.get(key, {}).get("details", {})
        amt = details.get("precipitation_amount")
        if amt is not None:
            try:
                return float(amt), hours
            except (TypeError, ValueError):
                continue
    return 0.0, 0


def _symbol_for_block(item: dict) -> str:
    data = item.get("data", {})
    for key in ("next_1_hours", "next_6_hours", "next_12_hours"):
        sym = ((data.get(key) or {}).get("summary") or {}).get("symbol_code")
        if sym:
            return str(sym).strip()
    return ""


def _aggreger_dagsprognoser(ts: list[dict], antall_dager: int = PROGNOSE_DAGER) -> list[Dagsprognose]:
    """Aggregerer timeserien til én rad per UTC-dato.

    Vi bruker UTC-dag som tilnærming. Avvik fra lokal tid er typisk 1-3
    timer, og fanger ikke opp natt-/dag-vippepunkter perfekt, men er
    presist nok for "hvor er det varmt og sol om dagen"-spørsmålet.
    """
    today_utc = datetime.now(timezone.utc).date()
    cutoff = today_utc + timedelta(days=antall_dager)

    # Samle rådata per dato.
    per_dag: dict[date, dict[str, list]] = {}
    blokk_nedbor: dict[date, dict[str, float]] = {}

    for item in ts:
        t = _parse_time(item)
        if t is None:
            continue
        d = t.date()
        if d < today_utc or d >= cutoff:
            continue

        details = (item.get("data") or {}).get("instant", {}).get("details", {})
        temp = details.get("air_temperature")
        cloud = details.get("cloud_area_fraction")

        bucket = per_dag.setdefault(d, {"temp": [], "cloud": [], "symboler": []})
        if temp is not None:
            try:
                bucket["temp"].append(float(temp))
            except (TypeError, ValueError):
                pass
        if cloud is not None:
            try:
                bucket["cloud"].append(float(cloud))
            except (TypeError, ValueError):
                pass

        sym = _symbol_for_block(item)
        if sym:
            # Telle kun symboler fra dagtid-vindu (06-21 UTC ≈ rimelig for Europa).
            if 6 <= t.hour <= 21:
                bucket["symboler"].append(sym)

        # Nedbør: summer over blokker, men unngå dobbelttelling
        # ved å foretrekke korteste blokk (next_1 før next_6 før next_12).
        amt, hours = _precip_for_block(item)
        if hours > 0:
            # Bruk en "claim"-mekanisme per time slik at lengre blokker ikke
            # legger seg oppå korteste blokk.
            for k in range(hours):
                slot = t + timedelta(hours=k)
                if slot.date() != d:
                    continue
                claim = blokk_nedbor.setdefault(d, {})
                slot_key = slot.isoformat()
                if slot_key not in claim:
                    claim[slot_key] = amt / hours

    resultater: list[Dagsprognose] = []
    for i in range(antall_dager):
        d = today_utc + timedelta(days=i)
        bucket = per_dag.get(d)
        if not bucket:
            resultater.append(Dagsprognose(dato=d))
            continue

        temps = bucket["temp"]
        clouds = bucket["cloud"]
        symboler: list[str] = bucket["symboler"]

        # Dominerende symbol = mest hyppige dag-symbol.
        dom_sym = ""
        if symboler:
            tellinger: dict[str, int] = {}
            for s in symboler:
                tellinger[s] = tellinger.get(s, 0) + 1
            dom_sym = max(tellinger.items(), key=lambda kv: kv[1])[0]

        regn_for_dag = sum(blokk_nedbor.get(d, {}).values())

        resultater.append(Dagsprognose(
            dato=d,
            temp_max=max(temps) if temps else None,
            temp_min=min(temps) if temps else None,
            temp_mean=(sum(temps) / len(temps)) if temps else None,
            nedbor_mm=round(regn_for_dag, 1),
            sky_mean_pct=(sum(clouds) / len(clouds)) if clouds else None,
            symbol=dom_sym,
        ))

    return resultater


# ---------- Filtrering og scoring ----------

@dataclass
class Kriterier:
    min_temp: float = 20.0
    maks_nedbor_mm: float = 3.0
    maks_sky_pct: float = 60.0
    min_treff_dager: int = 3   # antall dager i 7-dagers-perioden som må oppfylle
    kun_norge: bool = False
    tags: list[str] = field(default_factory=list)  # OR-filter; tom liste = ingen tag-filter


def _vurder_dag(dag: Dagsprognose, k: Kriterier) -> bool:
    if dag.temp_max is None:
        return False
    if dag.temp_max < k.min_temp:
        return False
    if dag.nedbor_mm > k.maks_nedbor_mm:
        return False
    if dag.sky_mean_pct is not None and dag.sky_mean_pct > k.maks_sky_pct:
        return False
    return True


def _scor_dag(dag: Dagsprognose) -> float:
    """Enkel score: temp – 3*nedbør – 0.1*sky.

    Brukes til å rangere både dager (for å finne "beste dag") og
    destinasjoner (sum over alle treff-dager).
    """
    if dag.temp_max is None:
        return -999.0
    temp = float(dag.temp_max)
    regn = float(dag.nedbor_mm or 0.0)
    sky = float(dag.sky_mean_pct) if dag.sky_mean_pct is not None else 50.0
    return temp - 3.0 * regn - 0.1 * sky


def _passer_filter(dest: Destinasjon, k: Kriterier) -> bool:
    if k.kun_norge and dest["land"] != "NO":
        return False
    if k.tags:
        if not any(tag in dest["tags"] for tag in k.tags):
            return False
    return True


# ---------- Hovedinngang ----------

def planlegg_korttid(
    kriterier: Kriterier,
    *,
    max_workers: int = MAX_WORKERS,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[DestinasjonResultat]:
    """Returnerer destinasjoner som matcher kriteriene, sortert etter score."""
    kandidater = [d for d in DESTINASJONER if _passer_filter(d, kriterier)]

    def _process(dest: Destinasjon) -> DestinasjonResultat:
        with requests.Session() as sess:
            ts = _fetch_yr_timeseries(dest["lat"], dest["lon"], sess, timeout=timeout)
        if ts is None:
            return DestinasjonResultat(destinasjon=dest, feil="prognose_utilgjengelig")

        dager = _aggreger_dagsprognoser(ts)
        treff = 0
        score_sum = 0.0
        beste: Optional[Dagsprognose] = None
        beste_score = -999.0

        for dag in dager:
            if _vurder_dag(dag, kriterier):
                dag.treff = True
                treff += 1
                s = _scor_dag(dag)
                score_sum += s
                if s > beste_score:
                    beste_score = s
                    beste = dag

        return DestinasjonResultat(
            destinasjon=dest,
            dager=dager,
            treff_dager=treff,
            score=round(score_sum, 2),
            beste_dag=beste,
        )

    resultater: list[DestinasjonResultat] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_process, d): d for d in kandidater}
        for fut in as_completed(futures):
            try:
                resultater.append(fut.result())
            except Exception:  # robust mot enkelttilfeller — hopp over
                continue

    # Behold bare destinasjoner med nok treff-dager.
    matchende = [r for r in resultater if r.feil is None and r.treff_dager >= kriterier.min_treff_dager]
    matchende.sort(key=lambda r: (r.score, r.treff_dager), reverse=True)
    return matchende


# ---------- Serialisering ----------

def dag_til_dict(dag: Dagsprognose) -> dict:
    return {
        "dato": dag.dato.isoformat(),
        "temp_max": dag.temp_max,
        "temp_min": dag.temp_min,
        "temp_mean": round(dag.temp_mean, 1) if dag.temp_mean is not None else None,
        "nedbor_mm": dag.nedbor_mm,
        "sky_pct": round(dag.sky_mean_pct, 0) if dag.sky_mean_pct is not None else None,
        "symbol": dag.symbol,
        "treff": dag.treff,
    }


def resultat_til_dict(r: DestinasjonResultat) -> dict:
    d = r.destinasjon
    return {
        "id": d["id"],
        "navn": d["navn"],
        "land": d["land"],
        "region": d["region"],
        "lat": d["lat"],
        "lon": d["lon"],
        "tags": d["tags"],
        "treff_dager": r.treff_dager,
        "score": r.score,
        "beste_dag": dag_til_dict(r.beste_dag) if r.beste_dag else None,
        "dager": [dag_til_dict(x) for x in r.dager],
    }


# =========================================================
# LANGTID (klimanormaler fra NASA POWER, 1991-2020)
# =========================================================

from pathlib import Path

import pandas as pd

from scripts.ferie_destinasjoner import DESTINASJONER_BY_ID

KLIMA_NORMALER_FIL = Path(__file__).resolve().parent.parent / "data" / "klima_normaler.parquet"


@dataclass
class LangtidKriterier:
    måned: int                       # 1-12
    min_temp_mean: float = 18.0      # månedssnitt °C
    min_sol: float = 4.0             # kWh/m²/dag
    maks_nedbor_mm: float = 80.0     # mm/måned
    kun_norge: bool = False
    tags: list[str] = field(default_factory=list)


_KLIMA_DF: Optional[pd.DataFrame] = None
_KLIMA_LOCK = threading.Lock()


def _last_klima_normaler() -> pd.DataFrame:
    """Lazy-load parquet med klimanormaler. Cachet i prosessens levetid."""
    global _KLIMA_DF
    if _KLIMA_DF is not None:
        return _KLIMA_DF
    with _KLIMA_LOCK:
        if _KLIMA_DF is None:
            if not KLIMA_NORMALER_FIL.exists():
                raise FileNotFoundError(
                    f"Mangler klimanormaler. Kjør først: "
                    f"python -m scripts.bygg_klima_normaler  "
                    f"(forventet fil: {KLIMA_NORMALER_FIL})"
                )
            _KLIMA_DF = pd.read_parquet(KLIMA_NORMALER_FIL)
    return _KLIMA_DF


def _scor_klima_rad(temp: float, sol: float, nedbor: float) -> float:
    """Enkel score: temperatur + 2·sol – 0.1·nedbør_mm."""
    return float(temp) + 2.0 * float(sol) - 0.1 * float(nedbor)


def planlegg_langtid(kriterier: LangtidKriterier) -> list[dict]:
    """Returnerer destinasjoner som matcher klimanormaler, sortert etter score."""
    if not (1 <= kriterier.måned <= 12):
        raise ValueError(f"Ugyldig måned: {kriterier.måned}")

    df = _last_klima_normaler()
    df = df[df["month"] == kriterier.måned].copy()

    if kriterier.kun_norge:
        df = df[df["land"] == "NO"]

    if kriterier.tags:
        valid_ids = {
            d["id"] for d in DESTINASJONER_BY_ID.values()
            if any(t in d["tags"] for t in kriterier.tags)
        }
        df = df[df["dest_id"].isin(valid_ids)]

    # Hardfiltrer på brukerens kriterier.
    df = df[df["temp_mean"] >= kriterier.min_temp_mean]
    df = df[df["solar_kwh_day"] >= kriterier.min_sol]
    df = df[df["precip_mm_month"] <= kriterier.maks_nedbor_mm]

    if df.empty:
        return []

    df["score"] = df.apply(
        lambda r: _scor_klima_rad(r["temp_mean"], r["solar_kwh_day"], r["precip_mm_month"]),
        axis=1,
    ).round(2)
    df = df.sort_values("score", ascending=False)

    resultater: list[dict] = []
    for _, r in df.iterrows():
        dest = DESTINASJONER_BY_ID.get(r["dest_id"])
        if not dest:
            continue
        resultater.append({
            "id": dest["id"],
            "navn": dest["navn"],
            "land": dest["land"],
            "region": dest["region"],
            "lat": dest["lat"],
            "lon": dest["lon"],
            "tags": dest["tags"],
            "score": float(r["score"]),
            "klima": {
                "month": int(r["month"]),
                "temp_mean": float(r["temp_mean"]),
                "temp_min": float(r["temp_min"]),
                "temp_max": float(r["temp_max"]),
                "precip_mm_day": float(r["precip_mm_day"]),
                "precip_mm_month": float(r["precip_mm_month"]),
                "solar_kwh_day": float(r["solar_kwh_day"]),
            },
        })
    return resultater
