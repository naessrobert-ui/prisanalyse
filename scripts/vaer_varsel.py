"""Værside for de faste stedene: /ver/varsel/<sted>.

Siden svarer på tre spørsmål før alt annet: regner det de neste to timene
(radar), resten av dagen og i natt (eller i morgen). Under kommer time for
time, åtte dager fremover og hvor godt Yr og Google stemmer overens.

Kilder:
* MET Locationforecast (Yr): timesvarsel, symboler og dagsvarsel.
* MET Nowcast: radarbasert nedbør i 5-minutterssteg de neste to timene.
* MET Radar: animasjon for landsdelen, proxet via serveren.
* Google Weather: gjenbruker cachen i `weather_comparison` (48 timer). Værsøket
  og dager uten fullt timesvarsel fra Yr henter i tillegg 240 timer ved behov
  (`/ver/api/timeserie/<sted>`).
* Frost: målt nedbør hittil i dag på stasjonen scoreboardet bruker.

Alle tekster lages her, så siden kan testes uten nettleser. Byggefunksjonene
er rene funksjoner; bare `_fetch_*` går mot nettet.
"""
from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import requests
from flask import Blueprint, Response, abort, jsonify, make_response, redirect, render_template

from scripts.weather_comparison import PLACES
from scripts.weather_comparison import _provider as _comparison_provider

OSLO = ZoneInfo("Europe/Oslo")
MET = "https://api.met.no/weatherapi"
HEADERS = {"User-Agent": "prisanalyse.no/1.0 kontakt@prisanalyse.no"}
RADAR_AREA = {"bergen": "western_norway", "kvamskogen": "western_norway"}
#: Steder som får Googles dagsvarsel (ett ekstra Google-kall i timen per sted).
GOOGLE_DAYS_PLACES = {"bergen"}

#: Steder med WeatherNext 3. Cron-jobben lagrer varselet hver time, så siden
#: leser bare det lagrede og gjør ingen egne Earth Engine-kall.
WEATHERNEXT_PLACES = {"bergen"}
#: Eldre lagret WeatherNext-varsel enn dette vises ikke.
WEATHERNEXT_MAX_AGE = timedelta(hours=12)

#: Timer med minst så mye nedbør regnes som "regn" i tekstene.
WET_MM = 0.1
#: Google oppgir ofte hundredeler. Fra denne grensen omtales det som "litt yr".
GOOGLE_DRIZZLE_MM = 0.03

vaer_varsel = Blueprint("vaer_varsel", __name__)


# ---------------------------------------------------------------------------
# Hjelpere
# ---------------------------------------------------------------------------

def _num(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result else None  # NaN -> None


def _parse_time(value: str) -> Optional[datetime]:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return result.astimezone(timezone.utc) if result.tzinfo else None


def _local(value: datetime) -> datetime:
    return value.astimezone(OSLO)


def _hh(value: datetime) -> str:
    return f"{_local(value).hour:02d}"


def mm_text(value: float) -> str:
    """0,4 / 1,3 / 9 (hele mm over 5, som Yr)."""
    if value >= 5:
        return f"{int(value + 0.5):d}"  # vanlig avrunding, ikke bankers
    return f"{value:.1f}".replace(".", ",")


def _ranges(hours: list[datetime]) -> list[tuple[datetime, datetime]]:
    """Sammenhengende timer -> [(start, slutt)] der slutt er timen etter siste."""
    spans: list[tuple[datetime, datetime]] = []
    for hour in sorted(hours):
        if spans and spans[-1][1] == hour:
            spans[-1] = (spans[-1][0], hour + timedelta(hours=1))
        else:
            spans.append((hour, hour + timedelta(hours=1)))
    return spans


def _range_text(spans: list[tuple[datetime, datetime]], day_end: datetime) -> str:
    parts = []
    for start, end in spans:
        if end >= day_end:
            parts.append(f"fra kl. {_hh(start)}")
        elif end - start == timedelta(hours=1):
            parts.append(f"kl. {_hh(start)}")
        else:
            parts.append(f"kl. {_hh(start)}–{_hh(end)}")
    if len(parts) > 1:
        return ", ".join(parts[:-1]) + " og " + parts[-1]
    return parts[0] if parts else ""


SYMBOL_TEXT = [
    ("thunder", "Torden"), ("heavysleet", "Kraftig sludd"), ("lightsleet", "Lett sludd"),
    ("sleet", "Sludd"), ("heavysnow", "Kraftig snø"), ("lightsnow", "Lett snø"), ("snow", "Snø"),
    ("heavyrainshowers", "Kraftige regnbyger"), ("lightrainshowers", "Lette regnbyger"),
    ("rainshowers", "Regnbyger"), ("heavyrain", "Kraftig regn"), ("lightrain", "Lett regn"),
    ("rain", "Regn"), ("fog", "Tåke"), ("partlycloudy", "Delvis skyet"), ("fair", "Lettskyet"),
    ("clearsky", "Klarvær"), ("cloudy", "Skyet"),
]


def feels_like(temp: Optional[float], wind: Optional[float]) -> Optional[float]:
    """Effektiv temperatur med vindavkjøling (JAG/TI), slik Yr viser «Føles som».

    Yr bruker formelen også over 10 grader. Over 20 grader og i svak vind
    (under 1,34 m/s = 4,8 km/t) gir formelen ingen mening, og vi viser lufttemperaturen.
    """
    if temp is None:
        return None
    if wind is None or wind < 1.34 or temp > 20:
        return temp
    v = (wind * 3.6) ** 0.16
    return min(temp, 13.12 + 0.6215 * temp - 11.37 * v + 0.3965 * temp * v)


def symbol_text(symbol: Optional[str]) -> str:
    for key, text in SYMBOL_TEXT:
        if symbol and symbol.startswith(key):
            return text
    return ""


# ---------------------------------------------------------------------------
# Yr
# ---------------------------------------------------------------------------

def parse_yr(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Locationforecast -> én rad per tidspunkt med de feltene siden bruker."""
    rows = []
    for item in (payload.get("properties") or {}).get("timeseries") or []:
        t = _parse_time(item.get("time"))
        if t is None:
            continue
        data = item.get("data") or {}
        inst = (data.get("instant") or {}).get("details") or {}
        row: dict[str, Any] = {
            "time": t,
            "temp": _num(inst.get("air_temperature")),
            "wind": _num(inst.get("wind_speed")),
            "gust": _num(inst.get("wind_speed_of_gust")),
            "wind_dir": _num(inst.get("wind_from_direction")),
            "cloud": _num(inst.get("cloud_area_fraction")),
            "n1": None, "n6": None,
        }
        for key, hours in (("next_1_hours", "n1"), ("next_6_hours", "n6")):
            block = data.get(key) or {}
            details = block.get("details") or {}
            symbol = (block.get("summary") or {}).get("symbol_code")
            if symbol is None and "precipitation_amount" not in details:
                continue
            row[hours] = {
                "symbol": symbol,
                "rain": _num(details.get("precipitation_amount")) or 0.0,
                "rain_max": _num(details.get("precipitation_amount_max")),
                "pop": _num(details.get("probability_of_precipitation")),
                "tmax": _num(details.get("air_temperature_max")),
                "tmin": _num(details.get("air_temperature_min")),
            }
        rows.append(row)
    rows.sort(key=lambda r: r["time"])
    return rows


def yr_hours(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bare timene med eget timesvarsel (normalt de første ~60 timene)."""
    return [r for r in rows if r["n1"] is not None]


def slices(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Del tidslinjen i ikke-overlappende biter: timer først, så seks-timersblokker."""
    result = []
    cursor: Optional[datetime] = None
    for row in rows:
        block, length = (row["n1"], 1) if row["n1"] else (row["n6"], 6)
        if block is None or (cursor is not None and row["time"] < cursor):
            continue
        end = row["time"] + timedelta(hours=length)
        result.append({"start": row["time"], "end": end, "symbol": block["symbol"],
                       "rain": block["rain"], "tmax": block.get("tmax"), "tmin": block.get("tmin")})
        cursor = end
    return result


# ---------------------------------------------------------------------------
# Byggeklosser for siden
# ---------------------------------------------------------------------------

def _google_index(google_hours: list[dict[str, Any]]) -> dict[datetime, dict[str, Any]]:
    index = {}
    for hour in google_hours or []:
        t = _parse_time(hour.get("start"))
        if t is not None:
            index[t] = hour
    return index


def build_hours(rows: list[dict[str, Any]], google_hours: list[dict[str, Any]],
                start: datetime, count: int = 25) -> list[dict[str, Any]]:
    google = _google_index(google_hours)
    result = []
    for row in yr_hours(rows):
        if row["time"] < start or len(result) >= count:
            continue
        n1 = row["n1"]
        g = google.get(row["time"]) or {}
        g_temp, g_rain = _num(g.get("temp")), _num(g.get("rain"))
        disagree = False
        if g_rain is not None and abs(n1["rain"] - g_rain) >= 1.0:
            disagree = True
        if g_temp is not None and row["temp"] is not None and abs(row["temp"] - g_temp) >= 2.5:
            disagree = True
        local = _local(row["time"])
        result.append({
            "time": local.isoformat(), "hour": local.hour,
            "symbol": n1["symbol"], "temp": row["temp"], "rain": n1["rain"],
            "rain_max": n1["rain_max"], "pop": n1["pop"],
            "wind": row["wind"], "gust": row["gust"], "wind_dir": row["wind_dir"],
            "g_temp": g_temp, "g_rain": g_rain, "disagree": disagree,
        })
    return result


PERIODS = (("natt", 0), ("morgen", 6), ("ettermiddag", 12), ("kveld", 18))
WEEKDAYS = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "Lørdag", "Søndag"]


def _period_symbol(parts: list[dict[str, Any]]) -> Optional[str]:
    if not parts:
        return None
    total = sum(p["rain"] for p in parts)
    if total >= 0.3:
        return max(parts, key=lambda p: p["rain"])["symbol"]
    weight: Counter = Counter()
    for p in parts:
        if p["symbol"]:
            weight[p["symbol"]] += (p["end"] - p["start"]).total_seconds()
    return weight.most_common(1)[0][0] if weight else None


def build_days(rows: list[dict[str, Any]], google_hours: list[dict[str, Any]],
               observed: dict[datetime, float], now: datetime, count: int = 8) -> list[dict[str, Any]]:
    """Åtte døgn i lokal tid. I dag tar med målt nedbør for timene som er gått."""
    parts = slices(rows)
    google = _google_index(google_hours)
    today = _local(now).date()
    days = []
    for offset in range(count):
        date = today + timedelta(days=offset)
        day_start = datetime(date.year, date.month, date.day, tzinfo=OSLO)
        day_end = day_start + timedelta(days=1)
        in_day = [p for p in parts if day_start <= _local(p["start"] + (p["end"] - p["start"]) / 2) < day_end]
        if offset > 0 and not in_day:
            break
        periods = []
        for name, hour in PERIODS:
            p_start = day_start + timedelta(hours=hour)
            p_end = p_start + timedelta(hours=6)
            chunk = [p for p in in_day if p_start <= _local(p["start"] + (p["end"] - p["start"]) / 2) < p_end]
            obs = sum(v for t, v in observed.items() if p_start <= _local(t) < p_end) if offset == 0 else 0.0
            periods.append({
                "name": name, "symbol": _period_symbol(chunk),
                "rain": round(sum(p["rain"] for p in chunk) + obs, 1),
                "observed": bool(obs) and not chunk,
            })
        day_rows = [r for r in rows if day_start <= _local(r["time"]) < day_end]
        winds = [r["wind"] for r in day_rows if r["wind"] is not None]
        gusts = [r["gust"] for r in day_rows if r["gust"] is not None]
        temps = [r["temp"] for r in day_rows if r["temp"] is not None]
        temps += [v for p in in_day for v in (p["tmax"], p["tmin"]) if v is not None]
        observed_today = round(sum(observed.values()), 1) if offset == 0 else None
        yr_rain = sum(p["rain"] for p in in_day) + (observed_today or 0.0)

        # Google teller bare når hele resten av døgnet er dekket.
        first_needed = max(day_start, _local(now).replace(minute=0, second=0, microsecond=0))
        needed = []
        t = first_needed
        while t < day_end:
            needed.append(t.astimezone(timezone.utc))
            t += timedelta(hours=1)
        g_rain = None
        if needed and all(n in google and _num(google[n].get("rain")) is not None for n in needed):
            g_rain = round(sum(_num(google[n]["rain"]) for n in needed) + (observed_today or 0.0), 1)

        days.append({
            "date": date.isoformat(),
            "label": "I dag" if offset == 0 else ("I morgen" if offset == 1 else WEEKDAYS[date.weekday()]),
            "weekday": WEEKDAYS[date.weekday()],
            "periods": periods,
            "tmin": round(min(temps)) if temps else None,
            "tmax": round(max(temps)) if temps else None,
            "rain": round(yr_rain, 1),
            "g_rain": g_rain,
            "observed": observed_today,
            "wind_max": round(max(winds)) if winds else None,
            "gust_max": round(max(gusts)) if gusts else None,
        })
    return days


def _wn_symbol(rain: float, sky: Optional[float], night: bool) -> str:
    """Yr-symbolkode fra WeatherNext-nedbør (mm per seksjon) og skydekke (%)."""
    if rain >= 6:
        return "heavyrain"
    if rain >= 1.5:
        return "rain"
    if rain >= 0.3:
        return "lightrain"
    sky = 70.0 if sky is None else sky
    suffix = "_night" if night else "_day"
    if sky < 20:
        return "clearsky" + suffix
    if sky < 45:
        return "fair" + suffix
    if sky < 75:
        return "partlycloudy" + suffix
    return "cloudy"


def weathernext_by_day(varsel: Optional[dict[str, Any]], now: datetime) -> dict[str, dict[str, Any]]:
    """WeatherNext per lokal dato: temperatur med 80 %-spenn, nedbør og fire seksjoner.

    Timesummer i WeatherNext gjelder timen som starter på klokkeslettet (se
    `scripts.weathernext`), så timene kan summeres rett per seksjon.
    """
    if not varsel:
        return {}
    by_date: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
    for h in varsel.get("timer") or []:
        t = _parse_time(h.get("t", ""))
        if t is None or t < now - timedelta(hours=1):
            continue
        by_date.setdefault(_local(t).date().isoformat(), []).append((_local(t), h))

    def mean(h: dict[str, Any], key: str, stat: str = "mean") -> Optional[float]:
        return _num((h.get(key) or {}).get(stat))

    result: dict[str, dict[str, Any]] = {}
    for date, hrs in by_date.items():
        temps = [v for _, h in hrs if (v := mean(h, "temp")) is not None]
        if not temps:
            continue
        lows = [v for _, h in hrs if (v := mean(h, "temp", "p10")) is not None]
        highs = [v for _, h in hrs if (v := mean(h, "temp", "p90")) is not None]
        winds = [v for _, h in hrs if (v := mean(h, "vind")) is not None]
        periods = []
        for name, start in PERIODS:
            chunk = [h for t, h in hrs if start <= t.hour < start + 6]
            if not chunk:
                periods.append({"name": name, "symbol": None, "rain": 0.0, "observed": False})
                continue
            rain = sum(mean(h, "regn") or 0.0 for h in chunk)
            skies = [v for h in chunk if (v := mean(h, "sky")) is not None]
            ptemps = [v for h in chunk if (v := mean(h, "temp")) is not None]
            periods.append({
                "name": name,
                "symbol": _wn_symbol(rain, sum(skies) / len(skies) if skies else None, name in ("natt", "kveld")),
                "rain": round(rain, 1), "observed": False,
                "tmin": round(min(ptemps)) if ptemps else None,
                "tmax": round(max(ptemps)) if ptemps else None,
            })
        result[date] = {
            "hours": len(hrs),
            "tmin": round(min(temps)), "tmax": round(max(temps)),
            "lo": round(min(lows)) if lows else None, "hi": round(max(highs)) if highs else None,
            "rain": round(sum(mean(h, "regn") or 0.0 for _, h in hrs), 1),
            "rain_hi": round(sum(mean(h, "regn", "p90") or 0.0 for _, h in hrs), 1),
            "wind_max": round(max(winds)) if winds else None,
            "periods": periods,
        }
    return result


def weathernext_extra_days(wn_days: dict[str, dict[str, Any]], last_yr_date: Optional[str],
                           min_hours: int = 18) -> list[dict[str, Any]]:
    """Dager etter at Yr slutter, i samme form som `build_days`, merket som WeatherNext."""
    extra = []
    for date in sorted(wn_days):
        if last_yr_date and date <= last_yr_date:
            continue
        d = wn_days[date]
        if d["hours"] < min_hours:
            continue
        weekday = WEEKDAYS[datetime.fromisoformat(date).weekday()]
        extra.append({
            "date": date, "label": weekday, "weekday": weekday, "source": "weathernext",
            "periods": d["periods"], "tmin": d["tmin"], "tmax": d["tmax"],
            "lo": d["lo"], "hi": d["hi"], "rain": d["rain"], "rain_hi": d["rain_hi"],
            "g_rain": None, "observed": None, "wind_max": d["wind_max"], "gust_max": None,
        })
    return extra


def _window(rows: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    return [r for r in yr_hours(rows) if start <= r["time"] < end]


def rest_of_day(rows, google_hours, now: datetime) -> Optional[dict[str, Any]]:
    hour_now = now.replace(minute=0, second=0, microsecond=0)
    local = _local(hour_now)
    day_end = (local.replace(hour=0) + timedelta(days=1)).astimezone(timezone.utc)
    hours = _window(rows, hour_now, day_end)
    if not hours:
        return None
    wet = [h["time"] for h in hours if h["n1"]["rain"] >= WET_MM]
    total = sum(h["n1"]["rain"] for h in hours)
    temps = [h for h in hours if h["temp"] is not None]
    warmest = max(temps, key=lambda h: h["temp"]) if temps else None

    if not wet:
        symbols = Counter(h["n1"]["symbol"] for h in hours if h["n1"]["symbol"])
        head, icon, wet_flag = "Tørt resten av dagen", (symbols.most_common(1)[0][0] if symbols else "cloudy"), False
    elif wet[0] <= hour_now:
        dry = [h["time"] for h in hours if h["time"] > wet[0] and h["n1"]["rain"] < WET_MM]
        head = f"Regn nå, tørt fra kl. {_hh(dry[0])}" if dry else "Regn resten av dagen"
        icon, wet_flag = max(hours, key=lambda h: h["n1"]["rain"])["n1"]["symbol"], True
    else:
        head, wet_flag = f"Tørt til kl. {_hh(wet[0])}", False
        icon = next(h["n1"]["symbol"] for h in hours if h["time"] < wet[0])

    parts = []
    if warmest and warmest["time"] > hour_now:
        parts.append(f"Opptil {round(warmest['temp'])}° rundt kl. {_hh(warmest['time'])}.")
    if wet:
        parts.append(f"Yr: {mm_text(total)} mm {_range_text(_ranges(wet), day_end)}.")

    google = _google_index(google_hours)
    g_hours = [google[h["time"]] for h in hours if h["time"] in google and _num(google[h["time"]].get("rain")) is not None]
    google_text = None
    if len(g_hours) == len(hours):
        g_total = sum(_num(g["rain"]) for g in g_hours)
        g_wet = [h["time"] for h in hours if _num(google[h["time"]]["rain"]) >= GOOGLE_DRIZZLE_MM]
        if not g_wet:
            google_text = "Google: tørt."
        else:
            word = "litt yr" if max(_num(google[t]["rain"]) for t in g_wet) < 0.3 else "regn"
            google_text = f"Google: {word} {_range_text(_ranges(g_wet), day_end)}, til sammen {mm_text(g_total)} mm."
    return {"head": head, "icon": icon, "wet": wet_flag, "text": " ".join(parts), "google": google_text}


def night_or_tomorrow(rows, google_hours, now: datetime) -> Optional[dict[str, Any]]:
    local = _local(now)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if local.hour < 20:
        title, start, end = "I natt", midnight + timedelta(hours=22), midnight + timedelta(days=1, hours=8)
    else:
        title, start, end = "I morgen", midnight + timedelta(days=1, hours=8), midnight + timedelta(days=1, hours=22)
    start_u, end_u = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    hours = _window(rows, start_u, end_u)
    if not hours:
        return None
    total = sum(h["n1"]["rain"] for h in hours)
    wet = [h for h in hours if h["n1"]["rain"] >= WET_MM]
    parts = []
    if not wet:
        symbols = Counter(h["n1"]["symbol"] for h in hours if h["n1"]["symbol"])
        head, icon, wet_flag = "Opphold", (symbols.most_common(1)[0][0] if symbols else "cloudy"), False
        temps = [h["temp"] for h in hours if h["temp"] is not None]
        if temps:
            parts.append(f"{round(min(temps))}° til {round(max(temps))}°.")
    else:
        peak = max(hours, key=lambda h: h["n1"]["rain"])
        head = f"Regn, {mm_text(total)} mm" if total >= 1 else f"Litt regn, {mm_text(total)} mm"
        icon, wet_flag = peak["n1"]["symbol"], True
        heavy = [h["time"] for h in hours if h["n1"]["rain"] >= 0.5 * peak["n1"]["rain"]]
        for s, e in _ranges(heavy):
            if s <= peak["time"] < e:
                parts.append(f"Mest mellom kl. {_hh(s)} og {_hh(e)}." if e - s > timedelta(hours=1)
                             else f"Mest rundt kl. {_hh(s)}.")
        last_wet = wet[-1]["time"]
        dry = [h["time"] for h in hours if h["time"] > last_wet]
        if dry:
            parts.append(f"Tørt igjen fra kl. {_hh(dry[0])}.")
    winds = [h["wind"] for h in hours if h["wind"] is not None]
    if winds and max(winds) >= 8:
        parts.append(f"Vind opptil {round(max(winds))} m/s.")
    google = _google_index(google_hours)
    google_text = None
    if all(h["time"] in google and _num(google[h["time"]].get("rain")) is not None for h in hours):
        g_total = sum(_num(google[h["time"]]["rain"]) for h in hours)
        google_text = "Google: tørt." if g_total < 0.05 else f"Google: {mm_text(g_total)} mm."
    return {"title": title, "head": head, "icon": icon, "wet": wet_flag,
            "text": " ".join(parts), "google": google_text}


def best_window(rows, now: datetime) -> Optional[dict[str, Any]]:
    """Lengste sammenhengende oppholdsperiode mellom kl. 07 og 22, i dag eller i morgen."""
    local = _local(now)
    for offset, label in ((0, "i dag"), (1, "i morgen")):
        day = local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=offset)
        start = max(day + timedelta(hours=7), local.replace(minute=0, second=0, microsecond=0))
        end = day + timedelta(hours=22)
        if end - start < timedelta(hours=2):
            continue
        hours = _window(rows, start.astimezone(timezone.utc), end.astimezone(timezone.utc))
        good = [h for h in hours if h["n1"]["rain"] < WET_MM and (h["n1"]["pop"] or 0) < 30
                and (h["wind"] or 0) < 8]
        spans = _ranges([h["time"] for h in good])
        if not spans:
            return {"label": label, "text": None}
        s, e = max(spans, key=lambda span: span[1] - span[0])
        if e - s < timedelta(hours=2):
            return {"label": label, "text": None}
        temps = [h["temp"] for h in good if s <= h["time"] < e and h["temp"] is not None]
        lo, hi = round(min(temps)), round(max(temps))
        temp_text = f"{hi}°" if lo == hi else f"{lo}–{hi}°"
        return {"label": label, "text": f"kl. {_hh(s)}–{_hh(e)}, opphold og {temp_text}"}
    return None


def agreement(hours: list[dict[str, Any]]) -> Optional[dict[str, str]]:
    both = [h for h in hours if h["g_rain"] is not None]
    if len(both) < 12:
        return None
    yr = sum(h["rain"] for h in both)
    g = sum(h["g_rain"] for h in both)
    first_yr = next((i for i, h in enumerate(both) if h["rain"] >= WET_MM), None)
    first_g = next((i for i, h in enumerate(both) if h["g_rain"] >= WET_MM), None)
    if yr < 0.2 and g < 0.2:
        title, text = "Yr og Google er enige", "Begge venter tørt vær det neste døgnet."
    elif abs(yr - g) <= max(1.0, 0.3 * max(yr, g)):
        title = "Yr og Google er enige om regnet"
        text = f"Begge venter rundt {mm_text((yr + g) / 2)} mm det neste døgnet."
        if first_yr is not None and first_g is not None and abs(first_yr - first_g) >= 2:
            g_hour, y_hour = both[first_g]["hour"], both[first_yr]["hour"]
            word = "tidligere" if first_g < first_yr else "senere"
            text += f" Google tror det starter litt {word}, kl. {g_hour:02d} mot kl. {y_hour:02d} hos Yr."
    else:
        title = "Yr og Google er uenige om regnet"
        text = f"Yr venter {mm_text(yr)} mm og Google {mm_text(g)} mm det neste døgnet."
    diffs = [h["g_temp"] - h["temp"] for h in both if h["g_temp"] is not None and h["temp"] is not None]
    if diffs:
        mean = sum(diffs) / len(diffs)
        if abs(mean) >= 1.5:
            text += f" Google er i snitt {abs(mean):.0f}° {'kaldere' if mean < 0 else 'varmere'}."
    return {"title": title, "text": text}


def parse_nowcast(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    props = payload.get("properties") or {}
    coverage = (props.get("meta") or {}).get("radar_coverage")
    steps, temp, wind, wind_dir = [], None, None, None
    for item in props.get("timeseries") or []:
        t = _parse_time(item.get("time"))
        details = ((item.get("data") or {}).get("instant") or {}).get("details") or {}
        if t is None:
            continue
        if temp is None:
            temp, wind = _num(details.get("air_temperature")), _num(details.get("wind_speed"))
            wind_dir = _num(details.get("wind_from_direction"))
        minutes = (t - now).total_seconds() / 60
        if -5 < minutes <= 120:
            rate = _num(details.get("precipitation_rate"))
            steps.append({"time": _local(t).strftime("%H:%M"), "minutes": round(minutes), "rate": rate or 0.0})
    return {"available": coverage == "ok" and bool(steps), "coverage": coverage, "steps": steps,
            "temp": temp, "wind": wind, "wind_dir": wind_dir}


def observed_rain(rows: list[dict[str, Any]], now: datetime) -> dict[datetime, float]:
    """Frost-rader -> {timens start (UTC): mm} for timene i dag lokal tid."""
    midnight = _local(now).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    result = {}
    for row in rows:
        if row.get("element") != "sum(precipitation_amount PT1H)":
            continue
        end = _parse_time(row.get("reference_time"))
        value = _num(row.get("value"))
        if end is None or value is None:
            continue
        start = end - timedelta(hours=1)
        if midnight <= start and end <= now:
            result[start] = value
    return result


def trust_text(score_rows: list[dict[str, Any]], place: str, days: int) -> Optional[dict[str, str]]:
    names = {"temp": "temperatur", "rain": "nedbør", "wind": "vind", "gust": "vindkast"}
    yr, google = [], []
    for row in score_rows:
        if row.get("place") != place or row.get("bucket") != "alle" or row.get("element") not in names:
            continue
        if row.get("winner") == "yr":
            yr.append(names[row["element"]])
        elif row.get("winner") == "google":
            google.append(names[row["element"]])

    def join(items: list[str]) -> str:
        return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " og " + items[-1]

    if not yr and not google:
        return {"title": "Ingen klar vinner ennå",
                "text": f"Siste {days} dager er forskjellene mellom Yr og Google for små til å kåre en vinner."}
    where = ("på " if place == "kvamskogen" else "i ") + PLACES[place]["name"].split()[0]
    if yr and not google:
        title = f"Yr har truffet best {where}"
    elif google and not yr:
        title = f"Google har truffet best {where}"
    else:
        title = "Yr og Google er gode på hver sine ting"
    parts = []
    if yr:
        parts.append(f"Yr bommet minst på {join(yr)}")
    if google:
        parts.append(f"Google bommet minst på {join(google)}")
    return {"title": title, "text": f"Siste {days} dager: " + ", og ".join(parts) + ". Resten er uavgjort."}


def observations_by_hour(rows: list[dict[str, Any]]) -> dict[datetime, dict[str, float]]:
    """Frost-rader -> {timens start (UTC): {temp, wind, rain, gust}}.

    Samme konvensjon som scoreboardet: temperatur og vind er øyeblikksverdier
    ved referansetiden og hører til timen som starter da. Nedbør og vindkast
    gjelder timen som slutter ved referansetiden.
    """
    keys = {"air_temperature": ("temp", 0), "wind_speed": ("wind", 0),
            "sum(precipitation_amount PT1H)": ("rain", 1), "max(wind_speed_of_gust PT1H)": ("gust", 1)}
    result: dict[datetime, dict[str, float]] = {}
    for row in rows:
        spec = keys.get(row.get("element"))
        ref = _parse_time(row.get("reference_time"))
        value = _num(row.get("value"))
        if spec is None or ref is None or value is None:
            continue
        name, shift = spec
        result.setdefault(ref - timedelta(hours=shift), {})[name] = value
    return result


def reference_forecast(frame_rows: list[dict[str, Any]], place: str,
                       day_start: datetime) -> tuple[Optional[datetime], dict[datetime, dict[str, Any]]]:
    """Varselet slik det så ut ved midnatt: siste lagrede kjøring før døgnet startet.

    Mangler den (cron nede), brukes den tidligste kjøringen i døgnet.
    Returnerer (kjøretime, {valid_start: {"yr": {...}, "google": {...}}}).
    """
    rows = [r for r in frame_rows if r.get("place") == place]
    runs = sorted({t for t in (_parse_time(r.get("run_hour")) for r in rows) if t is not None})
    if not runs:
        return None, {}
    before = [r for r in runs if r <= day_start]
    run = before[-1] if before else runs[0]
    result: dict[datetime, dict[str, Any]] = {}
    for r in rows:
        if _parse_time(r.get("run_hour")) != run:
            continue
        start = _parse_time(r.get("valid_start"))
        if start is None:
            continue
        result.setdefault(start, {})[r.get("provider")] = {"temp": _num(r.get("temp")), "rain": _num(r.get("rain"))}
    return run, result


def build_today(rows: list[dict[str, Any]], google_hours: list[dict[str, Any]],
                observed: dict[datetime, dict[str, float]], reference: dict[datetime, dict[str, Any]],
                now: datetime) -> list[dict[str, Any]]:
    """Inneværende døgn time for time: målt og varslet ved midnatt for timene som
    er gått, og gjeldende varsel for resten. Etter kl. 18 tas natten med til kl. 08."""
    local = _local(now)
    day_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end = day_start + timedelta(days=1, hours=8 if local.hour >= 18 else 0)
    yr = {r["time"]: r for r in yr_hours(rows)}
    google = _google_index(google_hours)
    result = []
    t = day_start.astimezone(timezone.utc)
    while t < end.astimezone(timezone.utc):
        lt = _local(t)
        past = t + timedelta(hours=1) <= now
        ref = reference.get(t) or {}
        obs = observed.get(t) or {}
        item: dict[str, Any] = {"time": lt.isoformat(), "hour": lt.hour, "past": past}
        if past:
            item.update({
                "obs_temp": obs.get("temp"), "obs_rain": obs.get("rain"), "obs_wind": obs.get("wind"),
                "obs_gust": obs.get("gust"),
                "ref_temp": (ref.get("yr") or {}).get("temp"), "ref_rain": (ref.get("yr") or {}).get("rain"),
                "g_temp": (ref.get("google") or {}).get("temp"), "g_rain": (ref.get("google") or {}).get("rain"),
            })
        else:
            row = yr.get(t)
            g = google.get(t) or {}
            if row is not None:
                n1 = row["n1"]
                item.update({"symbol": n1["symbol"], "temp": row["temp"], "rain": n1["rain"],
                             "rain_max": n1["rain_max"], "pop": n1["pop"], "wind": row["wind"],
                             "gust": row["gust"], "wind_dir": row["wind_dir"]})
            item.update({"g_temp": _num(g.get("temp")), "g_rain": _num(g.get("rain"))})
            if row is not None and item["g_rain"] is not None:
                item["disagree"] = (abs(item["rain"] - item["g_rain"]) >= 1.0 or (
                    item["g_temp"] is not None and item["temp"] is not None and abs(item["temp"] - item["g_temp"]) >= 2.5))
        result.append(item)
        t += timedelta(hours=1)
    return result


def today_score(today: list[dict[str, Any]], run: Optional[datetime]) -> Optional[dict[str, str]]:
    """Kort fasit for timene som er gått: målt mot varslet ved midnatt."""
    past = [h for h in today if h["past"]]
    rain_hours = [h for h in past if h.get("obs_rain") is not None]
    if not rain_hours and not any(h.get("obs_temp") is not None for h in past):
        return None
    when = "ved midnatt"
    if run is not None:
        run_local = _local(run)
        day = datetime.fromisoformat(today[0]["time"]).date()
        if run_local.hour != 0 or run_local.date() != day:
            when = ("i går " if run_local.date() < day else "") + f"kl. {run_local.hour:02d}"
    parts = []
    if rain_hours:
        measured = sum(h["obs_rain"] for h in rain_hours)
        yr = [h["ref_rain"] for h in rain_hours if h.get("ref_rain") is not None]
        g = [h["g_rain"] for h in rain_hours if h.get("g_rain") is not None]
        text = f"Målt hittil i dag: {mm_text(measured)} mm."
        if len(yr) == len(rain_hours):
            text += f" Varslet {when}: Yr {mm_text(sum(yr))} mm"
            text += f", Google {mm_text(sum(g))} mm." if len(g) == len(rain_hours) else "."
        parts.append(text)

    def mae(key: str) -> Optional[float]:
        pairs = [(h["obs_temp"], h[key]) for h in past if h.get("obs_temp") is not None and h.get(key) is not None]
        return sum(abs(a - b) for a, b in pairs) / len(pairs) if len(pairs) >= 3 else None

    yr_t, g_t = mae("ref_temp"), mae("g_temp")
    if yr_t is not None:
        def deg(v: float) -> str:
            return f"{v:.1f}".replace(".", ",")
        text = f"Temperaturen bommet i snitt med {deg(yr_t)}° hos Yr"
        text += f" og {deg(g_t)}° hos Google." if g_t is not None else "."
        parts.append(text)
    return {"text": " ".join(parts)} if parts else None


# ---------------------------------------------------------------------------
# Samlet timeserie (Yr + Google) for værsøket og timesvarsel langt frem
# ---------------------------------------------------------------------------

def sun_elevation(lat: float, lon: float, when: datetime) -> float:
    """Solhøyde i grader (NOAA sin forenklede formel, nøyaktig til ca. en kvart grad)."""
    import math

    t = when.astimezone(timezone.utc)
    doy = t.timetuple().tm_yday
    hour = t.hour + t.minute / 60 + t.second / 3600
    g = 2 * math.pi / 365 * (doy - 1 + (hour - 12) / 24)
    eqtime = 229.18 * (0.000075 + 0.001868 * math.cos(g) - 0.032077 * math.sin(g)
                       - 0.014615 * math.cos(2 * g) - 0.040849 * math.sin(2 * g))
    decl = (0.006918 - 0.399912 * math.cos(g) + 0.070257 * math.sin(g) - 0.006758 * math.cos(2 * g)
            + 0.000907 * math.sin(2 * g) - 0.002697 * math.cos(3 * g) + 0.00148 * math.sin(3 * g))
    tst = hour * 60 + eqtime + 4 * lon
    ha = math.radians(tst / 4 - 180)
    phi = math.radians(lat)
    cos_zen = math.sin(phi) * math.sin(decl) + math.cos(phi) * math.cos(decl) * math.cos(ha)
    return 90 - math.degrees(math.acos(max(-1.0, min(1.0, cos_zen))))


def _hour_symbol(rain: Optional[float], cloud: Optional[float], day: bool) -> str:
    """Yr-symbolkode for én time ut fra nedbør (mm) og skydekke (%)."""
    rain = rain or 0.0
    if rain >= 4:
        return "heavyrain"
    if rain >= 1:
        return "rain"
    if rain >= WET_MM:
        return "lightrain"
    cloud = 70.0 if cloud is None else cloud
    suffix = "_day" if day else "_night"
    if cloud < 20:
        return "clearsky" + suffix
    if cloud < 45:
        return "fair" + suffix
    if cloud < 75:
        return "partlycloudy" + suffix
    return "cloudy"


def _interpolate(points: list[tuple[datetime, Optional[float]]], t: datetime) -> Optional[float]:
    """Lineær interpolasjon mellom Yr sine øyeblikksverdier (hver sjette time langt frem)."""
    known = [(pt, v) for pt, v in points if v is not None]
    before = [(pt, v) for pt, v in known if pt <= t]
    after = [(pt, v) for pt, v in known if pt >= t]
    if not before or not after:
        # Siste blokk har ingen verdi etter seg: hold nærmeste kjente verdi.
        edge = before[-1:] or after[:1]
        return edge[0][1] if edge else None
    (t0, v0), (t1, v1) = before[-1], after[0]
    if t1 == t0:
        return v0
    return v0 + (v1 - v0) * (t - t0).total_seconds() / (t1 - t0).total_seconds()


def _previous(points: list[tuple[datetime, Optional[float]]], t: datetime) -> Optional[float]:
    """Siste kjente verdi før eller ved t (vindretning kan ikke interpoleres lineært over nord)."""
    known = [v for pt, v in points if v is not None and pt <= t]
    return known[-1] if known else None


def _r(value: Optional[float], digits: int = 2) -> Optional[float]:
    return None if value is None else round(value, digits)


def build_series(rows: list[dict[str, Any]], google_hours: list[dict[str, Any]], now: datetime,
                 lat: float, lon: float, max_hours: int = 240) -> list[dict[str, Any]]:
    """Én rad per time fra inneværende time og så langt Yr eller Google rekker.

    Kilden står i `src`:
    * "yr": Yr har eget timesvarsel (de første ~60 timene).
    * "yr6": Yr har bare seks-timersblokker. Blokkens nedbør fordeles på timene
      etter Googles timeprofil, så totalen er Yr sin. Mangler Google, eller
      venter Google tørt, fordeles den jevnt. Temperatur, vind og skydekke er
      Googles timeverdier, eller interpolert fra Yr når Google mangler.
    * "google": etter at Yr slutter, bare Google.

    Feltnavnene er de samme som i `build_hours`, så siden kan tegne radene med
    samme timesgraf.
    """
    hour_now = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    yr = {r["time"]: r for r in yr_hours(rows)}
    google = _google_index(google_hours)
    instants = {k: [(r["time"], r.get(k)) for r in rows] for k in ("temp", "wind", "gust", "cloud", "wind_dir")}

    # Seks-timersblokker: fordel nedbøren time for time.
    six: dict[datetime, tuple[dict[str, Any], float]] = {}
    for part in slices(rows):
        length = int((part["end"] - part["start"]).total_seconds() // 3600)
        if length <= 1:
            continue
        hours = [part["start"] + timedelta(hours=i) for i in range(length)]
        weights = [max(0.0, _num((google.get(h) or {}).get("rain")) or 0.0) for h in hours]
        total = sum(weights)
        for h, w in zip(hours, weights):
            share = w / total if total > 0 else 1 / length
            six[h] = (part, part["rain"] * share)

    last = max([max(yr, default=hour_now)] + [h for h in six] + [h for h in google], default=hour_now)
    result = []
    t = hour_now
    while t <= last and len(result) < max_hours:
        g = google.get(t) or {}
        g_rain, g_temp = _num(g.get("rain")), _num(g.get("temp"))
        g_wind, g_gust, g_cloud = _num(g.get("wind")), _num(g.get("gust")), _num(g.get("cloud"))
        day = sun_elevation(lat, lon, t + timedelta(minutes=30)) > 0
        local = _local(t)
        item: dict[str, Any] = {"time": local.isoformat(), "hour": local.hour, "day": day,
                                "g_rain": _r(g_rain), "g_temp": _r(g_temp, 1), "g_wind": _r(g_wind, 1),
                                "g_cloud": _r(g_cloud, 0), "rain_max": None, "pop": None, "disagree": False}
        row = yr.get(t)
        if row is not None:
            n1 = row["n1"]
            item.update(src="yr", symbol=n1["symbol"], rain=n1["rain"], rain_max=n1["rain_max"], pop=n1["pop"],
                        temp=_r(row["temp"], 1), wind=row["wind"], gust=row["gust"], wind_dir=row["wind_dir"],
                        cloud=row.get("cloud") if row.get("cloud") is not None else g_cloud)
            if g_rain is not None:
                item["disagree"] = abs(n1["rain"] - g_rain) >= 1.0 or (
                    g_temp is not None and row["temp"] is not None and abs(row["temp"] - g_temp) >= 2.5)
        elif t in six or g:
            src = "yr6" if t in six else "google"
            rain = six[t][1] if t in six else g_rain
            pick = {k: (gv if gv is not None else _interpolate(instants[k], t))
                    for k, gv in (("temp", g_temp), ("wind", g_wind), ("gust", g_gust), ("cloud", g_cloud))}
            item.update(src=src, rain=_r(rain), temp=_r(pick["temp"], 1), wind=_r(pick["wind"], 1),
                        gust=_r(pick["gust"], 1), cloud=_r(pick["cloud"], 0),
                        wind_dir=_previous(instants["wind_dir"], t) if src == "yr6" else None)
            item["symbol"] = _hour_symbol(item["rain"], item["cloud"], day)
        else:
            t += timedelta(hours=1)
            continue
        result.append(item)
        t += timedelta(hours=1)
    return result


def parse_google_days(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Google days:lookup -> dag (07-19) og natt (19-07) per dato."""
    def qpf(part: dict[str, Any]) -> Optional[float]:
        q = ((part or {}).get("precipitation") or {}).get("qpf") or {}
        value = _num(q.get("quantity"))
        if value is None:
            return None
        return value * 25.4 if q.get("unit") == "INCHES" else value

    result = []
    for day in payload.get("forecastDays") or []:
        d = day.get("displayDate") or {}
        try:
            date = datetime(int(d["year"]), int(d["month"]), int(d["day"])).date().isoformat()
        except (KeyError, TypeError, ValueError):
            continue
        dt, nt = day.get("daytimeForecast") or {}, day.get("nighttimeForecast") or {}
        result.append({
            "date": date,
            "day_rain": qpf(dt), "night_rain": qpf(nt),
            "day_pop": _num(((dt.get("precipitation") or {}).get("probability") or {}).get("percent")),
            "tmax": _num((day.get("maxTemperature") or {}).get("degrees")),
            "tmin": _num((day.get("minTemperature") or {}).get("degrees")),
            "day_text": ((dt.get("weatherCondition") or {}).get("description") or {}).get("text"),
        })
    return result


# ---------------------------------------------------------------------------
# Henting med cache
# ---------------------------------------------------------------------------

class _Cache:
    def __init__(self) -> None:
        self._data: dict[Any, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._key_locks: dict[Any, threading.Lock] = {}

    def get(self, key: Any, ttl: float, fn: Callable[[], Any]) -> Any:
        with self._lock:
            hit = self._data.get(key)
            if hit and hit[0] > time.monotonic():
                return hit[1]
            lock = self._key_locks.setdefault(key, threading.Lock())
        with lock:  # én henting om gangen per nøkkel
            with self._lock:
                hit = self._data.get(key)
                if hit and hit[0] > time.monotonic():
                    return hit[1]
            value = fn()
            with self._lock:
                self._data[key] = (time.monotonic() + ttl, value)
            return value

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_CACHE = _Cache()


def _get(url: str, **params: Any) -> requests.Response:
    response = requests.get(url, params=params, headers=HEADERS, timeout=(4, 12))
    response.raise_for_status()
    return response


def _fetch_yr(place: str) -> dict[str, Any]:
    p = PLACES[place]
    return _CACHE.get(("yr", place), 600, lambda: _get(
        f"{MET}/locationforecast/2.0/complete", lat=f"{p['lat']:.4f}", lon=f"{p['lon']:.4f}").json())


def _fetch_nowcast(place: str) -> dict[str, Any]:
    p = PLACES[place]
    return _CACHE.get(("nowcast", place), 240, lambda: _get(
        f"{MET}/nowcast/2.0/complete", lat=f"{p['lat']:.4f}", lon=f"{p['lon']:.4f}").json())


def _fetch_radar(area: str) -> bytes:
    return _CACHE.get(("radar", area), 300, lambda: _get(
        f"{MET}/radar/2.0/", type="5level_reflectivity", area=area, content="animation").content)


def _fetch_google_days(place: str) -> list[dict[str, Any]]:
    """Googles dagsvarsel (10 dager). Samme cachetid som timesvarselet, som
    Googles vilkår krever at slettes innen en time."""
    if place not in GOOGLE_DAYS_PLACES:
        return []
    p = PLACES[place]
    return _google_days_at(p["lat"], p["lon"], ("google_days", place))


def _google_days_at(lat: float, lon: float, cache_key: Any,
                    on_call: Optional[Callable[[], None]] = None) -> list[dict[str, Any]]:
    import os

    key = os.environ.get("GOOGLE_WEATHER_API_KEY", "").strip()
    if not key:
        return []

    def load():
        if on_call is not None:
            on_call()
        return parse_google_days(_get(
            "https://weather.googleapis.com/v1/forecast/days:lookup",
            **{"location.latitude": lat, "location.longitude": lon, "days": 10, "pageSize": 10,
               "unitsSystem": "METRIC", "languageCode": "no", "key": key}).json())

    return _CACHE.get(cache_key, 3300, load)


def _fetch_weathernext(place: str, now: datetime) -> Optional[dict[str, Any]]:
    if place not in WEATHERNEXT_PLACES:
        return None
    from scripts import weathernext

    varsel = _CACHE.get(("weathernext", place), 300, lambda: weathernext.les_siste(place))
    if not varsel:
        return None
    hentet = _parse_time(varsel.get("hentet", ""))
    if hentet is None or now - hentet > WEATHERNEXT_MAX_AGE:
        return None
    return varsel


def _fetch_reference(place: str, now: datetime) -> tuple[Optional[datetime], dict[datetime, dict[str, Any]]]:
    """Lagret varsel fra scoreboardet (S3) for inneværende døgn."""
    from scripts.weather_scoreboard import FORECAST_COLUMNS, _FORECASTS, _load

    day_start = _local(now).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    def load():
        frame = _load(_FORECASTS, day_start - timedelta(hours=2), now, FORECAST_COLUMNS)
        return reference_forecast(frame.to_dict("records"), place, day_start)

    return _CACHE.get(("reference", place, day_start), 1800, load)


def _fetch_google(place: str) -> list[dict[str, Any]]:
    payload = _comparison_provider(place, "google")
    return payload.get("hours") or []


def _google_long_hours() -> int:
    import os

    try:
        return max(48, min(240, int(os.environ.get("GOOGLE_LANG_TIMER", "240"))))
    except ValueError:
        return 240


def _fetch_google_long(place: str) -> list[dict[str, Any]]:
    """Googles timesvarsel så langt det rekker (240 timer = ti kall).

    Hentes bare når noen åpner værsøket eller en dag uten fullt timesvarsel
    fra Yr, og holdes under en time som Googles vilkår krever.
    """
    from scripts.weather_comparison import fetch_google_hours

    p = PLACES[place]
    return _CACHE.get(("google_long", place), 3300,
                      lambda: fetch_google_hours(p["lat"], p["lon"], hours=_google_long_hours()))


def _fetch_observations(place: str, now: datetime) -> tuple[list[dict[str, Any]], Optional[str]]:
    from scripts.weather_scoreboard import STATIONS, fetch_observations

    def load():
        midnight = _local(now).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        return fetch_observations(place, midnight, now)

    key = ("frost", place, now.replace(minute=0, second=0, microsecond=0))
    return _CACHE.get(key, 600, load), STATIONS.get(place, {}).get("name")


_TRUST: dict[str, Any] = {"rows": None, "days": 30, "loading": False, "at": 0.0}
_TRUST_LOCK = threading.Lock()


def _trust_rows() -> Optional[list[dict[str, Any]]]:
    """Treffsikkerhet fra scoreboardet. Tungt (leser 30 dager fra S3), så det
    regnes ut i bakgrunnen og holdes i 12 timer. Siden viser ingenting til da."""
    with _TRUST_LOCK:
        fresh = _TRUST["rows"] is not None and time.time() - _TRUST["at"] < 12 * 3600
        if fresh or _TRUST["loading"]:
            return _TRUST["rows"]
        _TRUST["loading"] = True

    def work():
        rows = _TRUST["rows"]
        try:
            from scripts.weather_scoreboard import report
            rows = report(days=_TRUST["days"])["score"].to_dict("records")
        except Exception:  # noqa: BLE001 - tillitskortet er pynt, ikke kritisk
            pass
        with _TRUST_LOCK:
            _TRUST.update(rows=rows, loading=False, at=time.time())

    threading.Thread(target=work, daemon=True).start()
    return _TRUST["rows"]


def _safe(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception:  # noqa: BLE001 - én kilde som feiler skal ikke velte siden
        return None


def build_payload(place: str, now: Optional[datetime] = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with ThreadPoolExecutor(max_workers=6) as pool:
        f_yr = pool.submit(_safe, lambda: _fetch_yr(place))
        f_nc = pool.submit(_safe, lambda: _fetch_nowcast(place))
        f_g = pool.submit(_safe, lambda: _fetch_google(place))
        f_gd = pool.submit(_safe, lambda: _fetch_google_days(place))
        f_obs = pool.submit(_safe, lambda: _fetch_observations(place, now))
        f_ref = pool.submit(_safe, lambda: _fetch_reference(place, now))
        f_wn = pool.submit(_safe, lambda: _fetch_weathernext(place, now))
        yr_payload, nc_payload, google, obs = f_yr.result(), f_nc.result(), f_g.result(), f_obs.result()
        google_days, (ref_run, reference) = f_gd.result() or [], f_ref.result() or (None, {})
        wn_varsel = f_wn.result()
    if not yr_payload:
        raise RuntimeError("Fikk ikke hentet varselet fra Yr.")
    rows = parse_yr(yr_payload)
    google = google or []
    obs_rows, station = obs if obs else ([], None)
    observed = observed_rain(obs_rows, now)
    today = build_today(rows, google, observations_by_hour(obs_rows), reference, now)
    hour_now = now.replace(minute=0, second=0, microsecond=0)
    hours = build_hours(rows, google, hour_now)
    nowcast = parse_nowcast(nc_payload, now) if nc_payload else {"available": False, "steps": []}

    current = next((h for h in hours), None) or {}
    now_block = {
        "temp": nowcast.get("temp") if nowcast.get("temp") is not None else current.get("temp"),
        "wind": nowcast.get("wind") if nowcast.get("wind") is not None else current.get("wind"),
        "wind_dir": nowcast.get("wind_dir") if nowcast.get("wind_dir") is not None else current.get("wind_dir"),
        "gust": current.get("gust"),
        "symbol": current.get("symbol"),
        "text": symbol_text(current.get("symbol")),
    }
    now_block["feels_like"] = feels_like(now_block["temp"], now_block["wind"])
    trust_rows = _trust_rows()
    meta = (yr_payload.get("properties") or {}).get("meta") or {}
    days = build_days(rows, google, observed, now)
    wn_days = weathernext_by_day(wn_varsel, now)
    yr_dates = [d["date"] for d in days if d.get("tmin") is not None]
    days += weathernext_extra_days(wn_days, yr_dates[-1] if yr_dates else None)
    return {
        "place": {"id": place, **PLACES[place]},
        "generated_at": _local(now).isoformat(),
        "yr_updated_at": meta.get("updated_at"),
        "now": now_block,
        "observed": {"station": station, "today": round(sum(observed.values()), 1) if observed else None,
                     "available": bool(obs_rows)},
        "nowcast": nowcast,
        "rest_of_day": rest_of_day(rows, google, now),
        "later": night_or_tomorrow(rows, google, now),
        "best": best_window(rows, now),
        "hours": hours,
        "days": days,
        "wn_days": {k: {kk: v[kk] for kk in ("tmin", "tmax", "lo", "hi", "rain", "rain_hi")}
                    for k, v in wn_days.items()},
        "weathernext": ({"init": wn_varsel.get("init_lang"), "kilde": wn_varsel.get("kilde"),
                         "attribusjon": wn_varsel.get("attribusjon")} if wn_varsel else None),
        "today": today,
        "today_score": today_score(today, ref_run),
        "detail_hours": build_hours(rows, google, hour_now, count=100),
        "blocks": [{"start": _local(p["start"]).isoformat(), "end": _local(p["end"]).isoformat(),
                    "symbol": p["symbol"], "rain": p["rain"], "tmax": p["tmax"], "tmin": p["tmin"]}
                   for p in slices(rows) if p["end"] - p["start"] > timedelta(hours=1)],
        "google_days": google_days,
        "agreement": agreement(hours),
        "trust": trust_text(trust_rows, place, _TRUST["days"]) if trust_rows else None,
        "google_available": bool(google),
    }


# ---------------------------------------------------------------------------
# Ruter
# ---------------------------------------------------------------------------

def _frame_headers(response: Response) -> Response:
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' https://visitkvamskogen.no https://www.visitkvamskogen.no "
        "https://visitkvamskogen.onrender.com"
    )
    return response


#: visitkvamskogen.no bygger sin egen værside på disse API-ene.
CORS_ORIGINS = {
    "https://visitkvamskogen.no", "https://www.visitkvamskogen.no",
    "https://visitkvamskogen.onrender.com", "http://localhost:5173",
}
CORS_PATHS = ("/ver/api/varsel/", "/ver/api/timeserie/")


@vaer_varsel.after_request
def _cors(response: Response) -> Response:
    from flask import request

    origin = request.headers.get("Origin")
    if origin in CORS_ORIGINS and request.path.startswith(CORS_PATHS):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers.add("Vary", "Origin")
    return response


@vaer_varsel.get("/ver/varsel")
def varsel_default():
    return redirect("/ver/varsel/bergen")


@vaer_varsel.get("/ver/varsel/<sted>")
def varsel_page(sted: str):
    if sted not in PLACES:
        abort(404)
    places = sorted(({"id": k, "name": v["name"].split()[0]} for k, v in PLACES.items()),
                    key=lambda p: p["id"] != "bergen")  # Bergen først
    return _frame_headers(make_response(render_template("ver/varsel.html", sted=sted, places=places)))


@vaer_varsel.get("/ver/api/varsel/<sted>")
def varsel_api(sted: str):
    if sted not in PLACES:
        return jsonify(error="Ukjent sted."), 404
    try:
        payload = build_payload(sted)
    except RuntimeError as exc:
        return jsonify(error=str(exc)), 503
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store"
    return response


def build_series_payload(place: str, now: Optional[datetime] = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_yr = pool.submit(_safe, lambda: _fetch_yr(place))
        f_g = pool.submit(_safe, lambda: _fetch_google_long(place))
        yr_payload, google = f_yr.result(), f_g.result() or []
    if not yr_payload:
        raise RuntimeError("Fikk ikke hentet varselet fra Yr.")
    p = PLACES[place]
    hours = build_series(parse_yr(yr_payload), google, now, p["lat"], p["lon"])
    last = {src: next((h["time"] for h in reversed(hours) if h["src"] == src), None)
            for src in ("yr", "yr6", "google")}
    return {
        "place": {"id": place, **p},
        "generated_at": _local(now).isoformat(),
        "google_available": bool(google),
        "yr_hourly_until": last["yr"],
        "hours": hours,
    }


@vaer_varsel.get("/ver/api/timeserie/<sted>")
def timeserie_api(sted: str):
    """Samlet timeserie for værsøket: Yr der Yr har timer, Google-fordelt ellers."""
    if sted not in PLACES:
        return jsonify(error="Ukjent sted."), 404
    try:
        payload = build_series_payload(sted)
    except RuntimeError as exc:
        return jsonify(error=str(exc)), 503
    response = jsonify(payload)
    # Kort nettlesercache: Googles timedata skal ikke ligge lenger enn en time.
    response.headers["Cache-Control"] = "private, max-age=600"
    return response


# ---------------------------------------------------------------------------
# Google for vilkårlige punkter (aktivt varsel)
# ---------------------------------------------------------------------------

class GoogleQuotaExceeded(Exception):
    pass


_QUOTA = {"day": None, "calls": 0}
_QUOTA_LOCK = threading.Lock()


def _google_quota() -> int:
    import os

    try:
        return max(0, int(os.environ.get("GOOGLE_PUNKT_MAKS_PER_DOGN", "200")))
    except ValueError:
        return 200


def _count_google_call() -> None:
    """Teller kall mot Google for vilkårlige steder og stopper ved dagens tak."""
    today = datetime.now(timezone.utc).date()
    with _QUOTA_LOCK:
        if _QUOTA["day"] != today:
            _QUOTA.update(day=today, calls=0)
        if _QUOTA["calls"] >= _google_quota():
            raise GoogleQuotaExceeded()
        _QUOTA["calls"] += 1


def _fixed_place(lat: float, lon: float) -> Optional[str]:
    for place, p in PLACES.items():
        if abs(p["lat"] - lat) <= 0.05 and abs(p["lon"] - lon) <= 0.05:
            return place
    return None


def google_point(lat: float, lon: float) -> dict[str, Any]:
    """Googles timesvarsel for et punkt, i samme form som WeatherNext-API-et.

    Faste steder bruker den felles cachen. Andre steder deles i ruter på
    0,1 grad (ca. 11 x 6 km her til lands) med én time cache per rute, slik at
    flere oppslag i samme område bare koster ett sett kall.
    """
    from scripts.weather_comparison import ForecastError, fetch_google_hours

    place = _fixed_place(lat, lon)
    if place:
        hours = _fetch_google(place)
        cell = (PLACES[place]["lat"], PLACES[place]["lon"])
    else:
        cell = (round(lat, 1), round(lon, 1))
        try:
            hours = _CACHE.get(("google_point", cell), 3300,
                               lambda: fetch_google_hours(cell[0], cell[1], on_call=_count_google_call))
        except ForecastError:
            raise RuntimeError("Google svarte ikke.") from None
    # Dagsvarselet (10 dager, ett kall) er valgfritt: feiler det, vises bare timene.
    try:
        if place:
            dager = _fetch_google_days(place)
        else:
            dager = _google_days_at(cell[0], cell[1], ("google_days_point", cell), on_call=_count_google_call)
    except GoogleQuotaExceeded:
        dager = []
    except Exception:  # noqa: BLE001
        dager = []
    timer = [{"t": h["start"],
              "temp": {"mean": h["temp"]} if h.get("temp") is not None else None,
              "regn": {"mean": h["rain"]} if h.get("rain") is not None else None,
              "vind": {"mean": h["wind"]} if h.get("wind") is not None else None}
             for h in hours]
    return {"kilde": "google-api", "celle": {"lat": cell[0], "lon": cell[1]}, "timer": timer, "dager": dager}


@vaer_varsel.get("/ver/api/google-punkt")
def google_point_api():
    from flask import request

    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)
    if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return jsonify(error="lat/lon mangler"), 400
    try:
        payload = google_point(lat, lon)
    except GoogleQuotaExceeded:
        return jsonify(error="Dagens kvote for Google-oppslag er brukt opp."), 429
    except RuntimeError as exc:
        return jsonify(error=str(exc)), 503
    if not payload["timer"]:
        return jsonify(error="Google returnerte ingen timer."), 503
    response = jsonify(payload)
    # Nettleseren kan gjenbruke svaret en stund, men ikke lenger enn Googles vilkår tillater.
    response.headers["Cache-Control"] = "private, max-age=900"
    return response


@vaer_varsel.get("/ver/api/radar/<sted>.gif")
def radar_gif(sted: str):
    area = RADAR_AREA.get(sted)
    if area is None:
        abort(404)
    try:
        body = _fetch_radar(area)
    except requests.RequestException:
        return Response("Radaren er ikke tilgjengelig akkurat nå.", status=502, mimetype="text/plain")
    return Response(body, mimetype="image/gif", headers={"Cache-Control": "public, max-age=300"})
