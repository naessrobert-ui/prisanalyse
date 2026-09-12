"""Felles, kortvarig sammenligning av punktvarsler fra MET og Google."""

import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, jsonify, make_response, render_template, request


weather_comparison = Blueprint("weather_comparison", __name__)
PLACES = {
    "kvamskogen": {"name": "Kvamskogen", "lat": 60.3783, "lon": 5.9796},
    "bergen": {"name": "Bergen sentrum", "lat": 60.3930, "lon": 5.3242},
}
_CACHE = {}
_LOCKS = {(p, s): threading.Lock() for p in PLACES for s in ("yr", "google")}
_TTL = 3300  # Googles timevarsel skal slettes innen én time, også uten nye kall.


def _iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _date(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Tidssone mangler")
    return result.astimezone(timezone.utc)


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _quantity(data, field, factors):
    data = data or {}
    value = _number(data.get(field))
    factor = factors.get(data.get("unit"))
    return value * factor if value is not None and factor is not None else None


def normalize_google(rows):
    result = []
    for row in rows:
        interval = row.get("interval") or {}
        try:
            start, end = _date(interval["startTime"]), _date(interval["endTime"])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        if end - start != timedelta(hours=1):
            continue
        wind, precip = row.get("wind") or {}, row.get("precipitation") or {}
        temperature = row.get("temperature") or {}
        temp = _number(temperature.get("degrees"))
        if temperature.get("unit") == "FAHRENHEIT" and temp is not None:
            temp = (temp - 32) * 5 / 9
        elif temperature.get("unit") != "CELSIUS":
            temp = None
        result.append({
            "start": _iso(start), "end": _iso(end), "temp": temp,
            "rain": _quantity(precip.get("qpf"), "quantity", {"MILLIMETERS": 1, "INCHES": 25.4}),
            "wind": _quantity(wind.get("speed"), "value", {"KILOMETERS_PER_HOUR": 1 / 3.6, "MILES_PER_HOUR": 0.44704}),
            "gust": _quantity(wind.get("gust"), "value", {"KILOMETERS_PER_HOUR": 1 / 3.6, "MILES_PER_HOUR": 0.44704}),
            "cloud": _number(row.get("cloudCover")),
        })
    return result


def normalize_yr(payload):
    result = []
    for row in payload.get("properties", {}).get("timeseries", []):
        try:
            start = _date(row["time"])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        data = row.get("data") or {}
        instant = (data.get("instant") or {}).get("details") or {}
        # Sekstimersnedbør kan ikke fordeles jevnt uten å finne på timedata.
        hourly = (data.get("next_1_hours") or {}).get("details") or {}
        result.append({
            "start": _iso(start), "end": _iso(start + timedelta(hours=1)),
            "temp": _number(instant.get("air_temperature")),
            "rain": _number(hourly.get("precipitation_amount")),
            "wind": _number(instant.get("wind_speed")),
            "gust": _number(instant.get("wind_speed_of_gust")),
            "cloud": _number(instant.get("cloud_area_fraction")),
        })
    return result


class ForecastError(Exception):
    pass


def _get_json(url, **kwargs):
    try:
        response = requests.get(url, timeout=(4, 12), **kwargs)
        if response.status_code != 200:
            # Ikke send Googles feilmelding eller URL med API-nøkkel videre.
            raise ForecastError(f"Værleverandøren svarte HTTP {response.status_code}.")
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Ugyldig svar")
        return payload
    except (requests.RequestException, ValueError):
        raise ForecastError("Kunne ikke hente et gyldig værvarsel. Prøv igjen senere.") from None


def _fetch(place, provider):
    coords = PLACES[place]
    fetched = datetime.now(timezone.utc)
    updated = None
    if provider == "google":
        key = os.environ.get("GOOGLE_WEATHER_API_KEY", "").strip()
        if not key:
            raise ForecastError("Google-varselet er ikke aktivert på serveren ennå.")
        params = {
            "location.latitude": coords["lat"], "location.longitude": coords["lon"],
            "hours": 48, "pageSize": 24, "unitsSystem": "METRIC", "key": key,
        }
        rows = []
        for page in range(2):
            payload = _get_json("https://weather.googleapis.com/v1/forecast/hours:lookup", params=params)
            rows.extend(payload.get("forecastHours") or [])
            token = payload.get("nextPageToken")
            if not token:
                break
            if page == 1:
                raise ForecastError("Google returnerte et ufullstendig timevarsel.")
            params["pageToken"] = token
        rows = normalize_google(rows)
    else:
        payload = _get_json(
            "https://api.met.no/weatherapi/locationforecast/2.0/complete",
            params={"lat": coords["lat"], "lon": coords["lon"]},
            headers={"User-Agent": "prisanalyse.no-vaersammenligning/1.0 kontakt@prisanalyse.no"},
        )
        updated = payload.get("properties", {}).get("meta", {}).get("updated_at")
        rows = normalize_yr(payload)
    if not rows:
        raise ForecastError("Værleverandøren returnerte ingen timedata.")
    return {"hours": rows, "fetched_at": _iso(fetched), "updated_at": updated,
            "expires_at": _iso(fetched + timedelta(seconds=_TTL)), "error": None}


def fetch_forecast(place, provider):
    """Ferskt varsel utenom prosesscachen. Brukes av treffsikkerhetsloggen.

    Timeloggen må hente på nytt hver time; et cachet svar ville blitt lagret som
    om det var et nytt varsel. Normalisering og enheter er de samme som siden.
    """
    if place not in PLACES or provider not in ("yr", "google"):
        raise ForecastError("Ukjent sted eller leverandør.")
    return _fetch(place, provider)


def _expire(key, entry):
    with _LOCKS[key]:
        if _CACHE.get(key) is entry:
            _CACHE.pop(key, None)


def _provider(place, provider):
    key = place, provider
    with _LOCKS[key]:
        cached = _CACHE.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        _CACHE.pop(key, None)
        started = time.monotonic()
        try:
            payload = _fetch(place, provider)
            ttl = max(1, _TTL - (time.monotonic() - started))
        except ForecastError as exc:
            ttl = 60
            payload = {"hours": [], "error": str(exc), "fetched_at": None,
                       "updated_at": None, "expires_at": _iso(datetime.now(timezone.utc) + timedelta(seconds=ttl))}
        entry = (time.monotonic() + ttl, payload)
        _CACHE[key] = entry
        timer = threading.Timer(ttl, _expire, args=(key, entry))
        timer.daemon = True
        timer.start()
        return payload


def aligned_hours(providers, start):
    lookups = {name: {(h["start"], h["end"]): h for h in data["hours"]}
               for name, data in providers.items()}
    rows = []
    for offset in range(48):
        begin = start + timedelta(hours=offset)
        key = _iso(begin), _iso(begin + timedelta(hours=1))
        rows.append({"start": key[0], "end": key[1],
                     **{name: lookup.get(key) for name, lookup in lookups.items()}})
    return rows


@weather_comparison.get("/ver/sammenlign")
def comparison_page():
    response = make_response(render_template("ver/weather_comparison.html"))
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' https://visitkvamskogen.no https://www.visitkvamskogen.no "
        "https://visitkvamskogen.onrender.com"
    )
    return response


@weather_comparison.get("/ver/api/sammenlign")
def comparison_api():
    place = request.args.get("sted", "kvamskogen")
    if place not in PLACES:
        return jsonify(error="Velg bergen eller kvamskogen."), 400
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {name: executor.submit(_provider, place, name) for name in ("yr", "google")}
        providers = {name: future.result() for name, future in futures.items()}
    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0)
    result = {
        "place": {"id": place, **PLACES[place]},
        "hours": aligned_hours(providers, start),
        "providers": {name: {k: v for k, v in data.items() if k != "hours"} for name, data in providers.items()},
        "expires_at": min(data["expires_at"] for data in providers.values()),
    }
    response = jsonify(result)
    response.headers["Cache-Control"] = "no-store"
    return response
