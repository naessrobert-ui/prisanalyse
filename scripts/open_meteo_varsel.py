"""Open-Meteo som varselkilde utenfor Norge.

Yr bruker MET Nordic/MEPS (høy oppløsning) i Norden, men faller tilbake på
ECMWF med lite lokal etterbehandling ellers. Utenfor Norge henter vi derfor
Open-Meteo, der `best_match` velger den beste tilgjengelige modellen for punktet.

Svaret konverteres til samme format som Yr sin locationforecast/2.0 `timeseries`,
slik at resten av værsidene kan brukes uendret.

Nedbørsspennet (`precipitation_amount_min/max`) settes til laveste og høyeste
verdi blant best_match, ECMWF IFS og ICON. Stor avstand betyr at modellene er
uenige, og siden viser det allerede som usikkerhetsspenn.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional

import requests

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MODELLER = ("best_match", "ecmwf_ifs025", "icon_seamless")
HOVEDMODELL = "best_match"
VARSEL_DAGER = 10

_TTL = 900.0       # 15 min
_TTL_FEIL = 120.0
_CACHE: dict[tuple, tuple[float, Optional[list]]] = {}
_LOCK = threading.Lock()

# Grov omriss av fastlands-Norge (lon, lat), med litt margin ut i havet.
# Holder for å skille Norge fra utlandet; i grensestrøk er begge kildene gode.
_NORGE = [
    (4.4, 58.0), (7.0, 57.8), (8.6, 58.1), (10.6, 58.8), (11.2, 58.9),
    (11.4, 59.1), (11.8, 59.9), (12.5, 60.4), (12.3, 61.0), (12.9, 61.4),
    (12.1, 61.8), (12.2, 63.0), (12.0, 63.4), (12.8, 64.1), (14.1, 64.5),
    (13.6, 65.1), (14.6, 66.1), (15.5, 66.3), (16.4, 67.0), (16.1, 67.5),
    (17.9, 68.4), (19.9, 68.4), (20.3, 69.0), (21.1, 69.1), (22.4, 68.7),
    (23.7, 68.7), (24.9, 68.6), (25.8, 69.0), (26.3, 69.9), (28.4, 69.8),
    (29.2, 69.6), (29.0, 69.0), (30.9, 69.6), (31.4, 70.4), (28.5, 71.3),
    (25.5, 71.3), (19.0, 70.4), (15.5, 69.4), (12.0, 68.0), (11.5, 66.0),
    (9.5, 64.3), (6.5, 63.0), (4.4, 61.5), (4.3, 59.3),
]
# Svalbard og Jan Mayen som enkle rektangler (lon_min, lat_min, lon_max, lat_max).
_NORSKE_OYER = [(9.0, 76.0, 35.0, 81.0), (-9.5, 70.6, -7.5, 71.3)]


def _i_polygon(lon: float, lat: float, poly: list[tuple[float, float]]) -> bool:
    inni = False
    j = len(poly) - 1
    for i in range(len(poly)):
        xi, yi = poly[i]
        xj, yj = poly[j]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inni = not inni
        j = i
    return inni


def i_norge(lat: float, lon: float) -> bool:
    if any(a <= lon <= c and b <= lat <= d for a, b, c, d in _NORSKE_OYER):
        return True
    return _i_polygon(lon, lat, _NORGE)


def _symbol(kode: Optional[float], er_dag: Optional[float], regn: Optional[float]) -> str:
    """WMO-værkode til Yr sine symbolkoder."""
    k = int(kode) if kode is not None else -1
    dag = (er_dag or 0) >= 1
    sfx = "_day" if dag else "_night"
    r = float(regn or 0.0)
    # Modellen setter ofte en byge-/yrkode selv om timen gir under 0,1 mm.
    # Det ser ut som regn på kartet uten å være det, så vis skyer i stedet.
    if r < 0.1 and k in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82):
        return "partlycloudy" + sfx
    if k == 0:
        return "clearsky" + sfx
    if k == 1:
        return "fair" + sfx
    if k == 2:
        return "partlycloudy" + sfx
    if k in (45, 48):
        return "fog"
    if k in (51, 53, 56):
        return "lightrain"
    if k in (55, 57, 61, 66):
        return "lightrain" if r < 1.0 else "rain"
    if k == 63:
        return "rain"
    if k in (65, 67):
        return "heavyrain"
    if k == 80:
        return "lightrainshowers" + sfx
    if k == 81:
        return "rainshowers" + sfx
    if k == 82:
        return "heavyrainshowers" + sfx
    if k in (71, 77, 85):
        return "lightsnow"
    if k in (73, 86):
        return "snow"
    if k == 75:
        return "heavysnow"
    if k in (95, 96, 99):
        return "rainandthunder"
    if k == 3:
        return "cloudy"
    return "rain" if r >= 0.2 else "cloudy"


_FELT = (
    "temperature_2m", "precipitation", "precipitation_probability",
    "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m",
    "cloud_cover", "weather_code", "is_day", "sunshine_duration",
)


def _verdi(hourly: dict, felt: str, modell: str, i: int) -> Optional[float]:
    # Med flere modeller får hvert felt modellnavnet som suffiks.
    serie = hourly.get(f"{felt}_{modell}")
    if serie is None and modell == HOVEDMODELL:
        serie = hourly.get(felt)
    if not serie or i >= len(serie) or serie[i] is None:
        return None
    try:
        return float(serie[i])
    except (TypeError, ValueError):
        return None


def til_yr_timeserie(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly") or {}
    tider = hourly.get("time") or []
    ut: list[dict[str, Any]] = []
    for i, t in enumerate(tider):
        def v(felt: str, modell: str = HOVEDMODELL) -> Optional[float]:
            return _verdi(hourly, felt, modell, i)

        # Fall tilbake på ECMWF, så ICON, hvis best_match mangler en verdi.
        def hoved(felt: str) -> Optional[float]:
            for m in MODELLER:
                x = v(felt, m)
                if x is not None:
                    return x
            return None

        temp = hoved("temperature_2m")
        if temp is None:
            continue
        regn = hoved("precipitation")
        regn_alle = [x for x in (v("precipitation", m) for m in MODELLER) if x is not None]
        ut.append({
            "time": (t if t.endswith("Z") else t + ":00Z") if len(t) == 16 else t,
            "data": {
                "instant": {"details": {
                    "air_temperature": temp,
                    "wind_speed": hoved("wind_speed_10m"),
                    "wind_speed_of_gust": hoved("wind_gusts_10m"),
                    "wind_from_direction": hoved("wind_direction_10m"),
                    "cloud_area_fraction": hoved("cloud_cover"),
                    # Ikke en del av Yr-formatet: sekunder med direkte sol denne timen.
                    # Gir langt bedre soltimer enn å telle symboler.
                    "sunshine_duration": hoved("sunshine_duration"),
                }},
                "next_1_hours": {
                    "summary": {"symbol_code": _symbol(hoved("weather_code"), hoved("is_day"), regn)},
                    "details": {
                        "precipitation_amount": regn if regn is not None else 0.0,
                        "precipitation_amount_min": min(regn_alle) if regn_alle else None,
                        "precipitation_amount_max": max(regn_alle) if regn_alle else None,
                        "probability_of_precipitation": hoved("precipitation_probability"),
                    },
                },
            },
        })
    return ut


def hent_timeserie(lat: float, lon: float) -> list[dict[str, Any]]:
    """Timesvarsel fra Open-Meteo i Yr-format. Kaster unntak ved feil."""
    nokkel = (round(lat, 3), round(lon, 3))
    naa = time.time()
    with _LOCK:
        treff = _CACHE.get(nokkel)
        if treff and treff[0] > naa and treff[1] is not None:
            return treff[1]

    r = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "hourly": ",".join(_FELT),
            "models": ",".join(MODELLER),
            "timezone": "GMT",
            "wind_speed_unit": "ms",
            "past_days": 1,
            "forecast_days": VARSEL_DAGER,
        },
        timeout=12,
    )
    r.raise_for_status()
    ts = til_yr_timeserie(r.json() or {})
    with _LOCK:
        _CACHE[nokkel] = (naa + (_TTL if ts else _TTL_FEIL), ts)
        if len(_CACHE) > 300:
            for k in [k for k, (exp, _) in _CACHE.items() if exp <= naa]:
                _CACHE.pop(k, None)
    return ts


KILDE_YR = {"id": "yr", "navn": "Yr", "modell": "MET Nordic / MEPS (met.no)"}
KILDE_OM = {"id": "open-meteo", "navn": "Open-Meteo",
            "modell": "best_match, med ECMWF og ICON som spenn"}
