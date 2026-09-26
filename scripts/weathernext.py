#!/usr/bin/env python3
"""Google DeepMind WeatherNext 3 (0.1°) fra Earth Engine.

Brukes på to måter:

* **Faste steder** (`FASTE_STEDER`, i dag bare Bergen): cron-jobben
  `vaer-treffsikkerhet` kaller `collect()` hver time. Siste varsel lagres som JSON
  (rask lesing for nettsiden), hver ny modellkjøring arkiveres med alle 114 bånd,
  og timene 0-48 legges i treffsikkerhetsloggen som leverandør `weathernext`.
* **Andre steder etter behov**: `/ver/api/weathernext` henter direkte fra Earth
  Engine og cacher svaret i én time per rutenett-celle (0.1°, ca. 11 km).

Datasettet har 19 overflatevariabler, hver med mean, p10, p25, p50, p75 og p90
fra et ensemble på 64 medlemmer. Timesoppløsning. 6-timerskjøringene (00, 06,
12, 18 UTC) går 360 timer frem, mellomkjøringene hver time går 48 timer frem.
Data ligger i Earth Engine ca. 8 timer etter kjøringens starttid.

Tidskonvensjon (samme som Yr-radene på aktivt-varsel):

* En time identifiseres av starttiden `t`, altså intervallet [t, t + 1t).
* Øyeblikksverdier (temperatur, vind, skydekke, trykk) er verdien ved `t`,
  det vil si WN3-bildet med gyldighetstid `t`.
* Timesummer (`*_1hr`: nedbør, solinnstråling) gjelder timen som *slutter* ved
  gyldighetstiden, slik ECMWF-akkumuleringer gjør. Timen [t, t + 1t) hentes
  derfor fra bildet med gyldighetstid `t + 1t`.

Miljøvariabler:

* `EE_PROJECT` – GCP-prosjekt registrert for Earth Engine (påkrevd).
* `EE_SERVICE_ACCOUNT_KEY` – innholdet i service-account-JSON-nøkkelen, eller en
  sti til filen. Uten denne brukes lokal `earthengine authenticate`.
* Lagring gjenbruker S3-oppsettet til treffsikkerhetsloggen
  (`S3_BUCKET_NAME`/`WEATHER_SCOREBOARD_S3_BUCKET`), ellers `data/weathernext/`.
"""
from __future__ import annotations

import io
import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

log = logging.getLogger(__name__)

ASSET = "projects/gcp-public-data-weathernext/assets/weathernext_3_0_0_0p1deg"
PIXEL_M = 11132
STATS = ("mean", "p10", "p25", "p50", "p75", "p90")
KILDE = "Google DeepMind WeatherNext 3 (0.1°)"
ATTRIBUSJON = (
    "© 2026 DeepMind Technologies Limited. Eksperimentelle data, ikke validert "
    "for bruk i virkeligheten."
)

#: Siste varsel for et fast sted regnes som ferskt i så lang tid.
_FAST_MAKS_ALDER = timedelta(hours=3)
#: Cache for oppslag etter behov.
_CACHE_TTL = 3600
#: Et oppslag regnes som et fast sted når det er innenfor dette (grader).
_FAST_RADIUS = 0.06
#: Steder (nøkler i `weather_comparison.PLACES`) som hentes hver time av cron.
#: Alle andre, også Kvamskogen, hentes live ved søk og caches i én time.
FASTE_STEDER: tuple[str, ...] = ("bergen",)


def _k(v: float) -> float:
    return v - 273.15


def _mm(v: float) -> float:
    return max(0.0, v * 1000.0)


def _wm2(v: float) -> float:
    return max(0.0, v / 3600.0)


def _hpa(v: float) -> float:
    return v / 100.0


def _pct(v: float) -> float:
    return min(100.0, max(0.0, v * 100.0))


def _id(v: float) -> float:
    return v


#: basisnavn i datasettet -> (feltnavn, konvertering, enhet, er timesum)
VARIABLER: dict[str, tuple[str, Any, str, bool]] = {
    "temperature_2m": ("temp", _k, "°C", False),
    "dewpoint_temperature_2m": ("dugg", _k, "°C", False),
    "wind_speed_10m": ("vind", _id, "m/s", False),
    "wind_speed_100m": ("vind100", _id, "m/s", False),
    "u_component_of_wind_10m": ("u10", _id, "m/s", False),
    "v_component_of_wind_10m": ("v10", _id, "m/s", False),
    "u_component_of_wind_100m": ("u100", _id, "m/s", False),
    "v_component_of_wind_100m": ("v100", _id, "m/s", False),
    "total_cloud_cover": ("sky", _pct, "%", False),
    "low_cloud_cover": ("sky_lav", _pct, "%", False),
    "medium_cloud_cover": ("sky_mid", _pct, "%", False),
    "high_cloud_cover": ("sky_hoy", _pct, "%", False),
    "mean_sea_level_pressure": ("trykk", _hpa, "hPa", False),
    "sea_surface_temperature": ("sjotemp", _k, "°C", False),
    "total_precipitation_1hr": ("regn", _mm, "mm", True),
    "imerg_tp_1hr": ("regn_imerg", _mm, "mm", True),
    "experimental_tp_1hr": ("regn_eksp", _mm, "mm", True),
    "surface_solar_radiation_downwards_1hr": ("sol", _wm2, "W/m²", True),
    "total_sky_direct_solar_radiation_at_surface_1hr": ("sol_direkte", _wm2, "W/m²", True),
}
ENHETER = {navn: enhet for navn, _, enhet, _ in VARIABLER.values()}
ENHETER.update({"vindretning": "grader (fra)", "fukt": "%"})


class WeatherNextError(RuntimeError):
    """Earth Engine er ikke satt opp, eller oppslaget feilet."""


# ---------------------------------------------------------------------------
# Earth Engine
# ---------------------------------------------------------------------------

_EE_LOCK = threading.Lock()
_EE_KLAR = False


def konfigurert() -> bool:
    return bool(os.environ.get("EE_PROJECT", "").strip())


def _init_ee():
    global _EE_KLAR
    try:
        import ee  # lokal import: appen skal starte selv uten pakken
    except ImportError as exc:  # pragma: no cover - avhenger av miljø
        raise WeatherNextError("earthengine-api er ikke installert") from exc
    if _EE_KLAR:
        return ee
    with _EE_LOCK:
        if _EE_KLAR:
            return ee
        prosjekt = os.environ.get("EE_PROJECT", "").strip()
        if not prosjekt:
            raise WeatherNextError("WeatherNext er ikke aktivert (EE_PROJECT mangler).")
        nokkel = os.environ.get("EE_SERVICE_ACCOUNT_KEY", "").strip()
        bruker = os.environ.get("EE_USER_CREDENTIALS", "").strip()
        try:
            if nokkel:
                if not nokkel.lstrip().startswith("{"):
                    nokkel = Path(nokkel).read_text(encoding="utf-8")
                epost = json.loads(nokkel)["client_email"]
                creds = ee.ServiceAccountCredentials(epost, key_data=nokkel)
                ee.Initialize(creds, project=prosjekt)
            elif bruker:
                # Innholdet i ~/.config/earthengine/credentials fra
                # `earthengine authenticate` på egen PC. Serveren logger da inn
                # som deg, og bruker dermed din WeatherNext-tilgang.
                from google.oauth2.credentials import Credentials
                from ee import oauth

                lagret = json.loads(bruker)
                creds = Credentials(
                    None,
                    refresh_token=lagret["refresh_token"],
                    token_uri=oauth.TOKEN_URI,
                    client_id=lagret.get("client_id", oauth.CLIENT_ID),
                    client_secret=lagret.get("client_secret", oauth.CLIENT_SECRET),
                    scopes=lagret.get("scopes", oauth.SCOPES),
                )
                ee.Initialize(creds, project=prosjekt)
            else:
                ee.Initialize(project=prosjekt)
        except Exception as exc:
            raise WeatherNextError(f"Kunne ikke koble til Earth Engine: {exc}") from exc
        _EE_KLAR = True
    return ee


def _start_tid(verdi: str) -> datetime:
    return datetime.fromisoformat(str(verdi).replace("Z", "+00:00")).astimezone(timezone.utc)


def _nyeste_init(ee, vindu, full_lengde: int) -> Optional[str]:
    """Nyeste `start_time` der kjøringen er komplett (siste ledetid er på plass)."""
    liste = (
        vindu.filter(ee.Filter.eq("forecast_hour", full_lengde))
        .aggregate_array("start_time").distinct().getInfo()
    )
    return max(liste, key=_start_tid) if liste else None


def _hent_kjoring(ee, vindu, start_time: str, lat: float, lon: float) -> list[dict[str, Any]]:
    """Alle bånd for én kjøring i ett punkt, i én forespørsel."""
    def med_ledetid(img):
        ledetid = ee.Image.constant(ee.Number(img.get("forecast_hour"))).rename("ledetid").toFloat()
        return img.toFloat().addBands(ledetid)

    bilder = vindu.filter(ee.Filter.eq("start_time", start_time)).map(med_ledetid)
    tabell = bilder.getRegion(ee.Geometry.Point([lon, lat]), PIXEL_M).getInfo()
    return rader_fra_tabell(tabell, int(_start_tid(start_time).timestamp() * 1000))


def rader_fra_tabell(tabell: list[list[Any]], init_ms: int) -> list[dict[str, Any]]:
    """Gjør om `getRegion`-svaret til én rad per ledetid med råverdier."""
    if not tabell:
        return []
    hode, *rader = tabell
    idx = {navn: i for i, navn in enumerate(hode)}
    if "ledetid" not in idx:
        return []
    init = datetime.fromtimestamp(init_ms / 1000, tz=timezone.utc)
    hopp = {"id", "longitude", "latitude", "time", "ledetid"}
    ut: dict[int, dict[str, Any]] = {}
    for rad in rader:
        ledetid = rad[idx["ledetid"]]
        if ledetid is None:
            continue
        ledetid = int(round(ledetid))
        ut[ledetid] = {
            "init": init,
            "ledetid": ledetid,
            "gyldig": init + timedelta(hours=ledetid),
            "baand": {b: rad[i] for b, i in idx.items() if b not in hopp},
        }
    return [ut[k] for k in sorted(ut)]


def hent_kjoringer(lat: float, lon: float, now: Optional[datetime] = None) -> dict[str, Any]:
    """Nyeste mellomkjøring (48 t) og nyeste 6-timerskjøring (360 t) for et punkt.

    Kjøringene identifiseres med egenskapen `start_time` innenfor et datovindu,
    samme oppslag som ble verifisert mot Earth Engine med testskriptet.
    """
    ee = _init_ee()
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    col = ee.ImageCollection(ASSET)
    vindu = col.filterDate(ee.Date(now - timedelta(days=3)), ee.Date(now + timedelta(days=16)))
    try:
        kort_init = _nyeste_init(ee, vindu, 48)
        lang_init = _nyeste_init(ee, vindu, 360)
        if kort_init is None and lang_init is None:
            raise WeatherNextError("Fant ingen komplette WeatherNext-kjøringer siste tre døgn.")
        lang = _hent_kjoring(ee, vindu, lang_init, lat, lon) if lang_init else []
        kort: list[dict[str, Any]] = []
        if kort_init and (lang_init is None or _start_tid(kort_init) > _start_tid(lang_init)):
            kort = _hent_kjoring(ee, vindu, kort_init, lat, lon)
    except WeatherNextError:
        raise
    except Exception as exc:
        raise WeatherNextError(f"Earth Engine-oppslaget feilet: {exc}") from exc
    return {"lang": lang, "kort": kort}


# ---------------------------------------------------------------------------
# Omregning (ren Python, testbar uten Earth Engine)
# ---------------------------------------------------------------------------

def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def vindretning(u: float, v: float) -> float:
    """Meteorologisk retning: hvor vinden kommer fra, i grader."""
    return (270.0 - math.degrees(math.atan2(v, u))) % 360.0


def rel_fukt(t_c: float, td_c: float) -> float:
    a, b = 17.625, 243.04
    return min(100.0, 100.0 * math.exp(a * td_c / (b + td_c) - a * t_c / (b + t_c)))


def slaa_sammen(*kjoringer: Iterable[dict[str, Any]]) -> dict[datetime, dict[str, Any]]:
    """For hver gyldighetstid: bruk nyeste kjøring som dekker den."""
    beste: dict[datetime, dict[str, Any]] = {}
    for kjoring in kjoringer:
        for rad in kjoring:
            gammel = beste.get(rad["gyldig"])
            if gammel is None or rad["init"] > gammel["init"]:
                beste[rad["gyldig"]] = rad
    return beste


def _stats(baand: dict[str, Any], basis: str, fn) -> Optional[dict[str, float]]:
    ut = {}
    for s in STATS:
        v = baand.get(f"{basis}_{s}")
        if v is not None:
            ut[s] = round(fn(float(v)), 2)
    return ut or None


def til_timer(sammenslatt: dict[datetime, dict[str, Any]]) -> list[dict[str, Any]]:
    """Bygg timesrader nøklet på timens start. Se tidskonvensjonen øverst."""
    timer: list[dict[str, Any]] = []
    for t in sorted(sammenslatt):
        inst = sammenslatt[t]
        summ = sammenslatt.get(t + timedelta(hours=1))
        rad: dict[str, Any] = {
            "t": _iso(t),
            "init": _iso(inst["init"]),
            "ledetid": inst["ledetid"],
        }
        for basis, (navn, fn, _, er_sum) in VARIABLER.items():
            kilde = summ if er_sum else inst
            if kilde is None:
                continue
            verdier = _stats(kilde["baand"], basis, fn)
            if verdier:
                rad[navn] = verdier
        u, v = rad.get("u10", {}).get("mean"), rad.get("v10", {}).get("mean")
        if u is not None and v is not None:
            rad["vindretning"] = round(vindretning(u, v))
        t_c, td_c = rad.get("temp", {}).get("mean"), rad.get("dugg", {}).get("mean")
        if t_c is not None and td_c is not None:
            rad["fukt"] = round(rel_fukt(t_c, td_c))
        timer.append(rad)
    return timer


def bygg_varsel(kjoringer: dict[str, Any], lat: float, lon: float,
                now: Optional[datetime] = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lang, kort = kjoringer.get("lang") or [], kjoringer.get("kort") or []
    timer = til_timer(slaa_sammen(lang, kort))
    return {
        "kilde": KILDE,
        "attribusjon": ATTRIBUSJON,
        "lat": lat,
        "lon": lon,
        "celle": {"lat": round(lat, 1), "lon": round(lon, 1)},
        "init_lang": _iso(lang[0]["init"]) if lang else None,
        "init_kort": _iso(kort[0]["init"]) if kort else None,
        "hentet": _iso(now),
        "enheter": ENHETER,
        "timer": timer,
    }


def kompakt(varsel: dict[str, Any], fra: Optional[datetime] = None) -> dict[str, Any]:
    """Mindre utgave til nettsiden: bare det grafene og tabellen bruker."""
    felter = {
        "temp": ("mean", "p10", "p25", "p75", "p90"),
        "regn": ("mean", "p10", "p50", "p90"),
        "vind": ("mean", "p10", "p90"),
        "vind100": ("mean",),
        "sky": ("mean",),
        "sky_lav": ("mean",),
        "sky_mid": ("mean",),
        "sky_hoy": ("mean",),
        "sol": ("mean",),
        "trykk": ("mean",),
        "dugg": ("mean",),
    }
    timer = []
    for rad in varsel.get("timer", []):
        if fra is not None and _parse(rad["t"]) < fra:
            continue
        ny = {"t": rad["t"], "init": rad["init"], "ledetid": rad["ledetid"]}
        for navn, stats in felter.items():
            if navn in rad:
                ny[navn] = {s: rad[navn][s] for s in stats if s in rad[navn]}
        for navn in ("vindretning", "fukt"):
            if navn in rad:
                ny[navn] = rad[navn]
        timer.append(ny)
    return {**{k: v for k, v in varsel.items() if k != "timer"}, "timer": timer}


def scoreboard_rader(varsel: dict[str, Any], place: str, run_hour: datetime,
                     now: datetime) -> list[dict[str, Any]]:
    """Rader i samme format som `weather_scoreboard.snapshot`, leverandør `weathernext`.

    Ledetiden regnes fra kjøretimen i loggen, ikke fra modellens starttid, slik at
    WN3 måles på samme vilkår som Yr og Google: hva lå tilgjengelig da vi spurte.
    Vindkast finnes ikke i datasettet og blir `None`.
    """
    rader = []
    for rad in varsel.get("timer", []):
        start = _parse(rad["t"])
        lead = int((start - run_hour).total_seconds() // 3600)
        if not 0 <= lead <= 48:
            continue
        rader.append({
            "run_hour": _iso(run_hour),
            "fetched_at": _iso(now),
            "place": place,
            "provider": "weathernext",
            "valid_start": _iso(start),
            "lead_hours": lead,
            "temp": (rad.get("temp") or {}).get("mean"),
            "rain": (rad.get("regn") or {}).get("mean"),
            "wind": (rad.get("vind") or {}).get("mean"),
            "gust": None,
            "cloud": (rad.get("sky") or {}).get("mean"),
            "model_updated_at": rad.get("init"),
        })
    return rader


# ---------------------------------------------------------------------------
# Lagring: S3 om konfigurert (samme bøtte som treffsikkerhetsloggen), ellers lokalt
# ---------------------------------------------------------------------------

_DEFAULT_LOCAL_ROOT = Path(__file__).resolve().parents[1] / "data" / "weathernext"


def _s3() -> Optional[tuple[Any, str, str]]:
    from scripts import weather_scoreboard as ws

    cfg = ws._s3_config()
    if cfg is None:
        return None
    bucket, _ = cfg
    prefix = os.environ.get("WEATHERNEXT_S3_PREFIX", "weathernext").strip("/") or "weathernext"
    return ws._s3_client(), bucket, prefix


def _local_root() -> Path:
    override = os.environ.get("WEATHERNEXT_DIR", "").strip()
    return Path(override) if override else _DEFAULT_LOCAL_ROOT


def _skriv(nokkel: str, body: bytes, content_type: str) -> str:
    s3 = _s3()
    if s3 is not None:
        client, bucket, prefix = s3
        client.put_object(Bucket=bucket, Key=f"{prefix}/{nokkel}", Body=body, ContentType=content_type)
        return f"s3://{bucket}/{prefix}/{nokkel}"
    sti = _local_root() / nokkel
    sti.parent.mkdir(parents=True, exist_ok=True)
    sti.write_bytes(body)
    return str(sti)


def _les(nokkel: str) -> Optional[bytes]:
    s3 = _s3()
    if s3 is not None:
        client, bucket, prefix = s3
        try:
            return client.get_object(Bucket=bucket, Key=f"{prefix}/{nokkel}")["Body"].read()
        except Exception:
            return None
    sti = _local_root() / nokkel
    return sti.read_bytes() if sti.exists() else None


def _finnes(nokkel: str) -> bool:
    s3 = _s3()
    if s3 is not None:
        client, bucket, prefix = s3
        try:
            client.head_object(Bucket=bucket, Key=f"{prefix}/{nokkel}")
            return True
        except Exception:
            return False
    return (_local_root() / nokkel).exists()


def lagre_siste(place: str, varsel: dict[str, Any]) -> str:
    body = json.dumps(varsel, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _skriv(f"siste/{place}.json", body, "application/json")


def les_siste(place: str) -> Optional[dict[str, Any]]:
    body = _les(f"siste/{place}.json")
    if not body:
        return None
    try:
        return json.loads(body)
    except ValueError:
        return None


def arkiver_kjoring(place: str, kjoring: list[dict[str, Any]]) -> Optional[str]:
    """Én parquet-fil per sted og kjøring, med alle 114 råbånd. Skrives bare én gang."""
    if not kjoring:
        return None
    import pandas as pd

    init = kjoring[0]["init"]
    nokkel = f"kjoringer/{place}/{init.strftime('%Y-%m-%dT%HZ')}.parquet"
    if _finnes(nokkel):
        return None
    frame = pd.DataFrame([
        {"init": r["init"], "ledetid": r["ledetid"], "gyldig": r["gyldig"], **r["baand"]}
        for r in kjoring
    ])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return _skriv(nokkel, buffer.getvalue(), "application/octet-stream")


# ---------------------------------------------------------------------------
# Cron: faste steder
# ---------------------------------------------------------------------------

def collect(places: Optional[Iterable[str]] = None, now: Optional[datetime] = None) -> dict[str, Any]:
    """Hent, lagre og arkiver WN3 for de faste stedene. Kalles fra cron hver time."""
    from scripts.weather_comparison import PLACES
    from scripts import weather_scoreboard as ws

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_hour = now.replace(minute=0, second=0, microsecond=0)
    resultat: dict[str, Any] = {"steder": {}, "feil": {}, "scoreboard_rader": 0}
    if not konfigurert():
        resultat["feil"]["oppsett"] = "EE_PROJECT mangler, WeatherNext hoppes over"
        return resultat

    alle_rader: list[dict[str, Any]] = []
    for place in (list(places) if places else list(FASTE_STEDER)):
        coords = PLACES[place]
        try:
            kjoringer = hent_kjoringer(coords["lat"], coords["lon"], now=now)
        except WeatherNextError as exc:
            resultat["feil"][place] = str(exc)
            continue
        varsel = bygg_varsel(kjoringer, coords["lat"], coords["lon"], now=now)
        varsel["sted"] = place
        arkivert = [p for p in (arkiver_kjoring(place, kjoringer["lang"]),
                                arkiver_kjoring(place, kjoringer["kort"])) if p]
        resultat["steder"][place] = {
            "init_lang": varsel["init_lang"],
            "init_kort": varsel["init_kort"],
            "timer": len(varsel["timer"]),
            "siste": lagre_siste(place, varsel),
            "arkivert": arkivert,
        }
        alle_rader.extend(scoreboard_rader(varsel, place, run_hour, now))

    if alle_rader:
        import pandas as pd

        frame = pd.DataFrame(alle_rader, columns=ws.FORECAST_COLUMNS)
        frame["_day"] = run_hour.strftime("%Y-%m-%d")
        ws._append(ws._FORECASTS, frame, ["run_hour", "place", "provider", "valid_start"])
        resultat["scoreboard_rader"] = len(frame)
    return resultat


# ---------------------------------------------------------------------------
# Nettsiden: fast sted fra lager, ellers live med cache
# ---------------------------------------------------------------------------

_CACHE: dict[tuple[float, float], tuple[float, dict[str, Any]]] = {}
_CACHE_LOCK = threading.Lock()
_CELLE_LAASER: dict[tuple[float, float], threading.Lock] = {}


def _fast_sted(lat: float, lon: float) -> Optional[str]:
    from scripts.weather_comparison import PLACES

    for place in FASTE_STEDER:
        c = PLACES[place]
        if abs(c["lat"] - lat) <= _FAST_RADIUS and abs(c["lon"] - lon) <= _FAST_RADIUS:
            return place
    return None


def for_punkt(lat: float, lon: float, now: Optional[datetime] = None) -> dict[str, Any]:
    """Varsel for et punkt, raskest mulig. Kaster `WeatherNextError` ved feil."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    place = _fast_sted(lat, lon)
    if place:
        lagret = les_siste(place)
        if lagret and now - _parse(lagret["hentet"]) <= _FAST_MAKS_ALDER:
            return {**lagret, "modus": "fast"}

    if not konfigurert():
        raise WeatherNextError("WeatherNext er ikke aktivert på serveren.")

    celle = (round(lat, 1), round(lon, 1))
    with _CACHE_LOCK:
        treff = _CACHE.get(celle)
        if treff and time.time() - treff[0] < _CACHE_TTL:
            return {**treff[1], "modus": "cache"}
        laas = _CELLE_LAASER.setdefault(celle, threading.Lock())
    # Én henting per celle om gangen; de andre venter og får cachen.
    with laas:
        with _CACHE_LOCK:
            treff = _CACHE.get(celle)
            if treff and time.time() - treff[0] < _CACHE_TTL:
                return {**treff[1], "modus": "cache"}
        varsel = bygg_varsel(hent_kjoringer(celle[0], celle[1], now=now), celle[0], celle[1], now=now)
        with _CACHE_LOCK:
            _CACHE[celle] = (time.time(), varsel)
            for gammel in [k for k, (ts, _) in _CACHE.items() if time.time() - ts > _CACHE_TTL]:
                _CACHE.pop(gammel, None)
    return {**varsel, "modus": "live"}
