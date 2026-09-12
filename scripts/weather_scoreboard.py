#!/usr/bin/env python3
"""Fasit for /ver/sammenlign: lagrer Yr- og Google-varsler og scorer dem mot Frost.

Sammenligningssiden viser bare hva de to leverandørene tror akkurat nå. Dette
modulet svarer på det neste spørsmålet: hvem traff? En gang i timen lagres begge
varslene for de neste 48 timene. Når timen er over hentes den faktiske
observasjonen fra Frost, og prognosene scores mot den.

Tidskonvensjoner (viktig – feil her ødelegger scoren i stillhet):

* Varselstimen identifiseres av `valid_start`, altså intervallet
  [valid_start, valid_start + 1t) i UTC. Samme nøkkel som `/ver/api/sammenlign`.
* Frost `air_temperature` og `wind_speed` er øyeblikksverdier *ved* referansetiden.
  De pares mot `valid_start`.
* Frost `sum(precipitation_amount PT1H)` og `max(wind_speed_of_gust PT1H)` gjelder
  timen som *slutter* ved referansetiden. Verifisert mot
  `accumulated(precipitation_amount)`: sum(T) == akkumulert(T) - akkumulert(T-1t).
  De pares derfor mot `valid_start + 1t`.

Observasjonene lagres rå (én rad per element og referansetid) slik at en senere
endring i konvensjon kan regnes om uten å hente Frost på nytt.
"""
from __future__ import annotations

import io
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
import requests

from scripts.weather_comparison import PLACES, ForecastError, fetch_forecast


# ---------------------------------------------------------------------------
# Oppsett
# ---------------------------------------------------------------------------

#: Frost-stasjonen som brukes som fasit for hvert sted i `PLACES`.
#: Begge leverer air_temperature, wind_speed, sum(precipitation_amount PT1H) og
#: max(wind_speed_of_gust PT1H) på timesoppløsning.
STATIONS: dict[str, dict[str, Any]] = {
    "kvamskogen": {"source_id": "SN50310", "name": "Kvamskogen - Jonshøgdi", "km": 1.5},
    "bergen": {"source_id": "SN50540", "name": "Bergen - Florida", "km": 1.2},
}

PROVIDERS = ("yr", "google")

#: Elementer som scores. `obs` peker på nøkkelen i observasjonstabellen,
#: `shift` er antall timer referansetiden ligger etter `valid_start`.
#: `instant` betyr at Yr oppgir en øyeblikksverdi ved timens start mens Google
#: oppgir en verdi for hele timen. Da finnes det ingen nøytral fasit, og
#: `BASES` under gjør valget synlig i stedet for å skjule det.
ELEMENTS: dict[str, dict[str, Any]] = {
    "temp": {"label": "Temperatur", "unit": "°C", "frost": "air_temperature",
             "shift": 0, "instant": True},
    "rain": {"label": "Nedbør", "unit": "mm", "frost": "sum(precipitation_amount PT1H)",
             "shift": 1, "instant": False},
    "wind": {"label": "Vind", "unit": "m/s", "frost": "wind_speed",
             "shift": 0, "instant": True},
    "gust": {"label": "Vindkast", "unit": "m/s", "frost": "max(wind_speed_of_gust PT1H)",
             "shift": 1, "instant": False},
}

#: Fasitgrunnlag for `instant`-elementene.
#: * `instant`  – observasjonen ved timens start. Yrs egen konvensjon.
#: * `interval` – snittet av observasjonen ved start og slutt. Googles konvensjon.
#: Forskjellen er i snitt ~0,3 °C og ~0,4 m/s, altså stor nok til å avgjøre et
#: jevnt oppgjør. Kjør begge: snur svaret, er forskjellen for liten til å telle.
BASES = ("instant", "interval")

#: Skyer scores ikke: Frost har bare `cloud_area_fraction` på PT6H i Bergen og
#: ingenting på Kvamskogen, så det finnes ingen timesfasit å måle mot.

FROST_ELEMENTS = ",".join(spec["frost"] for spec in ELEMENTS.values())

#: Lead-tid i timer, regnet fra kjøretimen. Bøttene rapporteres hver for seg
#: fordi et varsel 2 timer fram og 40 timer fram ikke er samme oppgave.
LEAD_BUCKETS: tuple[tuple[str, int, int], ...] = (
    ("1-6t", 1, 6),
    ("7-12t", 7, 12),
    ("13-24t", 13, 24),
    ("25-48t", 25, 48),
)

#: Terskel for "det regnet" i den kategoriske nedbørscoren.
RAIN_THRESHOLD_MM = 0.1

_FROST_BASE = "https://frost.met.no"
_FROST_TIMEOUT = 40
_DEFAULT_LOCAL_ROOT = Path(__file__).resolve().parents[1] / "data" / "vaer_fasit"
_DEFAULT_S3_PREFIX = "weather-scoreboard"
_FORECASTS = "prognoser"
_OBSERVATIONS = "observasjoner"

FORECAST_COLUMNS = [
    "run_hour", "fetched_at", "place", "provider", "valid_start", "lead_hours",
    "temp", "rain", "wind", "gust", "cloud", "model_updated_at",
]
OBSERVATION_COLUMNS = [
    "place", "source_id", "reference_time", "element", "value", "quality", "fetched_at",
]


# ---------------------------------------------------------------------------
# Små hjelpere
# ---------------------------------------------------------------------------

def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if result.tzinfo is None:
        return None
    return result.astimezone(timezone.utc)


def _floor_hour(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _places(places: Optional[Iterable[str]] = None) -> list[str]:
    if places is None:
        return [p for p in PLACES if p in STATIONS]
    unknown = [p for p in places if p not in STATIONS]
    if unknown:
        raise ValueError(f"Ukjent sted: {', '.join(unknown)}")
    return list(places)


# ---------------------------------------------------------------------------
# Lagring: én parquet-fil per døgn, på S3 om konfigurert ellers lokalt
# ---------------------------------------------------------------------------

def _local_root() -> Path:
    override = os.environ.get("WEATHER_SCOREBOARD_DIR", "").strip()
    return Path(override) if override else _DEFAULT_LOCAL_ROOT


def _s3_config() -> Optional[tuple[str, str]]:
    bucket = (
        os.environ.get("WEATHER_SCOREBOARD_S3_BUCKET", "").strip()
        or os.environ.get("S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        return None
    prefix = os.environ.get("WEATHER_SCOREBOARD_S3_PREFIX", _DEFAULT_S3_PREFIX).strip()
    return bucket, (prefix or _DEFAULT_S3_PREFIX).strip("/")


def storage_location() -> str:
    """Hvor loggen faktisk leser og skriver. Vises i rapporten.

    Uten `S3_BUCKET_NAME` faller lagringen tilbake til en lokal mappe. Kjører du
    rapporten et sted uten den variabelen, leser du en tom mappe og ikke Renders
    historikk – og en tom rapport ser ut som «ingen forskjell».
    """
    s3 = _s3_config()
    if s3 is not None:
        return "s3://{0}/{1}".format(*s3)
    return str(_local_root())


def _s3_client():
    import boto3  # lokal import holder boto3 valgfri utenfor Render

    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")),
    )


def _to_parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _read_day(kind: str, day: str) -> Optional[pd.DataFrame]:
    s3 = _s3_config()
    if s3 is not None:
        bucket, prefix = s3
        try:
            obj = _s3_client().get_object(Bucket=bucket, Key=f"{prefix}/{kind}/{day}.parquet")
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        except Exception:
            return None
    path = _local_root() / kind / f"{day}.parquet"
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _write_day(kind: str, day: str, frame: pd.DataFrame) -> str:
    body = _to_parquet_bytes(frame)
    s3 = _s3_config()
    if s3 is not None:
        bucket, prefix = s3
        key = f"{prefix}/{kind}/{day}.parquet"
        _s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")
        return f"s3://{bucket}/{key}"
    path = _local_root() / kind / f"{day}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return str(path)


def _append(kind: str, frame: pd.DataFrame, key_columns: list[str]) -> list[str]:
    """Slå sammen nye rader med døgnfilene de hører hjemme i. Nyeste rad vinner.

    Frost retter observasjoner i ettertid, og en time kan kjøres om igjen. Derfor
    er skrivingen idempotent: samme nøkkel erstattes i stedet for å dupliseres.
    """
    if frame.empty:
        return []
    written: list[str] = []
    for day, chunk in frame.groupby("_day", sort=True):
        chunk = chunk.drop(columns="_day")
        existing = _read_day(kind, str(day))
        if existing is not None and not existing.empty:
            existing = existing.reindex(columns=chunk.columns)
            chunk = pd.concat([existing, chunk], ignore_index=True)
        chunk = chunk.drop_duplicates(subset=key_columns, keep="last")
        chunk = chunk.sort_values(key_columns, ignore_index=True)
        written.append(_write_day(kind, str(day), chunk))
    return written


def _load(kind: str, start: datetime, end: datetime, columns: list[str]) -> pd.DataFrame:
    days = pd.date_range(start.date(), end.date(), freq="D")
    frames = [f for f in (_read_day(kind, d.strftime("%Y-%m-%d")) for d in days) if f is not None]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Innsamling: prognoser
# ---------------------------------------------------------------------------

def snapshot(places: Optional[Iterable[str]] = None, now: Optional[datetime] = None) -> dict[str, Any]:
    """Hent begge varslene for hvert sted og legg dem i døgnfilen for kjøretimen."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_hour = _floor_hour(now)
    rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for place in _places(places):
        for provider in PROVIDERS:
            try:
                payload = fetch_forecast(place, provider)
            except ForecastError as exc:
                errors[f"{place}/{provider}"] = str(exc)
                continue
            for hour in payload["hours"]:
                start = _parse(hour.get("start"))
                end = _parse(hour.get("end"))
                if start is None or end is None or end - start != timedelta(hours=1):
                    continue
                lead = int((start - run_hour).total_seconds() // 3600)
                if not 0 <= lead <= 48:
                    continue
                rows.append({
                    "run_hour": _iso(run_hour),
                    "fetched_at": payload.get("fetched_at") or _iso(now),
                    "place": place,
                    "provider": provider,
                    "valid_start": _iso(start),
                    "lead_hours": lead,
                    "temp": _as_float(hour.get("temp")),
                    "rain": _as_float(hour.get("rain")),
                    "wind": _as_float(hour.get("wind")),
                    "gust": _as_float(hour.get("gust")),
                    "cloud": _as_float(hour.get("cloud")),
                    "model_updated_at": payload.get("updated_at"),
                })
    frame = pd.DataFrame(rows, columns=FORECAST_COLUMNS)
    written: list[str] = []
    if not frame.empty:
        frame["_day"] = run_hour.strftime("%Y-%m-%d")
        written = _append(_FORECASTS, frame, ["run_hour", "place", "provider", "valid_start"])
    return {
        "run_hour": _iso(run_hour),
        "rows": len(frame),
        "written": written,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Innsamling: observasjoner
# ---------------------------------------------------------------------------

def _frost_session() -> requests.Session:
    client_id = os.environ.get("FROST_CLIENT_ID", "").strip()
    if not client_id:
        raise ForecastError("FROST_CLIENT_ID mangler i miljøet.")
    session = requests.Session()
    session.auth = (client_id, os.environ.get("FROST_CLIENT_SECRET", ""))
    return session


def fetch_observations(place: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Hent timesobservasjoner for stedets Frost-stasjon i [start, end]."""
    station = STATIONS[place]
    params = {
        "sources": station["source_id"],
        "referencetime": f"{_iso(start)}/{_iso(end)}",
        "elements": FROST_ELEMENTS,
        "timeoffsets": "default",
        "levels": "default",
        "limit": 10000,
        "qualities": "0,1,2,3,4",
    }
    try:
        response = _frost_session().get(f"{_FROST_BASE}/observations/v0.jsonld", params=params, timeout=_FROST_TIMEOUT)
    except requests.RequestException:
        raise ForecastError("Fikk ikke kontakt med Frost.") from None
    if response.status_code == 404:
        return []  # Frost svarer 404 når perioden ikke har data ennå.
    if response.status_code != 200:
        raise ForecastError(f"Frost svarte HTTP {response.status_code}.")
    try:
        data = response.json().get("data", [])
    except ValueError:
        raise ForecastError("Ugyldig svar fra Frost.") from None

    fetched = _iso(datetime.now(timezone.utc))
    rows: list[dict[str, Any]] = []
    for item in data:
        reference = _parse(item.get("referenceTime"))
        if reference is None or reference.minute or reference.second:
            continue
        for observation in item.get("observations", []):
            # SN50310 leverer wind_speed både som PT10M og PT1H. Bare timesserien
            # kan pares med et timesvarsel.
            if observation.get("timeResolution") != "PT1H":
                continue
            element = observation.get("elementId")
            value = _as_float(observation.get("value"))
            if element not in {spec["frost"] for spec in ELEMENTS.values()} or value is None:
                continue
            rows.append({
                "place": place,
                "source_id": item.get("sourceId", station["source_id"]).split(":")[0],
                "reference_time": _iso(reference),
                "element": element,
                "value": value,
                "quality": observation.get("qualityCode"),
                "fetched_at": fetched,
            })
    return rows


def collect_observations(
    places: Optional[Iterable[str]] = None,
    hours_back: int = 12,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Hent de siste timene på nytt og skriv dem over eventuelle eldre verdier.

    Vinduet er bevisst større enn én time: Frost kan levere en time forsinket og
    kvalitetskontrollerer verdier i ettertid.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    end = _floor_hour(now)
    start = end - timedelta(hours=max(1, hours_back))
    rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for place in _places(places):
        try:
            rows.extend(fetch_observations(place, start, end))
        except ForecastError as exc:
            errors[place] = str(exc)
    frame = pd.DataFrame(rows, columns=OBSERVATION_COLUMNS)
    written: list[str] = []
    if not frame.empty:
        frame["_day"] = frame["reference_time"].str.slice(0, 10)
        written = _append(_OBSERVATIONS, frame, ["place", "reference_time", "element"])
    return {
        "window": [_iso(start), _iso(end)],
        "rows": len(frame),
        "written": written,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def load_forecasts(start: datetime, end: datetime) -> pd.DataFrame:
    frame = _load(_FORECASTS, start - timedelta(days=3), end, FORECAST_COLUMNS)
    if frame.empty:
        return frame
    frame["valid_start"] = pd.to_datetime(frame["valid_start"], utc=True, errors="coerce")
    frame["run_hour"] = pd.to_datetime(frame["run_hour"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["valid_start", "run_hour"])
    return frame[(frame["valid_start"] >= start) & (frame["valid_start"] < end)]


def load_observations(start: datetime, end: datetime) -> pd.DataFrame:
    # Nedbør og vindkast for timen som starter i `end - 1t` har referansetid `end`.
    frame = _load(_OBSERVATIONS, start, end + timedelta(days=1), OBSERVATION_COLUMNS)
    if frame.empty:
        return frame
    frame["reference_time"] = pd.to_datetime(frame["reference_time"], utc=True, errors="coerce")
    return frame.dropna(subset=["reference_time"])


def observation_table(observations: pd.DataFrame, basis: str = "instant") -> pd.DataFrame:
    """Gjør rå Frost-rader om til én rad per (sted, varselstime) med fasitverdier.

    Her – og bare her – anvendes tidskonvensjonen fra modul-docstringen.
    `basis="interval"` erstatter øyeblikksverdiene med snittet av timens start og
    slutt. Timer der neste observasjon mangler faller ut i stedet for å bli gjettet.
    """
    if basis not in BASES:
        raise ValueError(f"Ukjent fasitgrunnlag: {basis}")
    columns = ["place", "valid_start", *ELEMENTS]
    if observations.empty:
        return pd.DataFrame(columns=columns)
    parts: list[pd.DataFrame] = []
    for name, spec in ELEMENTS.items():
        subset = observations[observations["element"] == spec["frost"]]
        if subset.empty:
            continue
        subset = subset.drop_duplicates(subset=["place", "reference_time"], keep="last")
        part = pd.DataFrame({
            "place": subset["place"].to_numpy(),
            "valid_start": subset["reference_time"] - pd.Timedelta(hours=spec["shift"]),
            name: pd.to_numeric(subset["value"], errors="coerce").to_numpy(),
        })
        if spec["instant"] and basis == "interval":
            part = part.sort_values(["place", "valid_start"], ignore_index=True)
            grouped = part.groupby("place", sort=False)
            adjacent = grouped["valid_start"].shift(-1) - part["valid_start"] == pd.Timedelta(hours=1)
            part[name] = (part[name] + grouped[name].shift(-1)).where(adjacent) / 2
        parts.append(part)
    if not parts:
        return pd.DataFrame(columns=columns)
    table = parts[0]
    for part in parts[1:]:
        table = table.merge(part, on=["place", "valid_start"], how="outer")
    return table.reindex(columns=columns)


def paired(start: datetime, end: datetime, basis: str = "instant",
           forecast_shift: int = 0) -> pd.DataFrame:
    """Bygg den parede tabellen: én rad per varsel, element og fasitverdi.

    Bare timer der *begge* leverandørene har en verdi og fasiten finnes blir med.
    Uten det kravet ville en leverandør kunne vinne på å la være å svare når
    været er vanskelig.
    """
    columns = ["place", "valid_start", "run_hour", "lead_hours", "element",
               "observed", "yr", "google", "err_yr", "err_google", "day"]
    forecasts = load_forecasts(start, end)
    if forecasts.empty:
        return pd.DataFrame(columns=columns)
    truth = observation_table(load_observations(start, end), basis=basis)
    if truth.empty:
        return pd.DataFrame(columns=columns)

    rows: list[pd.DataFrame] = []
    for name in ELEMENTS:
        wide = forecasts.pivot_table(
            index=["place", "valid_start", "run_hour", "lead_hours"],
            columns="provider", values=name, aggfunc="last",
        )
        missing = [p for p in PROVIDERS if p not in wide.columns]
        if missing:
            continue
        wide = wide.dropna(subset=list(PROVIDERS)).reset_index()
        if forecast_shift:
            # Brukes bare av `lag_scan`: lat som varselet gjaldt en annen time,
            # for å se om en leverandør er systematisk forskjøvet.
            wide["valid_start"] = wide["valid_start"] + pd.Timedelta(hours=forecast_shift)
        part = wide.merge(truth[["place", "valid_start", name]], on=["place", "valid_start"], how="inner")
        part = part.rename(columns={name: "observed"}).dropna(subset=["observed"])
        if part.empty:
            continue
        part["element"] = name
        part["err_yr"] = part["yr"] - part["observed"]
        part["err_google"] = part["google"] - part["observed"]
        rows.append(part.reindex(columns=[c for c in columns if c != "day"]))
    if not rows:
        return pd.DataFrame(columns=columns)
    pairs = pd.concat(rows, ignore_index=True)
    pairs["day"] = pairs["valid_start"].dt.strftime("%Y-%m-%d")
    return pairs


def _bucket(lead: pd.Series) -> pd.Series:
    labels = pd.Series(pd.NA, index=lead.index, dtype="object")
    for name, low, high in LEAD_BUCKETS:
        labels = labels.mask(lead.between(low, high), name)
    return labels


def _block_bootstrap_ci(
    frame: pd.DataFrame, column: str, *, samples: int = 2000, seed: int = 20
) -> tuple[Optional[float], Optional[float]]:
    """95 %-intervall for gjennomsnittet, med hele døgn som blokker.

    Feilen én time er sterkt korrelert med feilen neste time, så et vanlig
    konfidensintervall ville vært altfor smalt. Blokkbootstrap over døgn gir et
    ærligere bilde av hvor mye datagrunnlaget faktisk sier.
    """
    blocks = frame.groupby("day", sort=True)[column].agg(["sum", "count"])
    blocks = blocks[blocks["count"] > 0]
    if len(blocks) < 3:
        return None, None
    # Snittet av trukne blokker er sum av blokksummer delt på sum av blokklengder,
    # så hele bootstrappen kan gjøres uten å sette sammen verdiene på nytt.
    totals = blocks["sum"].to_numpy(dtype=float)
    counts = blocks["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(blocks), size=(samples, len(blocks)))
    means = totals[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


@dataclass(frozen=True)
class Verdict:
    winner: Optional[str]
    diff: float          # snitt(|feil google|) - snitt(|feil yr|); negativ = Google bedre
    low: Optional[float]
    high: Optional[float]

    @property
    def text(self) -> str:
        if self.winner is None:
            return "uavgjort"
        return f"{'Yr' if self.winner == 'yr' else 'Google'} best"


def head_to_head(frame: pd.DataFrame) -> Verdict:
    """Avgjør hvem som har minst gjennomsnittlig avvik, med usikkerhet."""
    diff = frame["err_google"].abs() - frame["err_yr"].abs()
    work = frame.assign(_diff=diff)
    mean = float(diff.mean())
    low, high = _block_bootstrap_ci(work, "_diff")
    winner: Optional[str] = None
    if low is not None and high is not None and low > 0:
        winner = "yr"
    elif low is not None and high is not None and high < 0:
        winner = "google"
    return Verdict(winner=winner, diff=mean, low=low, high=high)


def _metrics(frame: pd.DataFrame, provider: str) -> dict[str, float]:
    error = frame[f"err_{provider}"].to_numpy(dtype=float)
    return {
        "mae": float(np.abs(error).mean()),
        "bias": float(error.mean()),
        "rmse": float(np.sqrt((error ** 2).mean())),
    }


def score(pairs: pd.DataFrame) -> pd.DataFrame:
    """Ett sammendrag per sted, element og lead-bøtte."""
    columns = ["place", "element", "bucket", "n", "yr_mae", "google_mae", "yr_bias",
               "google_bias", "yr_rmse", "google_rmse", "yr_win_rate", "diff",
               "diff_low", "diff_high", "winner"]
    if pairs.empty:
        return pd.DataFrame(columns=columns)
    work = pairs.assign(bucket=_bucket(pairs["lead_hours"])).dropna(subset=["bucket"])
    rows: list[dict[str, Any]] = []
    order = {name: i for i, (name, _, _) in enumerate(LEAD_BUCKETS)}
    elements = {name: i for i, name in enumerate(ELEMENTS)}
    groups = sorted(work.groupby(["place", "element"], sort=False),
                    key=lambda item: (item[0][0], elements.get(item[0][1], 99)))
    for (place, element), by_place in groups:
        buckets = [(name, group) for name, group in by_place.groupby("bucket", sort=False)]
        buckets.sort(key=lambda item: order.get(item[0], 99))
        for name, group in [*buckets, ("alle", by_place)]:
            verdict = head_to_head(group)
            yr, google = _metrics(group, "yr"), _metrics(group, "google")
            better = group["err_yr"].abs() < group["err_google"].abs()
            rows.append({
                "place": place, "element": element, "bucket": name, "n": int(len(group)),
                "yr_mae": yr["mae"], "google_mae": google["mae"],
                "yr_bias": yr["bias"], "google_bias": google["bias"],
                "yr_rmse": yr["rmse"], "google_rmse": google["rmse"],
                "yr_win_rate": float(better.mean()),
                "diff": verdict.diff, "diff_low": verdict.low, "diff_high": verdict.high,
                "winner": verdict.winner,
            })
    return pd.DataFrame(rows, columns=columns)


def rain_skill(pairs: pd.DataFrame, threshold: float = RAIN_THRESHOLD_MM) -> pd.DataFrame:
    """Traff de på *om* det regnet? Millimeteravvik alene skjuler dette.

    POD er andelen nedbørstimer som ble varslet, FAR andelen varslede
    nedbørstimer som ble tørre.
    """
    columns = ["place", "provider", "n", "wet_hours", "accuracy", "pod", "far"]
    rain = pairs[pairs["element"] == "rain"] if not pairs.empty else pairs
    if rain.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for place, group in rain.groupby("place", sort=True):
        observed = group["observed"] >= threshold
        for provider in PROVIDERS:
            predicted = group[provider] >= threshold
            hits = int((predicted & observed).sum())
            misses = int((~predicted & observed).sum())
            false_alarms = int((predicted & ~observed).sum())
            rows.append({
                "place": place, "provider": provider, "n": int(len(group)),
                "wet_hours": int(observed.sum()),
                "accuracy": float((predicted == observed).mean()),
                "pod": float(hits / (hits + misses)) if hits + misses else float("nan"),
                "far": float(false_alarms / (hits + false_alarms)) if hits + false_alarms else float("nan"),
            })
    return pd.DataFrame(rows, columns=columns)


def lag_scan(start: datetime, end: datetime, basis: str = "instant",
             lags: tuple[int, ...] = (-1, 0, 1)) -> pd.DataFrame:
    """Er en leverandørs timeverdier systematisk forskjøvet en time?

    Svarer på det ved å score varslene som om de gjaldt timen før eller etter,
    og se om det treffer bedre. Ligger bunnpunktet på 0, er timene riktig
    innrettet. Ligger det på ±1 for én leverandør, er det en reell forskyvning –
    og da måler hovedrapporten delvis konvensjon i stedet for treffsikkerhet.

    Merk at fasiten selv har en konvensjon: nedbør og vindkast pares mot
    `valid_start + 1t`. Slår dette ut på *begge* leverandørene likt, er det
    fasiten som er feil innrettet, ikke leverandøren.
    """
    columns = ["place", "element", "provider", *[f"mae_{lag:+d}" for lag in lags], "best_lag", "n"]
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for lag in lags:
        pairs = paired(start, end, basis=basis, forecast_shift=lag)
        if pairs.empty:
            continue
        for (place, element), group in pairs.groupby(["place", "element"], sort=True):
            for provider in PROVIDERS:
                entry = rows.setdefault((place, element, provider), {
                    "place": place, "element": element, "provider": provider, "n": int(len(group)),
                })
                entry[f"mae_{lag:+d}"] = float(group[f"err_{provider}"].abs().mean())
                if lag == 0:
                    entry["n"] = int(len(group))
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(list(rows.values()))
    lag_of = {f"mae_{lag:+d}": lag for lag in lags if f"mae_{lag:+d}" in frame.columns}
    frame["best_lag"] = frame[list(lag_of)].idxmin(axis=1).map(lag_of)
    frame["_order"] = frame["element"].map({name: i for i, name in enumerate(ELEMENTS)})
    frame = frame.sort_values(["place", "_order", "provider"])
    return frame.reindex(columns=columns).reset_index(drop=True)


def report(days: int = 14, now: Optional[datetime] = None, basis: str = "instant",
           with_lag: bool = False) -> dict[str, Any]:
    """Alt en rapport trenger: scoretabell, nedbørstreff og datagrunnlag."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    end = _floor_hour(now)
    start = end - timedelta(days=max(1, days))
    pairs = paired(start, end, basis=basis)
    coverage: dict[str, Any] = {"comparisons": int(len(pairs)), "hours": 0, "runs": 0,
                                "days": days, "basis": basis, "storage": storage_location(),
                                "forecast_rows": int(len(load_forecasts(start, end))),
                                "from": _iso(start), "to": _iso(end)}
    if not pairs.empty:
        # `comparisons` teller hver (varsel, element)-sammenligning; `hours`
        # teller hvor mange faktiske klokketimer som har fasit.
        coverage["hours"] = int(pairs.groupby(["place", "valid_start"]).ngroups)
        coverage["runs"] = int(pairs["run_hour"].nunique())
    return {
        "coverage": coverage,
        "score": score(pairs),
        "rain": rain_skill(pairs),
        "lag": lag_scan(start, end, basis=basis) if with_lag else None,
        "pairs": pairs,
    }
