# ver_station_db.py
# -*- coding: utf-8 -*-
"""
Felles, lokal database (Parquet/CSV) med Frost-stasjonsmetadata + capabilities.

Målet: unngå å hente stasjonslisten fra Frost i hvert kart-endepunkt.
- Bygg/oppdater databasen periodisk (f.eks. ukentlig) med build_frost_station_db.py
- Kartskriptene leser herfra (hurtig) og gjør kun observasjonskall mot Frost.

Forventet schema i DB (minstekrav):
  baseId, name, shortName, country, county, countyid, lat, lon, updated_at

Valgfrie capability-felter (anbefalt):
  has_snow, has_temp, has_precip, has_sun
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable, Tuple, Dict, Any

import pandas as pd


# ---------------------------
# Konstanter / kolonner
# ---------------------------

CAPABILITY_COLS = ["has_snow", "has_temp", "has_precip", "has_sun"]
BASE_COLS = ["baseId", "name", "shortName", "country", "county", "countyid", "lat", "lon", "updated_at"]
ALL_COLS = BASE_COLS + CAPABILITY_COLS

_EMPTY_DF = pd.DataFrame(columns=ALL_COLS)

# Cache per fil (mtime)
_CACHE: Dict[str, Dict[str, Any]] = {}  # key=abs path -> {"mtime": float, "df": DataFrame}


# ---------------------------
# Finn default plassering
# ---------------------------

def _find_project_root(start: Optional[Path] = None) -> Path:
    """Finn prosjektroten ved å lete etter 'static/data' i foreldrene."""
    p = (start or Path(__file__).resolve()).parent
    for cand in [p, *p.parents]:
        if (cand / "static" / "data").is_dir():
            return cand
    # fallback: samme mappe som denne filen
    return (start or Path(__file__).resolve()).parent


def default_db_path() -> Path:
    root = _find_project_root()
    return root / "static" / "data" / "frost_stations.parquet"


# ---------------------------
# Intern hjelp
# ---------------------------

def _normalize_bool_series(s: pd.Series) -> pd.Series:
    """
    Normaliser en kolonne til bool på en robust måte (True/False, 1/0, 'true'/'false', NaN).
    """
    if s.dtype == bool:
        return s.fillna(False)

    # prøv tolkning av str
    ss = s.astype(str).str.strip().str.lower()
    mapped = ss.map(
        {
            "true": True, "false": False,
            "1": True, "0": False,
            "yes": True, "no": False,
            "y": True, "n": False,
            "t": True, "f": False,
        }
    )

    # hvis vi ikke fikk mappet (NaN), prøv numeric
    if mapped.isna().any():
        num = pd.to_numeric(s, errors="coerce")
        mapped = mapped.where(mapped.notna(), num.map({1: True, 0: False}))

    return mapped.fillna(False).astype(bool)


def _ensure_capability_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sørg for at has_* kolonnene finnes og er bool.
    Hvis de mangler i filen, settes de til False.
    """
    d = df.copy()
    for c in CAPABILITY_COLS:
        if c not in d.columns:
            d[c] = False
        else:
            d[c] = _normalize_bool_series(d[c])
    return d


# ---------------------------
# Les DB (med cache)
# ---------------------------

def load_station_db(db_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Last stasjons-DB fra disk (Parquet foretrukket). Cache per mtime.
    Returnerer tom DF hvis filen ikke finnes.
    """
    path = Path(db_path) if db_path else default_db_path()
    key = str(path.resolve())

    if not path.exists():
        return _EMPTY_DF.copy()

    mtime = path.stat().st_mtime
    hit = _CACHE.get(key)
    if hit and hit.get("mtime") == mtime:
        return hit["df"]

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    # normaliser baseId
    if "baseId" not in df.columns and "id" in df.columns:
        df = df.rename(columns={"id": "baseId"})

    # lat/lon som numeric
    for c in ["lat", "lon"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # dedup
    if "baseId" in df.columns:
        df = df.dropna(subset=["baseId"]).drop_duplicates(subset=["baseId"], keep="first")

    # capabilities
    df = _ensure_capability_cols(df)

    # behold kolonner vi bryr oss om, men ikke krasj hvis noen mangler
    keep_cols = [c for c in ALL_COLS if c in df.columns]
    df = df[keep_cols].copy()

    # hvis updated_at mangler i filen, lag den (tom)
    if "updated_at" not in df.columns:
        df["updated_at"] = ""

    # sørg for at alle kolonner finnes i riktig rekkefølge
    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = False if c in CAPABILITY_COLS else ""
    df = df[ALL_COLS].copy()

    _CACHE[key] = {"mtime": mtime, "df": df}
    return df


# ---------------------------
# Filtre/lookup
# ---------------------------

def stations_by_ids(
    source_ids: Iterable[str],
    *,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    ids = [str(x) for x in source_ids if x]
    if not ids:
        return _EMPTY_DF.copy()
    df = load_station_db(db_path=db_path)
    if df.empty:
        return _EMPTY_DF.copy()
    out = df[df["baseId"].astype(str).isin(ids)].copy()
    return out.reset_index(drop=True)


def stations_in_bbox_swne(
    bbox: Tuple[float, float, float, float],
    *,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """bbox = (south, west, north, east)."""
    south, west, north, east = bbox
    df = load_station_db(db_path=db_path)
    if df.empty:
        return _EMPTY_DF.copy()

    d = df.copy()
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")
    d = d.dropna(subset=["lat", "lon"])
    d = d[(d["lat"].between(south, north)) & (d["lon"].between(west, east))].copy()
    return d.reset_index(drop=True)


def stations_in_bbox_wsen(
    w: float,
    s: float,
    e: float,
    n: float,
    *,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """bbox = (west, south, east, north)."""
    return stations_in_bbox_swne((s, w, n, e), db_path=db_path)


def stations_in_county(
    county: str,
    *,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Filtrer stasjoner på fylke (county-felt fra Frost).
    Returnerer DF med baseId + metadata + capabilities.
    """
    if not county:
        return _EMPTY_DF.copy()
    df = load_station_db(db_path=db_path)
    if df.empty:
        return _EMPTY_DF.copy()

    d = df.copy()
    if "county" not in d.columns:
        return _EMPTY_DF.copy()

    # eksakt match først
    m = d[d["county"].astype(str) == str(county)].copy()

    # fallback contains
    if m.empty:
        c = str(county).lower()
        m = d[d["county"].astype(str).str.lower().str.contains(c, na=False)].copy()

    return m.dropna(subset=["baseId", "lat", "lon"]).reset_index(drop=True)


# ---------------------------
# Capability-hjelpere
# ---------------------------

def filter_by_capabilities(
    df: pd.DataFrame,
    *,
    require_snow: bool = False,
    require_temp: bool = False,
    require_precip: bool = False,
    require_sun: bool = False,
) -> pd.DataFrame:
    """
    Filtrer et DF på has_* felter.
    """
    d = df
    if d.empty:
        return d

    # sørg for kolonnene finnes (om df kommer fra annen kilde)
    for c in CAPABILITY_COLS:
        if c not in d.columns:
            d = d.copy()
            d[c] = False

    if require_snow:
        d = d[d["has_snow"] == True]
    if require_temp:
        d = d[d["has_temp"] == True]
    if require_precip:
        d = d[d["has_precip"] == True]
    if require_sun:
        d = d[d["has_sun"] == True]

    return d


def stations_in_bbox_swne_with(
    bbox: Tuple[float, float, float, float],
    *,
    require_snow: bool = False,
    require_temp: bool = False,
    require_precip: bool = False,
    require_sun: bool = False,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Kombinasjon: bbox-filter + capability-filter.
    """
    df = stations_in_bbox_swne(bbox, db_path=db_path)
    if df.empty:
        return df
    return filter_by_capabilities(
        df,
        require_snow=require_snow,
        require_temp=require_temp,
        require_precip=require_precip,
        require_sun=require_sun,
    ).reset_index(drop=True)


def stations_by_ids_with(
    source_ids: Iterable[str],
    *,
    require_snow: bool = False,
    require_temp: bool = False,
    require_precip: bool = False,
    require_sun: bool = False,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Kombinasjon: ids-filter + capability-filter.
    """
    df = stations_by_ids(source_ids, db_path=db_path)
    if df.empty:
        return df
    return filter_by_capabilities(
        df,
        require_snow=require_snow,
        require_temp=require_temp,
        require_precip=require_precip,
        require_sun=require_sun,
    ).reset_index(drop=True)
