# ver_station_db.py
# -*- coding: utf-8 -*-
"""
Felles, lokal database (Parquet/CSV) med Frost-stasjonsmetadata.

Målet: unngå å hente stasjonslisten fra Frost i hvert kart-endepunkt.
- Bygg/oppdater databasen periodisk (f.eks. daglig/ukentlig) med build_frost_station_db.py
- Kartskriptene leser herfra (hurtig) og gjør kun observasjonskall mot Frost.

Forventet schema i DB:
baseId, name, shortName, country, county, countyid, lat, lon, updated_at
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Iterable, Tuple, Dict, Any

import pandas as pd


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
# Cache på tvers av kall
# ---------------------------

_CACHE: Dict[str, Dict[str, Any]] = {}  # key=abs path -> {"mtime": float, "df": DataFrame}


def load_station_db(db_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Last stasjons-DB fra disk (Parquet foretrukket). Cache per mtime.
    Returnerer tom DF hvis filen ikke finnes.
    """
    path = Path(db_path) if db_path else default_db_path()
    key = str(path.resolve())

    if not path.exists():
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "county", "countyid", "lat", "lon"])

    mtime = path.stat().st_mtime
    hit = _CACHE.get(key)
    if hit and hit.get("mtime") == mtime:
        return hit["df"]

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        # støtt CSV som fallback
        df = pd.read_csv(path)

    # normaliser kolonner
    if "baseId" not in df.columns and "id" in df.columns:
        df = df.rename(columns={"id": "baseId"})

    for c in ["lat", "lon"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # dedup
    if "baseId" in df.columns:
        df = df.dropna(subset=["baseId"]).drop_duplicates(subset=["baseId"], keep="first")

    # beholde bare kjente kolonner hvis de finnes
    keep_cols = [c for c in ["baseId", "name", "shortName", "country", "county", "countyid", "lat", "lon"] if c in df.columns]
    df = df[keep_cols].copy()

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
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "lat", "lon"])
    df = load_station_db(db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "lat", "lon"])
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
        return pd.DataFrame(columns=["baseId", "name", "shortName", "country", "lat", "lon"])
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
    Returnerer DF med baseId, name, shortName, county, countyid, lat, lon.
    """
    if not county:
        return pd.DataFrame(columns=["baseId", "name", "shortName", "county", "countyid", "lat", "lon"])
    df = load_station_db(db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=["baseId", "name", "shortName", "county", "countyid", "lat", "lon"])

    d = df.copy()
    # eksakt match først
    if "county" in d.columns:
        m = d[d["county"].astype(str) == str(county)].copy()
    else:
        m = pd.DataFrame()

    if m.empty and "county" in d.columns:
        # wildcard-ish fallback: "Møre og Romsdal" <-> "Møre*Romsdal"
        c = str(county).lower()
        m = d[d["county"].astype(str).str.lower().str.contains(c, na=False)].copy()

    keep = [c for c in ["baseId", "name", "shortName", "county", "countyid", "lat", "lon"] if c in m.columns]
    return m[keep].dropna(subset=["baseId", "lat", "lon"]).reset_index(drop=True)
