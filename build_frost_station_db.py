# build_frost_station_db.py
# -*- coding: utf-8 -*-
"""
Bygg/oppdater lokal stasjonsdatabase for Frost (/sources) + capabilities (has_*).

Kjør f.eks. ukentlig:
  python build_frost_station_db.py --out static/data/frost_stations.parquet

Krever miljøvariabel:
  FROST_CLIENT_ID
(og ev. FROST_CLIENT_SECRET hvis du bruker secret; Frost funker ofte med blank)

Output-kolonner:
  baseId, name, shortName, country, county, countyid, lat, lon, updated_at,
  has_snow, has_temp, has_precip, has_sun

Capabilities hentes fra:
  /observations/availableTimeSeries (GET)
For å unngå 414 (URI too long) auto-reduserer skriptet batch_size dersom det trengs.
"""

from __future__ import annotations

import os
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterator, Optional, Dict, Set, List

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 30

# Element-IDer (Frost)
ELEMENT_SNOW = "surface_snow_thickness"
ELEMENT_TEMP = "air_temperature"
ELEMENT_PRECIP = "sum(precipitation_amount P1D)"
ELEMENT_SUN = "sum(duration_of_sunshine P1M)"


CAPABILITY_MAP: Dict[str, str] = {
    "has_snow": ELEMENT_SNOW,
    "has_temp": ELEMENT_TEMP,
    "has_precip": ELEMENT_PRECIP,
    "has_sun": ELEMENT_SUN,
}


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


class FrostHTTPError(RuntimeError):
    def __init__(self, status_code: int, url: str, detail: Any):
        super().__init__(f"Frost-feil {status_code} for {url}: {detail}")
        self.status_code = status_code
        self.url = url
        self.detail = detail


class FrostURITooLong(FrostHTTPError):
    pass


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError("Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env).")
    return FrostAuth(client_id=cid, client_secret=os.getenv("FROST_CLIENT_SECRET", ""))


def frost_get_json(
    session: requests.Session,
    path: str,
    params: Optional[dict[str, str | int]] = None,
    *,
    auth: FrostAuth,
    retries: int = 6,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """GET med retries (429/5xx). Returnerer JSON (inkl. ErrorResponse ved 404)."""
    url = f"{FROST_BASE}{path}"
    backoff = 1.0

    for _attempt in range(1, retries + 1):
        r = session.get(
            url,
            params=params or None,
            auth=(auth.client_id, auth.client_secret),
            headers={"Accept": "application/json"},
            timeout=timeout,
        )

        if r.status_code == 200:
            return r.json()

        # Frost kan svare 404 med JSON
        if r.status_code == 404:
            return r.json()

        # URI for lang (414) -> la caller håndtere batch-size
        if r.status_code == 414:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:500]
            raise FrostURITooLong(414, url, detail)

        # Retry på 429 / 5xx
        if r.status_code == 429 or 500 <= r.status_code < 600:
            import time

            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise FrostHTTPError(r.status_code, url, detail)

    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk for {url}")


def iter_pages(
    session: requests.Session,
    path: str,
    params: dict[str, str | int],
    *,
    auth: FrostAuth,
    timeout: int,
) -> Iterator[dict[str, Any]]:
    """Paginering via offset/itemsPerPage/totalItemCount."""
    p = params.copy()
    first = frost_get_json(session, path, p, auth=auth, timeout=timeout)
    yield first

    if first.get("@type") == "ErrorResponse":
        return

    try:
        total = int(first.get("totalItemCount", 0))
        offset = int(first.get("offset", 0))
        per_page = int(first.get("itemsPerPage", 0))
    except Exception:
        return

    if total <= 0 or per_page <= 0:
        return

    next_offset = offset + per_page
    while next_offset < total:
        p2 = params.copy()
        p2["offset"] = next_offset
        page = frost_get_json(session, path, p2, auth=auth, timeout=timeout)
        yield page

        if page.get("@type") == "ErrorResponse":
            return

        try:
            offset = int(page.get("offset", next_offset))
            per_page = int(page.get("itemsPerPage", per_page))
            total = int(page.get("totalItemCount", total))
        except Exception:
            return

        next_offset = offset + per_page


def build_df(*, auth: FrostAuth, timeout: int) -> pd.DataFrame:
    """Henter stasjoner fra /sources."""
    path = "/sources/v0.jsonld"
    params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "fields": "id,name,shortName,country,geometry,county,countyid",
    }

    rows: list[dict[str, Any]] = []
    with requests.Session() as sess:
        for page in iter_pages(sess, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
                # litt logging hvis noe går galt
                msg = page.get("error", {}).get("message") if isinstance(page.get("error"), dict) else None
                print(f"[WARN] /sources ErrorResponse: {msg or page}")
                continue

            for item in page.get("data", []):
                geom = item.get("geometry") or {}
                coords = geom.get("coordinates")  # [lon, lat]
                lon = lat = None
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]

                rows.append(
                    {
                        "baseId": item.get("id"),
                        "name": item.get("name"),
                        "shortName": item.get("shortName"),
                        "country": item.get("country"),
                        "county": item.get("county"),
                        "countyid": item.get("countyid"),
                        "lat": lat,
                        "lon": lon,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["baseId", "lat", "lon"]).drop_duplicates(subset=["baseId"], keep="first")
    df = df.reset_index(drop=True)
    df["updated_at"] = datetime.now(timezone.utc).isoformat()
    return df


def _normalize_source_id(s: Any) -> Optional[str]:
    """Returner baseId (SNxxxx) fra 'SNxxxx:0' eller tilsvarende."""
    if not isinstance(s, str) or not s:
        return None
    return s.split(":")[0].strip() or None


def available_timeseries_batch_get(
    session: requests.Session,
    *,
    auth: FrostAuth,
    sources: List[str],
    elements: List[str],
    referencetime: str,
    timeout: int,
) -> List[Dict[str, Any]]:
    """
    Henter availableTimeSeries for en batch sources + elements (GET).
    Returnerer lista i 'data' (kan være tom).
    """
    path = "/observations/availableTimeSeries/v0.jsonld"
    params: dict[str, str | int] = {
        "sources": ",".join(sources),
        "elements": ",".join(elements),
        "referencetime": referencetime,
        "timeoffsets": "default",
        "levels": "default",
    }

    js = frost_get_json(session, path, params, auth=auth, timeout=timeout)
    if js.get("@type") == "ErrorResponse":
        # Viktig: logg så dette ikke skjer "silent"
        err = js.get("error")
        print(f"[WARN] availableTimeSeries ErrorResponse: {err or js}")
        return []
    data = js.get("data", [])
    return data if isinstance(data, list) else []


def compute_capabilities(
    base_ids: List[str],
    *,
    auth: FrostAuth,
    timeout: int,
    days: int = 365,
    batch_size: int = 200,
    min_batch_size: int = 10,
) -> Dict[str, Set[str]]:
    """
    Returnerer mapping:
      { "has_snow": {baseId,...}, "has_temp": {...}, ... }
    basert på availableTimeSeries i perioden.

    Auto-nedjusterer batch_size ved 414 (URI too long).
    """
    end = datetime.now(timezone.utc).date()
    start = (datetime.now(timezone.utc) - timedelta(days=int(days))).date()
    referencetime = f"{start.isoformat()}/{end.isoformat()}"

    elements = list(CAPABILITY_MAP.values())
    out: Dict[str, Set[str]] = {k: set() for k in CAPABILITY_MAP.keys()}
    element_to_flag = {v: k for k, v in CAPABILITY_MAP.items()}

    bsz = int(batch_size)
    if bsz < int(min_batch_size):
        bsz = int(min_batch_size)

    with requests.Session() as sess:
        i = 0
        while i < len(base_ids):
            batch = base_ids[i : i + bsz]

            try:
                data = available_timeseries_batch_get(
                    sess,
                    auth=auth,
                    sources=batch,
                    elements=elements,
                    referencetime=referencetime,
                    timeout=timeout,
                )
            except FrostURITooLong:
                # Reduser batch_size og prøv igjen samme i
                if bsz <= int(min_batch_size):
                    raise RuntimeError(
                        f"Får 414 selv med batch_size={bsz}. "
                        f"Prøv å redusere antall elements, eller sett min_batch_size lavere."
                    )
                new_bsz = max(int(min_batch_size), bsz // 2)
                print(f"[INFO] 414 URI too long ved batch_size={bsz}. Prøver igjen med batch_size={new_bsz}.")
                bsz = new_bsz
                continue

            # Hvis vi fikk data uten 414, kan vi gå videre
            for item in data:
                sid = _normalize_source_id(item.get("sourceId") or item.get("source") or item.get("id"))
                if not sid:
                    continue

                el = item.get("elementId") or item.get("element") or item.get("element_id")
                if isinstance(el, str) and el in element_to_flag:
                    out[element_to_flag[el]].add(sid)
                else:
                    # Hvis elementId mangler (sjeldent) – ikke gjett alt; bare hopp
                    # (heller litt falsk-negativ enn å markere alt True)
                    continue

            i += bsz

    return out


def add_capability_columns(
    df: pd.DataFrame,
    *,
    auth: FrostAuth,
    timeout: int,
    days: int,
    batch_size: int,
) -> pd.DataFrame:
    """Legger til has_* kolonner i df."""
    if df.empty:
        return df

    base_ids = df["baseId"].astype(str).tolist()
    caps = compute_capabilities(base_ids, auth=auth, timeout=timeout, days=days, batch_size=batch_size)

    for flag in CAPABILITY_MAP.keys():
        df[flag] = df["baseId"].isin(caps.get(flag, set()))

    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default="",
        help="Output path (.parquet eller .csv). Default: static/data/frost_stations.parquet",
    )
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument(
        "--cap-days",
        type=int,
        default=365,
        help="Antall dager bakover for capability-sjekk (availableTimeSeries). Default: 365",
    )
    ap.add_argument(
        "--cap-batch-size",
        type=int,
        default=200,
        help="Start batch-størrelse for sources mot availableTimeSeries (GET). Auto-reduseres ved 414. Default: 200",
    )
    args = ap.parse_args()

    auth = _env_auth()

    df = build_df(auth=auth, timeout=int(args.timeout))
    if df.empty:
        raise RuntimeError("Fikk ingen stasjoner fra Frost /sources.")

    df = add_capability_columns(
        df,
        auth=auth,
        timeout=int(args.timeout),
        days=int(args.cap_days),
        batch_size=int(args.cap_batch_size),
    )

    df["updated_at"] = datetime.now(timezone.utc).isoformat()

    out = Path(args.out) if args.out else Path("static") / "data" / "frost_stations.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.suffix.lower() == ".parquet":
        df.to_parquet(out, index=False)
    else:
        df.to_csv(out, index=False)

    stats = {k: int(df[k].sum()) for k in CAPABILITY_MAP.keys()}
    print(f"OK: skrev {len(df):,} stasjoner til {out}")
    print("Capabilities (antall True):", stats)


if __name__ == "__main__":
    main()
