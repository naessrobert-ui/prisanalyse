# build_frost_station_db.py
# -*- coding: utf-8 -*-
"""
Bygg/oppdater lokal stasjonsdatabase for Frost (/sources).

Kjør f.eks. som cron/Task Scheduler 1 gang per uke/dag:
  python build_frost_station_db.py --out static/data/frost_stations.parquet

Krever miljøvariabel:
  FROST_CLIENT_ID
(og ev. FROST_CLIENT_SECRET hvis du bruker secret; Frost funker ofte med blank)

Output-kolonner:
  baseId, name, shortName, country, county, countyid, lat, lon, updated_at
"""

from __future__ import annotations

import os
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")


FROST_BASE = "https://frost.met.no"
DEFAULT_TIMEOUT = 30


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


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

        if r.status_code == 404:
            return r.json()

        if r.status_code == 429 or 500 <= r.status_code < 600:
            import time
            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Frost-feil {r.status_code} for {url}: {detail}")

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
    path = "/sources/v0.jsonld"

    # NB: hold fields konservative (kjente fra dine scripts)
    params: dict[str, str | int] = {
        "types": "SensorSystem",
        "country": "NO",
        "fields": "id,name,shortName,country,geometry,county,countyid",
    }

    rows: list[dict[str, Any]] = []

    with requests.Session() as sess:
        for page in iter_pages(sess, path, params, auth=auth, timeout=timeout):
            if page.get("@type") == "ErrorResponse":
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
    df["updated_at"] = datetime.now(timezone.utc).isoformat()
    return df.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="", help="Output path (.parquet eller .csv). Default: static/data/frost_stations.parquet")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = ap.parse_args()

    auth = _env_auth()
    df = build_df(auth=auth, timeout=int(args.timeout))
    if df.empty:
        raise RuntimeError("Fikk ingen stasjoner fra Frost /sources.")

    out = Path(args.out) if args.out else Path("static") / "data" / "frost_stations.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.suffix.lower() == ".parquet":
        df.to_parquet(out, index=False)
    else:
        df.to_csv(out, index=False)

    print(f"OK: skrev {len(df):,} stasjoner til {out}")


if __name__ == "__main__":
    main()
