"""
Engangs-script: Finn alle norske Frost-stasjoner med wind_speed
og sammenlign med ver_station_db for å finne manglende stasjoner.

Kjør fra prosjektrotmappen:
    python update_wind_stations.py

Lager to filer:
    wind_stations_all.csv      - Alle Frost-stasjoner med wind_speed
    wind_stations_missing.csv  - Stasjoner som mangler i ver_station_db
"""

import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FROST_BASE = "https://frost.met.no"
CLIENT_ID = os.getenv("FROST_CLIENT_ID", "")
if not CLIENT_ID:
    print("❌ Mangler FROST_CLIENT_ID i miljø/.env")
    sys.exit(1)

session = requests.Session()

def frost_get(path, params):
    r = session.get(
        f"{FROST_BASE}{path}",
        params=params,
        auth=(CLIENT_ID, ""),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"  Frost {r.status_code}: {r.text[:200]}")
        return {}
    return r.json()

# ── 1. Hent alle norske vindstasjoner fra Frost ──────────────────────────────
print("Henter alle norske stasjoner med wind_speed fra Frost...")

data = frost_get("/sources/v0.jsonld", {
    "country": "NO",
    "elements": "wind_speed",
})

sources = data.get("data", []) or []
print(f"  Frost returnerte {len(sources)} stasjoner")

rows = []
for s in sources:
    sid = str(s.get("id") or "").split(":")[0]
    if not sid.startswith("SN"):
        continue
    name = s.get("name") or ""
    short = s.get("shortName") or ""
    county = (s.get("county") or {}).get("name") if isinstance(s.get("county"), dict) else s.get("county") or ""
    country = s.get("country") or "NO"
    geo = s.get("geometry") or {}
    coords = geo.get("coordinates") or []
    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None
    valid_from = s.get("validFrom") or ""
    valid_to = s.get("validTo") or ""
    rows.append({
        "baseId": sid,
        "name": name,
        "shortName": short,
        "county": county,
        "country": country,
        "lat": lat,
        "lon": lon,
        "validFrom": valid_from,
        "validTo": valid_to,
    })

frost_df = pd.DataFrame(rows)
# Kun aktive stasjoner (ingen validTo, eller validTo er langt frem)
active = frost_df[frost_df["validTo"] == ""].copy()
print(f"  Aktive vindstasjoner (ingen validTo): {len(active)}")

frost_df.to_csv("wind_stations_all.csv", index=False, encoding="utf-8")
print(f"  Lagret wind_stations_all.csv ({len(frost_df)} rader)")

# ── 2. Last inn ver_station_db og finn manglende ─────────────────────────────
print("\nLaster ver_station_db...")
try:
    sys.path.insert(0, "scripts")
    from ver_station_db import load_station_db
    db = load_station_db()
    db_ids = set(db["baseId"].astype(str).str.split(":").str[0].tolist())
    print(f"  Stasjoner i ver_station_db: {len(db_ids)}")
except Exception as e:
    print(f"  ⚠️  Kunne ikke laste ver_station_db: {e}")
    db_ids = set()

# ── 3. Finn manglende aktive stasjoner ───────────────────────────────────────
missing = active[~active["baseId"].isin(db_ids)].copy()
print(f"\nAktive vindstasjoner som MANGLER i ver_station_db: {len(missing)}")

if not missing.empty:
    print("\nTopp manglende (sortert på county):")
    print(missing.sort_values("county")[["baseId","name","county","lat","lon"]].to_string(index=False))
    missing.to_csv("wind_stations_missing.csv", index=False, encoding="utf-8")
    print(f"\nLagret wind_stations_missing.csv")
else:
    print("✅ Ingen manglende stasjoner!")

# ── 4. Oppsummering ───────────────────────────────────────────────────────────
print("\n── Oppsummering ──────────────────────────────────────────────────────")
print(f"Alle Frost vindstasjoner (NO):    {len(frost_df)}")
print(f"  Aktive (ingen validTo):         {len(active)}")
print(f"  Allerede i ver_station_db:      {len(active) - len(missing)}")
print(f"  Mangler i ver_station_db:       {len(missing)}")
print(f"\nFil: wind_stations_all.csv    - alle Frost vindstasjoner")
print(f"Fil: wind_stations_missing.csv - manglende, klare til å importere")
