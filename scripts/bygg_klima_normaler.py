"""Bygg klimanormaler for ferieplanlegger-langtid.

Henter månedlige klimanormaler (1991-2020) fra NASA POWER for hver
destinasjon, og lagrer som parquet i ``data/klima_normaler.parquet``.

Kjør én gang for å bygge tabellen:

    python -m scripts.bygg_klima_normaler

Hvilke parametere brukes:

* ``T2M``  – månedssnitt av temperatur (°C). Dette er det pålitelige
  målet for "hvor varmt det normalt er om dagen" — et månedssnitt på
  20 °C tilsvarer typisk daglige maksverdier på ~24-26 °C.
* ``T2M_MIN`` / ``T2M_MAX`` – ekstremverdier i normalperioden. Nyttig
  som referanse ("kan bli så kaldt/varmt"), men ikke "typisk daglig".
* ``PRECTOTCORR`` – snitt nedbør i mm/dag, brukt til å regne ut
  ``precip_mm_month``.
* ``ALLSKY_SFC_SW_DWN`` – solinnstråling, returneres av NASA POWER i
  MJ/m²/dag og konverteres til kWh/m²/dag (1 kWh = 3.6 MJ). Brukes som
  sol-proxy: typisk 0.3-1.5 i nordnorsk vinter, 6-8 i sør-europeisk
  sommer.

Periode 1991-2020 er WMO sin standard 30-årsperiode for klimanormaler.
Datakilde er MERRA-2 (satellitt-reanalyse), tilstrekkelig nøyaktig for
relativ sammenligning mellom destinasjoner.
"""
from __future__ import annotations

import calendar
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# Sørg for at scripts-pakken kan importeres når skriptet kjøres direkte.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ferie_destinasjoner import DESTINASJONER, Destinasjon  # noqa: E402


POWER_URL = "https://power.larc.nasa.gov/api/temporal/climatology/point"
USER_AGENT = "prisanalyse.no/1.0 (kontakt@prisanalyse.no)"

START_YEAR = 1991
END_YEAR = 2020

PARAMETERE = "T2M,T2M_MIN,T2M_MAX,PRECTOTCORR,ALLSKY_SFC_SW_DWN"

OUTPUT = ROOT / "data" / "klima_normaler.parquet"

MAX_WORKERS = 6
REQUEST_TIMEOUT = 60

# NASA POWER bruker måneds-forkortelser.
MND_KODE = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def _fetch_climatology(dest: Destinasjon) -> Optional[dict]:
    params = {
        "parameters": PARAMETERE,
        "community": "AG",
        "latitude": dest["lat"],
        "longitude": dest["lon"],
        "format": "JSON",
        "start": START_YEAR,
        "end": END_YEAR,
    }
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(4):
        try:
            r = requests.get(POWER_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                return data.get("properties", {}).get("parameter") or None
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(3.0 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(2.0)
    return None


def _normaler_til_df(parametere: dict, dest: Destinasjon) -> Optional[pd.DataFrame]:
    """Pakk ut 12 månedsrader fra NASA POWER sitt JSON-svar."""
    if not parametere:
        return None

    rader: list[dict] = []
    for mnd_idx, kode in enumerate(MND_KODE, start=1):
        try:
            t_mean = float(parametere["T2M"][kode])
            t_min = float(parametere["T2M_MIN"][kode])
            t_max = float(parametere["T2M_MAX"][kode])
            precip_day = float(parametere["PRECTOTCORR"][kode])
            # NASA POWER returnerer MJ/m²/dag; konverter til kWh/m²/dag.
            sol_day = float(parametere["ALLSKY_SFC_SW_DWN"][kode]) / 3.6
        except (KeyError, TypeError, ValueError):
            return None

        dager_i_mnd = calendar.monthrange(2001, mnd_idx)[1]  # ikke-skuddår
        rader.append({
            "dest_id": dest["id"],
            "dest_navn": dest["navn"],
            "land": dest["land"],
            "region": dest["region"],
            "month": mnd_idx,
            "temp_mean": round(t_mean, 1),
            "temp_min": round(t_min, 1),
            "temp_max": round(t_max, 1),
            "precip_mm_day": round(precip_day, 2),
            "precip_mm_month": round(precip_day * dager_i_mnd, 1),
            "solar_kwh_day": round(sol_day, 2),
        })
    return pd.DataFrame(rader)


def bygg_tabell() -> pd.DataFrame:
    rader: list[pd.DataFrame] = []
    feilet: list[str] = []
    total = len(DESTINASJONER)

    def _process(dest: Destinasjon) -> tuple[Destinasjon, Optional[pd.DataFrame]]:
        params = _fetch_climatology(dest)
        if params is None:
            return dest, None
        return dest, _normaler_til_df(params, dest)

    print(f"Henter NASA POWER klimanormaler {START_YEAR}-{END_YEAR} for {total} destinasjoner …")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_process, d): d for d in DESTINASJONER}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                dest, df = fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  [{i:>3}/{total}] EXCEPTION: {e}")
                continue
            if df is None or df.empty:
                feilet.append(dest["id"])
                print(f"  [{i:>3}/{total}] FEIL  {dest['navn']}", flush=True)
            else:
                rader.append(df)
                print(f"  [{i:>3}/{total}] ok    {dest['navn']}", flush=True)

    if not rader:
        raise SystemExit("Ingen data hentet – avbryter.")

    out = pd.concat(rader, ignore_index=True)
    out = out.sort_values(["dest_id", "month"]).reset_index(drop=True)

    dt = time.time() - t0
    print(f"\nFerdig på {dt:.1f}s. {len(out)} rader fra {len(rader)} destinasjoner.")
    if feilet:
        print(f"  Feilet: {feilet}")
    return out


def lagre(df: pd.DataFrame, sti: Path = OUTPUT) -> None:
    sti.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(sti, index=False)
    print(f"Lagret til {sti}  ({sti.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    tabell = bygg_tabell()
    lagre(tabell)
