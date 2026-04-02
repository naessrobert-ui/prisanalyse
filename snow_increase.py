"""
Snøprognose for Kvamskogen (og andre steder)
- Henter værvarsel fremover fra YR via metno_locationforecast
  (timesdata de første 48t, deretter 6t-intervaller)
- Henter nåværende snødybde fra Frost API – samme auth-mønster som snow_map.py
- Beregner forventet snøutvikling basert på nedbør og temperatur

Stasjon Kvamskogen: SN50310
Frost API-nøkkel: sett FROST_CLIENT_ID i .env eller miljøvariabler
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from metno_locationforecast import Place, Forecast

# --- .env loading (samme som snow_map.py) ---
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
load_dotenv()


# ══════════════════════════════════════════════════════════════════════════════
#  Konfigurasjon
# ══════════════════════════════════════════════════════════════════════════════

BRUKER = "Robert Næss (privat) robert.naess@online.no"

STASJONER = {
    "Kvamskogen": {
        "place": Place("Kvamskogen", 60.3983, 5.9728, 500),
        "frost_id": "SN50310",
    },
    # Legg til flere ved behov:
    # "Voss": {
    #     "place": Place("Voss", 60.6281, 6.4179, 56),
    #     "frost_id": "SN50600",
    # },
}

# Snømodell-parametre (juster gjerne disse)
SNØ_FAKTOR_BASE           = 10    # mm snø per mm nedbør ved 0 °C
SNØ_FAKTOR_EKSTRA         = 0.5   # ekstra faktor per grad under 0 °C (tørrere snø)
SMELTING_MM_PER_GRAD_TIME = 0.5   # mm snø per grad-time (lavt for fjell/skog vinterstid)
SMELTING_TERSKEL_C        = 0.5   # smelting starter ikke før temp overstiger denne verdien

FROST_BASE      = "https://frost.met.no"
DEFAULT_TIMEOUT = 20
ELEMENT_SNOW    = "surface_snow_thickness"   # enhet: cm


# ══════════════════════════════════════════════════════════════════════════════
#  Auth – identisk mønster med snow_map.py
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ""


def _env_auth() -> FrostAuth:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise RuntimeError(
            "Sett miljøvariabelen FROST_CLIENT_ID (evt. via .env + python-dotenv)."
        )
    return FrostAuth(
        client_id=cid,
        client_secret=os.getenv("FROST_CLIENT_SECRET", ""),
    )


def frost_get_json(
    session: requests.Session,
    path: str,
    params: Optional[dict[str, str | int]] = None,
    *,
    auth: FrostAuth,
    retries: int = 6,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """GET med retries (429 / 5xx). Returnerer JSON (inkl. ErrorResponse ved 404/412)."""
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
        if r.status_code in (404, 412):
            return r.json()
        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue
        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Frost-feil {r.status_code} for {url}: {detail}")

    raise RuntimeError(f"Frost: ga opp etter {retries} forsøk for {url}")


# ══════════════════════════════════════════════════════════════════════════════
#  Frost: hent siste snødybde for én stasjon
# ══════════════════════════════════════════════════════════════════════════════

def hent_snødybde_frost(
    stasjon_id: str,
    *,
    session: requests.Session,
    auth: FrostAuth,
    timeout: int = DEFAULT_TIMEOUT,
) -> float | None:
    """Henter siste observerte snødybde (cm). Returnerer float eller None."""
    params: dict[str, str | int] = {
        "sources":       stasjon_id,
        "elements":      ELEMENT_SNOW,
        "referencetime": "latest",
        "limit":         1,
        "timeoffsets":   "default",
        "levels":        "default",
        "qualities":     "0,1,2,3,4",
    }

    data = frost_get_json(session, "/observations/v0.jsonld", params, auth=auth, timeout=timeout)

    if data.get("@type") == "ErrorResponse":
        print(f"  Frost ErrorResponse for {stasjon_id}: {data.get('error', {}).get('message', '')}")
        return None

    for item in data.get("data", []):
        for obs in item.get("observations", []):
            verdi = obs.get("value")
            ref   = obs.get("referenceTime") or item.get("referenceTime", "?")
            if verdi is not None:
                cm = float(verdi)
                print(f"  Frost ({stasjon_id}): snødybde = {cm} cm  (obs: {ref})")
                return cm

    print(f"  Frost ({stasjon_id}): ingen observasjon funnet.")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  Snømodell
# ══════════════════════════════════════════════════════════════════════════════

def snøfaktor(temperatur: float) -> float:
    """
    Temperaturavhengig snøfaktor.
      0 °C  → 10  (tung, våt snø)
    -10 °C  → 15  (lett, tørr snø)
    > 0 °C  →  0  (regn)
    """
    if temperatur > 0:
        return 0.0
    return SNØ_FAKTOR_BASE + abs(temperatur) * SNØ_FAKTOR_EKSTRA


def beregn_smelting(temperatur: float, timer: float) -> float:
    """
    Degree-day-metoden med terskel.
    Smelting starter ikke før temp > SMELTING_TERSKEL_C.
    """
    if temperatur <= SMELTING_TERSKEL_C:
        return 0.0
    return (temperatur - SMELTING_TERSKEL_C) * SMELTING_MM_PER_GRAD_TIME * timer


def simuler_snøprognose(
    intervaller: list[dict],
    start_snødybde_cm: float,
) -> pd.DataFrame:
    """Simulerer snøutvikling intervall for intervall (1t eller 6t)."""
    snødybde_mm = start_snødybde_cm * 10
    rader: list[dict] = []

    for iv in intervaller:
        temp   = iv["temp"]
        nedbør = iv["nedbør_mm"]
        timer  = iv["timer"]

        faktor      = snøfaktor(temp)
        ny_snø_mm   = nedbør * faktor
        smelting_mm = beregn_smelting(temp, timer)

        snødybde_mm = max(0.0, snødybde_mm + ny_snø_mm - smelting_mm)

        rader.append({
            "start":        iv["start"],
            "slutt":        iv["slutt"],
            "timer":        timer,
            "temperatur_c": round(temp, 1),
            "nedbør_mm":    round(nedbør, 1),
            "nedbør_min_mm": round(iv.get("nedbør_min_mm"), 1) if iv.get("nedbør_min_mm") is not None else None,
            "nedbør_maks_mm": round(iv.get("nedbør_maks_mm"), 1) if iv.get("nedbør_maks_mm") is not None else None,
            "nedbør_sannsynlighet_pct": int(round(iv.get("nedbør_sannsynlighet_pct"))) if iv.get("nedbør_sannsynlighet_pct") is not None else None,
            "snøfaktor":    round(faktor, 1),
            "vind_ms":      round(iv.get("vind_ms"), 1) if iv.get("vind_ms") is not None else None,
            "vind_kast_ms": round(iv.get("vind_kast_ms"), 1) if iv.get("vind_kast_ms") is not None else None,
            "vindretning_grader": round(iv.get("vindretning_grader"), 0) if iv.get("vindretning_grader") is not None else None,
            "ny_snø_mm":    round(ny_snø_mm, 1),
            "smelting_mm":  round(smelting_mm, 1),
            "netto_mm":     round(ny_snø_mm - smelting_mm, 1),
            "snødybde_cm":  round(snødybde_mm / 10, 1),
        })

    return pd.DataFrame(rader)


# ══════════════════════════════════════════════════════════════════════════════
#  YR-varsel
# ══════════════════════════════════════════════════════════════════════════════

def _parse_verdi(variabel) -> float:
    """
    Trekker ut numerisk verdi fra en metno_locationforecast-variabel.
    Håndterer format som:
      "3.2 celsius"                  → 3.2
      "air_temperature: 3.2 celsius" → 3.2
    """
    if hasattr(variabel, "value"):
        return float(variabel.value)
    for token in str(variabel).split():
        try:
            return float(token)
        except ValueError:
            continue
    raise ValueError(f"Fant ingen numerisk verdi i: {variabel!r}")


def hent_intervaller(sted: Place) -> list[dict]:
    """
    Henter YR 'complete'-varsel.
    Returnerer timesintervaller (første 48t) og 6t-intervaller deretter.
    """
    varsel = Forecast(sted, BRUKER, "complete")
    varsel.update()

    intervaller: list[dict] = []
    for data in varsel.data.intervals:
        variabler = data.variables

        temp   = variabler.get("air_temperature")
        nedbør = variabler.get("precipitation_amount")
        nedbør_min = variabler.get("precipitation_amount_min")
        nedbør_maks = variabler.get("precipitation_amount_max")
        nedbør_sannsynlighet = variabler.get("probability_of_precipitation")
        vind   = variabler.get("wind_speed")
        vind_kast = variabler.get("wind_speed_of_gust")
        vindretning = variabler.get("wind_from_direction")

        if temp is None or nedbør is None:
            continue

        start = data.start_time
        slutt = data.end_time
        timer = (slutt - start).total_seconds() / 3600

        intervaller.append({
            "start":     start,
            "slutt":     slutt,
            "temp":      _parse_verdi(temp),
            "nedbør_mm": _parse_verdi(nedbør),
            "nedbør_min_mm": _parse_verdi(nedbør_min) if nedbør_min is not None else None,
            "nedbør_maks_mm": _parse_verdi(nedbør_maks) if nedbør_maks is not None else None,
            "nedbør_sannsynlighet_pct": _parse_verdi(nedbør_sannsynlighet) if nedbør_sannsynlighet is not None else None,
            "vind_ms":   _parse_verdi(vind) if vind is not None else None,
            "vind_kast_ms": _parse_verdi(vind_kast) if vind_kast is not None else None,
            "vindretning_grader": _parse_verdi(vindretning) if vindretning is not None else None,
            "timer":     timer,
        })

    return intervaller


# ══════════════════════════════════════════════════════════════════════════════
#  Utskrift
# ══════════════════════════════════════════════════════════════════════════════

def skriv_intervall_tabell(df: pd.DataFrame) -> None:
    """Skriver time/6t-tabell til terminalen med intervalltype synlig."""
    print(f"\n  {'Start':<20} {'t':>3}  {'°C':>5}  {'Nedbør':>7}  "
          f"{'NySnø':>7}  {'Smelt':>6}  {'Netto':>6}  {'Dybde cm':>9}")
    print("  " + "-" * 75)

    for _, rad in df.iterrows():
        start_str = pd.Timestamp(rad["start"]).strftime("%m-%d %H:%M")
        intervall = f"{int(rad['timer'])}t"
        print(
            f"  {start_str:<20} {intervall:>3}  "
            f"{rad['temperatur_c']:>5.1f}  "
            f"{rad['nedbør_mm']:>6.1f}mm  "
            f"{rad['ny_snø_mm']:>6.1f}mm  "
            f"{rad['smelting_mm']:>5.1f}mm  "
            f"{rad['netto_mm']:>+6.1f}mm  "
            f"{rad['snødybde_cm']:>8.1f}cm"
        )


def skriv_daglig_sammendrag(df: pd.DataFrame) -> None:
    df = df.copy()
    df["dato"] = pd.to_datetime(df["start"]).dt.date
    daglig = (
        df.groupby("dato")
        .agg(
            min_temp_c        = ("temperatur_c", "min"),
            maks_temp_c       = ("temperatur_c", "max"),
            total_nedbør_mm   = ("nedbør_mm",    "sum"),
            total_ny_snø_mm   = ("ny_snø_mm",    "sum"),
            total_smelting_mm = ("smelting_mm",  "sum"),
            snødybde_slutt_cm = ("snødybde_cm",  "last"),
        )
        .round(1)
    )
    print("\n  Daglig sammendrag:")
    print(daglig.to_string())


# ══════════════════════════════════════════════════════════════════════════════
#  Hovedprogram
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    auth    = _env_auth()
    session = requests.Session()

    for navn, config in STASJONER.items():
        print(f"\n{'='*60}")
        print(f"  Snøprognose: {navn}")
        print(f"  Smelterate: {SMELTING_MM_PER_GRAD_TIME} mm/°C/t  |  "
              f"Terskel: {SMELTING_TERSKEL_C} °C")
        print(f"{'='*60}")

        # 1) Observert snødybde
        snødybde_cm = hent_snødybde_frost(config["frost_id"], session=session, auth=auth)
        if snødybde_cm is None:
            snødybde_cm = 0.0
            print(f"  Bruker startverdi: {snødybde_cm} cm (ingen Frost-data)")
        else:
            print(f"  Startverdi snødybde: {snødybde_cm} cm")

        # 2) YR-varsel
        print("  Henter YR-varsel...")
        intervaller = hent_intervaller(config["place"])
        antall_1t  = sum(1 for iv in intervaller if iv["timer"] == 1.0)
        antall_6t  = sum(1 for iv in intervaller if iv["timer"] == 6.0)
        print(f"  Hentet {len(intervaller)} intervaller "
              f"({antall_1t} × 1t,  {antall_6t} × 6t)")

        # 3) Simuler
        df = simuler_snøprognose(intervaller, snødybde_cm)

        # 4) Vis intervall-tabell
        skriv_intervall_tabell(df)

        # 5) Vis daglig sammendrag
        skriv_daglig_sammendrag(df)

        # 6) Lagre CSV
        filnavn = f"snøprognose_{navn.lower().replace(' ', '_')}_{now_str}.csv"
        df.to_csv(filnavn, index=False, sep=";", encoding="utf-16")
        print(f"\n  Detaljert prognose lagret til: {filnavn}")


if __name__ == "__main__":
    main()
