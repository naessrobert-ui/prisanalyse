"""
scripts/lag_prislookup.py
==========================
Bygger en transparent prislookup-tabell direkte fra salgsdata, som et
alternativ til ML-modellen for biler der den bommer for grovt.

Logikk:
  - Grupperer salg paa (Produsent, Modell, hjuldrift, drivstoff, aarstall).
  - For hver gruppe: median_pris og median_km. Antall obs vises slik at
    du selv kan se hvor solid grunnlaget er.
  - Per-gruppe km-justering (NOK per km) beregnes ved OLS hvis gruppen
    har minst MIN_OBS_FOR_LOKAL_SLOPE salg og nok km-spredning. Ellers
    faller tabellen tilbake paa en global slope (ogsaa lagret i CSV).

Pris-oppslag (gjoeres etterpaa, f.eks. i scoreren):
    pris = median_pris + km_slope * (bil_km - median_km)

Eksempel:
    python -m scripts.lag_prislookup --s3 --output data/prislookup.csv
    python -m scripts.lag_prislookup --input database_biler.parquet --output data/prislookup.csv
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

# Filtrering — krever minst saa mange salg for at gruppen tas med
MIN_OBS_PER_GRUPPE = 5

# For lokal km-slope kreves mer data + km-spredning
MIN_OBS_FOR_LOKAL_SLOPE = 12
MIN_KM_STD_FOR_SLOPE = 5_000.0  # standardavvik i km innen gruppe

S3_INPUT_KEY = "calc/bil/database_biler.parquet"


def _hent_fra_s3() -> str:
    import boto3
    from config import S3_BUCKET_NAME, AWS_KEY, AWS_SECRET, AWS_REGION

    s3 = boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )
    local = os.path.join(tempfile.gettempdir(), "database_biler.parquet")
    print(f"[S3] Laster ned s3://{S3_BUCKET_NAME}/{S3_INPUT_KEY} -> {local}")
    s3.download_file(S3_BUCKET_NAME, S3_INPUT_KEY, local)
    return local


def _last_og_klargjor(path: str) -> pd.DataFrame:
    """Leser parquet og returnerer en ren DataFrame med kun radene vi vil
    bruke til lookup. Bruker faktiske salgspriser (Pris_ny) og krever at
    bilen har forsvunnet fra annonsene (er_solgt) — ellers bygger vi pris
    paa askingspriser, som typisk ligger over realisert pris."""
    df = pd.read_parquet(path)

    for c in ["Pris", "Pris_ny", "kjørelengde", "årstall"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["Dato", "Dato_ny"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    df["salgspris"] = df["Pris_ny"].fillna(df.get("Pris"))

    for c in ["Produsent", "Modell", "hjuldrift", "drivstoff"]:
        if c not in df.columns:
            df[c] = "Ukjent"
        df[c] = df[c].fillna("Ukjent").astype(str).str.strip()

    df = df.dropna(subset=["salgspris", "kjørelengde", "årstall", "Dato_ny"])
    df = df[(df["salgspris"] > 0) & (df["kjørelengde"] >= 0)]
    df["årstall"] = df["årstall"].astype(int)

    if "Dato_ny" in df.columns:
        siste = df["Dato_ny"].max()
        df = df[df["Dato_ny"] < siste]  # kun solgte/fjernede annonser

    return df


def _global_km_slope(lokale_slopes: list[float]) -> float:
    """Median av lokale slopes — brukes som fallback for grupper for tynne
    til en egen slope. Mer robust og lettere aa forklare enn et OLS-fit
    over alle grupper, der ulike pris/km-skalaer trekker resultatet skjevt."""
    arr = np.array([s for s in lokale_slopes if not np.isnan(s)])
    if arr.size < 5:
        return -1.0  # Konservativ default hvis vi knapt har lokale slopes
    return float(np.median(arr))


def _lokal_slope(sub: pd.DataFrame) -> float:
    """OLS-slope for én gruppe etter IQR-trimming, som beskytter mot
    outliers (feilregistrerte priser, sjeldne utstyrspakker). Returnerer
    NaN hvis grunnlaget er for tynt eller km-spredningen er for liten."""
    if len(sub) < MIN_OBS_FOR_LOKAL_SLOPE:
        return float("nan")

    pris = sub["salgspris"]
    km = sub["kjørelengde"]
    p_lo, p_hi = pris.quantile(0.10), pris.quantile(0.90)
    k_lo, k_hi = km.quantile(0.05), km.quantile(0.95)
    mask = (pris >= p_lo) & (pris <= p_hi) & (km >= k_lo) & (km <= k_hi)
    if mask.sum() < MIN_OBS_FOR_LOKAL_SLOPE:
        return float("nan")

    km_v = km[mask].values
    if np.std(km_v) < MIN_KM_STD_FOR_SLOPE:
        return float("nan")
    slope, _ = np.polyfit(km_v, pris[mask].values, 1)
    return float(slope)


def bygg_lookup(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    grupper = df.groupby(
        ["Produsent", "Modell", "hjuldrift", "drivstoff", "årstall"], sort=True
    )

    # Foerste pass: samle base-statistikk og lokale slopes
    rader: list[dict] = []
    for (merke, modell, hjul, dr, aar), sub in grupper:
        n = len(sub)
        if n < MIN_OBS_PER_GRUPPE:
            continue
        slope_lokal = _lokal_slope(sub)
        rader.append({
            "Produsent": merke,
            "Modell": modell,
            "hjuldrift": hjul,
            "drivstoff": dr,
            "årstall": int(aar),
            "n_obs": n,
            "median_pris": int(round(sub["salgspris"].median())),
            "median_km": int(round(sub["kjørelengde"].median())),
            "p25_pris": int(round(sub["salgspris"].quantile(0.25))),
            "p75_pris": int(round(sub["salgspris"].quantile(0.75))),
            "std_pris": int(round(sub["salgspris"].std() or 0)),
            "_slope_lokal": slope_lokal,
        })

    glob = _global_km_slope([r["_slope_lokal"] for r in rader])
    print(f"[LOOKUP] Global km-slope (fallback): {glob:.3f} NOK/km")

    for r in rader:
        if np.isnan(r["_slope_lokal"]):
            r["km_slope"] = round(glob, 3)
            r["km_slope_kilde"] = "global"
        else:
            r["km_slope"] = round(r["_slope_lokal"], 3)
            r["km_slope_kilde"] = "lokal"
        del r["_slope_lokal"]

    out = pd.DataFrame(rader).sort_values(
        ["Produsent", "Modell", "drivstoff", "årstall"]
    ).reset_index(drop=True)
    return out, glob


def main():
    parser = argparse.ArgumentParser(description="Bygg prislookup-tabell fra salgsdata.")
    parser.add_argument("--input", help="Lokal parquet (database_biler.parquet)")
    parser.add_argument("--s3", action="store_true", help="Hent fra S3")
    parser.add_argument("--output", default="data/prislookup.csv")
    args = parser.parse_args()

    if args.s3:
        input_path = _hent_fra_s3()
    elif args.input:
        input_path = args.input
    else:
        parser.error("Spesifiser enten --input <fil> eller --s3")

    print("=" * 70)
    print(f"PRISLOOKUP — bygging startet {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    df = _last_og_klargjor(input_path)
    print(f"[DATA] Solgte rader brukt til lookup: {len(df):,}")

    lookup, glob = bygg_lookup(df)
    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    lookup.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"[SAVE] Skrev {len(lookup):,} grupper til {args.output}")
    print()
    print("Topp 15 grupper etter datagrunnlag:")
    print(
        lookup.sort_values("n_obs", ascending=False)
        .head(15)
        [["Produsent", "Modell", "drivstoff", "årstall",
          "n_obs", "median_pris", "median_km", "km_slope", "km_slope_kilde"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
