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

from bilradar_lookup import _normaliser_hjuldrift  # noqa: E402
from bil_variant_klassifiserer import (  # noqa: E402
    klassifiser_varianter,
    last_variantkatalog,
)

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

# Filtrering — krever minst saa mange salg for at gruppen tas med
MIN_OBS_PER_GRUPPE = 5

# For lokal (variant-poolet) km-slope kreves mer data + km-spredning
MIN_OBS_FOR_LOKAL_SLOPE = 12
MIN_KM_STD_FOR_SLOPE = 5_000.0  # standardavvik i km innen variant

# Prosentvis km-slope (log-rom): pris = median_pris * exp(slope * (km - median_km)).
# Klemmes til [-1e-4, 0] slik at prisen alltid faller (eller er flat) med km.
# -1e-4 tilsvarer ~-10 % per 1000 km (ekstremt bratt); -8e-6 (~-0,8 %/1000 km)
# er en konservativ default naar vi knapt har lokale slopes.
PCT_SLOPE_MIN = -1e-4
PCT_SLOPE_MAX = 0.0
PCT_SLOPE_DEFAULT = -8e-6

# Hurtigpris: median blant biler solgt innen saa mange dager. Faller tilbake
# til p25 naar gruppa har faerre enn MIN_OBS_HURTIG hurtigsalg.
HURTIG_DAGER = 3
MIN_OBS_HURTIG = 3

# Innbyttepris = forventet_pris * (1 - INNBYTTE_RABATT), gulvet mot hurtigpris.
INNBYTTE_RABATT = 0.15

S3_INPUT_KEY = "calc/bil/database_biler.parquet"
# Må matche BILRADAR_LOOKUP_S3_KEY-defaulten i bilradar_lookup.py.
S3_OUTPUT_KEY = os.getenv("BILRADAR_LOOKUP_S3_KEY", "calc/bil/prislookup.csv")


def _s3_klient_og_bucket():
    import boto3
    from config import S3_BUCKET_NAME, AWS_KEY, AWS_SECRET, AWS_REGION

    s3 = boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )
    return s3, S3_BUCKET_NAME


def _hent_fra_s3() -> str:
    s3, bucket = _s3_klient_og_bucket()
    local = os.path.join(tempfile.gettempdir(), "database_biler.parquet")
    print(f"[S3] Laster ned s3://{bucket}/{S3_INPUT_KEY} -> {local}")
    s3.download_file(bucket, S3_INPUT_KEY, local)
    return local


def _last_opp_til_s3(local_path: str):
    s3, bucket = _s3_klient_og_bucket()
    print(f"[S3] Laster opp {local_path} -> s3://{bucket}/{S3_OUTPUT_KEY}")
    s3.upload_file(local_path, bucket, S3_OUTPUT_KEY)
    print("[S3] Opplasting ferdig")


def _tilfor_variant(df: pd.DataFrame) -> pd.DataFrame:
    """Klassifiser hver rad til en variant_id (elbil-batteripakke) med samme
    logikk som den daglige aggregat-motoren. Biler som ikke finnes i
    variantkatalogen (all bensin/diesel + elbiler uten katalogoppslag) faar
    "ikke_klassifisert" og faller dermed tilbake paa den grove nøkkelen."""
    # Rekkevidde er en av tie-breakerne i klassifisereren; utled den fra
    # rekkevidde_str naar egen km-kolonne mangler.
    if "rekkevidde_km" not in df.columns and "rekkevidde_str" in df.columns:
        df["rekkevidde_km"] = pd.to_numeric(
            df["rekkevidde_str"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
        )

    katalog = last_variantkatalog()
    vid, _kilde = klassifiser_varianter(df, katalog)
    df["variant_id"] = vid
    df["variant_id"] = df["variant_id"].fillna("ikke_klassifisert").astype(str)
    return df


def _last_og_klargjor(path: str) -> pd.DataFrame:
    """Leser parquet og returnerer en ren DataFrame med kun radene vi vil
    bruke til lookup. Bruker faktiske salgspriser (Pris_ny) og krever at
    bilen har forsvunnet fra annonsene (er_solgt) — ellers bygger vi pris
    paa askingspriser, som typisk ligger over realisert pris."""
    df = pd.read_parquet(path)

    # database_biler.parquet lagrer mange strengkolonner som 'category' (satt i
    # konsolider_data). fillna/map med nye verdier feiler paa Categorical, saa
    # konverter dem tilbake til vanlige object-kolonner foer bearbeiding.
    cat_cols = [c for c in df.columns if isinstance(df[c].dtype, pd.CategoricalDtype)]
    if cat_cols:
        df[cat_cols] = df[cat_cols].astype(object)

    for c in ["Pris", "Pris_ny", "kjørelengde", "årstall", "batterikapasitet_kwh"]:
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
    df["hjuldrift"] = df["hjuldrift"].map(_normaliser_hjuldrift)

    df = df.dropna(subset=["salgspris", "kjørelengde", "årstall", "Dato_ny"])
    df = df[(df["salgspris"] > 0) & (df["kjørelengde"] >= 0)]
    df["årstall"] = df["årstall"].astype(int)

    # Dager paa markedet (for hurtigpris). NaN naar Dato mangler — de radene
    # teller da ikke som hurtigsalg, men bidrar fortsatt til median/km-slope.
    if "Dato" in df.columns:
        df["dager_paa_markedet"] = (df["Dato_ny"] - df["Dato"]).dt.days
    else:
        df["dager_paa_markedet"] = np.nan

    if "Dato_ny" in df.columns:
        siste = df["Dato_ny"].max()
        df = df[df["Dato_ny"] < siste]  # kun solgte/fjernede annonser

    df = _tilfor_variant(df)

    return df


def _global_pct_slope(lokale_slopes: list[float]) -> float:
    """Median av lokale prosent-slopes — fallback for varianter for tynne til
    en egen slope. Median er robust mot enkelt-varianter med rar km-spredning."""
    arr = np.array([s for s in lokale_slopes if not np.isnan(s)])
    if arr.size < 5:
        return PCT_SLOPE_DEFAULT
    return float(np.clip(np.median(arr), PCT_SLOPE_MIN, PCT_SLOPE_MAX))


def _pooled_pct_slope(sub: pd.DataFrame) -> float:
    """Prosentvis km-slope (i log-rom) for én variant, poolet paa tvers av
    aarganger. Prisniva varierer mellom aar, saa vi de-mean-er log(pris) og km
    innen hver aargang (fixed effects) foer vi fitter en felles helning gjennom
    origo. Det gir "hvor mange %% mister denne varianten per km", uavhengig av
    aarets prisniva — og lar oss holde et robust slope-tall selv naar den fine
    (variant, aar)-gruppa er tynn.

    Returnerer NaN naar grunnlaget er for tynt eller km-spredningen for liten."""
    d = sub.loc[sub["salgspris"] > 0, ["salgspris", "kjørelengde", "årstall"]].copy()
    if len(d) < MIN_OBS_FOR_LOKAL_SLOPE:
        return float("nan")

    d["_y"] = np.log(d["salgspris"].astype(float))
    d["_x"] = d["kjørelengde"].astype(float)
    d["_y_res"] = d["_y"] - d.groupby("årstall")["_y"].transform("mean")
    d["_x_res"] = d["_x"] - d.groupby("årstall")["_x"].transform("mean")

    # IQR-trim paa residualene for aa dempe feilpriser/rare utstyrspakker.
    x_lo, x_hi = d["_x_res"].quantile(0.05), d["_x_res"].quantile(0.95)
    y_lo, y_hi = d["_y_res"].quantile(0.05), d["_y_res"].quantile(0.95)
    m = d["_x_res"].between(x_lo, x_hi) & d["_y_res"].between(y_lo, y_hi)
    d = d[m]
    if len(d) < MIN_OBS_FOR_LOKAL_SLOPE:
        return float("nan")

    x = d["_x_res"].values
    y = d["_y_res"].values
    if np.std(x) < MIN_KM_STD_FOR_SLOPE:
        return float("nan")

    denom = float(np.sum(x * x))
    if denom <= 0:
        return float("nan")
    slope = float(np.sum(x * y) / denom)  # gjennom origo (residualene er sentrert)
    return float(np.clip(slope, PCT_SLOPE_MIN, PCT_SLOPE_MAX))


def _beregn_variant_slopes(df: pd.DataFrame) -> tuple[dict, float]:
    """Prosent-slope per (Produsent, Modell, variant_id) + global fallback."""
    slopes: dict = {}
    for key, sub in df.groupby(["Produsent", "Modell", "variant_id"], sort=False):
        s = _pooled_pct_slope(sub)
        if not np.isnan(s):
            slopes[key] = s
    glob = _global_pct_slope(list(slopes.values()))
    return slopes, glob


def bygg_lookup(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    variant_slopes, glob = _beregn_variant_slopes(df)
    print(
        f"[LOOKUP] Global km-slope (fallback): {glob*1000*100:.3f} %/1000 km "
        f"| variant-slopes: {len(variant_slopes)}"
    )

    grupper = df.groupby(
        ["Produsent", "Modell", "variant_id", "hjuldrift", "drivstoff", "årstall"],
        sort=True,
    )

    rader: list[dict] = []
    for (merke, modell, variant, hjul, dr, aar), sub in grupper:
        n = len(sub)
        if n < MIN_OBS_PER_GRUPPE:
            continue

        median_pris = int(round(sub["salgspris"].median()))
        p25_pris = int(round(sub["salgspris"].quantile(0.25)))
        p75_pris = int(round(sub["salgspris"].quantile(0.75)))

        # Hurtigpris: median blant biler solgt innen HURTIG_DAGER dager.
        # Faa hurtigsalg -> bruk p25 som proxy for markedsklarende pris.
        fast = sub[sub["dager_paa_markedet"].notna()
                   & (sub["dager_paa_markedet"] <= HURTIG_DAGER)]
        if len(fast) >= MIN_OBS_HURTIG:
            hurtigpris = int(round(fast["salgspris"].median()))
        else:
            hurtigpris = p25_pris
        # Hurtigpris skal aldri ligge over det typiske prisnivaaet.
        hurtigpris = min(hurtigpris, median_pris)

        innbyttepris = min(int(round(median_pris * (1 - INNBYTTE_RABATT))), hurtigpris)

        slope = variant_slopes.get((merke, modell, variant), glob)
        kilde = "variant" if (merke, modell, variant) in variant_slopes else "global"

        rader.append({
            "Produsent": merke,
            "Modell": modell,
            "variant_id": variant,
            "hjuldrift": hjul,
            "drivstoff": dr,
            "årstall": int(aar),
            "n_obs": n,
            "median_pris": median_pris,
            "median_km": int(round(sub["kjørelengde"].median())),
            "p25_pris": p25_pris,
            "p75_pris": p75_pris,
            "std_pris": int(round(sub["salgspris"].std() or 0)),
            "hurtigpris": hurtigpris,
            "innbyttepris": innbyttepris,
            "km_slope_pct": round(slope, 9),
            "km_slope_kilde": kilde,
        })

    out = pd.DataFrame(rader).sort_values(
        ["Produsent", "Modell", "variant_id", "drivstoff", "årstall"]
    ).reset_index(drop=True)
    return out, glob


def main():
    parser = argparse.ArgumentParser(description="Bygg prislookup-tabell fra salgsdata.")
    parser.add_argument("--input", help="Lokal parquet (database_biler.parquet)")
    parser.add_argument("--s3", action="store_true", help="Hent input fra S3")
    parser.add_argument("--output", default="data/prislookup.csv")
    parser.add_argument("--upload", action="store_true",
                        help="Last opp resultatet til S3 (calc/bil/prislookup.csv)")
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

    if args.upload:
        _last_opp_til_s3(args.output)

    print()
    print("Topp 15 grupper etter datagrunnlag:")
    print(
        lookup.sort_values("n_obs", ascending=False)
        .head(15)
        [["Produsent", "Modell", "variant_id", "drivstoff", "årstall",
          "n_obs", "median_pris", "hurtigpris", "innbyttepris",
          "km_slope_pct", "km_slope_kilde"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
