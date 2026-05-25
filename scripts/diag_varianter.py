"""
scripts/diag_varianter.py - Diagnose for variant-klassifiserer
==============================================================

Laster ned database_biler.parquet fra S3, kjorer klassifisereren, og
rapporterer:
  * Hvor mange EV-annonser som ble klassifisert per kilde
    (enkel / alias / rekkevidde / ukjent)
  * Topp 20 (Produsent, Modell)-kombinasjoner blant EV-er som IKKE
    finnes i katalogen - kandidater til neste runde med utviding.
  * For hver klassifiserte variant: antall annonser + min/median/max
    rekkevidde i annonsen. Sjekk om rekkevidde-distribusjonen ser
    fornuftig ut.
  * For valgte populaere modeller: enkel klyngeanalyse paa rekkevidde
    (Gaussian Mixture, opp til 4 komponenter) for aa flagge modeller
    der antall klynger != antall katalog-varianter.

Kjor lokalt:
    python -m scripts.diag_varianter

Kjor mot en lokal parquet (raskere ved iterering):
    python -m scripts.diag_varianter --parquet C:/path/database_biler.parquet
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Sorg for at vi kan importere fra repo-rot uavhengig av cwd
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

from bil_variant_klassifiserer import (  # noqa: E402
    klassifiser_varianter,
    last_variantkatalog,
)

S3_BUCKET_DEFAULT = "prisanalyse-data"
S3_KEY = "calc/bil/database_biler.parquet"

KOLONNER = [
    "FinnKode",
    "Produsent",
    "Modell",
    "Overskrift",
    "årstall",
    "drivstoff",
    "rekkevidde_str",
    "rekkevidde_wltp_km",
    "batterikapasitet_kwh",
    "svv_handelsbetegnelse",
    "Pris",
    "Pris_ny",
]


def _last_parquet_lokalt(path: Path) -> pd.DataFrame:
    # Hvilke av oensket-kolonner finnes faktisk i parquet-en
    schema = pq.read_schema(str(path))
    tilgjengelig = set(schema.names)
    cols = [c for c in KOLONNER if c in tilgjengelig]
    table = pq.read_table(str(path), columns=cols)
    return table.to_pandas()


def _last_parquet_fra_s3() -> pd.DataFrame:
    import boto3
    from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

    bucket = S3_BUCKET_NAME or S3_BUCKET_DEFAULT
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )
    cache = Path(tempfile.gettempdir()) / "diag_varianter_database_biler.parquet"
    if not cache.exists():
        print(f"[diag] Laster ned s3://{bucket}/{S3_KEY} -> {cache}")
        obj = s3.get_object(Bucket=bucket, Key=S3_KEY)
        cache.write_bytes(obj["Body"].read())
    else:
        print(f"[diag] Bruker cache: {cache}")
    return _last_parquet_lokalt(cache)


def _parse_rekkevidde(v) -> float:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return float("nan")
    s = str(v).lower().replace(",", ".")
    digits = "".join(ch if (ch.isdigit() or ch == ".") else " " for ch in s).split()
    if not digits:
        return float("nan")
    try:
        return float(digits[0])
    except Exception:
        return float("nan")


def _er_elbil(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.lower().str.startswith("el")


def kjor_diagnose(df: pd.DataFrame) -> None:
    print(f"\n[diag] Totalt rader i parquet: {len(df):,}")

    # Avgrens til EV-er (drivstoff begynner med 'el')
    mask_ev = _er_elbil(df["drivstoff"])
    ev = df.loc[mask_ev].copy()
    print(f"[diag] EV-rader: {len(ev):,}")

    if ev.empty:
        print("[diag] Ingen EV-er funnet - avbryter.")
        return

    # Bygg rekkevidde_km. Foretrekk wltp hvis fylt ut, fall til rekkevidde_str
    # (som her er int64). Sanitiser: alt utenfor 50-1200 km er datafeil (folk
    # skriver km-stand i feil felt; vi har sett verdier opp til 514000).
    if "rekkevidde_km" not in ev.columns:
        rk = pd.Series([np.nan] * len(ev), index=ev.index, dtype=float)
        for kolonne in ("rekkevidde_wltp_km", "rekkevidde_str"):
            if kolonne in ev.columns:
                kand = pd.to_numeric(ev[kolonne], errors="coerce")
                rk = rk.where(rk.notna(), kand)
        if rk.isna().all() and "rekkevidde_str" in ev.columns:
            rk = ev["rekkevidde_str"].apply(_parse_rekkevidde)
        rk = rk.where((rk >= 50) & (rk <= 1200), np.nan)
        ev["rekkevidde_km"] = rk

    # Dekning av batterikapasitet_kwh blant EV-er (avgjor om vi kan stole paa
    # det som primaer-signal).
    if "batterikapasitet_kwh" in ev.columns:
        kwh_serie = pd.to_numeric(ev["batterikapasitet_kwh"], errors="coerce")
        kwh_serie = kwh_serie.where((kwh_serie >= 15) & (kwh_serie <= 200))
        n_kwh = kwh_serie.notna().sum()
        print(
            f"[diag] batterikapasitet_kwh fylt ut: "
            f"{n_kwh:,} / {len(ev):,}  ({100*n_kwh/len(ev):.1f}%)"
        )
    else:
        print("[diag] batterikapasitet_kwh-kolonne mangler")

    katalog = last_variantkatalog()
    if katalog.empty:
        print("[diag] Katalog er tom - sjekk data/bil_varianter.csv")
        return

    variant_id, kilde = klassifiser_varianter(ev, katalog)
    ev["variant_id"] = variant_id
    ev["variant_kilde"] = kilde

    # ---- 1) Klassifiseringsdekning ----
    print("\n" + "=" * 70)
    print("KLASSIFISERINGSDEKNING (av EV-rader)")
    print("=" * 70)
    n_total = len(ev)
    n_klassifisert = ev["variant_id"].notna().sum()
    print(f"Klassifisert:     {n_klassifisert:,} / {n_total:,}  "
          f"({100*n_klassifisert/n_total:.1f}%)")
    print("\nFordeling per kilde:")
    print(ev["variant_kilde"].fillna("ikke_i_katalog").value_counts().to_string())

    # ---- 2) Top ikke-dekket (Produsent, Modell) ----
    print("\n" + "=" * 70)
    print("TOPP 20 (PRODUSENT, MODELL) IKKE DEKKET AV KATALOG")
    print("=" * 70)
    udekket = ev[ev["variant_id"].isna()]
    top = (
        udekket.groupby(["Produsent", "Modell"])
        .size()
        .sort_values(ascending=False)
        .head(20)
    )
    if top.empty:
        print("(ingen)")
    else:
        for (p, m), n in top.items():
            print(f"  {n:>6,}  {p}  {m}")

    # ---- 3) Per variant: antall + rekkevidde-statistikk ----
    print("\n" + "=" * 70)
    print("PER VARIANT: ANTALL OG REKKEVIDDE-DISTRIBUSJON")
    print("=" * 70)
    klassifisert = ev[ev["variant_id"].notna()].copy()
    agg = klassifisert.groupby("variant_id").agg(
        n=("variant_id", "size"),
        rekke_min=("rekkevidde_km", "min"),
        rekke_p25=("rekkevidde_km", lambda s: s.quantile(0.25)),
        rekke_med=("rekkevidde_km", "median"),
        rekke_p75=("rekkevidde_km", lambda s: s.quantile(0.75)),
        rekke_max=("rekkevidde_km", "max"),
    )
    agg = agg.sort_values("n", ascending=False)
    print(agg.to_string(float_format=lambda x: f"{x:.0f}" if pd.notna(x) else "-"))

    # ---- 4) Klyngeanalyse for modeller med >=2 katalog-varianter ----
    print("\n" + "=" * 70)
    print("KLYNGEANALYSE (sanity-sjekk: matcher antall topper antall varianter?)")
    print("=" * 70)
    try:
        from sklearn.mixture import GaussianMixture
    except ImportError:
        print("(sklearn ikke tilgjengelig - hopper over)")
        return

    # Modeller med 2+ varianter i katalog
    kat_counts = (
        katalog.groupby(["Produsent", "Modell"]).size().reset_index(name="n_var")
    )
    flerevariant = kat_counts[kat_counts["n_var"] >= 2]

    # Map (Produsent, Modell) -> sett av variant_id som katalogen produserer for
    # den modellen. Da kan vi bruke faktisk klassifiserte annonser i stedet for
    # modell-substring (som lekket Ioniq 5 inn i IONIQ-klyngen).
    katalog_grupper = katalog.groupby(["Produsent", "Modell"])["variant_id"].apply(set).to_dict()

    for _, row in flerevariant.iterrows():
        p, m, n_var = row["Produsent"], row["Modell"], int(row["n_var"])
        vids = katalog_grupper.get((p, m), set())
        delsett = ev[
            ev["variant_id"].isin(vids)
            & ev["rekkevidde_km"].notna()
            & (ev["rekkevidde_km"] >= 50)
            & (ev["rekkevidde_km"] <= 1200)
        ]
        if len(delsett) < 30:
            continue

        x = delsett["rekkevidde_km"].values.reshape(-1, 1)
        beste_k = 1
        beste_bic = float("inf")
        sentre: list[float] = []
        for k in range(1, min(5, n_var + 2)):
            try:
                gmm = GaussianMixture(n_components=k, random_state=42).fit(x)
                bic = gmm.bic(x)
                if bic < beste_bic:
                    beste_bic = bic
                    beste_k = k
                    sentre = sorted(float(c) for c in gmm.means_.flatten())
            except Exception:
                pass
        flagg = "  <-- FLAGG" if beste_k != n_var else ""
        sentre_s = ", ".join(f"{c:.0f}" for c in sentre)
        print(
            f"  {p} {m}: n={len(delsett):,}  "
            f"katalog={n_var}  klynger={beste_k}  sentre=[{sentre_s}]{flagg}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose variant-klassifiserer")
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Lokal sti til database_biler.parquet (hvis ikke gitt, lastes fra S3)",
    )
    args = parser.parse_args()

    if args.parquet and args.parquet.exists():
        print(f"[diag] Leser lokal parquet: {args.parquet}")
        df = _last_parquet_lokalt(args.parquet)
    else:
        df = _last_parquet_fra_s3()

    kjor_diagnose(df)


if __name__ == "__main__":
    main()
