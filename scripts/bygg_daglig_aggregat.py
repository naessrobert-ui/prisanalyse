"""
scripts/bygg_daglig_aggregat.py - Daglig prisaggregat per biltype
==================================================================

Leser daglige CSV-snapshots fra s3://prisanalyse-data/raw/bil-daglig/, kjorer
variant-klassifisereren, og skriver partisjonert prisaggregat til
s3://prisanalyse-data/calc/bil/aggregat/dato=YYYY-MM-DD/data.parquet.

Aggregatet har en rad per (Produsent, Modell, aarstall, drivstoff, hjuldrift,
Karosseri, variant_id, km_bin) per dag, med pris-percentiler og volum.
Analyseportalen leser herfra for aa lage prisutvikling og volumkurver.

Kjor lokalt:
    # Backfill alle dager som mangler aggregat:
    python -m scripts.bygg_daglig_aggregat --backfill

    # Bare i gaar:
    python -m scripts.bygg_daglig_aggregat

    # Spesifikk dato:
    python -m scripts.bygg_daglig_aggregat --dato 2025-05-20

    # Tving rebuild av en dag som allerede er skrevet:
    python -m scripts.bygg_daglig_aggregat --dato 2025-05-20 --force

Brukes av:
  - Render cron (daglig kl. 04 UTC) for inkrementell oppdatering
  - Manuelt for backfill av historiske dager
"""

from __future__ import annotations

import argparse
import io
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

# ---- S3-konfig ----
RAW_PREFIX = "raw/bil-daglig/"
AGGREGAT_PREFIX = "calc/bil/aggregat/"

# ---- Aggregeringsgrenser ----
MIN_GRUPPESTORRELSE = 3  # filtrer bort grupper med faerre annonser - for stoyete

KM_BINS = [0, 30_000, 60_000, 100_000, 150_000, 200_000, float("inf")]
KM_BIN_LABELS = ["0-30k", "30-60k", "60-100k", "100-150k", "150-200k", "200k+"]


def _s3_client():
    import boto3
    from config import AWS_KEY, AWS_SECRET, AWS_REGION
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def _bucket() -> str:
    from config import S3_BUCKET_NAME
    return S3_BUCKET_NAME


# ---- Lasting ----

def list_raw_csvs(s3) -> list[dict]:
    """Returner liste over alle daglige CSV-objekter med LastModified-dato."""
    objekter: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for side in paginator.paginate(Bucket=_bucket(), Prefix=RAW_PREFIX):
        for o in side.get("Contents", []):
            if o["Key"].lower().endswith(".csv") and o["Size"] > 0:
                o["_dato"] = o["LastModified"].date()
                objekter.append(o)
    return objekter


def list_eksisterende_aggregat_dager(s3) -> set[date]:
    """Returner sett over datoer som allerede har aggregat skrevet."""
    dager: set[date] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for side in paginator.paginate(Bucket=_bucket(), Prefix=AGGREGAT_PREFIX):
        for o in side.get("Contents", []):
            # Forventet noekkel: calc/bil/aggregat/dato=YYYY-MM-DD/data.parquet
            key = o["Key"]
            biter = key.split("/")
            for b in biter:
                if b.startswith("dato="):
                    try:
                        d = datetime.strptime(b[5:], "%Y-%m-%d").date()
                        dager.add(d)
                    except ValueError:
                        pass
    return dager


def les_csv_fra_s3(s3, key: str) -> pd.DataFrame:
    """Last en daglig CSV. utf-16 + ; er standard fra scraperen, fall til
    utf-8 + ; for sikkerhets skyld."""
    obj = s3.get_object(Bucket=_bucket(), Key=key)
    raw = obj["Body"].read()
    for enc in ("utf-16", "utf-8"):
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=";")
            if len(df.columns) > 1:
                return df
        except Exception:
            continue
    raise RuntimeError(f"Klarte ikke parse CSV: {key}")


# ---- Forprosessering ----

def _coerce_pris(s: pd.Series) -> pd.Series:
    """Tilgi 'kr', mellomrom, komma, og fang "solgt"-tekst som NaN."""
    if s.dtype.kind in ("i", "f"):
        return pd.to_numeric(s, errors="coerce")
    s2 = (
        s.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("kr", "", case=False, regex=False)
        .str.replace(",", ".", regex=False)
    )
    # Tekstverdier som "solgt", "fjernet" -> NaN
    s2 = s2.where(s2.str.match(r"^-?\d+(\.\d+)?$", na=False), np.nan)
    return pd.to_numeric(s2, errors="coerce")


def _coerce_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def klargjor_for_aggregering(df: pd.DataFrame, katalog: pd.DataFrame) -> pd.DataFrame:
    """Tilfor variant_id og km_bin, og normaliser kategorifelter."""
    df = df.copy()

    # aarstall kan vaere baade 'aarstall' og 'årstall' avhengig av kilde
    if "aarstall" not in df.columns and "årstall" in df.columns:
        df["aarstall"] = df["årstall"]

    # kjorelengde-tilsvarende
    km_kol = None
    for kand in ("kjørelengde", "kjorelengde", "Km", "km"):
        if kand in df.columns:
            km_kol = kand
            break
    df["_km"] = _coerce_int(df[km_kol]) if km_kol else np.nan

    # Pris: foretrekk Pris_ny (sist observerte pris paa denne FinnKode i dette
    # snapshotet); fall til Pris.
    pris = None
    for kand in ("Pris_ny", "Pris", "pris_num", "pris"):
        if kand in df.columns:
            kand_s = _coerce_pris(df[kand])
            pris = kand_s if pris is None else pris.where(pris.notna(), kand_s)
    df["_pris"] = pris if pris is not None else np.nan

    # Variant-klassifisering
    vid, kilde = klassifiser_varianter(df, katalog)
    df["variant_id"] = vid
    df["variant_kilde"] = kilde

    # km_bin
    df["km_bin"] = pd.cut(
        df["_km"], bins=KM_BINS, labels=KM_BIN_LABELS, right=False, include_lowest=True
    ).astype(str)
    df.loc[df["_km"].isna(), "km_bin"] = "ukjent"

    # Normaliser kategorier som vi grupperer paa - tomme/NaN -> "Ukjent"
    for kol in ("Produsent", "Modell", "drivstoff", "hjuldrift", "Karosseri"):
        if kol not in df.columns:
            df[kol] = "Ukjent"
        df[kol] = df[kol].fillna("Ukjent").astype(str).str.strip().replace("", "Ukjent")

    df["aarstall"] = _coerce_int(df["aarstall"]).astype("Int64")

    # Variant blank -> "ikke_klassifisert" (lettere aa filtrere i frontend)
    df["variant_id"] = df["variant_id"].fillna("ikke_klassifisert").astype(str)

    return df


# ---- Aggregering ----

GRUPPENOKLER = [
    "Produsent",
    "Modell",
    "aarstall",
    "drivstoff",
    "hjuldrift",
    "Karosseri",
    "variant_id",
    "km_bin",
]


def bygg_aggregat(df: pd.DataFrame, dag: date) -> pd.DataFrame:
    """Group_by gruppenokler og beregn pris- og volum-statistikk."""
    gyldig = df[df["_pris"].notna() & (df["_pris"] > 1000) & df["aarstall"].notna()].copy()
    if gyldig.empty:
        return pd.DataFrame()

    grp = gyldig.groupby(GRUPPENOKLER, dropna=False, observed=True)
    agg = grp["_pris"].agg(
        n_annonser="size",
        median_pris="median",
        mean_pris="mean",
        p25_pris=lambda s: s.quantile(0.25),
        p75_pris=lambda s: s.quantile(0.75),
        min_pris="min",
        max_pris="max",
    )
    median_km = grp["_km"].median().rename("median_km")
    agg = agg.join(median_km).reset_index()

    # Filtrer stoyete sma grupper
    agg = agg[agg["n_annonser"] >= MIN_GRUPPESTORRELSE]
    if agg.empty:
        return agg

    # Cast tall ned til kompakt dtype - sparer mye plass over millioner rader
    for kol in ("median_pris", "mean_pris", "p25_pris", "p75_pris", "min_pris", "max_pris", "median_km"):
        agg[kol] = agg[kol].round().astype("Int64")
    agg["n_annonser"] = agg["n_annonser"].astype("int32")
    agg["aarstall"] = agg["aarstall"].astype("Int64")
    agg.insert(0, "dato", pd.Timestamp(dag).date())

    return agg


# ---- Skriving ----

def skriv_aggregat(s3, agg: pd.DataFrame, dag: date) -> str:
    """Skriv aggregat for en dag til S3 som hive-partisjonert parquet."""
    if agg.empty:
        print(f"  [skriv] Ingen rader for {dag} - hopper over")
        return ""

    table = pa.Table.from_pandas(agg, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=6)
    key = f"{AGGREGAT_PREFIX}dato={dag:%Y-%m-%d}/data.parquet"
    s3.put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())
    print(f"  [skriv] {len(agg):,} grupper -> s3://{_bucket()}/{key}  ({buf.tell()/1024:.0f} KB)")
    return key


# ---- Hovedlogikk ----

def prosesser_en_dag(s3, csv_objekt: dict, dag: date, katalog: pd.DataFrame) -> bool:
    key = csv_objekt["Key"]
    print(f"\n[dag {dag}] Leser {key}  ({csv_objekt['Size']/1024/1024:.1f} MB)")
    try:
        df = les_csv_fra_s3(s3, key)
    except Exception as e:
        print(f"  [feil] Klarte ikke lese CSV: {e}")
        return False
    print(f"  Rader: {len(df):,}")

    df = klargjor_for_aggregering(df, katalog)
    n_klassifisert = (df["variant_id"] != "ikke_klassifisert").sum()
    print(
        f"  Variant-klassifisering: {n_klassifisert:,} / {len(df):,} "
        f"({100*n_klassifisert/max(1,len(df)):.1f}%)"
    )

    agg = bygg_aggregat(df, dag)
    print(f"  Aggregat: {len(agg):,} grupper")

    skriv_aggregat(s3, agg, dag)
    return True


def velg_dager_aa_kjore(
    s3,
    eksplisitt_dato: date | None,
    backfill: bool,
    force: bool,
) -> list[tuple[date, dict]]:
    """Returnerer (dato, csv-objekt)-par som skal prosesseres."""
    csv_objekter = list_raw_csvs(s3)
    # Hvis flere CSV-er deler dato, behold den nyeste (siste snapshot pr dag)
    per_dag: dict[date, dict] = {}
    for o in csv_objekter:
        d = o["_dato"]
        if d not in per_dag or o["LastModified"] > per_dag[d]["LastModified"]:
            per_dag[d] = o

    eksisterende = set() if force else list_eksisterende_aggregat_dager(s3)

    if eksplisitt_dato is not None:
        if eksplisitt_dato in per_dag and (force or eksplisitt_dato not in eksisterende):
            return [(eksplisitt_dato, per_dag[eksplisitt_dato])]
        if eksplisitt_dato not in per_dag:
            print(f"[velg] Ingen CSV funnet for {eksplisitt_dato}")
        else:
            print(f"[velg] {eksplisitt_dato} allerede prosessert (bruk --force for aa kjore paa nytt)")
        return []

    if backfill:
        dager = sorted(d for d in per_dag if d not in eksisterende)
        return [(d, per_dag[d]) for d in dager]

    # Default: i gaar (eller siste tilgjengelige hvis i gaar mangler)
    i_gaar = date.today() - timedelta(days=1)
    if i_gaar in per_dag and (force or i_gaar not in eksisterende):
        return [(i_gaar, per_dag[i_gaar])]

    # Fall: siste tilgjengelige dag som ikke er prosessert
    for d in sorted(per_dag.keys(), reverse=True):
        if force or d not in eksisterende:
            return [(d, per_dag[d])]
    print("[velg] Alle tilgjengelige dager er allerede prosessert")
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Bygg daglig prisaggregat for bil-annonser")
    parser.add_argument("--dato", type=str, default=None, help="Spesifikk dato YYYY-MM-DD (default: i gaar)")
    parser.add_argument("--backfill", action="store_true", help="Bygg alle dager som mangler aggregat")
    parser.add_argument("--force", action="store_true", help="Tving rebuild selv om dagen allerede er prosessert")
    args = parser.parse_args()

    eksplisitt_dato: date | None = None
    if args.dato:
        eksplisitt_dato = datetime.strptime(args.dato, "%Y-%m-%d").date()

    s3 = _s3_client()
    katalog = last_variantkatalog()
    if katalog.empty:
        print("[main] Variantkatalog er tom - klassifisering blir blank, men aggregering kjorer.")

    oppgaver = velg_dager_aa_kjore(s3, eksplisitt_dato, args.backfill, args.force)
    print(f"[main] {len(oppgaver)} dag(er) skal prosesseres")

    ok = 0
    for dag, obj in oppgaver:
        if prosesser_en_dag(s3, obj, dag, katalog):
            ok += 1
    print(f"\n[main] Ferdig: {ok}/{len(oppgaver)} dager skrevet")
    return 0 if ok == len(oppgaver) else 1


if __name__ == "__main__":
    sys.exit(main())
