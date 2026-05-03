# diagnose_daily_completeness.py
# Sjekker om daily-CSV-filene faktisk dekker alle biler.
# Hvis JA, er Dato_ny-buggen et merge-problem.
# Hvis NEI, er det scraper-problemet.

import io
import re
from datetime import datetime
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

# ---- 1) List de siste daily- og hourly-filene ----
def list_csv_files(prefix, max_files=10):
    resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    files = [
        o for o in resp.get("Contents", [])
        if o["Key"].lower().endswith(".csv") and o["Size"] > 0
    ]
    files.sort(key=lambda o: o["LastModified"], reverse=True)
    return files[:max_files]

daily_files = list_csv_files("raw/bil-daglig/", 7)
hourly_files = list_csv_files("raw/bil-time/", 10)

print("=" * 70)
print("SISTE 7 DAILY-FILER")
print("=" * 70)
for f in daily_files:
    print(f"  {f['LastModified']:%Y-%m-%d %H:%M}  {f['Size']/1024/1024:6.2f} MB  {f['Key']}")

print()
print("=" * 70)
print("SISTE 10 HOURLY-FILER")
print("=" * 70)
for f in hourly_files:
    print(f"  {f['LastModified']:%Y-%m-%d %H:%M}  {f['Size']/1024/1024:6.2f} MB  {f['Key']}")

# ---- 2) Les de 3 nyeste daily-filene og tell unike FinnKoder ----
def read_csv_finnkoder(key):
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    content = obj["Body"].read()
    # Prov utf-16 med semikolon (samme som konsolider_data)
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-16", sep=";")
    except Exception:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", sep=";")

    # Finn FinnKode-kolonnen uansett case
    fk_col = None
    for c in df.columns:
        if c.lower().replace("_", "").replace(" ", "") in ("finnkode", "finnid"):
            fk_col = c
            break
    if fk_col is None:
        return None, list(df.columns), len(df)

    codes = df[fk_col].astype(str).str.extract(r"(\d+)", expand=False).dropna()
    return set(codes.values), list(df.columns), len(df)

print()
print("=" * 70)
print("UNIKE FinnKoder PER DAILY-FIL")
print("=" * 70)

daily_sets = []
for f in daily_files[:7]:
    codes, cols, n_rows = read_csv_finnkoder(f["Key"])
    if codes is None:
        print(f"  {f['Key']}: FANT IKKE FinnKode-kolonne! Kolonner: {cols[:5]}...")
        continue
    print(f"  {f['LastModified']:%Y-%m-%d %H:%M}  rader={n_rows:>7,}  unike FinnKode={len(codes):>7,}  {f['Key'].split('/')[-1]}")
    daily_sets.append((f["LastModified"], codes))

# ---- 3) Overlap mellom etterfolgende daily-filer ----
if len(daily_sets) >= 2:
    print()
    print("=" * 70)
    print("OVERLAP MELLOM DAILY-FILER (kronologisk eldste til nyeste)")
    print("=" * 70)
    daily_sets_sorted = sorted(daily_sets, key=lambda x: x[0])
    for i in range(len(daily_sets_sorted) - 1):
        ts_a, set_a = daily_sets_sorted[i]
        ts_b, set_b = daily_sets_sorted[i + 1]
        felles = len(set_a & set_b)
        bare_a = len(set_a - set_b)
        bare_b = len(set_b - set_a)
        print(f"  {ts_a:%m-%d %H:%M} -> {ts_b:%m-%d %H:%M}: "
              f"felles={felles:>7,}  forsvunnet={bare_a:>6,}  nye={bare_b:>6,}")

# ---- 4) Sammenlign siste daily med database_biler_siste.parquet ----
if daily_sets:
    print()
    print("=" * 70)
    print("SAMMENLIGNING: SISTE DAILY vs database_biler_siste.parquet")
    print("=" * 70)

    latest_ts, latest_daily_set = sorted(daily_sets, key=lambda x: x[0])[-1]
    print(f"Siste daily:  {latest_ts} med {len(latest_daily_set):,} unike FinnKoder")

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler_siste.parquet")
    df_siste = pq.read_table(
        pa.BufferReader(obj["Body"].read()), columns=["FinnKode"]
    ).to_pandas()
    siste_set = set(df_siste["FinnKode"].astype(str).values)
    print(f"database_biler_siste.parquet: {len(siste_set):,} unike FinnKoder")

    felles = len(latest_daily_set & siste_set)
    kun_daily = len(latest_daily_set - siste_set)
    kun_siste = len(siste_set - latest_daily_set)
    print(f"  felles:              {felles:>7,}")
    print(f"  kun i daily:         {kun_daily:>7,}  (skulle vaert svart liten - nye biler siden siste scrape)")
    print(f"  kun i siste-parquet: {kun_siste:>7,}  (HER er problemet hvis stort)")

    if kun_siste > 1000:
        print()
        print(f"  DIAGNOSE: {kun_siste:,} biler er i siste-parquet men IKKE i siste daily.")
        print(f"  Daily-jobben er altsaa ikke komplett - den mangler mange biler.")

# ---- 5) Sammenlign siste daily med database_biler.parquet (alle aktive) ----
if daily_sets:
    print()
    print("=" * 70)
    print("SAMMENLIGNING: SISTE DAILY vs database_biler.parquet (Solgt=NEI)")
    print("=" * 70)

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler.parquet")
    df_main = pq.read_table(
        pa.BufferReader(obj["Body"].read()),
        columns=["FinnKode", "Solgt"],
    ).to_pandas()
    df_main["FinnKode"] = df_main["FinnKode"].astype(str)
    aktive_main = set(df_main.loc[df_main["Solgt"] == "NEI", "FinnKode"].values)
    print(f"Aktive (NEI) i hovedfilen: {len(aktive_main):,}")
    felles = len(latest_daily_set & aktive_main)
    kun_main = aktive_main - latest_daily_set
    print(f"  felles:                           {felles:,}")
    print(f"  NEI i hovedfil men IKKE i daily:  {len(kun_main):,}")
