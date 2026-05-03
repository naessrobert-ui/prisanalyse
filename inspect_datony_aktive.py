# inspect_datony_aktive.py
# Sjekker Dato_ny for biler som ER i siste daily (altsaa aktive).
# Deres Dato_ny burde vaere lik siste daily-kjoring.

import io
from datetime import date
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

# ---- Les hovedfil ----
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler.parquet")
df = pq.read_table(
    pa.BufferReader(obj["Body"].read()),
    columns=["FinnKode", "Produsent", "Modell", "Dato", "Dato_ny", "Solgt"],
).to_pandas()
df["FinnKode"] = df["FinnKode"].astype(str)
df["Dato"] = pd.to_datetime(df["Dato"])
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"])

# ---- Les siste daily-fil ----
resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw/bil-daglig/")
daily_files = sorted(
    [o for o in resp.get("Contents", []) if o["Key"].endswith(".csv")],
    key=lambda o: o["LastModified"], reverse=True,
)
latest_daily = daily_files[0]
print(f"Siste daily: {latest_daily['Key']}")
print(f"Last modified: {latest_daily['LastModified']}")

obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=latest_daily["Key"])
content = obj["Body"].read()
df_daily = pd.read_csv(io.BytesIO(content), encoding="utf-16", sep=";")
fk_col = next(c for c in df_daily.columns if c.lower().replace("_","") in ("finnkode","finnid"))
daily_fk = set(df_daily[fk_col].astype(str).str.extract(r"(\d+)", expand=False).dropna().values)
print(f"Antall FinnKoder i siste daily: {len(daily_fk):,}")

# ---- Filter: biler som er i siste daily OG i hovedfilen ----
aktive_i_hovedfil = df[df["FinnKode"].isin(daily_fk)].copy()
print(f"\nAv de {len(daily_fk):,} i siste daily, finnes {len(aktive_i_hovedfil):,} i hovedfilen")
print()

# ---- Distribusjon av Dato_ny for disse aktive ----
print("=" * 70)
print("Dato_ny-FORDELING for biler som er i siste daily (altsaa aktive)")
print("=" * 70)
dag_dist = aktive_i_hovedfil["Dato_ny"].dt.date.value_counts().sort_index(ascending=False).head(20)
print(dag_dist)
print()

# ---- Solgt-fordeling for disse aktive ----
print("=" * 70)
print("Solgt-FORDELING for biler som er i siste daily")
print("=" * 70)
print(aktive_i_hovedfil["Solgt"].value_counts())
print()

# ---- Hvor mange aktive biler har Dato_ny eldre enn 2 dager? ----
print("=" * 70)
print("KRITISK: aktive biler med gammel Dato_ny")
print("=" * 70)
latest_daily_date = latest_daily["LastModified"].date()
dager_gamle = (pd.Timestamp(latest_daily_date) - aktive_i_hovedfil["Dato_ny"]).dt.days

for n in [0, 1, 2, 7, 30]:
    n_eldre = (dager_gamle > n).sum()
    print(f"  Dato_ny > {n} dager gammel: {n_eldre:,} ({n_eldre/len(aktive_i_hovedfil)*100:.1f}%)")

# ---- Eksempler paa aktive med gammel Dato_ny ----
print()
print("=" * 70)
print("10 EKSEMPLER: aktive biler (i siste daily) med gammel Dato_ny")
print("=" * 70)
gamle = aktive_i_hovedfil[dager_gamle > 2].sort_values("Dato_ny").head(10)
print(gamle[["FinnKode", "Produsent", "Modell", "Dato", "Dato_ny", "Solgt"]].to_string())

# ---- Sjekk om ogsaa Dato (foerste sett) ser rar ut ----
print()
print("=" * 70)
print("Dato-FORDELING (foerste gang sett) for aktive biler")
print("=" * 70)
print(f"Min:    {aktive_i_hovedfil['Dato'].min()}")
print(f"Max:    {aktive_i_hovedfil['Dato'].max()}")
print(f"Median: {aktive_i_hovedfil['Dato'].median()}")
# Foerste dag-fordeling
dato_dist = aktive_i_hovedfil["Dato"].dt.date.value_counts().sort_values(ascending=False).head(10)
print(f"\n10 vanligste Dato-verdier for aktive biler:")
print(dato_dist)
