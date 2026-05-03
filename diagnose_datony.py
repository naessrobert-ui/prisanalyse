# diagnose_datony.py
# Sjekker om Dato_ny holdes oppdatert for aktive (Solgt=NEI) annonser.
# Hvis Dato_ny for NEI-rader ligger langt tilbake i tid, betyr det at
# pipelinen ikke oppdaterer Dato_ny hver gang annonsen sees paa nytt.

from datetime import date
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

PARQUET_KEY = "calc/bil/database_biler.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

# Les database_biler.parquet
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
df = pq.read_table(
    pa.BufferReader(obj["Body"].read()),
    columns=["FinnKode", "Produsent", "Modell", "Dato", "Dato_ny", "Solgt"],
).to_pandas()

df["FinnKode"] = df["FinnKode"].astype(str)
df["Dato"] = pd.to_datetime(df["Dato"], errors="coerce")
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")
df["Solgt_norm"] = df["Solgt"].astype(str).str.strip().str.upper()

print(f"Max Dato_ny i hele datasettet: {df['Dato_ny'].max()}")
print()

# --- Fordeling av Dato_ny per Solgt-status ---
print("=" * 70)
print("FORDELING AV Dato_ny PER Solgt-STATUS")
print("=" * 70)

for status in ["JA", "NEI", "FJERNET"]:
    sub = df[df["Solgt_norm"] == status]
    if len(sub) == 0:
        continue
    print(f"\n--- Solgt = {status} ({len(sub):,} rader) ---")
    print(f"  Dato_ny min:    {sub['Dato_ny'].min()}")
    print(f"  Dato_ny max:    {sub['Dato_ny'].max()}")
    print(f"  Dato_ny median: {sub['Dato_ny'].median()}")

    # Hvor mange har Dato_ny eldre enn 7 dager?
    cutoff = df["Dato_ny"].max() - pd.Timedelta(days=7)
    old = (sub["Dato_ny"] < cutoff).sum()
    print(f"  Dato_ny >7d eldre enn max: {old:,} ({old/len(sub)*100:.1f}%)")

    cutoff_30 = df["Dato_ny"].max() - pd.Timedelta(days=30)
    old30 = (sub["Dato_ny"] < cutoff_30).sum()
    print(f"  Dato_ny >30d eldre enn max: {old30:,} ({old30/len(sub)*100:.1f}%)")

# --- Sammenlign med database_biler_siste.parquet (aktive annonser) ---
print()
print("=" * 70)
print("KRYSSJEKK MED database_biler_siste.parquet (aktive annonser)")
print("=" * 70)

obj2 = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler_siste.parquet")
df_siste = pq.read_table(
    pa.BufferReader(obj2["Body"].read()),
    columns=["FinnKode"],
).to_pandas()
df_siste["FinnKode"] = df_siste["FinnKode"].astype(str)
aktive_ids = set(df_siste["FinnKode"].values)
print(f"database_biler_siste.parquet har {len(aktive_ids):,} aktive annonser")

# Hvor mange av de "FJERNET"-merkede finnes FAKTISK fortsatt i siste-filen?
fjernet_men_aktiv = df[
    (df["Solgt_norm"] == "FJERNET") & (df["FinnKode"].isin(aktive_ids))
]
print(f"\nRader merket FJERNET, men finnes i siste-filen: {len(fjernet_men_aktiv):,}")
if len(fjernet_men_aktiv) > 0:
    print("  --> Pipeline-bug: annonser merkes FJERNET uten grunn")
    print("  Eksempler:")
    print(fjernet_men_aktiv[["FinnKode", "Produsent", "Modell", "Dato", "Dato_ny", "Solgt"]].head(10).to_string())

# Motsatt: hvor mange NEI er IKKE i siste-filen?
nei_men_borte = df[
    (df["Solgt_norm"] == "NEI") & (~df["FinnKode"].isin(aktive_ids))
]
print(f"\nRader merket NEI, men IKKE i siste-filen: {len(nei_men_borte):,}")
if len(nei_men_borte) > 0:
    print("  --> Enten solgt siden siste kjoring, eller pipeline-lag")

# Hvor mange JA finnes i siste-filen (burde vaere 0 eller veldig faa)?
ja_men_aktiv = df[
    (df["Solgt_norm"] == "JA") & (df["FinnKode"].isin(aktive_ids))
]
print(f"\nRader merket JA, men finnes i siste-filen: {len(ja_men_aktiv):,}")

# --- Dato_ny for NEI-rader som faktisk er aktive ---
print()
print("=" * 70)
print("Dato_ny FOR AKTIVE ANNONSER (Solgt=NEI OG i siste-filen)")
print("=" * 70)
aktive_nei = df[
    (df["Solgt_norm"] == "NEI") & (df["FinnKode"].isin(aktive_ids))
]
print(f"Antall: {len(aktive_nei):,}")
print(f"Dato_ny min:    {aktive_nei['Dato_ny'].min()}")
print(f"Dato_ny max:    {aktive_nei['Dato_ny'].max()}")
print(f"Dato_ny median: {aktive_nei['Dato_ny'].median()}")

# Histogram: hvor mange aktive annonser har Dato_ny X dager gammel?
dager_gamle = (df["Dato_ny"].max() - aktive_nei["Dato_ny"]).dt.days
print("\nFordeling av 'dager siden Dato_ny' for aktive annonser:")
bins = [-1, 0, 1, 3, 7, 14, 30, 60, 100, 1000]
hist = pd.cut(dager_gamle, bins=bins)
print(hist.value_counts().sort_index())
