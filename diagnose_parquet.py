# diagnose_parquet.py
# Kjør dette lokalt for å se HVA som faktisk ligger i Parquet-filen.
# Forventet output avslører hvorfor _is_sold() aldri matcher.

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

PARQUET_KEY = "calc/bil/bil_time.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
table = pq.read_table(pa.BufferReader(obj["Body"].read()))
df = table.to_pandas()

print("=" * 60)
print("KOLONNER OG DTYPES:")
print("=" * 60)
print(df.dtypes)

print("\n" + "=" * 60)
print("ANTALL RADER:", len(df))
print("UNIKE FINNKODER:", df["FinnKode"].nunique() if "FinnKode" in df.columns else "N/A")
print("=" * 60)

# Finn pris-kolonnen
for pris_col in ["Pris", "pris", "pris_num"]:
    if pris_col in df.columns:
        print(f"\n--- Kolonne '{pris_col}' ---")
        print("dtype:", df[pris_col].dtype)
        print("NaN-andel:", df[pris_col].isna().mean())
        print("Sample (10 første ikke-NaN):")
        print(df[pris_col].dropna().head(10).tolist())
        
        # Hvis streng: sjekk om 'solgt' finnes
        if df[pris_col].dtype == object:
            s = df[pris_col].astype(str).str.lower()
            n_solgt = s.str.contains("solgt", na=False).sum()
            print(f"Rader med 'solgt' i strengen: {n_solgt}")
        # Hvis numerisk: sjekk om 0 finnes
        else:
            n_null = (df[pris_col] == 0).sum()
            print(f"Rader med pris == 0: {n_null}")

# Sjekk om det finnes noen "solgt-markør" andre steder
print("\n" + "=" * 60)
print("LETER ETTER 'solgt'-MARKØR I ALLE STRENG-KOLONNER:")
print("=" * 60)
for col in df.columns:
    if df[col].dtype == object:
        try:
            n = df[col].astype(str).str.lower().str.contains("solgt", na=False).sum()
            if n > 0:
                print(f"  {col}: {n} rader inneholder 'solgt'")
        except Exception:
            pass

# Sjekk tidsoppløsning
print("\n" + "=" * 60)
print("TIDSSTEMPLER:")
print("=" * 60)
for tcol in ["snapshot_time", "dato", "tidspunkt"]:
    if tcol in df.columns:
        print(f"{tcol}: min={df[tcol].min()}, max={df[tcol].max()}")

# Hvor mange FinnKoder har MER ENN ETT snapshot? (trengs for tid-til-salg)
if "FinnKode" in df.columns:
    counts = df.groupby("FinnKode").size()
    print(f"\nFinnKoder med >1 snapshot: {(counts > 1).sum()} av {len(counts)}")
    print(f"Median antall snapshots per FinnKode: {counts.median()}")
