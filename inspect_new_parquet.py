# inspect_new_parquet.py
# Kjør to ganger — én for hver PARQUET_KEY nedenfor.
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

# >>> Kjør først med denne, så bytt til "database_biler_siste.parquet" <<<
PARQUET_KEY = "calc/bil/database_biler_siste.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
table = pq.read_table(pa.BufferReader(obj["Body"].read()))
df = table.to_pandas()

print(f"FIL: {PARQUET_KEY}")
print(f"RADER: {len(df):,}")
if "FinnKode" in df.columns:
    print(f"UNIKE FinnKode: {df['FinnKode'].nunique():,}")
elif "finnkode" in df.columns:
    print(f"UNIKE finnkode: {df['finnkode'].nunique():,}")

print("\n--- DTYPES ---")
print(df.dtypes)

print("\n--- 5 FOERSTE RADER ---")
with pd.option_context("display.max_columns", None, "display.width", 220):
    print(df.head())

print("\n--- LETER ETTER 'solgt' I STRENG-KOLONNER ---")
for col in df.columns:
    if df[col].dtype == object:
        try:
            n = df[col].astype(str).str.lower().str.contains("solgt", na=False).sum()
            if n > 0:
                print(f"  {col}: {n} rader inneholder 'solgt'")
        except Exception:
            pass

print("\n--- TIDSSTEMPEL-KANDIDATER ---")
for col in df.columns:
    if any(k in col.lower() for k in
           ["time", "dato", "date", "sold", "solgt", "seen", "sett", "aktiv", "status"]):
        try:
            print(f"  {col}: dtype={df[col].dtype}, min={df[col].min()}, max={df[col].max()}")
        except Exception as e:
            print(f"  {col}: dtype={df[col].dtype}, (kunne ikke hente min/max: {e})")
