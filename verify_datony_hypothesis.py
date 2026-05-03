# verify_datony_hypothesis.py
# Tester hypotesen: "hvis Dato_ny == siste DAG i datasettet, er bilen aktiv"
# Sjekker ved aa krysse mot database_biler_siste.parquet som fasit.

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

# Les hovedfilen
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler.parquet")
df = pq.read_table(
    pa.BufferReader(obj["Body"].read()),
    columns=["FinnKode", "Dato", "Dato_ny", "Solgt"],
).to_pandas()
df["FinnKode"] = df["FinnKode"].astype(str)
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")
df["Solgt_norm"] = df["Solgt"].astype(str).str.strip().str.upper()

# Les siste-filen (fasit for aktive)
obj2 = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler_siste.parquet")
df_siste = pq.read_table(
    pa.BufferReader(obj2["Body"].read()),
    columns=["FinnKode"],
).to_pandas()
df_siste["FinnKode"] = df_siste["FinnKode"].astype(str)
aktive_fasit = set(df_siste["FinnKode"].values)

# Siste DAG i hele datasettet
siste_dato = df["Dato_ny"].max().date()
print(f"Siste Dato_ny i hele datasettet: {df['Dato_ny'].max()}")
print(f"Siste DAG (normalisert):         {siste_dato}")
print(f"Aktive i siste-filen:            {len(aktive_fasit):,}")
print()

# Hypoteserule: aktiv <=> Dato_ny er paa siste_dato
df["_dato_ny_dag"] = df["Dato_ny"].dt.date
df["_hypotese_aktiv"] = df["_dato_ny_dag"] == siste_dato
df["_fasit_aktiv"] = df["FinnKode"].isin(aktive_fasit)

# Forvirringsmatrise
print("=" * 70)
print("CONFUSION MATRIX: hypotese vs fasit")
print("=" * 70)
print(pd.crosstab(
    df["_hypotese_aktiv"].map({True: "hyp_aktiv", False: "hyp_solgt"}),
    df["_fasit_aktiv"].map({True: "fasit_aktiv", False: "fasit_solgt"}),
    margins=True,
))
print()

# False positives: hypotesen sier aktiv, men fasit sier solgt
fp = df[df["_hypotese_aktiv"] & ~df["_fasit_aktiv"]]
print(f"FALSE POSITIVE (hyp=aktiv, fasit=solgt): {len(fp):,}")
if len(fp) > 0:
    print("  Fordeling av Solgt-status:")
    print(fp["Solgt_norm"].value_counts())

# False negatives: hypotesen sier solgt, men fasit sier aktiv
fn = df[~df["_hypotese_aktiv"] & df["_fasit_aktiv"]]
print(f"\nFALSE NEGATIVE (hyp=solgt, fasit=aktiv): {len(fn):,}")
if len(fn) > 0:
    print("  Fordeling av Solgt-status:")
    print(fn["Solgt_norm"].value_counts())
    print(f"  Dato_ny min: {fn['Dato_ny'].min()}")
    print(f"  Dato_ny max: {fn['Dato_ny'].max()}")
    print(f"  Dato_ny median: {fn['Dato_ny'].median()}")

# Test loser hypotese: Dato_ny er innenfor siste DOEGN (ikke bare dag)
print()
print("=" * 70)
print("ALTERNATIV HYPOTESE: Dato_ny >= (siste_dato - N dager)")
print("=" * 70)

for n in [1, 2, 3, 5, 7]:
    cutoff = pd.Timestamp(siste_dato) - pd.Timedelta(days=n-1)
    hyp = df["Dato_ny"] >= cutoff
    fp_n = ((hyp) & (~df["_fasit_aktiv"])).sum()
    fn_n = ((~hyp) & (df["_fasit_aktiv"])).sum()
    tp_n = ((hyp) & (df["_fasit_aktiv"])).sum()
    tn_n = ((~hyp) & (~df["_fasit_aktiv"])).sum()
    presisjon = tp_n / (tp_n + fp_n) if (tp_n + fp_n) > 0 else 0
    recall = tp_n / (tp_n + fn_n) if (tp_n + fn_n) > 0 else 0
    print(f"  Cutoff = siste_dato - {n-1}d (>= {cutoff.date()}): "
          f"FP={fp_n:>6,}  FN={fn_n:>6,}  "
          f"presisjon={presisjon:.1%}  recall={recall:.1%}")

# Dag-distribusjon av Dato_ny for fasit-aktive annonser
print()
print("=" * 70)
print("Dato_ny-DAG for fasit-AKTIVE annonser (burde klynge seg paa siste dag)")
print("=" * 70)
aktive_hovedfil = df[df["_fasit_aktiv"]]
dag_dist = aktive_hovedfil["_dato_ny_dag"].value_counts().sort_index(ascending=False).head(15)
print(dag_dist)
