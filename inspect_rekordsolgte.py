# inspect_rekordsolgte.py
# Plukker ut et utvalg rekordsolgte biler og sjekker:
#   1. Er de i siste daily? (hvis ja -> fortsatt paa Finn, feilmerket)
#   2. Er de i database_biler_siste.parquet?
#   3. Finnes de paa Finn fortsatt (sjekker vi manuelt etterpaa)

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
    columns=["FinnKode", "Produsent", "Modell", "årstall", "kjørelengde",
             "Dato", "Dato_ny", "Pris", "Pris_ny", "Solgt", "forhandler_type"],
).to_pandas()
df["FinnKode"] = df["FinnKode"].astype(str)
df["Dato"] = pd.to_datetime(df["Dato"])
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"])

# ---- Les siste daily-fil (sannhet for aktive) ----
resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw/bil-daglig/")
daily_files = sorted(
    [o for o in resp.get("Contents", []) if o["Key"].endswith(".csv")],
    key=lambda o: o["LastModified"], reverse=True,
)
latest_daily = daily_files[0]
print(f"Siste daily: {latest_daily['Key']} ({latest_daily['LastModified']})")

obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=latest_daily["Key"])
content = obj["Body"].read()
df_daily = pd.read_csv(io.BytesIO(content), encoding="utf-16", sep=";")
fk_col = next(c for c in df_daily.columns if c.lower().replace("_","") in ("finnkode","finnid"))
daily_fk = set(df_daily[fk_col].astype(str).str.extract(r"(\d+)", expand=False).dropna().values)
print(f"Aktive i siste daily: {len(daily_fk):,}")

# Les siste-parquet ogsaa
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler_siste.parquet")
df_siste = pq.read_table(
    pa.BufferReader(obj["Body"].read()), columns=["FinnKode"],
).to_pandas()
siste_fk = set(df_siste["FinnKode"].astype(str).values)
print(f"I database_biler_siste.parquet: {len(siste_fk):,}")

# ---- Filter: rekordsolgt med samme kriterier som UI ----
FRA_DATO = date(2026, 3, 24)
TIL_DATO = date(2026, 4, 23)
MAKS_DAGER = 3
PRIS_FRA = 1500

solgt_norm = df["Solgt"].astype(str).str.strip().str.upper()
er_solgt = solgt_norm.isin({"JA", "FJERNET"})
i_periode = (df["Dato_ny"] >= pd.Timestamp(FRA_DATO)) & (df["Dato_ny"] < pd.Timestamp(TIL_DATO) + pd.Timedelta(days=1))

diff_d = (df["Dato_ny"] - df["Dato"]).dt.total_seconds() / 86400.0
rekord = (diff_d <= MAKS_DAGER) & (diff_d >= 0)

pris_eff = pd.to_numeric(df["Pris_ny"], errors="coerce").fillna(pd.to_numeric(df["Pris"], errors="coerce"))
pris_ok = pris_eff >= PRIS_FRA

maske = er_solgt & i_periode & rekord & pris_ok
rs = df.loc[maske].copy()
rs["dager_til_salg"] = diff_d.loc[maske]

print(f"\nRekordsolgte (maks {MAKS_DAGER} dager): {len(rs):,}")

# ---- Kryssjekk: hvor mange finnes fortsatt i siste daily? ----
rs["i_siste_daily"] = rs["FinnKode"].isin(daily_fk)
rs["i_siste_parquet"] = rs["FinnKode"].isin(siste_fk)

print()
print("=" * 70)
print("FALSKE POSITIVE: merket rekordsolgt men finnes fortsatt i siste daily")
print("=" * 70)
falske = rs[rs["i_siste_daily"]]
print(f"Antall falske positive: {len(falske):,} av {len(rs):,} ({len(falske)/len(rs)*100:.1f}%)")
print()
print("Fordeling paa Solgt-status:")
print(falske["Solgt"].value_counts())
print()

print("10 EKSEMPLER PAA FEILMERKEDE (finnes i siste daily men merket rekordsolgt):")
cols = ["FinnKode", "Produsent", "Modell", "årstall", "Dato", "Dato_ny",
        "dager_til_salg", "Solgt", "Pris_ny", "forhandler_type"]
print(falske[cols].head(10).to_string())

# ---- Ekte salg (ikke i daily) ----
print()
print("=" * 70)
print("SANNSYNLIG EKTE SALG: merket rekordsolgt OG ikke i siste daily")
print("=" * 70)
ekte = rs[~rs["i_siste_daily"]]
print(f"Antall: {len(ekte):,} av {len(rs):,} ({len(ekte)/len(rs)*100:.1f}%)")
print()
print(ekte["Solgt"].value_counts())

# ---- Fordeling av mistenkelige data ----
print()
print("=" * 70)
print("PRISFORDELING (falske vs ekte)")
print("=" * 70)
print(f"Falske positive:  min={falske['Pris_ny'].min()}, median={falske['Pris_ny'].median()}, max={falske['Pris_ny'].max()}")
print(f"Ekte salg:        min={ekte['Pris_ny'].min()}, median={ekte['Pris_ny'].median()}, max={ekte['Pris_ny'].max()}")

# ---- Er 'forhandler_type' nyttig? ----
print()
print("=" * 70)
print("FORHANDLER_TYPE fordeling")
print("=" * 70)
print("Falske positive:")
print(falske["forhandler_type"].value_counts(dropna=False).head())
print("\nEkte salg:")
print(ekte["forhandler_type"].value_counts(dropna=False).head())
