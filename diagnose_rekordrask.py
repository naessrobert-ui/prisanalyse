# diagnose_rekordrask.py
# Simulerer hele rekordrask-logikken trinn for trinn og viser hvor biler
# faller fra. Kjoer lokalt.

from datetime import date
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

PARQUET_KEY = "calc/bil/database_biler.parquet"

# Samme parametre som UI-et viser
FRA_DATO = date(2026, 3, 24)
TIL_DATO = date(2026, 4, 23)
MAKS_DAGER = 100
PRIS_FRA = 1500
AARSTALL_FRA = 1950

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
df = pq.read_table(
    pa.BufferReader(obj["Body"].read()),
    columns=[
        "FinnKode", "Produsent", "Modell", "årstall", "kjørelengde",
        "drivstoff", "forhandler_type",
        "Dato", "Dato_ny", "Pris", "Pris_ny", "Solgt",
    ],
).to_pandas()

print(f"Totalt rader: {len(df):,}")
print()

# --- Solgt-fordeling ---
print("=" * 60)
print("FORDELING AV Solgt-KOLONNEN (raa)")
print("=" * 60)
print(df["Solgt"].value_counts(dropna=False))
print()

# Normalisert
solgt_norm = df["Solgt"].astype(str).str.strip().str.upper()
print("Normalisert (upper + strip):")
print(solgt_norm.value_counts(dropna=False))
print()

# --- Datokolonner ---
df["Dato"] = pd.to_datetime(df["Dato"], errors="coerce")
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")

print("=" * 60)
print("DATO-OMRAADER")
print("=" * 60)
print(f"Dato:    min={df['Dato'].min()}, max={df['Dato'].max()}")
print(f"Dato_ny: min={df['Dato_ny'].min()}, max={df['Dato_ny'].max()}")
print()

# --- Steg for steg: filter ---
print("=" * 60)
print("FUNNEL: HVOR FALLER BILER FRA?")
print("=" * 60)

# 1. Alle med gyldig tid
gyldig = df["Dato"].notna() & df["Dato_ny"].notna()
print(f"1) Gyldig tid (Dato + Dato_ny): {gyldig.sum():,}")

# 2. Solgt = JA eller FJERNET
er_solgt = solgt_norm.isin({"JA", "FJERNET"})
print(f"2) Solgt in (JA, FJERNET):      {er_solgt.sum():,}")
print(f"   - JA alene:                  {(solgt_norm == 'JA').sum():,}")
print(f"   - FJERNET alene:             {(solgt_norm == 'FJERNET').sum():,}")

# 3. Kombinert 1+2
komb = gyldig & er_solgt
print(f"3) Gyldig + solgt:              {komb.sum():,}")

# 4. Dato_ny >= FRA_DATO
i_periode = df["Dato_ny"] >= pd.Timestamp(FRA_DATO)
print(f"4) Dato_ny >= {FRA_DATO}:   {i_periode.sum():,}")

komb4 = komb & i_periode
print(f"5) Gyldig + solgt + i periode:  {komb4.sum():,}")

# 5. Dato_ny <= TIL_DATO (slutten av dagen)
til_ts = pd.Timestamp(TIL_DATO) + pd.Timedelta(days=1)  # ekskluderende
i_periode2 = df["Dato_ny"] < til_ts
komb5 = komb4 & i_periode2
print(f"6) Ogsaa Dato_ny <= {TIL_DATO}: {komb5.sum():,}")

# 6. Timer til salg <= MAKS_DAGER*24
diff_h = (df["Dato_ny"] - df["Dato"]).dt.total_seconds() / 3600.0
rekord_maske = diff_h <= (MAKS_DAGER * 24)
print(f"7) timer_til_salg <= {MAKS_DAGER}d*24: {rekord_maske.sum():,}")

# Kombinert
endelig = komb5 & rekord_maske & (diff_h >= 0)
print(f"8) Alt kombinert:               {endelig.sum():,}")

# --- Pris og aarstall-filter ---
pris_eff = pd.to_numeric(df["Pris_ny"], errors="coerce").fillna(
    pd.to_numeric(df["Pris"], errors="coerce")
)
aarstall_num = pd.to_numeric(df["årstall"], errors="coerce")

pris_ok = pris_eff >= PRIS_FRA
aar_ok = aarstall_num >= AARSTALL_FRA
med_pris_aar = endelig & pris_ok & aar_ok
print(f"9) + pris >= {PRIS_FRA} + aar >= {AARSTALL_FRA}: {med_pris_aar.sum():,}")
print()

# --- Hva ligger i "DF_alle" (43 468)? ---
# UI viser 43 468 "i DF_alle". Sannsynligvis antall innenfor periode+pris+aarstall
# uavhengig av solgt. Sjekk:
alle_i_periode = gyldig & i_periode & i_periode2 & pris_ok & aar_ok
print(f"KANDIDAT FOR 'DF_alle' (gyldig+periode+pris+aar, uansett solgt): {alle_i_periode.sum():,}")
print()

# --- Se paa noen solgte biler innenfor perioden ---
print("=" * 60)
print("10 EKSEMPLER PAA SOLGTE I PERIODEN")
print("=" * 60)
s = df.loc[komb5].copy()
s["timer_til_salg"] = diff_h.loc[komb5]
s["dager_til_salg"] = s["timer_til_salg"] / 24
print(s[["FinnKode", "Produsent", "Modell", "årstall", "Dato", "Dato_ny",
        "dager_til_salg", "Solgt"]].head(10).to_string())
print()

# --- Histogram over dager til salg for solgte i perioden ---
print("=" * 60)
print("FORDELING: dager_til_salg for solgte i periode")
print("=" * 60)
if len(s) > 0:
    bins = [0, 1, 3, 7, 14, 30, 60, 100, 200, 500, 10000]
    hist = pd.cut(s["dager_til_salg"], bins=bins, include_lowest=True)
    print(hist.value_counts().sort_index())
