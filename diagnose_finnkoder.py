# diagnose_finnkoder.py
# Punktdiagnose for konkrete FinnKoder som mistenkes feilmerket som
# "solgt rekordraskt". Svarer paa: er bilen fortsatt aktiv, hvorfor ble den
# merket solgt, og fryser Dato_ny?
#
# Kjoer lokalt med gyldige AWS-nokler:  python diagnose_finnkoder.py

import io
import sys
from datetime import date, datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

# FinnKoder aa granske (endre fritt / send som argumenter)
FINNKODER = sys.argv[1:] or [
    "472120400",  # bruker: IKKE solgt
    "471526349",  # bruker: IKKE solgt
    "470074377",  # bruker: solgt, men oppdatert 2. aug
    "469644320",  # bruker: oppdatert 23. juli
]
FINNKODER = [str(x).strip() for x in FINNKODER]

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)


def _read_csv_fk_and_time(key):
    """Les et snapshot og returner (set av FinnKoder, LastModified)."""
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    content = obj["Body"].read()
    df = pd.read_csv(
        io.BytesIO(content), encoding="utf-16", sep=";", dtype=str,
        usecols=lambda c: c.lower().replace("_", "").replace(" ", "")
        in ("finnkode", "finnid"),
    )
    if df.empty or df.shape[1] == 0:
        return set(), None
    col = df.columns[0]
    return set(df[col].astype(str).str.extract(r"(\d+)", expand=False).dropna()), key


# ---------- 1) Pipeline-ferskhet (state) ----------
print("=" * 72)
print("1) PIPELINE-FERSKHET")
print("=" * 72)
try:
    st = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/state.json")["Body"].read()
    import json
    state = json.loads(st)
    print(f"  state.updated_at              = {state.get('updated_at')}")
    print(f"  state.last_processed_observed = {state.get('last_processed_observed_at')}")
    print(f"  state.total_cars              = {state.get('total_cars')}")
    print(f"  I dag                         = {datetime.now(timezone.utc).isoformat()}")
except Exception as e:
    print(f"  (fant ikke state.json: {e})")

# ---------- 2) Hovedfil (parquet) ----------
print("\n" + "=" * 72)
print("2) RAD I database_biler.parquet")
print("=" * 72)
obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="calc/bil/database_biler.parquet")
df = pq.read_table(
    pa.BufferReader(obj["Body"].read()),
    columns=["FinnKode", "Produsent", "Modell", "Dato", "Dato_ny", "Solgt",
             "Pris", "Pris_ny", "forhandler_type", "selger"],
).to_pandas()
df["FinnKode"] = pd.to_numeric(df["FinnKode"], errors="coerce").astype("Int64").astype(str)
df["Dato"] = pd.to_datetime(df["Dato"], errors="coerce")
df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")
latest_observed = df["Dato_ny"].max()
print(f"  Global max Dato_ny (siste observerte) = {latest_observed}")
sub = df[df["FinnKode"].isin(FINNKODER)]
if sub.empty:
    print("  Ingen av FinnKodene finnes i parquet!")
else:
    for _, r in sub.iterrows():
        alder = (latest_observed - r["Dato_ny"]).days if pd.notna(r["Dato_ny"]) else None
        print(f"  {r['FinnKode']}: Solgt={r['Solgt']!r} Dato={r['Dato']} "
              f"Dato_ny={r['Dato_ny']} (alder={alder}d) "
              f"selger={r['selger']!r} f_type={r['forhandler_type']!r}")

# ---------- 3) Siste daily: er bilene der (= aktiv)? ----------
print("\n" + "=" * 72)
print("3) FINNES DE I DE SISTE DAGLIGE SNAPSHOTENE? (aktiv-sannhet)")
print("=" * 72)
resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw/bil-daglig/")
daily = sorted([o for o in resp.get("Contents", []) if o["Key"].endswith(".csv")],
               key=lambda o: o["LastModified"], reverse=True)[:7]
for o in daily:
    fkset, _ = _read_csv_fk_and_time(o["Key"])
    treff = [fk for fk in FINNKODER if fk in fkset]
    print(f"  {o['LastModified']:%Y-%m-%d %H:%M}  ({len(fkset):,} biler)  "
          f"inneholder: {treff or '—'}")

# ---------- 4) Siste hourly: samme sjekk ----------
print("\n" + "=" * 72)
print("4) FINNES DE I DE SISTE TIME-SNAPSHOTENE?")
print("=" * 72)
resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw/bil-time/")
hourly = sorted([o for o in resp.get("Contents", []) if o["Key"].endswith(".csv")],
                key=lambda o: o["LastModified"], reverse=True)[:5]
for o in hourly:
    fkset, _ = _read_csv_fk_and_time(o["Key"])
    treff = [fk for fk in FINNKODER if fk in fkset]
    print(f"  {o['LastModified']:%Y-%m-%d %H:%M}  ({len(fkset):,} biler)  "
          f"inneholder: {treff or '—'}")

print("\nTolkning:")
print("  - Ligger bilen i siste daily men er merket JA/FJERNET => feilmerket,")
print("    og kryss-sjekken skal fjerne den (sjekk at prod er redeployet).")
print("  - Ligger den IKKE i noen fersk daily selv om den er live paa Finn =>")
print("    scraperen fanger den ikke (datainnsamling), Dato_ny fryser korrekt-")
print("    men-uheldig, og FJERNET-regelen slaar til.")
print("  - Er state.updated_at gammel => hele pipelinen har stoppet.")
