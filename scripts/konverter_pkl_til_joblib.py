"""
scripts/konverter_pkl_til_joblib.py
====================================
Engangskonvertering: leser eksisterende bil_prismodell.pkl fra S3, lagrer
som lz4-komprimert .joblib og laster opp på ny key. Sparer ny trening.

Kjør:
    python -m scripts.konverter_pkl_til_joblib

Fjern den gamle .pkl-fila etter at scoreren er flyttet til .joblib:
    aws s3 rm s3://prisanalyse-data/calc/bil/bil_prismodell.pkl
"""
from __future__ import annotations

import io
import os
import pickle
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

import boto3
import joblib

from config import AWS_KEY, AWS_REGION, AWS_SECRET, S3_BUCKET_NAME

S3_KEY_INN = "calc/bil/bil_prismodell.pkl"
S3_KEY_UT = "calc/bil/bil_prismodell.joblib"
KOMPRIMERING = ("lz4", 3)


def main():
    s3 = boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )

    print(f"[1/4] Laster ned s3://{S3_BUCKET_NAME}/{S3_KEY_INN}")
    t0 = time.time()
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_INN)
    pkl_bytes = obj["Body"].read()
    print(f"      {len(pkl_bytes) / 1024 / 1024:.1f} MB lastet ned ({time.time()-t0:.1f}s)")

    print("[2/4] Deserialiserer pickle ...")
    t0 = time.time()
    modeller = pickle.loads(pkl_bytes)
    print(f"      ferdig ({time.time()-t0:.1f}s)")

    lokal_joblib = os.path.join(tempfile.gettempdir(), "bil_prismodell.joblib")
    print(f"[3/4] Lagrer komprimert joblib til {lokal_joblib}")
    t0 = time.time()
    joblib.dump(modeller, lokal_joblib, compress=KOMPRIMERING)
    storrelse_mb = os.path.getsize(lokal_joblib) / 1024 / 1024
    inn_mb = len(pkl_bytes) / 1024 / 1024
    print(f"      {storrelse_mb:.1f} MB ({100 * storrelse_mb / inn_mb:.0f}% av original) ({time.time()-t0:.1f}s)")

    print(f"[4/4] Laster opp til s3://{S3_BUCKET_NAME}/{S3_KEY_UT}")
    t0 = time.time()
    s3.upload_file(lokal_joblib, S3_BUCKET_NAME, S3_KEY_UT)
    print(f"      ferdig ({time.time()-t0:.1f}s)")

    print()
    print("Verifiserer at den nye .joblib-fila kan lastes tilbake ...")
    t0 = time.time()
    test = joblib.load(lokal_joblib)
    n_l1 = len(test.market_l1)
    n_l2 = len(test.market_l2)
    print(f"      OK — market L1={n_l1}, L2={n_l2}, lasting tok {time.time()-t0:.1f}s")
    print()
    print(f"Ferdig. Endre scoreren til S3-key '{S3_KEY_UT}' og slett gammel .pkl manuelt:")
    print(f"  aws s3 rm s3://{S3_BUCKET_NAME}/{S3_KEY_INN}")


if __name__ == "__main__":
    main()
