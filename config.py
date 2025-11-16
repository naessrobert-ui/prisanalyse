# config.py
import os
from datetime import date

# AWS-nøkler – hentes fra miljøvariabler (Render + lokalt)
AWS_KEY = os.environ.get("AWS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.environ.get("AWS_SECRET") or os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")

# S3 / Athena
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "prisanalyse-data")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "database_biler_parquet")

# Standard startdato for analyser (kan justeres)
DEFAULT_STARTDATE = date(2025, 6, 1)

# Litt enkel logging – men ingen hard stopp
if not AWS_KEY or not AWS_SECRET:
    print("ADVARSEL: Kritiske AWS-miljøvariabler mangler.")

