# config.py
import os
from datetime import date

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "prisanalyse-data")

# 👉 Dette SKAL være *Athena-databasen* (schema), f.eks. "default" eller "prisanalyse"
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "default")

# 👉 Dette SKAL være tabellen der bil-dataene ligger
#   (den het sannsynligvis "database_biler_parquet" hos deg)
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "database_biler_parquet")

# Default startdato
DEFAULT_STARTDATE = date(2025, 6, 1)


