# config.example.py

AWS_KEY = "DIN_AWS_KEY_HER"
AWS_SECRET = "DIN_AWS_SECRET_HER"
AWS_REGION = "eu-west-1"

S3_BUCKET_NAME = "prisanalyse-data"
ATHENA_DATABASE = "database_biler_parquet"

# Startdato fallback
from datetime import date
DEFAULT_STARTDATE = date(2025, 6, 1)
