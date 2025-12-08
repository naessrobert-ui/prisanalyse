# bolig_data.py
import io
import boto3
import pandas as pd
from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME
from helpers import find_latest_file_in_s3


def load_latest_bolig_df():
    """Laster siste bolig_X_*.csv fra S3, returnerer pandas DataFrame."""
    # Bruk eksplisitte nøkler fra config.py
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    latest_file_key = find_latest_file_in_s3(
        s3_client,
        S3_BUCKET_NAME,
        "raw/bolig-daglig/",
        r"bolig_X_(\d{2}-\d{2}-\d{4})\.csv",
    )

    if not latest_file_key:
        return None

    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=latest_file_key)
    df = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep=";",
        encoding="utf-16",
        on_bad_lines="skip",
    )
    df.columns = df.columns.str.strip()
    return df
