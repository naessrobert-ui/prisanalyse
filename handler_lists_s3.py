# handler_lists_s3.py
"""
S3-basert lagring av investor-/aksjelister for Handler Oslo Børs.

Lister lagres som små CSV-filer i S3 under en konfigurerbar prefix.
Hver liste har et navn og inneholder patterns (investornavn/id-er).

Miljøvariabler:
    HANDLER_S3_BUCKET       – S3 bucket-navn (påkrevd)
    HANDLER_S3_PREFIX       – Prefix/mappe i bucket (default: "handler-lists/")
    AWS_ACCESS_KEY_ID       – AWS credentials (allerede satt i Render)
    AWS_SECRET_ACCESS_KEY   – AWS credentials (allerede satt i Render)
    AWS_DEFAULT_REGION      – AWS region (default: eu-north-1)
"""
from __future__ import annotations

import io
import os
import datetime as dt
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError


# =========================================================
# Config
# =========================================================
def _get_bucket() -> str:
    # Bruker HANDLER_S3_BUCKET hvis satt, ellers faller tilbake til S3_BUCKET_NAME
    bucket = os.environ.get("HANDLER_S3_BUCKET", "").strip()
    if not bucket:
        bucket = os.environ.get("S3_BUCKET_NAME", "").strip()
    if not bucket:
        raise RuntimeError(
            "Verken HANDLER_S3_BUCKET eller S3_BUCKET_NAME er satt. "
            "Sett en av disse i Render."
        )
    return bucket


def _get_prefix() -> str:
    return os.environ.get("HANDLER_S3_PREFIX", "handler-lists/").strip().rstrip("/") + "/"


def _get_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")),
    )


# =========================================================
# Helpers
# =========================================================
def _list_key(name: str) -> str:
    """Bygg S3-nøkkel fra listenavn."""
    safe_name = name.strip().replace("/", "_").replace("\\", "_")
    return f"{_get_prefix()}{safe_name}.csv"


def _meta_key() -> str:
    """Metadata-fil som holder oversikt over alle lister."""
    return f"{_get_prefix()}_index.json"


# =========================================================
# CRUD
# =========================================================
def list_all() -> list[dict]:
    """
    List alle CSV-filer i S3 under prefix.
    Returnerer [{"name": "Beste", "key": "handler-lists/Beste.csv",
                 "last_modified": "2026-02-17", "size_kb": 1.2}, ...]
    """
    client = _get_client()
    bucket = _get_bucket()
    prefix = _get_prefix()

    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except ClientError as e:
        raise RuntimeError(f"Kunne ikke liste S3-objekter: {e}")

    results = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        # Skip index og mapper
        if key == _meta_key() or key.endswith("/"):
            continue
        if not key.lower().endswith(".csv"):
            continue

        # Ekstraher navn fra key
        filename = key[len(prefix):]  # "Beste.csv"
        name = filename.rsplit(".", 1)[0]  # "Beste"

        results.append({
            "name": name,
            "key": key,
            "last_modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M"),
            "size_kb": round(obj["Size"] / 1024, 1),
        })

    results.sort(key=lambda x: x["name"].lower())
    return results


def read_list(name: str) -> pd.DataFrame:
    """
    Les en navngitt liste fra S3. Returnerer DataFrame med kolonnene
    som finnes i CSV-filen (typisk: pattern, type, kommentar).
    """
    client = _get_client()
    bucket = _get_bucket()
    key = _list_key(name)

    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError(f"Listen '{name}' finnes ikke i S3")
        raise

    # Prøv semikolon først (norsk standard), deretter komma
    try:
        df = pd.read_csv(io.BytesIO(body), sep=";", encoding="utf-8", dtype=str).fillna("")
    except Exception:
        df = pd.read_csv(io.BytesIO(body), sep=",", encoding="utf-8", dtype=str).fillna("")

    return df


def read_list_patterns(name: str) -> list[str]:
    """Les en liste og returner kun pattern-verdiene (første kolonne)."""
    df = read_list(name)
    if df.empty:
        return []
    col = df.columns[0]
    patterns = df[col].astype(str).str.strip().tolist()
    return [p for p in patterns if p and p.lower() not in ("pattern", "eier", "selskap", "investor")]


def save_list(name: str, patterns: list[str], list_type: str = "investor",
              description: str = "") -> dict:
    """
    Lagre en ny eller oppdatert liste til S3.

    Args:
        name: Listenavn (blir filnavn)
        patterns: Liste med investor-patterns eller aksje-patterns
        list_type: "investor" eller "aksje"
        description: Valgfri beskrivelse

    Returns:
        {"name": name, "key": key, "count": len(patterns)}
    """
    if not name or not name.strip():
        raise ValueError("Listenavn kan ikke være tomt")

    if not patterns:
        raise ValueError("Listen må inneholde minst én verdi")

    # Bygg DataFrame
    df = pd.DataFrame({
        "pattern": [p.strip() for p in patterns if p.strip()],
    })

    # Skriv til CSV-buffer
    buf = io.StringIO()
    df.to_csv(buf, index=False, sep=";", encoding="utf-8")
    csv_bytes = buf.getvalue().encode("utf-8")

    client = _get_client()
    bucket = _get_bucket()
    key = _list_key(name)

    metadata = {
        "list-type": list_type,
        "description": description,
        "created": dt.datetime.now().isoformat(),
        "count": str(len(df)),
    }

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_bytes,
            ContentType="text/csv; charset=utf-8",
            Metadata=metadata,
        )
    except ClientError as e:
        raise RuntimeError(f"Kunne ikke lagre til S3: {e}")

    return {"name": name.strip(), "key": key, "count": len(df)}


def delete_list(name: str) -> bool:
    """Slett en liste fra S3. Returnerer True hvis slettet."""
    client = _get_client()
    bucket = _get_bucket()
    key = _list_key(name)

    try:
        client.delete_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def rename_list(old_name: str, new_name: str) -> dict:
    """Gi nytt navn til en liste (kopier + slett gammel)."""
    df = read_list(old_name)
    patterns = df.iloc[:, 0].astype(str).str.strip().tolist()
    patterns = [p for p in patterns if p and p.lower() not in ("pattern",)]
    result = save_list(new_name, patterns)
    delete_list(old_name)
    return result


def list_exists(name: str) -> bool:
    """Sjekk om en liste finnes."""
    client = _get_client()
    bucket = _get_bucket()
    key = _list_key(name)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
