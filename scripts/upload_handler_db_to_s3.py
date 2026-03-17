#!/usr/bin/env python3
"""Upload local Handler SQLite DB file to S3.

Run this on your local machine (where the DB file exists), not on Render.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload local Handler DB file to S3")
    parser.add_argument("--source", required=True, help="Local path to SQLite file (e.g. C:/.../topchanges)")
    parser.add_argument("--bucket", help="Target S3 bucket")
    parser.add_argument("--key", help="Target S3 key (e.g. topchanges/topchanges.db)")
    parser.add_argument(
        "--s3-uri",
        help="Target S3 URI (e.g. s3://my-bucket/topchanges/topchanges.db). Alternative to --bucket/--key.",
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"), help="AWS region (optional if configured in profile)")
    return parser.parse_args()


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError("Ugyldig --s3-uri. Bruk format: s3://bucket/key")
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError("Ugyldig --s3-uri: mangler key")
    return parsed.netloc, key


def main() -> int:
    args = parse_args()
    src = Path(args.source).expanduser()

    if not src.is_file():
        print(f"❌ Source file not found: {src}")
        return 1

    try:
        if args.s3_uri:
            bucket, key = parse_s3_uri(args.s3_uri)
        else:
            if not args.bucket or not args.key:
                print("❌ Du må enten bruke --s3-uri eller begge --bucket og --key")
                return 1
            bucket, key = args.bucket, args.key
    except ValueError as exc:
        print(f"❌ {exc}")
        return 1

    import boto3

    client_args: dict[str, str] = {}
    if args.region:
        client_args["region_name"] = args.region

    s3 = boto3.client("s3", **client_args)

    print(f"Uploading {src} -> s3://{bucket}/{key}")
    s3.upload_file(str(src), bucket, key)
    print("✅ Upload complete")
    print(f"Set this on Render: HANDLER_DB_S3_URI=s3://{bucket}/{key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
