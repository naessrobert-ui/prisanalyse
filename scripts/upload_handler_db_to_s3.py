#!/usr/bin/env python3
"""Upload local Handler SQLite DB file to S3.

Run this on your local machine (where the DB file exists), not on Render.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload local Handler DB file to S3")
    parser.add_argument("--source", required=True, help="Local path to SQLite file (e.g. C:/.../topchanges)")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket")
    parser.add_argument("--key", required=True, help="Target S3 key (e.g. topchanges/topchanges.db)")
    parser.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"), help="AWS region (optional if configured in profile)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    src = Path(args.source).expanduser()

    if not src.is_file():
        print(f"❌ Source file not found: {src}")
        return 1

    client_args: dict[str, str] = {}
    if args.region:
        client_args["region_name"] = args.region

    s3 = boto3.client("s3", **client_args)

    print(f"Uploading {src} -> s3://{args.bucket}/{args.key}")
    s3.upload_file(str(src), args.bucket, args.key)
    print("✅ Upload complete")
    print(f"Set this on Render: HANDLER_DB_S3_URI=s3://{args.bucket}/{args.key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
