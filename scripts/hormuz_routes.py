# -*- coding: utf-8 -*-
"""S3-backed global seaborne crude-oil and Hormuz traffic dashboards."""

from __future__ import annotations

import csv
import io
import json
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Blueprint, Response, jsonify, render_template

hormuz_bp = Blueprint("hormuz", __name__, url_prefix="/hormuz")

S3_BUCKET = os.getenv("OIL_TRACKER_S3_BUCKET") or os.getenv("S3_BUCKET_NAME") or "prisanalyse-data"
S3_PREFIX = (os.getenv("OIL_TRACKER_S3_PREFIX") or "oil-tanker-data").strip("/")
FLOW_HISTORY_KEY = f"{S3_PREFIX}/aggregates/flow_history.csv"
LATEST_MAP_KEY = f"{S3_PREFIX}/maps/latest.html"
HORMUZ_LATEST_KEY = f"{S3_PREFIX}/hormuz/latest.json"
CACHE_SECONDS = max(30, int(os.getenv("OIL_TRACKER_CACHE_SECONDS", "300")))

_cache: dict = {"loaded_at": 0.0, "payload": None}
_strait_cache: dict = {"loaded_at": 0.0, "payload": None}
_cache_lock = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _s3_client():
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"
    return boto3.client("s3", region_name=region)


def _number(value, integer: bool = False):
    try:
        number = float(value or 0)
        return int(number) if integer else number
    except (TypeError, ValueError):
        return 0 if integer else 0.0


def _read_history_from_s3() -> dict:
    obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=FLOW_HISTORY_KEY)
    text = obj["Body"].read().decode("utf-8-sig")
    rows = []
    for raw in csv.DictReader(io.StringIO(text)):
        rows.append(
            {
                "collected_at": raw.get("collected_at", ""),
                "destination_region": raw.get("destination_region", "Unclassified"),
                "vessel_count": _number(raw.get("vessel_count"), integer=True),
                "loaded_count": _number(raw.get("loaded_count"), integer=True),
                "estimated_barrels": _number(raw.get("estimated_barrels")),
                "total_dwt": _number(raw.get("total_dwt")),
            }
        )

    if not rows:
        raise ValueError("Aggregatfilen er tom")

    timestamps = sorted({row["collected_at"] for row in rows if row["collected_at"]})
    latest_at = timestamps[-1] if timestamps else ""
    latest = [row for row in rows if row["collected_at"] == latest_at]

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if row["destination_region"] != "Unclassified":
            grouped[row["destination_region"]].append(row)
    history = {region: values[-240:] for region, values in sorted(grouped.items())}
    return {
        "generated_at": _utc_now_iso(),
        "last_updated": latest_at,
        "source": "TankerMap AIS via S3",
        "latest": latest,
        "history": history,
        "meta": {
            "bucket": S3_BUCKET,
            "observations": len(timestamps),
            "regions": len(grouped),
            "method": "AIS destination text and estimated cargo from DWT",
        },
    }


def _cached_payload(cache: dict, loader, force: bool = False) -> dict:
    now = time.monotonic()
    with _cache_lock:
        if not force and cache["payload"] and now - cache["loaded_at"] < CACHE_SECONDS:
            return cache["payload"]
        payload = loader()
        cache.update({"loaded_at": now, "payload": payload})
        return payload


def _flow_payload(force: bool = False) -> dict:
    return _cached_payload(_cache, _read_history_from_s3, force)


def _read_strait_from_s3() -> dict:
    obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=HORMUZ_LATEST_KEY)
    payload = json.loads(obj["Body"].read().decode("utf-8-sig"))
    if not payload.get("collected_at"):
        raise ValueError("Hormuz-filen mangler tidsstempel")
    return payload


def _strait_payload(force: bool = False) -> dict:
    return _cached_payload(_strait_cache, _read_strait_from_s3, force)


@hormuz_bp.route("/")
def hormuz_dashboard():
    return render_template("hormuz_traffic.html")


@hormuz_bp.route("/strait")
def hormuz_strait_dashboard():
    return render_template("hormuz_strait.html")


@hormuz_bp.route("/api/flows")
def oil_flow_data():
    try:
        return jsonify(_flow_payload())
    except (BotoCoreError, ClientError, ValueError, UnicodeError) as exc:
        return jsonify(
            {
                "ok": False,
                "generated_at": _utc_now_iso(),
                "error": "Kunne ikke laste oljeflytdata fra S3",
                "detail": str(exc),
            }
        ), 503


@hormuz_bp.route("/api/strait")
def hormuz_strait_data():
    try:
        return jsonify(_strait_payload())
    except (BotoCoreError, ClientError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        return jsonify(
            {
                "ok": False,
                "generated_at": _utc_now_iso(),
                "error": "Kunne ikke laste Hormuz-data fra S3",
                "detail": str(exc),
            }
        ), 503


@hormuz_bp.route("/api/status")
def oil_flow_status():
    try:
        payload = _flow_payload(force=True)
        result = {
            "ok": True,
            "generated_at": _utc_now_iso(),
            "last_updated": payload["last_updated"],
            "observations": payload["meta"]["observations"],
            "source": payload["source"],
        }
        try:
            strait = _strait_payload(force=True)
            result["hormuz_last_updated"] = strait["collected_at"]
            result["hormuz_crossings_24h"] = strait.get("summary", {}).get("crossings_24h", 0)
        except (BotoCoreError, ClientError, ValueError, UnicodeError, json.JSONDecodeError):
            result["hormuz_last_updated"] = None
        return jsonify(result)
    except (BotoCoreError, ClientError, ValueError, UnicodeError) as exc:
        return jsonify({"ok": False, "generated_at": _utc_now_iso(), "error": str(exc)}), 503


@hormuz_bp.route("/map")
def oil_flow_map():
    try:
        obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=LATEST_MAP_KEY)
        html = obj["Body"].read()
        return Response(html, mimetype="text/html", headers={"Cache-Control": "public, max-age=300"})
    except (BotoCoreError, ClientError):
        return render_template("hormuz_map_missing.html"), 404

