#!/usr/bin/env python3
"""Collect AIS data from AISStream for the Strait of Hormuz area."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, create_connection
from websocket._exceptions import WebSocketTimeoutException

WS_URL = "wss://stream.aisstream.io/v0/stream"
DEFAULT_DB_PATH = Path("data/hormuz_ais.sqlite")
DEFAULT_BBOX = ((24.0, 54.0), (28.5, 58.8))
DEFAULT_MESSAGE_TYPES = [
    "PositionReport",
    "ShipStaticData",
    "StaticDataReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
]


@dataclass(slots=True)
class NormalizedMessage:
    received_at_utc: str
    ais_time_utc: str | None
    message_type: str
    mmsi: int | None
    ship_name: str | None
    latitude: float | None
    longitude: float | None
    sog: float | None
    cog: float | None
    true_heading: float | None
    navigational_status: int | None
    source: str
    raw_json: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hent AIS-data for Hormuz-området")
    parser.add_argument("--api-key", help="AISStream API-nøkkel. Kan også settes i AISTREAM_API_KEY.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--minutes", type=float, default=None)
    parser.add_argument("--include-static", action="store_true")
    parser.add_argument("--min-lat", type=float, default=DEFAULT_BBOX[0][0])
    parser.add_argument("--min-lon", type=float, default=DEFAULT_BBOX[0][1])
    parser.add_argument("--max-lat", type=float, default=DEFAULT_BBOX[1][0])
    parser.add_argument("--max-lon", type=float, default=DEFAULT_BBOX[1][1])
    parser.add_argument(
        "--ws-timeout",
        type=float,
        default=10.0,
        help="Socket-timeout i sekunder per recv-kall (standard: 10).",
    )
    return parser.parse_args()


def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ais_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at_utc TEXT NOT NULL,
            ais_time_utc TEXT,
            message_type TEXT NOT NULL,
            mmsi INTEGER,
            ship_name TEXT,
            latitude REAL,
            longitude REAL,
            sog REAL,
            cog REAL,
            true_heading REAL,
            navigational_status INTEGER,
            source TEXT NOT NULL,
            raw_json TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_mmsi_time ON ais_messages (mmsi, received_at_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_position ON ais_messages (latitude, longitude)")
    conn.commit()
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def get_nested(data: dict[str, Any], *paths: tuple[str, ...] | str) -> Any:
    for path in paths:
        if isinstance(path, str):
            value = data.get(path)
        else:
            value = data
            for key in path:
                if not isinstance(value, dict):
                    value = None
                    break
                value = value.get(key)
        if value is not None:
            return value
    return None


def normalize_message(message: dict[str, Any]) -> NormalizedMessage:
    metadata = get_nested(message, "MetaData", "Metadata") or {}
    message_type = str(message.get("MessageType") or "Unknown")
    body = message.get("Message") or {}
    typed_body = body.get(message_type) or {}

    lat = get_nested(metadata, "latitude", "Latitude") or get_nested(typed_body, "Latitude")
    lon = get_nested(metadata, "longitude", "Longitude") or get_nested(typed_body, "Longitude")
    ais_time = get_nested(metadata, "time_utc", "Time_utc", "TimeUTC")
    ship_name = get_nested(metadata, "ShipName", "ship_name")
    if ship_name is None and isinstance(typed_body, dict):
        ship_name = typed_body.get("Name") or typed_body.get("ShipName")

    return NormalizedMessage(
        received_at_utc=utc_now_iso(),
        ais_time_utc=str(ais_time) if ais_time is not None else None,
        message_type=message_type,
        mmsi=_to_int(get_nested(metadata, "MMSI") or get_nested(typed_body, "UserID")),
        ship_name=str(ship_name).strip() if ship_name else None,
        latitude=_to_float(lat),
        longitude=_to_float(lon),
        sog=_to_float(get_nested(typed_body, "Sog")),
        cog=_to_float(get_nested(typed_body, "Cog")),
        true_heading=_to_float(get_nested(typed_body, "TrueHeading")),
        navigational_status=_to_int(get_nested(typed_body, "NavigationalStatus")),
        source="AISStream",
        raw_json=json.dumps(message, ensure_ascii=False),
    )


def store_message(conn: sqlite3.Connection, msg: NormalizedMessage) -> None:
    conn.execute(
        """
        INSERT INTO ais_messages (
            received_at_utc, ais_time_utc, message_type, mmsi, ship_name,
            latitude, longitude, sog, cog, true_heading, navigational_status,
            source, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            msg.received_at_utc,
            msg.ais_time_utc,
            msg.message_type,
            msg.mmsi,
            msg.ship_name,
            msg.latitude,
            msg.longitude,
            msg.sog,
            msg.cog,
            msg.true_heading,
            msg.navigational_status,
            msg.source,
            msg.raw_json,
        ),
    )


def build_subscription(args: argparse.Namespace) -> dict[str, Any]:
    message_types = DEFAULT_MESSAGE_TYPES if args.include_static else [
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
    ]
    return {
        "APIKey": args.api_key,
        "BoundingBoxes": [[[args.min_lat, args.min_lon], [args.max_lat, args.max_lon]]],
        "FilterMessageTypes": message_types,
    }


def main() -> int:
    load_dotenv()
    args = parse_args()
    args.api_key = args.api_key or os.getenv("AISTREAM_API_KEY")
    if not args.api_key:
        print("Feil: mangler API-nøkkel. Sett AISTREAM_API_KEY eller bruk --api-key.", file=sys.stderr)
        return 2

    stop_after = datetime.now(timezone.utc) + timedelta(minutes=args.minutes) if args.minutes is not None else None
    conn = ensure_db(args.db)
    ws_timeout = max(2.0, float(args.ws_timeout))
    ws = create_connection(WS_URL, timeout=ws_timeout)
    ws.send(json.dumps(build_subscription(args)))

    count = 0
    last_commit = time.monotonic()
    try:
        while True:
            if stop_after and datetime.now(timezone.utc) >= stop_after:
                break
            if args.max_messages is not None and count >= args.max_messages:
                break

            try:
                payload = json.loads(ws.recv())
            except WebSocketTimeoutException:
                # Ingen melding i dette intervallet. Fortsett til tidsgrense/max.
                continue
            if isinstance(payload, dict) and payload.get("error"):
                raise RuntimeError(f"AISStream-feil: {payload['error']}")

            store_message(conn, normalize_message(payload))
            count += 1

            now = time.monotonic()
            if now - last_commit > 2:
                conn.commit()
                last_commit = now
    except KeyboardInterrupt:
        pass
    except WebSocketConnectionClosedException as exc:
        print(f"Tilkoblingen ble lukket: {exc}", file=sys.stderr)
        return 1
    except WebSocketTimeoutException:
        print("WebSocket timeout.", file=sys.stderr)
        return 1
    finally:
        conn.commit()
        conn.close()
        try:
            ws.close()
        except Exception:
            pass

    print(f"Ferdig. Lagret {count} meldinger i {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
