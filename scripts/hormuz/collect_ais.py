#!/usr/bin/env python3
"""Collect AIS data from AISStream for the Strait of Hormuz area.

Bruker asyncio + websockets (offisiell AISstream-metode).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import websockets
except ImportError:
    print("Mangler 'websockets'. Kjør: pip install websockets", file=sys.stderr)
    sys.exit(1)

WS_URL = "wss://stream.aisstream.io/v0/stream"
DEFAULT_DB_PATH = Path("data/hormuz_ais.sqlite")

# Hormuz-stredet: [[min_lat, min_lon], [max_lat, max_lon]]
DEFAULT_MIN_LAT = 23.5
DEFAULT_MIN_LON = 55.5
DEFAULT_MAX_LAT = 27.5
DEFAULT_MAX_LON = 60.5

DEFAULT_MESSAGE_TYPES = [
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StaticDataReport",
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
    parser.add_argument("--api-key")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--minutes", type=float, default=None)
    parser.add_argument("--include-static", action="store_true")
    parser.add_argument("--min-lat", type=float, default=DEFAULT_MIN_LAT)
    parser.add_argument("--min-lon", type=float, default=DEFAULT_MIN_LON)
    parser.add_argument("--max-lat", type=float, default=DEFAULT_MAX_LAT)
    parser.add_argument("--max-lon", type=float, default=DEFAULT_MAX_LON)
    parser.add_argument("--ws-timeout", type=float, default=10.0)
    return parser.parse_args()


def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
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
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_mmsi_time ON ais_messages (mmsi, received_at_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_position ON ais_messages (latitude, longitude)")
    conn.commit()
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _to_float(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return None if value is None or value == "" else int(value)
    except (TypeError, ValueError):
        return None


def normalize_message(message: dict[str, Any]) -> NormalizedMessage:
    metadata = message.get("MetaData") or message.get("Metadata") or {}
    message_type = str(message.get("MessageType") or "Unknown")
    body = message.get("Message") or {}
    typed_body = body.get(message_type) or {}

    lat = metadata.get("latitude") or metadata.get("Latitude") or typed_body.get("Latitude")
    lon = metadata.get("longitude") or metadata.get("Longitude") or typed_body.get("Longitude")
    ais_time = metadata.get("time_utc") or metadata.get("Time_utc")
    ship_name = metadata.get("ShipName") or metadata.get("ship_name")
    if not ship_name:
        ship_name = typed_body.get("Name") or typed_body.get("ShipName")

    return NormalizedMessage(
        received_at_utc=utc_now_iso(),
        ais_time_utc=str(ais_time) if ais_time else None,
        message_type=message_type,
        mmsi=_to_int(metadata.get("MMSI") or typed_body.get("UserID")),
        ship_name=str(ship_name).strip() if ship_name else None,
        latitude=_to_float(lat),
        longitude=_to_float(lon),
        sog=_to_float(typed_body.get("Sog")),
        cog=_to_float(typed_body.get("Cog")),
        true_heading=_to_float(typed_body.get("TrueHeading")),
        navigational_status=_to_int(typed_body.get("NavigationalStatus")),
        source="AISStream",
        raw_json=json.dumps(message, ensure_ascii=False),
    )


def store_message(conn: sqlite3.Connection, msg: NormalizedMessage) -> None:
    conn.execute("""
        INSERT INTO ais_messages (
            received_at_utc, ais_time_utc, message_type, mmsi, ship_name,
            latitude, longitude, sog, cog, true_heading, navigational_status,
            source, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        msg.received_at_utc, msg.ais_time_utc, msg.message_type, msg.mmsi,
        msg.ship_name, msg.latitude, msg.longitude, msg.sog, msg.cog,
        msg.true_heading, msg.navigational_status, msg.source, msg.raw_json,
    ))


async def collect(args: argparse.Namespace, api_key: str) -> int:
    stop_at = (
        datetime.now(timezone.utc) + timedelta(minutes=args.minutes)
        if args.minutes is not None else None
    )

    message_types = DEFAULT_MESSAGE_TYPES if args.include_static else [
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
    ]

    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [[[args.min_lat, args.min_lon], [args.max_lat, args.max_lon]]],
        "FilterMessageTypes": message_types,
    }

    print(
        f"Kobler til AISstream.io (asyncio/websockets) | "
        f"bbox=[[{args.min_lat},{args.min_lon}],[{args.max_lat},{args.max_lon}]]",
        flush=True,
    )

    conn = ensure_db(args.db)
    count = 0
    last_commit = time.monotonic()

    try:
        async with websockets.connect(
            WS_URL,
            open_timeout=15,
            ping_interval=20,
            ping_timeout=30,
        ) as ws:
            await ws.send(json.dumps(subscription))
            print("Abonnement sendt, venter på meldinger ...", flush=True)

            async for raw in ws:
                # Sjekk tidsgrense
                if stop_at and datetime.now(timezone.utc) >= stop_at:
                    print("Tidsgrense nådd.", flush=True)
                    break
                if args.max_messages is not None and count >= args.max_messages:
                    print(f"Maks antall meldinger nådd ({args.max_messages}).", flush=True)
                    break

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Feilmelding fra server
                if isinstance(data, dict) and data.get("error"):
                    print(f"FEIL fra AISstream: {data['error']}", file=sys.stderr)
                    break

                store_message(conn, normalize_message(data))
                count += 1

                if count % 25 == 0 or count == 1:
                    print(f"Lagret {count} meldinger ...", flush=True)

                now = time.monotonic()
                if now - last_commit > 2:
                    conn.commit()
                    last_commit = now

    except websockets.exceptions.InvalidStatus as exc:
        print(f"WebSocket avvist (HTTP {exc.response.status_code}): sjekk API-nøkkel", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"Uventet feil: {exc}", file=sys.stderr)
        return 0
    finally:
        conn.commit()
        conn.close()

    # Denne strengen parses av hormuz_routes.py: r"Lagret\s+(\d+)\s+meldinger"
    print(f"Lagret {count} meldinger i {args.db}")
    return count


def main() -> int:
    args = parse_args()
    api_key = args.api_key or os.getenv("AISTREAM_API_KEY", "").strip()
    if not api_key:
        print("Feil: mangler API-nøkkel. Sett AISTREAM_API_KEY eller bruk --api-key.", file=sys.stderr)
        return 2

    count = asyncio.run(collect(args, api_key))
    return 0 if count >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
