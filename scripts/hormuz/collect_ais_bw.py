#!/usr/bin/env python3
"""Collect AIS data from BarentsWatch Live AIS API (HTTP streaming).

Brukes som fallback når AISstream er nede.
Dekker kun norske farvann, men samme SQLite-skjema og output-format
som collect_ais.py slik at hormuz_routes.py fungerer uendret.

Bruk:
    python collect_ais_bw.py --max-messages 200 --db data/hormuz_ais.sqlite
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import requests
except ImportError:
    print("Mangler 'requests'. Kjør: pip install requests", file=sys.stderr)
    sys.exit(1)

TOKEN_URL = "https://id.barentswatch.no/connect/token"
STREAM_URL = "https://live.ais.barentswatch.no/v1/combined"
DEFAULT_DB_PATH = Path("data/hormuz_ais.sqlite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hent AIS-data fra BarentsWatch")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--max-messages", type=int, default=200)
    parser.add_argument("--minutes", type=float, default=None)
    parser.add_argument("--ws-timeout", type=float, default=10.0)  # ignorert, for kompatibilitet
    parser.add_argument("--include-static", action="store_true")   # ignorert, for kompatibilitet
    return parser.parse_args()


def get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "ais",
            "grant_type": "client_credentials",
        },
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Ingen access_token i svar: {resp.text}")
    print("Token hentet fra BarentsWatch.", flush=True)
    return token


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
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def store_vessel(conn: sqlite3.Connection, vessel: dict) -> None:
    conn.execute("""
        INSERT INTO ais_messages (
            received_at_utc, ais_time_utc, message_type, mmsi, ship_name,
            latitude, longitude, sog, cog, true_heading, navigational_status,
            source, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        utc_now_iso(),
        vessel.get("msgtime"),
        "PositionReport",
        _to_int(vessel.get("mmsi")),
        vessel.get("name"),
        _to_float(vessel.get("latitude")),
        _to_float(vessel.get("longitude")),
        _to_float(vessel.get("speedOverGround")),
        _to_float(vessel.get("courseOverGround")),
        _to_int(vessel.get("trueHeading")),
        _to_int(vessel.get("navigationalStatus")),
        "BarentsWatch",
        json.dumps(vessel, ensure_ascii=False),
    ))


def collect(args: argparse.Namespace, token: str) -> int:
    import time as _time
    stop_at = (
        _time.monotonic() + args.minutes * 60
        if args.minutes is not None else None
    )

    conn = ensure_db(args.db)
    count = 0
    last_commit = _time.monotonic()

    print(f"Kobler til BarentsWatch AIS stream ...", flush=True)

    try:
        with requests.get(
            STREAM_URL,
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=30,
        ) as resp:
            resp.raise_for_status()
            print("Tilkoblet. Mottar meldinger ...", flush=True)

            for line in resp.iter_lines():
                if stop_at and _time.monotonic() >= stop_at:
                    print("Tidsgrense nådd.", flush=True)
                    break
                if args.max_messages and count >= args.max_messages:
                    print(f"Maks antall meldinger nådd ({args.max_messages}).", flush=True)
                    break

                if not line:
                    continue

                try:
                    vessel = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not vessel.get("latitude") or not vessel.get("longitude"):
                    continue

                store_vessel(conn, vessel)
                count += 1

                if count % 25 == 0 or count == 1:
                    print(f"Lagret {count} meldinger ...", flush=True)

                now = _time.monotonic()
                if now - last_commit > 2:
                    conn.commit()
                    last_commit = now

    except requests.HTTPError as exc:
        print(f"HTTP-feil: {exc}", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"Feil: {exc}", file=sys.stderr)
        return 0
    finally:
        conn.commit()
        conn.close()

    # Denne strengen parses av hormuz_routes.py: r"Lagret\s+(\d+)\s+meldinger"
    print(f"Lagret {count} meldinger i {args.db}")
    return count


def main() -> int:
    args = parse_args()
    client_id = os.getenv("BW_CLIENT_ID", "").strip()
    client_secret = os.getenv("BW_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print(
            "Feil: Sett BW_CLIENT_ID og BW_CLIENT_SECRET som miljøvariabler.",
            file=sys.stderr,
        )
        return 2

    try:
        token = get_token(client_id, client_secret)
    except Exception as exc:
        print(f"Kunne ikke hente token: {exc}", file=sys.stderr)
        return 1

    count = collect(args, token)
    return 0 if count >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
