#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from scripts.ver_routes import (
    _LEADERBOARD_CACHE_PATH,
    _build_weather_topplister_payload,
    _save_cached_topplister,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Forhåndsberegn klima-topplister og lagre til lokal filcache."
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Skriv payload til stdout i tillegg til å lagre cache.",
    )
    args = parser.parse_args()

    started = datetime.now(timezone.utc)
    payload = _build_weather_topplister_payload()
    _save_cached_topplister(payload)
    finished = datetime.now(timezone.utc)

    summary = {
        "status": "ok",
        "cache_file": str(_LEADERBOARD_CACHE_PATH),
        "generated_at": payload.get("generated_at"),
        "station_count": payload.get("station_count"),
        "top_n": payload.get("top_n"),
        "duration_seconds": round((finished - started).total_seconds(), 2),
    }

    print(json.dumps(summary, ensure_ascii=False))
    if args.print_json:
        print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
