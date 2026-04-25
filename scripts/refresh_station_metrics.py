#!/usr/bin/env python3
"""Cron entrypoint: refresh the precomputed station-metric cache."""
from __future__ import annotations

import json

from scripts.station_metrics_cache import refresh_all


def main() -> int:
    summary = refresh_all()
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
