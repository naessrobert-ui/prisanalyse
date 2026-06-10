"""Render cron: oppdater og berik media-arkivet for Robert Naess.

Henter alle åpne kilder, merger nye treff inn i parquet-arkivet på S3 og
beriker nye rader (sentiment/tema via Anthropic, og:image + sitat fra
artikkelsidene). Kjøres flere ganger daglig - se render.yaml.

Manuell backfill (henter lengre tilbake i tid der kildene støtter det):
    python -m scripts.oppdater_media_arkiv --days 365
"""

from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Oppdater media-arkivet for Robert Naess")
    parser.add_argument("--days", type=int, default=3, help="Hentevindu i døgn (default 3)")
    parser.add_argument("--uten-x", action="store_true", help="Hopp over X-API-et")
    args = parser.parse_args()

    from media_arkiv import update_archive_from_sources

    result = update_archive_from_sources(days=max(1, args.days), include_x=not args.uten_x)
    print(json.dumps(result, ensure_ascii=False))
    if result["errors"]:
        print(f"Advarsel: {len(result['errors'])} kilde(r) feilet", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
