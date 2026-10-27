"""Flytt eksisterende handler-data fra oppgjørsdato til handelsdato (T+2).

Samme migrering kjøres automatisk ved neste CSV-opplasting. Dette skriptet gjør den
med en gang:

    python scripts/flytt_til_handelsdato.py                # lokal DB (HANDLER_LOCAL_DB_PATH)
    python scripts/flytt_til_handelsdato.py --db sti.db    # en bestemt fil
    python scripts/flytt_til_handelsdato.py --s3           # hent fra S3, migrer, last opp igjen

Med --s3 lastes originalen opp som topchanges.for_handelsdato.db før den overskrives.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import handler_data as hd  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", default=hd.HANDLER_DB_PATH)
    p.add_argument("--s3", action="store_true", help="hent fra og last opp til HANDLER_DB_S3_URI")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    if a.s3 and not hd.refresh_local_db_from_s3(a.db):
        print("Klarte ikke hente DB fra S3")
        return 1
    if not Path(a.db).is_file():
        print(f"Fant ikke {a.db}")
        return 1

    conn = sqlite3.connect(a.db, timeout=60)
    try:
        if not hd.trenger_handelsdato_migrering(conn):
            print("Allerede flyttet til handelsdato, ingenting å gjøre.")
            return 0
        if a.s3:
            print("Backup:", hd.upload_db_backup_to_s3(a.db, "for_handelsdato"))
        else:
            backup = f"{a.db}.for_handelsdato"
            shutil.copy2(a.db, backup)
            print("Backup:", backup)
        n = hd.migrer_til_handelsdato(conn)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        print(f"Flyttet {n} rader. Siste handelsdato nå: {hd.get_max_date_today(conn)}")
    finally:
        conn.close()

    if a.s3:
        print("Lastet opp:", hd.upload_db_bytes_to_s3(Path(a.db).read_bytes(), filename=Path(a.db).name))
    if a.db == hd.HANDLER_DB_PATH:
        hd.refresh_top20_snapshot_db(a.db, hd.HANDLER_TOP20_DB_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
