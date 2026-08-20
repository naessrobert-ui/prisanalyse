#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
opprett_fylke_indekser.py
=========================
Oppretter indeksene som gjør fylkes-uttrekk raske (se
scripts/sql/idx_entity_kommune_prefix.sql), uten at du trenger psql.

Kjøres ETT sted som har DB-tilgang – typisk på serveren der appen kjører
(f.eks. Render Shell), ikke i det nett-begrensede agentmiljøet:

    python scripts/opprett_fylke_indekser.py
    # eller som modul (samme mønster som cron-jobbene):
    python -m scripts.opprett_fylke_indekser

Tilkobling:
  - Bruker DATABASE_URL hvis satt.
  - Ellers appens egen IAM-baserte tilkobling (Fastapi_Backend.get_conn).

Trygt å kjøre om igjen: alle indekser er `IF NOT EXISTS`. `CONCURRENTLY`
gjør at bygging ikke låser tabellen for skriving; derfor kjøres hver setning
med autocommit (utenfor en transaksjon).
"""

from __future__ import annotations

import os
import re
import sys

# Sørg for at repo-roten er på sys.path (så `import Fastapi_Backend` virker
# også når scriptet kjøres direkte fra scripts/).
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

SQL_STI = os.path.join(_ROOT, "scripts", "sql", "idx_entity_kommune_prefix.sql")


def _connect():
    """Åpner en DB-tilkobling via DATABASE_URL eller appens IAM-tilkobling."""
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        import psycopg
        return psycopg.connect(url)
    # Fallback: gjenbruk appens egen tilkobling (IAM-token mot RDS).
    from Fastapi_Backend import get_conn
    return get_conn()


def _statements(sql_tekst: str) -> list[str]:
    """Fjerner kommentarlinjer og deler opp i enkeltsetninger."""
    uten_kommentarer = "\n".join(
        linje for linje in sql_tekst.splitlines() if not linje.lstrip().startswith("--")
    )
    return [s.strip() for s in uten_kommentarer.split(";") if s.strip()]


def main() -> int:
    try:
        with open(SQL_STI, encoding="utf-8") as f:
            setninger = _statements(f.read())
    except OSError as exc:
        print(f"[FEIL] Fant ikke SQL-fila: {exc}", file=sys.stderr)
        return 1

    if not setninger:
        print("[FEIL] Ingen SQL-setninger å kjøre.", file=sys.stderr)
        return 1

    try:
        conn = _connect()
    except Exception as exc:  # noqa: BLE001 - vil vise hva som gikk galt
        print(f"[FEIL] Klarte ikke koble til databasen: {exc}", file=sys.stderr)
        return 1

    conn.autocommit = True  # CONCURRENTLY kan ikke kjøre i en transaksjon
    feil = 0
    try:
        with conn.cursor() as cur:
            for setning in setninger:
                navn = re.search(r"idx_[a-z_]+", setning)
                etikett = navn.group(0) if navn else setning[:40]
                print(f"→ Oppretter {etikett} …", flush=True)
                try:
                    cur.execute(setning)
                    print(f"  OK: {etikett}")
                except Exception as exc:  # noqa: BLE001
                    feil += 1
                    print(f"  [FEIL] {etikett}: {exc}", file=sys.stderr)
    finally:
        conn.close()

    if feil:
        print(f"\nFerdig med {feil} feil. Se meldingene over.", file=sys.stderr)
        return 1
    print("\nAlle indekser er på plass. Fylkes-uttrekk skal nå være raske.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
