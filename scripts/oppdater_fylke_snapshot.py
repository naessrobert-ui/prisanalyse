#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
oppdater_fylke_snapshot.py
==========================
Beregner fylkesaggregat for alle fylker og lagrer dem i snapshot-tabellen,
slik at /regnskap/api/fylke/aggregat svarer umiddelbart.

Kjøres av den ukentlige Render-cron-jobben, og kan kjøres manuelt:

    python -m scripts.oppdater_fylke_snapshot

Bruker appens egen DB-tilkobling (DATABASE_URL eller IAM). Samme rutine som
«Oppdater underliggende tall»-knappen på nettsiden kaller.
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main() -> int:
    from analysis_api_compat import oppdater_fylke_snapshot

    print("Oppdaterer fylke-snapshot for alle fylker …", flush=True)
    resultat = oppdater_fylke_snapshot()
    print(resultat)
    return 0 if resultat.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
