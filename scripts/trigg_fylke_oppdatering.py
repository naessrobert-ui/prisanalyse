#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trigg_fylke_oppdatering.py
==========================
Ber web-tjenesten om å forhåndsberegne fylkestallene, ved å kalle det åpne
endepunktet over HTTPS. Web-tjenesten gjør selve DB-jobben.

Hvorfor via HTTP og ikke direkte mot databasen:
  En egen cron-tjeneste på Render har en annen utgående IP enn web-tjenesten,
  og den er ikke nødvendigvis åpnet i RDS-sikkerhetsgruppen (gir
  ConnectionTimeout). Web-tjenesten når derimot databasen. Ved å trigge over
  HTTPS slipper cron-en både DB-tilgang og AWS-nøkler – den trenger bare
  vanlig utgående internett.

Brukes av den ukentlige Render-cron-jobben. URL styres av FYLKE_OPPDATER_URL
(default peker på produksjon).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

STANDARD_URL = "https://prisanalyse.no/regnskap/api/fylke/oppdater"
TIMEOUT = int(os.getenv("FYLKE_OPPDATER_TIMEOUT", "90"))


def main() -> int:
    url = os.getenv("FYLKE_OPPDATER_URL", STANDARD_URL).strip() or STANDARD_URL
    print(f"Trigger fylke-oppdatering: POST {url}", flush=True)

    req = urllib.request.Request(url, method="POST", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read().decode("utf-8", "replace").strip()
            print(f"HTTP {resp.status}: {body}")
            if 200 <= resp.status < 300:
                print("Oppdatering startet i web-tjenesten.")
                return 0
            print(f"[FEIL] Uventet status {resp.status}", file=sys.stderr)
            return 1
    except urllib.error.HTTPError as exc:
        detalj = exc.read().decode("utf-8", "replace").strip() if exc.fp else ""
        print(f"[FEIL] HTTP {exc.code}: {detalj}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"[FEIL] Klarte ikke nå {url}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
