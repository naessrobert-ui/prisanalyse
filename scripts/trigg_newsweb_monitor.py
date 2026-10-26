#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trigg_newsweb_monitor.py
========================
Starter GitHub-workflowen «NewsWeb AI monitor» via workflow_dispatch.

Hvorfor ikke bare GitHub sin egen schedule:
  GitHub-cron blir ofte forsinket i timevis eller hoppet over helt (morgenen
  26.10.2026 kom ingen av de planlagte kvartersjobbene). Render-cron er
  presis, så den brukes som klokke, mens GitHub gjør selve jobben.

Kjøringen sendes med scheduled=true, slik at workflowen oppfører seg som en
planlagt kjøring (Oslo-tidsvindu, morgen-backfill, vars.NEWSWEB_DRY_RUN).
Tidsvinduet sjekkes også her, så vi ikke starter tomme kjøringer:
  06–10 Oslo: hvert kvarter. 10–23 Oslo: én gang i timen (minutt < 15).

Krever NEWSWEB_GITHUB_TOKEN med Actions: write på repoet (samme token som
«Kjør nå»-knappen på /newsweb/ bruker).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

DISPATCH_URL = (
    "https://api.github.com/repos/naessrobert-ui/prisanalyse/"
    "actions/workflows/newsweb-monitor.yml/dispatches"
)


def innenfor_vindu(naa: datetime) -> bool:
    h, m = naa.hour, naa.minute
    return 6 <= h < 10 or (10 <= h <= 23 and m < 15)


def main() -> int:
    naa = datetime.now(ZoneInfo("Europe/Oslo"))
    if not innenfor_vindu(naa) and os.getenv("NEWSWEB_TVING_KJORING") != "1":
        print(f"Utenfor Oslo-vinduet ({naa:%H:%M}), starter ikke NewsWeb-jobben.")
        return 0

    token = os.getenv("NEWSWEB_GITHUB_TOKEN", "").strip()
    if not token:
        print("[FEIL] NEWSWEB_GITHUB_TOKEN mangler", file=sys.stderr)
        return 1

    payload = json.dumps({"ref": "main", "inputs": {
        "mode": "newsweb", "dry_run": "true", "history_backfill": "false",
        "scheduled": "true"}}).encode()
    req = urllib.request.Request(DISPATCH_URL, data=payload, method="POST", headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "Prisanalyse-NewsWeb/1.0"})
    print(f"Starter NewsWeb-jobben ({naa:%H:%M} Oslo)", flush=True)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 204:
                print("Workflow startet.")
                return 0
            print(f"[FEIL] Uventet status {resp.status}", file=sys.stderr)
            return 1
    except urllib.error.HTTPError as exc:
        detalj = exc.read().decode("utf-8", "replace").strip() if exc.fp else ""
        print(f"[FEIL] HTTP {exc.code}: {detalj}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"[FEIL] Klarte ikke nå GitHub: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
