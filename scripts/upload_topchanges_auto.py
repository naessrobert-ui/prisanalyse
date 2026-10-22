#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Automatisk opplasting av nyeste TopChanges-CSV til prisanalyse-appen.

Plukker nyeste `TopChanges_Nordea_Invest_DAG_YYMMDD.csv` fra en mappe
(f.eks. en Nordea-nettverksdeling), og sender den til appens endepunkt
`/handler-oslo-bors/api/upload-csv-update-db`, som oppdaterer databasen
og laster den opp til S3.

Designet for å kjøres av Windows Task Scheduler én gang per virkedag.
Bruker KUN Python-standardbiblioteket (ingen pip-avhengigheter), slik at
det kjører på en låst maskin uten installasjon.

Oppsett (miljøvariabler – kan også overstyres med argumenter):
    TOPCHANGES_DIR        Mappe der CSV-filene ligger. Bruk UNC-sti, IKKE
                          en tilkoblet stasjon (I:), ellers finner ikke
                          jobben filen når ingen er pålogget.
                          Standard: \\\\no-fileserver\\NIMNOR\\6_EQUITIES\\Database\\Eiere
    HANDLER_UPLOAD_URL    Full URL til opplastings-endepunktet.
                          Standard: https://prisanalyse.no/handler-oslo-bors/api/upload-csv-update-db
    HANDLER_UPLOAD_TOKEN  Hemmelig token (samme som i Render). PÅKREVD.
    TOPCHANGES_STATE      Fil som husker sist opplastede dato.
    TOPCHANGES_LOG        Loggfil.

Avslutningskoder:
    0  OK, eller ingenting nytt å gjøre (helg/helligdag).
    2  Feil (ingen token, mappe mangler, opplasting feilet osv.).
       Task Scheduler viser da "Last Run Result" != 0.

Eksempel (manuell test):
    python upload_topchanges_auto.py --dry-run
    python upload_topchanges_auto.py --force
"""
from __future__ import annotations

import argparse
import glob
import logging
import mimetypes
import os
import re
import sys
import time
import uuid
from urllib import request as urlrequest
from urllib import error as urlerror

# --- Standardverdier (kan overstyres med miljøvariabler/argumenter) ---
DEFAULT_DIR = r"\\no-fileserver\NIMNOR\6_EQUITIES\Database\Eiere"
DEFAULT_URL = "https://prisanalyse.no/handler-oslo-bors/api/upload-csv-update-db"
DEFAULT_PATTERN = "TopChanges_Nordea_Invest_DAG_*.csv"
DEFAULT_STATE = os.path.join(
    os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
    "topchanges_upload", "sist_opplastet.txt",
)
DEFAULT_LOG = os.path.join(
    os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
    "topchanges_upload", "upload.log",
)

_DATE_RE = re.compile(r"(\d{6})")  # YYMMDD i filnavnet
_NETWORK_RETRIES = 4
_TIMEOUT_SEC = 600


def _setup_logging(log_path: str) -> None:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _file_date_key(path: str) -> str:
    """Henter YYMMDD fra filnavnet; faller tilbake til mtime hvis mangler."""
    m = _DATE_RE.search(os.path.basename(path))
    if m:
        return m.group(1)
    # Ingen dato i navnet -> bruk endringstid som sorteringsnøkkel.
    return time.strftime("%y%m%d", time.localtime(os.path.getmtime(path)))


def _find_newest(folder: str, pattern: str) -> str | None:
    files = glob.glob(os.path.join(folder, pattern))
    if not files:
        return None
    return max(files, key=_file_date_key)


def _read_state(state_path: str) -> str:
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _write_state(state_path: str, value: str) -> None:
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        f.write(value)


def _build_multipart(file_path: str, field_name: str = "csv_files") -> tuple[bytes, str]:
    """Bygger en multipart/form-data body (kun stdlib)."""
    boundary = "----topchanges-" + uuid.uuid4().hex
    filename = os.path.basename(file_path)
    content_type = mimetypes.guess_type(filename)[0] or "text/csv"
    with open(file_path, "rb") as f:
        file_bytes = f.read()

    crlf = b"\r\n"
    parts = [
        b"--" + boundary.encode(),
        (
            'Content-Disposition: form-data; name="%s"; filename="%s"'
            % (field_name, filename)
        ).encode(),
        ("Content-Type: %s" % content_type).encode(),
        b"",
        file_bytes,
        b"--" + boundary.encode() + b"--",
        b"",
    ]
    body = crlf.join(parts)
    return body, boundary


def _post_file(url: str, token: str, file_path: str) -> tuple[int, str]:
    body, boundary = _build_multipart(file_path)
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "multipart/form-data; boundary=" + boundary,
        "Content-Length": str(len(body)),
    }
    last_err: Exception | None = None
    for attempt in range(1, _NETWORK_RETRIES + 1):
        try:
            req = urlrequest.Request(url, data=body, headers=headers, method="POST")
            with urlrequest.urlopen(req, timeout=_TIMEOUT_SEC) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")
        except urlerror.HTTPError as e:
            # HTTP-feil (401/400/500) er ikke forbigående -> ikke prøv igjen.
            return e.code, e.read().decode("utf-8", "replace")
        except (urlerror.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
            wait = 2 ** attempt
            logging.warning("Nettverksfeil (forsøk %d/%d): %s – venter %ds",
                            attempt, _NETWORK_RETRIES, e, wait)
            time.sleep(wait)
    raise RuntimeError("Opplasting feilet etter %d forsøk: %s"
                       % (_NETWORK_RETRIES, last_err))


def main() -> int:
    ap = argparse.ArgumentParser(description="Last opp nyeste TopChanges-CSV automatisk.")
    ap.add_argument("--dir", default=os.environ.get("TOPCHANGES_DIR", DEFAULT_DIR))
    ap.add_argument("--pattern", default=os.environ.get("TOPCHANGES_PATTERN", DEFAULT_PATTERN))
    ap.add_argument("--url", default=os.environ.get("HANDLER_UPLOAD_URL", DEFAULT_URL))
    ap.add_argument("--state", default=os.environ.get("TOPCHANGES_STATE", DEFAULT_STATE))
    ap.add_argument("--log", default=os.environ.get("TOPCHANGES_LOG", DEFAULT_LOG))
    ap.add_argument("--force", action="store_true", help="Last opp selv om datoen alt er sendt.")
    ap.add_argument("--dry-run", action="store_true", help="Finn filen, men ikke last opp.")
    args = ap.parse_args()

    _setup_logging(args.log)
    logging.info("=== Start automatisk TopChanges-opplasting ===")

    token = (os.environ.get("HANDLER_UPLOAD_TOKEN", "") or "").strip()
    if not token and not args.dry_run:
        logging.error("HANDLER_UPLOAD_TOKEN er ikke satt. Avbryter.")
        return 2

    if not os.path.isdir(args.dir):
        logging.error("Mappen finnes ikke / ikke tilgjengelig: %s", args.dir)
        logging.error("Tips: bruk UNC-sti, ikke en tilkoblet stasjon (I:), "
                      "når jobben kjører uten at du er pålogget.")
        return 2

    newest = _find_newest(args.dir, args.pattern)
    if not newest:
        logging.info("Ingen filer som matcher %r i %s – avslutter (helg/helligdag?).",
                     args.pattern, args.dir)
        return 0

    file_date = _file_date_key(newest)
    logging.info("Nyeste fil: %s (dato=%s)", newest, file_date)

    last = _read_state(args.state)
    if last == file_date and not args.force:
        logging.info("Dato %s er allerede lastet opp – ingenting å gjøre.", file_date)
        return 0

    if args.dry_run:
        logging.info("[dry-run] Ville ha lastet opp %s til %s", newest, args.url)
        return 0

    logging.info("Laster opp til %s ...", args.url)
    status, text = _post_file(args.url, token, newest)
    snippet = text if len(text) <= 500 else text[:500] + "…"
    if status == 200:
        logging.info("OK (HTTP 200): %s", snippet)
        _write_state(args.state, file_date)
        logging.info("Oppdaterte status-fil: %s = %s", args.state, file_date)
        return 0

    logging.error("Opplasting feilet (HTTP %s): %s", status, snippet)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 – logg alt, returner feilkode
        logging.exception("Uventet feil under opplasting")
        sys.exit(2)
