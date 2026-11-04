"""Analytikerprofiler for NewsWeb: hvilke selskaper hver analytiker følger.

Lagres PRIVAT i samme S3-bøtte som konsensus (NEWSWEB_CONSENSUS_BUCKET, med krav om
Public Access Block), aldri i det offentlige GitHub-repoet. For lokal utvikling kan
NEWSWEB_PROFILES_FILE peke til en lokal JSON-fil.

Datamodell: {"profiles": {"Simen": {"tickers": ["EQNR", ...]}, ...},
             "updated_at": ISO, "updated_by": "Simen"}
Samme selskap kan ligge i flere profiler.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

S3_KEY = "newsweb/profiles/profiler.json"
S3_HISTORY = "newsweb/profiles/history/"
TICKER = re.compile(r"[A-Z0-9][A-Z0-9.\-]{0,14}")
NAME = re.compile(r"[A-Za-zÆØÅæøåÉéÜüÖöÄä][A-Za-zÆØÅæøåÉéÜüÖöÄä .\-]{0,29}")
MAX_TICKERS = 400
_CACHE_SECONDS = 30
_lock = threading.Lock()
_cache: dict = {"at": 0.0, "data": None}


def empty() -> dict:
    return {"profiles": {}, "updated_at": "", "updated_by": ""}


def clean_name(name: object) -> str | None:
    name = " ".join(str(name or "").split())
    return name if NAME.fullmatch(name) else None


def parse_tickers(text: object) -> list[str]:
    """Tickere fra fritekst: komma, semikolon, mellomrom eller linjeskift. Unike, i rekkefølge."""
    out: list[str] = []
    for raw in re.split(r"[\s,;]+", str(text or "")):
        ticker = raw.strip().upper()
        if ticker and TICKER.fullmatch(ticker) and ticker not in out:
            out.append(ticker)
    return out[:MAX_TICKERS]


def parse_csv(payload: bytes) -> dict[str, list[str]]:
    """CSV med kolonnene Ticker og Ansvarlig (semikolon eller komma). UTF-8 eller Windows-1252."""
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = payload.decode("cp1252")
    dialect = ";" if text.count(";") >= text.count(",") else ","
    rows = [r for r in csv.reader(io.StringIO(text), delimiter=dialect) if len(r) >= 2]
    if rows and rows[0][0].strip().lower() in ("ticker", "aksje", "symbol"):
        rows = rows[1:]
    result: dict[str, list[str]] = {}
    for ticker_raw, owner_raw, *_ in rows:
        owner = clean_name(owner_raw)
        tickers = parse_tickers(ticker_raw)
        if not owner or not tickers:
            continue
        lst = result.setdefault(owner, [])
        for t in tickers:
            if t not in lst:
                lst.append(t)
    if not result:
        raise ValueError("Fant ingen rader med Ticker og Ansvarlig i filen.")
    return result


def normalize(data: object) -> dict:
    out = empty()
    if not isinstance(data, dict):
        return out
    for name, prof in (data.get("profiles") or {}).items():
        name = clean_name(name)
        if name and isinstance(prof, dict):
            out["profiles"][name] = {"tickers": parse_tickers(" ".join(prof.get("tickers") or []))}
    out["updated_at"] = str(data.get("updated_at") or "")[:40]
    out["updated_by"] = str(data.get("updated_by") or "")[:40]
    return out


# ---------- lagring ----------
def _local_path() -> Path | None:
    path = os.getenv("NEWSWEB_PROFILES_FILE", "").strip()
    return Path(path) if path else None


def _backend() -> str:
    if os.getenv("NEWSWEB_CONSENSUS_BUCKET", "").strip():
        return "s3"
    if _local_path():
        return "file"
    raise RuntimeError("Profiler krever NEWSWEB_CONSENSUS_BUCKET (privat S3) eller NEWSWEB_PROFILES_FILE.")


def load(force: bool = False) -> dict:
    now = time.monotonic()
    with _lock:
        if not force and _cache["data"] is not None and now - _cache["at"] < _CACHE_SECONDS:
            return json.loads(json.dumps(_cache["data"]))
    backend = _backend()
    if backend == "s3":
        from newsweb_consensus import _s3
        client, bucket = _s3()
        try:
            blob = client.get_object(Bucket=bucket, Key=S3_KEY)["Body"].read(1_000_001)
            data = normalize(json.loads(blob))
        except client.exceptions.NoSuchKey:
            data = empty()
    else:
        path = _local_path()
        data = normalize(json.loads(path.read_text(encoding="utf-8"))) if path.exists() else empty()
    with _lock:
        _cache.update(at=time.monotonic(), data=data)
    return json.loads(json.dumps(data))


def save(data: dict, *, by: str) -> dict:
    data = normalize(data)
    data["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    data["updated_by"] = (clean_name(by) or "")[:40]
    body = json.dumps(data, ensure_ascii=False, indent=1).encode("utf-8")
    if _backend() == "s3":
        from newsweb_consensus import _s3
        client, bucket = _s3()
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        for key in (S3_KEY, S3_HISTORY + stamp + ".json"):  # historikk gir angremulighet
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json",
                              ServerSideEncryption="AES256", CacheControl="private,no-store")
    else:
        path = _local_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    with _lock:
        _cache.update(at=time.monotonic(), data=data)
    return data


def update_profile(name: str, tickers: list[str], *, by: str) -> dict:
    """Les siste versjon og endre bare én profil, så samtidige endringer i andre profiler bevares."""
    name = clean_name(name)
    if not name:
        raise ValueError("Ugyldig profilnavn (1–30 bokstaver).")
    data = load(force=True)
    data["profiles"][name] = {"tickers": parse_tickers(" ".join(tickers))}
    return save(data, by=by)


def delete_profile(name: str, *, by: str) -> dict:
    data = load(force=True)
    data["profiles"].pop(str(name), None)
    return save(data, by=by)


def import_profiles(lists: dict[str, list[str]], *, replace: bool, by: str) -> dict:
    data = load(force=True)
    for name, tickers in lists.items():
        current = [] if replace else data["profiles"].get(name, {}).get("tickers", [])
        data["profiles"][name] = {"tickers": parse_tickers(" ".join(current + tickers))}
    return save(data, by=by)


# ---------- bruk i dashboardet ----------
def owners_by_ticker(data: dict) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for name, prof in sorted((data.get("profiles") or {}).items()):
        for t in prof.get("tickers") or []:
            out.setdefault(t, []).append(name)
    return out


def check_tickers(tickers: list[str], issuers: dict[str, str]) -> dict[str, str]:
    """{ticker: merknad} for tickere som ikke er aktive i NewsWeb. Tom hvis listen mangler."""
    if not issuers:
        return {}
    return {t: "ikke aktiv/ukjent i NewsWeb" for t in tickers if t not in issuers}
