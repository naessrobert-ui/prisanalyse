# handler_data.py
"""
Data-layer for Handler Oslo Børs — ren Python, ingen Streamlit-avhengighet.
Inneholder alle SQL-spørringer og hjelpefunksjoner som trengs av Flask-rutene.
"""
from __future__ import annotations

import os
import sqlite3
import datetime as dt
import logging
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import pandas as pd
import boto3
from botocore.exceptions import ClientError


# =========================================================
# Config (mirrors handler/app_config.py)
# =========================================================
import tempfile


def _path_from_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value:
        return os.path.expandvars(os.path.expanduser(value))
    return default


HANDLER_DB_PATH = _path_from_env(
    "HANDLER_LOCAL_DB_PATH",
    _path_from_env(
        "HANDLER_LOCAL_WORKDIR",
        os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work"),
    )
    + os.sep
    + _path_from_env("HANDLER_LOCAL_DB_NAME", "topchanges.db"),
)

HANDLER_LIST_DIR = _path_from_env(
    "HANDLER_LIST_DIR",
    r"I:\6_EQUITIES\Database\Eiere-Styring",
)
HANDLER_LIST_S3_PREFIX = _path_from_env("HANDLER_LIST_S3_PREFIX", "")
HANDLER_LIST_CACHE_DIR = _path_from_env(
    "HANDLER_LIST_CACHE_DIR",
    os.path.join(tempfile.gettempdir(), "topchanges_list_cache"),
)

HANDLER_DB_S3_URI = _path_from_env("HANDLER_DB_S3_URI", "")
HANDLER_DB_S3_REGION = _path_from_env("HANDLER_DB_S3_REGION", "")
HANDLER_DB_S3_AUTO_DOWNLOAD = _path_from_env("HANDLER_DB_S3_AUTO_DOWNLOAD", "1").lower() not in {
    "0",
    "false",
    "no",
}
HANDLER_DB_S3_PREFER = _path_from_env("HANDLER_DB_S3_PREFER", "1").lower() not in {
    "0",
    "false",
    "no",
}
HANDLER_DB_S3_FORCE_DOWNLOAD = _path_from_env("HANDLER_DB_S3_FORCE_DOWNLOAD", "0").lower() in {
    "1",
    "true",
    "yes",
}

_LOG = logging.getLogger(__name__)
_S3_SYNC_ATTEMPTED: set[str] = set()


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    raw = (uri or "").strip()
    if not raw:
        raise ValueError("Tom S3-URI")

    normalized = raw[5:] if raw.startswith("s3://") else raw
    parts = normalized.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Ugyldig S3-URI. Forventet format: s3://bucket/key")
    return parts[0], parts[1]


def _parse_s3_bucket_prefix(value: str) -> tuple[str, str]:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Tom S3 bucket/prefix")
    normalized = raw[5:] if raw.startswith("s3://") else raw
    parts = normalized.split("/", 1)
    bucket = parts[0].strip()
    if not bucket:
        raise ValueError("Mangler bucket i S3 sti")
    prefix = parts[1].strip() if len(parts) > 1 else ""
    return bucket, prefix


def _candidate_s3_keys(key: str, local_path: str) -> list[str]:
    clean = key.strip().lstrip("/")
    if not clean:
        return []

    candidates = [clean]
    if clean.endswith("/"):
        local_name = Path(local_path).name or "topchanges.db"
        for suffix in ("topchanges.db", "topchanges", local_name):
            c = f"{clean}{suffix}".replace("//", "/")
            if c not in candidates:
                candidates.append(c)
    return candidates


def _download_db_from_s3(local_path: str | None = None) -> bool:
    if not HANDLER_DB_S3_URI:
        return False

    path = Path(local_path or HANDLER_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        bucket, key = _parse_s3_uri(HANDLER_DB_S3_URI)
        client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
        s3 = boto3.client("s3", **client_args)

        candidates = _candidate_s3_keys(key, str(path))
        for candidate in candidates:
            try:
                s3.download_file(bucket, candidate, str(path))
                _LOG.info("Lastet handler-db fra S3: s3://%s/%s til %s", bucket, candidate, path)
                return path.is_file()
            except ClientError as exc:
                err_code = exc.response.get("Error", {}).get("Code", "")
                if err_code in {"404", "NoSuchKey", "NotFound"}:
                    _LOG.warning("S3 key ikke funnet: s3://%s/%s", bucket, candidate)
                    continue
                _LOG.warning("S3-feil ved nedlasting av s3://%s/%s: %s", bucket, candidate, exc)
                return False

        _LOG.warning("Fant ingen gyldig S3 DB-fil for %s. Forsøkte nøkler: %s", HANDLER_DB_S3_URI, candidates)
        return False
    except Exception as exc:
        _LOG.warning("Klarte ikke laste handler-db fra S3 (%s): %s", HANDLER_DB_S3_URI, exc)
        return False


def ensure_local_db(local_path: str | None = None) -> bool:
    path = local_path or HANDLER_DB_PATH

    # Force-download on each check (overwrites local cache) when explicitly enabled
    if HANDLER_DB_S3_URI and HANDLER_DB_S3_FORCE_DOWNLOAD:
        if _download_db_from_s3(path):
            return True

    # Prefer S3 copy when configured (attempt once per process/path)
    if HANDLER_DB_S3_URI and HANDLER_DB_S3_PREFER and path not in _S3_SYNC_ATTEMPTED:
        _S3_SYNC_ATTEMPTED.add(path)
        if _download_db_from_s3(path):
            return True

    if os.path.isfile(path):
        return True

    if not HANDLER_DB_S3_AUTO_DOWNLOAD:
        return False

    return _download_db_from_s3(path)

HANDLER_DB_S3_URI = _path_from_env("HANDLER_DB_S3_URI", "")
HANDLER_DB_S3_REGION = _path_from_env("HANDLER_DB_S3_REGION", "")
HANDLER_DB_S3_AUTO_DOWNLOAD = _path_from_env("HANDLER_DB_S3_AUTO_DOWNLOAD", "1").lower() not in {
    "0",
    "false",
    "no",
}
HANDLER_DB_S3_PREFER = _path_from_env("HANDLER_DB_S3_PREFER", "1").lower() not in {
    "0",
    "false",
    "no",
}
HANDLER_DB_S3_FORCE_DOWNLOAD = _path_from_env("HANDLER_DB_S3_FORCE_DOWNLOAD", "0").lower() in {
    "1",
    "true",
    "yes",
}

_LOG = logging.getLogger(__name__)
_S3_SYNC_ATTEMPTED: set[str] = set()


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    raw = (uri or "").strip()
    if not raw:
        raise ValueError("Tom S3-URI")

    normalized = raw[5:] if raw.startswith("s3://") else raw
    parts = normalized.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Ugyldig S3-URI. Forventet format: s3://bucket/key")
    return parts[0], parts[1]


def _candidate_s3_keys(key: str, local_path: str) -> list[str]:
    clean = key.strip().lstrip("/")
    if not clean:
        return []

    candidates = [clean]
    if clean.endswith("/"):
        local_name = Path(local_path).name or "topchanges.db"
        for suffix in ("topchanges.db", "topchanges", local_name):
            c = f"{clean}{suffix}".replace("//", "/")
            if c not in candidates:
                candidates.append(c)
    return candidates


def _download_db_from_s3(local_path: str | None = None) -> bool:
    if not HANDLER_DB_S3_URI:
        return False

    path = Path(local_path or HANDLER_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        bucket, key = _parse_s3_uri(HANDLER_DB_S3_URI)
        client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
        s3 = boto3.client("s3", **client_args)

        candidates = _candidate_s3_keys(key, str(path))
        for candidate in candidates:
            try:
                s3.download_file(bucket, candidate, str(path))
                _LOG.info("Lastet handler-db fra S3: s3://%s/%s til %s", bucket, candidate, path)
                return path.is_file()
            except ClientError as exc:
                err_code = exc.response.get("Error", {}).get("Code", "")
                if err_code in {"404", "NoSuchKey", "NotFound"}:
                    _LOG.warning("S3 key ikke funnet: s3://%s/%s", bucket, candidate)
                    continue
                _LOG.warning("S3-feil ved nedlasting av s3://%s/%s: %s", bucket, candidate, exc)
                return False

        _LOG.warning("Fant ingen gyldig S3 DB-fil for %s. Forsøkte nøkler: %s", HANDLER_DB_S3_URI, candidates)
        return False
    except Exception as exc:
        _LOG.warning("Klarte ikke laste handler-db fra S3 (%s): %s", HANDLER_DB_S3_URI, exc)
        return False


def ensure_local_db(local_path: str | None = None) -> bool:
    path = local_path or HANDLER_DB_PATH

    # Force-download on each check (overwrites local cache) when explicitly enabled
    if HANDLER_DB_S3_URI and HANDLER_DB_S3_FORCE_DOWNLOAD:
        if _download_db_from_s3(path):
            return True

    # Prefer S3 copy when configured (attempt once per process/path)
    if HANDLER_DB_S3_URI and HANDLER_DB_S3_PREFER and path not in _S3_SYNC_ATTEMPTED:
        _S3_SYNC_ATTEMPTED.add(path)
        if _download_db_from_s3(path):
            return True

    if os.path.isfile(path):
        return True

    if not HANDLER_DB_S3_AUTO_DOWNLOAD:
        return False

    return _download_db_from_s3(path)


# =========================================================
# DB connection
# =========================================================
def db_connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or HANDLER_DB_PATH
    if not ensure_local_db(path):
        raise FileNotFoundError(f"Database ikke funnet på sti: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def db_available(db_path: str | None = None) -> bool:
    return ensure_local_db(db_path)


def db_diagnostics(local_path: str | None = None) -> dict:
    path = local_path or HANDLER_DB_PATH
    p = Path(path)
    parsed = None
    parse_error = ""
    if HANDLER_DB_S3_URI:
        try:
            bucket, key = _parse_s3_uri(HANDLER_DB_S3_URI)
            parsed = f"s3://{bucket}/{key}"
        except Exception as exc:
            parse_error = str(exc)

    return {
        "path": str(p),
        "path_exists": p.is_file(),
        "parent_exists": p.parent.exists(),
        "s3_uri_configured": bool(HANDLER_DB_S3_URI),
        "s3_uri_raw": HANDLER_DB_S3_URI,
        "s3_uri_parsed": parsed,
        "s3_parse_error": parse_error,
        "s3_region": HANDLER_DB_S3_REGION,
        "s3_auto_download": HANDLER_DB_S3_AUTO_DOWNLOAD,
        "s3_prefer": HANDLER_DB_S3_PREFER,
        "s3_force_download": HANDLER_DB_S3_FORCE_DOWNLOAD,
    }

# =========================================================
# Helpers
# =========================================================
def clean_name(first: str, last: str, fallback: str = "") -> str:
    def fix(x):
        x = (x or "").strip()
        return "" if x.lower() == "nan" else x
    f, l = fix(first), fix(last)
    name = " ".join([f, l]).strip()
    return name if name else (fallback or "(Ukjent)")


# =========================================================
# 1) Handler per eier
# =========================================================
def search_investors(conn: sqlite3.Connection, query: str, limit: int = 50) -> list[dict]:
    if len((query or "").strip()) < 4:
        return []
    q = query.upper().strip()
    like = f"%{q}%"
    sql = """
    SELECT investor_id, investor_type, first_name, last_name
    FROM investor
    WHERE UPPER(COALESCE(investor_id,'')) LIKE ?
       OR UPPER(COALESCE(first_name,'')) LIKE ?
       OR UPPER(COALESCE(last_name,'')) LIKE ?
       OR UPPER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE ?
    ORDER BY COALESCE(last_name,''), COALESCE(first_name,'')
    LIMIT ?
    """
    rows = conn.execute(sql, (like, like, like, like, limit)).fetchall()
    result = []
    for r in rows:
        first = clean_name(r["first_name"] or "", r["last_name"] or "", str(r["investor_id"]))
        result.append({
            "investor_id": str(r["investor_id"]).strip(),
            "label": f"{first} ({r['investor_id']})",
            "investor_type": r["investor_type"] or "",
        })
    return result


def fetch_handler_per_eier(conn, investor_id: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.isin, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS trade_price
        FROM position_change pc
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.investor_id=? AND pc.date_today BETWEEN ? AND ?
    )
    SELECT s.ticker, t.isin, COALESCE(s.isin_name,'') AS navn,
           COUNT(*) AS antall_obs,
           SUM(COALESCE(t.change_qty,0)) AS netto_antall,
           SUM(COALESCE(t.change_qty,0)*t.trade_price) AS netto_belop,
           SUM(ABS(COALESCE(t.change_qty,0)*t.trade_price)) AS brutto_belop
    FROM trades t JOIN security s ON s.isin=t.isin
    WHERE COALESCE(t.trade_price,0)>0
    GROUP BY s.ticker, t.isin, s.isin_name
    ORDER BY ABS(netto_belop) DESC
    """
    rows = conn.execute(sql, (investor_id, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
        df["brutto_mnok"] = df["brutto_belop"] / 1_000_000
    return df


def fetch_eier_transactions(conn, investor_id: str, isin: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    )
    SELECT pc.date_today AS dato, pc.change_qty AS antall,
           COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS kurs,
           (COALESCE(pc.change_qty,0)*COALESCE(NULLIF(pc.price_yesterday,0),p2.p)) AS belop
    FROM position_change pc
    LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
    WHERE pc.investor_id=? AND pc.isin=? AND pc.date_today BETWEEN ? AND ?
      AND COALESCE(NULLIF(pc.price_yesterday,0),p2.p)>0
    ORDER BY pc.date_today ASC
    """
    rows = conn.execute(sql, (investor_id, isin, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["belop_mnok"] = df["belop"] / 1_000_000
    return df


# =========================================================
# 2) Handler per aksje
# =========================================================
def search_securities(conn: sqlite3.Connection, query: str, limit: int = 50) -> list[dict]:
    q = (query or "").strip()
    if len(q) < 2:
        return []
    q_up = q.upper()
    like_pfx = f"{q_up}%"
    like_any = f"%{q_up}%"
    sql = """
    SELECT isin, COALESCE(ticker,'') AS ticker, COALESCE(isin_name,'') AS isin_name
    FROM security
    WHERE UPPER(COALESCE(ticker,'')) LIKE :pfx
       OR UPPER(COALESCE(isin_name,'')) LIKE :pfx
       OR UPPER(COALESCE(ticker,'')) LIKE :any
       OR UPPER(COALESCE(isin_name,'')) LIKE :any
    ORDER BY
        CASE WHEN UPPER(COALESCE(ticker,'')) LIKE :pfx THEN 0
             WHEN UPPER(COALESCE(isin_name,'')) LIKE :pfx THEN 1 ELSE 2 END,
        COALESCE(ticker,'') ASC
    LIMIT :lim
    """
    rows = conn.execute(sql, {"pfx": like_pfx, "any": like_any, "lim": limit}).fetchall()
    return [{"isin": r["isin"], "ticker": r["ticker"], "isin_name": r["isin_name"],
             "label": f"{r['ticker']} | {r['isin_name']} | {r['isin']}"} for r in rows]


def fetch_handler_per_aksje(conn, isin: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.investor_id, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS trade_price
        FROM position_change pc
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.isin=? AND pc.date_today BETWEEN ? AND ?
    )
    SELECT t.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COALESCE(i.investor_type,'') AS investor_type,
           COUNT(*) AS antall_obs,
           SUM(CASE WHEN COALESCE(t.change_qty,0)>0 THEN COALESCE(t.change_qty,0) ELSE 0 END) AS kjop_antall,
           SUM(CASE WHEN COALESCE(t.change_qty,0)>0 THEN COALESCE(t.change_qty,0)*t.trade_price ELSE 0 END) AS kjop_belop,
           SUM(CASE WHEN COALESCE(t.change_qty,0)<0 THEN ABS(COALESCE(t.change_qty,0)) ELSE 0 END) AS salg_antall,
           SUM(CASE WHEN COALESCE(t.change_qty,0)<0 THEN ABS(COALESCE(t.change_qty,0)*t.trade_price) ELSE 0 END) AS salg_belop,
           SUM(COALESCE(t.change_qty,0)*t.trade_price) AS netto_belop
    FROM trades t
    LEFT JOIN investor i ON i.investor_id=t.investor_id
    WHERE COALESCE(t.trade_price,0)>0
    GROUP BY t.investor_id, i.first_name, i.last_name, i.investor_type
    """
    rows = conn.execute(sql, (isin, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["eier"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id","")) for _, r in df.iterrows()]
        df["kjop_mnok"] = df["kjop_belop"].fillna(0) / 1_000_000
        df["salg_mnok"] = df["salg_belop"].fillna(0) / 1_000_000
        df["netto_mnok"] = df["netto_belop"].fillna(0) / 1_000_000
    return df


# =========================================================
# 3) Eier oversikt
# =========================================================
def fetch_eier_oversikt_per_security(conn, investor_id: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """Same as handler_per_eier but used by eier_oversikt tab."""
    return fetch_handler_per_eier(conn, investor_id, date_from, date_to)


def fetch_eier_oversikt_timeseries(conn, investor_id: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.date_today AS dato, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS trade_price
        FROM position_change pc
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.investor_id=? AND pc.date_today BETWEEN ? AND ?
    )
    SELECT dato, SUM(COALESCE(change_qty,0)*trade_price) AS netto_belop
    FROM trades WHERE COALESCE(trade_price,0)>0
    GROUP BY dato ORDER BY dato ASC
    """
    rows = conn.execute(sql, (investor_id, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
    return df


def fetch_aksje_oversikt_per_investor(conn, isin: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.investor_id, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS trade_price
        FROM position_change pc
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.isin=? AND pc.date_today BETWEEN ? AND ?
    )
    SELECT t.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COUNT(*) AS antall_obs,
           SUM(COALESCE(t.change_qty,0)) AS netto_antall,
           SUM(COALESCE(t.change_qty,0)*t.trade_price) AS netto_belop
    FROM trades t JOIN investor i ON i.investor_id=t.investor_id
    WHERE COALESCE(t.trade_price,0)>0
    GROUP BY t.investor_id, i.first_name, i.last_name
    ORDER BY ABS(netto_belop) DESC
    """
    rows = conn.execute(sql, (isin, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["navn"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id","")) for _, r in df.iterrows()]
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
    return df


# =========================================================
# 4) Handler de beste / viktige
# =========================================================
def read_csv_guess(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", encoding="latin-1", dtype=str).fillna("")


def extract_owner_patterns(list_name: str, df: pd.DataFrame) -> list[str]:
    if list_name.lower() == "beste":
        first_col = df.columns[0]
        patterns = df[first_col].astype(str).str.strip().tolist()
    else:
        eier_col = None
        for c in df.columns:
            if str(c).strip().lower() == "eier":
                eier_col = c
                break
        if eier_col is None:
            eier_col = df.columns[1] if df.shape[1] >= 2 else df.columns[0]
        patterns = df[eier_col].astype(str).str.strip().tolist()

    cleaned = []
    seen = set()
    for p in patterns:
        p2 = (p or "").strip()
        if not p2 or p2.lower() in {"selskap", "eier"}:
            continue
        if p2.lower() not in seen:
            seen.add(p2.lower())
            cleaned.append(p2)
    return cleaned


def resolve_investor_ids(conn, patterns: list[str], max_hits: int = 50) -> list[str]:
    sql = """
    SELECT investor_id FROM investor
    WHERE UPPER(COALESCE(investor_id,'')) LIKE :q
       OR UPPER(COALESCE(first_name,'')) LIKE :q
       OR UPPER(COALESCE(last_name,'')) LIKE :q
       OR UPPER(COALESCE(first_name,'')||' '||COALESCE(last_name,'')) LIKE :q
       OR UPPER(COALESCE(last_name,'')||' '||COALESCE(first_name,'')) LIKE :q
    LIMIT :lim
    """
    ids = set()
    for pat in patterns:
        rows = conn.execute(sql, {"q": f"%{pat.upper()}%", "lim": max_hits}).fetchall()
        for r in rows:
            ids.add(str(r["investor_id"]).strip())
    return sorted(ids)


def fetch_best_viktige_summary(conn, investor_ids: list[str], date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """Aggreger handler per aksje for en gruppe investorer."""
    if not investor_ids:
        return pd.DataFrame()

    conn.execute("DROP TABLE IF EXISTS temp_selected_investors")
    conn.execute("CREATE TEMP TABLE temp_selected_investors (investor_id TEXT PRIMARY KEY)")
    conn.executemany("INSERT OR IGNORE INTO temp_selected_investors(investor_id) VALUES (?)",
                     [(x,) for x in investor_ids])
    conn.commit()

    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change WHERE COALESCE(price_yesterday,0)>0
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.isin, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), p2.p) AS trade_price
        FROM position_change pc
        JOIN temp_selected_investors t ON t.investor_id=pc.investor_id
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.date_today BETWEEN ? AND ?
    )
    SELECT COALESCE(s.ticker,'') AS ticker, t.isin,
           COALESCE(s.isin_name,'') AS navn,
           COUNT(*) AS antall_obs,
           SUM(CASE WHEN COALESCE(t.change_qty,0)>0 THEN COALESCE(t.change_qty,0)*t.trade_price ELSE 0 END) AS kjop_belop,
           SUM(CASE WHEN COALESCE(t.change_qty,0)<0 THEN ABS(COALESCE(t.change_qty,0)*t.trade_price) ELSE 0 END) AS salg_belop,
           SUM(COALESCE(t.change_qty,0)*t.trade_price) AS netto_belop,
           SUM(ABS(COALESCE(t.change_qty,0)*t.trade_price)) AS brutto_belop
    FROM trades t JOIN security s ON s.isin=t.isin
    WHERE COALESCE(t.trade_price,0)>0
    GROUP BY s.ticker, t.isin, s.isin_name
    """
    rows = conn.execute(sql, (date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        for c in ["kjop_belop","salg_belop","netto_belop","brutto_belop"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
        df["salg_mnok"] = df["salg_belop"] / 1_000_000
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
        df["brutto_mnok"] = df["brutto_belop"] / 1_000_000
    return df


def _list_csv_files_s3() -> list[str]:
    if not HANDLER_LIST_S3_PREFIX:
        return []
    try:
        bucket, prefix = _parse_s3_bucket_prefix(HANDLER_LIST_S3_PREFIX)
        client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
        s3 = boto3.client("s3", **client_args)

        params = {"Bucket": bucket}
        if prefix:
            params["Prefix"] = prefix.rstrip("/") + "/"

        names: list[str] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                key = str(obj.get("Key", ""))
                if key.lower().endswith(".csv"):
                    names.append(Path(key).name)
        return sorted(set(names))
    except Exception as exc:
        _LOG.warning("Klarte ikke liste CSV fra S3 (%s): %s", HANDLER_LIST_S3_PREFIX, exc)
        return []


def _download_list_csv_from_s3(csv_filename: str) -> str | None:
    if not HANDLER_LIST_S3_PREFIX:
        return None

    clean_name = Path(csv_filename).name
    local_dir = Path(HANDLER_LIST_CACHE_DIR)
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / clean_name

    try:
        bucket, prefix = _parse_s3_bucket_prefix(HANDLER_LIST_S3_PREFIX)
        client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
        s3 = boto3.client("s3", **client_args)

        candidates = []
        pfx = prefix.rstrip("/")
        if pfx:
            candidates.append(f"{pfx}/{clean_name}")
        candidates.append(clean_name)

        for key in candidates:
            try:
                s3.download_file(bucket, key, str(local_path))
                _LOG.info("Lastet listefil fra S3: s3://%s/%s -> %s", bucket, key, local_path)
                return str(local_path)
            except ClientError as exc:
                err_code = exc.response.get("Error", {}).get("Code", "")
                if err_code in {"404", "NoSuchKey", "NotFound"}:
                    continue
                _LOG.warning("S3-feil ved nedlasting av listefil s3://%s/%s: %s", bucket, key, exc)
                return None

        _LOG.warning("Fant ikke listefil i S3 for %s (prefix=%s)", clean_name, HANDLER_LIST_S3_PREFIX)
        return None
    except Exception as exc:
        _LOG.warning("Klarte ikke laste listefil fra S3 (%s): %s", HANDLER_LIST_S3_PREFIX, exc)
        return None


def resolve_list_csv_path(csv_filename: str, list_dir: str | None = None) -> str | None:
    clean_name = Path(csv_filename).name
    d = list_dir or HANDLER_LIST_DIR
    local_path = os.path.join(d, clean_name)
    if os.path.isfile(local_path):
        return local_path

    return _download_list_csv_from_s3(clean_name)


def list_csv_files(list_dir: str | None = None) -> list[str]:
    files: list[str] = []
    d = list_dir or HANDLER_LIST_DIR
    if os.path.isdir(d):
        files.extend(f for f in os.listdir(d) if f.lower().endswith(".csv"))

    files.extend(_list_csv_files_s3())
    return sorted(set(files))
