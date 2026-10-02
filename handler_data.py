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
import re
import time
import io
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
HANDLER_TOP20_DB_PATH = _path_from_env(
    "HANDLER_TOP20_DB_PATH",
    _path_from_env(
        "HANDLER_LOCAL_WORKDIR",
        os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work"),
    )
    + os.sep
    + "top20_shareholders.db",
)
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
_INDEX_INIT_DONE: set[str] = set()
_INVESTOR_SEARCH_ROWS_CACHE: dict[str, list[tuple[str, str, str, str, str]]] = {}
_TOP20_DB_REFRESHED: set[str] = set()


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




def build_db_s3_target(filename: str = "topchanges.db") -> tuple[str, str, str]:
    """Resolve configured S3 target and return (bucket, key, s3_uri)."""
    if not HANDLER_DB_S3_URI:
        raise ValueError("HANDLER_DB_S3_URI er ikke satt")

    bucket, key = _parse_s3_uri(HANDLER_DB_S3_URI)
    final_key = key.strip().lstrip("/")
    if final_key.endswith("/"):
        safe_name = Path(filename).name or "topchanges.db"
        final_key = f"{final_key}{safe_name}".replace("//", "/")
    return bucket, final_key, f"s3://{bucket}/{final_key}"


def create_db_upload_presigned_url(filename: str = "topchanges.db", expires_seconds: int = 900) -> dict[str, str]:
    """Create presigned PUT URL for direct browser upload to S3."""
    bucket, final_key, s3_uri = build_db_s3_target(filename=filename)
    client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
    s3 = boto3.client("s3", **client_args)
    upload_url = s3.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": bucket,
            "Key": final_key,
            "ContentType": "application/octet-stream",
        },
        ExpiresIn=max(60, int(expires_seconds)),
    )
    return {
        "upload_url": upload_url,
        "s3_uri": s3_uri,
        "content_type": "application/octet-stream",
    }


def refresh_local_db_from_s3(local_path: str | None = None) -> bool:
    """Force refresh local DB from configured S3 object."""
    path = local_path or HANDLER_DB_PATH
    return _download_db_from_s3(path)


def upload_db_bytes_to_s3(db_bytes: bytes, filename: str = "topchanges.db") -> str:
    """Upload raw DB bytes to configured S3 URI and return final s3:// URI."""
    if not HANDLER_DB_S3_URI:
        raise ValueError("HANDLER_DB_S3_URI er ikke satt")

    bucket, final_key, s3_uri = build_db_s3_target(filename=filename)

    client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
    s3 = boto3.client("s3", **client_args)
    s3.put_object(
        Bucket=bucket,
        Key=final_key,
        Body=db_bytes,
        ContentType="application/octet-stream",
    )
    return s3_uri


def get_db_s3_object_last_modified(filename: str = "topchanges.db") -> str | None:
    if not HANDLER_DB_S3_URI:
        return None
    bucket, final_key, _ = build_db_s3_target(filename=filename)
    client_args = {"region_name": HANDLER_DB_S3_REGION} if HANDLER_DB_S3_REGION else {}
    s3 = boto3.client("s3", **client_args)
    try:
        head = s3.head_object(Bucket=bucket, Key=final_key)
    except ClientError:
        return None
    last_modified = head.get("LastModified")
    if not last_modified:
        return None
    return last_modified.isoformat()


def _clean_num(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _normalize_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None
    if re.fullmatch(r"\d+", s):
        n = int(s)
        if len(s) == 8:
            try:
                d = dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                return d.isoformat()
            except Exception:
                return None
        if len(s) == 6:
            try:
                d = dt.date(2000 + int(s[0:2]), int(s[2:4]), int(s[4:6]))
                return d.isoformat()
            except Exception:
                return None
        if 20000 <= n <= 80000:
            try:
                d = pd.to_datetime(n, unit="D", origin="1899-12-30").date()
                return d.isoformat()
            except Exception:
                return None
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isna(d):
            return None
        return d.date().isoformat()
    except Exception:
        return None


def _resolve_file_date_today(date_values: pd.Series, filename: str) -> pd.Series:
    """
    Fyller inn manglende DatoIdag for en fil.
    Prioritet:
      1) Mest brukte gyldige DatoIdag funnet i samme CSV.
      2) Dato tolket fra filnavn (YYYY-MM-DD / YYYY_MM_DD / YYYYMMDD / YYMMDD).
    """
    dates = date_values.map(_normalize_date)
    missing_mask = dates.isna()
    if not missing_mask.any():
        return dates

    non_missing = dates[~missing_mask]
    fallback_date = None
    if not non_missing.empty:
        # I filer med små avvik i rådata (f.eks. et par "feile" datoer)
        # vil vi bruke den datoen som forekommer oftest.
        fallback_date = non_missing.value_counts().index[0]

    if fallback_date is None:
        inferred = _infer_date_from_filename(filename)
        fallback_date = inferred.isoformat() if inferred else None

    if fallback_date is None:
        return dates

    dates.loc[missing_mask] = fallback_date
    return dates


def _pick_col(df: pd.DataFrame, *names: str) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _pick_col_ci(df: pd.DataFrame, *names: str) -> str | None:
    lookup = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        key = str(n).strip().lower()
        if key in lookup:
            return lookup[key]
    return None


def _ensure_upload_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
    CREATE TABLE IF NOT EXISTS ingested_files (
        filename TEXT PRIMARY KEY,
        mtime REAL NOT NULL,
        ingested_at TEXT NOT NULL
    );
    """
    )
    sec_cols = [r[1] for r in conn.execute("PRAGMA table_info(security)").fetchall()]
    if "last_price" not in sec_cols:
        conn.execute("ALTER TABLE security ADD COLUMN last_price REAL")

    pc_cols = [r[1] for r in conn.execute("PRAGMA table_info(position_change)").fetchall()]
    if "price_today" not in pc_cols:
        conn.execute("ALTER TABLE position_change ADD COLUMN price_today REAL")

    conn.executescript(
        """
    CREATE INDEX IF NOT EXISTS idx_pc_isin_date_today ON position_change(isin, date_today);
    CREATE INDEX IF NOT EXISTS idx_pc_isin_price_yest ON position_change(isin, price_yesterday);
    CREATE INDEX IF NOT EXISTS idx_pc_isin_price_today ON position_change(isin, price_today);
    CREATE INDEX IF NOT EXISTS idx_pc_date_isin ON position_change(date_today, isin);
    CREATE INDEX IF NOT EXISTS idx_pc_date_investor ON position_change(date_today, investor_id);
    """
    )
    conn.commit()


def get_max_date_today(conn: sqlite3.Connection) -> dt.date | None:
    row = conn.execute("SELECT MAX(date_today) FROM position_change").fetchone()
    if not row or not row[0]:
        return None
    try:
        return dt.date.fromisoformat(str(row[0]))
    except Exception:
        return None


def _refresh_security_last_price(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
    WITH candidates AS (
        SELECT
            isin,
            date_today,
            CASE
                WHEN COALESCE(price_today, 0) > 0 THEN price_today
                WHEN COALESCE(price_yesterday, 0) > 0 THEN price_yesterday
                ELSE NULL
            END AS eff_price
        FROM position_change
    ),
    last_dates AS (
        SELECT isin, MAX(date_today) AS last_date
        FROM candidates
        WHERE COALESCE(eff_price, 0) > 0
        GROUP BY isin
    ),
    last_prices AS (
        SELECT c.isin, MAX(c.eff_price) AS last_price
        FROM candidates c
        JOIN last_dates ld
          ON ld.isin = c.isin
         AND ld.last_date = c.date_today
        WHERE COALESCE(c.eff_price, 0) > 0
        GROUP BY c.isin
    )
    UPDATE security
    SET last_price = (
        SELECT lp.last_price
        FROM last_prices lp
        WHERE lp.isin = security.isin
    )
    WHERE isin IN (SELECT isin FROM last_prices)
    """
    )
    conn.commit()


def _ingest_one_csv_bytes(conn: sqlite3.Connection, filename: str, content: bytes) -> int:
    try:
        df = pd.read_csv(io.BytesIO(content), sep=None, engine="python", encoding="latin-1", dtype=str)
    except Exception:
        df = pd.read_csv(io.BytesIO(content), sep=";", encoding="latin-1", dtype=str, engine="python")

    col_investor_id = _pick_col_ci(df, "New_ID", "investor_ID", "Investor_ID", "InvestorID", "investorId")
    col_investor_type = _pick_col_ci(df, "Investortype", "InvestorType", "ktotype", "private")
    col_first = _pick_col_ci(df, "Fornavn", "FirstName", "First Name")
    col_last = _pick_col_ci(df, "Etternavn", "LastName", "Last Name")
    col_country = _pick_col_ci(df, "Country code", "Country_code", "CountryCode")
    col_raw_id = _pick_col_ci(df, "Date of Birth", "DOB", "Raw_ID")

    col_isin = _pick_col_ci(df, "ISIN", "isin")
    col_ticker = _pick_col_ci(df, "Ticker", "ticker")
    col_isin_name = _pick_col_ci(df, "ISINNAVN", "ISINNAVN ", "ISINName", "companyname", "name")
    col_paper_group = _pick_col(df, "PAPIRGRUPPE", "Papirgruppe")
    col_issuer_orgnr = _pick_col(df, "Orgnr", "Org.nr", "IssuerOrgnr")
    col_issuer_name = _pick_col(df, "Utsteder navn", "Utsteder_navn", "IssuerName")
    col_reg_country = _pick_col(df, "Registrert land", "Registered country")
    col_market = _pick_col(df, "Markedsplass", "Market")
    col_sector = _pick_col(df, "Sektor", "Sector")
    col_gics = _pick_col(df, "GICS_SECTOR", "GICS Sector")
    col_ask = _pick_col(df, "ASK-papir", "ASK_papir")
    col_issued = _pick_col_ci(df, "Utstedt antall", "Issued_shares", "sharesOut")

    col_date_today = _pick_col_ci(df, "DatoIdag", "Dato idag", "DateToday", "date")
    col_date_yest = _pick_col_ci(df, "DatoIgaar", "Dato igaar", "DateYesterday")
    col_h_today = _pick_col_ci(df, "Beh. idag", "Beh idag", "Holding today", "noOfStocks")
    col_h_yest = _pick_col_ci(df, "Beh. igaar", "Beh igaar", "Holding yesterday")
    col_price_today = _pick_col_ci(df, "Kurs idag", "Kurs idag ", "Price today")
    col_price_yest = _pick_col_ci(df, "Kurs igaar", "Kurs igaar ", "Price yesterday")
    col_change = _pick_col_ci(df, "Change", "ChangeQty")
    col_abs_change = _pick_col_ci(df, "AbsChange", "Abs change")
    col_change_pct = _pick_col_ci(df, "ChangePercent", "Change %", "percentage")
    col_flag_exit = _pick_col_ci(df, "Forlatt", "Exit")
    col_flag_new = _pick_col_ci(df, "Ny", "New")
    col_rank = _pick_col_ci(df, "Rank", "ranking")

    if col_isin is None or col_investor_id is None or col_date_today is None:
        raise ValueError(f"Mangler nødvendige kolonner i {filename}. Trenger minst ISIN, investor_id og DatoIdag.")

    inv = pd.DataFrame({
        "investor_id": df[col_investor_id].astype(str).str.strip(),
        "investor_type": df[col_investor_type].astype(str).str.strip() if col_investor_type else None,
        "first_name": df[col_first].astype(str).str.strip() if col_first else None,
        "last_name": df[col_last].astype(str).str.strip() if col_last else None,
        "country_code": df[col_country].astype(str).str.strip() if col_country else None,
        "raw_id": df[col_raw_id].astype(str).str.strip() if col_raw_id else None,
    }).dropna(subset=["investor_id"])
    inv["investor_id"] = inv["investor_id"].replace({"nan": None, "": None})
    inv = inv.dropna(subset=["investor_id"]).drop_duplicates(subset=["investor_id"])
    conn.executemany(
        """
        INSERT INTO investor(investor_id, investor_type, first_name, last_name, country_code, raw_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(investor_id) DO UPDATE SET
            investor_type=COALESCE(excluded.investor_type, investor.investor_type),
            first_name=COALESCE(excluded.first_name, investor.first_name),
            last_name=COALESCE(excluded.last_name, investor.last_name),
            country_code=COALESCE(excluded.country_code, investor.country_code),
            raw_id=COALESCE(excluded.raw_id, investor.raw_id)
        """,
        inv[["investor_id", "investor_type", "first_name", "last_name", "country_code", "raw_id"]].itertuples(index=False, name=None),
    )

    sec = pd.DataFrame({
        "isin": df[col_isin].astype(str).str.strip(),
        "ticker": df[col_ticker].astype(str).str.strip() if col_ticker else None,
        "isin_name": df[col_isin_name].astype(str).str.strip() if col_isin_name else None,
        "paper_group": df[col_paper_group].astype(str).str.strip() if col_paper_group else None,
        "issuer_orgnr": df[col_issuer_orgnr].astype(str).str.strip() if col_issuer_orgnr else None,
        "issuer_name": df[col_issuer_name].astype(str).str.strip() if col_issuer_name else None,
        "registered_country": df[col_reg_country].astype(str).str.strip() if col_reg_country else None,
        "market": df[col_market].astype(str).str.strip() if col_market else None,
        "sector": df[col_sector].astype(str).str.strip() if col_sector else None,
        "gics_sector": df[col_gics].astype(str).str.strip() if col_gics else None,
        "ask_paper": df[col_ask].astype(str).str.strip() if col_ask else None,
        "issued_shares": df[col_issued].map(_clean_num) if col_issued else None,
    }).dropna(subset=["isin"])
    sec["isin"] = sec["isin"].replace({"nan": None, "": None})
    sec = sec.dropna(subset=["isin"]).drop_duplicates(subset=["isin"])
    conn.executemany(
        """
        INSERT INTO security(isin, ticker, isin_name, paper_group, issuer_orgnr, issuer_name,
                             registered_country, market, sector, gics_sector, ask_paper, issued_shares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(isin) DO UPDATE SET
            ticker=COALESCE(excluded.ticker, security.ticker),
            isin_name=COALESCE(excluded.isin_name, security.isin_name),
            paper_group=COALESCE(excluded.paper_group, security.paper_group),
            issuer_orgnr=COALESCE(excluded.issuer_orgnr, security.issuer_orgnr),
            issuer_name=COALESCE(excluded.issuer_name, security.issuer_name),
            registered_country=COALESCE(excluded.registered_country, security.registered_country),
            market=COALESCE(excluded.market, security.market),
            sector=COALESCE(excluded.sector, security.sector),
            gics_sector=COALESCE(excluded.gics_sector, security.gics_sector),
            ask_paper=COALESCE(excluded.ask_paper, security.ask_paper),
            issued_shares=COALESCE(excluded.issued_shares, security.issued_shares)
        """,
        sec[["isin", "ticker", "isin_name", "paper_group", "issuer_orgnr", "issuer_name", "registered_country", "market", "sector", "gics_sector", "ask_paper", "issued_shares"]].itertuples(index=False, name=None),
    )

    date_today = _resolve_file_date_today(df[col_date_today], filename=filename)

    facts = pd.DataFrame({
        "isin": df[col_isin].astype(str).str.strip(),
        "investor_id": df[col_investor_id].astype(str).str.strip(),
        "date_today": date_today,
        "date_yesterday": df[col_date_yest].map(_normalize_date) if col_date_yest else None,
        "holding_today": df[col_h_today].map(_clean_num) if col_h_today else None,
        "holding_yesterday": df[col_h_yest].map(_clean_num) if col_h_yest else None,
        "price_today": df[col_price_today].map(_clean_num) if col_price_today else None,
        "price_yesterday": df[col_price_yest].map(_clean_num) if col_price_yest else None,
        "change_qty": df[col_change].map(_clean_num) if col_change else None,
        "abs_change_qty": df[col_abs_change].map(_clean_num) if col_abs_change else None,
        "change_percent": df[col_change_pct].map(_clean_num) if col_change_pct else None,
        "flag_new_source": df[col_flag_new].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_flag_new else None,
        "flag_exit_source": df[col_flag_exit].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_flag_exit else None,
        "rank": df[col_rank].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_rank else None,
        "source_file": filename,
    }).dropna(subset=["isin", "investor_id", "date_today"])
    facts = facts.drop_duplicates(subset=["isin", "investor_id", "date_today"])

    conn.executemany(
        """
        INSERT OR REPLACE INTO position_change(
            isin, investor_id, date_today, date_yesterday,
            holding_today, holding_yesterday, price_today, price_yesterday,
            change_qty, abs_change_qty, change_percent,
            flag_new_source, flag_exit_source, rank, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        facts[["isin", "investor_id", "date_today", "date_yesterday", "holding_today", "holding_yesterday", "price_today", "price_yesterday", "change_qty", "abs_change_qty", "change_percent", "flag_new_source", "flag_exit_source", "rank", "source_file"]].itertuples(index=False, name=None),
    )

    conn.execute(
        "INSERT OR REPLACE INTO ingested_files(filename, mtime, ingested_at) VALUES (?, ?, ?)",
        (filename, float(dt.datetime.now().timestamp()), dt.datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()
    return int(len(facts))


def refresh_top20_snapshot_db(source_db_path: str | None = None, target_db_path: str | None = None) -> str:
    src_path = source_db_path or HANDLER_DB_PATH
    dst_path = target_db_path or HANDLER_TOP20_DB_PATH
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row
    try:
        rows = src.execute(
            """
            WITH obs AS (
                SELECT
                    pc.isin,
                    pc.investor_id,
                    pc.date_today,
                    COALESCE(pc.holding_today, 0) AS holding_today,
                    COALESCE(pc.rank, 999999) AS ranking,
                    LAG(COALESCE(pc.holding_today, 0)) OVER (
                        PARTITION BY pc.isin, pc.investor_id
                        ORDER BY pc.date_today
                    ) AS prev_holding
                FROM position_change pc
                -- Viktig: ingen datofilter her.
                -- Vi bygger snapshot fra hele historikken i kildedatabasen,
                -- slik at "siste/forrige handel" finnes også når siste handel
                -- ligger langt tilbake i tid (f.eks. 2024-04-01).
            ),
            snap AS (
                SELECT isin, MAX(date_today) AS snapshot_date
                FROM obs
                GROUP BY isin
            ),
            current_pos AS (
                SELECT o.*
                FROM obs o
                JOIN snap s
                  ON s.isin = o.isin
                 AND s.snapshot_date = o.date_today
            ),
            change_events AS (
                SELECT
                    isin,
                    investor_id,
                    date_today,
                    COALESCE(holding_today, 0) - COALESCE(prev_holding, 0) AS change_qty,
                    COALESCE(prev_holding, 0) AS prev_holding,
                    COALESCE(holding_today, 0) AS holding_today,
                    CASE
                        WHEN COALESCE(prev_holding, 0) > 0
                         AND COALESCE(holding_today, 0) = 0 THEN 1
                        ELSE 0
                    END AS is_sellout,
                    ROW_NUMBER() OVER (
                        PARTITION BY isin, investor_id
                        ORDER BY date_today DESC
                    ) AS rn
                FROM obs
                WHERE prev_holding IS NOT NULL
                  AND COALESCE(holding_today, 0) <> COALESCE(prev_holding, 0)
            ),
            last_change AS (
                SELECT
                    isin,
                    investor_id,
                    MAX(CASE WHEN rn = 1 THEN date_today END) AS last_trade_date,
                    MAX(CASE WHEN rn = 2 THEN date_today END) AS previous_trade_date,
                    MAX(CASE WHEN rn = 1 THEN change_qty END) AS last_trade_change_qty,
                    MAX(CASE WHEN rn = 1 THEN prev_holding END) AS last_trade_prev_holding,
                    MAX(CASE WHEN rn = 1 THEN holding_today END) AS last_trade_holding_today,
                    MAX(CASE WHEN rn = 1 THEN is_sellout END) AS last_trade_is_sellout
                FROM change_events
                GROUP BY isin, investor_id
            ),
            ranked AS (
                SELECT
                    c.*,
                    s.snapshot_date,
                    lc.last_trade_date,
                    lc.previous_trade_date,
                    lc.last_trade_change_qty,
                    lc.last_trade_prev_holding,
                    lc.last_trade_holding_today,
                    lc.last_trade_is_sellout,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.isin
                        ORDER BY c.ranking ASC, c.holding_today DESC, c.investor_id ASC
                    ) AS rn
                FROM current_pos c
                JOIN snap s ON s.isin = c.isin
                LEFT JOIN last_change lc
                  ON lc.isin = c.isin
                 AND lc.investor_id = c.investor_id
            )
            SELECT
                r.snapshot_date,
                r.isin,
                r.investor_id,
                COALESCE(i.first_name, '') AS first_name,
                COALESCE(i.last_name, '') AS last_name,
                r.ranking,
                r.holding_today AS no_of_stocks,
                sec.issued_shares AS shares_out,
                sec.ticker,
                sec.isin_name AS company_name,
                r.last_trade_date,
                r.previous_trade_date,
                r.last_trade_change_qty,
                r.last_trade_prev_holding,
                r.last_trade_holding_today,
                r.last_trade_is_sellout,
                CAST((julianday(r.snapshot_date) - julianday(r.last_trade_date)) AS INTEGER) AS days_since_last_change,
                CAST((julianday(r.last_trade_date) - julianday(r.previous_trade_date)) AS INTEGER) AS days_between_trades
            FROM ranked r
            LEFT JOIN investor i ON i.investor_id = r.investor_id
            LEFT JOIN security sec ON sec.isin = r.isin
            WHERE r.rn <= 20
               OR (
                    COALESCE(r.last_trade_is_sellout, 0) = 1
                AND r.last_trade_date = r.snapshot_date
               )
            ORDER BY r.snapshot_date DESC, r.isin, r.ranking ASC, r.investor_id ASC
            """
        ).fetchall()
    finally:
        src.close()

    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    dst = sqlite3.connect(dst_path)
    try:
        dst.executescript(
            """
            DROP TABLE IF EXISTS top20_snapshot;
            CREATE TABLE top20_snapshot (
                snapshot_date TEXT NOT NULL,
                isin TEXT NOT NULL,
                investor_id TEXT NOT NULL,
                name TEXT,
                ranking INTEGER,
                no_of_stocks REAL,
                shares_out REAL,
                percentage REAL,
                ticker TEXT,
                company_name TEXT,
                last_change_date TEXT,
                days_since_last_change INTEGER,
                last_trade_date TEXT,
                previous_trade_date TEXT,
                days_between_trades INTEGER,
                last_trade_change_qty REAL,
                last_trade_share_pct REAL,
                exited_recently INTEGER,
                PRIMARY KEY (snapshot_date, isin, investor_id)
            );
            CREATE INDEX idx_top20_isin_snapshot ON top20_snapshot(isin, snapshot_date);
            """
        )
        payload = []
        for r in rows:
            first = str(r["first_name"] or "").strip()
            last = str(r["last_name"] or "").strip()
            investor_id = str(r["investor_id"] or "").strip()
            name = clean_name(first, last, investor_id)
            shares_out = _clean_num(r["shares_out"])
            no_of_stocks = _clean_num(r["no_of_stocks"])
            pct = ((no_of_stocks or 0.0) / shares_out * 100.0) if shares_out and shares_out > 0 else 0.0
            last_trade_change_qty = _clean_num(r["last_trade_change_qty"])
            last_trade_share_pct = ((last_trade_change_qty or 0.0) / shares_out * 10000.0) if shares_out and shares_out > 0 else 0.0
            exited_recently = int(r["last_trade_is_sellout"]) if r["last_trade_is_sellout"] is not None else 0
            payload.append(
                (
                    r["snapshot_date"],
                    r["isin"],
                    investor_id,
                    name,
                    int(r["ranking"]) if r["ranking"] is not None else None,
                    no_of_stocks,
                    shares_out,
                    pct,
                    r["ticker"],
                    r["company_name"],
                    r["last_trade_date"],
                    int(r["days_since_last_change"]) if r["days_since_last_change"] is not None else None,
                    r["last_trade_date"],
                    r["previous_trade_date"],
                    int(r["days_between_trades"]) if r["days_between_trades"] is not None else None,
                    last_trade_change_qty,
                    last_trade_share_pct,
                    exited_recently,
                )
            )
        dst.executemany(
            """
            INSERT INTO top20_snapshot(
                snapshot_date, isin, investor_id, name, ranking,
                no_of_stocks, shares_out, percentage, ticker, company_name,
                last_change_date, days_since_last_change, last_trade_date, previous_trade_date, days_between_trades,
                last_trade_change_qty, last_trade_share_pct, exited_recently
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        dst.commit()
    finally:
        dst.close()
    return dst_path


def _infer_date_from_filename(filename: str) -> dt.date | None:
    """
    Prøver å finne en dato i filnavnet (YYYY-MM-DD, YYYY_MM_DD, YYYYMMDD eller YYMMDD).
    Brukes for stabil ingest-rekkefølge ved historisk backfill (f.eks. månedsfiler fra 2022).
    """
    name = (filename or "").strip()
    if not name:
        return None
    stem = Path(name).stem
    for pattern in (r"(20\d{2})[-_](\d{2})[-_](\d{2})", r"(20\d{2})(\d{2})(\d{2})"):
        m = re.search(pattern, stem)
        if not m:
            continue
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return dt.date(y, mo, d)
        except Exception:
            continue
    m = re.search(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)", stem)
    if m:
        try:
            y, mo, d = 2000 + int(m.group(1)), int(m.group(2)), int(m.group(3))
            return dt.date(y, mo, d)
        except Exception:
            pass
    return None


def _sort_upload_files_for_backfill(files: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    """
    Sorterer filer kronologisk når dato kan leses fra filnavn.
    Filer uten dato legges til slutt i alfabetisk rekkefølge.
    """
    indexed: list[tuple[dt.date | None, str, bytes]] = []
    for filename, content in files:
        indexed.append((_infer_date_from_filename(filename), filename, content))
    indexed.sort(key=lambda x: (x[0] is None, x[0] or dt.date.max, x[1].lower()))
    return [(name, content) for _, name, content in indexed]


def apply_csv_updates_and_push_to_s3(files: list[tuple[str, bytes]]) -> dict:
    if not files:
        raise ValueError("Ingen CSV-filer mottatt.")
    if not HANDLER_DB_S3_URI:
        raise ValueError("HANDLER_DB_S3_URI er ikke satt")
    if not ensure_local_db(HANDLER_DB_PATH):
        raise FileNotFoundError(f"Fant ikke lokal DB på {HANDLER_DB_PATH}")

    conn = sqlite3.connect(HANDLER_DB_PATH, timeout=60)
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        _ensure_upload_schema(conn)
        before = get_max_date_today(conn)
        processed_rows = 0
        processed_files: list[str] = []

        ordered_files = _sort_upload_files_for_backfill(files)
        for filename, content in ordered_files:
            if not filename.lower().endswith(".csv"):
                continue
            processed_rows += _ingest_one_csv_bytes(conn, filename, content)
            processed_files.append(filename)

        _refresh_security_last_price(conn)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
        after = get_max_date_today(conn)
    finally:
        conn.close()

    if not processed_files:
        raise ValueError("Ingen gyldige .csv-filer å prosessere.")

    with open(HANDLER_DB_PATH, "rb") as f:
        payload = f.read()
    s3_uri = upload_db_bytes_to_s3(payload, filename=Path(HANDLER_DB_PATH).name or "topchanges.db")
    s3_last_modified = get_db_s3_object_last_modified(filename=Path(HANDLER_DB_PATH).name or "topchanges.db")
    top20_db_path = refresh_top20_snapshot_db(HANDLER_DB_PATH, HANDLER_TOP20_DB_PATH)
    return {
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "db_last_date_before": before.isoformat() if before else None,
        "db_last_date_after": after.isoformat() if after else None,
        "s3_uri": s3_uri,
        "s3_last_modified": s3_last_modified,
        "top20_db_path": top20_db_path,
    }


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
def _ensure_runtime_indexes(conn: sqlite3.Connection, db_key: str) -> None:
    if db_key in _INDEX_INIT_DONE:
        return
    try:
        conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_pc_inv_date_isin ON position_change(investor_id, date_today, isin);
        CREATE INDEX IF NOT EXISTS idx_pc_isin_date ON position_change(isin, date_today);
        CREATE INDEX IF NOT EXISTS idx_pc_isin_date_price ON position_change(isin, date_today, price_yesterday);
        """)
        conn.commit()
    except Exception:
        _LOG.warning("Klarte ikke opprette runtime-indekser for handler-db", exc_info=True)
    finally:
        _INDEX_INIT_DONE.add(db_key)


def db_connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or HANDLER_DB_PATH
    if not ensure_local_db(path):
        raise FileNotFoundError(f"Database ikke funnet på sti: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-200000")
    _ensure_runtime_indexes(conn, path)
    return conn


def _ensure_top20_snapshot_db() -> str:
    """
    Sørger for at top20 snapshot-db finnes og ikke er eldre enn kildedatabasen.
    Returnerer sti til top20-db.
    """
    src = Path(HANDLER_DB_PATH)
    dst = Path(HANDLER_TOP20_DB_PATH)
    refresh_key = f"{src.resolve()}->{dst.resolve()}"

    if refresh_key in _TOP20_DB_REFRESHED and dst.is_file():
        return str(dst)

    if not dst.is_file():
        refresh_top20_snapshot_db(str(src), str(dst))
        _TOP20_DB_REFRESHED.add(refresh_key)
        return str(dst)

    try:
        with sqlite3.connect(str(dst)) as chk:
            cols = {str(r[1]).strip().lower() for r in chk.execute("PRAGMA table_info(top20_snapshot)").fetchall()}
        required_cols = {
            "last_trade_date",
            "previous_trade_date",
            "days_between_trades",
            "last_trade_change_qty",
            "last_trade_share_pct",
            "exited_recently",
        }
        if not required_cols.issubset(cols):
            refresh_top20_snapshot_db(str(src), str(dst))
            _TOP20_DB_REFRESHED.add(refresh_key)
            return str(dst)
    except Exception:
        _LOG.warning("Klarte ikke verifisere top20_snapshot-skjema", exc_info=True)

    try:
        src_mtime = src.stat().st_mtime if src.is_file() else 0
        dst_mtime = dst.stat().st_mtime
        if src_mtime > dst_mtime:
            refresh_top20_snapshot_db(str(src), str(dst))
    except Exception:
        _LOG.warning("Klarte ikke verifisere/oppdatere top20 snapshot-db", exc_info=True)

    _TOP20_DB_REFRESHED.add(refresh_key)
    return str(dst)


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


def _postprocess_handler_per_eier_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for c in ("kjop_snitt_kurs", "salg_snitt_kurs", "siste_kurs"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    for c in ("kjop_antall", "salg_antall", "netto_antall", "kjop_belop", "salg_belop", "netto_belop", "brutto_belop"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce").fillna(0)

    har_kurs = df["siste_kurs"].fillna(0) > 0
    siste = df["siste_kurs"].fillna(0)
    ks = df["kjop_snitt_kurs"].fillna(0)
    ss = df["salg_snitt_kurs"].fillna(0)

    # Netto snittkurs vises ut fra nettoretning i perioden (kjøp ved netto > 0, salg ved netto < 0)
    df["netto_snitt_kurs"] = df["kjop_snitt_kurs"]
    df.loc[df["netto_antall"] < 0, "netto_snitt_kurs"] = df.loc[df["netto_antall"] < 0, "salg_snitt_kurs"]

    # Gevinst/tap mot siste kurs, splittet på kjøp og salg:
    #   kjøp: aksjene kjøpt i perioden verdsatt til siste kurs minus kostpris
    #   salg: salgssum minus hva aksjene ville vært verdt i dag (tapt/unngått kursutvikling)
    df["kjop_gevinst_belop"] = (df["kjop_antall"] * (siste - ks)).where(har_kurs, 0)
    df["salg_gevinst_belop"] = (df["salg_antall"] * (ss - siste)).where(har_kurs, 0)
    df["gevinst_belop"] = df["kjop_gevinst_belop"] + df["salg_gevinst_belop"]

    # Samme totalsum delt i realisert (matchet kjøp mot salg) og urealisert (netto posisjon)
    matchet = df[["kjop_antall", "salg_antall"]].min(axis=1)
    df["realisert_belop"] = matchet * (ss - ks)
    df.loc[matchet <= 0, "realisert_belop"] = 0
    df["urealisert_belop"] = (df["gevinst_belop"] - df["realisert_belop"]).where(har_kurs, 0)

    # Avkastning i % av omsatt beløp (kjøp: siste/kjøpskurs - 1, salg: 1 - siste/salgskurs)
    df["gevinst_pct"] = (df["gevinst_belop"] / df["brutto_belop"].where(df["brutto_belop"] > 0) * 100).where(har_kurs)

    df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
    df["salg_mnok"] = df["salg_belop"] / 1_000_000
    df["netto_mnok"] = df["netto_belop"] / 1_000_000
    df["brutto_mnok"] = df["brutto_belop"] / 1_000_000
    df["gevinst_mnok"] = df["gevinst_belop"] / 1_000_000
    df["kjop_gevinst_mnok"] = df["kjop_gevinst_belop"] / 1_000_000
    df["salg_gevinst_mnok"] = df["salg_gevinst_belop"] / 1_000_000
    df["realisert_mnok"] = df["realisert_belop"] / 1_000_000
    df["urealisert_mnok"] = df["urealisert_belop"] / 1_000_000
    for c in ("siste_kurs", "netto_snitt_kurs", "kjop_snitt_kurs", "salg_snitt_kurs"):
        df[c] = df[c].round(4)
    df["siste_kurs"] = df["siste_kurs"].fillna(0)
    # Behold eksisterende kolonnenavn for bakoverkompatibilitet
    df["u_realisert_belop"] = df["urealisert_belop"]
    return df


_PER_EIER_AGG = """
    SELECT COALESCE(s.ticker,'') AS ticker, t.isin, COALESCE(s.isin_name,'') AS navn,
           COUNT(*) AS antall_obs,
           SUM(t.q) AS netto_antall,
           SUM(CASE WHEN t.q>0 THEN t.q ELSE 0 END) AS kjop_antall,
           SUM(CASE WHEN t.q<0 THEN -t.q ELSE 0 END) AS salg_antall,
           SUM(CASE WHEN t.q>0 THEN t.q*t.trade_price ELSE 0 END) AS kjop_belop,
           SUM(CASE WHEN t.q<0 THEN -t.q*t.trade_price ELSE 0 END) AS salg_belop,
           SUM(t.q*t.trade_price) AS netto_belop,
           SUM(CASE WHEN t.q>0 THEN t.q*t.trade_price ELSE 0 END)
             / NULLIF(SUM(CASE WHEN t.q>0 THEN t.q ELSE 0 END),0) AS kjop_snitt_kurs,
           SUM(CASE WHEN t.q<0 THEN -t.q*t.trade_price ELSE 0 END)
             / NULLIF(SUM(CASE WHEN t.q<0 THEN -t.q ELSE 0 END),0) AS salg_snitt_kurs,
           SUM(ABS(t.q*t.trade_price)) AS brutto_belop,
           (
             SELECT lp.price_yesterday
             FROM position_change lp
             WHERE lp.isin=t.isin AND lp.price_yesterday>0
             ORDER BY lp.date_today DESC, lp.price_yesterday DESC
             LIMIT 1
           ) AS siste_kurs
    FROM trades t
    JOIN security s ON s.isin=t.isin
    WHERE COALESCE(t.trade_price,0)>0 AND t.q<>0
    GROUP BY t.isin
    ORDER BY ABS(netto_belop) DESC
"""


def fetch_handler_per_eier(conn, investor_id: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    # Henter først investorens egne rader (indeks investor_id, date_today), og slår bare opp
    # neste dags kurs for de radene som mangler kurs. Tidligere ble det bygget en pris-CTE
    # over hele position_change for hvert kall.
    sql = """
    WITH trades AS (
        SELECT pc.isin,
               COALESCE(pc.change_qty,0) AS q,
               COALESCE(
                   NULLIF(pc.price_yesterday,0),
                   NULLIF(pc.price_today,0),
                   (
                     SELECT MAX(p2.price_yesterday)
                     FROM position_change p2
                     WHERE p2.isin=pc.isin
                       AND p2.date_today >= date(pc.date_today,'+1 day')
                       AND p2.date_today <  date(pc.date_today,'+2 day')
                       AND p2.price_yesterday>0
                   )
               ) AS trade_price
        FROM position_change pc
        WHERE pc.investor_id=? AND pc.date_today BETWEEN ? AND ?
    )
    """ + _PER_EIER_AGG

    fallback_sql = """
    WITH trades AS (
        SELECT pc.isin, COALESCE(pc.change_qty,0) AS q, pc.price_yesterday AS trade_price
        FROM position_change pc
        WHERE pc.investor_id=? AND pc.date_today BETWEEN ? AND ?
    )
    """ + _PER_EIER_AGG

    params = (investor_id, date_from.isoformat(), date_to.isoformat())
    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.DatabaseError:
        _LOG.warning("Primær per-eier-query feilet, prøver fallback uten neste-dags-kurs", exc_info=True)
        rows = conn.execute(fallback_sql, params).fetchall()

    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    return _postprocess_handler_per_eier_df(df)


def fetch_eier_transactions(conn, investor_id: str, isin: str | None, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """Alle enkelthandler for en investor, for én aksje (isin) eller alle (isin=None)."""
    isin_filter = "AND pc.isin=?" if isin else ""
    sql = f"""
    WITH tx AS (
        SELECT pc.isin, pc.date_today AS dato, COALESCE(pc.change_qty,0) AS antall,
               COALESCE(
                   NULLIF(pc.price_yesterday,0),
                   NULLIF(pc.price_today,0),
                   (
                     SELECT MAX(p2.price_yesterday)
                     FROM position_change p2
                     WHERE p2.isin=pc.isin
                       AND p2.date_today >= date(pc.date_today,'+1 day')
                       AND p2.date_today <  date(pc.date_today,'+2 day')
                       AND p2.price_yesterday>0
                   )
               ) AS kurs
        FROM position_change pc
        WHERE pc.investor_id=? {isin_filter} AND pc.date_today BETWEEN ? AND ?
    )
    SELECT isin, dato, antall, kurs, antall*kurs AS belop
    FROM tx
    WHERE COALESCE(kurs,0)>0 AND antall<>0
    ORDER BY isin, dato ASC
    """
    params = [investor_id]
    if isin:
        params.append(isin)
    params += [date_from.isoformat(), date_to.isoformat()]
    rows = conn.execute(sql, params).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["belop_mnok"] = df["belop"] / 1_000_000
        df["kum_antall"] = df.groupby("isin")["antall"].cumsum()
    return df


def compute_trade_runs(tx: pd.DataFrame, max_gap_days: int = 7, last_data_date: dt.date | None = None) -> pd.DataFrame:
    """Grupperer enkelthandler i sammenhengende rekker med samme retning (kjøp/salg).

    En rekke brytes når retningen snur eller det går mer enn max_gap_days kalenderdager
    mellom to handler. Store investorer som begynner å selge gjør det gjerne over mange
    dager/uker, og en rekke som fortsatt pågår er det mest interessante signalet.

    Grupperer per isin, eller per (investor_id, isin) når tx har kolonnen investor_id.
    Har tx holding_yesterday/holding_today/flag_exit_source, tas beholdning med.
    """
    keys = ["investor_id", "isin"] if (tx is not None and "investor_id" in tx.columns) else ["isin"]
    cols = keys + ["rekke_nr", "retning", "start", "slutt", "handelsdager", "kalenderdager",
                   "antall", "belop_mnok", "snitt_kurs", "forste_kurs", "pagar"]
    if tx is None or tx.empty:
        return pd.DataFrame(columns=cols)

    extra = [c for c in ("holding_yesterday", "holding_today", "flag_exit_source") if c in tx.columns]
    d = tx[keys + ["dato", "antall", "kurs", "belop"] + extra].copy()
    d["dato"] = pd.to_datetime(d["dato"].astype(str).str[:10])
    d = d.sort_values(keys + ["dato"]).reset_index(drop=True)
    d["sign"] = (d["antall"] > 0).astype(int) * 2 - 1
    grp = d.groupby(keys, sort=False)
    prev_sign = grp["sign"].shift()
    gap = grp["dato"].diff().dt.days
    ny = prev_sign.isna() | (d["sign"] != prev_sign) | (gap > max_gap_days)
    d["rekke_nr"] = ny.astype(int).groupby([d[k] for k in keys]).cumsum().astype(int)

    aggs = dict(
        sign=("sign", "first"),
        start=("dato", "min"),
        slutt=("dato", "max"),
        handelsdager=("dato", "nunique"),
        antall=("antall", "sum"),
        belop=("belop", "sum"),
        forste_kurs=("kurs", "first"),
    )
    if "holding_yesterday" in extra:
        aggs["beholdning_start"] = ("holding_yesterday", "first")
    if "holding_today" in extra:
        aggs["beholdning_na"] = ("holding_today", "last")
    if "flag_exit_source" in extra:
        aggs["ut_av_topp20"] = ("flag_exit_source", "last")
    runs = d.groupby(keys + ["rekke_nr"], sort=False).agg(**aggs).reset_index()
    runs["retning"] = runs["sign"].map({1: "Kjøp", -1: "Salg"})
    runs["kalenderdager"] = (runs["slutt"] - runs["start"]).dt.days + 1
    runs["belop_mnok"] = runs["belop"] / 1_000_000
    runs["snitt_kurs"] = (runs["belop"] / runs["antall"]).where(runs["antall"] != 0)
    ref = pd.Timestamp(last_data_date) if last_data_date else d["dato"].max()
    runs["pagar"] = (ref - runs["slutt"]).dt.days <= max_gap_days
    runs["start"] = runs["start"].dt.date.astype(str)
    runs["slutt"] = runs["slutt"].dt.date.astype(str)
    out_cols = cols + [c for c in ("beholdning_start", "beholdning_na", "ut_av_topp20") if c in runs.columns]
    return runs[out_cols]


def summarize_latest_runs(runs: pd.DataFrame) -> pd.DataFrame:
    """Siste rekke per aksje, til bruk som kolonner i hovedtabellen."""
    if runs is None or runs.empty:
        return pd.DataFrame(columns=["isin", "rekke_retning", "rekke_dager", "rekke_start",
                                     "rekke_mnok", "rekke_pagar", "antall_rekker"])
    last = runs.sort_values(["isin", "rekke_nr"]).groupby("isin").tail(1)
    n = runs.groupby("isin").size().rename("antall_rekker")
    out = last.rename(columns={
        "retning": "rekke_retning", "handelsdager": "rekke_dager",
        "start": "rekke_start", "belop_mnok": "rekke_mnok", "pagar": "rekke_pagar",
    })[["isin", "rekke_retning", "rekke_dager", "rekke_start", "rekke_mnok", "rekke_pagar"]]
    return out.merge(n, on="isin", how="left")


def get_last_data_date(conn) -> dt.date | None:
    try:
        r = conn.execute("SELECT MAX(date_today) FROM position_change").fetchone()
        return _normalize_date(r[0]) if r and r[0] else None
    except sqlite3.DatabaseError:
        return None


_SCAN_CACHE: dict[tuple, tuple[float, pd.DataFrame]] = {}
_SCAN_CACHE_TTL_SECONDS = 15 * 60


def scan_trade_runs(conn, date_from: dt.date, date_to: dt.date, max_gap_days: int = 7) -> pd.DataFrame:
    """Handelsrekker for alle investorer i topp-20-listene i perioden.

    Én spørring over perioden, rekkene beregnes i pandas. Resultatet caches i 15 minutter
    per (periode, opphold, databasefil-endringstid).
    """
    try:
        db_mtime = os.path.getmtime(HANDLER_DB_PATH)
    except OSError:
        db_mtime = 0.0
    key = (date_from.isoformat(), date_to.isoformat(), int(max_gap_days), db_mtime)
    hit = _SCAN_CACHE.get(key)
    if hit and (time.time() - hit[0]) < _SCAN_CACHE_TTL_SECONDS:
        return hit[1].copy()

    pc_cols = {r[1] for r in conn.execute("PRAGMA table_info(position_change)").fetchall()}
    h_y = "pc.holding_yesterday" if "holding_yesterday" in pc_cols else "NULL"
    h_t = "pc.holding_today" if "holding_today" in pc_cols else "NULL"
    f_x = "pc.flag_exit_source" if "flag_exit_source" in pc_cols else "NULL"
    sql = f"""
    SELECT pc.investor_id, pc.isin, pc.date_today AS dato, pc.change_qty AS antall,
           COALESCE(
               NULLIF(pc.price_yesterday,0),
               NULLIF(pc.price_today,0),
               (
                 SELECT MAX(p2.price_yesterday)
                 FROM position_change p2
                 WHERE p2.isin=pc.isin
                   AND p2.date_today >= date(pc.date_today,'+1 day')
                   AND p2.date_today <  date(pc.date_today,'+2 day')
                   AND p2.price_yesterday>0
               )
           ) AS kurs,
           {h_y} AS holding_yesterday, {h_t} AS holding_today, {f_x} AS flag_exit_source
    FROM position_change pc
    WHERE pc.date_today BETWEEN ? AND ?
      AND COALESCE(pc.change_qty,0)<>0
    """
    tx = pd.read_sql_query(sql, conn, params=(date_from.isoformat(), date_to.isoformat()))
    if tx.empty:
        _SCAN_CACHE[key] = (time.time(), tx)
        return tx
    tx["kurs"] = pd.to_numeric(tx["kurs"], errors="coerce")
    tx = tx[tx["kurs"] > 0].copy()
    tx["antall"] = pd.to_numeric(tx["antall"], errors="coerce").fillna(0)
    for c in ("holding_yesterday", "holding_today", "flag_exit_source"):
        tx[c] = pd.to_numeric(tx[c], errors="coerce")
    tx["belop"] = tx["antall"] * tx["kurs"]
    tx["investor_id"] = tx["investor_id"].astype(str).str.strip()

    runs = compute_trade_runs(tx, max_gap_days=max_gap_days, last_data_date=get_last_data_date(conn))
    if runs.empty:
        _SCAN_CACHE[key] = (time.time(), runs)
        return runs

    # Navn på aksje og investor, siste kurs per aksje
    isins = runs["isin"].unique().tolist()
    sec = pd.read_sql_query(
        "SELECT isin, COALESCE(ticker,'') AS ticker, COALESCE(isin_name,'') AS navn FROM security", conn
    )
    runs = runs.merge(sec, on="isin", how="left")
    inv_ids = runs["investor_id"].unique().tolist()
    inv_rows = []
    for i in range(0, len(inv_ids), 900):
        chunk = inv_ids[i:i + 900]
        ph = ",".join("?" * len(chunk))
        inv_rows += conn.execute(
            f"SELECT investor_id, investor_type, first_name, last_name FROM investor WHERE TRIM(investor_id) IN ({ph})",
            chunk,
        ).fetchall()
    inv = pd.DataFrame(
        [{
            "investor_id": str(r["investor_id"]).strip(),
            "investor": clean_name(r["first_name"] or "", r["last_name"] or "", str(r["investor_id"])),
            "investor_type": r["investor_type"] or "",
        } for r in inv_rows],
        columns=["investor_id", "investor", "investor_type"],
    ).drop_duplicates("investor_id")
    runs = runs.merge(inv, on="investor_id", how="left")
    runs["investor"] = runs["investor"].fillna(runs["investor_id"])

    last_px = {}
    for isin in isins:
        r = conn.execute(
            "SELECT price_yesterday FROM position_change WHERE isin=? AND price_yesterday>0 "
            "ORDER BY date_today DESC, price_yesterday DESC LIMIT 1",
            (isin,),
        ).fetchone()
        last_px[isin] = float(r[0]) if r else None
    runs["siste_kurs"] = runs["isin"].map(last_px)

    # Kursutvikling siden rekken startet og andel av beholdning som er omsatt i rekken
    runs["kurs_endring_pct"] = (runs["siste_kurs"] / runs["forste_kurs"] - 1) * 100
    if "beholdning_start" in runs.columns:
        bs = runs["beholdning_start"].where(runs["beholdning_start"] > 0)
        runs["andel_av_beholdning_pct"] = runs["antall"].abs() / bs * 100
    runs["ut_av_topp20"] = runs.get("ut_av_topp20", pd.Series(0, index=runs.index)).fillna(0).astype(int) == 1

    _SCAN_CACHE.clear()
    _SCAN_CACHE[key] = (time.time(), runs)
    return runs.copy()


def summarize_runs_per_security(runs: pd.DataFrame) -> pd.DataFrame:
    """Salgspress per aksje: pågående rekker fordelt på kjøp og salg."""
    if runs is None or runs.empty:
        return pd.DataFrame()
    r = runs.copy()
    r["salg"] = (r["retning"] == "Salg").astype(int)
    r["kjop"] = (r["retning"] == "Kjøp").astype(int)
    r["salg_mnok"] = r["belop_mnok"].where(r["retning"] == "Salg", 0)
    r["kjop_mnok"] = r["belop_mnok"].where(r["retning"] == "Kjøp", 0)
    g = r.groupby(["isin", "ticker", "navn"], dropna=False).agg(
        eiere_salg=("salg", "sum"),
        eiere_kjop=("kjop", "sum"),
        salg_mnok=("salg_mnok", "sum"),
        kjop_mnok=("kjop_mnok", "sum"),
        netto_mnok=("belop_mnok", "sum"),
        lengste_rekke=("handelsdager", "max"),
        siste_kurs=("siste_kurs", "first"),
    ).reset_index()
    g["netto_eiere"] = g["eiere_kjop"] - g["eiere_salg"]
    return g.sort_values("netto_mnok")


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
               COALESCE(NULLIF(pc.price_yesterday,0), NULLIF(pc.price_today,0), p2.p) AS trade_price
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
               COALESCE(NULLIF(pc.price_yesterday,0), NULLIF(pc.price_today,0), p2.p) AS trade_price
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
    WITH latest_date AS (
        SELECT MAX(date_today) AS d
        FROM position_change
        WHERE isin=? AND date_today<=?
    ),
    current_pos AS (
        SELECT
            pc.investor_id,
            MAX(COALESCE(pc.holding_today, 0)) AS holding_today,
            MAX(COALESCE(pc.rank, 999999)) AS ranking
        FROM position_change pc
        JOIN latest_date ld ON ld.d = pc.date_today
        WHERE pc.isin=?
        GROUP BY pc.investor_id
    ),
    baseline_date AS (
        SELECT
            investor_id,
            MAX(date_today) AS d
        FROM position_change
        WHERE isin=? AND date_today<=?
        GROUP BY investor_id
    ),
    baseline_pos AS (
        SELECT
            pc.investor_id,
            COALESCE(pc.holding_today, 0) AS holding_baseline
        FROM position_change pc
        JOIN baseline_date bd
          ON bd.investor_id=pc.investor_id
         AND bd.d=pc.date_today
        WHERE pc.isin=?
    ),
    obs AS (
        SELECT investor_id, COUNT(*) AS antall_obs
        FROM position_change
        WHERE isin=? AND date_today BETWEEN ? AND ?
        GROUP BY investor_id
    )
    SELECT
        c.investor_id,
        COALESCE(i.first_name,'') AS first_name,
        COALESCE(i.last_name,'') AS last_name,
        COALESCE(o.antall_obs, 0) AS antall_obs,
        c.holding_today AS no_of_stocks,
        (c.holding_today - COALESCE(b.holding_baseline, 0)) AS endring_antall,
        c.ranking AS ranking,
        s.issued_shares AS shares_out,
        ld.d AS latest_date
    FROM current_pos c
    LEFT JOIN baseline_pos b ON b.investor_id=c.investor_id
    LEFT JOIN obs o ON o.investor_id=c.investor_id
    LEFT JOIN investor i ON i.investor_id=c.investor_id
    LEFT JOIN security s ON s.isin=?
    LEFT JOIN latest_date ld
    ORDER BY c.holding_today DESC, c.ranking ASC, c.investor_id ASC
    """
    rows = conn.execute(
        sql,
        (
            isin,
            date_to.isoformat(),
            isin,
            isin,
            date_from.isoformat(),
            isin,
            isin,
            date_from.isoformat(),
            date_to.isoformat(),
            isin,
        ),
    ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["navn"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id","")) for _, r in df.iterrows()]
        shares_out = pd.to_numeric(df["shares_out"], errors="coerce")
        no_of_stocks = pd.to_numeric(df["no_of_stocks"], errors="coerce").fillna(0)
        df["percentage"] = ((no_of_stocks / shares_out) * 100).where(shares_out > 0)
        df["percentage"] = df["percentage"].fillna(0.0)
    return df


def fetch_top_shareholders_with_last_change(
    conn: sqlite3.Connection, isin: str, as_of_date: dt.date, limit: int = 20
) -> pd.DataFrame:
    safe_limit = max(1, min(int(limit), 50))
    sql = """
    WITH obs AS (
        SELECT
            pc.isin,
            pc.investor_id,
            pc.date_today,
            COALESCE(pc.holding_today, 0) AS holding_today,
            COALESCE(pc.rank, 999999) AS ranking,
            LAG(COALESCE(pc.holding_today, 0)) OVER (
                PARTITION BY pc.isin, pc.investor_id
                ORDER BY pc.date_today
            ) AS prev_holding
        FROM position_change pc
        -- Live-endepunktet respekterer "as_of", men søker fortsatt i hele
        -- historikken fram til den datoen (ikke bare "nå").
        WHERE pc.isin = ? AND pc.date_today <= ?
    ),
    snap AS (
        SELECT MAX(date_today) AS d FROM obs
    ),
    current_pos AS (
        SELECT o.*
        FROM obs o
        JOIN snap s ON s.d = o.date_today
    ),
    change_events AS (
        SELECT
            investor_id,
            date_today,
            ROW_NUMBER() OVER (
                PARTITION BY investor_id
                ORDER BY date_today DESC
            ) AS rn
        FROM obs
        WHERE prev_holding IS NOT NULL
          AND COALESCE(holding_today, 0) <> COALESCE(prev_holding, 0)
    ),
    last_change AS (
        SELECT
            investor_id,
            MAX(CASE WHEN rn = 1 THEN date_today END) AS last_trade_date,
            MAX(CASE WHEN rn = 2 THEN date_today END) AS previous_trade_date
        FROM change_events
        GROUP BY investor_id
    )
    SELECT
        c.investor_id,
        COALESCE(i.first_name, '') AS first_name,
        COALESCE(i.last_name, '') AS last_name,
        c.holding_today AS no_of_stocks,
        c.ranking,
        s.ticker,
        s.isin_name AS company_name,
        s.issued_shares AS shares_out,
        snap.d AS snapshot_date,
        lc.last_trade_date,
        lc.previous_trade_date
    FROM current_pos c
    LEFT JOIN investor i ON i.investor_id = c.investor_id
    LEFT JOIN security s ON s.isin = c.isin
    LEFT JOIN last_change lc ON lc.investor_id = c.investor_id
    LEFT JOIN snap
    ORDER BY c.ranking ASC, c.holding_today DESC, c.investor_id ASC
    LIMIT ?
    """
    rows = conn.execute(sql, (isin, as_of_date.isoformat(), safe_limit)).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if df.empty:
        return df

    df["name"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id", "")) for _, r in df.iterrows()]
    shares_out = pd.to_numeric(df["shares_out"], errors="coerce")
    no_of_stocks = pd.to_numeric(df["no_of_stocks"], errors="coerce").fillna(0)
    df["percentage"] = ((no_of_stocks / shares_out) * 100).where(shares_out > 0).fillna(0.0)

    snap_dt = pd.to_datetime(df["snapshot_date"], errors="coerce")
    last_trade_dt = pd.to_datetime(df["last_trade_date"], errors="coerce")
    prev_trade_dt = pd.to_datetime(df["previous_trade_date"], errors="coerce")
    df["last_change_date"] = df["last_trade_date"]
    df["days_since_last_change"] = (snap_dt - last_trade_dt).dt.days
    df["days_between_trades"] = (last_trade_dt - prev_trade_dt).dt.days
    return df


def fetch_top_shareholders_snapshot(
    isin: str, as_of_date: dt.date, limit: int = 20
) -> pd.DataFrame:
    safe_limit = max(1, min(int(limit), 50))
    top20_db_path = _ensure_top20_snapshot_db()
    conn = sqlite3.connect(top20_db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                name,
                investor_id,
                ranking,
                no_of_stocks,
                percentage,
                last_change_date,
                days_since_last_change,
                last_trade_date,
                previous_trade_date,
                days_between_trades,
                last_trade_change_qty,
                last_trade_share_pct,
                exited_recently,
                snapshot_date,
                ticker,
                company_name
            FROM top20_snapshot
            WHERE isin = ?
              AND snapshot_date = (
                  SELECT MAX(snapshot_date)
                  FROM top20_snapshot
                  WHERE isin = ? AND snapshot_date <= ?
              )
            ORDER BY ranking ASC, no_of_stocks DESC, investor_id ASC
            LIMIT ?
            """,
            (isin, isin, as_of_date.isoformat(), safe_limit),
        ).fetchall()
    finally:
        conn.close()

    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def scan_top_shareholder_first_trades(
    as_of_date: dt.date,
    since_date: dt.date,
    min_idle_days: int = 20,
    top_n: int = 20,
    isins: list[str] | None = None,
    tickers: list[str] | None = None,
    direction: str = "both",
) -> pd.DataFrame:
    """
    Finn aksjer der en toppaksjonær (topp-N på snapshot) har gjort en handel etter `since_date`,
    og hvor dette er første handel på minst `min_idle_days`.

    Parametre:
      - as_of_date: snapshot-dato (bruker siste snapshot <= denne datoen per aksje)
      - since_date: handler må være >= denne datoen
      - min_idle_days: minimum dager mellom siste og forrige handel
      - top_n: hvilke rangeringer som regnes som "store eiere" (1..50)
      - isins/tickers: valgfri filtrering til valgt utvalg selskaper
      - direction: "buy", "sell" eller "both"
    """
    safe_top_n = max(1, min(int(top_n), 50))
    safe_idle_days = max(0, int(min_idle_days))
    normalized_direction = (direction or "both").strip().lower()
    if normalized_direction not in {"buy", "sell", "both"}:
        normalized_direction = "both"

    clean_isins = sorted({str(x).strip().upper() for x in (isins or []) if str(x).strip()})
    clean_tickers = sorted({str(x).strip().upper() for x in (tickers or []) if str(x).strip()})

    filters: list[str] = []
    bind_values: list = [as_of_date.isoformat(), safe_top_n, since_date.isoformat(), safe_idle_days]

    if clean_isins:
        placeholders = ",".join("?" for _ in clean_isins)
        filters.append(f"UPPER(base.isin) IN ({placeholders})")
        bind_values.extend(clean_isins)

    if clean_tickers:
        placeholders = ",".join("?" for _ in clean_tickers)
        filters.append(f"UPPER(COALESCE(base.ticker,'')) IN ({placeholders})")
        bind_values.extend(clean_tickers)

    if normalized_direction == "buy":
        filters.append("COALESCE(base.last_trade_change_qty, 0) > 0")
    elif normalized_direction == "sell":
        filters.append("COALESCE(base.last_trade_change_qty, 0) < 0")

    where_tail = f" AND {' AND '.join(filters)}" if filters else ""

    top20_db_path = _ensure_top20_snapshot_db()
    conn = sqlite3.connect(top20_db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            WITH latest_snapshot AS (
                SELECT isin, MAX(snapshot_date) AS snapshot_date
                FROM top20_snapshot
                WHERE snapshot_date <= ?
                GROUP BY isin
            ),
            base AS (
                SELECT t.*
                FROM top20_snapshot t
                JOIN latest_snapshot ls
                  ON ls.isin = t.isin
                 AND ls.snapshot_date = t.snapshot_date
                WHERE COALESCE(t.ranking, 999999) <= ?
            )
            SELECT
                base.snapshot_date,
                base.isin,
                COALESCE(base.ticker, '') AS ticker,
                COALESCE(base.company_name, '') AS company_name,
                base.investor_id,
                COALESCE(base.name, '') AS name,
                base.ranking,
                base.no_of_stocks,
                base.percentage,
                base.last_trade_date,
                base.previous_trade_date,
                base.days_between_trades,
                base.last_trade_change_qty,
                base.last_trade_share_pct,
                CASE
                    WHEN COALESCE(base.last_trade_change_qty, 0) > 0 THEN 'buy'
                    WHEN COALESCE(base.last_trade_change_qty, 0) < 0 THEN 'sell'
                    ELSE 'flat'
                END AS trade_direction
            FROM base
            WHERE base.last_trade_date IS NOT NULL
              AND base.last_trade_date >= ?
              AND (
                    base.previous_trade_date IS NULL
                    OR COALESCE(base.days_between_trades, 999999) >= ?
                  )
              {where_tail}
            ORDER BY base.last_trade_date DESC,
                     COALESCE(base.ticker, ''),
                     COALESCE(base.ranking, 999999),
                     base.investor_id
            """
            ,
            tuple(bind_values),
        ).fetchall()
    finally:
        conn.close()

    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


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
    if not patterns:
        return []

    clean_patterns = []
    seen_patterns = set()
    for pattern in patterns:
        p = str(pattern or "").strip().upper()
        if not p or p in seen_patterns:
            continue
        seen_patterns.add(p)
        clean_patterns.append(p)
    if not clean_patterns:
        return []

    db_key_row = conn.execute("PRAGMA database_list").fetchone()
    db_key = str(db_key_row[2]) if db_key_row and len(db_key_row) >= 3 else "__memory__"

    rows = _INVESTOR_SEARCH_ROWS_CACHE.get(db_key)
    if rows is None:
        raw_rows = conn.execute(
            """
            SELECT
                COALESCE(investor_id,'') AS investor_id,
                UPPER(COALESCE(investor_id,'')) AS investor_id_u,
                UPPER(COALESCE(first_name,'')) AS first_name_u,
                UPPER(COALESCE(last_name,'')) AS last_name_u
            FROM investor
            """
        ).fetchall()
        rows = []
        for r in raw_rows:
            investor_id = str(r["investor_id"] or "").strip()
            if not investor_id:
                continue
            fn = str(r["first_name_u"] or "")
            ln = str(r["last_name_u"] or "")
            rows.append((
                investor_id,
                str(r["investor_id_u"] or ""),
                fn,
                ln,
                f"{fn} {ln}".strip(),
            ))
        _INVESTOR_SEARCH_ROWS_CACHE[db_key] = rows

    pattern_re = re.compile("|".join(re.escape(p) for p in clean_patterns))
    max_total_hits = max(int(max_hits), 1) * len(clean_patterns)

    investor_ids: list[str] = []
    seen_ids: set[str] = set()
    for investor_id, iid_u, fn_u, ln_u, full_u in rows:
        if (
            pattern_re.search(iid_u)
            or pattern_re.search(fn_u)
            or pattern_re.search(ln_u)
            or pattern_re.search(full_u)
        ):
            if investor_id not in seen_ids:
                seen_ids.add(investor_id)
                investor_ids.append(investor_id)
                if len(investor_ids) >= max_total_hits:
                    break
    return sorted(investor_ids)


def populate_temp_selected_investors(conn, investor_ids: list[str]) -> None:
    conn.execute("DROP TABLE IF EXISTS temp_selected_investors")
    conn.execute("CREATE TEMP TABLE temp_selected_investors (investor_id TEXT PRIMARY KEY)")
    conn.executemany(
        "INSERT OR IGNORE INTO temp_selected_investors(investor_id) VALUES (?)",
        [(x,) for x in investor_ids],
    )
    conn.commit()


def fetch_best_viktige_summary(conn, investor_ids: list[str], date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """Aggreger handler per aksje for en gruppe investorer."""
    if not investor_ids:
        return pd.DataFrame()

    populate_temp_selected_investors(conn, investor_ids)

    sql = """
    WITH selected_trades AS (
        SELECT pc.isin,
               pc.change_qty,
               pc.price_yesterday,
               date(pc.date_today,'+1 day') AS price_d
        FROM position_change pc
        JOIN temp_selected_investors t ON t.investor_id=pc.investor_id
        WHERE pc.date_today BETWEEN ? AND ?
    ),
    needed_prices AS (
        SELECT DISTINCT isin, price_d AS d
        FROM selected_trades
        WHERE COALESCE(price_yesterday,0) <= 0
    ),
    prices AS (
        SELECT p.isin, date(p.date_today) AS d, MAX(p.price_yesterday) AS p
        FROM position_change p
        JOIN needed_prices n ON n.isin=p.isin AND n.d=date(p.date_today)
        WHERE COALESCE(p.price_yesterday,0)>0
        GROUP BY p.isin, date(p.date_today)
    ),
    trades AS (
        SELECT st.isin,
               st.change_qty,
               COALESCE(NULLIF(st.price_yesterday,0), NULLIF(st.price_today,0), p2.p) AS trade_price
        FROM selected_trades st
        LEFT JOIN prices p2 ON p2.isin=st.isin AND p2.d=st.price_d
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
    bind_values = (
        date_from.isoformat(),
        date_to.isoformat(),
        (date_to + dt.timedelta(days=1)).isoformat(),
        date_from.isoformat(),
        date_to.isoformat(),
    )
    rows = conn.execute(sql, bind_values[:sql.count("?")]).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        for c in ["kjop_belop","salg_belop","netto_belop","brutto_belop"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
        df["salg_mnok"] = df["salg_belop"] / 1_000_000
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
        df["brutto_mnok"] = df["brutto_belop"] / 1_000_000
    return df


def fetch_best_viktige_trades(conn, investor_ids: list[str], date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """Alle handler på transaksjonsnivå for investorer i valgt liste og periode."""
    if not investor_ids:
        return pd.DataFrame()

    populate_temp_selected_investors(conn, investor_ids)

    sql = """
    WITH prices AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change
        WHERE COALESCE(price_yesterday,0)>0
          AND date_today BETWEEN ? AND ?
        GROUP BY isin, date(date_today)
    ),
    trades AS (
        SELECT pc.date_today AS dato,
               pc.isin,
               pc.investor_id,
               pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), NULLIF(pc.price_today,0), p2.p) AS trade_price
        FROM position_change pc
        JOIN temp_selected_investors t ON t.investor_id=pc.investor_id
        LEFT JOIN prices p2 ON p2.isin=pc.isin AND p2.d=date(pc.date_today,'+1 day')
        WHERE pc.date_today BETWEEN ? AND ?
    )
    SELECT t.dato,
           COALESCE(s.ticker,'') AS ticker,
           t.isin,
           COALESCE(s.isin_name,'') AS navn,
           t.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COALESCE(i.investor_type,'') AS investor_type,
           COALESCE(t.change_qty,0) AS antall,
           t.trade_price AS kurs,
           (COALESCE(t.change_qty,0)*t.trade_price) AS belop
    FROM trades t
    JOIN security s ON s.isin=t.isin
    LEFT JOIN investor i ON i.investor_id=t.investor_id
    WHERE COALESCE(t.trade_price,0)>0
    ORDER BY t.dato DESC
    """
    date_to_plus_1 = (date_to + dt.timedelta(days=1)).isoformat()
    bind_values = (
        date_from.isoformat(),
        date_to_plus_1,
        date_from.isoformat(),
        date_to.isoformat(),
    )
    rows = conn.execute(sql, bind_values[:sql.count("?")]).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["eier"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id", "")) for _, r in df.iterrows()]
        df["belop_mnok"] = df["belop"].fillna(0) / 1_000_000
    return df


def fetch_best_viktige_trades_for_isin(
    conn,
    investor_ids: list[str],
    isin: str,
    date_from: dt.date,
    date_to: dt.date,
) -> pd.DataFrame:
    """Samlet per investor for valgt aksje (raskere enn transaksjonsliste)."""
    if not investor_ids or not isin:
        return pd.DataFrame()

    populate_temp_selected_investors(conn, investor_ids)

    sql = """
    WITH selected_trades AS (
        SELECT pc.investor_id,
               pc.change_qty,
               pc.price_yesterday,
               date(pc.date_today,'+1 day') AS price_d
        FROM position_change pc
        JOIN temp_selected_investors t ON t.investor_id=pc.investor_id
        WHERE pc.date_today BETWEEN ? AND ?
          AND pc.isin = ?
    ),
    needed_prices AS (
        SELECT DISTINCT price_d AS d
        FROM selected_trades
        WHERE COALESCE(price_yesterday,0) <= 0
    ),
    prices AS (
        SELECT date(p.date_today) AS d, MAX(p.price_yesterday) AS p
        FROM position_change p
        JOIN needed_prices n ON n.d=date(p.date_today)
        WHERE p.isin = ?
          AND COALESCE(p.price_yesterday,0)>0
        GROUP BY date(p.date_today)
    ),
    trades AS (
        SELECT st.investor_id,
               st.change_qty,
               COALESCE(NULLIF(st.price_yesterday,0), NULLIF(st.price_today,0), p2.p) AS trade_price
        FROM selected_trades st
        LEFT JOIN prices p2 ON p2.d=st.price_d
    )
    SELECT tr.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COALESCE(i.investor_type,'') AS investor_type,
           COUNT(*) AS antall_obs,
           SUM(CASE WHEN COALESCE(tr.change_qty,0) > 0 THEN COALESCE(tr.change_qty,0) ELSE 0 END) AS kjop_antall,
           SUM(CASE WHEN COALESCE(tr.change_qty,0) > 0 THEN COALESCE(tr.change_qty,0) * tr.trade_price ELSE 0 END) AS kjop_belop,
           SUM(CASE WHEN COALESCE(tr.change_qty,0) < 0 THEN ABS(COALESCE(tr.change_qty,0)) ELSE 0 END) AS salg_antall,
           SUM(CASE WHEN COALESCE(tr.change_qty,0) < 0 THEN ABS(COALESCE(tr.change_qty,0) * tr.trade_price) ELSE 0 END) AS salg_belop,
           SUM(COALESCE(tr.change_qty,0) * tr.trade_price) AS netto_belop
    FROM trades tr
    LEFT JOIN investor i ON i.investor_id = tr.investor_id
    WHERE COALESCE(tr.trade_price,0) > 0
    GROUP BY tr.investor_id, i.first_name, i.last_name, i.investor_type
    ORDER BY kjop_belop DESC
    """

    bind_values = (
        date_from.isoformat(),
        date_to.isoformat(),
        isin,
        isin,
    )
    rows = conn.execute(sql, bind_values[:sql.count("?")]).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df["eier"] = [clean_name(r["first_name"], r["last_name"], r.get("investor_id", "")) for _, r in df.iterrows()]
        for c in ["kjop_belop", "salg_belop", "netto_belop"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
        df["salg_mnok"] = df["salg_belop"] / 1_000_000
        df["netto_mnok"] = df["netto_belop"] / 1_000_000
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
