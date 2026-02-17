# handler_data_beste.py
"""
Data-layer for 'Beste investorer' — den mest komplekse analysen.
Ren Python, ingen Streamlit. Importeres av handler_routes.py.

Denne modulen har dynamisk kolonne-deteksjon, investor/aksje-filtrering,
gevinst-beregning med last_price, og transaksjons-drilldown.
"""
from __future__ import annotations

import os
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import pandas as pd

import handler_data as hd


# =========================================================
# Column detection
# =========================================================
@dataclass
class DbCols:
    date_col: str
    isin_col: str
    investor_col: str
    qty_col: str
    price_trade_col: str
    sec_isin_col: str
    sec_ticker_col: Optional[str]
    sec_name_col: Optional[str]
    sec_last_price_col: Optional[str]
    inv_type_col: Optional[str]
    inv_first_col: Optional[str]
    inv_last_col: Optional[str]
    inv_name_col: Optional[str]
    inv_country_col: Optional[str]


def _list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return [r[0] if isinstance(r, tuple) else r["name"] for r in rows]


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {(r[1] if isinstance(r, tuple) else r["name"]) for r in rows}


def detect_cols(conn: sqlite3.Connection) -> DbCols:
    tables = set(_list_tables(conn))
    pc_cols = _table_cols(conn, "position_change")
    sec_cols = _table_cols(conn, "security")
    inv_cols = _table_cols(conn, "investor") if "investor" in tables else set()

    date_col = "date_today" if "date_today" in pc_cols else ("date" if "date" in pc_cols else None)
    isin_col = "isin" if "isin" in pc_cols else None
    investor_col = "investor_id" if "investor_id" in pc_cols else None
    qty_col = "change_qty" if "change_qty" in pc_cols else ("qty" if "qty" in pc_cols else None)

    price_trade_col = None
    for c in ["price_yesterday", "price_today", "price"]:
        if c in pc_cols:
            price_trade_col = c
            break

    sec_isin_col = "isin"
    sec_ticker_col = "ticker" if "ticker" in sec_cols else None
    sec_name_col = "isin_name" if "isin_name" in sec_cols else ("name" if "name" in sec_cols else None)
    sec_last_price_col = "last_price" if "last_price" in sec_cols else None

    inv_type_col = None
    for c in ["investor_type", "type", "investor_category"]:
        if c in inv_cols:
            inv_type_col = c
            break

    inv_first_col = "first_name" if "first_name" in inv_cols else None
    inv_last_col = "last_name" if "last_name" in inv_cols else None
    inv_name_col = "name" if "name" in inv_cols else None

    inv_country_col = None
    for c in ["country_code", "countryCode", "country", "iso_country"]:
        if c in inv_cols:
            inv_country_col = c
            break

    return DbCols(
        date_col=date_col, isin_col=isin_col, investor_col=investor_col,
        qty_col=qty_col, price_trade_col=price_trade_col,
        sec_isin_col=sec_isin_col, sec_ticker_col=sec_ticker_col,
        sec_name_col=sec_name_col, sec_last_price_col=sec_last_price_col,
        inv_type_col=inv_type_col, inv_first_col=inv_first_col,
        inv_last_col=inv_last_col, inv_name_col=inv_name_col,
        inv_country_col=inv_country_col,
    )


def ensure_indexes(conn: sqlite3.Connection, cols: DbCols) -> None:
    try:
        conn.executescript(f"""
        CREATE INDEX IF NOT EXISTS idx_pc_isin_date ON position_change({cols.isin_col}, {cols.date_col});
        CREATE INDEX IF NOT EXISTS idx_pc_investor_date ON position_change({cols.investor_col}, {cols.date_col});
        CREATE INDEX IF NOT EXISTS idx_pc_date ON position_change({cols.date_col});
        """)
        conn.commit()
    except Exception:
        pass


# =========================================================
# CSV / investor list helpers
# =========================================================
def read_semicolon_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", encoding="latin-1")
    except Exception:
        return pd.read_csv(path, sep=",", encoding="latin-1")


def load_first_column_values(csv_path: str) -> List[str]:
    df = read_semicolon_csv(csv_path)
    col = df.columns[0]
    vals = df[col].astype(str).str.strip().tolist()
    vals = [x for x in vals if x and x.lower() not in ("selskap", "investor_id", "id", "ticker", "isin", "aksje")]
    out, seen = [], set()
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# =========================================================
# Filters
# =========================================================
def _clean_nan(s: str) -> str:
    s2 = (s or "").strip()
    return "" if s2.lower() == "nan" else s2


def fetch_distinct_country_codes(conn: sqlite3.Connection, cols: DbCols) -> List[str]:
    if not cols.inv_country_col:
        return []
    rows = conn.execute(
        f"SELECT DISTINCT {cols.inv_country_col} AS cc FROM investor WHERE COALESCE({cols.inv_country_col},'') <> '' ORDER BY cc"
    ).fetchall()
    return [str(r["cc"]).strip() for r in rows if r["cc"]]


def fetch_distinct_investor_types(conn: sqlite3.Connection, cols: DbCols) -> List[str]:
    if not cols.inv_type_col:
        return []
    rows = conn.execute(
        f"SELECT DISTINCT {cols.inv_type_col} AS t FROM investor WHERE COALESCE({cols.inv_type_col},'') <> '' ORDER BY t"
    ).fetchall()
    return [str(r["t"]).strip() for r in rows if r["t"]]


def fetch_investor_type_map(conn: sqlite3.Connection, cols: DbCols) -> Dict[str, str]:
    if not cols.inv_type_col:
        return {}
    rows = conn.execute(f"SELECT investor_id, {cols.inv_type_col} AS t FROM investor").fetchall()
    return {str(r["investor_id"]).strip(): ("" if r["t"] is None else str(r["t"])) for r in rows}


def fetch_investor_country_map(conn: sqlite3.Connection, cols: DbCols) -> Dict[str, str]:
    if not cols.inv_country_col:
        return {}
    rows = conn.execute(f"SELECT investor_id, {cols.inv_country_col} AS cc FROM investor").fetchall()
    return {str(r["investor_id"]).strip(): ("" if r["cc"] is None else str(r["cc"]).strip()) for r in rows}


def normalize_invtype_filter(selected: str, raw_type: str) -> bool:
    sel = (selected or "").strip().lower()
    t = (raw_type or "").strip().lower()
    if sel in ("", "alle", "begge"):
        return True
    if t in ("p", "priv", "privat", "private", "person", "individual"):
        t_norm = "privat"
    elif t in ("o", "org", "organisasjon", "organization", "company", "firm", "corporate"):
        t_norm = "org"
    else:
        t_norm = t
    if sel == "privat":
        return any(x in t_norm for x in ["priv", "person", "individual"])
    if sel == "organisasjon":
        return any(x in t_norm for x in ["org", "company", "firm", "corpor", "as", "asa", "ltd", "plc"])
    return True


# =========================================================
# Security resolving
# =========================================================
def search_securities_by_prefix(conn: sqlite3.Connection, cols: DbCols, query: str, limit: int = 50) -> List[dict]:
    q = (query or "").strip()
    if not q:
        return []
    q_up = q.upper()
    prefix = f"{q_up}%"
    where, params = [], []
    if cols.sec_ticker_col:
        where.append(f"UPPER(COALESCE({cols.sec_ticker_col},'')) LIKE ?")
        params.append(prefix)
    if cols.sec_name_col:
        where.append(f"UPPER(COALESCE({cols.sec_name_col},'')) LIKE ?")
        params.append(prefix)
    if not where:
        return []

    ticker_sel = f"COALESCE({cols.sec_ticker_col},'') AS ticker" if cols.sec_ticker_col else "'' AS ticker"
    name_sel = f"COALESCE({cols.sec_name_col},'') AS navn" if cols.sec_name_col else "'' AS navn"

    sql = f"""
    SELECT {cols.sec_isin_col} AS isin, {ticker_sel}, {name_sel}
    FROM security WHERE {" OR ".join(where)}
    ORDER BY COALESCE({cols.sec_ticker_col or "''"}, '') LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [{"isin": str(r["isin"]).strip(), "ticker": r["ticker"], "navn": r["navn"]} for r in rows]


def resolve_isin_list_by_prefix(conn: sqlite3.Connection, cols: DbCols, tokens: List[str]) -> List[str]:
    isins = set()
    for t in tokens:
        for r in search_securities_by_prefix(conn, cols, t, limit=500):
            isins.add(r["isin"])
    return sorted(isins)


# =========================================================
# Last price cache
# =========================================================
def _detect_price_table(conn: sqlite3.Connection) -> Optional[Tuple[str, str, str]]:
    tables = _list_tables(conn)
    candidates = [t for t in tables if "price" in t.lower() or "kurs" in t.lower()]
    for t in candidates:
        cols_set = _table_cols(conn, t)
        if "isin" not in cols_set:
            continue
        date_col = next((c for c in ["date", "dato", "day", "trade_date", "date_today"] if c in cols_set), None)
        if not date_col:
            continue
        price_col = next((c for c in ["close", "close_price", "price", "kurs", "last", "adj_close"] if c in cols_set), None)
        if not price_col:
            continue
        return (t, date_col, price_col)
    return None


def build_last_price_cache(conn: sqlite3.Connection, cols: DbCols) -> Tuple[Dict[str, float], str]:
    # Priority 1: security.last_price
    if cols.sec_last_price_col:
        df = pd.read_sql(
            f"SELECT {cols.sec_isin_col} AS isin, {cols.sec_last_price_col} AS last_price "
            f"FROM security WHERE COALESCE({cols.sec_last_price_col},0)>0", conn)
        if not df.empty:
            df["isin"] = df["isin"].astype(str).str.strip()
            df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
            return dict(zip(df["isin"], df["last_price"])), f"Pris-kilde: security.{cols.sec_last_price_col}"

    # Priority 2: dedicated price table
    price_info = _detect_price_table(conn)
    if price_info:
        table, date_col, price_col = price_info
        sql = f"""
        WITH last_dates AS (
            SELECT isin, MAX({date_col}) AS last_date FROM {table}
            WHERE COALESCE({price_col},0)>0 GROUP BY isin
        )
        SELECT p.isin, p.{price_col} AS last_price
        FROM {table} p JOIN last_dates ld ON ld.isin=p.isin AND ld.last_date=p.{date_col}
        """
        df = pd.read_sql(sql, conn)
        if not df.empty:
            df["isin"] = df["isin"].astype(str).str.strip()
            df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
            return dict(zip(df["isin"], df["last_price"])), f"Pris-kilde: {table}.{price_col}"

    # Priority 3: position_change fallback
    sql = f"""
    WITH last_dates AS (
        SELECT {cols.isin_col} AS isin, MAX({cols.date_col}) AS last_date
        FROM position_change WHERE COALESCE({cols.price_trade_col},0)>0
        GROUP BY {cols.isin_col}
    )
    SELECT pc.{cols.isin_col} AS isin, pc.{cols.price_trade_col} AS last_price
    FROM position_change pc JOIN last_dates ld
      ON ld.isin=pc.{cols.isin_col} AND ld.last_date=pc.{cols.date_col}
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        return {}, "Pris-kilde: (ingen)"
    df["isin"] = df["isin"].astype(str).str.strip()
    df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
    return dict(zip(df["isin"], df["last_price"])), f"Pris-kilde: position_change.{cols.price_trade_col}"


# =========================================================
# Core calculation
# =========================================================
def compute_best_investors(
    conn: sqlite3.Connection,
    cols: DbCols,
    date_from: dt.date,
    date_to: dt.date,
    investor_ids: Optional[List[str]],
    invtype_choice: str,
    country_codes: Optional[List[str]],
    isin_filter: Optional[List[str]],
    min_trades: int,
    min_brutto_mnok: float,
    last_price_map: Dict[str, float],
) -> pd.DataFrame:
    where = [f"date(pc.{cols.date_col}) BETWEEN date(?) AND date(?)"]
    params: list = [date_from.isoformat(), date_to.isoformat()]

    if investor_ids:
        placeholders = ",".join(["?"] * len(investor_ids))
        where.append(f"pc.{cols.investor_col} IN ({placeholders})")
        params.extend(investor_ids)

    if isin_filter:
        placeholders = ",".join(["?"] * len(isin_filter))
        where.append(f"pc.{cols.isin_col} IN ({placeholders})")
        params.extend(isin_filter)

    sql = f"""
    WITH prices AS (
        SELECT {cols.isin_col} AS isin, date({cols.date_col}) AS d,
               MAX({cols.price_trade_col}) AS p
        FROM position_change WHERE COALESCE({cols.price_trade_col},0)>0
        GROUP BY {cols.isin_col}, date({cols.date_col})
    )
    SELECT date(pc.{cols.date_col}) AS dato,
           pc.{cols.investor_col} AS investor_id,
           pc.{cols.isin_col} AS isin,
           pc.{cols.qty_col} AS qty,
           pc.{cols.price_trade_col} AS price_main,
           p2.p AS price_nextday
    FROM position_change pc
    LEFT JOIN prices p2
      ON p2.isin=pc.{cols.isin_col} AND p2.d=date(pc.{cols.date_col},'+1 day')
    WHERE {" AND ".join(where)}
    """
    df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return pd.DataFrame()

    df["investor_id"] = df["investor_id"].astype(str).str.strip()
    df["isin"] = df["isin"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["price_main"] = pd.to_numeric(df["price_main"], errors="coerce").fillna(0.0)
    df["price_nextday"] = pd.to_numeric(df["price_nextday"], errors="coerce").fillna(0.0)

    df["trade_price"] = df["price_main"]
    mask0 = df["trade_price"] <= 0
    df.loc[mask0, "trade_price"] = df.loc[mask0, "price_nextday"]
    df = df[df["trade_price"] > 0]
    if df.empty:
        return pd.DataFrame()

    # Investor type filter
    type_map = fetch_investor_type_map(conn, cols)
    if type_map and invtype_choice and invtype_choice.strip().lower() not in ("", "alle"):
        mask = df["investor_id"].map(type_map).apply(lambda t: normalize_invtype_filter(invtype_choice, t))
        df = df[mask.fillna(False).astype(bool)]
        if df.empty:
            return pd.DataFrame()

    # Country filter
    cc_map = fetch_investor_country_map(conn, cols)
    if cc_map and country_codes:
        cc_set = {c.strip().upper() for c in country_codes}
        mask_cc = df["investor_id"].map(cc_map).apply(lambda cc: (cc or "").strip().upper() in cc_set)
        df = df[mask_cc.fillna(False).astype(bool)]
        if df.empty:
            return pd.DataFrame()

    df["last_price"] = df["isin"].map(last_price_map).fillna(0.0)
    df["profit_kr"] = df["qty"] * (df["last_price"] - df["trade_price"])
    df["gross_kr"] = (df["qty"].abs() * df["trade_price"]).abs()

    agg = df.groupby("investor_id").agg(
        trades=("qty", "count"),
        gross_kr=("gross_kr", "sum"),
        profit_kr=("profit_kr", "sum"),
    ).reset_index()

    agg["brutto_mnok"] = agg["gross_kr"] / 1_000_000
    agg["gevinst_mnok"] = agg["profit_kr"] / 1_000_000
    agg["gevinst_pct"] = agg.apply(
        lambda r: (r["profit_kr"] / r["gross_kr"]) * 100.0 if r["gross_kr"] else 0.0, axis=1)

    agg = agg[(agg["trades"] >= min_trades) & (agg["brutto_mnok"] >= min_brutto_mnok)]
    if agg.empty:
        return pd.DataFrame()

    # Investor names
    navn_map: Dict[str, str] = {}
    if "investor" in _list_tables(conn):
        select_parts = ["investor_id"]
        if cols.inv_first_col:
            select_parts.append(f"COALESCE({cols.inv_first_col},'') AS first_name")
        else:
            select_parts.append("'' AS first_name")
        if cols.inv_last_col:
            select_parts.append(f"COALESCE({cols.inv_last_col},'') AS last_name")
        else:
            select_parts.append("'' AS last_name")
        if cols.inv_name_col:
            select_parts.append(f"COALESCE({cols.inv_name_col},'') AS name")
        else:
            select_parts.append("'' AS name")

        inv = pd.read_sql(f"SELECT {', '.join(select_parts)} FROM investor", conn)
        inv["investor_id"] = inv["investor_id"].astype(str).str.strip()

        def mk_name(r):
            first = _clean_nan(str(r.get("first_name", "")))
            last = _clean_nan(str(r.get("last_name", "")))
            name = _clean_nan(str(r.get("name", "")))
            disp = " ".join(x for x in [first, last] if x).strip()
            return disp if disp else name
        inv["navn"] = inv.apply(mk_name, axis=1)
        navn_map = dict(zip(inv["investor_id"], inv["navn"]))

    agg["navn"] = agg["investor_id"].map(navn_map).fillna("")

    out = agg[["investor_id", "navn", "trades", "brutto_mnok", "gevinst_mnok", "gevinst_pct"]].copy()
    out = out.sort_values("gevinst_mnok", ascending=False).reset_index(drop=True)
    return out


# =========================================================
# Transactions drill-down
# =========================================================
def fetch_transactions(
    conn: sqlite3.Connection,
    cols: DbCols,
    investor_id: str,
    date_from: dt.date,
    date_to: dt.date,
    isin_filter: Optional[List[str]],
    last_price_map: Dict[str, float],
) -> pd.DataFrame:
    ticker_expr = f"COALESCE(s.{cols.sec_ticker_col},'') AS ticker" if cols.sec_ticker_col else "'' AS ticker"
    name_expr = f"COALESCE(s.{cols.sec_name_col},'') AS navn" if cols.sec_name_col else "'' AS navn"

    where = [f"pc.{cols.investor_col}=?", f"date(pc.{cols.date_col}) BETWEEN date(?) AND date(?)"]
    params: list = [investor_id, date_from.isoformat(), date_to.isoformat()]

    if isin_filter:
        placeholders = ",".join(["?"] * len(isin_filter))
        where.append(f"pc.{cols.isin_col} IN ({placeholders})")
        params.extend(isin_filter)

    sql = f"""
    WITH prices AS (
        SELECT {cols.isin_col} AS isin, date({cols.date_col}) AS d,
               MAX({cols.price_trade_col}) AS p
        FROM position_change WHERE COALESCE({cols.price_trade_col},0)>0
        GROUP BY {cols.isin_col}, date({cols.date_col})
    )
    SELECT date(pc.{cols.date_col}) AS dato, pc.{cols.isin_col} AS isin,
           {ticker_expr}, {name_expr},
           pc.{cols.qty_col} AS qty,
           pc.{cols.price_trade_col} AS price_main,
           p2.p AS price_nextday
    FROM position_change pc
    LEFT JOIN security s ON s.{cols.sec_isin_col}=pc.{cols.isin_col}
    LEFT JOIN prices p2 ON p2.isin=pc.{cols.isin_col} AND p2.d=date(pc.{cols.date_col},'+1 day')
    WHERE {" AND ".join(where)}
    ORDER BY date(pc.{cols.date_col}) ASC
    """
    df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return df

    df["isin"] = df["isin"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["price_main"] = pd.to_numeric(df["price_main"], errors="coerce").fillna(0.0)
    df["price_nextday"] = pd.to_numeric(df["price_nextday"], errors="coerce").fillna(0.0)

    df["trade_price"] = df["price_main"]
    mask0 = df["trade_price"] <= 0
    df.loc[mask0, "trade_price"] = df.loc[mask0, "price_nextday"]
    df = df[df["trade_price"] > 0]
    if df.empty:
        return df

    df["last_price"] = df["isin"].map(last_price_map).fillna(0.0)
    df["gross_kr"] = (df["qty"].abs() * df["trade_price"]).abs()
    df["profit_kr"] = df["qty"] * (df["last_price"] - df["trade_price"])
    df["brutto_mnok"] = df["gross_kr"] / 1_000_000
    df["gevinst_mnok"] = df["profit_kr"] / 1_000_000
    return df


# =========================================================
# Investor search (with country support)
# =========================================================
def search_investors_advanced(conn: sqlite3.Connection, cols: DbCols, query: str,
                              country_filter: Optional[List[str]] = None, limit: int = 50) -> List[dict]:
    if len((query or "").strip()) < 3:
        return []
    q = query.upper().strip()
    like = f"%{q}%"

    where, params = ["UPPER(COALESCE(investor_id,'')) LIKE ?"], [like]
    if cols.inv_first_col:
        where.append(f"UPPER(COALESCE({cols.inv_first_col},'')) LIKE ?"); params.append(like)
    if cols.inv_last_col:
        where.append(f"UPPER(COALESCE({cols.inv_last_col},'')) LIKE ?"); params.append(like)
    if cols.inv_name_col:
        where.append(f"UPPER(COALESCE({cols.inv_name_col},'')) LIKE ?"); params.append(like)

    select_parts = ["investor_id"]
    select_parts.append(f"COALESCE({cols.inv_first_col},'') AS first_name" if cols.inv_first_col else "'' AS first_name")
    select_parts.append(f"COALESCE({cols.inv_last_col},'') AS last_name" if cols.inv_last_col else "'' AS last_name")
    select_parts.append(f"COALESCE({cols.inv_name_col},'') AS name" if cols.inv_name_col else "'' AS name")
    select_parts.append(f"{cols.inv_country_col} AS country_code" if cols.inv_country_col else "'' AS country_code")

    sql = f"SELECT {', '.join(select_parts)} FROM investor WHERE {' OR '.join(where)} LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()

    result = []
    for r in rows:
        cc = _clean_nan(str(r["country_code"] if "country_code" in r.keys() else ""))
        if country_filter:
            if cc.upper() not in {c.strip().upper() for c in country_filter}:
                continue
        first = _clean_nan(str(r["first_name"]))
        last = _clean_nan(str(r["last_name"]))
        name = _clean_nan(str(r["name"]))
        disp = " ".join(x for x in [first, last] if x).strip() or name or "(Ukjent)"
        label = f"{disp} [{cc}]" if cc else disp
        result.append({
            "investor_id": str(r["investor_id"]).strip(),
            "label": label,
            "country_code": cc,
        })
    return result
