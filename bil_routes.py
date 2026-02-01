# bil_routes.py (DuckDB + Parquet fra S3 via lokal /tmp-cache, per-file cache)
import json
import os
import re
import tempfile
import threading
from datetime import datetime, timedelta, date

import boto3
import pandas as pd
import numpy as np
import duckdb

from flask import Blueprint, render_template, jsonify, request
import traceback

from config import (
    AWS_KEY,
    AWS_SECRET,
    AWS_REGION,
    S3_BUCKET_NAME,
    DEFAULT_STARTDATE
)

bil_bp = Blueprint('bil', __name__, url_prefix='/bil')
from svv_app import fetch_svv_data, flatten_svv_data, compute_eu_status

FINN_BASE_URL = "https://www.finn.no/mobility/item/"

# -------------------------
# S3 keys (VIKTIG)
# -------------------------
PARQUET_KEY_SOLGT = "calc/bil/database_biler.parquet"             # ✅ hele historikken (AWS)
PARQUET_KEY_REKORDRASK = "calc/bil/database_biler_siste.parquet"  # (beholdt hvis annet bruker den)

METADATA_KEY = "calc/metadata.json"


# ------------------ S3 helpers ------------------

def _get_s3_client():
    return boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )


def _get_metadata():
    try:
        s3 = _get_s3_client()
        meta_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=METADATA_KEY)
        metadata = json.loads(meta_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"ADVARSEL: Kunne ikke laste metadata. Feil: {e}")
        metadata = {}
    return metadata


# ------------------ Lokal cache av parquet (per S3-key) ------------------

_PARQUET_CACHE_LOCK = threading.Lock()

# cache per S3-key
# {
#   s3_key: {
#      "local_path": "...",
#      "etag": "...",
#      "last_modified": datetime,
#      "colmap": {...}  (DuckDB kolonnemapping)
#   }
# }
_PARQUET_CACHE = {}


def _safe_tmp_name_from_key(s3_key: str) -> str:
    # unngå "/" i filnavn
    return s3_key.replace("/", "__")


def _ensure_local_parquet(s3_key: str) -> str:
    """
    Henter parquet fra S3 til lokal /tmp ved behov.
    Laster ned på nytt hvis ETag/LastModified har endret seg.
    Cache er per S3-key slik at ulike filer aldri overskriver hverandre.
    """
    with _PARQUET_CACHE_LOCK:
        s3 = _get_s3_client()
        head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)

        etag = (head.get("ETag") or "").strip('"')
        last_modified = head.get("LastModified")

        if s3_key not in _PARQUET_CACHE:
            local_name = _safe_tmp_name_from_key(s3_key)
            local_path = os.path.join(tempfile.gettempdir(), local_name)
            _PARQUET_CACHE[s3_key] = {
                "local_path": local_path,
                "etag": None,
                "last_modified": None,
                "colmap": None
            }

        meta = _PARQUET_CACHE[s3_key]
        local_path = meta["local_path"]

        # Har vi allerede riktig versjon på disk?
        if os.path.exists(local_path):
            if meta["etag"] == etag and meta["last_modified"] == last_modified:
                return local_path

        # Last ned atomisk
        tmp_path = local_path + ".download"
        with open(tmp_path, "wb") as f:
            s3.download_fileobj(S3_BUCKET_NAME, s3_key, f)

        os.replace(tmp_path, local_path)

        # Oppdater cache-meta
        meta["etag"] = etag
        meta["last_modified"] = last_modified
        meta["colmap"] = None  # refresh colmap hvis fila endres

        return local_path


# ------------------ DuckDB connection + colmap ------------------

_DUCKDB_LOCK = threading.Lock()
_DUCKDB_CON = None


def _duckdb_con():
    global _DUCKDB_CON
    with _DUCKDB_LOCK:
        if _DUCKDB_CON is None:
            con = duckdb.connect(database=":memory:", read_only=False)
            con.execute("PRAGMA threads=4;")
            con.execute("PRAGMA enable_progress_bar=false;")
            _DUCKDB_CON = con
        return _DUCKDB_CON


def _qident(name: str) -> str:
    """Sikker quoting for kolonnenavn (inkl. æøå/space)."""
    if name is None:
        return None
    return '"' + name.replace('"', '""') + '"'


def _duckdb_get_colmap(local_path: str, s3_key: str) -> dict:
    """
    Mapper canonical feltnavn -> faktisk kolonnenavn i parquet.
    Cache per S3-key.
    """
    with _PARQUET_CACHE_LOCK:
        meta = _PARQUET_CACHE.get(s3_key)
        if meta and meta.get("colmap") is not None:
            return meta["colmap"]

    con = _duckdb_con()

    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{local_path}')").fetchall()
    actual_cols = [r[0] for r in cols]
    lower_map = {c.lower(): c for c in actual_cols}

    def pick(candidates):
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None

    colmap = {
        "produsent": pick(["Produsent", "produsent"]),
        "modell": pick(["Modell", "modell"]),
        "overskrift": pick(["Overskrift", "overskrift", "info"]),
        "selger": pick(["Selger", "selger"]),
        "pris_start": pick(["Pris", "pris"]),
        "pris_ny": pick(["Pris_ny", "pris_ny"]),
        "dato_start": pick(["Dato", "dato"]),
        "dato_end": pick(["Dato_ny", "dato_ny"]),
        "finnkode": pick(["FinnKode", "finnkode"]),
        "solgt": pick(["Solgt", "solgt"]),
        "km": pick(["kjørelengde", "km"]),
        "aar": pick(["årstall", "year"]),
        "rekkevidde": pick(["rekkevidde_str", "rekkevidde"]),
        "drivstoff": pick(["drivstoff"]),
        "hjuldrift": pick(["hjuldrift"]),
    }

    with _PARQUET_CACHE_LOCK:
        _PARQUET_CACHE[s3_key]["colmap"] = colmap

    return colmap


def _bool_expr(col_ident: str) -> str:
    """
    Returnerer ALWAYS BOOLEAN.
    Tåler bool, 0/1, og strenger som 'true'/'ja' osv.
    """
    return f"""
    (
      case
        when {col_ident} is null then false
        when try_cast({col_ident} as BOOLEAN) is not null then try_cast({col_ident} as BOOLEAN)
        when lower(trim(cast({col_ident} as varchar))) in ('1','true','t','yes','y','ja') then true
        else false
      end
    )
    """


def _build_where_sql(filters: dict, colmap: dict):
    """
    WHERE + params
    - Dato-filter bruker kun Dato_ny (dato_end)
    - Pris-filter bruker kun Pris_ny
    """
    clauses = []
    params = []

    # Dato (Dato_ny)
    if filters.get("startdato") and colmap.get("dato_end"):
        clauses.append(f"try_cast({_qident(colmap['dato_end'])} AS TIMESTAMP) >= try_cast(? AS TIMESTAMP)")
        params.append(filters["startdato"])

    # Produsent/modell
    if filters.get("produsent") and colmap.get("produsent"):
        clauses.append(f"{_qident(colmap['produsent'])} = ?")
        params.append(filters["produsent"])

    if filters.get("modell") and colmap.get("modell"):
        clauses.append(f"{_qident(colmap['modell'])} = ?")
        params.append(filters["modell"])

    # Tekstsøk
    if filters.get("modell_sok") and colmap.get("overskrift"):
        clauses.append(
            f"lower(cast({_qident(colmap['overskrift'])} AS VARCHAR)) LIKE '%' || lower(?) || '%'"
        )
        params.append(filters["modell_sok"])

    if filters.get("seller_sok") and colmap.get("selger"):
        clauses.append(
            f"lower(trim(cast({_qident(colmap['selger'])} AS VARCHAR))) LIKE '%' || lower(trim(?)) || '%'"
        )
        params.append(filters["seller_sok"])

    # Default: pris_min = 1 hvis klient ikke sender noe (ønsket standard i UI)
    if filters.get("pris_min") in (None, ""):
        filters["pris_min"] = 1

    # Pris (Pris_ny)
    if colmap.get("pris_ny"):
        if filters.get("pris_min") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['pris_ny'])} AS BIGINT) >= ?")
            params.append(int(filters["pris_min"]))
        if filters.get("pris_max") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['pris_ny'])} AS BIGINT) <= ?")
            params.append(int(filters["pris_max"]))

    # Km max
    if colmap.get("km") and filters.get("km_max") not in (None, ""):
        clauses.append(f"try_cast({_qident(colmap['km'])} AS BIGINT) <= ?")
        params.append(int(filters["km_max"]))

    # År min/max
    if colmap.get("aar"):
        if filters.get("year_min") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['aar'])} AS BIGINT) >= ?")
            params.append(int(filters["year_min"]))
        if filters.get("year_max") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['aar'])} AS BIGINT) <= ?")
            params.append(int(filters["year_max"]))

    # Rekkevidde min/max
    if colmap.get("rekkevidde"):
        if filters.get("range_min") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['rekkevidde'])} AS BIGINT) >= ?")
            params.append(int(filters["range_min"]))
        if filters.get("range_max") not in (None, ""):
            clauses.append(f"try_cast({_qident(colmap['rekkevidde'])} AS BIGINT) <= ?")
            params.append(int(filters["range_max"]))

    # Drivstoff/hjuldrift (multi)
    if colmap.get("drivstoff") and isinstance(filters.get("drivstoff"), list) and filters["drivstoff"]:
        vals = filters["drivstoff"]
        placeholders = ",".join(["?"] * len(vals))
        clauses.append(f"{_qident(colmap['drivstoff'])} IN ({placeholders})")
        params.extend(vals)

    if colmap.get("hjuldrift") and isinstance(filters.get("hjuldrift"), list) and filters["hjuldrift"]:
        vals = filters["hjuldrift"]
        placeholders = ",".join(["?"] * len(vals))
        clauses.append(f"{_qident(colmap['hjuldrift'])} IN ({placeholders})")
        params.extend(vals)

    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params


# ------------------ Ruter ------------------

@bil_bp.route('/')
def bil_landing():
    return render_template('bil_landing.html')


# ==========================================================
# SOLGT-OVERSIKT (DuckDB)  -- bruker HELE historikken
# ==========================================================

@bil_bp.route('/solgt/oversikt')
def bil_solgt_oversikt_side():
    metadata = _get_metadata()
    return render_template(
        'bil_solgt_oversikt.html',
        tittel="Solgte biler",
        data_url="/bil/solgt/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=json.dumps(metadata.get('models_by_prod', {})),
        default_startdate=DEFAULT_STARTDATE,
    )


@bil_bp.route('/solgt/data', methods=['POST'])
def get_bil_solgt_data():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}
        s3_key = PARQUET_KEY_SOLGT
        local_path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(local_path, s3_key)

        where_sql, params = _build_where_sql(filters, colmap)

        con = _duckdb_con()

        # Velg kolonner vi viser (med aliases)
        selects = []
        def add(key, alias):
            if colmap.get(key):
                selects.append(f"{_qident(colmap[key])} AS {alias}")

        add("produsent", "Produsent")
        add("modell", "Modell")
        add("aar", "årstall")
        add("drivstoff", "drivstoff")
        add("hjuldrift", "hjuldrift")
        add("km", "kjørelengde")
        add("pris_ny", "Pris_ny")
        add("dato_start", "Dato")
        add("dato_end", "Dato_ny")
        add("solgt", "Solgt")
        add("selger", "Selger")
        add("finnkode", "FinnKode")

        if not selects:
            return jsonify({"status": "error", "message": "Fant ingen kolonner i parquet."}), 500

        sql = f"""
            SELECT {", ".join(selects)}
            FROM read_parquet('{local_path}')
            {where_sql}
            ORDER BY try_cast({_qident(colmap.get('dato_end'))} AS TIMESTAMP) DESC NULLS LAST
            LIMIT 50000
        """

        df = con.execute(sql, params).df()
        df = df.where(pd.notna(df), None)
        rows = json.loads(df.to_json(orient='records', date_format='iso'))
        return jsonify({"status": "ok", "rows": rows, "kpis": {}})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


# ==========================================================
# NY: REKORDSOLGT / REKORDRASK (DuckDB, samme AWS-parquet)
# ==========================================================

@bil_bp.route('/rekordrask')
def bil_rekordrask_side():
    """Ny UI (inspirert av Streamlit) + ny backend mot database_biler.parquet (AWS)."""
    # Vi henter valglister direkte fra samme parquet som ellers i bil-appen
    s3_key = PARQUET_KEY_SOLGT
    path = _ensure_local_parquet(s3_key)
    colmap = _duckdb_get_colmap(path, s3_key)
    con = _duckdb_con()

    # Distinct-valglister (robust mot NULL/blank)
    def distinct_list(colkey: str):
        col = colmap.get(colkey)
        if not col:
            return ["Alle"]
        c = _qident(col)
        rows = con.execute(f"""
            SELECT DISTINCT trim(cast({c} as varchar)) AS v
            FROM read_parquet('{path}')
            WHERE {c} IS NOT NULL AND trim(cast({c} as varchar)) <> ''
            ORDER BY 1
        """).fetchall()
        vals = [r[0] for r in rows if r and r[0] is not None and str(r[0]).strip() != ""]
        return ["Alle"] + vals

    produsenter = distinct_list("produsent")
    drivstoff = distinct_list("drivstoff")
    hjuldrift = distinct_list("hjuldrift")

    # Fornuftige defaultdatoer (siste 30 dager)
    today = date.today()
    default_from = (today - timedelta(days=30)).isoformat()
    default_to = today.isoformat()

    return render_template(
        "bil_rekordrask.html",
        tittel="Biler solgt rekordraskt",
        grupper_url="/bil/rekordrask/grupper",
        data_url="/bil/rekordrask/data",
        produsenter=produsenter,
        drivstoff=drivstoff,
        hjuldrift=hjuldrift,
        default_from=default_from,
        default_to=default_to,
        default_max_days=3,
        default_min_obs=30,
    )


def _rekordsolgt_conditions_sql(colmap: dict):
    """Returnerer (days_expr, is_sold_expr)."""
    c_dato = _qident(colmap.get("dato_start") or "Dato")
    c_dato_ny = _qident(colmap.get("dato_end") or "Dato_ny")
    c_solgt = _qident(colmap.get("solgt") or "Solgt")

    # dager ute (kan bli NULL hvis datoer mangler)
    days_expr = f"(epoch(try_cast({c_dato_ny} AS TIMESTAMP)) - epoch(try_cast({c_dato} AS TIMESTAMP))) / 86400.0"

    # Solgt != 'nei' (inkluderer 'fjernet' etc.)
    sold_norm = f"lower(trim(cast({c_solgt} as varchar)))"
    is_sold_expr = f"({c_solgt} IS NOT NULL AND {sold_norm} <> 'nei')"

    return days_expr, is_sold_expr


def _rekordrask_group_cols(choice: str, custom_cols):
    """UI->kanoniske kolonnenavn."""
    presets = {
        "Produsent + Modell": ["produsent", "modell"],
        "Produsent + Modell + årstall": ["produsent", "modell", "aar"],
        "Produsent + Modell + årstall + drivstoff": ["produsent", "modell", "aar", "drivstoff"],
    }
    if choice in presets:
        return presets[choice]
    # Egendefinert: forvent kanoniske nøkler fra UI
    if isinstance(custom_cols, list) and custom_cols:
        allowed = {"produsent", "modell", "aar", "drivstoff", "hjuldrift", "km", "selger"}
        return [c for c in custom_cols if c in allowed]
    return ["produsent", "modell"]


@bil_bp.route('/rekordrask/grupper', methods=['POST'])
def bil_rekordrask_grupper():
    """Returnerer grupperte andeler + KPIer."""
    try:
        payload = request.get_json() or {}
        filters = payload.get("filters", {}) or {}

        # UI-felter (ny)
        fra_dato = filters.get("fra_dato")  # YYYY-MM-DD (Dato)
        til_dato = filters.get("til_dato")  # YYYY-MM-DD (Dato)
        max_days = int(filters.get("max_days", 3))
        min_obs = int(filters.get("min_obs", 30))

        group_choice = filters.get("group_choice", "Produsent + Modell + årstall + drivstoff")
        custom_cols = filters.get("group_cols")
        group_cols_keys = _rekordrask_group_cols(group_choice, custom_cols)

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        # (canonical_key, actual_column)
        group_cols_pairs = []
        for k in group_cols_keys:
            actual = colmap.get(k)
            if actual:
                group_cols_pairs.append((k, actual))

        if not group_cols_pairs:
            return jsonify({"status": "error", "message": "Ingen gyldige grupperingskolonner funnet i datagrunnlaget."}), 400

        group_cols_keys_valid = [k for (k, _) in group_cols_pairs]

        days_expr, is_sold_expr = _rekordsolgt_conditions_sql(colmap)
        is_rekord_expr = f"({is_sold_expr} AND {days_expr} <= ?)"

        # WHERE for basis-filter (dato/pris/år/drivstoff/hjuldrift/produsent)
        # NB: vi filtrerer på Dato (dato_start) for fra/til slik som i Streamlit
        where_clauses = []
        params = [max_days]  # først til is_rekord_expr

        if colmap.get("dato_start"):
            c_dato = _qident(colmap["dato_start"])
            if fra_dato:
                where_clauses.append(f"date(try_cast({c_dato} AS TIMESTAMP)) >= date(?)")
                params.append(fra_dato)
            if til_dato:
                where_clauses.append(f"date(try_cast({c_dato} AS TIMESTAMP)) <= date(?)")
                params.append(til_dato)

        if filters.get("produsent") and filters["produsent"] != "Alle" and colmap.get("produsent"):
            where_clauses.append(f"{_qident(colmap['produsent'])} = ?")
            params.append(filters["produsent"])

        if filters.get("drivstoff") and filters["drivstoff"] != "Alle" and colmap.get("drivstoff"):
            where_clauses.append(f"{_qident(colmap['drivstoff'])} = ?")
            params.append(filters["drivstoff"])
        if filters.get("hjuldrift") and filters["hjuldrift"] != "Alle" and colmap.get("hjuldrift"):
            where_clauses.append(f"{_qident(colmap['hjuldrift'])} = ?")
            params.append(filters["hjuldrift"])

        if colmap.get("pris_ny"):
            c_pris = _qident(colmap["pris_ny"])
            if filters.get("pris_fra") not in (None, ""):
                where_clauses.append(f"try_cast({c_pris} AS BIGINT) >= ?")
                params.append(int(filters["pris_fra"]))
            if filters.get("pris_til") not in (None, ""):
                where_clauses.append(f"try_cast({c_pris} AS BIGINT) <= ?")
                params.append(int(filters["pris_til"]))

        if colmap.get("aar"):
            c_aar = _qident(colmap["aar"])
            if filters.get("aar_fra") not in (None, ""):
                where_clauses.append(f"try_cast({c_aar} AS BIGINT) >= ?")
                params.append(int(filters["aar_fra"]))
            if filters.get("aar_til") not in (None, ""):
                where_clauses.append(f"try_cast({c_aar} AS BIGINT) <= ?")
                params.append(int(filters["aar_til"]))

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        select_group = ", ".join([f"{_qident(actual)} AS {k}" for (k, actual) in group_cols_pairs])
        group_by = ", ".join([str(i+1) for i in range(len(group_cols_pairs))])

        sql_groups = f"""
            WITH base AS (
                SELECT
                    {select_group},
                    {days_expr} AS dager,
                    {is_sold_expr} AS is_sold,
                    {is_rekord_expr} AS is_rekord
                FROM read_parquet('{path}')
                {where_sql}
            )
            SELECT
                {", ".join(group_cols_keys_valid)},
                COUNT(*) AS alle_antall,
                SUM(CASE WHEN is_rekord THEN 1 ELSE 0 END) AS rekordsolgt_antall,
                CASE WHEN COUNT(*) = 0 THEN 0 ELSE (SUM(CASE WHEN is_rekord THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) END AS andel_rekordsolgt
            FROM base
            GROUP BY {group_by}
            HAVING COUNT(*) >= ?
            ORDER BY andel_rekordsolgt DESC, rekordsolgt_antall DESC, alle_antall DESC
            LIMIT 5000
        """

        df_groups = con.execute(sql_groups, params + [min_obs]).df()

        # KPIer (hele utvalget)
        sql_kpi = f"""
            SELECT
                COUNT(*) AS alle_antall,
                SUM(CASE WHEN {is_rekord_expr} THEN 1 ELSE 0 END) AS rekordsolgt_antall
            FROM read_parquet('{path}')
            {where_sql}
        """
        df_kpi = con.execute(sql_kpi, params).df()
        alle = int(df_kpi.loc[0, "alle_antall"]) if not df_kpi.empty else 0
        rek = int(df_kpi.loc[0, "rekordsolgt_antall"]) if not df_kpi.empty else 0
        andel = (rek / alle) if alle else 0.0

        df_groups = df_groups.where(pd.notna(df_groups), None)

        return jsonify({
            "status": "ok",
            "group_cols": group_cols_keys_valid,
            "groups": json.loads(df_groups.to_json(orient="records")),
            "kpis": {"alle_antall": alle, "rekordsolgt_antall": rek, "andel": andel}
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@bil_bp.route('/rekordrask/data', methods=['POST'])
def get_bil_rekordrask_data():
    """Returnerer enkeltbiler i valgt gruppe (rekordsolgte)."""
    try:
        payload = request.get_json() or {}
        filters = payload.get("filters", {}) or {}
        group = payload.get("group", {}) or {}

        fra_dato = filters.get("fra_dato")
        til_dato = filters.get("til_dato")
        max_days = int(filters.get("max_days", 3))

        group_choice = filters.get("group_choice", "Produsent + Modell + årstall + drivstoff")
        custom_cols = filters.get("group_cols")
        group_cols_keys = _rekordrask_group_cols(group_choice, custom_cols)

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        days_expr, is_sold_expr = _rekordsolgt_conditions_sql(colmap)
        is_rekord_expr = f"({is_sold_expr} AND {days_expr} <= ?)"

        where_clauses = []
        params = [max_days]

        if colmap.get("dato_start"):
            c_dato = _qident(colmap["dato_start"])
            if fra_dato:
                where_clauses.append(f"date(try_cast({c_dato} AS TIMESTAMP)) >= date(?)")
                params.append(fra_dato)
            if til_dato:
                where_clauses.append(f"date(try_cast({c_dato} AS TIMESTAMP)) <= date(?)")
                params.append(til_dato)

        if filters.get("produsent") and filters["produsent"] != "Alle" and colmap.get("produsent"):
            where_clauses.append(f"{_qident(colmap['produsent'])} = ?")
            params.append(filters["produsent"])

        if filters.get("drivstoff") and filters["drivstoff"] != "Alle" and colmap.get("drivstoff"):
            where_clauses.append(f"{_qident(colmap['drivstoff'])} = ?")
            params.append(filters["drivstoff"])

        if filters.get("hjuldrift") and filters["hjuldrift"] != "Alle" and colmap.get("hjuldrift"):
            where_clauses.append(f"{_qident(colmap['hjuldrift'])} = ?")
            params.append(filters["hjuldrift"])

        if colmap.get("pris_ny"):
            c_pris = _qident(colmap["pris_ny"])
            if filters.get("pris_fra") not in (None, ""):
                where_clauses.append(f"try_cast({c_pris} AS BIGINT) >= ?")
                params.append(int(filters["pris_fra"]))
            if filters.get("pris_til") not in (None, ""):
                where_clauses.append(f"try_cast({c_pris} AS BIGINT) <= ?")
                params.append(int(filters["pris_til"]))

        if colmap.get("aar"):
            c_aar = _qident(colmap["aar"])
            if filters.get("aar_fra") not in (None, ""):
                where_clauses.append(f"try_cast({c_aar} AS BIGINT) >= ?")
                params.append(int(filters["aar_fra"]))
            if filters.get("aar_til") not in (None, ""):
                where_clauses.append(f"try_cast({c_aar} AS BIGINT) <= ?")
                params.append(int(filters["aar_til"]))

        # Gruppe-match
        for k in group_cols_keys:
            if k in group and group[k] not in (None, "", "Alle") and colmap.get(k):
                where_clauses.append(f"{_qident(colmap[k])} = ?")
                params.append(group[k])

        # Kun rekordsolgte i detaljvisningen
        where_clauses.append(is_rekord_expr)

        where_sql = " WHERE " + " AND ".join(where_clauses)

        # Kolonner til UI
        c_prod = _qident(colmap.get("produsent")) if colmap.get("produsent") else "NULL"
        c_mod = _qident(colmap.get("modell")) if colmap.get("modell") else "NULL"
        c_aar = _qident(colmap.get("aar")) if colmap.get("aar") else "NULL"
        c_driv = _qident(colmap.get("drivstoff")) if colmap.get("drivstoff") else "NULL"
        c_hjul = _qident(colmap.get("hjuldrift")) if colmap.get("hjuldrift") else "NULL"
        c_pris = _qident(colmap.get("pris_ny")) if colmap.get("pris_ny") else "NULL"
        c_km = _qident(colmap.get("km")) if colmap.get("km") else "NULL"
        c_finn = _qident(colmap.get("finnkode")) if colmap.get("finnkode") else "NULL"
        c_dato = _qident(colmap.get("dato_start")) if colmap.get("dato_start") else "NULL"
        c_dato_ny = _qident(colmap.get("dato_end")) if colmap.get("dato_end") else "NULL"
        c_selger = _qident(colmap.get("selger")) if colmap.get("selger") else "NULL"

        finn_url_expr = f"CASE WHEN {c_finn} IS NULL THEN NULL ELSE '{FINN_BASE_URL}' || cast({c_finn} as varchar) END"

        sql_rows = f"""
            SELECT
                {c_prod} AS produsent,
                {c_mod} AS modell,
                try_cast({c_aar} AS BIGINT) AS aarstall,
                {c_driv} AS drivstoff,
                {c_hjul} AS hjuldrift,
                try_cast({c_pris} AS BIGINT) AS pris_ny,
                try_cast({c_km} AS BIGINT) AS km,
                try_cast({c_dato} AS TIMESTAMP) AS dato,
                try_cast({c_dato_ny} AS TIMESTAMP) AS dato_ny,
                {days_expr} AS dager,
                {c_selger} AS selger,
                {c_finn} AS finnkode,
                {finn_url_expr} AS finn_url
            FROM read_parquet('{path}')
            {where_sql}
            ORDER BY dager ASC, dato_ny ASC
            LIMIT 5000
        """

        df = con.execute(sql_rows, params).df()
        df = df.where(pd.notna(df), None)
        rows = json.loads(df.to_json(orient="records", date_format="iso"))

        return jsonify({"status": "ok", "rows": rows})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


# ==========================================================
# SVV (beholdt)
# ==========================================================

@bil_bp.route('/svv', methods=['GET', 'POST'])
def bil_svv_side():
    svv_raw = None
    flat = None
    error = None
    eu_status = None

    if request.method == 'POST':
        try:
            regnr = request.form.get('regnr', '').strip()
            vin = request.form.get('vin', '').strip()
            if not regnr and not vin:
                error = "Du må fylle inn regnr eller VIN."
            else:
                svv_raw = fetch_svv_data(regnr=regnr or None, vin=vin or None)
                if not svv_raw:
                    error = "Ingen data funnet fra Statens vegvesen."
                else:
                    flat = flatten_svv_data(svv_raw)
                    eu_status = compute_eu_status(svv_raw)
        except Exception as e:
            error = str(e)

    return render_template(
        'bil_svv.html',
        svv_raw=svv_raw,
        flat=flat,
        error=error,
        eu_status=eu_status
    )
