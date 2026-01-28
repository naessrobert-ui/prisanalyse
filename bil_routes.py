# bil_routes.py (DuckDB + Parquet fra S3 via lokal /tmp-cache, per-file cache)
import json
import os
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

from rekordrask_parquet import bygg_visning_for_solgte_fra_parquet

bil_bp = Blueprint('bil', __name__, url_prefix='/bil')
from svv_app import fetch_svv_data, flatten_svv_data, compute_eu_status

FINN_BASE_URL = "https://www.finn.no/mobility/item/"

# -------------------------
# S3 keys (VIKTIG)
# -------------------------
PARQUET_KEY_SOLGT = "calc/bil/database_biler.parquet"           # ✅ hele historikken (til /bil/solgt)
PARQUET_KEY_REKORDRASK = "calc/bil/database_biler_siste.parquet"  # ✅ “siste”-fila (til rekordrask / annen app)

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
    """Trygg quoting av SQL identifier."""
    if name is None:
        return "NULL"
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
            # trim() gjør søk mer robust mot tilfeldige mellomrom i datakilden
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
        tittel="Antall solgte biler",
        data_url="/bil/solgt/oversikt/data",
        drivstoff_opts=metadata.get('drivstoff_opts', []),
        hjuldrift_opts=metadata.get('hjuldrift_opts', []),
        year_min=metadata.get('year_min', 2000),
        year_max=metadata.get('year_max', datetime.now().year),
    )


@bil_bp.route('/solgt/oversikt/data', methods=['POST'])
def bil_solgt_oversikt_data():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        if not colmap.get("produsent") or not colmap.get("solgt"):
            return jsonify({
                'status': 'error',
                'message': 'Datasettet mangler nødvendige kolonner: produsent/solgt.'
            }), 500

        prod = _qident(colmap["produsent"])
        solgt_col = _qident(colmap["solgt"])
        driv = _qident(colmap["drivstoff"]) if colmap.get("drivstoff") else None
        aar = _qident(colmap["aar"]) if colmap.get("aar") else None

        where_parts = [f"{_bool_expr(solgt_col)} = true"]
        params = []

        if driv and isinstance(filters.get("drivstoff"), list) and filters["drivstoff"]:
            vals = filters["drivstoff"]
            where_parts.append(f"{driv} IN ({','.join(['?'] * len(vals))})")
            params.extend(vals)

        if aar:
            if filters.get("year_min") not in (None, ""):
                where_parts.append(f"try_cast({aar} AS BIGINT) >= ?")
                params.append(int(filters["year_min"]))
            if filters.get("year_max") not in (None, ""):
                where_parts.append(f"try_cast({aar} AS BIGINT) <= ?")
                params.append(int(filters["year_max"]))

        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        group_cols = [prod]
        select_cols = [f"{prod} AS produsent"]

        if filters.get("group_by_fuel") and driv:
            group_cols.append(driv)
            select_cols.append(f"{driv} AS drivstoff")

        if filters.get("group_by_age") and aar:
            now_year = datetime.now().year
            alder_expr = f"""
              CASE
                WHEN try_cast({aar} AS BIGINT) IS NULL THEN NULL
                ELSE
                  CASE
                    WHEN ({now_year} - try_cast({aar} AS BIGINT)) <= 1 THEN '0–1'
                    WHEN ({now_year} - try_cast({aar} AS BIGINT)) <= 3 THEN '2–3'
                    WHEN ({now_year} - try_cast({aar} AS BIGINT)) <= 5 THEN '4–5'
                    WHEN ({now_year} - try_cast({aar} AS BIGINT)) <= 8 THEN '6–8'
                    WHEN ({now_year} - try_cast({aar} AS BIGINT)) <= 12 THEN '9–12'
                    ELSE '13+'
                  END
              END
            """
            group_cols.append(alder_expr)
            select_cols.append(f"{alder_expr} AS alder")

        sql = f"""
          SELECT {', '.join(select_cols)},
                 COUNT(*) AS antall_solgt
          FROM read_parquet('{path}')
          {where_sql}
          GROUP BY {', '.join(group_cols)}
          ORDER BY antall_solgt DESC
        """

        out_df = con.execute(sql, params).df()
        out_df = out_df.where(pd.notna(out_df), None)
        summary = json.loads(out_df.to_json(orient='records'))

        return jsonify({'status': 'ok', 'summary': summary})

    except Exception as e:
        print(f"Feil i /bil/solgt/oversikt/data: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e), "message": "En uventet feil oppstod på serveren."}), 500


# ==========================================================
# SOLGT-ANALYSE SIDE  -- bruker HELE historikken
# ==========================================================

@bil_bp.route('/solgt')
def bil_solgt_analyse_side():
    metadata = _get_metadata()
    return render_template(
        'bil_analyse_template.html',
        tittel="Dette ble bilene solgt for",
        data_url="/bil/solgt/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=metadata.get('models_by_prod', {}),
        drivstoff_opts=metadata.get('drivstoff_opts', []),
        hjuldrift_opts=metadata.get('hjuldrift_opts', []),
        year_min=metadata.get('year_min', 2000),
        year_max=metadata.get('year_max', datetime.now().year),
        km_min=metadata.get('km_min', 0),
        km_max=metadata.get('km_max', 200000),
    )


@bil_bp.route('/solgt/data', methods=['POST'])
def get_bil_solgt_data():
    """
    ✅ Ingen early-return.
    ✅ Visning begrenses til MAX_ROWS_LIMIT billigste (pris_ny ASC).
    ✅ KPI + daily_stats beregnes på ALLE treff.
    """
    try:
        MAX_ROWS_LIMIT = 300

        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        where_sql, params = _build_where_sql(filters, colmap)

        def col_or_null(key: str) -> str:
            c = colmap.get(key)
            return _qident(c) if c else "NULL"

        c_dato_end = col_or_null("dato_end")
        c_solgt = col_or_null("solgt")
        dato_end_ts = f"try_cast({c_dato_end} AS TIMESTAMP)"

        # ---------------------------------
        # Statusfilter:
        # - finn_na: biler som er "sist sett" på max-dato i datasettet
        # - solgt_fjernet: alt som IKKE er sist sett på max-dato
        # - alle: ingen statusfilter på tabellen
        #
        # NB: Uansett status kutter vi max-dato i 'antall solgte'-diagrammet,
        # fordi max-dato representerer biler som fortsatt ligger ute.
        # ---------------------------------
        status = (filters.get("status") or "solgt_fjernet").strip()  # default: solgt/fjernet

        max_date = None
        if colmap.get("dato_end"):
            max_date = con.execute(
                f"SELECT max(date({dato_end_ts})) FROM read_parquet('{path}') WHERE {dato_end_ts} IS NOT NULL"
            ).fetchone()[0]

        status_sql = ""
        status_params = []
        if max_date is not None:
            if status == "finn_na":
                status_sql = f" AND date({dato_end_ts}) = ?"
                status_params.append(str(max_date))
                if colmap.get("solgt"):
                    status_sql += f" AND ({_bool_expr(c_solgt)}) = false"
            elif status == "solgt_fjernet":
                status_sql = f" AND date({dato_end_ts}) < ?"
                status_params.append(str(max_date))

        # brukes til diagram/KPI: kutt alltid maxdato, ellers blir siste stolpe "aktive" biler
        exclude_maxdate_sql = ""
        exclude_maxdate_params = []
        if max_date is not None:
            exclude_maxdate_sql = f" AND date({dato_end_ts}) < ?"
            exclude_maxdate_params.append(str(max_date))

        # Total treff (respekter status-filter for tabell/utvalg)
        count_sql = f"SELECT COUNT(*) AS cnt FROM read_parquet('{path}') {where_sql} {status_sql}"
        total_count = int(con.execute(count_sql, params + status_params).fetchone()[0])

        if total_count == 0:
            return jsonify({
                'status': 'ok',
                'historikk': [],
                'daily_stats': [],
                'kpis': {},
                'total_count': 0,
                'returned_count': 0,
                'limit': MAX_ROWS_LIMIT,
                'truncated': False
            })

        c_prod = col_or_null("produsent")
        c_mod = col_or_null("modell")
        c_aar = col_or_null("aar")
        c_km = col_or_null("km")
        c_driv = col_or_null("drivstoff")
        c_hjul = col_or_null("hjuldrift")
        c_rekk = col_or_null("rekkevidde")
        c_selger = col_or_null("selger")
        c_over = col_or_null("overskrift")
        c_pris_start = col_or_null("pris_start")
        c_pris_ny = col_or_null("pris_ny")
        c_dato_start = col_or_null("dato_start")
        c_finn = col_or_null("finnkode")

        pris_start_num = f"coalesce(try_cast({c_pris_start} AS BIGINT), 0)"
        pris_ny_num = f"coalesce(try_cast({c_pris_ny} AS BIGINT), 0)"

        dato_start_ts = f"try_cast({c_dato_start} AS TIMESTAMP)"

        dager_expr = f"""
          greatest(
            coalesce(date_diff('day', {dato_start_ts}, {dato_end_ts}), 0),
            0
          )
        """

        pris_endring_expr = f"({pris_ny_num} - {pris_start_num})"
        finnkode_str = f"regexp_replace(cast({c_finn} as varchar), '\\\\.0$', '')"
        finn_url_expr = f"CASE WHEN {c_finn} IS NULL THEN NULL ELSE '{FINN_BASE_URL}' || {finnkode_str} END"

        solgt_expr = _bool_expr(c_solgt) if colmap.get("solgt") else None

        # ----------------------------
        # KPI + daily_stats på ALLE treff (ikke bare 300)
        #
        # Bruk "solgt/fjernet"-logikk ved å kutte max-dato i datasettet.
        # Da unngår vi at siste dag i diagrammet blir "biler som fortsatt er ute".
        # ----------------------------
        solgt_filter_sql = ""
        solgt_filter_params = []
        if solgt_expr:
            solgt_filter_sql = f" AND ({solgt_expr}) = true"
        else:
            solgt_filter_sql = f" AND ({pris_ny_num}) > 1000"

        # Kutt alltid maxdato i KPI/diagram
        solgt_filter_sql += exclude_maxdate_sql
        solgt_filter_params.extend(exclude_maxdate_params)

        kpi_sql = f"""
          SELECT
            CAST(avg({dager_expr}) AS BIGINT) AS avg_dager,
            CAST(median({dager_expr}) AS BIGINT) AS median_dager,
            CAST(avg({pris_ny_num}) AS BIGINT) AS avg_pris,
            CAST(median({pris_ny_num}) AS BIGINT) AS median_pris,
            CAST(min({pris_ny_num}) AS BIGINT) AS laveste_pris,
            COUNT(*) AS antall
          FROM read_parquet('{path}')
          {where_sql}
          {solgt_filter_sql}
        """

        kpi_row = con.execute(kpi_sql, params + solgt_filter_params).fetchone()
        kpis = {}
        if kpi_row and kpi_row[5] and int(kpi_row[5]) > 0:
            kpis = {
                "avg_dager": int(kpi_row[0] or 0),
                "median_dager": int(kpi_row[1] or 0),
                "avg_pris": int(kpi_row[2] or 0),
                "median_pris": int(kpi_row[3] or 0),
                "laveste_pris": int(kpi_row[4] or 0),
                "antall": int(kpi_row[5] or 0),
            }

        daily_stats = []
        if colmap.get("dato_end"):
            daily_sql = f"""
              SELECT
                CAST(date({dato_end_ts}) AS VARCHAR) AS Dato,
                COUNT(*) AS Antall_Solgt,
                median({pris_ny_num}) AS Median_Pris,
                median({pris_ny_num}) AS Median_Pris_Usolgt
              FROM read_parquet('{path}')
              {where_sql}
              {solgt_filter_sql}
              AND {dato_end_ts} IS NOT NULL
              GROUP BY 1
              ORDER BY 1
            """
            daily_df = con.execute(daily_sql, params + solgt_filter_params).df()
            daily_df = daily_df.where(pd.notna(daily_df), None)
            daily_stats = json.loads(daily_df.to_json(orient="records"))

        # ----------------------------
        # Visningstabell: 300 billigste (pris_ny ASC)
        # ----------------------------
        data_sql = f"""
          SELECT
            {c_prod} AS produsent,
            {c_mod} AS modell,
            {c_aar} AS årstall,
            {c_km} AS kjørelengde,
            {c_driv} AS drivstoff,
            {c_hjul} AS hjuldrift,
            {c_rekk} AS rekkevidde,
            {c_selger} AS selger,
            {c_over} AS overskrift,
            {dato_start_ts} AS dato_start,
            {dato_end_ts} AS dato_end,
            {pris_start_num} AS pris_start,
            {pris_ny_num} AS pris_ny,
            {pris_ny_num} AS pris_last,
            {pris_endring_expr} AS pris_endring,
            {dager_expr} AS dager,
            {finnkode_str} AS finnkode,
            {finn_url_expr} AS finn_url
            {"," + solgt_expr + " AS solgt" if solgt_expr else ""}
          FROM read_parquet('{path}')
          {where_sql}
          {status_sql}
          ORDER BY {pris_ny_num} ASC
          LIMIT {MAX_ROWS_LIMIT}
        """

        output_df = con.execute(data_sql, params + status_params).df()
        output_df = output_df.where(pd.notna(output_df), None)

        historikk = json.loads(output_df.to_json(orient='records', date_format='iso'))
        returned_count = len(historikk)

        return jsonify({
            'status': 'ok',
            'historikk': historikk,
            'daily_stats': daily_stats,
            'kpis': kpis,
            'total_count': total_count,
            'returned_count': returned_count,
            'limit': MAX_ROWS_LIMIT,
            'truncated': total_count > returned_count
        })

    except Exception as e:
        print(f"Feil i /bil/solgt/data: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e), "message": "En uventet feil oppstod på serveren."}), 500


# ==========================================================
# REKORDRASK (beholdt som før, bruker din eksisterende pipeline)
# ==========================================================

@bil_bp.route('/rekordrask')
def bil_rekordrask_side():
    metadata = _get_metadata()
    return render_template(
        'bil_rekordrask.html',
        tittel="Biler solgt rekordraskt",
        data_url="/bil/rekordrask/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=json.dumps(metadata.get('models_by_prod', {})),
        default_startdate=(date.today() - timedelta(days=3)).isoformat(),
    )


@bil_bp.route('/rekordrask/grupper', methods=['POST'])
def bil_rekordrask_grupper():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        start_str = filters.get('startdato')
        maks_dager = int(filters.get('maks_dager', 5))

        if start_str:
            startdato = datetime.strptime(start_str, "%Y-%m-%d").date()
        else:
            startdato = date.today() - timedelta(days=3)

        df = bygg_visning_for_solgte_fra_parquet(startdato)
        if df.empty:
            return jsonify({'status': 'ok', 'groups': []})

        cols = {c.lower(): c for c in df.columns}
        c_prod = cols.get('produsent')
        c_mod = cols.get('modell')
        c_dager = cols.get('dager')

        if not c_prod or not c_mod:
            return jsonify({'status': 'error', 'message': 'Mangler produsent/modell i visningen'}), 500

        if c_dager and maks_dager is not None:
            df[c_dager] = pd.to_numeric(df[c_dager], errors='coerce')
            df = df[df[c_dager] <= maks_dager]

        groups = (
            df.groupby([c_prod, c_mod], dropna=False)
              .size()
              .reset_index(name='antall')
              .sort_values('antall', ascending=False)
        )

        groups = groups.where(pd.notna(groups), None)
        return jsonify({'status': 'ok', 'groups': json.loads(groups.to_json(orient='records'))})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@bil_bp.route('/rekordrask/data', methods=['POST'])
def get_bil_rekordrask_data():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}
        start_str = filters.get('startdato')

        if start_str:
            startdato = datetime.strptime(start_str, "%Y-%m-%d").date()
        else:
            startdato = date.today() - timedelta(days=3)

        vis_solgte = bygg_visning_for_solgte_fra_parquet(startdato)

        if vis_solgte.empty:
            return jsonify({'status': 'ok', 'rows': [], 'kpis': {}})

        vis_solgte = vis_solgte.where(pd.notna(vis_solgte), None)
        rows = json.loads(vis_solgte.to_json(orient='records'))
        return jsonify({'status': 'ok', 'rows': rows, 'kpis': {}})

    except Exception as e:
        print(f"Feil i /bil/rekordrask/data: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


# ==========================================================
# SVV (beholdt)
# ==========================================================

@bil_bp.route('/svv', methods=['GET', 'POST'])
def bil_svv_side():
    svv_raw = None
    flat = None
    error = None
    eu_status = None
    eu_dager_igjen = None

    if request.method == "POST":
        ident = (request.form.get("identifier") or "").strip()
        if not ident:
            error = "Du må oppgi et registreringsnummer eller understellsnummer."
        else:
            svv_raw, error = fetch_svv_data(ident)
            if svv_raw and not error:
                flat = flatten_svv_data(svv_raw)
                eu_status, eu_dager_igjen = compute_eu_status(
                    flat.get("svv_kontrollfrist")
                )

    pretty_json = json.dumps(svv_raw, indent=2, ensure_ascii=False) if svv_raw else None

    return render_template(
        "bil_svv.html",
        flat=flat,
        raw_json=pretty_json,
        error=error,
        eu_status=eu_status,
        eu_dager_igjen=eu_dager_igjen,
    )
