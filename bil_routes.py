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
PARQUET_KEY_SOLGT = "calc/bil/database_biler.parquet"           # ✅ hele historikken (til /bil/solgt og /bil/rekordrask)
PARQUET_KEY_REKORDRASK = "calc/bil/database_biler_siste.parquet"  # (ikke brukt lenger her – beholdt for kompatibilitet)

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




def _normalize_date_input(s: str | int | float | None) -> str | None:
    """Normaliserer dato fra UI til ISO-format.

    Støtter:
      - dd.mm.yyyy [hh:mm[:ss]]
      - yyyy-mm-dd [hh:mm[:ss]]
      - mm/dd/yyyy [hh:mm[:ss]] (typisk browser/locale)
      - epoch (sekunder eller millisekunder): int/float eller streng med 10/13 siffer

    Returnerer:
      - 'YYYY-MM-DD' eller 'YYYY-MM-DD HH:MM:SS'
      - None hvis tom/ugyldig
    """
    if s is None:
        return None

    # Epoch i sek/ms (kan komme fra JS Date)
    if isinstance(s, (int, float)):
        epoch = float(s)
        if epoch > 1e12:  # ms
            epoch = epoch / 1000.0
        return datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%d")

    s = str(s).strip()
    if not s:
        return None

    if re.fullmatch(r"\d{10,13}", s):
        epoch = float(s)
        if len(s) >= 13:
            epoch = epoch / 1000.0
        return datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%d")

    # dd.mm.yyyy [hh:mm[:ss]]
    m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$', s)
    if m:
        dd, mm, yyyy, hh, mi, ss = m.groups()
        if hh is None:
            return f"{yyyy}-{mm}-{dd}"
        ss = ss or "00"
        return f"{yyyy}-{mm}-{dd} {hh}:{mi}:{ss}"

    # yyyy-mm-dd [hh:mm[:ss]]
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$', s)
    if m:
        yyyy, mm, dd, hh, mi, ss = m.groups()
        if hh is None:
            return f"{yyyy}-{mm}-{dd}"
        ss = ss or "00"
        return f"{yyyy}-{mm}-{dd} {hh}:{mi}:{ss}"

    # mm/dd/yyyy eller dd/mm/yyyy [hh:mm[:ss]]
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$', s)
    if m:
        a, b, yyyy, hh, mi, ss = m.groups()
        a_i, b_i = int(a), int(b)
        # heuristikk: hvis a > 12 og b <= 12, tolkes som dd/mm
        if a_i > 12 and b_i <= 12:
            dd, mm = f"{a_i:02d}", f"{b_i:02d}"
        else:
            mm, dd = f"{a_i:02d}", f"{b_i:02d}"
        if hh is None:
            return f"{yyyy}-{mm}-{dd}"
        ss = ss or "00"
        return f"{yyyy}-{mm}-{dd} {hh}:{mi}:{ss}"

    # fallback: returner som den er (DuckDB try_cast/try_strptime kan fortsatt klare det)
    return s


def _to_bigint_sql(col_ident: str) -> str:
    """Robust tall-cast i DuckDB: fjern alt som ikke er 0-9 og try_cast til BIGINT."""
    return f"try_cast(regexp_replace(cast({col_ident} as varchar), '[^0-9]', '', 'g') as BIGINT)"


def _to_timestamp_sql(col_ident: str) -> str:
    """
    Robust timestamp-parse i DuckDB.

    Støtter:
      - Tekst:
        'YYYY-MM-DD HH:MM:SS'
        'YYYY-MM-DD'
        'DD.MM.YYYY'
        'DD.MM.YYYY HH:MM:SS'
      - Tall (ofte i parquet):
        10 siffer  = epoch seconds
        13 siffer  = epoch milliseconds
        16 siffer  = epoch microseconds
        19 siffer  = epoch nanoseconds
        8 siffer   = yyyymmdd
    """
    s = f"trim(cast({col_ident} as varchar))"
    n = f"try_cast({s} as BIGINT)"

    numeric_ts = f"""
      CASE
        WHEN regexp_matches({s}, '^[0-9]{{19}}$') THEN to_timestamp({n} / 1000000000.0)
        WHEN regexp_matches({s}, '^[0-9]{{16}}$') THEN to_timestamp({n} / 1000000.0)
        WHEN regexp_matches({s}, '^[0-9]{{13}}$') THEN to_timestamp({n} / 1000.0)
        WHEN regexp_matches({s}, '^[0-9]{{10}}$') THEN to_timestamp({n} * 1.0)
        WHEN regexp_matches({s}, '^[0-9]{{8}}$')  THEN try_strptime({s}, '%Y%m%d')
        ELSE NULL
      END
    """

    return (
        "coalesce("
        f"{numeric_ts},"
        f"try_strptime({s}, '%Y-%m-%d %H:%M:%S'),"
        f"try_strptime({s}, '%Y-%m-%d'),"
        f"try_strptime({s}, '%d.%m.%Y %H:%M:%S'),"
        f"try_strptime({s}, '%d.%m.%Y')"
        ")"
    )



def _safe_timestamp_sql(col_ident: str) -> str:
    """Bakoverkompatibel wrapper brukt i eldre filterkode."""
    return _to_timestamp_sql(col_ident)


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
        "farge": pick(["farge", "Farge", "eksteriorfarge", "ExteriorColor"]),
        "personlig_skilt": pick(["personlig_skilt", "Personlig_skilt", "personligskilt", "har_personlig_skilt"]),
        "storrelseklasse": pick(["storrelseklasse", "størrelseklasse", "bilstorrelse", "bilstørrelse", "segment"]),
        "motor_effekt_hk": pick(["motor_effekt_hk", "effekt_hk", "hestekrefter", "hk", "power_hp"]),
        "motor_effekt_kw": pick(["motor_effekt_kw", "effekt_kw", "kw", "power_kw"]),
        "bruktimport": pick(["svv_bruktimportert", "bruktimport", "Bruktimport", "brukt_import", "importert_brukt", "is_imported_used"]),
        "import_land": pick(["svv_importland_navn", "import_land", "importland", "opprinnelsesland", "import_country", "origin_country"]),
        "svv_registrert_forste_gang_norge": pick([
            "svv_registrert_forste_gang_norge",
            "registrert_forste_gang_norge",
            "forstegangsregistrert_norge",
            "forste_gang_registrert_norge",
            "first_registered_norway",
        ]),
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


def _imported_expr(col_ident: str) -> str:
    """
    Returnerer true for eksplisitte import-verdier (SANN/TRUE/1/JA).
    Alt annet (inkl. NULL/blank/manglende) blir false.
    """
    txt = f"trim(cast({col_ident} as varchar))"
    return f"""
    (
      case
        when {col_ident} is null then false
        when {txt} = '' then false
        when try_cast({col_ident} as BOOLEAN) is not null then try_cast({col_ident} as BOOLEAN)
        when lower({txt}) in ('sann','true','t','1','ja','yes','y') then true
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

    # Farge (multi)
    if colmap.get("farge") and isinstance(filters.get("farge"), list) and filters["farge"]:
        vals = filters["farge"]
        placeholders = ",".join(["?"] * len(vals))
        clauses.append(f"{_qident(colmap['farge'])} IN ({placeholders})")
        params.extend(vals)

    # Personlig skilt
    if colmap.get("personlig_skilt") and filters.get("personlig_skilt") in ("ja", "nei"):
        bool_expr = _bool_expr(_qident(colmap["personlig_skilt"]))
        if filters["personlig_skilt"] == "ja":
            clauses.append(f"({bool_expr}) = true")
        else:
            clauses.append(f"({bool_expr}) = false")

    # Størrelseklasse
    if colmap.get("storrelseklasse") and isinstance(filters.get("storrelseklasse"), list) and filters["storrelseklasse"]:
        vals = filters["storrelseklasse"]
        placeholders = ",".join(["?"] * len(vals))
        clauses.append(f"{_qident(colmap['storrelseklasse'])} IN ({placeholders})")
        params.extend(vals)

    # Bruktimport (ja/nei)
    if filters.get("bruktimport") in ("ja", "nei"):
        if colmap.get("bruktimport"):
            bool_expr = _imported_expr(_qident(colmap["bruktimport"]))
            if filters["bruktimport"] == "ja":
                clauses.append(f"({bool_expr}) = true")
            else:
                clauses.append(f"({bool_expr}) = false")
        else:
            # Manglende felt tolkes som usann: "ja" skal gi 0 treff, "nei" gir ingen ekstra begrensning.
            if filters["bruktimport"] == "ja":
                clauses.append("1 = 0")

    # Nylig registrert i Norge (svv_registrert_forste_gang_norge)
    # Treffer når feltet mangler/tomt (ikke registrert ennå), eller når registreringsdato er nyere enn valgt antall dager.
    recent_import_days = filters.get("recent_import_days")
    if recent_import_days not in (None, "") and colmap.get("svv_registrert_forste_gang_norge"):
        days = max(int(recent_import_days), 0)
        reg_col = _qident(colmap["svv_registrert_forste_gang_norge"])
        reg_ts = _safe_timestamp_sql(reg_col)
        reg_txt = f"trim(cast({reg_col} as varchar))"
        clauses.append(
            "(" \
            f"{reg_col} IS NULL " \
            f"OR {reg_txt} = '' " \
            f"OR ({reg_ts} IS NOT NULL AND date_diff('day', date({reg_ts}), current_date) BETWEEN 0 AND ?)" \
            ")"
        )
        params.append(days)

    # Importland (multi)
    if colmap.get("import_land") and isinstance(filters.get("import_land"), list) and filters["import_land"]:
        vals = filters["import_land"]
        placeholders = ",".join(["?"] * len(vals))
        clauses.append(f"{_qident(colmap['import_land'])} IN ({placeholders})")
        params.extend(vals)

    # Motorstyrke i hk (primært), evt. kw fallback
    c_hk = _qident(colmap["motor_effekt_hk"]) if colmap.get("motor_effekt_hk") else None
    c_kw = _qident(colmap["motor_effekt_kw"]) if colmap.get("motor_effekt_kw") else None
    motor_expr = None
    if c_hk and c_kw:
        motor_expr = f"coalesce(try_cast({c_hk} AS DOUBLE), try_cast({c_kw} AS DOUBLE) * 1.34102209)"
    elif c_hk:
        motor_expr = f"try_cast({c_hk} AS DOUBLE)"
    elif c_kw:
        motor_expr = f"(try_cast({c_kw} AS DOUBLE) * 1.34102209)"

    if motor_expr:
        if filters.get("motor_hk_min") not in (None, ""):
            clauses.append(f"{motor_expr} >= ?")
            params.append(float(filters["motor_hk_min"]))
        if filters.get("motor_hk_max") not in (None, ""):
            clauses.append(f"{motor_expr} <= ?")
            params.append(float(filters["motor_hk_max"]))

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
        preset_bruktimport="",
        preset_recent_import_days="",
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


@bil_bp.route('/solgt/bruktimport')
def bil_solgt_bruktimport_side():
    metadata = _get_metadata()
    return render_template(
        'bil_analyse_template.html',
        tittel="Bruktimporterte biler",
        preset_bruktimport="ja",
        preset_recent_import_days=30,
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

        exclude_maxdate_sql = ""
        exclude_maxdate_params = []
        if max_date is not None:
            exclude_maxdate_sql = f" AND date({dato_end_ts}) < ?"
            exclude_maxdate_params.append(str(max_date))

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
        c_farge = col_or_null("farge")
        c_personlig_skilt = col_or_null("personlig_skilt")
        c_storrelseklasse = col_or_null("storrelseklasse")
        c_motor_hk = col_or_null("motor_effekt_hk")
        c_motor_kw = col_or_null("motor_effekt_kw")
        c_bruktimport = col_or_null("bruktimport")
        c_import_land = col_or_null("import_land")

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
        motor_hk_expr = f"coalesce(try_cast({c_motor_hk} AS DOUBLE), try_cast({c_motor_kw} AS DOUBLE) * 1.34102209)"
        personlig_skilt_expr = _bool_expr(c_personlig_skilt) if colmap.get("personlig_skilt") else "NULL"
        bruktimport_expr = _imported_expr(c_bruktimport) if colmap.get("bruktimport") else "false"

        solgt_expr = _bool_expr(c_solgt) if colmap.get("solgt") else None

        solgt_filter_sql = ""
        solgt_filter_params = []
        if solgt_expr:
            solgt_filter_sql = f" AND ({solgt_expr}) = true"
        else:
            solgt_filter_sql = f" AND ({pris_ny_num}) > 1000"

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

        grouped_stats = []
        if colmap.get("produsent") and colmap.get("modell"):
            grouped_sql = f"""
              SELECT
                {c_prod} AS produsent,
                {c_mod} AS modell,
                COUNT(*) AS antall
              FROM read_parquet('{path}')
              {where_sql}
              {status_sql}
              GROUP BY 1, 2
              ORDER BY antall DESC, produsent ASC, modell ASC
            """
            grouped_df = con.execute(grouped_sql, params + status_params).df()
            grouped_df = grouped_df.where(pd.notna(grouped_df), None)
            grouped_stats = json.loads(grouped_df.to_json(orient="records"))

        data_sql = f"""
          SELECT
            {c_prod} AS produsent,
            {c_mod} AS modell,
            {c_aar} AS årstall,
            {c_km} AS kjørelengde,
            {c_driv} AS drivstoff,
            {c_hjul} AS hjuldrift,
            {c_rekk} AS rekkevidde,
            {c_farge} AS farge,
            {c_storrelseklasse} AS storrelseklasse,
            {motor_hk_expr} AS motor_hk,
            {personlig_skilt_expr} AS personlig_skilt,
            {bruktimport_expr} AS bruktimport,
            {c_import_land} AS import_land,
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
            'grouped_stats': grouped_stats,
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
# REKORDRASK (DuckDB) – logikk basert på analyse_rekordsolgt.py
#
# Definisjon:
#   rekordsolgt = (Solgt indikerer faktisk "solgt/fjernet") AND (Dato_ny - Dato) <= maks_dager
# Filtrering:
#   - fra/til dato filtrerer på Dato (startdato / publiseringsdato)
#   - pris_fra/pris_til filtrerer på Pris_ny
#   - årstall fra/til filtrerer på årstall
#   - drivstoff/hjuldrift/produsent filtrerer på eksakt match (Alle = ingen filter)
#
# Endepunkter:
#   GET  /bil/rekordrask
#   POST /bil/rekordrask/grupper   -> grupper + kpis + group_cols
#   POST /bil/rekordrask/data      -> rader for valgt gruppe
# ==========================================================


def _normalize_str_sql(col_ident: str) -> str:
    """lower(trim(cast(.. as varchar)))"""
    return f"lower(trim(cast({col_ident} as varchar)))"


def _solgt_true_expr(solgt_norm_expr: str) -> str:
    """
    Returnerer SQL-uttrykk som tolker om "Solgt"-feltet faktisk betyr faktisk solgt.

    Viktig:
      - Tom streng / NULL skal IKKE regnes som solgt.
      - Tidligere logikk brukte `solgt != 'nei'`, som ga falske positive når feltet var tomt.
      - "fjernet/removed" regnes ikke som solgt, fordi annonser kan fjernes uten salg.
    """
    return f"""
      (
        {solgt_norm_expr} IN ('ja', 'true', '1', 'solgt', 'sold')
      )
    """


def _rekordrask_group_cols(filters: dict, colmap: dict):
    """Returner (group_cols_sql, group_cols_names) der names er feltnavn i JSON."""
    choice = (filters.get("group_choice") or "").strip()
    custom = filters.get("group_cols") or None

    m = {
        "produsent": ( _qident(colmap.get("produsent")), "produsent"),
        "modell":    ( _qident(colmap.get("modell")), "modell"),
        "aar":       ( _qident(colmap.get("aar")), "aarstall"),
        "drivstoff": ( _qident(colmap.get("drivstoff")), "drivstoff"),
        "hjuldrift": ( _qident(colmap.get("hjuldrift")), "hjuldrift"),
        "selger":    ( _qident(colmap.get("selger")), "selger"),
        "km":        ( _qident(colmap.get("km")), "km"),
    }

    def pick(keys):
        cols_sql = []
        cols_names = []
        for k in keys:
            sql_ident, out_name = m.get(k, (None, None))
            if sql_ident and sql_ident != "NULL":
                cols_sql.append(sql_ident)
                cols_names.append(out_name)
        return cols_sql, cols_names

    if choice == "Produsent + Modell":
        return pick(["produsent", "modell"])
    if choice == "Produsent + Modell + årstall":
        return pick(["produsent", "modell", "aar"])
    if choice == "Produsent + Modell + årstall + drivstoff":
        return pick(["produsent", "modell", "aar", "drivstoff"])

    if isinstance(custom, list) and custom:
        return pick(custom)

    return pick(["produsent", "modell"])


def _rekordrask_where(filters: dict, colmap: dict):
    """
    WHERE + params for DF_alle.

    Viktig:
      - date_field bestemmer om fra/til gjelder:
          * "dato"    => Dato (dato_start / publisert)
          * "dato_ny" => Dato_ny (dato_end / solgt/fjernet)
      - Default: "dato" (matcher etiketten i UI: "Fra-dato (Dato)")
    """
    clauses = []
    params = []

    # Velg hvilket datofelt som skal brukes for fra/til
    date_field = (filters.get("date_field") or "dato").strip().lower()
    if date_field in ("dato", "start", "dato_start"):
        c_dato = colmap.get("dato_start")
    else:
        c_dato = colmap.get("dato_end")  # default: dato_ny

    c_prod = colmap.get("produsent")
    c_pris = colmap.get("pris_ny")
    c_aar  = colmap.get("aar")
    c_driv = colmap.get("drivstoff")
    c_hjul = colmap.get("hjuldrift")

    # Dato-filter
    if c_dato:
        dato_ts = _to_timestamp_sql(_qident(c_dato))

        fra = _normalize_date_input(filters.get("fra_dato"))
        til = _normalize_date_input(filters.get("til_dato"))

        # NB: sammenligner som DATE på begge sider
        if fra:
            clauses.append(f"date({dato_ts}) >= date(try_cast(? as TIMESTAMP))")
            params.append(fra)
        if til:
            clauses.append(f"date({dato_ts}) <= date(try_cast(? as TIMESTAMP))")
            params.append(til)

    # Produsent
    produsent = (filters.get("produsent") or "Alle").strip()
    if produsent != "Alle" and c_prod:
        clauses.append(f"{_qident(c_prod)} = ?")
        params.append(produsent)

    # Pris (Pris_ny)
    pris_fra = filters.get("pris_fra")
    pris_til = filters.get("pris_til")
    if c_pris and pris_fra not in (None, ""):
        pris_expr = f"coalesce({_to_bigint_sql(_qident(c_pris))}, 0)"
        clauses.append(f"{pris_expr} >= ?")
        params.append(int(pris_fra))
    if c_pris and pris_til not in (None, ""):
        pris_expr = f"coalesce({_to_bigint_sql(_qident(c_pris))}, 0)"
        clauses.append(f"{pris_expr} <= ?")
        params.append(int(pris_til))

    # Årstall
    aar_fra = filters.get("aar_fra")
    aar_til = filters.get("aar_til")
    if c_aar and aar_fra not in (None, ""):
        aar_expr = f"coalesce({_to_bigint_sql(_qident(c_aar))}, 0)"
        clauses.append(f"{aar_expr} >= ?")
        params.append(int(aar_fra))
    if c_aar and aar_til not in (None, ""):
        aar_expr = f"coalesce({_to_bigint_sql(_qident(c_aar))}, 0)"
        clauses.append(f"{aar_expr} <= ?")
        params.append(int(aar_til))

    # Drivstoff
    drivstoff = (filters.get("drivstoff") or "Alle").strip()
    if drivstoff != "Alle" and c_driv:
        clauses.append(f"{_qident(c_driv)} = ?")
        params.append(drivstoff)

    # Hjuldrift
    hjuldrift = (filters.get("hjuldrift") or "Alle").strip()
    if hjuldrift != "Alle" and c_hjul:
        clauses.append(f"{_qident(c_hjul)} = ?")
        params.append(hjuldrift)

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


def _rekordrask_base_sql(path: str, colmap: dict, where_sql: str):
    """
    CTE base:
      - normaliserer solgt (string)
      - parser datoer robust (støtter dd.mm.yyyy)
      - beregner days_to_end
      - _is_rekord (binder maks_dager én gang)
    """
    c_dato = _qident(colmap.get("dato_start"))
    c_dato_ny = _qident(colmap.get("dato_end"))
    c_solgt = _qident(colmap.get("solgt"))

    dato_ts = _to_timestamp_sql(c_dato)
    dato_ny_ts = _to_timestamp_sql(c_dato_ny)

    solgt_norm = _normalize_str_sql(c_solgt)
    is_solgt = _solgt_true_expr(solgt_norm)

    days_to_end = f"(date_diff('second', {dato_ts}, {dato_ny_ts}) / 86400.0)"

    return f"""
      WITH base AS (
        SELECT
          *,
          {dato_ts} AS _dato,
          {dato_ny_ts} AS _dato_ny,
          {solgt_norm} AS _solgt_norm,
          {days_to_end} AS _days_to_end,
          (
            {is_solgt}
            AND {dato_ts} IS NOT NULL
            AND {dato_ny_ts} IS NOT NULL
            AND {days_to_end} IS NOT NULL
            AND {days_to_end} <= ?
          ) AS _is_rekord
        FROM read_parquet('{path}')
        {where_sql}
      )
    """


@bil_bp.route('/rekordrask')
def bil_rekordrask_side():
    """
    Denne siden bruker bil_rekordrask.html.
    Den henter grupper via POST /bil/rekordrask/grupper
    og detaljer via POST /bil/rekordrask/data
    """
    metadata = _get_metadata()

    today = date.today()
    default_from = (today - timedelta(days=30)).isoformat()
    default_to = today.isoformat()

    return render_template(
        'bil_rekordrask.html',
        tittel="Biler solgt rekordraskt",
        grupper_url="/bil/rekordrask/grupper",
        data_url="/bil/rekordrask/data",
        produsenter=metadata.get('produsenter', []),
        drivstoff=metadata.get('drivstoff_opts', []),
        hjuldrift=metadata.get('hjuldrift_opts', []),
        default_from=default_from,
        default_to=default_to,
        default_max_days=3,
        default_min_obs=30,
    )


@bil_bp.route('/rekordrask/grupper', methods=['POST'])
def bil_rekordrask_grupper():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        max_days = float(filters.get("max_days") or 3)
        min_obs = int(filters.get("min_obs") or 30)

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        for need in ("dato_start", "dato_end", "solgt"):
            if not colmap.get(need):
                return jsonify({"status": "error", "message": f"Datasettet mangler kolonne for {need}."}), 500

        where_sql, params = _rekordrask_where(filters, colmap)
        group_cols_sql, group_cols_names = _rekordrask_group_cols(filters, colmap)
        if not group_cols_sql:
            return jsonify({"status": "error", "message": "Ingen gyldige grupperingskolonner valgt."}), 400

        base = _rekordrask_base_sql(path, colmap, where_sql)

        select_group = []
        group_by = []
        for sql_ident, out_name in zip(group_cols_sql, group_cols_names):
            select_group.append(f"{sql_ident} AS {out_name}")
            group_by.append(sql_ident)

        sql = f"""
          {base}
          SELECT
            {', '.join(select_group)},
            COUNT(*) AS alle_antall,
            SUM(CASE WHEN _is_rekord THEN 1 ELSE 0 END) AS rekordsolgt_antall,
            (SUM(CASE WHEN _is_rekord THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS andel_rekordsolgt
          FROM base
          GROUP BY {', '.join(group_by)}
          HAVING COUNT(*) >= ?
          ORDER BY andel_rekordsolgt DESC, rekordsolgt_antall DESC, alle_antall DESC
          LIMIT 5000
        """

        df = con.execute(sql, [max_days] + params + [min_obs]).df()
        df = df.where(pd.notna(df), None)
        groups = json.loads(df.to_json(orient="records"))

        kpi_sql = f"""
          {base}
          SELECT
            COUNT(*) AS alle_antall,
            SUM(CASE WHEN _is_rekord THEN 1 ELSE 0 END) AS rekordsolgt_antall
          FROM base
        """
        kpi_row = con.execute(kpi_sql, [max_days] + params).fetchone()
        alle_antall = int(kpi_row[0] or 0)
        rekord_antall = int(kpi_row[1] or 0)
        andel = (rekord_antall / alle_antall) if alle_antall else 0.0

        return jsonify({
            "status": "ok",
            "group_cols": group_cols_names,
            "groups": groups,
            "kpis": {
                "alle_antall": alle_antall,
                "rekordsolgt_antall": rekord_antall,
                "andel": andel,
            }
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@bil_bp.route('/rekordrask/data', methods=['POST'])
def bil_rekordrask_data():
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}
        group = payload.get('group', {}) or {}

        max_days = float(filters.get("max_days") or 3)

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        where_sql, params = _rekordrask_where(filters, colmap)
        group_cols_sql, group_cols_names = _rekordrask_group_cols(filters, colmap)

        base = _rekordrask_base_sql(path, colmap, where_sql)

        c_prod = _qident(colmap.get("produsent"))
        c_mod  = _qident(colmap.get("modell"))
        c_aar  = _qident(colmap.get("aar"))
        c_driv = _qident(colmap.get("drivstoff"))
        c_hjul = _qident(colmap.get("hjuldrift"))
        c_pris = _qident(colmap.get("pris_ny"))
        c_km   = _qident(colmap.get("km"))
        c_sel  = _qident(colmap.get("selger"))
        c_finn = _qident(colmap.get("finnkode"))

        finnkode_str = f"regexp_replace(cast({c_finn} as varchar), '\\\\.0$', '')"
        finn_url_expr = f"CASE WHEN {c_finn} IS NULL THEN NULL ELSE '{FINN_BASE_URL}' || {finnkode_str} END"

        group_where_parts = []
        group_params = []

        name_to_sql = dict(zip(group_cols_names, group_cols_sql))
        for out_name, sql_ident in name_to_sql.items():
            if out_name not in group:
                continue
            val = group.get(out_name)
            if val is None or val == "":
                group_where_parts.append(f"{sql_ident} IS NULL")
            else:
                if out_name == "aarstall":
                    group_where_parts.append(f"try_cast({sql_ident} AS BIGINT) IS NOT DISTINCT FROM ?")
                    group_params.append(int(val))
                else:
                    group_where_parts.append(f"cast({sql_ident} as varchar) IS NOT DISTINCT FROM ?")
                    group_params.append(str(val))

        group_where_sql = (" AND " + " AND ".join(group_where_parts)) if group_where_parts else ""

        sql = f"""
          {base}
          SELECT
            {c_prod} AS produsent,
            {c_mod} AS modell,
            try_cast({c_aar} AS BIGINT) AS aarstall,
            {c_driv} AS drivstoff,
            {c_hjul} AS hjuldrift,
            coalesce(try_cast({c_pris} AS BIGINT), 0) AS pris_ny,
            try_cast({c_km} AS BIGINT) AS km,
            _days_to_end AS dager,
            CAST(_dato AS VARCHAR) AS dato,
            CAST(_dato_ny AS VARCHAR) AS dato_ny,
            {c_sel} AS selger,
            {finnkode_str} AS finnkode,
            {finn_url_expr} AS finn_url
          FROM base
          WHERE _is_rekord
          {group_where_sql}
          ORDER BY dager ASC, dato_ny ASC
          LIMIT 2000
        """

        df = con.execute(sql, [max_days] + params + group_params).df()
        df = df.where(pd.notna(df), None)
        rows = json.loads(df.to_json(orient="records"))

        return jsonify({"status": "ok", "rows": rows})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

# ==========================================================
# BILRADAR  –  leser forventet_pris/rabatt_pct fra parquet
#              (scoring skjer i konsolider_data.py på Pi)
# ==========================================================

BILRADAR_PARQUET_KEY = "calc/bil/database_biler.parquet"
BILRADAR_SISTE_PREFIX = "raw/bil-time/"
GOOD_DEAL_THRESHOLD = 10  # % rabatt for å regnes som godt kjøp

_BILRADAR_PARQUET_CACHE = {"etag": None, "df": None}
_BILRADAR_PARQUET_LOCK = threading.Lock()

BILRADAR_HTML_CACHE = {"alle": {"html": None, "etag": None},
                       "siste": {"html": None, "csv_key": None}}
BILRADAR_HTML_LOCK = threading.Lock()

def _get_bilradar_html_template() -> str:
    """Leser BilRadar HTML-template fra disk."""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "bil_radar.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def _lag_json_data_fra_parquet(df: pd.DataFrame) -> str:
    import json as _json
    col_map = {
        "FinnKode": "i", "Produsent": "m", "Modell": "mo",
        "Overskrift": "nf", "årstall": "a", "kjørelengde": "k",
        "girkasse": "g", "drivstoff": "d", "hjuldrift": "hj",
        "Karosseri": "ka", "Pris_ny": "p", "selger": "s",
        "sted": "st", "fylke": "fy", "forhandler": "fh",
        "BildeURL": "im", "forventet_pris": "ep", "rabatt_pct": "r",
    }
    int_keys = {"i", "a", "k", "p", "ep"}
    cars = []
    for _, row in df.iterrows():
        car = {}
        for src, dst in col_map.items():
            v = row.get(src)
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            if dst in int_keys:
                try:
                    iv = int(float(v))
                    if iv != 0:
                        car[dst] = iv
                except Exception:
                    pass
            elif dst == "r":
                try:
                    car[dst] = round(float(v), 1)
                except Exception:
                    pass
            else:
                sv = str(v).strip()
                if sv and sv.lower() not in ("nan", "none", ""):
                    car[dst] = sv
        if "p" not in car:
            car["p"] = 0
        if "r" not in car:
            car["r"] = 0
        cars.append(car)
    return _json.dumps(cars, ensure_ascii=False, separators=(",", ":"))


def _les_parquet_aktive(s3) -> tuple:
    with _BILRADAR_PARQUET_LOCK:
        head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=BILRADAR_PARQUET_KEY)
        etag = (head.get("ETag") or "").strip('"')
        if _BILRADAR_PARQUET_CACHE["etag"] == etag and _BILRADAR_PARQUET_CACHE["df"] is not None:
            return _BILRADAR_PARQUET_CACHE["df"], etag
        import io as _io
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=BILRADAR_PARQUET_KEY)
        df = pd.read_parquet(_io.BytesIO(obj["Body"].read()))
        if "Solgt" in df.columns:
            df = df[df["Solgt"] == "NEI"].copy()
        for col in ["forventet_pris", "rabatt_pct"]:
            if col not in df.columns:
                df[col] = np.nan
        _BILRADAR_PARQUET_CACHE["etag"] = etag
        _BILRADAR_PARQUET_CACHE["df"] = df
        return df, etag


@bil_bp.route('/radar')
def bil_radar_velger():
    return render_template('bil_radar_velger.html')


@bil_bp.route('/radar/alle')
def bil_radar_alle():
    import time as _time
    from flask import Response
    t0 = _time.perf_counter()
    try:
        s3 = _get_s3_client()
        df, etag = _les_parquet_aktive(s3)

        with BILRADAR_HTML_LOCK:
            cached = BILRADAR_HTML_CACHE["alle"].copy()
        if cached["html"] and cached["etag"] == etag:
            print("[BilRadar/alle] Cache – serverer direkte")
            return Response(cached["html"], mimetype='text/html')

        df_scoret = df[df["forventet_pris"].notna() & (df["forventet_pris"] > 0)].copy()
        print(f"[BilRadar/alle] {len(df_scoret)}/{len(df)} biler med scoring")

        data_json = _lag_json_data_fra_parquet(df_scoret)
        elapsed = _time.perf_counter() - t0
        dato = datetime.now().strftime("%d. %b %Y kl. %H:%M") + f" (lest på {elapsed:.1f}s)"

        html_template = _get_bilradar_html_template()
        html = html_template.replace("__DATA_JSON__", data_json)
        html = html.replace("__ANTALL__", str(len(df_scoret)))
        html = html.replace("__DATO__", dato)
        html = html.replace("__THRESHOLD__", str(GOOD_DEAL_THRESHOLD))

        with BILRADAR_HTML_LOCK:
            BILRADAR_HTML_CACHE["alle"]["html"] = html
            BILRADAR_HTML_CACHE["alle"]["etag"] = etag

        print(f"[BilRadar/alle] Ferdig: {len(df_scoret)} biler på {elapsed:.1f}s")
        return Response(html, mimetype='text/html')
    except Exception as e:
        traceback.print_exc()
        from flask import abort
        abort(500, description=f"Feil i BilRadar (alle): {e}")


@bil_bp.route('/radar/siste')
def bil_radar_siste():
    import io as _io
    import time as _time
    from flask import Response
    t0 = _time.perf_counter()
    try:
        s3 = _get_s3_client()

        paginator = s3.get_paginator("list_objects_v2")
        csv_files = []
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=BILRADAR_SISTE_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and obj.get("Size", 0) > 0:
                    csv_files.append((obj["LastModified"], key))

        if not csv_files:
            from flask import abort
            abort(404, description="Ingen biler_siste filer funnet i S3.")

        csv_files.sort(reverse=True)
        latest_key = csv_files[0][1]

        with BILRADAR_HTML_LOCK:
            cached = BILRADAR_HTML_CACHE["siste"].copy()
        if cached["html"] and cached["csv_key"] == latest_key:
            print("[BilRadar/siste] Cache – serverer direkte")
            return Response(cached["html"], mimetype='text/html')

        print(f"[BilRadar/siste] Leser {latest_key}")
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=latest_key)
        content = resp["Body"].read().decode("utf-16")
        df_csv = pd.read_csv(_io.StringIO(content), sep=";", dtype=str)

        df_csv["FinnKode"] = pd.to_numeric(
            df_csv["FinnKode"].astype(str).str.replace(r"\D", "", regex=True),
            errors="coerce"
        )
        df_csv = df_csv[df_csv["FinnKode"].notna()].copy()
        df_csv["FinnKode"] = df_csv["FinnKode"].astype("int64")
        siste_koder = set(df_csv["FinnKode"])
        print(f"[BilRadar/siste] {len(siste_koder)} biler i CSV")

        df_parquet, _ = _les_parquet_aktive(s3)
        df_siste = df_parquet[df_parquet["FinnKode"].isin(siste_koder)].copy()

        mangler = siste_koder - set(df_siste["FinnKode"])
        if mangler:
            df_ekstra = df_csv[df_csv["FinnKode"].isin(mangler)].copy()
            df_siste = pd.concat([df_siste, df_ekstra], ignore_index=True)
            print(f"[BilRadar/siste] +{len(mangler)} nye ikke scoret ennå")

        df_scoret = df_siste[df_siste["forventet_pris"].notna() & (df_siste["forventet_pris"] > 0)].copy()
        print(f"[BilRadar/siste] {len(df_scoret)}/{len(df_siste)} biler med scoring")

        data_json = _lag_json_data_fra_parquet(df_scoret)
        elapsed = _time.perf_counter() - t0
        dato = datetime.now().strftime("%d. %b %Y kl. %H:%M") + f" (lest på {elapsed:.1f}s)"

        html_template = _get_bilradar_html_template()
        html = html_template.replace("__DATA_JSON__", data_json)
        html = html.replace("__ANTALL__", str(len(df_scoret)))
        html = html.replace("__DATO__", dato)
        html = html.replace("__THRESHOLD__", str(GOOD_DEAL_THRESHOLD))
        html = html.replace(
            "<title>BilRadar \u2013 Finn gode bilkj\u00f8p</title>",
            "<title>BilRadar \u2013 Siste d\u00f8gn</title>"
        )
        html = html.replace("Bil<span>Radar</span>", "Bil<span>Radar</span> \u2013 Siste d\u00f8gn")

        with BILRADAR_HTML_LOCK:
            BILRADAR_HTML_CACHE["siste"]["html"] = html
            BILRADAR_HTML_CACHE["siste"]["csv_key"] = latest_key

        print(f"[BilRadar/siste] Ferdig: {len(df_scoret)} biler p\u00e5 {elapsed:.1f}s")
        return Response(html, mimetype='text/html')
    except Exception as e:
        traceback.print_exc()
        from flask import abort
        abort(500, description=f"Feil i BilRadar (siste): {e}")


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
