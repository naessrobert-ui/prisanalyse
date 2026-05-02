# bil_routes.py (DuckDB + Parquet fra S3 via lokal /tmp-cache, per-file cache)
import json
import io
import os
import re
import tempfile
import threading
from difflib import SequenceMatcher
from datetime import datetime, timedelta, date

import boto3
import pandas as pd
import numpy as np
import duckdb

from flask import Blueprint, render_template, jsonify, request
import traceback

from bilradar_scorer import last_modell_lokal_eller_s3, scorer_biler

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
# Cache for aktive FinnKoder fra siste daily
_ACTIVE_FK_CACHE = {
    "s3_key": None,
    "etag": None,
    "registered_in_duckdb": False,
}
_ACTIVE_FK_LOCK = threading.Lock()

ACTIVE_FK_TABLE = "active_finnkoder"  # DuckDB-tabellnavn


def _find_latest_daily_csv(s3) -> dict | None:
    """Finn S3-objektet for nyeste daily-CSV."""
    paginator = s3.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME, Prefix="raw/bil-daglig/"
    ):
        for obj in page.get("Contents", []):
            if not obj["Key"].lower().endswith(".csv"):
                continue
            if obj.get("Size", 0) < 1024:
                continue
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj
    return latest


def _ensure_active_finnkoder_table() -> bool:
    """
    Sikrer at DuckDB-tabellen {ACTIVE_FK_TABLE} finnes og inneholder
    FinnKoder fra nyeste daily-CSV. Returnerer True hvis tabellen er klar.
    """
    with _ACTIVE_FK_LOCK:
        try:
            s3 = _get_s3_client()
            obj_meta = _find_latest_daily_csv(s3)
            if obj_meta is None:
                print("[rekordrask] Fant ingen daily-CSV. Hopper over cross-check.")
                return False

            key = obj_meta["Key"]
            head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
            etag = (head.get("ETag") or "").strip('"')

            cache = _ACTIVE_FK_CACHE
            if (
                cache["s3_key"] == key
                and cache["etag"] == etag
                and cache["registered_in_duckdb"]
            ):
                return True

            print(f"[rekordrask] Laster daily-CSV for cross-check: {key}")
            resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            content = resp["Body"].read()

            import io as _io
            df = pd.read_csv(
                _io.BytesIO(content),
                encoding="utf-16",
                sep=";",
                dtype=str,
                usecols=lambda c: c.lower().replace("_", "").replace(" ", "")
                in ("finnkode", "finnid"),
            )
            if df.empty or df.shape[1] == 0:
                print(f"[rekordrask] Daily-CSV mangler FinnKode-kolonne: {key}")
                return False

            fk_col = df.columns[0]
            df["FinnKode"] = (
                df[fk_col].astype(str).str.extract(r"(\d+)", expand=False)
            )
            fk_df = df[["FinnKode"]].dropna().drop_duplicates()

            con = _duckdb_con()
            con.execute(f"DROP TABLE IF EXISTS {ACTIVE_FK_TABLE}")
            con.register("_fk_tmp", fk_df)
            con.execute(
                f"CREATE TABLE {ACTIVE_FK_TABLE} AS "
                f"SELECT CAST(FinnKode AS VARCHAR) AS FinnKode FROM _fk_tmp"
            )
            con.unregister("_fk_tmp")

            cache["s3_key"] = key
            cache["etag"] = etag
            cache["registered_in_duckdb"] = True

            print(f"[rekordrask] Registrerte {len(fk_df):,} aktive FinnKoder i DuckDB")
            return True

        except Exception as e:
            print(f"[rekordrask] Klarte ikke laste daily-CSV: {e}")
            traceback.print_exc()
            return False

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
        "fylke": pick(["Fylke", "fylke", "county"]),
        "sted": pick(["Sted", "sted", "location"]),
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
    Returnerer ALWAYS BOOLEAN for "ute av markedet"-flagget Solgt.
    Tåler bool, 0/1, og strenger som 'true'/'ja'/'solgt'/'fjernet'.
    'fjernet' og 'solgt' regnes som true: pipelinen markerer annonser som
    forsvinner fra FINN uten salgsbekreftelse med 'fjernet', og disse skal
    telle som "ikke lenger aktiv" på lik linje med 'ja'.
    """
    return f"""
    (
      case
        when {col_ident} is null then false
        when try_cast({col_ident} as BOOLEAN) is not null then try_cast({col_ident} as BOOLEAN)
        when lower(trim(cast({col_ident} as varchar))) in ('1','true','t','yes','y','ja','solgt','fjernet') then true
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
        {solgt_norm_expr} IN ('ja', 'true', '1', 'solgt', 'sold', 'fjernet', 'removed')
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
    CTE base for rekordrask med cross-check mot nyeste daily-CSV.
    Hvis en FinnKode finnes i siste daily, er bilen aktiv paa Finn,
    uansett hva Solgt-kolonnen sier. Fjerner ~0.4% falske positive.
    """
    c_dato = _qident(colmap.get("dato_start"))
    c_dato_ny = _qident(colmap.get("dato_end"))
    c_solgt = _qident(colmap.get("solgt"))
    c_finnkode = _qident(colmap.get("finnkode"))

    dato_ts = _to_timestamp_sql(c_dato)
    dato_ny_ts = _to_timestamp_sql(c_dato_ny)

    solgt_norm = _normalize_str_sql(c_solgt)
    is_solgt = _solgt_true_expr(solgt_norm)

    days_to_end = f"(date_diff('second', {dato_ts}, {dato_ny_ts}) / 86400.0)"

    # Cross-check mot aktive FinnKoder. Fail-open hvis tabellen ikke
    # kan bygges.
    has_active = _ensure_active_finnkoder_table()
    if has_active:
        not_aktiv_sql = (
            f"CAST({c_finnkode} AS VARCHAR) NOT IN ("
            f"  SELECT FinnKode FROM {ACTIVE_FK_TABLE}"
            f")"
        )
    else:
        not_aktiv_sql = "TRUE"

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
            AND ({not_aktiv_sql})
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
#              (scoring skjer i bil_kupp_analyse.py — peer-gruppe-basert)
# ==========================================================

BILRADAR_PARQUET_KEY = "calc/bil/bilradar_aktive.parquet"
BILRADAR_SISTE_PREFIX = "raw/bil-time/"
GOOD_DEAL_THRESHOLD = 10  # % rabatt for å regnes som godt kjøp

_BILRADAR_PARQUET_CACHE = {"etag": None, "df": None}
_BILRADAR_PARQUET_LOCK = threading.Lock()

BILRADAR_HTML_CACHE = {"alle": {"html": None, "etag": None},
                       "siste": {"html": None, "csv_key": None}}
BILRADAR_HTML_LOCK = threading.Lock()
_BILRADAR_MODEL_LOCK = threading.Lock()
_BILRADAR_MODEL_CACHE = {"modeller": None}
_BILRADAR_MODEL_LOCAL_PATH = os.path.join(os.path.dirname(__file__), "bil_prismodell_v2.joblib")
_BILRADAR_MODEL_S3_KEY = "calc/bil/bil_prismodell_v2.joblib"

def _get_bilradar_html_template() -> str:
    """Leser BilRadar HTML-template fra disk."""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "bil_radar.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def _lag_json_data_fra_parquet(df: pd.DataFrame) -> str:
    import json as _json
    today = date.today()
    col_map = {
        "FinnKode": "i", "Produsent": "m", "Modell": "mo",
        "Overskrift": "nf", "årstall": "a", "kjørelengde": "k",
        "girkasse": "g", "drivstoff": "d", "hjuldrift": "hj",
        "Karosseri": "ka", "Pris_ny": "p", "Pris": "pf",
        "selger": "s", "sted": "st", "fylke": "fy", "forhandler": "fh",
        "BildeURL": "im", "forventet_pris": "ep", "rabatt_pct": "r",
    }
    int_keys = {"i", "a", "k", "p", "pf", "ep"}
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
        # Dager bilen har vært til salgs (today - Dato). Beregnes ved cache-rebuild.
        dato = row.get("Dato")
        if pd.notna(dato):
            try:
                d = pd.Timestamp(dato).date()
                dm = (today - d).days
                if dm > 0:
                    car["dm"] = dm
            except Exception:
                pass
        if "p" not in car:
            car["p"] = 0
        if "r" not in car:
            car["r"] = 0
        cars.append(car)
    return _json.dumps(cars, ensure_ascii=False, separators=(",", ":"))


def _hent_bilradar_modell(s3):
    with _BILRADAR_MODEL_LOCK:
        if _BILRADAR_MODEL_CACHE["modeller"] is not None:
            return _BILRADAR_MODEL_CACHE["modeller"]
        modeller = last_modell_lokal_eller_s3(
            local_path=_BILRADAR_MODEL_LOCAL_PATH,
            s3_client=s3,
            bucket=S3_BUCKET_NAME,
            key=_BILRADAR_MODEL_S3_KEY,
        )
        _BILRADAR_MODEL_CACHE["modeller"] = modeller
        return modeller


def _normaliser_df_for_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliser kolonner slik at scorer_biler kan brukes på både CSV- og parquet-data."""
    out = df.copy()
    rename_map = {
        "fylke": "Fylke",
        "sted": "Sted",
        "selger": "Selger",
        "forhandler": "Forhandler",
        "girkasse": "Girkasse",
        "drivstoff": "Drivstoff",
        "hjuldrift": "Hjuldrift",
        "årstall": "Årstall",
        "kjørelengde": "Kjørelengde",
        "Pris_ny": "Pris",
    }
    for src, dst in rename_map.items():
        if src in out.columns and dst not in out.columns:
            out = out.rename(columns={src: dst})
    if "Pris" in out.columns:
        out["Pris"] = (
            out["Pris"]
            .astype(str)
            .str.replace(r"[^\d]", "", regex=True)
            .replace("", np.nan)
        )
    return out


def _score_manglende_biler(df: pd.DataFrame, s3) -> pd.DataFrame:
    """Kjør live-scoring for biler som mangler forventet_pris/rabatt_pct."""
    if df.empty:
        return df
    modeller = _hent_bilradar_modell(s3)
    df_norm = _normaliser_df_for_scoring(df)
    return scorer_biler(df_norm, modeller, threshold=GOOD_DEAL_THRESHOLD)


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
    import time as _time
    from flask import Response
    t0 = _time.perf_counter()
    try:
        s3 = _get_s3_client()
        latest_key = PARQUET_KEY_REKORDRASK
        head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=latest_key)
        latest_etag = (head.get("ETag") or "").strip('"')

        with BILRADAR_HTML_LOCK:
            cached = BILRADAR_HTML_CACHE["siste"].copy()
        if cached["html"] and cached["csv_key"] == latest_etag:
            print("[BilRadar/siste] Cache – serverer direkte")
            return Response(cached["html"], mimetype='text/html')

        print(f"[BilRadar/siste] Leser {latest_key}")
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=latest_key)
        df_siste = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        if "Solgt" in df_siste.columns:
            solgt_norm = (
                df_siste["Solgt"]
                .astype(str)
                .str.strip()
                .str.upper()
            )
            aktiv_mask = solgt_norm.isin(["NEI", "FALSE", "0", "NAN", "NONE", ""])
            if aktiv_mask.any():
                df_siste = df_siste[aktiv_mask].copy()
            else:
                print("[BilRadar/siste] Ingen kjente 'ikke-solgt'-verdier i Solgt-kolonnen, hopper over Solgt-filter")
        for col in ["forventet_pris", "rabatt_pct"]:
            if col not in df_siste.columns:
                df_siste[col] = np.nan
        if "modell_nivaa" not in df_siste.columns:
            # Skal inneholde tekstverdier ("Nivå 1", "Nivå 2", "Generell"), så bruk objekt-dtype.
            # Hindrer pandas FutureWarning ved senere df_siste.loc[..., "modell_nivaa"] = <str>.
            df_siste["modell_nivaa"] = pd.Series(pd.NA, index=df_siste.index, dtype="object")
        print(f"[BilRadar/siste] {len(df_siste)} biler i database_biler_siste")

        mangler_scoring_mask = df_siste["forventet_pris"].isna() | (pd.to_numeric(df_siste["forventet_pris"], errors="coerce") <= 0)
        antall_mangler_scoring = int(mangler_scoring_mask.sum())
        if antall_mangler_scoring > 0:
            print(f"[BilRadar/siste] Live-scoring av {antall_mangler_scoring} biler uten forventet pris")
            df_live = _score_manglende_biler(df_siste[mangler_scoring_mask].copy(), s3)
            oppdatert = df_live.set_index("FinnKode")[["forventet_pris", "rabatt_pct", "modell_nivaa"]]
            df_siste = df_siste.set_index("FinnKode")
            for col in ["forventet_pris", "rabatt_pct", "modell_nivaa"]:
                if col not in df_siste.columns:
                    if col == "modell_nivaa":
                        df_siste[col] = pd.Series(pd.NA, index=df_siste.index, dtype="object")
                    else:
                        df_siste[col] = np.nan
                if col == "modell_nivaa" and df_siste[col].dtype != "object":
                    df_siste[col] = df_siste[col].astype("object")
                df_siste.loc[oppdatert.index, col] = oppdatert[col]
            df_siste = df_siste.reset_index()

        df_scoret = df_siste[df_siste["forventet_pris"].notna() & (df_siste["forventet_pris"] > 0)].copy()
        if df_scoret.empty and "FinnKode" in df_siste.columns:
            print("[BilRadar/siste] 0 scorede biler i database_biler_siste, forsøker å hente scoring fra bilradar_aktive.parquet")
            df_aktive, _ = _les_parquet_aktive(s3)
            if "FinnKode" in df_aktive.columns:
                koder = set(pd.to_numeric(df_siste["FinnKode"], errors="coerce").dropna().astype("int64"))
                df_scoret = df_aktive[
                    df_aktive["FinnKode"].isin(koder)
                    & df_aktive["forventet_pris"].notna()
                    & (df_aktive["forventet_pris"] > 0)
                ].copy()
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
            BILRADAR_HTML_CACHE["siste"]["csv_key"] = latest_etag

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


@bil_bp.route('/innbytte', methods=['GET', 'POST'])
def bil_innbytte_side():
    """
    Foreslår innbyttepris ved å:
      1) hente kjøretøydata fra SVV (regnr)
      2) finne sammenlignbare biler i solgt-historikken
      3) beregne en robust prisindikasjon (median av mest sammenlignbare)
    """
    result = None
    error = None
    regnr = ""
    km_input = ""
    fra_dato_input = ""
    debug_context = {}
    svv_preview = None
    model_selection = None

    if request.method == "POST":
        regnr = (request.form.get("regnr") or "").strip().upper()
        km_input = (request.form.get("km") or "").strip()
        fra_dato_input = (request.form.get("fra_dato") or "").strip()
        selected_modell = (request.form.get("selected_modell") or "").strip()
        fra_dato = None
        if fra_dato_input:
            try:
                fra_dato = datetime.strptime(fra_dato_input, "%Y-%m-%d").date()
            except ValueError:
                error = "Fra dato må være gyldig dato (YYYY-MM-DD)."

        if not regnr:
            error = "Du må oppgi registreringsnummer."
        elif not km_input:
            error = "Du må oppgi kjørelengde."
        else:
            try:
                km_value = int(str(km_input).replace(" ", ""))
                if km_value < 0:
                    raise ValueError("negativ km")
            except Exception:
                error = "Kjørelengde må være et gyldig heltall (km)."
                km_value = None

            if not error:
                svv_raw, svv_err = fetch_svv_data(regnr)
                if svv_err or not svv_raw:
                    error = svv_err or "Fant ikke kjøretøydata fra SVV."
                else:
                    flat = flatten_svv_data(svv_raw)

                    merke = (flat.get("svv_merke") or "").strip()
                    modell = (flat.get("svv_handelsbetegnelse") or flat.get("svv_typebetegnelse") or "").strip()
                    drivstoff_svv = (flat.get("svv_drivstoff_navn") or "").strip()
                    reg_norge = (flat.get("svv_registrert_forste_gang_norge") or "")

                    target_year = None
                    if reg_norge and len(reg_norge) >= 4 and reg_norge[:4].isdigit():
                        target_year = int(reg_norge[:4])

                    aksler_med_drift = flat.get("svv_antall_aksler_med_drift")
                    hjuldrift_filter = None
                    if aksler_med_drift:
                        try:
                            amd = int(aksler_med_drift)
                        except Exception:
                            amd = None
                        if amd is not None:
                            if amd >= 2:
                                hjuldrift_filter = "firehjulsdrift (4x4)"
                            elif amd == 1:
                                hjuldrift_filter = "tohjulsdrift (2WD)"

                    svv_preview = {
                        "regnr": flat.get("svv_regnr") or regnr,
                        "merke": merke or None,
                        "modell": modell or "Ukjent modell",
                        "drivstoff": flat.get("svv_drivstoff_navn"),
                        "motor_cm3": flat.get("svv_slagvolum_cm3"),
                        "motor_kw": flat.get("svv_maks_netto_effekt_kw"),
                        "aksler_med_drift": flat.get("svv_antall_aksler_med_drift"),
                        "forstegang_norge": flat.get("svv_registrert_forste_gang_norge"),
                        "forstegang_utland": flat.get("svv_forstegang_reg_dato_utland"),
                        "bruktimportert": bool(flat.get("svv_bruktimportert")),
                        "importland": flat.get("svv_importland_navn") or flat.get("svv_importland_kode"),
                    }

                    debug_context = {
                        "merke": merke or None,
                        "modell": selected_modell or modell or None,
                        "år": target_year,
                        "drivstoff": drivstoff_svv or None,
                        "hjuldrift": hjuldrift_filter,
                        "km_sokt_til": km_value + 20000,
                        "datakilde": PARQUET_KEY_SOLGT,
                    }

                    if not merke:
                        error = "SVV-oppslaget manglet merke. Klarer ikke hente sammenlignbare biler."
                    else:
                        try:
                            import time as _innbytte_time
                            _t0 = _innbytte_time.time()
                            s3_key = PARQUET_KEY_SOLGT
                            path = _ensure_local_parquet(s3_key)
                            print(f"[innbytte] parquet ready in {_innbytte_time.time()-_t0:.2f}s ({regnr})")
                            _t1 = _innbytte_time.time()
                            colmap = _duckdb_get_colmap(path, s3_key)
                            con = _duckdb_con()
                            print(f"[innbytte] colmap+con in {_innbytte_time.time()-_t1:.2f}s")

                            def col_or_null(key: str) -> str:
                                c = colmap.get(key)
                                return _qident(c) if c else "NULL"

                            c_prod = col_or_null("produsent")
                            c_mod = col_or_null("modell")
                            c_aar = col_or_null("aar")
                            c_km = col_or_null("km")
                            c_hjul = col_or_null("hjuldrift")
                            c_rekkevidde = col_or_null("rekkevidde")
                            c_selger = col_or_null("selger")
                            c_pris_ny = col_or_null("pris_ny")
                            c_pris_start = col_or_null("pris_start")
                            c_dato_end = col_or_null("dato_end")
                            c_dato_start = col_or_null("dato_start")
                            c_solgt = col_or_null("solgt")
                            c_finn = col_or_null("finnkode")

                            pris_ny_num = f"coalesce(try_cast({c_pris_ny} AS BIGINT), 0)"
                            pris_start_num = f"try_cast({c_pris_start} AS BIGINT)"
                            km_num = _to_bigint_sql(c_km)
                            aar_num = f"try_cast({c_aar} AS BIGINT)"
                            dato_end_ts = _to_timestamp_sql(c_dato_end)
                            dato_start_ts = _to_timestamp_sql(c_dato_start)
                            finnkode_str = f"regexp_replace(cast({c_finn} as varchar), '\\\\.0$', '')"
                            finn_url_expr = f"CASE WHEN {c_finn} IS NULL THEN NULL ELSE '{FINN_BASE_URL}' || {finnkode_str} END"

                            def _normalize_model_text(text: str) -> str:
                                if not text:
                                    return ""
                                out = text.lower()
                                out = out.replace(merke.lower(), " ")
                                out = re.sub(r"[^a-z0-9]+", " ", out)
                                return re.sub(r"\s+", " ", out).strip()

                            def _expand_model_candidates(base_text: str):
                                model_text = (base_text or "").strip()
                                model_tokens = [t for t in re.split(r"\s+", model_text) if t]
                                candidates = []
                                if model_text:
                                    candidates.append(model_text)
                                if len(model_tokens) >= 2:
                                    candidates.append(str(" ".join(model_tokens[:2])))
                                elif len(model_tokens) == 1:
                                    candidates.append(str(model_tokens[0]))

                                expanded = []
                                for cand in candidates:
                                    expanded.append(cand)
                                    with_space = re.sub(r"(?i)([a-z])(\d)", r"\1 \2", cand)
                                    with_space = re.sub(r"(?i)(\d)([a-z])", r"\1 \2", with_space)
                                    with_dash = re.sub(r"(?i)([a-z])(\d)", r"\1-\2", cand)
                                    with_dash = re.sub(r"(?i)(\d)([a-z])", r"\1-\2", with_dash)
                                    expanded.extend([with_space, with_dash])

                                return list(dict.fromkeys([m.strip() for m in expanded if str(m).strip()]))

                            def _fetch_brand_models():
                                where_parts = [f"lower(cast({c_prod} as varchar)) = ?"]
                                params = [merke.lower()]
                                where_sql = " WHERE " + " AND ".join(where_parts)
                                models_sql = f"""
                                  SELECT cast({c_mod} as varchar) AS modell, count(*) AS antall
                                  FROM read_parquet('{path}')
                                  {where_sql}
                                  AND {c_mod} IS NOT NULL
                                  GROUP BY 1
                                  ORDER BY antall DESC, modell ASC
                                  LIMIT 200
                                """
                                return con.execute(models_sql, params).fetchall()

                            svv_model_clean = _normalize_model_text(modell)
                            modell_candidates = _expand_model_candidates(selected_modell or modell)

                            # Unngå tung max()-scan på hele parquet ved hvert oppslag.
                            # Bruk solgt-flagg når det finnes, ellers krev at dato_end er satt <= i dag.
                            c_driv = col_or_null("drivstoff")
                            km_upper_bound = km_value + 20000

                            try:
                                amd = int(aksler_med_drift) if aksler_med_drift is not None else None
                            except Exception:
                                amd = None

                            def run_comparable_query(include_hjuldrift: bool):
                                where_parts = [f"lower(cast({c_prod} as varchar)) = ?"]
                                params = [merke.lower()]

                                if modell_candidates:
                                    mod_like = " OR ".join([f"lower(cast({c_mod} as varchar)) LIKE ?" for _ in modell_candidates])
                                    where_parts.append(f"({mod_like})")
                                    params.extend([f"%{m.lower()}%" for m in modell_candidates])

                                # Årsmodell: samme år eller nyere
                                if target_year and colmap.get("aar"):
                                    where_parts.append(f"{aar_num} >= ?")
                                    params.append(target_year)

                                # Kjørelengde: lavere eller opptil 20 000 km mer enn innsendt km
                                if colmap.get("km"):
                                    where_parts.append(f"{km_num} IS NOT NULL")
                                    where_parts.append(f"{km_num} <= ?")
                                    params.append(km_upper_bound)

                                # Inkluder alle annonser (solgt, fjernet og aktive).
                                # Bruk kun en pris-sanity for å luke ut tomme/ugyldige rader.
                                where_parts.append(f"{pris_ny_num} > 1000")
                                if fra_dato and colmap.get("dato_end"):
                                    where_parts.append(f"date({dato_end_ts}) >= ?")
                                    params.append(fra_dato.isoformat())

                                if drivstoff_svv and colmap.get("drivstoff"):
                                    where_parts.append(f"lower(cast({c_driv} as varchar)) LIKE ?")
                                    params.append(f"%{drivstoff_svv.lower()}%")

                                if include_hjuldrift and amd is not None and colmap.get("hjuldrift"):
                                    if amd >= 2:
                                        where_parts.append(
                                            f"(lower(cast({c_hjul} as varchar)) LIKE '%fire%' OR lower(cast({c_hjul} as varchar)) LIKE '%4%')"
                                        )
                                    elif amd == 1:
                                        where_parts.append(
                                            f"(lower(cast({c_hjul} as varchar)) LIKE '%to%' OR lower(cast({c_hjul} as varchar)) LIKE '%2%')"
                                        )

                                where_sql = " WHERE " + " AND ".join(where_parts)
                                score_sql = f"""
                                  SELECT
                                    {c_prod} AS produsent,
                                    {c_mod} AS modell,
                                    {aar_num} AS arstall,
                                    {km_num} AS kjorelengde,
                                    cast({c_hjul} as varchar) AS hjuldrift,
                                    try_cast({c_rekkevidde} AS BIGINT) AS rekkevidde,
                                    cast({c_selger} as varchar) AS selger,
                                    {pris_ny_num} AS pris,
                                    {pris_start_num} AS pris_start,
                                    {dato_start_ts} AS dato_start,
                                    {dato_end_ts} AS dato_end,
                                    CASE
                                      WHEN {dato_start_ts} IS NULL THEN NULL
                                      WHEN {dato_end_ts} IS NOT NULL THEN datediff('day', date({dato_start_ts}), date({dato_end_ts}))
                                      ELSE datediff('day', date({dato_start_ts}), current_date)
                                    END AS dager_annonsert,
                                    CASE
                                      WHEN {pris_start_num} IS NULL OR {pris_ny_num} IS NULL THEN NULL
                                      ELSE {pris_start_num} - {pris_ny_num}
                                    END AS prisendring,
                                    {finnkode_str} AS finnkode,
                                    {finn_url_expr} AS finn_url,
                                    (
                                      coalesce(abs({km_num} - ?), 999999) * 1.0
                                      + coalesce(abs({aar_num} - ?), 4) * 20000.0
                                    ) AS score
                                  FROM read_parquet('{path}')
                                  {where_sql}
                                  ORDER BY score ASC, pris ASC
                                  LIMIT 40
                                """
                                # NB: De to første parameterne tilhører score-uttrykket.
                                score_params = [km_value, target_year or datetime.utcnow().year] + params
                                _tq = _innbytte_time.time()
                                df = con.execute(score_sql, score_params).df()
                                print(f"[innbytte] query (hjuldrift={include_hjuldrift}) {_innbytte_time.time()-_tq:.2f}s -> {len(df)} rader")
                                return df

                            debug_context["modellvarianter"] = modell_candidates[:8]
                            debug_context["filtre"] = [
                                "produsent=SVV merke",
                                "modell ~ en av modellvarianter",
                                "år >= førstegangsregistrert i Norge",
                                "km <= oppgitt km + 20 000",
                                "alle annonser (solgte, fjernede og aktive)",
                            ]
                            if fra_dato:
                                debug_context["filtre"].append(f"solgt/fjernet fra og med {fra_dato.isoformat()}")
                            if drivstoff_svv:
                                debug_context["filtre"].append("drivstoff matcher SVV")
                            if amd is not None and colmap.get("hjuldrift"):
                                debug_context["filtre"].append("hjuldrift matcher SVV (fallback: uten hjuldrift)")

                            rows = run_comparable_query(include_hjuldrift=True)
                            if rows.empty and amd is not None and colmap.get("hjuldrift"):
                                # Fallback: dropp hjuldrift-filter dersom første søk gir 0 treff.
                                rows = run_comparable_query(include_hjuldrift=False)
                                debug_context["fallback_brukt"] = True
                            else:
                                debug_context["fallback_brukt"] = False

                            if rows.empty and not selected_modell:
                                brand_models = _fetch_brand_models()
                                if brand_models:
                                    ranked = []
                                    for model_name, count in brand_models:
                                        candidate = (model_name or "").strip()
                                        if not candidate:
                                            continue
                                        norm_cand = _normalize_model_text(candidate)
                                        ratio = SequenceMatcher(None, svv_model_clean, norm_cand).ratio() if svv_model_clean else 0.0
                                        contains_bonus = 0.35 if svv_model_clean and svv_model_clean in norm_cand else 0.0
                                        token_overlap = 0.0
                                        if svv_model_clean and norm_cand:
                                            svv_tokens = set(svv_model_clean.split())
                                            cand_tokens = set(norm_cand.split())
                                            if svv_tokens and cand_tokens:
                                                token_overlap = len(svv_tokens & cand_tokens) / len(svv_tokens)
                                        score = ratio + contains_bonus + (token_overlap * 0.4)
                                        ranked.append((score, int(count or 0), candidate))

                                    ranked.sort(key=lambda x: (-x[0], -x[1], x[2]))
                                    suggested = [r[2] for r in ranked[:12]]
                                    all_models = [r[2] for r in ranked]
                                    model_selection = {
                                        "merke": merke,
                                        "svv_modell": modell or None,
                                        "suggested_models": suggested,
                                        "all_models": all_models,
                                    }
                                    debug_context["modellvalg_trengs"] = True
                                    debug_context["modeller_funnet_for_merke"] = len(all_models)
                                    error = (
                                        "Fant ingen treff på automatisk modellmatch. "
                                        "Velg modell manuelt for dette merket og beregn på nytt."
                                    )

                            rows = rows.where(pd.notna(rows), None)
                            records = json.loads(rows.to_json(orient='records', date_format='iso'))

                            if not records and not model_selection:
                                error = "Fant ingen gode sammenlignbare biler med dagens kriterier."
                            elif not model_selection:
                                topp = records[:10]
                                priser = [
                                    int(r["pris"])
                                    for r in topp
                                    if isinstance(r.get("pris"), (int, float)) and int(r.get("pris") or 0) > 0
                                ]
                                if not priser:
                                    error = "Fant sammenlignbare biler, men manglet prisgrunnlag."
                                else:
                                    prisserie = pd.Series(priser)
                                    median_pris = int(prisserie.median())
                                    p25_pris = int(prisserie.quantile(0.25))
                                    p75_pris = int(prisserie.quantile(0.75))
                                    minimum_salgspris = int(min(priser))
                                    median_minus_15 = int(round(median_pris * 0.85))
                                    innbyttepris = min(median_minus_15, minimum_salgspris)

                                    result = {
                                        "svv": {
                                            **(svv_preview or {}),
                                            "km_input": km_value,
                                        },
                                        "kriterier_brukt": debug_context,
                                        "innbyttepris": innbyttepris,
                                        "median_minus_15": median_minus_15,
                                        "laveste_salgspris": minimum_salgspris,
                                        "median_pris": median_pris,
                                        "pris_p25": p25_pris,
                                        "pris_p75": p75_pris,
                                        "antall_sammenlignbare": len(records),
                                        "sammenlignbare": records,
                                    }

                        except Exception as e:
                            traceback.print_exc()
                            error = f"Klarte ikke beregne innbyttepris: {e}"

    return render_template(
        "bil_innbytte.html",
        result=result,
        error=error,
        regnr=regnr,
        km=km_input,
        fra_dato=fra_dato_input,
        debug_context=debug_context,
        svv_preview=svv_preview,
        model_selection=model_selection,
    )


# ============================================================
# /bil/finn-sok – live FINN-søk + DB-berikelse + BilRadar-score
# ============================================================
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

_FINN_LISTING_TTL_SEC = 60
_FINN_LISTING_CACHE: dict = {}
_FINN_LISTING_CACHE_LOCK = threading.Lock()

_FINN_FUEL_MAP = {
    "Bensin": "1",
    "Diesel": "2",
    "Elektrisitet": "4",
    "Elektrisk": "4",
    "El": "4",
    "Hybrid": "6",
    "Gass": "3",
}

_FINN_FYLKE_LOCATION_MAP = {
    "agder": "0.20002",
    "akershus": "0.20003",
    "buskerud": "0.20004",
    "finnmark": "0.20006",
    "innlandet": "0.20034",
    "møre og romsdal": "0.20015",
    "nordland": "0.20018",
    "oslo": "0.20061",
    "rogaland": "0.20020",
    "telemark": "0.20039",
    "troms": "0.20060",
    "trøndelag": "0.20016",
    "vestfold": "0.20038",
    "vestland": "0.22046",
}


def _get_finn_sok_filter_options() -> dict:
    """Bygger dropdown-verdier fra samme parquet som /bil/solgt bruker."""
    out = {"merker": [], "modeller": [], "fylker": [], "drivstoff": [], "models_by_merke": {}}
    try:
        path = _ensure_local_parquet(PARQUET_KEY_SOLGT)
        colmap = _duckdb_get_colmap(path, PARQUET_KEY_SOLGT)
        con = _duckdb_con()

        def _opts_for(col_key: str, max_n: int = 1000) -> list:
            col = colmap.get(col_key)
            if not col:
                return []
            c = _qident(col)
            q = f"""
              SELECT DISTINCT trim(cast({c} AS VARCHAR)) AS v
              FROM read_parquet('{path}')
              WHERE {c} IS NOT NULL AND trim(cast({c} AS VARCHAR)) <> ''
              ORDER BY 1
              LIMIT {max_n}
            """
            return [r[0] for r in con.execute(q).fetchall() if r and r[0]]

        out["merker"] = _opts_for("produsent")
        out["modeller"] = _opts_for("modell")
        out["fylker"] = _opts_for("fylke")
        raw_drivstoff = _opts_for("drivstoff")
        canon_drivstoff = []
        for d in raw_drivstoff:
            d_norm = (d or "").strip().lower()
            if d_norm in ("elektrisk", "elektrisitet"):
                d_clean = "El"
            else:
                d_clean = (d or "").strip().title()
            if d_clean and d_clean not in canon_drivstoff:
                canon_drivstoff.append(d_clean)
        out["drivstoff"] = canon_drivstoff
        c_prod = _qident(colmap.get("produsent")) if colmap.get("produsent") else None
        c_mod = _qident(colmap.get("modell")) if colmap.get("modell") else None
        if c_prod and c_mod:
            pairs = con.execute(f"""
              SELECT trim(cast({c_prod} AS VARCHAR)) AS merke,
                     trim(cast({c_mod} AS VARCHAR))  AS modell
              FROM read_parquet('{path}')
              WHERE {c_prod} IS NOT NULL AND {c_mod} IS NOT NULL
                AND trim(cast({c_prod} AS VARCHAR)) <> ''
                AND trim(cast({c_mod} AS VARCHAR)) <> ''
              GROUP BY 1, 2
              ORDER BY 1, 2
            """).fetchall()
            by_merke = {}
            for merke, modell in pairs:
                by_merke.setdefault(merke, []).append(modell)
            out["models_by_merke"] = by_merke

        # Fallback: dersom "fylke"-kolonnen mangler/tom, utled fylke fra "sted"
        # (typisk format: "Sted, Fylke").
        if not out["fylker"]:
            c_sted = _qident(colmap.get("sted")) if colmap.get("sted") else None
            if c_sted:
                sted_rows = con.execute(f"""
                  SELECT DISTINCT trim(cast({c_sted} AS VARCHAR)) AS sted
                  FROM read_parquet('{path}')
                  WHERE {c_sted} IS NOT NULL AND trim(cast({c_sted} AS VARCHAR)) <> ''
                  LIMIT 5000
                """).fetchall()
                fylker = set()
                for row in sted_rows:
                    sted = (row[0] or "").strip() if row else ""
                    if "," in sted:
                        maybe_fylke = sted.split(",")[-1].strip()
                        if maybe_fylke:
                            fylker.add(maybe_fylke)
                out["fylker"] = sorted(fylker)
    except Exception as e:
        print(f"[finn_sok] Klarte ikke å bygge filtervalg fra parquet: {e}")
    return out


def _to_int_safe(v):
    try:
        if v is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        return int(float(v))
    except Exception:
        return None


def _normaliser_finn_sok_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if not raw.startswith("http"):
        raw = "https://www.finn.no" + ("" if raw.startswith("/") else "/") + raw
    raw = re.sub(r"[?&]page=\d+", "", raw)
    return raw


def _build_finn_sok_url(merke, modell, drivstoff, fylke, pris_min, pris_max,
                       km_min, km_max, ar_min, ar_max, q_extra) -> str:
    base = "https://www.finn.no/mobility/search/car"
    parts = []
    q_terms = [t.strip() for t in [merke, modell, q_extra] if t and str(t).strip()]
    if q_terms:
        parts.append(("q", " ".join(q_terms)))
    if drivstoff and drivstoff in _FINN_FUEL_MAP:
        parts.append(("fuel", _FINN_FUEL_MAP[drivstoff]))
    fylke_key = (fylke or "").strip().lower()
    if fylke_key in _FINN_FYLKE_LOCATION_MAP:
        parts.append(("location", _FINN_FYLKE_LOCATION_MAP[fylke_key]))
    for key, val in [("price_from", pris_min), ("price_to", pris_max),
                     ("mileage_from", km_min), ("mileage_to", km_max),
                     ("year_from", ar_min), ("year_to", ar_max)]:
        n = _to_int_safe(val)
        if n is not None:
            parts.append((key, str(n)))
    return f"{base}?{urlencode(parts)}" if parts else base


def _hent_finn_listing(finn_url: str, max_biler: int = 50) -> list:
    """TTL-cached fetch av FINN-trefflisten via eksisterende scraper."""
    now = datetime.now()
    with _FINN_LISTING_CACHE_LOCK:
        entry = _FINN_LISTING_CACHE.get(finn_url)
        if entry and (now - entry["ts"]).total_seconds() < _FINN_LISTING_TTL_SEC:
            return entry["data"]
    from bil_import import hent_annonser_fra_søk
    data = hent_annonser_fra_søk(finn_url, max_biler)
    with _FINN_LISTING_CACHE_LOCK:
        _FINN_LISTING_CACHE[finn_url] = {"ts": now, "data": data}
    return data


def _hent_db_for_finnkoder(finnkoder: list) -> pd.DataFrame:
    """Henter berikelse fra PARQUET_KEY_SOLGT for gitte FinnKoder."""
    fk_clean = [str(int(float(fk))) for fk in finnkoder
                if str(fk).replace(".", "", 1).isdigit()]
    if not fk_clean:
        return pd.DataFrame()

    s3_key = PARQUET_KEY_SOLGT
    local_path = _ensure_local_parquet(s3_key)
    colmap = _duckdb_get_colmap(local_path, s3_key)

    # Sjekk hvilke ekstra kolonner som finnes i parquet (BildeURL/Sted/Fylke
    # er ikke i den canonical colmap-en).
    con = _duckdb_con()
    actual_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{local_path}')"
    ).fetchall()]
    lower_actual = {c.lower(): c for c in actual_cols}

    def pick_extra(*cands):
        for c in cands:
            if c.lower() in lower_actual:
                return lower_actual[c.lower()]
        return None

    c_bilde = pick_extra("BildeURL", "bildeurl", "image_url", "bilde")
    c_sted  = pick_extra("sted", "Sted")
    c_fylke = pick_extra("fylke", "Fylke")

    c_fk     = _qident(colmap.get("finnkode"))
    c_dato   = _qident(colmap.get("dato_start"))
    c_dato2  = _qident(colmap.get("dato_end"))
    c_solgt  = _qident(colmap.get("solgt"))
    c_pris0  = _qident(colmap.get("pris_start"))
    c_pris1  = _qident(colmap.get("pris_ny"))
    c_aar    = _qident(colmap.get("aar"))
    c_km     = _qident(colmap.get("km"))
    c_rekk   = _qident(colmap.get("rekkevidde"))
    c_driv   = _qident(colmap.get("drivstoff"))
    c_hjul   = _qident(colmap.get("hjuldrift"))
    c_prod   = _qident(colmap.get("produsent"))
    c_modell = _qident(colmap.get("modell"))
    c_brukt  = _qident(colmap.get("bruktimport"))
    c_land   = _qident(colmap.get("import_land"))

    sel_bilde = f"{_qident(c_bilde)} AS BildeURL," if c_bilde else "NULL AS BildeURL,"
    sel_sted  = f"{_qident(c_sted)}  AS Sted,"     if c_sted  else "NULL AS Sted,"
    sel_fylke = f"{_qident(c_fylke)} AS Fylke,"    if c_fylke else "NULL AS Fylke,"

    fk_str_expr = f"regexp_replace(cast({c_fk} as varchar), '\\.0$', '')"
    fk_csv = ",".join(f"'{fk}'" for fk in fk_clean)

    sql = f"""
      SELECT
        {fk_str_expr}                              AS FinnKode,
        {c_prod}                                   AS Merke,
        {c_modell}                                 AS Modell,
        try_cast({c_aar} as INTEGER)               AS Årstall,
        try_cast({c_km}  as BIGINT)                AS Kjørelengde,
        try_cast({c_rekk} as INTEGER)              AS Rekkevidde,
        {c_driv}                                   AS Drivstoff,
        {c_hjul}                                   AS Hjuldrift,
        try_cast({c_pris0} as BIGINT)              AS Pris_forste,
        try_cast({c_pris1} as BIGINT)              AS Pris,
        {c_brukt}                                  AS svv_bruktimportert,
        {c_land}                                   AS svv_importland_navn,
        {sel_bilde}
        {sel_sted}
        {sel_fylke}
        date_diff(
          'day',
          cast({c_dato} as date),
          coalesce(cast({c_dato2} as date), current_date)
        )                                          AS dager_for_salg
      FROM read_parquet('{local_path}')
      WHERE {fk_str_expr} IN ({fk_csv})
        AND coalesce(try_cast({c_solgt} as boolean), false) = false
    """
    df = con.execute(sql).df()
    if "FinnKode" in df.columns:
        df = df.drop_duplicates(subset="FinnKode", keep="last")
    return df


def _parse_finn_detalj(html: str) -> dict:
    """Trekker ut tittel, bilde-URL, pris, km, år fra en FINN-annonse."""
    from bs4 import BeautifulSoup
    out: dict = {}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    # Tittel: prioriter og:title meta, fallback h1
    og = soup.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        out["title"] = og["content"].strip()
    else:
        h1 = soup.find("h1")
        if h1:
            out["title"] = h1.get_text(" ", strip=True)

    # Bilde-URL: og:image
    ogi = soup.find("meta", {"property": "og:image"})
    if ogi and ogi.get("content"):
        out["image_url"] = ogi["content"].strip()

    # Pris (Totalpris)
    p = soup.find("p", string="Totalpris")
    if p:
        h2 = p.find_next_sibling("h2")
        if h2:
            digits = "".join(filter(str.isdigit, h2.get_text()))
            if digits:
                out["pris"] = int(digits)

    # Spesifikasjoner: Kilometerstand, 1. gang registrert
    for header_tag in ("h2", "h3"):
        spec_header = soup.find(lambda t, ht=header_tag:
                                t.name == ht and "Spesifikasjoner" in t.get_text())
        if spec_header:
            dl = spec_header.find_next_sibling("dl")
            if dl:
                for dt in dl.find_all("dt"):
                    dd = dt.find_next_sibling("dd")
                    if not dd:
                        continue
                    key = dt.get_text(strip=True)
                    val = dd.get_text(strip=True)
                    if "Kilometerstand" in key:
                        d = "".join(filter(str.isdigit, val))
                        if d:
                            out["km"] = int(d)
                    elif "1. gang registrert" in key:
                        m = re.search(r"(\d{4})", val)
                        if m:
                            out["aar"] = int(m.group(1))
            break
    return out


def _hent_finn_detalj_for_ukjent(annonser_ukjent: list) -> dict:
    """Per-ad detail-fetch (parallell) for biler som mangler i DB.

    Henter tittel, bilde, pris, km og år fra hver FINN-annonse.
    """
    if not annonser_ukjent:
        return {}
    import requests as _requests
    from bil_import import FINN_HEADERS

    def _hent(a):
        try:
            r = _requests.get(a["url"], headers=FINN_HEADERS, timeout=10)
            r.raise_for_status()
            data = _parse_finn_detalj(r.text)
            data["finnkode"] = a["finnkode"]
            data["url"] = a["url"]
            return data
        except Exception:
            return {"finnkode": a["finnkode"], "url": a["url"]}

    out: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(_hent, a) for a in annonser_ukjent]
        for fut in as_completed(futs):
            try:
                res = fut.result()
                if res and res.get("finnkode"):
                    out[str(res["finnkode"])] = res
            except Exception:
                continue
    return out


@bil_bp.route('/finn-sok', methods=['GET'])
def bil_finn_sok():
    finn_url_raw = request.args.get("finn_url", "").strip()
    merke        = request.args.get("merke", "").strip()
    modell       = request.args.get("modell", "").strip()
    drivstoff    = request.args.get("drivstoff", "").strip()
    fylke_filter = request.args.get("fylke", "").strip()
    pris_min     = request.args.get("pris_min")
    pris_max     = request.args.get("pris_max")
    km_min       = request.args.get("km_min")
    km_max       = request.args.get("km_max")
    ar_min       = request.args.get("ar_min")
    ar_max       = request.args.get("ar_max")
    alder_min    = request.args.get("alder_min")
    alder_max    = request.args.get("alder_max")
    rabatt_min   = request.args.get("rabatt_min")
    rekkevidde_min = request.args.get("rekkevidde_min")
    rekkevidde_max = request.args.get("rekkevidde_max")
    q_extra      = request.args.get("q_extra", "").strip()
    sort_by      = request.args.get("sort", "rabatt_desc").strip()
    max_biler    = _to_int_safe(request.args.get("max_biler")) or 50
    max_biler    = max(10, min(max_biler, 200))
    filter_opts  = _get_finn_sok_filter_options()

    has_query = bool(finn_url_raw) or any([merke, modell, drivstoff, fylke_filter, pris_min, pris_max,
                                           km_min, km_max, ar_min, ar_max, alder_min, alder_max,
                                           rabatt_min, rekkevidde_min, rekkevidde_max, q_extra])
    if not has_query:
        return render_template("bil_finn_sok.html",
                               treff=None, finn_url="", form=request.args, filter_opts=filter_opts)

    finn_url = (_normaliser_finn_sok_url(finn_url_raw) if finn_url_raw
                else _build_finn_sok_url(merke, modell, drivstoff, fylke_filter, pris_min, pris_max,
                                         km_min, km_max, ar_min, ar_max, q_extra))

    try:
        annonser = _hent_finn_listing(finn_url, max_biler=max_biler)
    except Exception as e:
        traceback.print_exc()
        return render_template("bil_finn_sok.html",
                               treff=[], finn_url=finn_url, form=request.args, filter_opts=filter_opts,
                               error=f"Kunne ikke lese FINN: {e}")

    if not annonser:
        return render_template("bil_finn_sok.html",
                               treff=[], finn_url=finn_url, form=request.args, filter_opts=filter_opts,
                               melding="Ingen treff på FINN.")

    finnkoder = [str(a["finnkode"]) for a in annonser]
    df_db = _hent_db_for_finnkoder(finnkoder)
    matched_set = set(df_db["FinnKode"].astype(str)) if not df_db.empty else set()

    annonser_ukjent = [a for a in annonser if str(a["finnkode"]) not in matched_set]
    detalj_ukjent = _hent_finn_detalj_for_ukjent(annonser_ukjent)

    rows = []
    db_by_fk = ({str(r["FinnKode"]): r for _, r in df_db.iterrows()}
                if not df_db.empty else {})
    for a in annonser:
        fk = str(a["finnkode"])
        if fk in matched_set:
            r = dict(db_by_fk[fk])
            r["finnkode"] = fk
            r["url"] = a["url"]
            r["i_db"] = True
            r["title"] = None
            rows.append(r)
        else:
            d = detalj_ukjent.get(fk, {})
            title = d.get("title")
            merke_guess, modell_guess = None, None
            if title:
                parts = title.split(None, 1)
                merke_guess = parts[0] if parts else None
                modell_guess = parts[1] if len(parts) > 1 else None
            rows.append({
                "FinnKode": fk, "finnkode": fk, "url": a["url"],
                "Merke": merke_guess, "Modell": modell_guess,
                "Årstall": d.get("aar"), "Kjørelengde": d.get("km"),
                "Drivstoff": None, "Hjuldrift": None,
                "Pris_forste": d.get("pris"),  # ny → førstpris = dagens pris
                "Pris": d.get("pris"),
                "svv_bruktimportert": None,
                "svv_importland_navn": None,
                "BildeURL": d.get("image_url"),
                "Sted": None, "Fylke": None,
                "dager_for_salg": 1,           # ny → ca. 1 dag
                "i_db": False,
                "title": title,
            })

    # BilRadar-score: forsøk scoring av ALLE biler (både i DB og ikke i DB).
    score_map: dict = {}
    if rows:
        try:
            df_score = pd.DataFrame(rows)
            modeller = _hent_bilradar_modell(_get_s3_client())
            df_scored = scorer_biler(df_score, modeller)
            for _, sc_row in df_scored.iterrows():
                fk = str(sc_row.get("FinnKode"))
                score_map[fk] = {
                    "forventet_pris": sc_row.get("forventet_pris"),
                    "rabatt_pct": sc_row.get("rabatt_pct"),
                    "modell_nivaa": sc_row.get("modell_nivaa"),
                }
        except Exception as e:
            print(f"[finn_sok] BilRadar-scoring feilet: {e}")
            traceback.print_exc()

    treff = []
    innev_ar = datetime.now().year
    alder_min_i = _to_int_safe(alder_min)
    alder_max_i = _to_int_safe(alder_max)
    rabatt_min_i = _to_int_safe(rabatt_min)
    rekkevidde_min_i = _to_int_safe(rekkevidde_min)
    rekkevidde_max_i = _to_int_safe(rekkevidde_max)
    fylke_filter_lc = fylke_filter.lower()
    for r in rows:
        fk = str(r["finnkode"])
        sc = score_map.get(fk, {})
        rab = sc.get("rabatt_pct")
        rab_val = None
        if rab is not None and not (isinstance(rab, float) and np.isnan(rab)):
            try:
                rab_val = round(float(rab), 1)
            except Exception:
                rab_val = None
        bi = r.get("svv_bruktimportert")
        if bi is not None and not (isinstance(bi, float) and np.isnan(bi)):
            try:
                bi_bool = bool(int(bi)) if str(bi).strip() in ("0", "1") else \
                          str(bi).strip().lower() in ("true", "ja", "sann", "yes", "y", "t", "1")
            except Exception:
                bi_bool = bool(bi)
        else:
            bi_bool = None
        sted_parts = [p for p in [r.get("Sted"), r.get("Fylke")] if p]
        sted = ", ".join(sted_parts) if sted_parts else None
        treff.append({
            "finnkode": fk,
            "url": r["url"],
            "merke": r.get("Merke"),
            "modell": r.get("Modell"),
            "title": r.get("title"),
            "aar": _to_int_safe(r.get("Årstall")),
            "km": _to_int_safe(r.get("Kjørelengde")),
            "drivstoff": r.get("Drivstoff"),
            "hjuldrift": r.get("Hjuldrift"),
            "pris": _to_int_safe(r.get("Pris")),
            "forstpris": _to_int_safe(r.get("Pris_forste")),
            "dager_for_salg": _to_int_safe(r.get("dager_for_salg")) or 1,
            "rekkevidde": _to_int_safe(r.get("Rekkevidde")),
            "bruktimport": bi_bool,
            "importland": r.get("svv_importland_navn"),
            "image_url": r.get("BildeURL"),
            "sted": sted,
            "forventet_pris": _to_int_safe(sc.get("forventet_pris")),
            "rabatt_pct": rab_val,
            "modell_nivaa": sc.get("modell_nivaa"),
            "i_db": r.get("i_db", False),
        })

    if fylke_filter_lc:
        # Ikke kast biler som mangler sted/fylke i berikelsen (typisk ny/ukjent i DB),
        # ellers blir trefflisten kunstig lav sammenlignet med FINN.
        treff = [
            t for t in treff
            if (not t.get("sted")) or (fylke_filter_lc in str(t.get("sted", "")).lower())
        ]

    if alder_min_i is not None or alder_max_i is not None:
        filtrert = []
        for t in treff:
            if t.get("aar") is None:
                continue
            alder = innev_ar - int(t["aar"])
            if alder_min_i is not None and alder < alder_min_i:
                continue
            if alder_max_i is not None and alder > alder_max_i:
                continue
            filtrert.append(t)
        treff = filtrert

    if rabatt_min_i is not None:
        treff = [
            t for t in treff
            if t.get("rabatt_pct") is not None and float(t.get("rabatt_pct")) >= float(rabatt_min_i)
        ]

    if drivstoff.lower() in ("elektrisitet", "el", "elektrisk") and (rekkevidde_min_i is not None or rekkevidde_max_i is not None):
        filtrert = []
        for t in treff:
            rv = _to_int_safe(t.get("rekkevidde"))
            if rv is None:
                continue
            if rekkevidde_min_i is not None and rv < rekkevidde_min_i:
                continue
            if rekkevidde_max_i is not None and rv > rekkevidde_max_i:
                continue
            filtrert.append(t)
        treff = filtrert

    def _sort_num(v, fallback):
        return fallback if v is None else v

    if sort_by == "pris_asc":
        treff.sort(key=lambda t: _sort_num(t.get("pris"), float("inf")))
    elif sort_by == "pris_desc":
        treff.sort(key=lambda t: _sort_num(t.get("pris"), -1), reverse=True)
    elif sort_by == "sist_lagt_ut":
        treff.sort(key=lambda t: _sort_num(t.get("dager_for_salg"), float("inf")))
    elif sort_by == "km_asc":
        treff.sort(key=lambda t: _sort_num(t.get("km"), float("inf")))
    elif sort_by == "km_desc":
        treff.sort(key=lambda t: _sort_num(t.get("km"), -1), reverse=True)
    elif sort_by == "aar_desc":
        treff.sort(key=lambda t: _sort_num(t.get("aar"), -1), reverse=True)
    elif sort_by == "aar_asc":
        treff.sort(key=lambda t: _sort_num(t.get("aar"), float("inf")))
    else:  # rabatt_desc default
        treff.sort(key=lambda t: _sort_num(t.get("rabatt_pct"), -999), reverse=True)

    return render_template("bil_finn_sok.html",
                           treff=treff, finn_url=finn_url, form=request.args, filter_opts=filter_opts)
