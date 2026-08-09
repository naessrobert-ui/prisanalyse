# bil_routes.py (DuckDB + Parquet fra S3 via lokal /tmp-cache, per-file cache)
import json
import io
import os
import re
import tempfile
import threading
import time
from difflib import SequenceMatcher
from datetime import datetime, timedelta, date

import boto3
import pandas as pd
import numpy as np
import duckdb

from flask import Blueprint, render_template, jsonify, request
import traceback

from bilradar_scorer import scorer_biler, LOOKUP_LOCAL_PATH
from bilradar_lookup import last_lookup

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

# Hvor mange av de nyeste daily-CSV-ene som slaas sammen til "aktiv"-settet.
# En bil regnes som fortsatt aktiv (og dermed IKKE rekordsolgt) hvis den er sett
# i minst en av de siste N daglige kjoringene. Union over flere dager gjoer at en
# enkelt manglende/ufullstendig scraping ikke feilaktig "gjenoppliver" en aktiv
# bil som solgt.
ACTIVE_FK_DAILY_LOOKBACK = 3


def _find_recent_daily_csvs(s3, n: int = 1) -> list[dict]:
    """Finn S3-objektene for de {n} nyeste daily-CSV-ene (nyeste foerst)."""
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME, Prefix="raw/bil-daglig/"
    ):
        for obj in page.get("Contents", []):
            if not obj["Key"].lower().endswith(".csv"):
                continue
            if obj.get("Size", 0) < 1024:
                continue
            objs.append(obj)
    objs.sort(key=lambda o: o["LastModified"], reverse=True)
    return objs[: max(1, n)]


def _find_latest_daily_csv(s3) -> dict | None:
    """Finn S3-objektet for nyeste daily-CSV."""
    recent = _find_recent_daily_csvs(s3, 1)
    return recent[0] if recent else None


def _finnkode_norm_sql(col_ident: str) -> str:
    """
    Normaliser en FinnKode-kolonne til rene siffer i SQL.

    Viktig: parquet lagrer ofte FinnKode som flyttall, slik at
    ``CAST(FinnKode AS VARCHAR)`` gir ``'123456.0'``. Cross-check-tabellen
    inneholder rene siffer (``'123456'``), saa uten normalisering matcher
    ingenting og cross-checken blir en stille no-op. regexp_extract henter
    foerste siffergruppe og speiler pandas ``str.extract(r"(\\d+)")`` som
    brukes naar aktiv-tabellen bygges.
    """
    return f"regexp_extract(cast({col_ident} as varchar), '[0-9]+')"


def _ensure_active_finnkoder_table() -> bool:
    """
    Sikrer at DuckDB-tabellen {ACTIVE_FK_TABLE} finnes og inneholder
    FinnKoder fra de siste {ACTIVE_FK_DAILY_LOOKBACK} daily-CSV-ene (union).
    Returnerer True hvis tabellen er klar.
    """
    with _ACTIVE_FK_LOCK:
        try:
            s3 = _get_s3_client()
            recent = _find_recent_daily_csvs(s3, ACTIVE_FK_DAILY_LOOKBACK)
            if not recent:
                print("[rekordrask] Fant ingen daily-CSV. Hopper over cross-check.")
                return False

            # Cache-signatur bygges paa nyeste fil (key+etag). Endres nyeste
            # daily, bygges hele unionen paa nytt.
            key = recent[0]["Key"]
            head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
            etag = (head.get("ETag") or "").strip('"')

            cache = _ACTIVE_FK_CACHE
            if (
                cache["s3_key"] == key
                and cache["etag"] == etag
                and cache["registered_in_duckdb"]
            ):
                return True

            import io as _io

            fk_frames = []
            for obj_meta in recent:
                k = obj_meta["Key"]
                try:
                    resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=k)
                    content = resp["Body"].read()
                    df = pd.read_csv(
                        _io.BytesIO(content),
                        encoding="utf-16",
                        sep=";",
                        dtype=str,
                        usecols=lambda c: c.lower().replace("_", "").replace(" ", "")
                        in ("finnkode", "finnid"),
                    )
                    if df.empty or df.shape[1] == 0:
                        print(f"[rekordrask] Daily-CSV mangler FinnKode-kolonne: {k}")
                        continue
                    fk_col = df.columns[0]
                    fk = df[fk_col].astype(str).str.extract(r"(\d+)", expand=False)
                    fk_frames.append(fk.dropna())
                except Exception as e:
                    # Fail-open per fil: en daarlig daily skal ikke velte hele
                    # cross-checken saa lenge minst en fil er lesbar.
                    print(f"[rekordrask] Klarte ikke lese daily-CSV {k}: {e}")
                    continue

            if not fk_frames:
                print("[rekordrask] Ingen lesbare daily-CSV-er for cross-check.")
                return False

            fk_df = (
                pd.concat(fk_frames, ignore_index=True)
                .rename("FinnKode")
                .to_frame()
                .drop_duplicates()
            )

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

            print(
                f"[rekordrask] Registrerte {len(fk_df):,} aktive FinnKoder i DuckDB "
                f"(union av {len(fk_frames)} daily-fil(er))"
            )
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


def _truthy(v) -> bool:
    """Tolker JSON/skjema-verdier som boolean (tåler bool og strenger)."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "ja", "on", "yes")




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
        sel_col = _qident(colmap["selger"]) if colmap.get("selger") else "NULL"
        driv = _qident(colmap["drivstoff"]) if colmap.get("drivstoff") else None
        aar = _qident(colmap["aar"]) if colmap.get("aar") else None

        # Selger-bevisst: forsvunne forhandler-annonser (FJERNET) teller ikke som solgt.
        where_parts = [f"{_solgt_bool_seller_aware(solgt_col, sel_col)} = true"]
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
        c_selger = col_or_null("selger")
        c_pris_ny = col_or_null("pris_ny")
        dato_end_ts = f"try_cast({c_dato_end} AS TIMESTAMP)"
        pris_ny_num = f"coalesce(try_cast({c_pris_ny} AS BIGINT), 0)"

        # Biler uten gyldig pris (Pris_ny mangler/0 - typisk annonser som viste
        # "Solgt" eller hadde tom pris) skal ikke telle med i solgt-analysen:
        # verken i lista, KPI-ene (snitt/median/laveste) eller antall.
        gyldig_pris_sql = f" AND {pris_ny_num} > 0" if colmap.get("pris_ny") else ""

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
                    status_sql += f" AND ({_solgt_bool_seller_aware(c_solgt, c_selger)}) = false"
            elif status == "solgt_fjernet":
                status_sql = f" AND date({dato_end_ts}) < ?"
                status_params.append(str(max_date))

        # Ekskluder biler uten gyldig pris fra count/grouped/liste.
        status_sql += gyldig_pris_sql

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
        c_over = col_or_null("overskrift")
        c_pris_start = col_or_null("pris_start")
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

        solgt_expr = _solgt_bool_seller_aware(c_solgt, c_selger) if colmap.get("solgt") else None

        solgt_filter_sql = ""
        solgt_filter_params = []
        if solgt_expr:
            solgt_filter_sql = f" AND ({solgt_expr}) = true"
        else:
            solgt_filter_sql = f" AND ({pris_ny_num}) > 1000"

        # Ekskluder biler uten gyldig pris fra KPI-ene (snitt/median/laveste)
        # og daily_stats, i tråd med lista/telling.
        solgt_filter_sql += gyldig_pris_sql
        solgt_filter_sql += exclude_maxdate_sql
        solgt_filter_params.extend(exclude_maxdate_params)

        kpi_sql = f"""
          SELECT
            CAST(avg({dager_expr}) AS BIGINT) AS avg_dager,
            CAST(median({dager_expr}) AS BIGINT) AS median_dager,
            CAST(avg({pris_ny_num}) AS BIGINT) AS avg_pris,
            CAST(median({pris_ny_num}) AS BIGINT) AS median_pris,
            CAST(min({pris_ny_num}) FILTER (WHERE {pris_ny_num} > 0) AS BIGINT) AS laveste_pris,
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
          -- Biler uten gyldig pris (Pris_ny mangler -> coalesce 0) skal IKKE
          -- regnes som "billigst". Sorter dem sist, ekte priser stigende foerst.
          ORDER BY ({pris_ny_num} <= 0), {pris_ny_num} ASC
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


def _privat_selger_expr(selger_ident: str) -> str:
    """
    SANN for private selgere. Speiler klassifiser_selgertype() i
    bil_nye_annonser.py: Selger tom / NULL / whitespace / 'nan'/'none'/'<na>'
    => privat; en hvilken som helst annen verdi (forhandler-/bedriftsnavn)
    => forhandler/bedrift.

    NB: forhandler_type-kolonnen er tom i dataene og ubrukelig som kilde -
    Selger-navnet er det paalitelige signalet paa selger-type.
    """
    if selger_ident == "NULL":
        # Ingen selger-kolonne i datasettet -> kan ikke skille, anta privat
        # (bevarer gammel adferd der FJERNET talte som solgt for alle).
        return "TRUE"
    norm = f"lower(trim(cast({selger_ident} as varchar)))"
    return f"({selger_ident} IS NULL OR {norm} IN ('', 'nan', 'none', '<na>'))"


def _solgt_true_seller_aware_expr(solgt_norm_expr: str, selger_ident: str) -> str:
    """
    Som _solgt_true_expr, men skiller mellom bekreftet salg og forsvunnet annonse:

      - 'ja'/'solgt'/'sold'/'true'/'1'  => bekreftet solgt (uansett selger-type).
      - 'fjernet'/'removed'             => annonse forsvunnet. Regnes som solgt
                                           KUN for private selgere. Forhandlere
                                           re-publiserer stadig annonser, saa en
                                           forsvunnet forhandler-annonse er ikke
                                           paalitelig 'solgt' - den ville ellers
                                           feilaktig telle som "solgt rekordraskt"
                                           selv om bilen fortsatt er til salgs.

    Speiler den gamle CSV-logikken (rekordrask_logic.py): "Forsvunnet + PRIVAT
    => solgt. Forsvunnet + forhandler => IKKE solgt".
    """
    er_privat = _privat_selger_expr(selger_ident)
    return f"""
      (
        {solgt_norm_expr} IN ('ja', 'true', '1', 'solgt', 'sold')
        OR (
          {solgt_norm_expr} IN ('fjernet', 'removed')
          AND {er_privat}
        )
      )
    """


def _solgt_bool_seller_aware(col_ident: str, selger_ident: str) -> str:
    """
    BOOLEAN "er bilen solgt", selger-bevisst. Som _bool_expr for Solgt-kolonnen,
    men 'fjernet' (annonse forsvunnet) regnes som solgt KUN for private selgere.
    Forhandlere re-publiserer annonser uten salg, saa en forsvunnet forhandler-
    annonse er ikke paalitelig solgt. Bekreftet salg ('ja'/'solgt' m.fl.) teller
    uansett selger-type.

    Fail-open: mangler selger-kolonnen faller vi tilbake til _bool_expr
    (gammel adferd der 'fjernet' talte som solgt for alle).
    """
    if selger_ident == "NULL":
        return _bool_expr(col_ident)
    norm = f"lower(trim(cast({col_ident} as varchar)))"
    er_privat = _privat_selger_expr(selger_ident)
    return f"""
    (
      case
        when {col_ident} is null then false
        when try_cast({col_ident} as BOOLEAN) is not null then try_cast({col_ident} as BOOLEAN)
        when {norm} in ('1','true','t','yes','y','ja','solgt','sold') then true
        when {norm} in ('fjernet','removed') then ({er_privat})
        else false
      end
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
    c_sel  = colmap.get("selger")
    c_fylke = colmap.get("fylke")
    c_sted = colmap.get("sted")

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

    # Selger-type (privat / forhandler). Speiler _privat_selger_expr():
    # tom/NULL Selger => privat, ellers forhandler/bedrift.
    selger_type = (filters.get("selger_type") or "Alle").strip().lower()
    if c_sel and selger_type in ("privat", "private"):
        clauses.append(_privat_selger_expr(_qident(c_sel)))
    elif c_sel and selger_type in ("forhandler", "bedrift", "dealer", "forhandler/bedrift"):
        clauses.append(f"NOT ({_privat_selger_expr(_qident(c_sel))})")

    # Fylke. Bruker Fylke-kolonnen når den finnes, og faller tilbake til å
    # utlede fra Sted ("Sted, Fylke"). Matcher begge når begge finnes, så
    # filteret virker uansett hvor fylket faktisk er lagret.
    fylke = (filters.get("fylke") or "Alle").strip()
    if fylke and fylke != "Alle":
        fylke_parts = []
        if c_fylke:
            fylke_parts.append(
                f"lower(trim(cast({_qident(c_fylke)} as varchar))) = lower(trim(?))"
            )
            params.append(fylke)
        if c_sted:
            fylke_parts.append(
                f"lower(trim(cast({_qident(c_sted)} as varchar))) LIKE '%, ' || lower(trim(?))"
            )
            params.append(fylke)
        if fylke_parts:
            clauses.append("(" + " OR ".join(fylke_parts) + ")")

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params

def _rekordrask_base_sql(path: str, colmap: dict, where_sql: str,
                         inkluder_fjernet_forhandler: bool = False):
    """
    CTE base for rekordrask med cross-check mot nyeste daily-CSV.
    Hvis en FinnKode finnes i siste daily, er bilen aktiv paa Finn,
    uansett hva Solgt-kolonnen sier. Fjerner ~0.4% falske positive.

    inkluder_fjernet_forhandler:
      False (default) -> selger-bevisst: 'fjernet' teller som solgt KUN for
                         private. Forsvunne forhandler-annonser teller ikke.
      True            -> 'fjernet'/'removed' teller som solgt for ALLE selgere,
                         inkludert forhandlere.
    """
    c_dato = _qident(colmap.get("dato_start"))
    c_dato_ny = _qident(colmap.get("dato_end"))
    c_solgt = _qident(colmap.get("solgt"))
    c_finnkode = _qident(colmap.get("finnkode"))
    c_selger = _qident(colmap.get("selger"))

    dato_ts = _to_timestamp_sql(c_dato)
    dato_ny_ts = _to_timestamp_sql(c_dato_ny)

    # "solgt"-tolkning. Bekreftet salg ('ja'/'solgt') teller alltid. Forskjellen
    # gjelder forsvunne annonser ('fjernet'/'removed'):
    #   - selger-bevisst (default): teller kun for private, fordi forhandlere
    #     re-publiserer annonser stadig -> forsvunnet forhandler-annonse er ikke
    #     paalitelig solgt.
    #   - inkluder_fjernet_forhandler=True: teller ogsaa forsvunne forhandler-
    #     annonser som solgt.
    solgt_norm = _normalize_str_sql(c_solgt)
    if inkluder_fjernet_forhandler:
        is_solgt = _solgt_true_expr(solgt_norm)
    else:
        is_solgt = _solgt_true_seller_aware_expr(solgt_norm, c_selger)

    days_to_end = f"(date_diff('second', {dato_ts}, {dato_ny_ts}) / 86400.0)"

    # Cross-check mot aktive FinnKoder. Fail-open hvis tabellen ikke
    # kan bygges.
    # NB: FinnKode normaliseres til rene siffer paa begge sider. Parquet
    # lagrer ofte FinnKode som flyttall ('123456.0'), mens aktiv-tabellen
    # har rene siffer ('123456'). Uten normalisering matcher ingenting og
    # cross-checken blir en stille no-op som slipper gjennom biler som
    # fortsatt er aktive paa Finn.
    has_active = _ensure_active_finnkoder_table()
    if has_active:
        finnkode_norm = _finnkode_norm_sql(c_finnkode)
        not_aktiv_sql = (
            f"{finnkode_norm} NOT IN ("
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


def _get_fylke_options() -> list:
    """
    Distinkte fylker fra samme parquet som rekordrask bruker. Faller tilbake
    til å utlede fylke fra 'Sted' ("Sted, Fylke") hvis Fylke-kolonnen mangler
    eller er tom. Speiler fylke-logikken i _get_finn_sok_filter_options().
    """
    try:
        path = _ensure_local_parquet(PARQUET_KEY_SOLGT)
        colmap = _duckdb_get_colmap(path, PARQUET_KEY_SOLGT)
        con = _duckdb_con()

        c_fylke = colmap.get("fylke")
        if c_fylke:
            c = _qident(c_fylke)
            rows = con.execute(f"""
              SELECT DISTINCT trim(cast({c} AS VARCHAR)) AS v
              FROM read_parquet('{path}')
              WHERE {c} IS NOT NULL AND trim(cast({c} AS VARCHAR)) <> ''
              ORDER BY 1
              LIMIT 1000
            """).fetchall()
            fylker = [r[0] for r in rows if r and r[0]]
            if fylker:
                return fylker

        # Fallback: utled fra 'Sted' ("Sted, Fylke")
        c_sted = colmap.get("sted")
        if c_sted:
            c = _qident(c_sted)
            rows = con.execute(f"""
              SELECT DISTINCT trim(cast({c} AS VARCHAR)) AS sted
              FROM read_parquet('{path}')
              WHERE {c} IS NOT NULL AND trim(cast({c} AS VARCHAR)) <> ''
              LIMIT 5000
            """).fetchall()
            fylker = set()
            for row in rows:
                sted = (row[0] or "").strip() if row else ""
                if "," in sted:
                    maybe = sted.split(",")[-1].strip()
                    if maybe:
                        fylker.add(maybe)
            return sorted(fylker)
    except Exception as e:
        print(f"[rekordrask] Klarte ikke bygge fylke-valg: {e}")
    return []


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
        fylker=_get_fylke_options(),
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
        inkluder_fjernet_forhandler = _truthy(filters.get("inkluder_fjernet_forhandler"))

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

        base = _rekordrask_base_sql(path, colmap, where_sql,
                                    inkluder_fjernet_forhandler=inkluder_fjernet_forhandler)

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
        inkluder_fjernet_forhandler = _truthy(filters.get("inkluder_fjernet_forhandler"))

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        where_sql, params = _rekordrask_where(filters, colmap)
        group_cols_sql, group_cols_names = _rekordrask_group_cols(filters, colmap)

        base = _rekordrask_base_sql(path, colmap, where_sql,
                                    inkluder_fjernet_forhandler=inkluder_fjernet_forhandler)

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
# OPPSLAG (debug) – all info vi har om én FinnKode
#
#   GET  /bil/oppslag         -> siden (input for FinnKode)
#   POST /bil/oppslag/data     -> hele parquet-raden + tolkning
#
# Bygget for å jakte på "rusk": hvorfor havner en bil som solgt/rekordsolgt?
# ==========================================================

@bil_bp.route('/oppslag')
def bil_oppslag_side():
    return render_template(
        'bil_oppslag.html',
        tittel="Bil-oppslag (FinnKode)",
    )


@bil_bp.route('/oppslag/data', methods=['POST'])
def bil_oppslag_data():
    try:
        payload = request.get_json() or {}
        raw_input = str(payload.get("finnkode") or "").strip()
        # Trekk ut rene siffer (tåler hele Finn-URL-er også)
        m = re.search(r"(\d{3,})", raw_input)
        if not m:
            return jsonify({"status": "error", "message": "Oppgi et gyldig FinnKode-nummer."}), 400
        finnkode = m.group(1)

        s3_key = PARQUET_KEY_SOLGT
        path = _ensure_local_parquet(s3_key)
        colmap = _duckdb_get_colmap(path, s3_key)
        con = _duckdb_con()

        c_finn = colmap.get("finnkode")
        if not c_finn:
            return jsonify({"status": "error", "message": "Datasettet mangler FinnKode-kolonne."}), 500

        finn_norm = _finnkode_norm_sql(_qident(c_finn))

        # Hele raden (alle kolonner)
        row_df = con.execute(
            f"SELECT * FROM read_parquet('{path}') WHERE {finn_norm} = ? LIMIT 5",
            [finnkode],
        ).df()

        if row_df.empty:
            return jsonify({
                "status": "ok",
                "finnkode": finnkode,
                "funnet": False,
                "finn_url": FINN_BASE_URL + finnkode,
                "message": "Fant ingen rad for denne FinnKoden i database_biler.parquet.",
            })

        row_df = row_df.where(pd.notna(row_df), None)
        # Hele raden som ordnet key/value (første treff)
        alle_felt = json.loads(row_df.head(1).to_json(orient="records"))[0]

        # ---- Tolkning ----
        c_solgt = _qident(colmap.get("solgt")) if colmap.get("solgt") else "NULL"
        c_selger = _qident(colmap.get("selger")) if colmap.get("selger") else "NULL"
        c_dato = _qident(colmap.get("dato_start")) if colmap.get("dato_start") else "NULL"
        c_dato_ny = _qident(colmap.get("dato_end")) if colmap.get("dato_end") else "NULL"

        solgt_norm = _normalize_str_sql(c_solgt)
        is_solgt_expr = _solgt_true_seller_aware_expr(solgt_norm, c_selger)
        er_privat_expr = _privat_selger_expr(c_selger)
        dato_ts = _to_timestamp_sql(c_dato)
        dato_ny_ts = _to_timestamp_sql(c_dato_ny)
        days_expr = f"(date_diff('second', {dato_ts}, {dato_ny_ts}) / 86400.0)"

        # Aktiv i siste daglige snapshots (samme kilde som rekordrask-cross-check)
        has_active = _ensure_active_finnkoder_table()
        if has_active:
            aktiv_expr = (
                f"CASE WHEN {finn_norm} IN (SELECT FinnKode FROM {ACTIVE_FK_TABLE}) "
                f"THEN true ELSE false END"
            )
        else:
            aktiv_expr = "NULL"

        tolk_sql = f"""
          SELECT
            {aktiv_expr} AS aktiv_i_siste_daily,
            {er_privat_expr} AS er_privat_selger,
            ({is_solgt_expr}) AS regnes_som_solgt,
            {days_expr} AS dager_dato_til_dato_ny,
            CAST({dato_ts} AS VARCHAR) AS dato_parsed,
            CAST({dato_ny_ts} AS VARCHAR) AS dato_ny_parsed
          FROM read_parquet('{path}')
          WHERE {finn_norm} = ?
          LIMIT 1
        """
        t = con.execute(tolk_sql, [finnkode]).df()
        t = t.where(pd.notna(t), None)
        tolk = json.loads(t.to_json(orient="records"))[0] if not t.empty else {}

        aktiv = bool(tolk.get("aktiv_i_siste_daily")) if tolk.get("aktiv_i_siste_daily") is not None else None
        regnes_solgt = bool(tolk.get("regnes_som_solgt"))
        dager = tolk.get("dager_dato_til_dato_ny")
        # Rekordsolgt (gjeldende logikk): solgt + ikke aktiv + gyldige datoer + dager <= maks (3)
        MAKS_DAGER_DEFAULT = 3
        regnes_rekordsolgt = bool(
            regnes_solgt
            and aktiv is False
            and dager is not None
            and 0 <= dager <= MAKS_DAGER_DEFAULT
        )

        tolkning = {
            "aktiv_i_siste_daily": aktiv,
            "er_privat_selger": bool(tolk.get("er_privat_selger")) if tolk.get("er_privat_selger") is not None else None,
            "solgt_rå": alle_felt.get(colmap.get("solgt")) if colmap.get("solgt") else None,
            "selger": alle_felt.get(colmap.get("selger")) if colmap.get("selger") else None,
            "regnes_som_solgt": regnes_solgt,
            "regnes_som_rekordsolgt": regnes_rekordsolgt,
            "maks_dager_brukt": MAKS_DAGER_DEFAULT,
            "dager_dato_til_dato_ny": round(dager, 3) if isinstance(dager, (int, float)) else None,
            "dato_parsed": tolk.get("dato_parsed"),
            "dato_ny_parsed": tolk.get("dato_ny_parsed"),
        }

        # Kort forklaring på hvorfor
        forklaring = []
        if aktiv:
            forklaring.append("Bilen ligger i siste daglige snapshot → tolkes som fortsatt aktiv på Finn (skal ikke telle som solgt/rekordsolgt).")
        else:
            forklaring.append("Bilen ligger IKKE i siste daglige snapshot (borte fra Finn eller ikke fanget av scraper).")
        if tolkning["er_privat_selger"] is True:
            forklaring.append("Selger tolkes som PRIVAT → 'FJERNET' regnes som solgt.")
        elif tolkning["er_privat_selger"] is False:
            forklaring.append("Selger tolkes som FORHANDLER → 'FJERNET' regnes IKKE som solgt (kun bekreftet 'JA').")

        return jsonify({
            "status": "ok",
            "finnkode": finnkode,
            "funnet": True,
            "finn_url": FINN_BASE_URL + finnkode,
            "flere_rader": len(row_df) > 1,
            "tolkning": tolkning,
            "forklaring": forklaring,
            "alle_felt": alle_felt,
        })

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
RADAR_SISTE_TIMER = 24  # /radar/siste viser kun biler først sett innen så mange timer

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
    today = date.today()
    col_map = {
        "FinnKode": "i", "Produsent": "m", "Modell": "mo",
        "Overskrift": "nf", "årstall": "a", "kjørelengde": "k",
        "girkasse": "g", "drivstoff": "d", "hjuldrift": "hj",
        "Karosseri": "ka", "Pris_ny": "p", "Pris": "pf",
        "selger": "s", "sted": "st", "fylke": "fy", "forhandler": "fh",
        "BildeURL": "im", "forventet_pris": "ep", "rabatt_pct": "r",
        "hurtigpris": "hp", "innbyttepris": "ib",
    }
    int_keys = {"i", "a", "k", "p", "pf", "ep", "hp", "ib"}
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


def start_bilradar_warmup():
    """Varm opp pris-tabellene (lookup/variant + peer-WLS) i en bakgrunnstråd
    ved app-oppstart, slik at scorer_biler er rask når første
    /bil/finn-sok-request kommer.

    Merk: den tunge ML-modellen (~150 MB) lastes ikke lenger — all scoring
    bruker lookup + peer-WLS (modeller=None), og /radar leser ferdig-scoret
    parquet."""
    def _warmup():
        # Varmer opp de lette tabellene scorer_biler bruker (lookup/variant +
        # peer-WLS). Den tunge ML-modellen lastes IKKE lenger — /finn-sok scorer
        # med modeller=None (lookup + peer), og /radar leser ferdig-scoret parquet.
        try:
            print("[BilRadar/warmup] Varmer opp pris-tabeller i bakgrunn ...")
            t0 = time.time()
            s3 = _get_s3_client()
            try:
                last_lookup(local_path=LOOKUP_LOCAL_PATH, s3_client=s3, bucket=S3_BUCKET_NAME)
                print("[BilRadar/warmup] Lookup-tabell klar")
            except Exception as exc:
                print(f"[BilRadar/warmup] Lookup feilet ({exc!r}) — bruker lokal/ingen")
            try:
                from bilradar_peer import last_peer_koef, PEER_LOCAL_PATH
                last_peer_koef(local_path=PEER_LOCAL_PATH, s3_client=s3, bucket=S3_BUCKET_NAME)
                print("[BilRadar/warmup] Peer-koeffisienter klare")
            except Exception as exc:
                print(f"[BilRadar/warmup] Peer-koeff feilet ({exc!r}) — bruker lokal/ingen")
            print(f"[BilRadar/warmup] Ferdig etter {time.time() - t0:.1f}s")
        except Exception as exc:
            # Logg, men ikke krasje appen — lazy load vil fortsatt funke.
            print(f"[BilRadar/warmup] Feilet ({exc!r}) — faller tilbake til lazy load")

    threading.Thread(target=_warmup, name="bilradar-warmup", daemon=True).start()


@bil_bp.route('/reload', methods=['POST', 'GET'])
def bil_reload():
    """Tøm cachene for lookup-tabell (+ variantkatalog) og re-last fra S3, slik
    at ny verdsettelse slår inn uten ny deploy. Kjøres av cron/GitHub Action
    etter at ny prislookup.csv er lastet opp til S3."""
    from bilradar_lookup import reload_lookup

    reloaded = {"lookup": True}
    reload_lookup()
    try:
        from bil_variant_klassifiserer import reload_variantkatalog
        reload_variantkatalog()
        reloaded["variantkatalog"] = True
    except Exception:
        reloaded["variantkatalog"] = False

    try:
        s3 = _get_s3_client()
        lookup = last_lookup(local_path=LOOKUP_LOCAL_PATH, s3_client=s3, bucket=S3_BUCKET_NAME)
        return jsonify({"ok": True, "reloaded": reloaded, "lookup_grupper": int(len(lookup))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
        for col in ["forventet_pris", "hurtigpris", "rabatt_pct"]:
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


def _filtrer_ferske_biler(df, timer=RADAR_SISTE_TIMER, naa=None):
    """Behold kun rader der bilen er *ny det siste døgnet* — dvs. første gang
    observert (kolonnen ``Dato``) innen de siste ``timer`` timene.

    Ren funksjon (ingen S3/Flask) slik at den er enkel å teste. Rader uten
    gyldig ``Dato`` droppes: vi kan da ikke vite at bilen er fersk. Mangler
    ``Dato``-kolonnen helt, returneres df uendret (med en advarsel) framfor å
    tømme siden på grunn av et dataproblem.
    """
    if df is None or df.empty:
        return df
    if "Dato" not in df.columns:
        print(
            "[BilRadar/siste] ADVARSEL: mangler Dato-kolonne — "
            "kan ikke begrense til siste døgn, viser alle scorede biler"
        )
        return df

    dato_sett = pd.to_datetime(df["Dato"], errors="coerce")
    # Datoene lagres tidssone-naivt (konsolider_data bruker naive datetime).
    # Skulle de likevel være tz-bevisste, fjern tz så sammenligningen mot en
    # naiv "nå" ikke feiler.
    if getattr(dato_sett.dtype, "tz", None) is not None:
        dato_sett = dato_sett.dt.tz_localize(None)

    if naa is None:
        naa = pd.Timestamp.now()
    grense = naa - pd.Timedelta(hours=timer)
    fersk_mask = dato_sett.notna() & (dato_sett >= grense)
    return df[fersk_mask].copy()


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

        # Cache-nøkkelen inkluderer en time-bøtte i tillegg til etag, slik at
        # ferskhetsfilteret (Dato innen siste RADAR_SISTE_TIMER timer) regnes på
        # nytt minst hver time – ellers ville en cachet side vist biler som har
        # falt ut av 24-timers-vinduet så lenge parquet-filen var uendret.
        time_bucket = int(pd.Timestamp.now().timestamp() // 3600)
        cache_key = f"{latest_etag}:{time_bucket}"

        with BILRADAR_HTML_LOCK:
            cached = BILRADAR_HTML_CACHE["siste"].copy()
        if cached["html"] and cached["csv_key"] == cache_key:
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
        recent_koder = set()
        if "FinnKode" in df_siste.columns:
            recent_koder = set(
                pd.to_numeric(df_siste["FinnKode"], errors="coerce").dropna().astype("int64")
            )
        print(f"[BilRadar/siste] {len(df_siste)} biler i database_biler_siste")

        # Scorene hentes fra den ferdig-scorede bilradar_aktive.parquet (skrevet
        # av scoring-jobben: lookup/variant + peer-WLS). Vi live-scorer IKKE her
        # lenger — det lastet den tunge ML-modellen (~150 MB) på selve
        # web-requesten og kunne henge i minutter → timeout. Ferske biler dukker
        # opp så snart neste scoring-kjøring (timevis / ved innlegging fra Pi)
        # har tatt dem med. Samme motor/tall som /radar/alle.
        df_aktive, _ = _les_parquet_aktive(s3)
        if "FinnKode" in df_aktive.columns and recent_koder:
            df_scoret = df_aktive[
                df_aktive["FinnKode"].isin(recent_koder)
                & df_aktive["forventet_pris"].notna()
                & (df_aktive["forventet_pris"] > 0)
            ].copy()
        else:
            df_scoret = df_aktive.iloc[0:0].copy()
        print(f"[BilRadar/siste] {len(df_scoret)}/{len(df_siste)} biler med scoring (fra bilradar_aktive)")

        # Ferskhetsfilter: "Siste døgn" skal KUN vise biler som er nye det siste
        # døgnet, dvs. første gang observert (Dato) innen de siste
        # RADAR_SISTE_TIMER timene. Uten dette listet siden alle aktive annonser
        # (samme utvalg som /radar/alle), stikk i strid med lovnaden på
        # velgersiden: "Scorer kun nye biler fra siste 24 timer".
        antall_for = len(df_scoret)
        df_scoret = _filtrer_ferske_biler(df_scoret, RADAR_SISTE_TIMER)
        print(
            f"[BilRadar/siste] Ferskhetsfilter (siste {RADAR_SISTE_TIMER} t): "
            f"{len(df_scoret)}/{antall_for} biler"
        )

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
            BILRADAR_HTML_CACHE["siste"]["csv_key"] = cache_key

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
    auto_submit = False

    if request.method == "GET":
        q_regnr = (request.args.get("regnr") or "").strip().upper()
        q_km_raw = request.args.get("km") or request.args.get("kjorelengde") or ""
        q_km = str(q_km_raw).strip().replace(" ", "")
        if q_regnr:
            regnr = q_regnr
        if q_km:
            km_input = q_km
        auto_submit = bool(q_regnr and q_km and q_km.isdigit())

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
                            s3_key = PARQUET_KEY_SOLGT
                            path = _ensure_local_parquet(s3_key)
                            colmap = _duckdb_get_colmap(path, s3_key)
                            con = _duckdb_con()

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

                            solgt_norm_expr = _normalize_str_sql(c_solgt)
                            solgt_true_expr = _solgt_true_expr(solgt_norm_expr) if colmap.get("solgt") else None

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
                                if colmap.get("solgt"):
                                    where_parts.append(solgt_true_expr)
                                elif colmap.get("dato_end"):
                                    where_parts.append(f"{dato_end_ts} IS NOT NULL")
                                    where_parts.append(f"date({dato_end_ts}) <= current_date")
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

                                if colmap.get("solgt"):
                                    where_parts.append(solgt_true_expr)
                                else:
                                    if colmap.get("dato_end"):
                                        where_parts.append(f"{dato_end_ts} IS NOT NULL")
                                        where_parts.append(f"date({dato_end_ts}) <= current_date")
                                    else:
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
                                return con.execute(score_sql, score_params).df()

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

                            # Variant-bevisst filtrering for elbil: hold de
                            # sammenlignbare bilene til samme batteripakke/
                            # utstyrsvariant som målbilen når vi klarer å
                            # klassifisere. Uendret for bensin/diesel (ingen
                            # katalog → ingen variant → ingen filtrering).
                            if records:
                                try:
                                    from bil_variant_klassifiserer import (
                                        klassifiser_varianter,
                                        last_variantkatalog,
                                    )
                                    katalog = last_variantkatalog()
                                    svv_rekkevidde = _to_int_safe(
                                        flat.get("svv_elektrisk_rekkevidde_km")
                                    )
                                    mal_df = pd.DataFrame([{
                                        "Produsent": merke,
                                        "Modell": selected_modell or modell,
                                        "årstall": target_year,
                                        "rekkevidde_km": svv_rekkevidde,
                                    }])
                                    mal_vid = klassifiser_varianter(mal_df, katalog)[0].iloc[0]
                                    if mal_vid:
                                        comp_df = pd.DataFrame([{
                                            "Produsent": r.get("produsent"),
                                            "Modell": r.get("modell"),
                                            "årstall": r.get("arstall"),
                                            "rekkevidde_km": r.get("rekkevidde"),
                                        } for r in records])
                                        comp_vid = klassifiser_varianter(comp_df, katalog)[0].tolist()
                                        same = [rec for rec, v in zip(records, comp_vid) if v == mal_vid]
                                        debug_context["variant_id"] = mal_vid
                                        debug_context["antall_for_variant"] = len(records)
                                        debug_context["antall_etter_variant"] = len(same)
                                        # Krev nok igjen til at medianen er meningsfull;
                                        # ellers behold alle (bedre bredt enn tomt).
                                        if len(same) >= 3:
                                            records = same
                                        else:
                                            debug_context["variant_for_faa_treff"] = True
                                except Exception:
                                    traceback.print_exc()

                            if not records and not model_selection:
                                error = "Fant ingen gode sammenlignbare biler med dagens kriterier."
                            elif not model_selection:
                                priser = []
                                for r in records:
                                    raw = r.get("pris")
                                    if raw is None:
                                        continue
                                    try:
                                        val = int(raw)
                                    except (TypeError, ValueError):
                                        continue
                                    if val > 0:
                                        priser.append(val)
                                if not priser:
                                    error = "Fant sammenlignbare biler, men manglet prisgrunnlag."
                                else:
                                    prisserie = pd.Series(priser)
                                    median_pris = int(prisserie.median())
                                    p25_pris = int(prisserie.quantile(0.25))
                                    p75_pris = int(prisserie.quantile(0.75))
                                    minimum_salgspris = int(min(priser))

                                    # Hurtigpris = markedsklarende pris: median
                                    # blant biler solgt innen 3 dager. For få
                                    # hurtigsalg → bruk p25 som proxy. Dette
                                    # erstatter den outlier-sårbare "billigste
                                    # enkeltbil" som gulv for innbyttet.
                                    fast_priser = []
                                    for r in records:
                                        d = r.get("dager_annonsert")
                                        pr = r.get("pris")
                                        if d is None or pr is None:
                                            continue
                                        try:
                                            if int(d) <= 3 and int(pr) > 0:
                                                fast_priser.append(int(pr))
                                        except (TypeError, ValueError):
                                            continue
                                    if len(fast_priser) >= 3:
                                        hurtigpris = int(pd.Series(fast_priser).median())
                                    else:
                                        hurtigpris = p25_pris
                                    hurtigpris = min(hurtigpris, median_pris)

                                    median_minus_15 = int(round(median_pris * 0.85))
                                    innbyttepris = min(median_minus_15, hurtigpris)

                                    # Sorter visningen stigende på pris (default).
                                    records_sorted = sorted(
                                        records,
                                        key=lambda r: (r.get("pris") if isinstance(r.get("pris"), (int, float)) else float("inf"))
                                    )

                                    result = {
                                        "svv": {
                                            **(svv_preview or {}),
                                            "km_input": km_value,
                                        },
                                        "kriterier_brukt": debug_context,
                                        "innbyttepris": innbyttepris,
                                        "median_minus_15": median_minus_15,
                                        "hurtigpris": hurtigpris,
                                        "laveste_salgspris": minimum_salgspris,
                                        "median_pris": median_pris,
                                        "pris_p25": p25_pris,
                                        "pris_p75": p75_pris,
                                        "antall_sammenlignbare": len(records),
                                        "sammenlignbare": records_sorted,
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
        auto_submit=auto_submit,
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

# Mapper skjemaverdier (fra dropdown / URL-parameter) til den kanoniske
# strengen prismodellen ble trent på. Frontend normaliserer "elektrisk"/
# "elektrisitet" til "El"; her mappes det tilbake slik at segmentnøkler
# som "Opel | Corsa | Elektrisitet" matcher i FlipModels.market_l1.
_DRIVSTOFF_FORM_TIL_MODELL = {
    "el": "Elektrisitet",
    "elektrisk": "Elektrisitet",
    "elektrisitet": "Elektrisitet",
    "bensin": "Bensin",
    "diesel": "Diesel",
    "hybrid": "Hybrid",
    "gass": "Gass",
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
                       km_min, km_max, ar_min, ar_max, q_extra,
                       bare_brukt=True, published_last_day=False,
                       hjuldrift="") -> str:
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
    if hjuldrift == "firehjul":
        parts.append(("wheel_drive", "2"))
    elif hjuldrift == "tohjul":
        # FINN tar både forhjul (1) og bakhjul (3) som to separate parametre
        parts.append(("wheel_drive", "1"))
        parts.append(("wheel_drive", "3"))
    if bare_brukt:
        parts.append(("sales_form", "1"))
    if published_last_day:
        parts.append(("published", "1"))
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
    c_over   = colmap.get("overskrift")
    c_brukt  = _qident(colmap.get("bruktimport"))
    c_land   = _qident(colmap.get("import_land"))

    sel_bilde = f"{_qident(c_bilde)} AS BildeURL," if c_bilde else "NULL AS BildeURL,"
    sel_sted  = f"{_qident(c_sted)}  AS Sted,"     if c_sted  else "NULL AS Sted,"
    sel_fylke = f"{_qident(c_fylke)} AS Fylke,"    if c_fylke else "NULL AS Fylke,"
    sel_over  = f"{_qident(c_over)} AS Overskrift," if c_over else "NULL AS Overskrift,"

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
        {sel_over}
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
    """Trekker ut metadata fra en FINN-bilannonse.

    Returnerer: title, image_url, pris, km, aar, drivstoff, hjuldrift,
    karosseri, girkasse, rekkevidde_km, garanti_mnd. Manglende felter
    utelates fra dict-en. Brukes for "NY – IKKE I DB"-rader slik at
    BilRadar-scoreren får et komplett feature-vektor i stedet for å
    måtte gjette med Hjuldrift/Karosseri="Ukjent" og rekkevidde=0.
    """
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

    # Spesifikasjoner: hent alle dt/dd-par i seksjonen
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
                    val = dd.get_text(" ", strip=True)
                    val_clean = val.strip()
                    if not val_clean:
                        continue
                    if "Kilometerstand" in key:
                        d = "".join(filter(str.isdigit, val))
                        if d:
                            out["km"] = int(d)
                    elif "1. gang registrert" in key:
                        m = re.search(r"(\d{4})", val)
                        if m:
                            out["aar"] = int(m.group(1))
                    elif key == "Drivstoff":
                        out["drivstoff"] = val_clean
                    elif key == "Hjuldrift":
                        out["hjuldrift"] = val_clean
                    elif key == "Karosseri":
                        out["karosseri"] = val_clean
                    elif key == "Girkasse":
                        out["girkasse"] = val_clean
                    elif "Rekkevidde" in key:
                        # Plukk første tall — verdier som "385 km (WLTP)"
                        m = re.search(r"(\d{2,4})", val)
                        if m:
                            out["rekkevidde_km"] = int(m.group(1))
                    elif key.startswith("Garanti"):
                        # Verdier som "24 mnd", "60 mnd" eller "Inntil 5 år"
                        m_aar = re.search(r"(\d+)\s*[åa]r", val.lower())
                        m_mnd = re.search(r"(\d+)\s*mnd", val.lower())
                        if m_mnd:
                            out["garanti_mnd"] = int(m_mnd.group(1))
                        elif m_aar:
                            out["garanti_mnd"] = int(m_aar.group(1)) * 12
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
    hjuldrift_filter = request.args.get("hjuldrift_filter", "").strip()
    q_extra      = request.args.get("q_extra", "").strip()
    sort_by      = request.args.get("sort", "rabatt_desc").strip()
    max_biler    = _to_int_safe(request.args.get("max_biler")) or 500
    max_biler    = max(10, min(max_biler, 500))
    # Hidden marker som skiller "skjema sendt" (avkrysningsbokser respekteres
    # som angitt) fra "førstegangsbesøk / direkte URL" (defaults gjelder).
    filter_set = request.args.get("filter_set") == "1"
    if filter_set:
        bare_brukt = request.args.get("bare_brukt") == "1"
        published_last_day = request.args.get("published_last_day") == "1"
    else:
        bare_brukt = True
        published_last_day = False
    filter_opts  = _get_finn_sok_filter_options()

    has_query = bool(finn_url_raw) or any([merke, modell, drivstoff, fylke_filter, pris_min, pris_max,
                                           km_min, km_max, ar_min, ar_max, alder_min, alder_max,
                                           rabatt_min, rekkevidde_min, rekkevidde_max, hjuldrift_filter,
                                           q_extra])
    if not has_query:
        return render_template("bil_finn_sok.html",
                               treff=None, finn_url="", form=request.args, filter_opts=filter_opts,
                               bare_brukt=bare_brukt, published_last_day=published_last_day)

    finn_url = (_normaliser_finn_sok_url(finn_url_raw) if finn_url_raw
                else _build_finn_sok_url(merke, modell, drivstoff, fylke_filter, pris_min, pris_max,
                                         km_min, km_max, ar_min, ar_max, q_extra,
                                         bare_brukt=bare_brukt,
                                         published_last_day=published_last_day,
                                         hjuldrift=hjuldrift_filter))

    try:
        annonser = _hent_finn_listing(finn_url, max_biler=max_biler)
    except Exception as e:
        traceback.print_exc()
        return render_template("bil_finn_sok.html",
                               treff=[], finn_url=finn_url, form=request.args, filter_opts=filter_opts,
                               bare_brukt=bare_brukt, published_last_day=published_last_day,
                               error=f"Kunne ikke lese FINN: {e}")

    if not annonser:
        return render_template("bil_finn_sok.html",
                               treff=[], finn_url=finn_url, form=request.args, filter_opts=filter_opts,
                               bare_brukt=bare_brukt, published_last_day=published_last_day,
                               melding="Ingen treff på FINN.")

    finnkoder = [str(a["finnkode"]) for a in annonser]
    df_db = _hent_db_for_finnkoder(finnkoder)
    matched_set = set(df_db["FinnKode"].astype(str)) if not df_db.empty else set()

    annonser_ukjent = [a for a in annonser if str(a["finnkode"]) not in matched_set]
    detalj_ukjent = _hent_finn_detalj_for_ukjent(annonser_ukjent)

    # Filterverdier brukes som fallback når FINN-detaljen ikke gir oss
    # drivstoff/fylke (f.eks. på dårlig parsede annonser).
    drivstoff_filter_fb = _DRIVSTOFF_FORM_TIL_MODELL.get(drivstoff.lower()) if drivstoff else None
    fylke_filter_fb = fylke_filter.strip() if fylke_filter else None

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
            over = r.get("Overskrift")
            if over is not None and (isinstance(over, float) and np.isnan(over)):
                over = None
            r["title"] = (str(over).strip() or None) if over else None
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
                "Drivstoff": d.get("drivstoff") or drivstoff_filter_fb,
                "Hjuldrift": d.get("hjuldrift"),
                "Karosseri": d.get("karosseri"),
                "Girkasse": d.get("girkasse"),
                "rekkevidde_km": d.get("rekkevidde_km"),
                "Garanti": d.get("garanti_mnd"),
                "Pris_forste": d.get("pris"),  # ny → førstpris = dagens pris
                "Pris": d.get("pris"),
                "svv_bruktimportert": None,
                "svv_importland_navn": None,
                "BildeURL": d.get("image_url"),
                "Sted": None, "Fylke": fylke_filter_fb,
                "dager_for_salg": 1,           # ny → ca. 1 dag
                "i_db": False,
                "title": title,
            })

    # BilRadar-score: gjenbruk ferdige scorer fra bilradar_aktive.parquet der vi
    # har bilen (raskt, og samme tall som /radar), og live-score kun resten.
    # Rabatt regnes mot den LIVE FINN-prisen (den kan ha endret seg siden batch).
    score_map: dict = {}
    if rows:
        want = {str(r["finnkode"]) for r in rows}
        pris_by_fk = {str(r["finnkode"]): _to_int_safe(r.get("Pris")) for r in rows}

        # 1) Ferdige scorer fra bilradar_aktive.parquet
        aktive_scores: dict = {}
        try:
            df_aktive, _ = _les_parquet_aktive(_get_s3_client())
            if df_aktive is not None and not df_aktive.empty and "FinnKode" in df_aktive.columns:
                a = df_aktive.copy()
                a["_fk"] = a["FinnKode"].astype(str).str.replace(r"\.0$", "", regex=True)
                a = a[a["_fk"].isin(want)].drop_duplicates("_fk", keep="last")
                for _, ar in a.iterrows():
                    fk = ar["_fk"]
                    forv = ar.get("forventet_pris")
                    live_pris = pris_by_fk.get(fk)
                    rab = None
                    try:
                        fv = float(forv)
                        if fv > 0 and live_pris and live_pris > 0:
                            rab = round((fv - live_pris) / fv * 100, 1)
                    except (TypeError, ValueError):
                        pass
                    aktive_scores[fk] = {
                        "forventet_pris": forv,
                        "hurtigpris": ar.get("hurtigpris"),
                        "innbyttepris": ar.get("innbyttepris"),
                        "rabatt_pct": rab,
                        "modell_nivaa": ar.get("modell_nivaa"),
                    }
        except Exception as e:
            print(f"[finn_sok] Klarte ikke hente ferdige scorer fra bilradar_aktive: {e}")
        score_map.update(aktive_scores)

        # 2) Live-score kun biler uten ferdig score (nye/ukjente annonser)
        rows_to_score = [r for r in rows if str(r["finnkode"]) not in aktive_scores]
        if rows_to_score:
            try:
                df_score = pd.DataFrame(rows_to_score)
                # Uten ML-modell (modeller=None): lookup/variant primær +
                # peer-WLS fallback. Slipper å laste den ~150 MB store modellen
                # på web-requesten — samme motor som /radar.
                df_scored = scorer_biler(df_score, modeller=None)
                for _, sc_row in df_scored.iterrows():
                    fk = str(sc_row.get("FinnKode"))
                    score_map[fk] = {
                        "forventet_pris": sc_row.get("forventet_pris"),
                        "hurtigpris": sc_row.get("hurtigpris"),
                        "innbyttepris": sc_row.get("innbyttepris"),
                        "rabatt_pct": sc_row.get("rabatt_pct"),
                        "modell_nivaa": sc_row.get("modell_nivaa"),
                    }
            except Exception as e:
                print(f"[finn_sok] BilRadar-scoring feilet: {e}")
                traceback.print_exc()
        print(f"[finn_sok] {len(aktive_scores)} fra bilradar_aktive, {len(rows_to_score)} live-scoret")

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
            "hurtigpris": _to_int_safe(sc.get("hurtigpris")),
            "innbyttepris": _to_int_safe(sc.get("innbyttepris")),
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
                           treff=treff, finn_url=finn_url, form=request.args, filter_opts=filter_opts,
                           bare_brukt=bare_brukt, published_last_day=published_last_day)
