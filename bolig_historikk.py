import os
import re
import io
from pathlib import Path
from typing import Tuple, Optional, List, Dict

import pandas as pd

# Prøv å importere boto3, men ikke krav
try:
    import boto3  # type: ignore
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# ----------------------------
#  Konfigurasjon
# ----------------------------

BUCKET_NAME = os.environ.get("BOLIG_S3_BUCKET", "prisanalyse-data")
BOLIG_PREFIX = os.environ.get("BOLIG_HIST_PREFIX", "raw/bolig-daglig/")
LOCAL_DIR = Path("raw/bolig-daglig")  # lokal fallback

# Første fil som skal brukes i historikken (faktisk fil i S3: bolig_X_07-11-2025.csv)
MIN_VALID_DATE = pd.Timestamp("2025-11-07")

# Default startdato i UI (kan være før første fil – vi klipper i logikken)
DEFAULT_START_UI = pd.Timestamp("2025-11-01")

METRIC_LABELS: Dict[str, str] = {
    "median_m2": "Median m²-pris",
    "median_totalpris": "Median totalpris",
    "median_dager": "Median dager på markedet",
}

METRIC_TO_COLUMN = {
    "median_m2": "M2-pris",
    "median_totalpris": "totalpris",
    "median_dager": "dager_paa_markedet",
}

DATE_REGEX = re.compile(r"(\d{2})[-_](\d{2})[-_](\d{4})")


# ----------------------------
#  Hjelpere
# ----------------------------

def _extract_date_from_name(name: str) -> Optional[pd.Timestamp]:
    m = DATE_REGEX.search(name)
    if not m:
        return None
    day, month, year = m.groups()
    try:
        return pd.to_datetime(f"{year}-{month}-{day}")
    except Exception:
        return None


def _normalize_col_name(s: str) -> str:
    """
    Normaliser kolonnenavn (robust mot små variasjoner).
    """
    s = str(s)
    s = s.strip().lower()
    return s


def _find_col(df: pd.DataFrame, wanted: str, extra_aliases: Optional[List[str]] = None) -> Optional[str]:
    """
    Finn en kolonne i df som matcher 'wanted' (case-insensitiv).
    Returnerer faktisk kolonnenavn slik det står i df.
    """
    aliases: set[str] = set()
    aliases.add(_normalize_col_name(wanted))
    if extra_aliases:
        for a in extra_aliases:
            aliases.add(_normalize_col_name(a))

    for c in df.columns:
        c_norm = _normalize_col_name(c)
        if c_norm in aliases:
            return c
    return None


def _format_int_with_spaces(x: Optional[float]) -> str:
    """
    Formatér heltall med tusenskille (mellomrom). None/NaN -> tom streng.
    """
    if x is None or pd.isna(x):
        return ""
    try:
        i = int(round(float(x)))
        return f"{i:,}".replace(",", " ")
    except Exception:
        return ""


# ----------------------------
#  S3-fil-liste (kun listing – ingen CSV-lesing)
# ----------------------------

def list_bolig_files_from_s3() -> List[Tuple[pd.Timestamp, str]]:
    if not HAS_BOTO3:
        print("[HIST] boto3 ikke tilgjengelig – hopper over S3.")
        return []

    try:
        s3 = boto3.client("s3")
    except Exception as e:
        print(f"[HIST] Klarte ikke å opprette S3-klient: {e}")
        return []

    files_with_dates: List[Tuple[pd.Timestamp, str]] = []

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=BOLIG_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                dt = _extract_date_from_name(key)
                if dt is None:
                    continue
                files_with_dates.append((dt, key))
    except Exception as e:
        print(f"[HIST] Feil ved listing av S3-objekter: {e}")
        return []

    files_with_dates.sort(key=lambda x: x[0])
    print(f"[HIST] Fant {len(files_with_dates)} historikkfiler i S3.")
    return files_with_dates


# ----------------------------
#  Lokal fil-liste (kun listing – ingen CSV-lesing)
# ----------------------------

def list_bolig_files_local() -> List[Tuple[pd.Timestamp, Path]]:
    files_with_dates: List[Tuple[pd.Timestamp, Path]] = []
    if not LOCAL_DIR.exists():
        print(f"[HIST] Lokal katalog finnes ikke: {LOCAL_DIR}")
        return []

    for p in LOCAL_DIR.glob("*.csv"):
        dt = _extract_date_from_name(p.name)
        if dt is None:
            continue
        files_with_dates.append((dt, p))

    files_with_dates.sort(key=lambda x: x[0])
    print(f"[HIST] Fant {len(files_with_dates)} historikkfiler lokalt.")
    return files_with_dates


# ----------------------------
#  Felles listing – brukes både til dato-oversikt og historikk
# ----------------------------

def list_bolig_files_anywhere() -> List[Tuple[pd.Timestamp, str]]:
    """
    Returnerer liste (dato, key/filnavn) fra S3 hvis noe finnes,
    ellers fra lokal katalog. Leser IKKE CSV, bare lister filer.
    """
    files_s3 = list_bolig_files_from_s3()
    if files_s3:
        return files_s3

    files_local = list_bolig_files_local()
    return [(dt, str(path)) for (dt, path) in files_local]


def get_available_bolig_dates() -> List[pd.Timestamp]:
    """
    Bruk denne i Flask-routen for å vite hvilke datoer som finnes filer for.
    Leser IKKE CSV, bare lister objekter.
    """
    files = list_bolig_files_anywhere()
    dates = sorted({d for (d, _) in files})
    return dates


def get_default_dates_for_ui() -> Tuple[pd.Timestamp, Optional[pd.Timestamp]]:
    """
    Default verdier til kalenderen i UI:
    - start: 1. november 2025 (konstant)
    - slutt: siste tilgjengelige fil (eller None hvis ingenting)
    """
    dates = get_available_bolig_dates()
    if not dates:
        return DEFAULT_START_UI, None
    return DEFAULT_START_UI, dates[-1]


# ----------------------------
#  CSV-lesing (faktisk data)
# ----------------------------

def read_bolig_csv_from_s3(key: str) -> pd.DataFrame:
    import boto3  # type: ignore
    s3 = boto3.client("s3")
    print(f"[HIST] Leser CSV fra S3: {key}")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = obj["Body"].read()
    data = io.BytesIO(body)

    df = pd.read_csv(
        data,
        sep=";",
        encoding="utf-16",   # VIKTIG: samme som bolig_data.py
        on_bad_lines="skip",
        engine="python",
    )
    df.columns = df.columns.str.strip()
    return df


def read_bolig_csv_anywhere(key: str) -> pd.DataFrame:
    """
    Les CSV enten fra S3 (hvis key er S3-path) eller lokalt.
    """
    if key.startswith(BOLIG_PREFIX) and HAS_BOTO3:
        return read_bolig_csv_from_s3(key)

    path = Path(key)
    if not path.is_absolute():
        path = LOCAL_DIR / path.name

    print(f"[HIST] Leser CSV lokalt: {path}")
    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-16",
        on_bad_lines="skip",
        engine="python",
    )
    df.columns = df.columns.str.strip()
    return df


# ----------------------------
#  Rens & aggreger
# ----------------------------

def prepare_snapshot_df(df: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    df = df.copy()

    # --- Fylke ---
    fylke_col = _find_col(df, "fylke", extra_aliases=["Fylke", "fylke_navn"])
    if fylke_col is None:
        print("[HIST] Fant ingen fylke-kolonne i CSV. Kolonner:", list(df.columns))
        return pd.DataFrame(columns=["fylke", "M2-pris", "totalpris", "dager_paa_markedet"])

    df["fylke"] = df[fylke_col].astype(str).str.strip()
    df = df[df["fylke"].notna() & (df["fylke"] != "")]

    # --- M2-pris ---
    m2_col = _find_col(df, "M2-pris", extra_aliases=["m2_pris", "pris_per_m2", "m2pris"])
    if m2_col is not None:
        df["M2-pris"] = (
            df[m2_col]
            .astype(str)
            .str.replace("kr", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")
    else:
        df["M2-pris"] = pd.NA

    # --- totalpris ---
    tot_col = _find_col(df, "totalpris", extra_aliases=["Totalpris", "total_pris"])
    if tot_col is not None:
        df["totalpris"] = (
            df[tot_col]
            .astype(str)
            .str.replace("kr", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["totalpris"] = pd.to_numeric(df["totalpris"], errors="coerce")
    else:
        df["totalpris"] = pd.NA

    # --- publisert_dato -> dager på markedet ---
    pub_col = _find_col(df, "publisert_dato", extra_aliases=["PublisertDato", "publisert"])
    if pub_col is not None:
        df["publisert_dato_dt"] = pd.to_datetime(df[pub_col], errors="coerce", utc=True)
        snap_ts = pd.Timestamp(snapshot_date).tz_localize("UTC")
        df["dager_paa_markedet"] = (snap_ts - df["publisert_dato_dt"]).dt.days
    else:
        df["dager_paa_markedet"] = pd.NA

    return df[["fylke", "M2-pris", "totalpris", "dager_paa_markedet"]]


def aggregate_snapshot(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    colname = METRIC_TO_COLUMN[metric_col]

    if colname not in df.columns:
        print(f"[HIST] Advarsel: Kolonne '{colname}' finnes ikke i snapshot-df. Kolonner: {list(df.columns)}")
        return pd.DataFrame(columns=["fylke", "nivå", "antall"])

    sub = df[["fylke", colname]].dropna()
    if sub.empty:
        print(f"[HIST] Advarsel: Ingen gyldige verdier for '{colname}' etter dropna().")
        return pd.DataFrame(columns=["fylke", "nivå", "antall"])

    grouped = sub.groupby("fylke")[colname]
    agg = grouped.median().to_frame(name="nivå")
    agg["antall"] = grouped.size()
    agg = agg.reset_index()
    return agg


# ----------------------------
#  Velg to snapshots (første + siste)
# ----------------------------

def _pick_two_snapshots(
    files: List[Tuple[pd.Timestamp, str]],
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> Tuple[Tuple[pd.Timestamp, str], Tuple[pd.Timestamp, str]]:

    if not files:
        raise ValueError("Fant ingen historikkfiler verken i S3 eller lokalt.")

    # Filtrér vekk alle før MIN_VALID_DATE
    files = [(d, k) for (d, k) in files if d >= MIN_VALID_DATE]
    if not files:
        raise ValueError(f"Ingen filer nyere enn {MIN_VALID_DATE.date()}.")

    # Bruk evt. brukerens intervall, men aldri før MIN_VALID_DATE
    if start_date is not None:
        start_date = max(start_date, MIN_VALID_DATE)
        kandidater = [(d, k) for (d, k) in files if d >= start_date]
        if not kandidater:
            raise ValueError("Fant ingen filer som er nyere enn valgt startdato.")
        first = kandidater[0]
    else:
        first = files[0]

    if end_date is not None:
        kandidater = [(d, k) for (d, k) in files if d <= end_date]
        if not kandidater:
            raise ValueError("Fant ingen filer som er eldre enn valgt sluttdato.")
        last = kandidater[-1]
    else:
        last = files[-1]

    return first, last


# ----------------------------
#  Hovedfunksjon brukt av Flask
# ----------------------------

def build_historikk_tabell(
    metric_col: str = "median_m2",
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:

    if metric_col not in METRIC_LABELS:
        metric_col = "median_m2"

    files = list_bolig_files_anywhere()
    if not files:
        raise ValueError("Fant ingen bolig-daglig-filer i S3 eller lokalt.")

    first_snap, last_snap = _pick_two_snapshots(files, start_date, end_date)
    first_date, first_key = first_snap
    last_date, last_key = last_snap

    print(
        f"[HIST] Bruker filer:\n"
        f"  Første: {first_key} ({first_date.date()})\n"
        f"  Siste : {last_key} ({last_date.date()})"
    )

    df_first_raw = read_bolig_csv_anywhere(first_key)
    df_last_raw = read_bolig_csv_anywhere(last_key)

    # Litt debug-info om kolonner / head
    print("[HIST] Første CSV-kolonner:", list(df_first_raw.columns))
    print("[HIST] Siste CSV-kolonner:", list(df_last_raw.columns))
    try:
        print("[HIST] Første CSV-head:\n", df_first_raw.head().to_string())
        print("[HIST] Siste CSV-head:\n", df_last_raw.head().to_string())
    except Exception:
        pass

    df_first = prepare_snapshot_df(df_first_raw, first_date)
    df_last = prepare_snapshot_df(df_last_raw, last_date)

    agg_first = aggregate_snapshot(df_first, metric_col)
    agg_last = aggregate_snapshot(df_last, metric_col)

    merged = agg_first.merge(
        agg_last,
        on="fylke",
        how="outer",
        suffixes=("_første", "_siste"),
    )

    merged["endring"] = merged["nivå_siste"] - merged["nivå_første"]

    def _pct(row):
        if pd.notna(row["nivå_første"]) and row["nivå_første"] != 0:
            return (row["endring"] / row["nivå_første"]) * 100.0
        return pd.NA

    merged["endring_%"] = merged.apply(_pct, axis=1)

    def _weighted(agg: pd.DataFrame) -> Optional[float]:
        if agg.empty or agg["antall"].sum() == 0:
            return None
        return (agg["nivå"] * agg["antall"]).sum() / agg["antall"].sum()

    norge_first = _weighted(agg_first)
    norge_last = _weighted(agg_last)
    norge_first_n = int(agg_first["antall"].sum()) if not agg_first.empty else 0
    norge_last_n = int(agg_last["antall"].sum()) if not agg_last.empty else 0

    norge_row: Dict[str, object] = {
        "fylke": "Norge (vektet)",
        "nivå_første": norge_first,
        "antall_første": norge_first_n,
        "nivå_siste": norge_last,
        "antall_siste": norge_last_n,
    }
    norge_row["endring"] = (
        None if (norge_first is None or norge_last is None) else norge_last - norge_first
    )
    if norge_row["endring"] is not None and norge_first not in (None, 0):
        norge_row["endring_%"] = (norge_row["endring"] / norge_first) * 100.0
    else:
        norge_row["endring_%"] = None

    merged = merged.rename(
        columns={
            "nivå_første": "første_nivå",
            "nivå_siste": "siste_nivå",
        }
    )

    fylker_df = merged.sort_values("fylke").reset_index(drop=True)

    norge_df = pd.DataFrame([norge_row]).rename(
        columns={
            "nivå_første": "første_nivå",
            "nivå_siste": "siste_nivå",
        }
    )

    full_df = pd.concat([norge_df, fylker_df], ignore_index=True)

    # ----------------------------
    #  Formatering av tall for visning
    is_price_metric = metric_col in ("median_m2", "median_totalpris")

    if is_price_metric:
        # Prisfelter som heltall med tusenskille (mellomrom)
        for col in ["første_nivå", "siste_nivå", "endring"]:
            full_df[col] = full_df[col].apply(_format_int_with_spaces)
    else:
        # Ikke-pris:
        if metric_col == "median_dager":
            # Rund av til hele dager, behold som tall (Int64) så de vises uten desimaler
            for col in ["første_nivå", "siste_nivå", "endring"]:
                full_df[col] = pd.to_numeric(full_df[col], errors="coerce").round(0).astype("Int64")
        # evt. andre ikke-pris-metrikker kan få egen behandling senere
    # endring_% med én desimal
    def _fmt_pct(x):
        if x is None or pd.isna(x):
            return ""
        try:
            return f"{float(x):.1f}"
        except Exception:
            return ""

    full_df["endring_%"] = full_df["endring_%"].apply(_fmt_pct)

    full_df = full_df[
        [
            "fylke",
            "første_nivå",
            "siste_nivå",
            "endring",
            "endring_%",
            "antall_første",
            "antall_siste",
        ]
    ]

    full_df = full_df.rename(
        columns={
            "fylke": "Fylke",
        }
    )

    return full_df, first_date, last_date
