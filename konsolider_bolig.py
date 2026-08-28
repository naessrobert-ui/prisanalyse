#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bygger ÉN master Parquet (snappy) fra daglige CSV-filer i S3.

Resultat: én rad per finnkode (unik), beholder ALLE kolonner fra første observasjon,
og legger til:
- dato_første: første snapshot-dato vi så annonsen
- dato_siste: siste snapshot-dato vi så annonsen
- pris_første: første totalpris vi observerte
- pris_ny: første endrede totalpris (default = pris_første)
- dato_prisendring: dato for første prisendring (default = dato_første)

Denne versjonen:
- Fyller inn manglende felter i master fra senere observasjoner (hvis master mangler).
- Oppdaterer latitude/longitude hvis master mangler/ugyldig og dagens er gyldig.
- Enforcer schema før skriving til parquet.
- Dedupliserer på finnkode før indexering.
- Aligner serier eksplisitt før prissammenligning (fikser pandas ValueError).
- Rydder opp i pandas FutureWarning i replace()-bruk.

NB:
- Leser kun CSV-ene (IKKE snapshot parquet).
- Skriver én parquet-fil (overwrite) til S3 med Snappy.
- State (checkpoint) oppdateres som før.
"""

import re
import json
import io
from datetime import datetime, date
from typing import Optional

from botocore.config import Config
import boto3

import numpy as np
import pandas as pd

from bolig_price_validation import plausible_price, plausible_price_change

from pathlib import Path
import os

USE_KOMMUNE_MAPPING = True

BASE_DIR = Path(__file__).resolve().parent
GEOJSON_LOCAL_PATH = os.getenv(
    "KOMMUNE_GEOJSON",
    str((BASE_DIR / "static" / "geo" / "Kommuner-M.geojson").resolve())
)

try:
    import geopandas as gpd
    from shapely.geometry import Point
except Exception:
    gpd = None
    Point = None


# ===================== Konfig =====================
S3_BUCKET = "prisanalyse-data"
S3_INPUT_PREFIX = "raw/bolig-daglig/"

S3_OUTPUT_PREFIX = "calc/bolig/"
S3_OUTPUT_PREFIX2 = "calc/"

# Master output
MASTER_PREFIX = f"{S3_OUTPUT_PREFIX}bolig_master/"
MASTER_KEY = f"{MASTER_PREFIX}bolig_master.parquet"

# State/metadata
OUTPUT_STATE_KEY = f"{S3_OUTPUT_PREFIX}state_database_bolig.json"
OUTPUT_METADATA_KEY = f"{S3_OUTPUT_PREFIX2}metadata.json"  # valgfritt

# Filpattern: bolig_X_26-10-2025.csv
DAILY_PATTERN = r"bolig_X_(\d{2})-(\d{2})-(\d{4})\.csv$"
START_DATE = date(2025, 10, 26)  # første gyldige fil


# --------------------- helpers ---------------------
def parse_snapshot_date_from_key(key: str) -> Optional[date]:
    filename = key.split("/")[-1]
    m = re.search(DAILY_PATTERN, filename)
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    return date(yyyy, mm, dd)


def s3_list_csv_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            if obj.get("Size", 0) == 0:
                continue
            yield key


def try_load_state(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_STATE_KEY)
        raw = obj["Body"].read().decode("utf-8")
        return json.loads(raw)
    except Exception:
        return {}


def save_state(s3, state: dict):
    buf = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_STATE_KEY, Body=buf)


def try_load_master(s3) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=MASTER_KEY)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        if "finnkode" in df.columns:
            df["finnkode"] = df["finnkode"].astype(str)
            df = df.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _clean_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {
        "M2-pris": "m2_pris",
        "NY/Brukt": "ny_brukt",
    }
    df = df.rename(columns=rename)
    return df


def _to_int_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.replace(r"[^\d]", "", regex=True)
    x = x.mask(x.isin(["", "nan", "None"]))
    return pd.to_numeric(x, errors="coerce")


def _to_float_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.replace(",", ".", regex=False).str.strip()
    x = x.mask(x.isin(["", "nan", "None"]))
    return pd.to_numeric(x, errors="coerce")


def _parse_publisert_dato(s: pd.Series) -> pd.Series:
    """
    Robust parsing: støtter både 'dd.mm.yyyy' og ISO (2026-02-05T...+00:00).
    """
    return pd.to_datetime(s, errors="coerce", utc=True)


def s3_read_csv_auto(s3, key: str) -> pd.DataFrame:
    """
    Robust lesing: prøver utf-8 og utf-16, og prøver tab/;/,.
    Leser alt som string først.
    """
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()

    encodings = ["utf-8", "utf-16", "latin-1"]
    seps = ["\t", ";", ","]

    last_err = None
    for enc in encodings:
        try:
            text = raw.decode(enc)
        except Exception as e:
            last_err = e
            continue

        for sep in seps:
            try:
                df = pd.read_csv(
                    io.StringIO(text),
                    sep=sep,
                    dtype=str,
                    on_bad_lines="warn",
                )
                cols = {c.strip().lower() for c in df.columns}
                if "finnkode" in cols:
                    return df
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(f"Kunne ikke lese {key}. Siste feil: {last_err}")


def load_kommune_gdf() -> Optional["gpd.GeoDataFrame"]:
    if not USE_KOMMUNE_MAPPING:
        return None
    if gpd is None:
        print("ADVARSEL: geopandas ikke tilgjengelig. Skipper kommune-mapping.")
        return None
    try:
        if not Path(GEOJSON_LOCAL_PATH).exists():
            print(f"ADVARSEL: GeoJSON finnes ikke: {GEOJSON_LOCAL_PATH} (skipper kommune-mapping)")
            return None

        g = gpd.read_file(GEOJSON_LOCAL_PATH)
        if g.crs is None:
            g = g.set_crs(epsg=4326)
        else:
            g = g.to_crs(epsg=4326)

        cols = [c for c in g.columns]

        def pick(*names):
            for n in names:
                for c in cols:
                    if c.lower() == n.lower():
                        return c
            return None

        col_nr = pick("kommunenummer", "komm_nr", "kommune_nr", "KOMMUNENR", "KOMMUNE_NR")
        col_name = pick("kommunenavn", "kommune", "KOMMUNENAVN", "NAME", "navn")

        g["_komm_nr_col"] = col_nr or ""
        g["_komm_name_col"] = col_name or ""
        return g
    except Exception as e:
        print(f"ADVARSEL: Klarte ikke lese geojson ({GEOJSON_LOCAL_PATH}): {e}")
        return None


def add_kommune(df: pd.DataFrame, kommune_gdf: Optional["gpd.GeoDataFrame"]) -> pd.DataFrame:
    if kommune_gdf is None or gpd is None:
        df = df.copy()
        df["kommune_nr"] = pd.NA
        df["kommune_navn"] = pd.NA
        return df

    d = df.copy()
    d["latitude"] = _to_float_series(d["latitude"])
    d["longitude"] = _to_float_series(d["longitude"])

    ok = (
        d["latitude"].between(57, 72, inclusive="both")
        & d["longitude"].between(4, 32, inclusive="both")
    )
    d_ok = d[ok].copy()
    d_bad = d[~ok].copy()
    d_bad["kommune_nr"] = pd.NA
    d_bad["kommune_navn"] = pd.NA

    if d_ok.empty:
        d["kommune_nr"] = pd.NA
        d["kommune_navn"] = pd.NA
        return d

    g_points = gpd.GeoDataFrame(
        d_ok,
        geometry=[Point(xy) for xy in zip(d_ok["longitude"], d_ok["latitude"])],
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(g_points, kommune_gdf, how="left", predicate="within")

    nr_col = kommune_gdf["_komm_nr_col"].iloc[0]
    name_col = kommune_gdf["_komm_name_col"].iloc[0]

    if nr_col and nr_col in joined.columns:
        joined["kommune_nr"] = joined[nr_col].astype(str)
    else:
        joined["kommune_nr"] = pd.NA

    if name_col and name_col in joined.columns:
        joined["kommune_navn"] = joined[name_col].astype(str)
    else:
        joined["kommune_navn"] = pd.NA

    joined = pd.DataFrame(joined.drop(columns=["geometry"], errors="ignore"))
    out = pd.concat([joined, d_bad], axis=0, ignore_index=True)
    return out


def normalize_bolig_df(df: pd.DataFrame, snapshot_dt: date) -> pd.DataFrame:
    df = _clean_colnames(df)

    expected = [
        "finnkode", "full_title", "address", "broker_name",
        "totalpris", "prisantydning", "m2_pris",
        "fylke", "ny_brukt", "felles", "eierform", "boligtype",
        "soverom", "size", "viewing",
        "latitude", "longitude", "publisert_dato", "annonsepakke",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = pd.NA

    df["finnkode"] = (
        df["finnkode"].astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.strip()
    )
    df = df[df["finnkode"] != ""].copy()
    if df.empty:
        return df

    # Dedupliser tidlig for å unngå index-problemer senere
    df = df.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)

    # typer
    df["totalpris"] = _to_int_series(df["totalpris"])
    df["prisantydning"] = _to_int_series(df["prisantydning"])
    df["felles"] = _to_int_series(df["felles"])
    df["m2_pris"] = _to_int_series(df["m2_pris"])

    df["size"] = _to_float_series(df["size"])
    df["soverom"] = _to_int_series(df["soverom"])

    df["latitude"] = _to_float_series(df["latitude"])
    df["longitude"] = _to_float_series(df["longitude"])

    df["publisert_dato"] = _parse_publisert_dato(df["publisert_dato"])

    # snapshot date
    df["snapshot_date"] = pd.to_datetime(snapshot_dt)

    # beregnet m2 (valgfritt)
    df["m2_calc_totalpris"] = np.where(
        (df["totalpris"].notna()) & (df["size"].notna()) & (df["size"] > 0),
        (df["totalpris"] / df["size"]).round(0),
        np.nan,
    )

    return df


# ---------- helper-funksjoner for "fill missing" ----------
def _is_missing(s: pd.Series) -> pd.Series:
    """True hvis verdi regnes som manglende (NaN / None / tom streng / 'nan' / 'None')."""
    if s.dtype == "O":
        x = s.astype(str).str.strip().str.lower()
        return s.isna() | (x == "") | (x == "nan") | (x == "none")
    return s.isna()


def fill_missing_from_today(master_idx: pd.DataFrame, today_idx: pd.DataFrame, ids, cols_to_fill: list[str]):
    """
    Fyller KUN inn manglende felter i master fra dagens data.
    Overskriver ikke eksisterende verdier.
    """
    if len(ids) == 0:
        return

    for c in cols_to_fill:
        if c not in master_idx.columns or c not in today_idx.columns:
            continue

        m = master_idx.loc[ids, c]
        t = today_idx.loc[ids, c]

        m, t = m.align(t, join="inner")

        mask = _is_missing(m) & (~_is_missing(t))
        if mask.any():
            upd = mask.index[mask]
            master_idx.loc[upd, c] = t.loc[upd].values


def update_latlon_if_better(master_idx: pd.DataFrame, today_idx: pd.DataFrame, ids):
    """
    Koordinater: oppdater hvis master mangler/ugyldig og dagens er gyldig i Norge.
    """
    if len(ids) == 0:
        return

    if "latitude" not in master_idx.columns or "longitude" not in master_idx.columns:
        return
    if "latitude" not in today_idx.columns or "longitude" not in today_idx.columns:
        return

    mlat = pd.to_numeric(master_idx.loc[ids, "latitude"], errors="coerce")
    mlon = pd.to_numeric(master_idx.loc[ids, "longitude"], errors="coerce")
    tlat = pd.to_numeric(today_idx.loc[ids, "latitude"], errors="coerce")
    tlon = pd.to_numeric(today_idx.loc[ids, "longitude"], errors="coerce")

    mlat, tlat = mlat.align(tlat, join="inner")
    mlon, tlon = mlon.align(tlon, join="inner")

    common_ids = mlat.index.intersection(mlon.index).intersection(tlat.index).intersection(tlon.index)
    if len(common_ids) == 0:
        return

    mlat = mlat.loc[common_ids]
    mlon = mlon.loc[common_ids]
    tlat = tlat.loc[common_ids]
    tlon = tlon.loc[common_ids]

    master_ok = mlat.between(57, 72) & mlon.between(4, 32)
    today_ok = tlat.between(57, 72) & tlon.between(4, 32)

    to_update = (~master_ok) & today_ok
    if to_update.any():
        upd_ids = to_update.index[to_update]
        master_idx.loc[upd_ids, "latitude"] = tlat.loc[upd_ids].values
        master_idx.loc[upd_ids, "longitude"] = tlon.loc[upd_ids].values


def _ensure_master_cols(master: pd.DataFrame, df_today: pd.DataFrame) -> pd.DataFrame:
    # Sørg for at master har alle kolonner fra input
    for c in df_today.columns:
        if c not in master.columns:
            master[c] = pd.NA

    # Tracking-kolonner
    for c in ["dato_første", "dato_siste", "pris_første", "pris_ny", "dato_prisendring"]:
        if c not in master.columns:
            master[c] = pd.NA

    return master


def enforce_schema_before_write(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sørger for stabile dtypes før parquet-skriving.
    """
    d = df.copy()

    # UTC datetime
    if "publisert_dato" in d.columns:
        d["publisert_dato"] = pd.to_datetime(d["publisert_dato"], errors="coerce", utc=True)

    # naive datetime
    for c in ["dato_første", "dato_siste", "dato_prisendring", "snapshot_date"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    # Numeric
    for c in ["totalpris", "prisantydning", "felles", "m2_pris", "pris_første", "pris_ny", "soverom"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    for c in ["size", "latitude", "longitude", "m2_calc_totalpris"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    return d


def s3_put_parquet_snappy(s3, key: str, df: pd.DataFrame):
    """
    Skriver én Parquet-fil med Snappy-komprimering.
    """
    df = enforce_schema_before_write(df)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


def update_master(master: pd.DataFrame, df_today: pd.DataFrame, snapshot_dt: date) -> pd.DataFrame:
    snap_ts = pd.to_datetime(snapshot_dt)

    df_today = df_today.copy()
    df_today["finnkode"] = df_today["finnkode"].astype(str)
    df_today = df_today[df_today["finnkode"].str.strip() != ""].copy()
    df_today = df_today.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)

    if master.empty:
        master = df_today.copy()
        master["dato_første"] = snap_ts
        master["dato_siste"] = snap_ts
        master["pris_første"] = master["totalpris"]
        master["pris_ny"] = master["totalpris"]
        master["dato_prisendring"] = snap_ts
        master = master.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)
        return master

    master = master.copy()
    master["finnkode"] = master["finnkode"].astype(str)
    master = master[master["finnkode"].str.strip() != ""].copy()
    master = master.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)

    master = _ensure_master_cols(master, df_today)

    master_idx = master.set_index("finnkode", drop=False)
    today_idx = df_today.set_index("finnkode", drop=False)

    in_master = today_idx.index.intersection(master_idx.index)
    new_ids = today_idx.index.difference(master_idx.index)

    # Oppdater siste dato for alle vi ser i dag
    if len(in_master) > 0:
        master_idx.loc[in_master, "dato_siste"] = snap_ts

        # Sikre defaults for eksisterende rader
        need_defaults = master_idx.loc[in_master, "dato_første"].isna()
        if need_defaults.any():
            ids = need_defaults.index[need_defaults]
            master_idx.loc[ids, "dato_første"] = master_idx.loc[ids, "snapshot_date"]
            master_idx.loc[ids, "pris_første"] = master_idx.loc[ids, "totalpris"]
            master_idx.loc[ids, "pris_ny"] = master_idx.loc[ids, "totalpris"]
            master_idx.loc[ids, "dato_prisendring"] = master_idx.loc[ids, "dato_første"]

        # Fyll inn manglende felter
        cols_to_fill = [
            "full_title", "address", "broker_name",
            "fylke", "ny_brukt", "eierform", "boligtype", "annonsepakke",
            "publisert_dato", "viewing",
            "size", "soverom",
            "prisantydning", "felles",
            "m2_pris", "m2_calc_totalpris",
            "kommune_nr", "kommune_navn",
        ]
        fill_missing_from_today(master_idx, today_idx, in_master, cols_to_fill)

        # Koordinater
        update_latlon_if_better(master_idx, today_idx, in_master)

        # Prisendring: første gang en rimelig totalpris avviker fra pris_første.
        # Midlertidige plassholderpriser skal verken opprette eller låse en
        # prisendring i historikken.
        tp_today = today_idx.loc[in_master, "totalpris"]
        tp_first = master_idx.loc[in_master, "pris_første"]

        tp_today, tp_first = tp_today.align(tp_first, join="inner")

        # Hvis første observasjon var en åpenbar plassholderpris, bruk den
        # første senere realistiske observasjonen som startpris.
        repair_first = ~plausible_price(tp_first) & plausible_price(tp_today)
        if repair_first.any():
            ids = repair_first.index[repair_first]
            master_idx.loc[ids, "pris_første"] = tp_today.loc[ids].values
            master_idx.loc[ids, "pris_ny"] = tp_today.loc[ids].values
            master_idx.loc[ids, "dato_prisendring"] = master_idx.loc[ids, "dato_første"].values

        tp_first = master_idx.loc[tp_today.index, "pris_første"]
        tp_recorded = master_idx.loc[tp_today.index, "pris_ny"]

        # Reparer historiske plassholderendringer når annonsen senere igjen
        # viser en realistisk pris. Hvis prisen er tilbake på startnivået,
        # nullstilles prisendringen.
        dp = master_idx.loc[tp_today.index, "dato_prisendring"]
        dfirst = master_idx.loc[tp_today.index, "dato_første"]
        recorded_change = dp.ne(dfirst)
        repair_recorded = (
            recorded_change
            & ~plausible_price_change(tp_first, tp_recorded)
            & plausible_price_change(tp_first, tp_today)
        )
        if repair_recorded.any():
            ids = repair_recorded.index[repair_recorded]
            master_idx.loc[ids, "pris_ny"] = tp_today.loc[ids].values
            unchanged_ids = ids[tp_today.loc[ids].eq(tp_first.loc[ids])]
            changed_ids = ids.difference(unchanged_ids)
            if len(unchanged_ids) > 0:
                master_idx.loc[unchanged_ids, "dato_prisendring"] = master_idx.loc[
                    unchanged_ids, "dato_første"
                ].values
            if len(changed_ids) > 0:
                master_idx.loc[changed_ids, "dato_prisendring"] = snap_ts

        changed = (
            tp_today.ne(tp_first)
            & plausible_price_change(tp_first, tp_today)
        )

        dp = master_idx.loc[tp_today.index, "dato_prisendring"]
        dfirst = master_idx.loc[tp_today.index, "dato_første"]
        dp, dfirst = dp.align(dfirst, join="inner")

        not_changed_yet = dp.eq(dfirst)

        to_set = changed & not_changed_yet
        if to_set.any():
            ids = to_set.index[to_set]
            master_idx.loc[ids, "pris_ny"] = tp_today.loc[ids].values
            master_idx.loc[ids, "dato_prisendring"] = snap_ts

    # Nye rader
    if len(new_ids) > 0:
        new_rows = today_idx.loc[new_ids].copy()

        new_rows["dato_første"] = snap_ts
        new_rows["dato_siste"] = snap_ts
        new_rows["pris_første"] = new_rows["totalpris"]
        new_rows["pris_ny"] = new_rows["totalpris"]
        new_rows["dato_prisendring"] = snap_ts

        for c in master_idx.columns:
            if c not in new_rows.columns:
                new_rows[c] = pd.NA
        new_rows = new_rows[master_idx.columns]

        master_idx = pd.concat([master_idx, new_rows], axis=0)

    master_out = master_idx.reset_index(drop=True)
    master_out["finnkode"] = master_out["finnkode"].astype(str)
    master_out = master_out.drop_duplicates(subset=["finnkode"], keep="first").reset_index(drop=True)
    return master_out


# --------------------- main ---------------------
def oppdater_bolig_master():
    config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=30,
        read_timeout=120,
        tcp_keepalive=True,
    )
    s3 = boto3.client("s3", config=config)

    state = try_load_state(s3)

    last_processed_str = state.get("last_processed_snapshot_date")
    if last_processed_str:
        try:
            last_processed = datetime.fromisoformat(last_processed_str).date()
        except Exception:
            last_processed = date(1900, 1, 1)
    else:
        last_processed = date(1900, 1, 1)

    today = date.today()

    # 1) list kandidatfiler
    candidates = []
    for key in s3_list_csv_objects(s3, S3_INPUT_PREFIX):
        dt = parse_snapshot_date_from_key(key)
        if not dt:
            continue
        if dt < START_DATE or dt > today:
            continue
        candidates.append((dt, key))

    if not candidates:
        print("Fant ingen boligfiler å prosessere.")
        return

    candidates.sort(key=lambda x: x[0])

    # 2) prosesser kun nye datoer
    new_files = [(dt, key) for (dt, key) in candidates if dt > last_processed]
    if not new_files:
        print(f"Ingen nye filer etter {last_processed.isoformat()}.")
        return

    print(f"Skal prosessere {len(new_files)} filer (etter {last_processed.isoformat()}).")
    print(f"GEOJSON_LOCAL_PATH: {GEOJSON_LOCAL_PATH}")

    kommune_gdf = load_kommune_gdf()

    # 3) last master (hvis finnes)
    master = try_load_master(s3)
    print(f"Master loaded: {len(master)} rader")

    max_dt = last_processed

    for snapshot_dt, key in new_files:
        print(f"Leser {key} (snapshot_date={snapshot_dt.isoformat()})")

        try:
            df_raw = s3_read_csv_auto(s3, key)
        except Exception as e:
            print(f"  FEIL ved lesing: {e}")
            continue

        if df_raw.empty:
            print("  Tom fil, hopper over.")
            continue

        df_today = normalize_bolig_df(df_raw, snapshot_dt)
        if df_today.empty:
            print("  Ingen gyldige rader, hopper over.")
            continue

        if USE_KOMMUNE_MAPPING:
            df_today = add_kommune(df_today, kommune_gdf)
        else:
            df_today["kommune_nr"] = pd.NA
            df_today["kommune_navn"] = pd.NA

        # Oppdater master
        master = update_master(master, df_today, snapshot_dt)
        print(f"  Master nå: {len(master)} rader")

        # checkpoint state etter hver dag
        state["last_processed_snapshot_date"] = snapshot_dt.isoformat()
        state["updated_at"] = datetime.now().isoformat(timespec="seconds")
        save_state(s3, state)
        print(f"  State checkpoint: {state['last_processed_snapshot_date']}")

        if snapshot_dt > max_dt:
            max_dt = snapshot_dt

    # 4) skriv én Parquet master (snappy)
    s3_put_parquet_snappy(s3, MASTER_KEY, master)
    print(f"Skrev master -> s3://{S3_BUCKET}/{MASTER_KEY}")

    # 5) oppdater state til max_dt
    state["last_processed_snapshot_date"] = max_dt.isoformat()
    state["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_state(s3, state)
    print(f"State oppdatert: last_processed_snapshot_date={state['last_processed_snapshot_date']}")
    print("Ferdig!")


if __name__ == "__main__":
    oppdater_bolig_master()
