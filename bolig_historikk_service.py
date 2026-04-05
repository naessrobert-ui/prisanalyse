# bolig_historikk_service.py
# -*- coding: utf-8 -*-

import io
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from typing import Literal, Optional

import numpy as np
import pandas as pd

import boto3
from botocore.config import Config


Level = Literal["Fylke", "Kommune", "Sted"]


@dataclass(frozen=True)
class HistConfig:
    s3_bucket: str = os.environ.get("BOLIG_S3_BUCKET", "prisanalyse-data")
    master_key: str = os.environ.get("BOLIG_MASTER_KEY", "calc/bolig/bolig_master/bolig_master.parquet")
    cache_ttl_seconds: int = int(os.environ.get("BOLIG_MASTER_CACHE_TTL_SECONDS", "900"))


CFG = HistConfig()


def _parse_datetime_series(values, *, normalize: bool = True) -> pd.Series:
    """
    Robust datetime-parser for mixed boligdata-format.
    Tåler bl.a.:
      - ISO-tidsstempel
      - norsk format: DD.MM.YYYY HH:MM
      - tekst med innbakt label/linjeskift, f.eks. "publisert_dato\n22.12.2021 11:11"
    """

    if isinstance(values, pd.Series):
        s = values.copy()
    else:
        s = pd.Series(values)

    def _extract_date_text(v):
        if pd.isna(v):
            return None
        txt = str(v).strip()
        if not txt:
            return None

        match = re.search(
            r"(\d{1,2}\.\d{1,2}\.\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)",
            txt,
        )
        return match.group(1) if match else txt

    cleaned = s.map(_extract_date_text)

    # Parse først eksplisitt norsk datoformat, deretter ISO/andre formater.
    # Dette unngår advarselen om dayfirst=True på ISO-datoer og gjør parsing mer stabil.
    parsed_no = pd.to_datetime(
        cleaned,
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
        utc=True,
    )
    missing_time = parsed_no.isna()
    if missing_time.any():
        parsed_no.loc[missing_time] = pd.to_datetime(
            cleaned[missing_time],
            format="%d.%m.%Y %H:%M",
            errors="coerce",
            utc=True,
        )

    parsed = parsed_no
    missing = parsed.isna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(
            cleaned[missing],
            errors="coerce",
            utc=True,
            dayfirst=False,
        )
    parsed = parsed.dt.tz_convert(None)
    if normalize:
        parsed = parsed.dt.normalize()
    return parsed


def _s3_client():
    config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=30,
        read_timeout=120,
        tcp_keepalive=True,
    )
    return boto3.client("s3", config=config)


_MASTER_CACHE: dict[tuple[str, str], dict[str, object]] = {}


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_master_s3_cached(bucket: str, key: str) -> pd.DataFrame:
    """
    Leser master parquet fra S3 med cache som invalides når objektet endres
    eller når cache-levetiden utløper.
    """
    now = datetime.now(timezone.utc)
    cache_key = (bucket, key)
    cache_ttl = max(0, int(CFG.cache_ttl_seconds))
    cached = _MASTER_CACHE.get(cache_key)

    if cached is not None:
        loaded_at = _to_utc(cached.get("loaded_at"))
        if loaded_at is not None and (now - loaded_at).total_seconds() < cache_ttl:
            return cached["df"]

    s3 = _s3_client()
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        # Fallback: prøv direkte lesing om metadata-oppslag feiler.
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        _MASTER_CACHE[cache_key] = {"df": df, "loaded_at": now, "last_modified": None}
        return df

    remote_last_modified = _to_utc(head.get("LastModified"))

    if cached is not None:
        cached_last_modified = _to_utc(cached.get("last_modified"))
        if cached_last_modified is not None and remote_last_modified is not None:
            if remote_last_modified <= cached_last_modified:
                cached["loaded_at"] = now
                return cached["df"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    # Fjern gammel DataFrame eksplisitt før vi erstatter, slik at GC kan frigi minnet.
    if cache_key in _MASTER_CACHE:
        del _MASTER_CACHE[cache_key]
    _MASTER_CACHE[cache_key] = {
        "df": df,
        "loaded_at": now,
        "last_modified": remote_last_modified,
    }
    return df


# Manuell cache for normalisert master – invalideres når S3-data endres,
# i stedet for lru_cache som aldri invalideres og skaper doble DataFrames i minnet.
_NORMALIZED_CACHE: dict = {}

def load_normalized_master_cached(bucket: str, key: str) -> pd.DataFrame:
    """
    Leser og normaliserer master, cacher resultatet.
    Invalideres automatisk når S3-objektet endres (samme logikk som load_master_s3_cached).
    """
    cache_key = (bucket, key)
    master_entry = _MASTER_CACHE.get(cache_key)

    # Sjekk om vi allerede har en normalisert versjon med samme last_modified
    norm_entry = _NORMALIZED_CACHE.get(cache_key)
    if norm_entry is not None and master_entry is not None:
        if norm_entry.get("last_modified") == master_entry.get("last_modified"):
            return norm_entry["df"]

    # Hent rå data og normaliser
    raw = load_master_s3_cached(bucket, key)
    master_entry = _MASTER_CACHE.get(cache_key)  # oppdatert etter load
    normalized = normalize_master(raw)

    # Lagre normalisert versjon – frigjør rå-kopi eksplisitt
    _NORMALIZED_CACHE[cache_key] = {
        "df": normalized,
        "last_modified": master_entry.get("last_modified") if master_entry else None,
    }
    return normalized


def extract_poststed(address: pd.Series) -> pd.Series:
    s = address.astype(str)
    s = s.str.split(",").str[-1]
    s = s.str.replace(r"\b\d{4}\b", "", regex=True)
    s = s.str.strip()
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
    return s


def _parse_number_series(series: pd.Series) -> pd.Series:
    """
    Tåler både norske og internasjonale tusenskiller/desimaler.
    Eksempler som håndteres: "1 234 567", "1.234.567", "1,234,567", "123,45".
    """
    s = series.astype(str)
    s = s.str.replace("\u00a0", "", regex=False)  # NBSP
    s = s.str.replace("kr", "", regex=False, case=False)
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace(r"[^0-9,.-]", "", regex=True)

    # Hvis både punktum og komma finnes: bruk siste separator som desimaltegn.
    both_mask = s.str.contains(".", regex=False) & s.str.contains(",", regex=False)
    decimal_comma_mask = both_mask & (s.str.rfind(",") > s.str.rfind("."))
    decimal_dot_mask = both_mask & ~decimal_comma_mask

    s.loc[decimal_comma_mask] = s.loc[decimal_comma_mask].str.replace(".", "", regex=False)
    s.loc[decimal_comma_mask] = s.loc[decimal_comma_mask].str.replace(",", ".", regex=False)

    s.loc[decimal_dot_mask] = s.loc[decimal_dot_mask].str.replace(",", "", regex=False)

    # Kun komma: bruk komma som desimaltegn.
    comma_only_mask = (~both_mask) & s.str.contains(",", regex=False)
    s.loc[comma_only_mask] = s.loc[comma_only_mask].str.replace(",", ".", regex=False)

    # Punktum kun som tusenskiller (f.eks. 1.234.567) -> fjern punktum.
    thousands_dot_mask = s.str.match(r"^-?\d{1,3}(\.\d{3})+$", na=False)
    s.loc[thousands_dot_mask] = s.loc[thousands_dot_mask].str.replace(".", "", regex=False)

    return pd.to_numeric(s, errors="coerce")


def _parse_area_m2(df: pd.DataFrame) -> pd.Series:
    # NB: "size" i master kan være antall rom (3, 4, 5 ...) og må ikke brukes som areal.
    preferred = ["areal", "areal_m2", "bruksareal", "kvm", "area", "bra", "p-rom", "prom", "primærrom", "boareal"]
    area_col = next((c for c in preferred if c in df.columns), None)

    if area_col is None:
        # Fallback: finn sannsynlig arealkolonne med navneheuristikk
        for c in df.columns:
            cl = str(c).strip().lower()
            if cl == "size" or "rom" in cl:
                continue
            if any(k in cl for k in ["areal", "bra", "boareal", "kvm", "m2", "m²", "primær"]):
                area_col = c
                break

    if area_col is None:
        return pd.Series(np.nan, index=df.index, dtype=float)

    area = df[area_col].astype(str)
    area = area.str.replace("m²", "", regex=False)
    area = area.str.replace("m2", "", regex=False)
    area = area.str.replace(" ", "", regex=False)
    area = area.str.replace(",", ".", regex=False)
    area = pd.to_numeric(area, errors="coerce")
    return area


def normalize_master(df: pd.DataFrame) -> pd.DataFrame:
    # Behold kun kolonner vi faktisk bruker i historikk-endepunktene,
    # slik at vi ikke drar med store, ubrukte objektkolonner i minnet.
    used_columns = [
        "finnkode", "fylke", "kommune_nr", "kommune_navn",
        "address", "full_title", "boligtype",
        "totalpris", "m2_pris", "ny_brukt",
        "latitude", "longitude",
        "publisert_dato", "dato_første", "dato_siste", "dato_prisendring",
        "pris_første", "pris_ny",
        "areal", "areal_m2", "bruksareal", "kvm", "area", "bra", "p-rom", "prom", "primærrom", "boareal", "size",
    ]
    present_cols = [c for c in used_columns if c in df.columns]
    d = df.reindex(columns=present_cols).copy()

    required = [
        "finnkode", "fylke", "kommune_nr", "kommune_navn",
        "address", "full_title",
        "totalpris", "m2_pris", "ny_brukt",
        "latitude", "longitude",
        "publisert_dato",
        "dato_første", "dato_siste",
        "pris_første", "pris_ny", "dato_prisendring",
    ]
    for c in required:
        if c not in d.columns:
            d[c] = pd.NA

    # Dates
    d["publisert_dato"] = _parse_datetime_series(d.get("publisert_dato"), normalize=True)
    d["dato_første"] = _parse_datetime_series(d["dato_første"], normalize=True)
    d["dato_siste"] = _parse_datetime_series(d["dato_siste"], normalize=True)
    d["dato_prisendring"] = _parse_datetime_series(d["dato_prisendring"], normalize=True)

    # Numerics
    for col in ["totalpris", "m2_pris", "latitude", "longitude", "pris_første", "pris_ny"]:
        d[col] = _parse_number_series(d[col])

    # M2-pris kan i enkelte masterfiler være feil/skjevt serialisert.
    # Reparer verdier som er tomme eller åpenbart urimelige ved å bruke totalpris/areal.
    area_m2 = _parse_area_m2(d)
    with np.errstate(divide="ignore", invalid="ignore"):
        calc_m2 = d["totalpris"] / area_m2
    plausible_area = (area_m2 >= 15) & (area_m2 <= 1000)
    plausible_calc = (calc_m2 >= 5_000) & (calc_m2 <= 300_000)
    suspect_m2 = d["m2_pris"].isna() | (d["m2_pris"] <= 1_000) | (d["m2_pris"] >= 300_000)
    d.loc[suspect_m2 & plausible_area & plausible_calc, "m2_pris"] = calc_m2[suspect_m2 & plausible_area & plausible_calc]

    # Hvis m2 fortsatt er åpenbart urimelig etter eventuell reparasjon, sett til NaN
    d.loc[(d["m2_pris"] < 5_000) | (d["m2_pris"] > 300_000), "m2_pris"] = np.nan

    # Strings
    for c in ["fylke", "kommune_navn", "ny_brukt", "address", "full_title"]:
        d[c] = d[c].astype(str).replace({"None": "", "nan": ""})

    d["finnkode"] = d["finnkode"].astype(str)

    d["sted"] = extract_poststed(d["address"])
    mask = d["sted"].isna() | (d["sted"].astype(str).str.strip() == "")
    d.loc[mask, "sted"] = d["kommune_navn"]

    return d


def apply_ny_brukt_filter(df: pd.DataFrame, choice: str) -> pd.DataFrame:
    if choice in {"Begge", "NBrukt"}:
        return df.copy()
    s = df["ny_brukt"].astype(str).str.strip().str.lower()
    if choice == "Brukt":
        return df[s == "brukt"].copy()
    # Nybygg
    return df[s.isin(["nybygg", "ny", "nytt"])].copy()


def group_key(df: pd.DataFrame, level: Level) -> pd.Series:
    if level == "Fylke":
        return df["fylke"].fillna("").astype(str)
    if level == "Kommune":
        return df["kommune_navn"].fillna("").astype(str)
    return df["sted"].fillna("").astype(str)


def filter_by_level(df: pd.DataFrame, level: Level, value: str) -> pd.DataFrame:
    value = str(value)
    if level == "Fylke":
        return df[df["fylke"].fillna("").astype(str) == value].copy()
    if level == "Kommune":
        return df[df["kommune_navn"].fillna("").astype(str) == value].copy()
    return df[df["sted"].fillna("").astype(str) == value].copy()


def snapshot_metrics(df: pd.DataFrame, day: pd.Timestamp, level: Level) -> pd.DataFrame:
    day = pd.to_datetime(day).normalize()
    d = df.dropna(subset=["dato_siste"]).copy()

    # Bruk publisert_dato som startdato hvis tilgjengelig, ellers fallback til dato_første
    d["start_dato"] = _parse_datetime_series(d.get("publisert_dato"), normalize=True)
    d["start_dato"] = d["start_dato"].fillna(d["dato_første"])
    d = d.dropna(subset=["start_dato"]).copy()

    active = d[(d["start_dato"] <= day) & (d["dato_siste"] >= day)].copy()
    if active.empty:
        return pd.DataFrame(columns=["group", "active_count", "median_totalpris", "median_m2", "mean_days_on_market"])

    active["group"] = group_key(active, level)
    active["days_on_market_today"] = (day - active["start_dato"]).dt.days

    out = active.groupby("group", dropna=False).agg(
        active_count=("finnkode", "count"),
        median_totalpris=("totalpris", "median"),
        median_m2=("m2_pris", "median"),
        mean_days_on_market=("days_on_market_today", "median")  # median for active ads that day,
    ).reset_index()

    return out


def pct_change(end: pd.Series, start: pd.Series) -> pd.Series:
    start = pd.to_numeric(start, errors="coerce")
    end = pd.to_numeric(end, errors="coerce")
    base = start.replace({0: np.nan})
    pct = (end - start) / base * 100.0
    return pct.round(0)


def build_table(df: pd.DataFrame, level: Level, start_day: pd.Timestamp, end_day: pd.Timestamp) -> pd.DataFrame:
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()

    m_start = snapshot_metrics(df, start_day, level).set_index("group")
    m_end = snapshot_metrics(df, end_day, level).set_index("group")

    groups = m_start.index.union(m_end.index)
    out = pd.DataFrame({"Sted": groups.astype(str)})

    def get(name: str, frame: pd.DataFrame, default=np.nan) -> pd.Series:
        if frame.empty:
            return pd.Series([default] * len(out))
        s = frame.reindex(groups)[name]
        return s.reset_index(drop=True)

    # Slutt-metrikker
    out["Aktive (slutt)"] = get("active_count", m_end)
    out["M2 pris (slutt)"] = get("median_m2", m_end)
    out["Totalpris median (slutt)"] = get("median_totalpris", m_end)
    out["Dager på markedet (slutt)"] = get("mean_days_on_market", m_end)

    # Start-metrikker (brukes kun for endringer)
    out["M2 pris (start)"] = get("median_m2", m_start)
    out["Dager på markedet (start)"] = get("mean_days_on_market", m_start)

    # Endringer
    out["Endring M2 (%)"] = pct_change(out["M2 pris (slutt)"], out["M2 pris (start)"])
    out["Endring dager på markedet"] = (
        pd.to_numeric(out["Dager på markedet (slutt)"], errors="coerce")
        - pd.to_numeric(out["Dager på markedet (start)"], errors="coerce")
    )

    # Velg kolonner som skal vises (Sted + ønskede kolonner)
    out = out[
        [
            "Sted",
            "Endring M2 (%)",
            "M2 pris (slutt)",
            "Endring dager på markedet",
            "Totalpris median (slutt)",
            "Aktive (slutt)",
            "Dager på markedet (slutt)",
        ]
    ].copy()

    # --- Formatering for visning ---
    def _fmt_int_space(v) -> str:
        if pd.isna(v):
            return ""
        try:
            return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception:
            return str(v)

    def _to_int(v):
        if pd.isna(v):
            return pd.NA
        try:
            return int(round(float(v)))
        except Exception:
            return pd.NA

    # Gjør om til int der ønsket
    out["Endring M2 (%)"] = out["Endring M2 (%)"].apply(_to_int).astype("Int64")
    out["Endring dager på markedet"] = out["Endring dager på markedet"].apply(_to_int).astype("Int64")
    out["Dager på markedet (slutt)"] = out["Dager på markedet (slutt)"].apply(_to_int).astype("Int64")
    out["Aktive (slutt)"] = out["Aktive (slutt)"].apply(_to_int).astype("Int64")
    out["M2 pris (slutt)"] = out["M2 pris (slutt)"].apply(_to_int).astype("Int64")
    out["Totalpris median (slutt)"] = out["Totalpris median (slutt)"].apply(_to_int).astype("Int64")

    # Sorter default på Endring M2 (%) (numerisk), før vi formatter til tekst
    out = out.sort_values(by="Endring M2 (%)", ascending=False, na_position="last").reset_index(drop=True)

    # Formatter priser med tusenskille (mellomrom) til tekst for visning
    out["M2 pris (slutt)"] = out["M2 pris (slutt)"].apply(_fmt_int_space)
    out["Totalpris median (slutt)"] = out["Totalpris median (slutt)"].apply(_fmt_int_space)

    # Resten som vanlige heltall-tekster
    for c in ["Endring M2 (%)", "Endring dager på markedet", "Aktive (slutt)", "Dager på markedet (slutt)"]:
        out[c] = out[c].apply(lambda v: "" if pd.isna(v) else str(int(v)))

    return out



def daily_series_fast(df: pd.DataFrame, start_day: pd.Timestamp, end_day: pd.Timestamp) -> pd.DataFrame:
    """
    Daglig serie for et filtert df (f.eks. ett fylke/kommune/sted).
    """
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()
    days = pd.date_range(start_day, end_day, freq="D")

    d = df.dropna(subset=["dato_siste"]).copy()
    d["start_dato"] = _parse_datetime_series(d.get("publisert_dato"), normalize=True)
    d["start_dato"] = d["start_dato"].fillna(d["dato_første"])
    d = d.dropna(subset=["start_dato"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["dato", "active_count", "median_m2", "mean_m2", "median_totalpris", "mean_totalpris", "mean_days_on_market"])

    start_i = d["start_dato"].values.astype("datetime64[D]").astype(np.int64)
    end_i = d["dato_siste"].values.astype("datetime64[D]").astype(np.int64)

    m2 = pd.to_numeric(d.get("m2_pris"), errors="coerce").to_numpy(dtype=float)
    tp = pd.to_numeric(d.get("totalpris"), errors="coerce").to_numpy(dtype=float)

    day_i = days.values.astype("datetime64[D]").astype(np.int64)
    out_rows = []

    for di, day in zip(day_i, days):
        mask = (start_i <= di) & (end_i >= di)
        if not mask.any():
            out_rows.append((day, 0, np.nan, np.nan, np.nan, np.nan, np.nan))
            continue

        m2v = m2[mask]
        tpv = tp[mask]

        days_on_market = (di - start_i[mask]).astype(float)
        mean_dom = float(np.nanmedian(days_on_market)) if days_on_market.size else np.nan  # median days on market for active ads that day

        out_rows.append((
            day,
            int(mask.sum()),
            float(np.nanmedian(m2v)) if np.isfinite(np.nanmedian(m2v)) else np.nan,
            float(np.nanmean(m2v)),
            float(np.nanmedian(tpv)) if np.isfinite(np.nanmedian(tpv)) else np.nan,
            float(np.nanmean(tpv)),
            mean_dom,
        ))

    return pd.DataFrame(out_rows, columns=["dato", "active_count", "median_m2", "mean_m2", "median_totalpris", "mean_totalpris", "mean_days_on_market"])
