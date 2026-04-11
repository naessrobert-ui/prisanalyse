# bolig_routes.py
# -*- coding: utf-8 -*- sjekk

import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from flask import Blueprint, render_template, jsonify, request, redirect, url_for
from collections import Counter
import re

bolig_bp = Blueprint("bolig", __name__, url_prefix="/bolig")

from bolig_data import load_latest_bolig_df
from bolig_varmekart_service import clean_data

from bolig_historikk_service import (
    CFG as HIST_CFG,
    load_normalized_master_cached,
    apply_ny_brukt_filter,
    build_table,
    filter_by_level,
    daily_series_fast,
    _parse_datetime_series,
)

import base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from io import BytesIO


def _parse_date(s: str):
    if not s:
        return None
    return pd.to_datetime(s, errors="coerce")

@bolig_bp.route("/historikk/")
def bolig_historikk_view():
    level = request.args.get("level", "Fylke")
    ny_brukt = request.args.get("ny_brukt", "Brukt")

    metric = request.args.get("metric", "median_m2")
    metric_options = {
        "median_m2": "Median m²-pris",
        "median_totalpris": "Median totalpris",
        "active_count": "Antall aktive annonser",
        "mean_days_on_market": "Median dager på markedet",
    }

    start = _parse_date(request.args.get("start", ""))
    end = _parse_date(request.args.get("end", ""))

    # last master fra S3 og normaliser
    base_df = load_normalized_master_cached(HIST_CFG.s3_bucket, HIST_CFG.master_key)
    df = apply_ny_brukt_filter(base_df, ny_brukt)

    # Bruk publisert_dato som startdato hvis tilgjengelig, ellers fallback til dato_første
    df["start_dato"] = df["publisert_dato"].fillna(df["dato_første"])

    min_day = df["start_dato"].min()
    max_day = df["dato_siste"].max()


    # Liste med alle tilgjengelige datoer (for JS datepicker i template)
    try:
        available_dates_js = (
            pd.date_range(min_day, max_day, freq="D")
            .strftime("%Y-%m-%d")
            .tolist()
        )
    except Exception:
        available_dates_js = []
    # Hvis dataset mangler datoer: returnér tomt, men med alle template-variabler
    if pd.isna(min_day) or pd.isna(max_day):
        return render_template(
            "bolig_historikk.html",
            level=level,
            ny_brukt=ny_brukt,
            start=None,
            end=None,
            min_day=None,
            max_day=None,
            columns=[],
            rows=[],
            metric=metric,
            metric_options=metric_options,
            available_dates_js=available_dates_js,
        )

    today = pd.Timestamp.today().normalize()

    # Defaults: siste ~30 dager -> i dag (clampet til datasettets min/max)
    if start is None or pd.isna(start):
        start = today - pd.Timedelta(days=30)
    if end is None or pd.isna(end):
        end = today

    if start < min_day:
        start = min_day
    if end > max_day:
        end = max_day
    if end < start:
        end = start


    table = build_table(df, level, start, end)
    columns = list(table.columns)
    rows = table.to_dict(orient="records")

    return render_template(
        "bolig_historikk.html",
        level=level,
        ny_brukt=ny_brukt,
        start=str(pd.to_datetime(start).date()),
        end=str(pd.to_datetime(end).date()),
        min_day=str(pd.to_datetime(min_day).date()),
        max_day=str(pd.to_datetime(max_day).date()),
        columns=columns,
        rows=rows,
        metric=metric,
        metric_options=metric_options,
        available_dates_js=available_dates_js,
    )



@bolig_bp.route("/historikk/detalj/")
def bolig_historikk_detalj():
    """Detaljside for valgt gruppe fra tabellen."""
    level = request.args.get("level", "Fylke")
    value = request.args.get("value", "")
    ny_brukt = request.args.get("ny_brukt", "Brukt")

    start = _parse_date(request.args.get("start", ""))
    end = _parse_date(request.args.get("end", ""))

    base_df = load_normalized_master_cached(HIST_CFG.s3_bucket, HIST_CFG.master_key)
    df = apply_ny_brukt_filter(base_df, ny_brukt)
    df = filter_by_level(df, level, value)

    if df.empty or start is None or end is None or pd.isna(start) or pd.isna(end):
        return render_template(
            "bolig_historikk_detalj.html",
            level=level,
            value=value,
            ny_brukt=ny_brukt,
            start=None,
            end=None,
            plot_png=None,
            stats=None,
            detail_columns=[],
            detail_rows=[],
            removed_columns=[],
            removed_rows=[],
        )

    start_n = pd.to_datetime(start).normalize()
    end_n = pd.to_datetime(end).normalize()

    # Sørg for konsistente datoer + start_dato
    df = df.copy()
    df["start_dato"] = df["publisert_dato"].fillna(df["dato_første"])

    # ---------- 1) Daglig serie for aktive annonser ----------
    ser = daily_series_fast(df, start_n, end_n)

    # ---------- 2) "Flow": nye og forsvunne annonser per dag ----------
    d_period = df[
        df["start_dato"].notna()
        & df["dato_siste"].notna()
        & (df["start_dato"] <= end_n)
        & (df["dato_siste"] >= start_n)
    ].copy()

    # Nye annonser: publisert_dato i perioden
    new_ads = d_period[(d_period["publisert_dato"] >= start_n) & (d_period["publisert_dato"] <= end_n)].copy()
    # Forsvunnet: dato_siste i perioden
    gone_ads = d_period[(d_period["dato_siste"] >= start_n) & (d_period["dato_siste"] <= end_n)].copy()

    if not new_ads.empty:
        new_ads["day"] = new_ads["publisert_dato"]
    if not gone_ads.empty:
        gone_ads["day"] = gone_ads["dato_siste"]

    daily_new = (
        new_ads.groupby("day", dropna=False).agg(
            new_count=("finnkode", "count"),
            new_m2_median=("m2_pris", "median"),
        ).reset_index()
        if not new_ads.empty else pd.DataFrame(columns=["day", "new_count", "new_m2_median"])
    )
    daily_gone = (
        gone_ads.groupby("day", dropna=False).agg(
            gone_count=("finnkode", "count"),
            gone_m2_median=("m2_pris", "median"),
        ).reset_index()
        if not gone_ads.empty else pd.DataFrame(columns=["day", "gone_count", "gone_m2_median"])
    )

    days = pd.date_range(start_n, end_n, freq="D")
    flow = pd.DataFrame({"dato": days})
    flow = flow.merge(daily_new.rename(columns={"day": "dato"}), on="dato", how="left")
    flow = flow.merge(daily_gone.rename(columns={"day": "dato"}), on="dato", how="left")

    # ---------- 3) Plot: 3x2 (aktive + flow) ----------
    fig, axes = plt.subplots(3, 2, figsize=(18, 11))
    axes = axes.flatten()

    # (A) Aktive (samme som før)
    plots = [
        ("median_m2", "Median m²-pris (aktive)", "kr/m²"),
        ("median_totalpris", "Median totalpris (aktive)", "kr"),
        ("active_count", "Antall aktive annonser", "antall"),
        ("mean_days_on_market", "Median dager på markedet (aktive)", "dager"),
    ]
    for ax, (col, title, unit) in zip(axes[:4], plots):
        ax.plot(ser["dato"], pd.to_numeric(ser.get(col), errors="coerce"))
        ax.set_title(title)
        ax.set_ylabel(unit)
        ax.tick_params(axis="x", rotation=30)
        ax.grid(alpha=0.2)

    # (B) Flow: m²-pris nye vs forsvunne
    ax5 = axes[4]
    ax5.plot(flow["dato"], pd.to_numeric(flow.get("new_m2_median"), errors="coerce"), label="Nye annonser")
    ax5.plot(flow["dato"], pd.to_numeric(flow.get("gone_m2_median"), errors="coerce"), label="Forsvunnet annonser")
    ax5.set_title("Median m²-pris – nye vs forsvunne")
    ax5.set_ylabel("kr/m²")
    ax5.tick_params(axis="x", rotation=30)
    ax5.grid(alpha=0.2)
    ax5.legend()

    # (C) Flow: antall nye vs forsvunne
    ax6 = axes[5]
    ax6.plot(flow["dato"], pd.to_numeric(flow.get("new_count"), errors="coerce"), label="Nye")
    ax6.plot(flow["dato"], pd.to_numeric(flow.get("gone_count"), errors="coerce"), label="Forsvunnet")
    ax6.set_title("Antall per dag – nye vs forsvunne")
    ax6.set_ylabel("antall")
    ax6.tick_params(axis="x", rotation=30)
    ax6.grid(alpha=0.2)
    ax6.legend()

    fig.suptitle(f"{value} ({ny_brukt})", y=1.01)
    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=160)
    plt.close(fig)
    buf.seek(0)
    plot_png = base64.b64encode(buf.read()).decode("utf-8")

    stats = {
        "rader": int(len(df)),
        "aktive_max": int(pd.to_numeric(ser.get("active_count"), errors="coerce").max() or 0),
        "nye_sum": int(pd.to_numeric(flow.get("new_count"), errors="coerce").sum() or 0),
        "forsvunnet_sum": int(pd.to_numeric(flow.get("gone_count"), errors="coerce").sum() or 0),
    }

    # ---------- 4) Tabeller ----------
    # A) Overlapper perioden (samme som før)
    df_list = d_period.copy()
    df_list["dager_paa_markedet_slutt"] = (end_n - df_list["start_dato"]).dt.days
    df_list.loc[df_list["dager_paa_markedet_slutt"] < 0, "dager_paa_markedet_slutt"] = 0

    # B) Forsvunnet i perioden (dato_siste i [start, end])
    df_removed = df_list[(df_list["dato_siste"] >= start_n) & (df_list["dato_siste"] <= end_n)].copy()
    df_removed["dager_paa_markedet"] = (df_removed["dato_siste"] - df_removed["start_dato"]).dt.days
    df_removed.loc[df_removed["dager_paa_markedet"] < 0, "dager_paa_markedet"] = 0

    def fmt_int_space(v):
        if pd.isna(v):
            return ""
        try:
            return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception:
            return str(v)

    _months = {
        1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "mai", 6: "jun",
        7: "jul", 8: "aug", 9: "sep", 10: "okt", 11: "nov", 12: "des"
    }

    def fmt_date_no(v):
        if pd.isna(v) or v == "":
            return ""
        try:
            dt = pd.to_datetime(v).to_pydatetime().date()
            return f"{dt.day:02d}. {_months[dt.month]} {dt.year}"
        except Exception:
            return str(v)

    def finn_url(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        try:
            fk = str(int(float(val)))
        except Exception:
            fk = str(val)
        return f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk}"

    col = lambda *cands: next((c for c in cands if c in df_list.columns), None)

    c_finn = col("finnkode", "FINN-kode", "finn_code")
    c_title = col("full_title", "tittel", "title")
    c_addr = col("address", "adresse")
    c_place = col("sted", "place")
    c_type = col("boligtype", "property_type", "type")
    c_ny = col("ny_brukt", "nybrukt", "new_used")
    c_area = col("areal", "size", "areal_m2", "bruksareal", "kvm", "area")
    c_m2 = col("m2_pris", "m2_price", "m2pris")
    c_total = col("totalpris", "total_price", "pris")

    # --- Overlapp-tabell ---
    detail_columns = [
        "FINN", "Tittel", "Adresse", "Sted", "Boligtype", "Ny/Brukt",
        "Areal", "M²-pris", "Totalpris", "Publisert", "Siste dato", "Dager (slutt)"
    ]
    detail_rows = []
    for _, r in df_list.iterrows():
        fk = r.get(c_finn) if c_finn else ""
        url = finn_url(fk)
        if fk is None or (isinstance(fk, float) and pd.isna(fk)):
            fk_txt = ""
        else:
            try:
                fk_txt = str(int(float(fk)))
            except Exception:
                fk_txt = str(fk)

        detail_rows.append({
            "FINN": fk_txt,
            "Tittel": "" if not c_title else (r.get(c_title) or ""),
            "Adresse": "" if not c_addr else (r.get(c_addr) or ""),
            "Sted": "" if not c_place else (r.get(c_place) or ""),
            "Boligtype": "" if not c_type else (r.get(c_type) or ""),
            "Ny/Brukt": "" if not c_ny else (r.get(c_ny) or ""),
            "Areal": fmt_int_space(r.get(c_area)) if c_area else "",
            "M²-pris": fmt_int_space(r.get(c_m2)) if c_m2 else "",
            "Totalpris": fmt_int_space(r.get(c_total)) if c_total else "",
            "Publisert": fmt_date_no(r.get("publisert_dato") if pd.notna(r.get("publisert_dato")) else r.get("dato_første")),
            "Siste dato": fmt_date_no(r.get("dato_siste")),
            "Dager (slutt)": fmt_int_space(r.get("dager_paa_markedet_slutt")),
            "FINN_URL": url,
        })

    # --- Forsvunnet-tabell ---
    removed_columns = [
        "FINN", "Tittel", "Adresse", "Sted", "Boligtype",
        "Areal", "M²-pris", "Totalpris", "Publisert", "Forsvunnet", "Dager på markedet"
    ]
    removed_rows = []
    for _, r in df_removed.sort_values("dato_siste").iterrows():
        fk = r.get(c_finn) if c_finn else ""
        url = finn_url(fk)
        if fk is None or (isinstance(fk, float) and pd.isna(fk)):
            fk_txt = ""
        else:
            try:
                fk_txt = str(int(float(fk)))
            except Exception:
                fk_txt = str(fk)

        removed_rows.append({
            "FINN": fk_txt,
            "Tittel": "" if not c_title else (r.get(c_title) or ""),
            "Adresse": "" if not c_addr else (r.get(c_addr) or ""),
            "Sted": "" if not c_place else (r.get(c_place) or ""),
            "Boligtype": "" if not c_type else (r.get(c_type) or ""),
            "Areal": fmt_int_space(r.get(c_area)) if c_area else "",
            "M²-pris": fmt_int_space(r.get(c_m2)) if c_m2 else "",
            "Totalpris": fmt_int_space(r.get(c_total)) if c_total else "",
            "Publisert": fmt_date_no(r.get("publisert_dato") if pd.notna(r.get("publisert_dato")) else r.get("dato_første")),
            "Forsvunnet": fmt_date_no(r.get("dato_siste")),
            "Dager på markedet": fmt_int_space(r.get("dager_paa_markedet")),
            "FINN_URL": url,
        })

    return render_template(
        "bolig_historikk_detalj.html",
        level=level,
        value=value,
        ny_brukt=ny_brukt,
        start=str(pd.to_datetime(start_n).date()),
        end=str(pd.to_datetime(end_n).date()),
        plot_png=plot_png,
        stats=stats,
        detail_columns=detail_columns,
        detail_rows=detail_rows,
        removed_columns=removed_columns,
        removed_rows=removed_rows,
    )


# --------------------------------------------------
# Caching av siste bolig-DataFrame fra S3
# --------------------------------------------------


_OSLO_TZ = ZoneInfo("Europe/Oslo")
_BOLIG_DF_CACHE = {"df": None, "loaded_at": None}
_BOLIG_DAILY_REFRESH_HOUR = int(os.getenv("BOLIG_DAILY_REFRESH_HOUR", "8"))
_BOLIG_CACHE_MAX_AGE_HOURS = int(os.getenv("BOLIG_CACHE_MAX_AGE_HOURS", "30"))


def _daily_refresh_cutoff(now_local: datetime) -> datetime:
    return now_local.replace(
        hour=_BOLIG_DAILY_REFRESH_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )


def _should_refresh_bolig_cache(now_local: datetime, force_refresh: bool) -> bool:
    if force_refresh:
        return True

    if _BOLIG_DF_CACHE["df"] is None or _BOLIG_DF_CACHE["loaded_at"] is None:
        return True

    loaded_at = _BOLIG_DF_CACHE["loaded_at"]
    cutoff_today = _daily_refresh_cutoff(now_local)

    # Filen oppdateres normalt rundt kl. 08:00. Hvis vi har cache fra før cutoff,
    # og nå er passert cutoff, må vi hente på nytt.
    if now_local >= cutoff_today and loaded_at < cutoff_today:
        return True

    # Sikkerhetsnett hvis daglig refresh av en eller annen grunn uteblir.
    max_age = timedelta(hours=_BOLIG_CACHE_MAX_AGE_HOURS)
    if now_local - loaded_at >= max_age:
        return True

    return False


def get_cached_bolig_df(force_refresh: bool = False):
    """
    Returnerer cacha DataFrame fra S3.

    Cache oppdateres når vi passerer daglig oppdateringstid (default kl. 08:00 Oslo),
    eller ved force_refresh=True.
    """
    now_local = datetime.now(_OSLO_TZ)

    if not _should_refresh_bolig_cache(now_local, force_refresh):
        return _BOLIG_DF_CACHE["df"]

    try:
        df = load_latest_bolig_df()
        _BOLIG_DF_CACHE["df"] = df
        _BOLIG_DF_CACHE["loaded_at"] = now_local
        return df
    except Exception as e:
        print(f"[CACHE ERROR] Kunne ikke laste boligdata: {e}")
        return _BOLIG_DF_CACHE["df"]


def _resolve_days_on_market(df: pd.DataFrame) -> pd.Series:
    """
    Finn beste tilgjengelige verdi for dager på markedet.

    Prioriterer ferdigberegnede kolonner fra datakilden,
    og faller tilbake til beregning fra publiseringsdato.
    """
    day_columns = [
        "dager_paa_markedet",
        "dager_på_markedet",
        "Dager på markedet",
        "days_on_market",
    ]

    for col in day_columns:
        if col in df.columns:
            days = pd.to_numeric(df[col], errors="coerce")
            if days.notna().any():
                return days

    date_columns = ["publisert_dato", "published", "dato_første"]
    for col in date_columns:
        if col in df.columns:
            publisert = _parse_datetime_series(df[col], normalize=False).dt.tz_localize(
                "UTC", nonexistent="NaT", ambiguous="NaT"
            )
            now_utc = pd.Timestamp.now("UTC")
            return (now_utc - publisert).dt.days

    return pd.Series(np.nan, index=df.index)


def _resolve_days_on_market(df: pd.DataFrame) -> pd.Series:
    """
    Finn beste tilgjengelige verdi for dager på markedet.

    Prioriterer ferdigberegnede kolonner fra datakilden,
    og faller tilbake til beregning fra publiseringsdato.
    """
    day_columns = [
        "dager_paa_markedet",
        "dager_på_markedet",
        "Dager på markedet",
        "days_on_market",
    ]

    for col in day_columns:
        if col in df.columns:
            days = pd.to_numeric(df[col], errors="coerce")
            if days.notna().any():
                return days

    date_columns = ["publisert_dato", "published", "dato_første"]
    for col in date_columns:
        if col in df.columns:
            publisert = _parse_datetime_series(df[col], normalize=False).dt.tz_localize(
                "UTC", nonexistent="NaT", ambiguous="NaT"
            )
            now_utc = pd.Timestamp.now("UTC")
            return (now_utc - publisert).dt.days

    return pd.Series(np.nan, index=df.index)

# --------------------------------------------------
# Hjelpefunksjon for "priser per sted"
# --------------------------------------------------

def _prepare_priser_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.str.strip()

    # --- M2-pris ---
    if "M2-pris" not in df.columns:
        raise ValueError("Datasettet mangler kolonnen 'M2-pris'.")

    df["M2-pris"] = (
        df["M2-pris"]
        .astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- Totalpris ---
    if "totalpris" in df.columns:
        df["totalpris"] = (
            df["totalpris"]
            .astype(str)
            .str.replace("kr", "", regex=False, case=False)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["totalpris"] = pd.to_numeric(df["totalpris"], errors="coerce")
    else:
        df["totalpris"] = np.nan

    # --- Dager på markedet ---
    df["dager_paa_markedet"] = _resolve_days_on_market(df)

    # --- Adresse / sted / gate ---
    if "address" not in df.columns:
        df["address"] = ""
    df["address"] = df["address"].fillna("").astype(str)

    def extract_sted(addr: str) -> str:
        if not addr:
            return ""
        parts = addr.split(",")
        if len(parts) < 2:
            return ""
        return parts[-1].strip()

    def extract_gate(addr: str) -> str:
        if not addr:
            return ""
        gate_raw = addr.split(",")[0].strip()
        tokens = gate_raw.split()
        tokens_uten_nr = [t for t in tokens if not any(ch.isdigit() for ch in t)]
        return " ".join(tokens_uten_nr) if tokens_uten_nr else gate_raw

    df["sted"] = df["address"].apply(extract_sted)
    df["gate"] = df["address"].apply(extract_gate)

    def make_gate_sted(row):
        gate = (row.get("gate") or "").strip()
        sted = (row.get("sted") or "").strip()
        if gate and sted:
            return f"{gate}, {sted}"
        return gate or sted

    df["gate_sted"] = df.apply(make_gate_sted, axis=1)

    # Filtrer bort åpenbart tull
    df = df[df["M2-pris"] > 5000]

    # --- NY/Brukt (for filtrering i detaljtabell) ---
    if "NY/Brukt" not in df.columns:
        df["NY/Brukt"] = "Ukjent"
    df["NY/Brukt"] = df["NY/Brukt"].fillna("Ukjent").astype(str).str.strip()

    def _norm_nybrukt(x: str) -> str:
        xl = x.lower()
        if "ny" in xl:
            return "Nybygg"
        if "brukt" in xl:
            return "Brukt"
        return "Ukjent"

    df["NY/Brukt"] = df["NY/Brukt"].apply(_norm_nybrukt)

    return df

# --------------------------------------------------
# 1) Bolig-hub (menyside)
# --------------------------------------------------


@bolig_bp.route("/")
def bolig_hub():
    """
    Oversiktsside for bolig-analysene.
    """
    return render_template("bolig_hub.html")


# --------------------------------------------------
# 2) Priser per sted – REN FLASK-VERSJON
# --------------------------------------------------

@bolig_bp.route("/priser-sted/")
def bolig_priser_sted():
    """Flask-side for 'Priser per sted'.

    Hovednivå (query-param 'nivaa'):
      - 'fylke' (default): én rad per fylke
      - 'sted' : topp N steder
      - 'gate' : topp N gate+sted

    Parametre:
      - nivaa = 'fylke' | 'sted' | 'gate'
      - sort  = kolonnenavn (f.eks. 'median_m2pris', 'median_totalpris', 'median_dager', 'antall')
      - order = 'asc' | 'desc'
      - top_n = antall rader for sted/gate (default 20)
      - min_n = minimum antall observasjoner per gruppe for sted/gate (default 5)

    Detaljvisning:
      - detalj_nivaa = 'fylke' | 'sted' | 'gate'
      - detalj_key   = verdien (fylkenavn, sted, gate_sted)
      - nybrukt      = 'alle' | 'nybygg' | 'brukt'
      - dsort        = kolonne i detaljtabell (f.eks. 'm2pris', 'totalpris', 'dager')
      - dorder       = 'asc' | 'desc'
    """

    # ---- Globalt filter: nybygg / brukt / alle ----
    nybrukt_filter = request.args.get("nybrukt", "alle").lower()
    if nybrukt_filter not in {"alle", "nybygg", "brukt"}:
        nybrukt_filter = "alle"

    df_raw = get_cached_bolig_df()
    if df_raw is None or df_raw.empty:
        return render_template(
            "bolig_priser_sted.html",
            error="Fant ingen boligdata å analysere.",
            has_data=False,
            nybrukt_filter=nybrukt_filter,
            rows=[],
            columns=[],
            mode="fylke",
            title="Bolig – priser per sted",
            lead_text="",
            show_top_n=False,
            top_n=None,
            sort=None,
            order=None,
            min_n=None,
            detalj_nivaa=None,
            detalj_key=None,
            detalj_title=None,
            detalj_rows=[],
            dsort=None,
            dorder=None,
        )

    try:
        df = _prepare_priser_df(df_raw)
    except Exception as e:
        return render_template(
            "bolig_priser_sted.html",
            error=f"Feil ved klargjøring av boligdata: {e}",
            has_data=False,
            rows=[],
            columns=[],
            mode="fylke",
            title="Bolig – priser per sted",
            lead_text="",
            show_top_n=False,
            top_n=None,
            sort=None,
            order=None,
            min_n=None,
            detalj_nivaa=None,
            detalj_key=None,
            detalj_title=None,
            detalj_rows=[],
            nybrukt_filter=nybrukt_filter,
            dsort=None,
            dorder=None,
        )

    # ---- Bruk filter (nå som df finnes) ----
    if nybrukt_filter == "nybygg":
        df = df[df["NY/Brukt"] == "Nybygg"]
    elif nybrukt_filter == "brukt":
        df = df[df["NY/Brukt"] == "Brukt"]

    # ---- Hovedparametre ----
    mode = request.args.get("nivaa", "fylke")
    if mode not in {"fylke", "sted", "gate"}:
        mode = "fylke"

    try:
        top_n = int(request.args.get("top_n", "20"))
    except ValueError:
        top_n = 20
    top_n = max(1, min(top_n, 200))

    try:
        min_n = int(request.args.get("min_n", "5"))
    except ValueError:
        min_n = 5
    min_n = max(1, min(min_n, 500))

    sort_col = request.args.get("sort", None)
    order = request.args.get("order", None)
    if order not in {"asc", "desc"}:
        order = None

    # ---- Aggregasjon til hovedtabell ----
    if mode == "fylke":
        if "fylke" not in df.columns:
            return render_template(
                "bolig_priser_sted.html",
                error="Datasettet mangler kolonnen 'fylke'.",
                has_data=False,
                rows=[],
                columns=[],
                mode=mode,
                title="Bolig – priser per fylke",
                lead_text="",
                show_top_n=False,
                top_n=None,
                sort=sort_col,
                order=order,
                min_n=min_n,
                detalj_nivaa=None,
                detalj_key=None,
                detalj_title=None,
                detalj_rows=[],
                nybrukt_filter=nybrukt_filter,
                dsort=None,
                dorder=None,
            )

        agg = df.groupby("fylke").agg(
            antall=("M2-pris", "size"),
            median_m2pris=("M2-pris", "median"),
            gjennomsnitt_m2pris=("M2-pris", "mean"),
            median_totalpris=("totalpris", "median"),
            median_dager=("dager_paa_markedet", "median"),
        ).reset_index()

        title = "Bolig – priser per fylke"
        lead_text = (
            "Aggregert oversikt over boligmarkedet per fylke – antall boliger, "
            "m²-priser, median totalpris og median antall dager annonsene har ligget ute "
            "i den siste datasamlingen."
        )
        columns = [
            ("fylke", "Fylke"),
            ("antall", "Antall boliger"),
            ("median_m2pris", "Median m²-pris"),
            ("gjennomsnitt_m2pris", "Snitt m²-pris"),
            ("median_totalpris", "Median totalpris"),
            ("median_dager", "Median dager på markedet"),
        ]
        show_top_n = False

        if sort_col is None:
            sort_col = "fylke"
            order = "asc"

    elif mode == "sted":
        df_sted = df[df["sted"] != ""].copy()
        agg = df_sted.groupby("sted").agg(
            antall=("M2-pris", "size"),
            median_m2pris=("M2-pris", "median"),
            median_totalpris=("totalpris", "median"),
            median_dager=("dager_paa_markedet", "median"),
        ).reset_index()
        agg = agg[agg["antall"] >= min_n]

        title = "Bolig – priser per sted"
        lead_text = (
            f"Topp {top_n} steder basert på valgt sortering. "
            f"Kun steder med minst {min_n} boliger er tatt med."
        )
        columns = [
            ("sted", "Sted"),
            ("antall", "Antall boliger"),
            ("median_m2pris", "Median m²-pris"),
            ("median_totalpris", "Median totalpris"),
            ("median_dager", "Median dager på markedet"),
        ]
        show_top_n = True

        if sort_col is None:
            sort_col = "median_m2pris"
        if order is None:
            order = "desc"

    else:  # mode == "gate"
        df_gate = df[df["gate_sted"] != ""].copy()
        agg = df_gate.groupby("gate_sted").agg(
            antall=("M2-pris", "size"),
            median_m2pris=("M2-pris", "median"),
            median_totalpris=("totalpris", "median"),
            median_dager=("dager_paa_markedet", "median"),
        ).reset_index()
        agg = agg[agg["antall"] >= min_n]

        title = "Bolig – priser per gate"
        lead_text = (
            f"Topp {top_n} gate+sted-kombinasjoner (gatenavn uten husnummer, pluss sted). "
            f"Kun gater med minst {min_n} boliger er tatt med."
        )
        columns = [
            ("gate_sted", "Gate, sted"),
            ("antall", "Antall boliger"),
            ("median_m2pris", "Median m²-pris"),
            ("median_totalpris", "Median totalpris"),
            ("median_dager", "Median dager på markedet"),
        ]
        show_top_n = True

        if sort_col is None:
            sort_col = "median_m2pris"
        if order is None:
            order = "desc"

    # ---- Sortering i hovedtabell ----
    ascending = (order == "asc")
    if sort_col in agg.columns:
        agg = agg.sort_values(sort_col, ascending=ascending)

    if show_top_n:
        agg = agg.head(top_n)

    # ---- Formater tall med tusenskille (mellomrom) ----
    def fmt_int(v):
        if pd.isna(v):
            return ""
        try:
            return f"{int(round(v)):,}".replace(",", " ")
        except Exception:
            return str(v)

    for col in ["antall", "median_m2pris", "gjennomsnitt_m2pris", "median_totalpris", "median_dager"]:
        if col in agg.columns:
            agg[col] = agg[col].apply(fmt_int)

    rows = agg.to_dict(orient="records")

    # --------------------------------------------------
    # DETALJVISNING
    # --------------------------------------------------
    detalj_nivaa = request.args.get("detalj_nivaa")
    detalj_key = request.args.get("detalj_key")

    dsort = request.args.get("dsort", "m2pris")
    dorder = request.args.get("dorder", "desc")
    if dorder not in {"asc", "desc"}:
        dorder = "desc"

    detalj_rows = []
    detalj_title = None

    if detalj_nivaa in {"fylke", "sted", "gate"} and detalj_key:
        if detalj_nivaa == "fylke":
            df_det = df[df["fylke"] == detalj_key].copy()
            detalj_title = f"Detaljert liste – fylke: {detalj_key}"
        elif detalj_nivaa == "sted":
            df_det = df[df["sted"] == detalj_key].copy()
            detalj_title = f"Detaljert liste – sted: {detalj_key}"
        else:  # gate
            df_det = df[df["gate_sted"] == detalj_key].copy()
            detalj_title = f"Detaljert liste – gate+sted: {detalj_key}"

        if not df_det.empty:
            for col in ["M2-pris", "totalpris", "dager_paa_markedet"]:
                if col in df_det.columns:
                    df_det[col] = pd.to_numeric(df_det[col], errors="coerce")

            def make_finn_info(val):
                if pd.isna(val):
                    return (None, None)
                try:
                    fk_str = str(int(float(val)))
                except Exception:
                    fk_str = str(val)
                url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
                return (fk_str, url)

            def as_text(val):
                return "" if pd.isna(val) else str(val)

            for _, r in df_det.iterrows():
                fk_str, fk_url = make_finn_info(r.get("finnkode"))
                detalj_rows.append(
                    {
                        "adresse": as_text(r.get("address")),
                        "fylke": as_text(r.get("fylke")),
                        "sted": as_text(r.get("sted")),
                        "gate_sted": as_text(r.get("gate_sted")),
                        "boligtype": as_text(r.get("boligtype")),
                        "nybrukt": as_text(r.get("NY/Brukt")),
                        "m2pris": int(round(r["M2-pris"])) if pd.notna(r.get("M2-pris")) else None,
                        "totalpris": int(round(r["totalpris"])) if pd.notna(r.get("totalpris")) else None,
                        "dager": int(r["dager_paa_markedet"]) if pd.notna(r.get("dager_paa_markedet")) else None,
                        "finnkode": fk_str,
                        "finn_url": fk_url,
                    }
                )

            # Sortering i detaljtabell
            reverse = (dorder == "desc")

            if dsort in {"m2pris", "totalpris", "dager"}:
                def key_num(row):
                    v = row.get(dsort)
                    return v if v is not None else -1
                detalj_rows.sort(key=key_num, reverse=reverse)
            else:
                def key_text(row):
                    v = row.get(dsort)
                    if v is None:
                        return ""
                    return str(v).casefold()

                detalj_rows.sort(key=key_text, reverse=reverse)

            # Formater priser med mellomrom
            for r in detalj_rows:
                if r["m2pris"] is not None:
                    r["m2pris"] = f"{r['m2pris']:,}".replace(",", " ")
                if r["totalpris"] is not None:
                    r["totalpris"] = f"{r['totalpris']:,}".replace(",", " ")

    return render_template(
        "bolig_priser_sted.html",
        error=None,
        has_data=True,
        rows=rows,
        columns=columns,
        mode=mode,
        title=title,
        lead_text=lead_text,
        show_top_n=show_top_n,
        top_n=top_n,
        sort=sort_col,
        order=order,
        min_n=min_n,
        detalj_nivaa=detalj_nivaa,
        detalj_key=detalj_key,
        detalj_title=detalj_title,
        detalj_rows=detalj_rows,
        nybrukt_filter=nybrukt_filter,
        dsort=dsort,
        dorder=dorder,
    )
@bolig_bp.route("/salg/")
def bolig_analyse_side():
    """
    Viser analysesiden for boliger for salg (dagens løsning).
    Bruker analyse_template.html med filterboksene.
    """
    filter_data = {
        "fylker": [],
        "boligtyper": [],
        "meglere": [],
        "annonsepakker": [],
    }

    try:
        df = get_cached_bolig_df()
        if df is not None:
            if "fylke" in df.columns:
                filter_data["fylker"] = sorted(df["fylke"].dropna().unique().tolist())
            if "boligtype" in df.columns:
                filter_data["boligtyper"] = sorted(
                    df["boligtype"].dropna().unique().tolist()
                )
            if "broker_name" in df.columns:
                filter_data["meglere"] = sorted(
                    df["broker_name"].dropna().unique().tolist()
                )
            if "annonsepakke" in df.columns:
                filter_data["annonsepakker"] = sorted(
                    df["annonsepakke"].dropna().unique().tolist()
                )
    except Exception as e:
        print(f"Feil under forberedelse av bolig-filtre: {e}")

    return render_template(
        "analyse_template.html",
        tittel="Prisanalyse: Boliger for salg i Norge",
        data_url="/bolig/data",
        show_fritidsbolig_link=True,
        **filter_data,
    )


# --------------------------------------------------
# 4) API for filtrert boligdata (salg)
# --------------------------------------------------


@bolig_bp.route("/data", methods=["POST"])
def get_bolig_data():
    """
    API-endepunkt som henter og filtrerer boligdata til salgssiden.
    """
    try:
        df_full = get_cached_bolig_df()
        if df_full is None:
            return jsonify({"error": "Ingen bolig-datafil funnet"}), 404

        # Jobb alltid på en kopi, så vi ikke muterer cache-objektet
        df = df_full.copy()

        # Dager på markedet
        df["dager_paa_markedet"] = _resolve_days_on_market(df)

        for col in ["totalpris", "M2-pris", "dager_paa_markedet"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        filters = request.get_json().get("filters", {})

        if filters.get("fylke") and filters["fylke"] != "Alle":
            df = df[df["fylke"] == filters["fylke"]]

        if filters.get("status") and filters["status"] != "Alle" and "NY/Brukt" in df.columns:
            df = df[df["NY/Brukt"] == filters["status"]]

        if filters.get("totalpris_fra"):
            df = df[df["totalpris"] >= int(filters["totalpris_fra"])]
        if filters.get("totalpris_til"):
            df = df[df["totalpris"] <= int(filters["totalpris_til"])]

        if filters.get("dager_fra"):
            df = df[df["dager_paa_markedet"] >= int(filters["dager_fra"])]
        if filters.get("dager_til"):
            df = df[df["dager_paa_markedet"] <= int(filters["dager_til"])]

        if filters.get("m2pris_fra"):
            df = df[df["M2-pris"] >= int(filters["m2pris_fra"])]
        if filters.get("m2pris_til"):
            df = df[df["M2-pris"] <= int(filters["m2pris_til"])]

        if filters.get("boligtype") and filters["boligtype"] != "Alle":
            df = df[df["boligtype"] == filters["boligtype"]]

        if filters.get("megler") and filters["megler"] != "Alle":
            df = df[df["broker_name"] == filters["megler"]]

        if filters.get("annonsepakke") and filters["annonsepakke"] != "Alle":
            df = df[df["annonsepakke"] == filters["annonsepakke"]]

        if filters.get("keyword"):
            search_term = filters["keyword"]
            if "full_title" in df.columns:
                df = df[
                    df["full_title"]
                    .astype(str)
                    .str.contains(search_term, case=False, na=False)
                ]

        df = df.where(pd.notna(df), None)
        return jsonify(json.loads(df.to_json(orient="records")))

    except Exception as e:
        print(f"Feil i /bolig/data: {e}")
        return jsonify({"error": "Intern feil"}), 500


# --------------------------------------------------
# 5) Historikk-visning
# --------------------------------------------------




# --------------------------------------------------
# 6) Varmekart (Flask + Folium)
# --------------------------------------------------


@bolig_bp.route("/varmekart/")
def bolig_varmekart_view():
    """
    Varmekart over M2-pris i Norge – ren Flask-route.
    Filter styres via query-parametre (?fylke=Oslo&boligtype=Leilighet&...).
    """
    # 1) Hent og rens data
    df_raw = get_cached_bolig_df()
    if df_raw is None or df_raw.empty:
        return render_template(
            "bolig_varmekart.html",
            error="Fant ingen bolig-data å vise.",
            map_html=None,
            stats=None,
            filter_values={},
            filter_options={},
        )

    try:
        df = clean_data(df_raw)
    except Exception as e:
        return render_template(
            "bolig_varmekart.html",
            error=f"Feil ved rensing av data: {e}",
            map_html=None,
            stats=None,
            filter_values={},
            filter_options={},
        )

    # 2) Mulige filterverdier (til dropdowns)
    alle_fylker = (
        sorted(df["fylke"].dropna().unique().tolist())
        if "fylke" in df.columns
        else []
    )
    alle_typer = (
        sorted(df["boligtype"].dropna().unique().tolist())
        if "boligtype" in df.columns
        else []
    )
    alle_nybrukt = ["Alle", "Brukt", "Nybygg"]

    # 3) Les filter fra query-parametre (med defaults)
    valgt_fylke = request.args.get("fylke", "Alle")
    valgt_boligtype = request.args.get("boligtype", "Alle")
    valgt_nybrukt = request.args.get("nybrukt", "Alle")

    # Prisintervall – faller tilbake på data-min/max hvis ikke satt
    try:
        pris_min_data = int(df["M2-pris"].min())
        pris_max_data = int(df["M2-pris"].max())
    except ValueError:
        pris_min_data, pris_max_data = 0, 200_000

    pris_min = request.args.get("pris_min")
    pris_max = request.args.get("pris_max")

    if pris_min is None or pris_max is None:
        pris_min = max(pris_min_data, 20_000)
        pris_max = min(pris_max_data, 200_000)
    else:
        pris_min = int(pris_min)
        pris_max = int(pris_max)

    # 4) Filtrer DataFrame
    filtered = df.copy()

    if valgt_fylke != "Alle" and "fylke" in filtered.columns:
        filtered = filtered[filtered["fylke"] == valgt_fylke]

    if valgt_boligtype != "Alle" and "boligtype" in filtered.columns:
        filtered = filtered[filtered["boligtype"].isin([valgt_boligtype])]

    if valgt_nybrukt == "Brukt":
        filtered = filtered[filtered["NY/Brukt"] == "Brukt"]
    elif valgt_nybrukt == "Nybygg":
        filtered = filtered[filtered["NY/Brukt"] == "Nybygg"]

    filtered = filtered[
        (filtered["M2-pris"] >= pris_min)
        & (filtered["M2-pris"] <= pris_max)
    ]

    if filtered.empty:
        return render_template(
            "bolig_varmekart.html",
            error="Ingen boliger matcher filtrene.",
            map_html=None,
            stats=None,
            filter_values={
                "fylke": valgt_fylke,
                "boligtype": valgt_boligtype,
                "nybrukt": valgt_nybrukt,
                "pris_min": pris_min,
                "pris_max": pris_max,
            },
            filter_options={
                "fylker": alle_fylker,
                "boligtyper": alle_typer,
                "nybrukt": alle_nybrukt,
                "pris_min_data": pris_min_data,
                "pris_max_data": pris_max_data,
            },
        )

    # 5) Bygg folium-kart
    center_lat = filtered["latitude"].mean()
    center_lon = filtered["longitude"].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles="cartodbpositron",
    )

    heat_data = filtered[["latitude", "longitude", "M2-pris"]].values.tolist()

    from folium.plugins import HeatMap

    HeatMap(
        heat_data,
        name="M2 Pris Varmekart",
        min_opacity=0.3,
        max_zoom=15,
        radius=18,
        blur=15,
        gradient={0.2: "blue", 0.4: "cyan", 0.6: "lime", 0.8: "yellow", 1.0: "red"},
    ).add_to(m)

    map_html = m._repr_html_()  # HTML som kan embeddes direkte i template

    # 6) Enkle stats
    stats = {
        "mean_m2": int(filtered["M2-pris"].mean()),
        "max_m2": int(filtered["M2-pris"].max()),
        "count": len(filtered),
    }

    return render_template(
        "bolig_varmekart.html",
        error=None,
        map_html=map_html,
        stats=stats,
        filter_values={
            "fylke": valgt_fylke,
            "boligtype": valgt_boligtype,
            "nybrukt": valgt_nybrukt,
            "pris_min": pris_min,
            "pris_max": pris_max,
        },
        filter_options={
            "fylker": alle_fylker,
            "boligtyper": alle_typer,
            "nybrukt": alle_nybrukt,
            "pris_min_data": pris_min_data,
            "pris_max_data": pris_max_data,
        },
    )


@bolig_bp.route("/omsetningskart/")
def bolig_omsetningskart_view():
    """
    Kart over boliger som er solgt/fjernet raskt, eller aktive boliger som har ligget lenge.
    """
    base_df = load_normalized_master_cached(HIST_CFG.s3_bucket, HIST_CFG.master_key)
    if base_df is None or base_df.empty:
        return render_template(
            "bolig_omsetningskart.html",
            error="Fant ingen historiske boligdata å vise.",
            map_html=None,
            stats=None,
            filter_values={},
            filter_options={},
        )

    df = base_df.copy()
    df["start_dato"] = df["publisert_dato"].fillna(df["dato_første"])
    today = pd.Timestamp.today().normalize()
    df["er_aktiv"] = (df["start_dato"] <= today) & (df["dato_siste"] >= today)
    df["er_avsluttet"] = df["dato_siste"] < today
    df["dager_total"] = (df["dato_siste"] - df["start_dato"]).dt.days
    df["dager_aktiv"] = (today - df["start_dato"]).dt.days

    # Filtervalg
    alle_fylker = sorted(df["fylke"].dropna().astype(str).unique().tolist()) if "fylke" in df.columns else []
    alle_typer = sorted(df["boligtype"].dropna().astype(str).unique().tolist()) if "boligtype" in df.columns else []
    alle_nybrukt = ["Alle", "Brukt", "Nybygg"]
    status_options = ["Alle", "Solgt/fjernet", "Aktive ikke solgt"]

    valgt_fylke = request.args.get("fylke", "Alle")
    valgt_boligtype = request.args.get("boligtype", "Alle")
    valgt_nybrukt = request.args.get("nybrukt", "Alle")
    valgt_status = request.args.get("status", "Alle")
    sold_within_days = request.args.get("sold_within_days", "").strip()
    unsold_min_days = request.args.get("unsold_min_days", "").strip()
    run_analysis = request.args.get("run_analysis", "0") == "1"
    allow_all_norge = request.args.get("allow_all_norge", "0") == "1"

    sold_limit = int(sold_within_days) if sold_within_days.isdigit() else None
    unsold_limit = int(unsold_min_days) if unsold_min_days.isdigit() else None
    max_points_raw = request.args.get("max_points", "5000").strip()
    max_points = int(max_points_raw) if max_points_raw.isdigit() else 5000
    max_points = max(500, min(max_points, 15000))

    # To-trinns kjøring for bedre responstid:
    # 1) Velg område/filter
    # 2) Kjør analyse eksplisitt
    if not run_analysis:
        return render_template(
            "bolig_omsetningskart.html",
            error=None,
            map_html=None,
            stats=None,
            filter_values={
                "fylke": valgt_fylke,
                "boligtype": valgt_boligtype,
                "nybrukt": valgt_nybrukt,
                "status": valgt_status,
                "sold_within_days": sold_within_days,
                "unsold_min_days": unsold_min_days,
                "max_points": str(max_points),
                "allow_all_norge": "1" if allow_all_norge else "0",
            },
            filter_options={
                "fylker": alle_fylker,
                "boligtyper": alle_typer,
                "nybrukt": alle_nybrukt,
                "status": status_options,
            },
            using_thresholds=False,
            sampled=False,
            needs_run=True,
        )

    if valgt_fylke == "Alle" and not allow_all_norge:
        return render_template(
            "bolig_omsetningskart.html",
            error="Velg fylke først (anbefalt for fart), eller huk av for Hele Norge og kjør på nytt.",
            map_html=None,
            stats=None,
            filter_values={
                "fylke": valgt_fylke,
                "boligtype": valgt_boligtype,
                "nybrukt": valgt_nybrukt,
                "status": valgt_status,
                "sold_within_days": sold_within_days,
                "unsold_min_days": unsold_min_days,
                "max_points": str(max_points),
                "allow_all_norge": "1" if allow_all_norge else "0",
            },
            filter_options={
                "fylker": alle_fylker,
                "boligtyper": alle_typer,
                "nybrukt": alle_nybrukt,
                "status": status_options,
            },
            using_thresholds=False,
            sampled=False,
            needs_run=True,
        )

    filtered = df.copy()
    if valgt_fylke != "Alle" and "fylke" in filtered.columns:
        filtered = filtered[filtered["fylke"] == valgt_fylke]
    if valgt_boligtype != "Alle" and "boligtype" in filtered.columns:
        filtered = filtered[filtered["boligtype"] == valgt_boligtype]
    if valgt_nybrukt == "Brukt":
        filtered = filtered[filtered["ny_brukt"].astype(str).str.lower() == "brukt"]
    elif valgt_nybrukt == "Nybygg":
        filtered = filtered[filtered["ny_brukt"].astype(str).str.lower().isin(["nybygg", "ny", "nytt"])]

    has_thresholds = sold_limit is not None or unsold_limit is not None
    if has_thresholds:
        fast_mask = pd.Series(False, index=filtered.index)
        slow_mask = pd.Series(False, index=filtered.index)

        if sold_limit is not None:
            fast_mask = filtered["er_avsluttet"] & filtered["dager_total"].notna() & (filtered["dager_total"] <= sold_limit)
        if unsold_limit is not None:
            slow_mask = filtered["er_aktiv"] & filtered["dager_aktiv"].notna() & (filtered["dager_aktiv"] >= unsold_limit)

        filtered = filtered[fast_mask | slow_mask].copy()
    else:
        if valgt_status == "Solgt/fjernet":
            filtered = filtered[filtered["er_avsluttet"]].copy()
        elif valgt_status == "Aktive ikke solgt":
            filtered = filtered[filtered["er_aktiv"]].copy()
        else:
            filtered = filtered[(filtered["er_aktiv"] | filtered["er_avsluttet"])].copy()

    filtered["latitude"] = pd.to_numeric(filtered["latitude"], errors="coerce")
    filtered["longitude"] = pd.to_numeric(filtered["longitude"], errors="coerce")
    filtered = filtered.dropna(subset=["latitude", "longitude"])
    filtered = filtered[
        (filtered["latitude"] >= 57.0)
        & (filtered["latitude"] <= 72.0)
        & (filtered["longitude"] >= 4.0)
        & (filtered["longitude"] <= 32.0)
    ]
    total_match_count = int(len(filtered))

    # Beskyttelse mot timeout/502 i produksjon når veldig mange punkter matches.
    # Vi viser et representativt utvalg i kartet, men beholder totalantall i stats.
    sampled = False
    if len(filtered) > max_points:
        filtered = filtered.sample(n=max_points, random_state=42).copy()
        sampled = True

    if filtered.empty:
        return render_template(
            "bolig_omsetningskart.html",
            error="Ingen boliger matcher filtrene.",
            map_html=None,
            stats=None,
            filter_values={
                "fylke": valgt_fylke,
                "boligtype": valgt_boligtype,
                "nybrukt": valgt_nybrukt,
                "status": valgt_status,
                "sold_within_days": sold_within_days,
                "unsold_min_days": unsold_min_days,
                "max_points": str(max_points),
            },
            filter_options={
                "fylker": alle_fylker,
                "boligtyper": alle_typer,
                "nybrukt": alle_nybrukt,
                "status": status_options,
            },
            using_thresholds=has_thresholds,
            sampled=sampled,
            needs_run=False,
        )

    center_lat = filtered["latitude"].mean()
    center_lon = filtered["longitude"].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="cartodbpositron")
    cluster = MarkerCluster(name="Boliger").add_to(m)

    for _, row in filtered.iterrows():
        is_fast = sold_limit is not None and bool(row["er_avsluttet"]) and pd.notna(row["dager_total"]) and row["dager_total"] <= sold_limit
        is_slow = unsold_limit is not None and bool(row["er_aktiv"]) and pd.notna(row["dager_aktiv"]) and row["dager_aktiv"] >= unsold_limit

        if is_fast:
            color = "green"
            status_txt = f"Solgt/fjernet raskt (≤ {sold_limit} dager)"
            dager_txt = int(row["dager_total"])
        elif is_slow:
            color = "red"
            status_txt = f"Aktiv lenge (≥ {unsold_limit} dager)"
            dager_txt = int(row["dager_aktiv"])
        elif bool(row["er_avsluttet"]):
            color = "blue"
            status_txt = "Solgt/fjernet"
            dager_txt = int(row["dager_total"]) if pd.notna(row["dager_total"]) else None
        else:
            color = "orange"
            status_txt = "Aktiv, ikke solgt"
            dager_txt = int(row["dager_aktiv"]) if pd.notna(row["dager_aktiv"]) else None

        popup_parts = [
            f"<b>{row.get('full_title', 'Uten tittel')}</b>",
            f"Adresse: {row.get('address', 'Ukjent')}",
            f"Status: {status_txt}",
        ]
        if dager_txt is not None:
            popup_parts.append(f"Dager: {dager_txt}")
        if pd.notna(row.get("totalpris")):
            popup_parts.append(f"Totalpris: {int(row['totalpris']):,} kr".replace(",", " "))

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup("<br>".join(popup_parts), max_width=340),
        ).add_to(cluster)

    map_html = m._repr_html_()
    stats = {
        "count": total_match_count,
        "shown_count": int(len(filtered)),
        "sold_removed": int(filtered["er_avsluttet"].sum()),
        "active": int(filtered["er_aktiv"].sum()),
    }

    return render_template(
        "bolig_omsetningskart.html",
        error=None,
        map_html=map_html,
        stats=stats,
        filter_values={
            "fylke": valgt_fylke,
            "boligtype": valgt_boligtype,
            "nybrukt": valgt_nybrukt,
            "status": valgt_status,
            "sold_within_days": sold_within_days,
            "unsold_min_days": unsold_min_days,
            "max_points": str(max_points),
        },
        filter_options={
            "fylker": alle_fylker,
            "boligtyper": alle_typer,
            "nybrukt": alle_nybrukt,
            "status": status_options,
        },
        using_thresholds=has_thresholds,
        sampled=sampled,
        needs_run=False,
    )

# --------------------------------------------------
# BUZZ-ANALYSE – HJELPEFUNKSJONER
# --------------------------------------------------
# BUZZ-ANALYSE – FLASK-VERSJON
# --------------------------------------------------


@bolig_bp.route("/buzz/")
def bolig_buzz():
    """
    Flask-versjon av buzz-analysen.
    Bruker query-parametre til å styre valg (fylke, metrikk, kvantil).
    """
    df_raw = get_cached_bolig_df()
    if df_raw is None or df_raw.empty:
        return render_template(
            "bolig_buzz.html",
            error="Fant ingen boligdata til buzz-analysen.",
            has_data=False,
        )

    try:
        df = _clean_price_and_title(df_raw)
    except Exception as e:
        return render_template(
            "bolig_buzz.html",
            error=f"Feil ved klargjøring av data: {e}",
            has_data=False,
        )

    # -----------------------
    # Les parametre fra URL
    # -----------------------
    fylker = ["Hele Norge"] + sorted(df["fylke"].dropna().unique().tolist())
    valgt_fylke = request.args.get("fylke", "Hele Norge")
    if valgt_fylke not in fylker:
        valgt_fylke = "Hele Norge"

    metric_param = request.args.get("metric", "m2")  # 'm2' eller 'total'
    if metric_param == "total":
        metric_col = "totalpris"
        metric_label = "kr"
        metrikknavn = "Totalpris"
    else:
        metric_col = "M2-pris"
        metric_label = "kr/m²"
        metrikknavn = "M²-pris"

    metode = request.args.get("metode", "pct")  # bare 'pct' støttes her
    pct = int(request.args.get("pct", "10"))
    pct = max(1, min(40, pct))

    gruppe = request.args.get("gruppe", "dyreste")  # 'billigste', 'dyreste', 'midten'
    if gruppe not in {"billigste", "dyreste", "midten"}:
        gruppe = "dyreste"

    fjern_stedsnavn_param = request.args.get("fjern_stedsnavn", "1")
    fjern_stedsnavn = fjern_stedsnavn_param != "0"

    # -----------------------
    # Filtrer område
    # -----------------------
    if valgt_fylke == "Hele Norge":
        df_scope = df.copy()
    else:
        df_scope = df[df["fylke"] == valgt_fylke].copy()

    df_scope = df_scope.dropna(subset=[metric_col])
    metric_values = df_scope[metric_col].values

    if len(metric_values) == 0:
        return render_template(
            "bolig_buzz.html",
            error=f"Ingen gyldige verdier for {metric_col} i valgt område.",
            has_data=False,
        )

    # -----------------------
    # Definer prisklasse (kvantiler)
    # -----------------------
    q_low = np.percentile(metric_values, pct)
    q_high = np.percentile(metric_values, 100 - pct)

    if gruppe == "billigste":
        df_sel = df_scope[df_scope[metric_col] <= q_low]
        gruppebeskrivelse = f"Billigste {pct} %"
    elif gruppe == "dyreste":
        df_sel = df_scope[df_scope[metric_col] >= q_high]
        gruppebeskrivelse = f"Dyreste {pct} %"
    else:
        df_sel = df_scope[(df_scope[metric_col] > q_low) & (df_scope[metric_col] < q_high)]
        gruppebeskrivelse = f"Midterste {pct}–{100-pct} %"

    n_scope = len(df_scope)
    n_sel = len(df_sel)

    if n_sel == 0:
        return render_template(
            "bolig_buzz.html",
            error="Ingen boliger i valgt prisklasse.",
            has_data=False,
            fylker=fylker,
            valgt_fylke=valgt_fylke,
        )

    # -----------------------
    # Stopwords og tokenisering
    # -----------------------
    if fjern_stedsnavn:
        place_stopwords = _build_place_stopwords(df_scope)
        stopwords = BASE_STOPWORDS | place_stopwords
    else:
        stopwords = BASE_STOPWORDS

    counts_sel = _count_tokens(df_sel["full_title"], stopwords)
    df_rest = df_scope.loc[~df_scope.index.isin(df_sel.index)]
    counts_rest = _count_tokens(df_rest["full_title"], stopwords)

    if not counts_sel:
        return render_template(
            "bolig_buzz.html",
            error="Fant ingen ord å analysere i valgt prisklasse.",
            has_data=False,
        )

    # Toppliste-ord
    top_n = 30
    common_words = counts_sel.most_common(top_n)
    df_words = pd.DataFrame(common_words, columns=["ord", "antall"])

    # Differanseord
    df_diff = _compute_diff_words(counts_sel, counts_rest, min_total=5, top_n=40)

    # -----------------------
    # Tekstrapport
    # -----------------------
    median_sel = df_sel[metric_col].median()
    median_rest = df_rest[metric_col].median() if len(df_rest) > 0 else np.nan

    retning = "høyere" if median_sel > median_rest else "lavere"
    diff_prosent = (
        (median_sel - median_rest) / median_rest * 100
        if not np.isnan(median_rest) and median_rest > 0 else 0
    )

    område_txt = "i hele Norge" if valgt_fylke == "Hele Norge" else f"i {valgt_fylke}"
    metrikk_txt = "m²-pris" if metric_col == "M2-pris" else "totalpris"

    summary_lines: list[str] = []
    summary_lines.append(
        f"I denne analysen ser vi på **{gruppebeskrivelse.lower()}** "
        f"{område_txt}, målt etter **{metrikk_txt}**."
    )

    if not np.isnan(median_rest) and median_rest > 0:
        summary_lines.append(
            f"Median {metrikk_txt} i denne gruppen er omtrent "
            f"{median_sel:,.0f} {metric_label} mot {median_rest:,.0f} {metric_label} "
            f"i resten av markedet – altså ca. {diff_prosent:+.1f} % {retning}."
            .replace(",", " ")
        )

    if len(df_diff) > 0:
        n_highlight = 7
        highlight = df_diff.head(n_highlight)
        strong = highlight[highlight["relativ_faktor"] >= 3]["ord"].tolist()
        medium = highlight[
            (highlight["relativ_faktor"] < 3) & (highlight["relativ_faktor"] >= 1.5)
        ]["ord"].tolist()

        if strong:
            summary_lines.append(
                "Ord som peker seg spesielt ut i denne gruppen, sammenlignet med resten, "
                f"er blant annet **{', '.join(strong)}**."
            )
        if medium:
            summary_lines.append(
                f"I tillegg ser vi at ord som **{', '.join(medium)}** "
                "også er klart vanligere her enn i resten av markedet."
            )

    if fjern_stedsnavn:
        summary_lines.append(
            "Stedsnavn er filtrert bort, slik at forskjellene i hovedsak reflekterer "
            "hvordan boligene markedsføres – ikke hvor de ligger."
        )

    return render_template(
        "bolig_buzz.html",
        error=None,
        has_data=True,
        fylker=fylker,
        valgt_fylke=valgt_fylke,
        metric_param=metric_param,
        metode=metode,
        pct=pct,
        gruppe=gruppe,
        fjern_stedsnavn=fjern_stedsnavn,
        n_scope=n_scope,
        n_sel=n_sel,
        metrikknavn=metrikknavn,
        metric_label=metric_label,
        gruppebeskrivelse=gruppebeskrivelse,
        summary_lines=summary_lines,
        words=df_words.to_dict(orient="records"),
        diff_words=df_diff[["ord", "treff_valgt", "treff_rest", "relativ_faktor"]].to_dict(orient="records"),
    )

# --------------------------------------------------

def _clean_price_and_title(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.str.strip()

    # --- M2-pris ---
    if "M2-pris" not in df.columns:
        raise ValueError("Fant ikke kolonnen 'M2-pris' i boligdataene.")

    df["M2-pris"] = (
        df["M2-pris"]
        .astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- Totalpris (hvis finnes) ---
    if "totalpris" in df.columns:
        df["totalpris"] = (
            df["totalpris"]
            .astype(str)
            .str.replace("kr", "", regex=False, case=False)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["totalpris"] = pd.to_numeric(df["totalpris"], errors="coerce")
    else:
        df["totalpris"] = np.nan

    # --- full_title ---
    if "full_title" not in df.columns:
        raise ValueError("Fant ikke kolonnen 'full_title' i boligdataene.")
    df["full_title"] = df["full_title"].astype(str)

    # --- fylke ---
    if "fylke" not in df.columns:
        df["fylke"] = "Ukjent"
    else:
        df["fylke"] = df["fylke"].fillna("Ukjent").astype(str)

    # --- address (kan brukes til stedsnavn) ---
    if "address" not in df.columns:
        df["address"] = ""
    else:
        df["address"] = df["address"].fillna("").astype(str)

    # filtrer bort helt meningsløse m2-priser
    df = df[df["M2-pris"] > 5000]

    return df


def _build_place_stopwords(df_all: pd.DataFrame) -> set[str]:
    words: set[str] = set()

    # Fylkenavn
    for v in df_all["fylke"].dropna().astype(str):
        for tok in re.split(r"[^0-9a-zA-ZæøåÆØÅ]+", v):
            tok = tok.strip().lower()
            if len(tok) > 2:
                words.add(tok)

    # Adresse-komponenter
    for v in df_all["address"].dropna().astype(str):
        v = v.lower()
        v = re.sub(r"[^0-9a-zæøå]+", " ", v)
        for tok in v.split():
            if len(tok) > 2:
                words.add(tok)

    return words


BASE_STOPWORDS = {
    "og", "i", "på", "med", "til", "fra", "for", "av", "som", "en", "et", "den",
    "det", "de", "vi", "du", "er", "har", "kan", "må", "om", "år", "rom",
    "nye", "ny", "flott", "lekker", "meget", "stor", "pen", "fin",
    "bolig", "leilighet", "enebolig", "tomannsbolig", "rekkehus", "selges",
    "tilsalgs", "til", "salg", "midt", "sentral", "sentralbeliggende",
    "visning", "torsdag", "søndag", "mandag", "fredag", "lørdag",
}


def _tokenize(text: str, stopwords: set[str]) -> list[str]:
    text = text.lower()
    text = re.sub(r"[^0-9a-zæøå]+", " ", text)
    tokens = text.split()
    return [t for t in tokens if len(t) > 2 and t not in stopwords]


def _count_tokens(series: pd.Series, stopwords: set[str]) -> Counter:
    c: Counter = Counter()
    for t in series:
        c.update(_tokenize(t, stopwords))
    return c


def _compute_diff_words(
    counts_a: Counter,
    counts_b: Counter,
    min_total: int = 5,
    top_n: int = 30,
) -> pd.DataFrame:
    """
    Lik logikken i Streamlit-versjonen:
    ord som er relativt vanligere i gruppe A enn i gruppe B.
    """
    all_words = set(counts_a) | set(counts_b)
    total_a = sum(counts_a.values())
    total_b = sum(counts_b.values())
    alpha = 0.5  # smoothing

    rows = []
    for w in all_words:
        tot = counts_a[w] + counts_b[w]
        if tot < min_total:
            continue

        p_a = (counts_a[w] + alpha) / (total_a + alpha * len(all_words))
        p_b = (counts_b[w] + alpha) / (total_b + alpha * len(all_words))

        ratio = p_a / p_b
        log2_ratio = np.log2(ratio)
        rows.append((w, counts_a[w], counts_b[w], log2_ratio))

    if not rows:
        return pd.DataFrame(
            columns=["ord", "treff_valgt", "treff_rest", "log2_forhold", "relativ_faktor"]
        )

    df_diff_ = pd.DataFrame(
        rows,
        columns=["ord", "treff_valgt", "treff_rest", "log2_forhold"],
    )
    df_diff_["relativ_faktor"] = (2 ** df_diff_["log2_forhold"]).round(2)
    df_diff_ = df_diff_.sort_values("log2_forhold", ascending=False).head(top_n)
    return df_diff_

# --------------------------------------------------
# 7) Underprisradar – Flask-versjon
# --------------------------------------------------


@bolig_bp.route("/kupp/")
def bolig_kupp_view():
    """
    Underprisradar – kart og liste over underprisede boliger.
    Ren Flask-versjon (ingen Streamlit).
    """
    try:
        df_raw = get_cached_bolig_df()
    except Exception as e:
        return render_template(
            "bolig_kupp.html",
            error=f"Feil ved lasting av boligdata: {e}",
            has_data=False,
        )

    if df_raw is None or df_raw.empty:
        return render_template(
            "bolig_kupp.html",
            error="Fant ingen boligdata å analysere.",
            has_data=False,
        )

    # ---- Hjelpefunksjoner lokalt -----------------

    def _clean_kupp_data(df_raw_local: pd.DataFrame) -> pd.DataFrame:
        df_local = df_raw_local.copy()
        df_local.columns = [c.strip() for c in df_local.columns]

        # M2-pris
        if "M2-pris" not in df_local.columns:
            cand = [
                c
                for c in df_local.columns
                if "m2" in c.lower() and "pris" in c.lower()
            ]
            if cand:
                df_local.rename(columns={cand[0]: "M2-pris"}, inplace=True)
            else:
                raise ValueError("Fant ingen kolonne for M2-pris i boligdata.")

        df_local["M2-pris"] = (
            df_local["M2-pris"]
            .astype(str)
            .str.replace("kr", "", regex=False, case=False)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df_local["M2-pris"] = pd.to_numeric(df_local["M2-pris"], errors="coerce")

        # Totalpris
        if "totalpris" in df_local.columns:
            df_local["totalpris"] = (
                df_local["totalpris"]
                .astype(str)
                .str.replace("kr", "", regex=False, case=False)
                .str.replace(" ", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df_local["totalpris"] = pd.to_numeric(
                df_local["totalpris"], errors="coerce"
            )
        else:
            df_local["totalpris"] = np.nan

        # Koordinater
        lat_col = None
        lon_col = None
        for c in df_local.columns:
            cl = c.lower()
            if "lat" in cl and lat_col is None:
                lat_col = c
            if ("lon" in cl or "lng" in cl or "long" in cl) and lon_col is None:
                lon_col = c

        if lat_col is None or lon_col is None:
            raise ValueError("Fant ikke kolonner for latitude/longitude i boligdata.")

        for col in [lat_col, lon_col]:
            df_local[col] = df_local[col].astype(str).str.replace(",", ".", regex=False)
            df_local[col] = pd.to_numeric(df_local[col], errors="coerce")

        df_local.rename(
            columns={lat_col: "latitude", lon_col: "longitude"}, inplace=True
        )

        # Areal
        area_col = "size"
        if area_col not in df_local.columns:
            raise ValueError("Fant ikke arealkolonnen 'size' i boligdata.")

        df_local["areal_m2"] = (
            df_local[area_col]
            .astype(str)
            .str.replace("m²", "", regex=False)
            .str.replace("m2", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.replace(" ", "", regex=False)
        )
        df_local["areal_m2"] = pd.to_numeric(df_local["areal_m2"], errors="coerce")

        # Kategorier
        for col in ["fylke", "boligtype", "eierform"]:
            if col in df_local.columns:
                df_local[col] = df_local[col].fillna("Ukjent").astype(str)
            else:
                df_local[col] = "Ukjent"

        # Dager på markedet
        df_local["dager_på_markedet"] = _resolve_days_on_market(df_local)

        # Filter bort dårlige verdier
        df_local = df_local.dropna(
            subset=["latitude", "longitude", "M2-pris", "areal_m2"]
        )
        df_local = df_local[df_local["M2-pris"] > 5000]
        df_local = df_local[
            (df_local["latitude"] > 57)
            & (df_local["latitude"] < 72)
            & (df_local["longitude"] > 4)
            & (df_local["longitude"] < 32)
        ]

        return df_local

    def _add_segment_underpricing(df_local: pd.DataFrame) -> pd.DataFrame:
        df_local = df_local.copy()

        size_bins = [0, 40, 60, 80, 100, 150, 1000]
        size_labels = ["0-40", "40-60", "60-80", "80-100", "100-150", "150+"]

        df_local["størrelsesbånd"] = pd.cut(
            df_local["areal_m2"],
            bins=size_bins,
            labels=size_labels,
            right=False,
        )

        group_cols = ["fylke", "boligtype", "eierform", "størrelsesbånd"]

        stats = (
            df_local.groupby(group_cols, observed=False)["M2-pris"]
            .agg(referanse_M2="median", antall_i_segment="size")
            .reset_index()
        )

        df_local = df_local.merge(stats, on=group_cols, how="left")

        df_local["underpris_pct"] = (
            df_local["referanse_M2"] - df_local["M2-pris"]
        ) / df_local["referanse_M2"]
        df_local["underpris_kr"] = (
            df_local["referanse_M2"] - df_local["M2-pris"]
        ) * df_local["areal_m2"]

        return df_local

    def _resolve_sold_flag(df_local: pd.DataFrame) -> pd.Series:
        """
        Prøv å finne hvilke annonser som er solgt basert på tilgjengelige tekstfelter.
        Returnerer boolsk serie med samme index som input.
        """
        sold_tokens = ("solgt", "sold", "overtatt")
        negative_tokens = ("ikke solgt", "usolgt", "for salgs", "til salgs", "aktiv")

        status_candidates = [
            c for c in df_local.columns
            if any(k in c.lower() for k in ["status", "state", "utfall", "resultat"])
        ]

        sold = pd.Series(False, index=df_local.index)
        for col in status_candidates:
            s = df_local[col].fillna("").astype(str).str.lower().str.strip()
            has_sold_word = s.apply(lambda v: any(tok in v for tok in sold_tokens))
            has_negative_word = s.apply(lambda v: any(tok in v for tok in negative_tokens))
            sold = sold | (has_sold_word & ~has_negative_word)

        return sold

    def _add_quick_sale_benchmark(
        df_local: pd.DataFrame,
        quick_sale_days: int,
    ) -> pd.DataFrame:
        """
        Lager referanse basert på boliger som ser ut til å være solgt raskt.
        """
        df_local = df_local.copy()
        df_local["sold_flag"] = _resolve_sold_flag(df_local)

        if "dager_på_markedet" not in df_local.columns:
            df_local["dager_på_markedet"] = _resolve_days_on_market(df_local)

        group_cols = ["fylke", "boligtype", "eierform", "størrelsesbånd"]
        quick_sales = df_local[
            (df_local["sold_flag"])
            & (pd.to_numeric(df_local["dager_på_markedet"], errors="coerce").notna())
            & (pd.to_numeric(df_local["dager_på_markedet"], errors="coerce") <= quick_sale_days)
        ].copy()

        if quick_sales.empty:
            df_local["quick_ref_M2"] = np.nan
            df_local["quick_sale_count"] = 0
            df_local["quick_discount_pct"] = np.nan
            return df_local

        quick_stats = (
            quick_sales.groupby(group_cols)["M2-pris"]
            .agg(quick_ref_M2="median", quick_sale_count="size")
            .reset_index()
        )
        df_local = df_local.merge(quick_stats, on=group_cols, how="left")
        df_local["quick_sale_count"] = (
            pd.to_numeric(df_local["quick_sale_count"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        df_local["quick_discount_pct"] = (
            df_local["quick_ref_M2"] - df_local["M2-pris"]
        ) / df_local["quick_ref_M2"]
        return df_local

    # ---- Rensing og beregning ----------------------

    try:
        df = _clean_kupp_data(df_raw)
        df = _add_segment_underpricing(df)
        df = _add_quick_sale_benchmark(
            df,
            quick_sale_days=int(request.args.get("quick_sale_days", 14)),
        )
    except Exception as e:
        return render_template(
            "bolig_kupp.html",
            error=f"Feil ved datarensing/beregning: {e}",
            has_data=False,
        )

    # -----------------------
    # Les filter-parametre
    # -----------------------
    fylke = request.args.get("fylke", "Alle")
    boligtype = request.args.get("boligtype", "Alle")
    eierform = request.args.get("eierform", "Alle")
    min_segment_size = int(request.args.get("min_segment_size", 15))
    min_underpris_pct = float(request.args.get("min_underpris_pct", 10.0))
    top_n = int(request.args.get("top_n", 50))
    kun_dyre = request.args.get("kun_dyre", "0") == "1"
    min_dyrt_nivå = int(request.args.get("min_dyrt_nivå", 60000))
    max_days_on_market = int(request.args.get("max_days_on_market", 120))
    use_quick_sale_ref = request.args.get("use_quick_sale_ref", "1") == "1"
    quick_sale_days = int(request.args.get("quick_sale_days", 14))
    min_quick_sale_count = int(request.args.get("min_quick_sale_count", 5))
    min_quick_discount_pct = float(request.args.get("min_quick_discount_pct", 5.0))

    sub = df.copy()

    if fylke != "Alle":
        sub = sub[sub["fylke"] == fylke]

    if boligtype != "Alle":
        sub = sub[sub["boligtype"] == boligtype]

    if eierform != "Alle":
        sub = sub[sub["eierform"] == eierform]

    # Tidsfilter (reduserer "støy" fra gamle annonser)
    if "dager_på_markedet" in sub.columns:
        sub = sub[
            pd.to_numeric(sub["dager_på_markedet"], errors="coerce").fillna(9999)
            <= max_days_on_market
        ]

    # Segment-krav
    sub = sub[
        (sub["antall_i_segment"] >= min_segment_size)
        & (sub["underpris_pct"] > min_underpris_pct / 100.0)
    ]

    if kun_dyre:
        sub = sub[sub["referanse_M2"] >= min_dyrt_nivå]

    # Ekstra kvalitetssjekk mot raske salg
    if use_quick_sale_ref:
        sub = sub[
            (pd.to_numeric(sub.get("quick_sale_count", 0), errors="coerce").fillna(0) >= min_quick_sale_count)
            & (pd.to_numeric(sub.get("quick_discount_pct"), errors="coerce") > min_quick_discount_pct / 100.0)
        ]

    if len(sub) == 0:
        return render_template(
            "bolig_kupp.html",
            error="Ingen boliger matcher filtrene/kravene til underprising.",
            has_data=False,
            fylke_options=["Alle"] + sorted(df["fylke"].dropna().unique().tolist()),
            boligtype_options=["Alle"]
            + sorted(df["boligtype"].dropna().unique().tolist()),
            eierform_options=["Alle"]
            + sorted(df["eierform"].dropna().unique().tolist()),
            selected_fylke=fylke,
            selected_boligtype=boligtype,
            selected_eierform=eierform,
            min_segment_size=min_segment_size,
            min_underpris_pct=min_underpris_pct,
            top_n=top_n,
            kun_dyre=kun_dyre,
            min_dyrt_nivå=min_dyrt_nivå,
            max_days_on_market=max_days_on_market,
            use_quick_sale_ref=use_quick_sale_ref,
            quick_sale_days=quick_sale_days,
            min_quick_sale_count=min_quick_sale_count,
            min_quick_discount_pct=min_quick_discount_pct,
        )

    # Sorter og begrens topp N
    sub = sub.sort_values("underpris_pct", ascending=False).head(top_n)

    # -----------------------
    # Bygg kart (Folium)
    # -----------------------
    map_center_lat = sub["latitude"].mean()
    map_center_lon = sub["longitude"].mean()

    m = folium.Map(
        location=[map_center_lat, map_center_lon],
        zoom_start=5.5,
        tiles="cartodbpositron",
    )

    marker_cluster = MarkerCluster().add_to(m)

    for _, row in sub.iterrows():
        adresse = row.get("adresse") or row.get("address") or "Ukjent adresse"
        postnr = row.get("postnummer", "")
        sted = f"{adresse}, {postnr}" if postnr not in (None, "", np.nan) else adresse

        m2 = row["M2-pris"]
        ref = row["referanse_M2"]
        up_pct = row["underpris_pct"] * 100
        up_kr = row["underpris_kr"]
        areal = row["areal_m2"]
        tot = row.get("totalpris", np.nan)
        quick_ref = row.get("quick_ref_M2", np.nan)
        quick_discount = row.get("quick_discount_pct", np.nan)
        quick_n = row.get("quick_sale_count", 0)

        finnkode = row.get("finnkode")
        finnline = ""
        if pd.notna(finnkode):
            try:
                fk_str = str(int(float(finnkode)))
                finn_url = (
                    f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
                )
                finnline = (
                    f"<br><a href='{finn_url}' target='_blank'>"
                    f"Åpne på FINN (kode {fk_str})</a>"
                )
            except Exception:
                pass

        popup_html = (
            f"<b>{sted}</b><br>"
            f"{row.get('boligtype', '')} – {row.get('eierform', '')}<br>"
            f"Areal: {areal:.0f} m²<br>"
            f"M²-pris: {m2:,.0f} kr/m²<br>"
            f"Segment-ref: {ref:,.0f} kr/m²<br>"
            f"Underpris: {up_pct:.1f} % (~{up_kr:,.0f} kr)"
        ).replace(",", " ")

        if pd.notna(tot):
            popup_html += f"<br>Totalpris: {tot:,.0f} kr".replace(",", " ")
        if pd.notna(quick_ref):
            popup_html += (
                f"<br>Rask-salg ref (≤{quick_sale_days} dager): {quick_ref:,.0f} kr/m²"
                f"<br>Under rask-salg nivå: {(quick_discount * 100):.1f}% "
                f"(n={int(quick_n)})"
            ).replace(",", " ")

        popup_html += finnline

        tooltip_html = (
            f"<b>{sted}</b><br>"
            f"{areal:.0f} m², {m2:,.0f} kr/m²<br>"
            f"{up_pct:.1f}% under segment-ref."
        ).replace(",", " ")

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=10,
            color=None,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=folium.Tooltip(tooltip_html, sticky=True, direction="top"),
        ).add_to(marker_cluster)

    map_html = m._repr_html_()

    # -----------------------
    # Tabell (pandas -> HTML)
    # -----------------------
    display_cols = [
        "adresse",
        "postnummer",
        "fylke",
        "boligtype",
        "eierform",
        "areal_m2",
        "M2-pris",
        "referanse_M2",
        "underpris_pct",
        "underpris_kr",
        "antall_i_segment",
        "quick_ref_M2",
        "quick_discount_pct",
        "quick_sale_count",
        "dager_på_markedet",
        "finnkode",
    ]
    display_cols = [c for c in display_cols if c in sub.columns]

    table_df = sub[display_cols].copy()
    table_df["underpris_pct"] = (table_df["underpris_pct"] * 100).round(1)
    if "quick_discount_pct" in table_df.columns:
        table_df["quick_discount_pct"] = (table_df["quick_discount_pct"] * 100).round(1)
    for col in ["M2-pris", "referanse_M2", "underpris_kr"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].round(0).astype("Int64")
    if "quick_ref_M2" in table_df.columns:
        table_df["quick_ref_M2"] = table_df["quick_ref_M2"].round(0).astype("Int64")

     # 🔹 GJØR FINNKODE KLIKKBAR
    if "finnkode" in table_df.columns:
        def make_finn_link(val):
            if pd.isna(val):
                return ""
            try:
                fk_str = str(int(float(val)))
            except Exception:
                fk_str = str(val)
            url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
            return f"<a href='{url}' target='_blank'>{fk_str}</a>"

        table_df["finnkode"] = table_df["finnkode"].apply(make_finn_link)

    table_html = table_df.to_html(
        classes="table table-sm table-striped table-hover mb-0",
        index=False,
        border=0,
        escape=False,  # 🔹 VIKTIG: tillat HTML-lenker
    )

    return render_template(
        "bolig_kupp.html",
        has_data=True,
        error=None,
        fylke_options=["Alle"] + sorted(df["fylke"].dropna().unique().tolist()),
        boligtype_options=["Alle"]
        + sorted(df["boligtype"].dropna().unique().tolist()),
        eierform_options=["Alle"]
        + sorted(df["eierform"].dropna().unique().tolist()),
        selected_fylke=fylke,
        selected_boligtype=boligtype,
        selected_eierform=eierform,
        min_segment_size=min_segment_size,
        min_underpris_pct=min_underpris_pct,
        top_n=top_n,
        kun_dyre=kun_dyre,
        min_dyrt_nivå=min_dyrt_nivå,
        max_days_on_market=max_days_on_market,
        use_quick_sale_ref=use_quick_sale_ref,
        quick_sale_days=quick_sale_days,
        min_quick_sale_count=min_quick_sale_count,
        min_quick_discount_pct=min_quick_discount_pct,
        antall_kandidater=len(sub),
        map_html=map_html,
        table_html=table_html,
    )
