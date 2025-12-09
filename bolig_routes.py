# bolig_routes.py
# -*- coding: utf-8 -*-

import json
from functools import lru_cache
from collections import Counter
import re
import os

import numpy as np
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from flask import Blueprint, render_template, jsonify, request, redirect

from bolig_data import load_latest_bolig_df
from bolig_varmekart_service import clean_data
from bolig_historikk import (
    METRIC_LABELS,
    get_available_bolig_dates,
    get_default_dates_for_ui,
    build_historikk_tabell,
)

# ÉN blueprint, med navn "bolig" og url_prefix "/bolig"
bolig_bp = Blueprint("bolig", __name__, url_prefix="/bolig")


# --------------------------------------------------
# Caching av siste bolig-DataFrame fra S3
# --------------------------------------------------
@lru_cache(maxsize=1)
def get_cached_bolig_df():
    """
    Returnerer cacha DataFrame fra S3.
    Leser kun én gang per prosess via load_latest_bolig_df().
    """
    try:
        df = load_latest_bolig_df()
        return df
    except Exception as e:
        print(f"[CACHE ERROR] Kunne ikke laste boligdata: {e}")
        return None


# --------------------------------------------------
# HJELPEFUNKSJONER – UNDERPRISRADAR
# --------------------------------------------------
def _clean_kupp_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Rensing tilpasset Underprisradar.
    Forventer kolonner som i bolig_X_*.csv.
    """
    df = df_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # --- M2-pris ---
    if "M2-pris" not in df.columns:
        cand = [c for c in df.columns if "m2" in c.lower() and "pris" in c.lower()]
        if cand:
            df.rename(columns={cand[0]: "M2-pris"}, inplace=True)
        else:
            raise ValueError("Fant ingen kolonne for M2-pris i boligdata.")

    df["M2-pris"] = (
        df["M2-pris"]
        .astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- Totalpris (kun for visning) ---
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

    # --- Koordinater ---
    lat_col = None
    lon_col = None
    for c in df.columns:
        cl = c.lower()
        if "lat" in cl and lat_col is None:
            lat_col = c
        if ("lon" in cl or "lng" in cl or "long" in cl) and lon_col is None:
            lon_col = c

    if lat_col is None or lon_col is None:
        raise ValueError("Fant ikke kolonner for latitude/longitude i boligdata.")

    for col in [lat_col, lon_col]:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.rename(columns={lat_col: "latitude", lon_col: "longitude"}, inplace=True)

    # --- Areal (size -> areal_m2) ---
    area_col = "size"
    if area_col not in df.columns:
        raise ValueError("Fant ikke arealkolonnen 'size' i boligdata.")

    df["areal_m2"] = (
        df[area_col]
        .astype(str)
        .str.replace("m²", "", regex=False)
        .str.replace("m2", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df["areal_m2"] = pd.to_numeric(df["areal_m2"], errors="coerce")

    # --- Kategorier: fylke, boligtype, eierform ---
    for col in ["fylke", "boligtype", "eierform"]:
        if col in df.columns:
            df[col] = df[col].fillna("Ukjent").astype(str)
        else:
            df[col] = "Ukjent"

    # --- Dager på markedet (valgfritt) ---
    if "publisert_dato" in df.columns and "dager_på_markedet" not in df.columns:
        publisert = pd.to_datetime(df["publisert_dato"], errors="coerce", utc=True)
        today = pd.Timestamp.now(tz="UTC").normalize()
        df["dager_på_markedet"] = (today - publisert).dt.days

    # --- Filtrer bort åpenbart dårlige/manglende verdier ---
    df = df.dropna(subset=["latitude", "longitude", "M2-pris", "areal_m2"])
    df = df[df["M2-pris"] > 5000]
    df = df[
        (df["latitude"] > 57)
        & (df["latitude"] < 72)
        & (df["longitude"] > 4)
        & (df["longitude"] < 32)
    ]

    return df


def _add_segment_underpricing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Legger til segment-baserte underpris-metrikker:
    - referanse_M2 (median pris i segment)
    - antall_i_segment
    - underpris_pct, underpris_kr
    """
    df = df.copy()

    size_bins = [0, 40, 60, 80, 100, 150, 1000]
    size_labels = ["0-40", "40-60", "60-80", "80-100", "100-150", "150+"]

    df["størrelsesbånd"] = pd.cut(
        df["areal_m2"],
        bins=size_bins,
        labels=size_labels,
        right=False,
    )

    group_cols = ["fylke", "boligtype", "eierform", "størrelsesbånd"]

    stats = (
        df.groupby(group_cols)["M2-pris"]
        .agg(referanse_M2="median", antall_i_segment="size")
        .reset_index()
    )

    df = df.merge(stats, on=group_cols, how="left")

    df["underpris_pct"] = (df["referanse_M2"] - df["M2-pris"]) / df["referanse_M2"]
    df["underpris_kr"] = (df["referanse_M2"] - df["M2-pris"]) * df["areal_m2"]

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
# 5) Underprisradar – Flask-versjon
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

    try:
        df = _clean_kupp_data(df_raw)
        df = _add_segment_underpricing(df)
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

    sub = df.copy()

    if fylke != "Alle":
        sub = sub[sub["fylke"] == fylke]

    if boligtype != "Alle":
        sub = sub[sub["boligtype"] == boligtype]

    if eierform != "Alle":
        sub = sub[sub["eierform"] == eierform]

    # Segment-krav
    sub = sub[
        (sub["antall_i_segment"] >= min_segment_size)
        & (sub["underpris_pct"] > min_underpris_pct / 100.0)
    ]

    if kun_dyre:
        sub = sub[sub["referanse_M2"] >= min_dyrt_nivå]

    if len(sub) == 0:
        return render_template(
            "bolig_kupp.html",
            error="Ingen boliger matcher filtrene/kravene til underprising.",
            has_data=False,
            fylke_options=["Alle"] + sorted(df["fylke"].dropna().unique().tolist()),
            boligtype_options=["Alle"] + sorted(df["boligtype"].dropna().unique().tolist()),
            eierform_options=["Alle"] + sorted(df["eierform"].dropna().unique().tolist()),
            selected_fylke=fylke,
            selected_boligtype=boligtype,
            selected_eierform=eierform,
            min_segment_size=min_segment_size,
            min_underpris_pct=min_underpris_pct,
            top_n=top_n,
            kun_dyre=kun_dyre,
            min_dyrt_nivå=min_dyrt_nivå,
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

        finnkode = row.get("finnkode")
        finnline = ""
        if pd.notna(finnkode):
            try:
                fk_str = str(int(float(finnkode)))
                finn_url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
                finnline = f"<br><a href='{finn_url}' target='_blank'>Åpne på FINN (kode {fk_str})</a>"
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
        "finnkode",
    ]
    display_cols = [c for c in display_cols if c in sub.columns]

    table_df = sub[display_cols].copy()
    table_df["underpris_pct"] = (table_df["underpris_pct"] * 100).round(1)

    for col in ["M2-pris", "referanse_M2", "underpris_kr"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].round(0).astype("Int64")

    # Gjør FINN-kode klikkbar
    if "finnkode" in table_df.columns:
        def _make_finn_link(val):
            if pd.isna(val):
                return ""
            try:
                fk_int = int(float(val))
            except Exception:
                return val

            url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_int}"
            return f"<a href='{url}' target='_blank'>{fk_int}</a>"

        table_df["finnkode"] = table_df["finnkode"].apply(_make_finn_link)

    table_html = table_df.to_html(
        classes="table table-sm table-striped table-hover mb-0",
        index=False,
        border=0,
        escape=False,  # viktig for at <a>-taggene ikke escapes
    )

    return render_template(
        "bolig_kupp.html",
        has_data=True,
        error=None,
        fylke_options=["Alle"] + sorted(df["fylke"].dropna().unique().tolist()),
        boligtype_options=["Alle"] + sorted(df["boligtype"].dropna().unique().tolist()),
        eierform_options=["Alle"] + sorted(df["eierform"].dropna().unique().tolist()),
        selected_fylke=fylke,
        selected_boligtype=boligtype,
        selected_eierform=eierform,
        min_segment_size=min_segment_size,
        min_underpris_pct=min_underpris_pct,
        top_n=top_n,
        kun_dyre=kun_dyre,
        min_dyrt_nivå=min_dyrt_nivå,
        antall_kandidater=len(sub),
        map_html=map_html,
        table_html=table_html,
    )
