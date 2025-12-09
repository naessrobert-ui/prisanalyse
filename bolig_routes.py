# bolig_routes.py
# -*- coding: utf-8 -*-

import json
from functools import lru_cache

import numpy as np
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from flask import Blueprint, render_template, jsonify, request, redirect
from collections import Counter
import re


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
# 1) Bolig-hub (menyside)
# --------------------------------------------------


@bolig_bp.route("/")
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
