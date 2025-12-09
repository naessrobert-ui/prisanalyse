# bolig_routes.py
# -*- coding: utf-8 -*-

import json
from functools import lru_cache

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
    """
    Enkel Flask-side for 'Priser per sted'.

    Aggregert nivå: per fylke (kan lett bygges ut til kommune/bydel senere).
    """
    df = get_cached_bolig_df()
    if df is None or df.empty:
        return render_template(
            "bolig_priser_sted.html",
            error="Fant ingen boligdata å analysere.",
            has_data=False,
            rows=[],
            columns=[],
        )

    # Sørg for at nødvendige kolonner finnes
    if "fylke" not in df.columns:
        return render_template(
            "bolig_priser_sted.html",
            error="Datasettet mangler kolonnen 'fylke'.",
            has_data=False,
            rows=[],
            columns=[],
        )

    # Prøv å konvertere relevante kolonner til numerisk
    for col in ["M2-pris", "totalpris"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Gruppér per fylke og beregn noen enkle nøkkeltall
    agg = df.groupby("fylke").agg(
        antall=("fylke", "size"),
        median_m2pris=("M2-pris", "median"),
        gjennomsnitt_m2pris=("M2-pris", "mean"),
        median_totalpris=("totalpris", "median") if "totalpris" in df.columns else ("M2-pris", "median"),
    )

    agg = agg.reset_index()

    # Ryddige heltall der det er naturlig
    for col in ["median_m2pris", "gjennomsnitt_m2pris", "median_totalpris"]:
        if col in agg.columns:
            agg[col] = agg[col].round(0).astype("Int64")

    columns = [
        ("fylke", "Fylke"),
        ("antall", "Antall boliger"),
        ("median_m2pris", "Median m²-pris"),
        ("gjennomsnitt_m2pris", "Snitt m²-pris"),
        ("median_totalpris", "Median totalpris"),
    ]
    rows = agg.to_dict(orient="records")

    return render_template(
        "bolig_priser_sted.html",
        error=None,
        has_data=True,
        rows=rows,
        columns=columns,
    )


# --------------------------------------------------
# 3) Dagens "Boliger for salg" (salgssiden)
# --------------------------------------------------


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
        if "publisert_dato" in df.columns:
            df["publisert_dato_dt"] = pd.to_datetime(
                df["publisert_dato"], errors="coerce", utc=True
            )
            now_utc = pd.Timestamp.now("UTC")
            df["dager_paa_markedet"] = (now_utc - df["publisert_dato_dt"]).dt.days
        else:
            df["dager_paa_markedet"] = None

        for col in ["totalpris", "M2-pris", "dager_paa_markedet"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        filters = request.get_json().get("filters", {})

        if filters.get("fylke") and filters["fylke"] != "Alle":
            df = df[df["fylke"] == filters["fylke"]]

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


@bolig_bp.route("/historikk/")
def bolig_historikk_view():
    """
    Viser historisk utvikling i boligpriser.
    - Første gang (uten start/end) -> ingen CSV-lesing, bare skjema.
    - Når bruker har valgt minst én dato -> leser to filer og bygger tabell.
    """

    # 1) Hent datoene det finnes filer for (KUN listing – ingen CSV-lesing)
    available_dates = get_available_bolig_dates()  # liste av pd.Timestamp
    default_start, default_end = get_default_dates_for_ui()

    if not available_dates or default_end is None:
        # Ingen filer i det hele tatt
        return render_template(
            "bolig_historikk.html",
            metric_options=METRIC_LABELS,
            metric_key="median_totalpris",
            metric_label=METRIC_LABELS["median_totalpris"],
            start_value="",
            end_value="",
            first_date=None,
            last_date=None,
            table_html="",
            error="Fant ingen historikkfiler for boliger.",
            has_data=False,
            available_dates_js=[],
        )

    # 2) Les query-parametre
    metric = request.args.get("metric", "median_totalpris")
    start_str = (request.args.get("start") or "").strip()
    end_str = (request.args.get("end") or "").strip()

    # Brukeren har "sendt inn" bare hvis minst én dato faktisk er satt
    submitted = bool(start_str or end_str)

    # 3) Verdier som skal stå i input-feltene (tekst dd.mm.åååå)
    if start_str:
        start_value = start_str
    else:
        start_value = default_start.strftime("%d.%m.%Y")

    if end_str:
        end_value = end_str
    else:
        end_value = default_end.strftime("%d.%m.%Y")

    # 4) Bare bygg tabell hvis brukeren faktisk har valgt dato
    table_html = ""
    first_date_disp = None
    last_date_disp = None
    error = None
    has_data = False

    if submitted:

        def _parse_d(s: str | None) -> pd.Timestamp | None:
            if not s:
                return None
            return pd.to_datetime(s, format="%d.%m.%Y", errors="coerce")

        start_dt = _parse_d(start_str) or default_start
        end_dt = _parse_d(end_str) or default_end

        try:
            df, first_date, last_date = build_historikk_tabell(
                metric_col=metric,
                start_date=start_dt,
                end_date=end_dt,
            )

            table_html = df.to_html(
                classes="table table-sm table-striped table-hover mb-0",
                index=False,
                border=0,
            )
            first_date_disp = first_date.strftime("%d.%m.%Y")
            last_date_disp = last_date.strftime("%d.%m.%Y")
            has_data = True
        except Exception as e:
            error = f"Feil i historikk-beregning: {e}"
            has_data = False

    # 5) Gyldige datoer til JS (YYYY-mm-dd)
    available_dates_js = [d.strftime("%Y-%m-%d") for d in available_dates]

    # 6) Label for valgt metrikk
    metric_label = METRIC_LABELS.get(metric, METRIC_LABELS["median_totalpris"])

    # 7) Render template
    return render_template(
        "bolig_historikk.html",
        metric_options=METRIC_LABELS,
        metric_key=metric,
        metric_label=metric_label,
        start_value=start_value,
        end_value=end_value,
        first_date=first_date_disp,
        last_date=last_date_disp,
        table_html=table_html,
        error=error,
        has_data=has_data,
        available_dates_js=available_dates_js,
    )


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
            .str.replace("kr", "", rege
