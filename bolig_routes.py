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
# Hjelpefunksjon for "priser per sted"
# --------------------------------------------------

def _prepare_priser_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Rens data for pris-tabellene og lag felt for sted / gate+sted.
    - M2-pris og totalpris gjøres numeriske
    - Dager på markedet beregnes
    - 'sted' og 'gate_sted' (gate uten husnummer + sted) lages fra address
    """
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
    if "publisert_dato" in df.columns:
        publisert = pd.to_datetime(df["publisert_dato"], errors="coerce", utc=True)
        now_utc = pd.Timestamp.now("UTC")
        df["dager_paa_markedet"] = (now_utc - publisert).dt.days
    else:
        df["dager_paa_markedet"] = np.nan

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
        # Kun gatenavn, uten husnummer
        if not addr:
            return ""
        gate_raw = addr.split(",")[0].strip()
        tokens = gate_raw.split()
        tokens_uten_nr = [t for t in tokens if not any(ch.isdigit() for ch in t)]
        if not tokens_uten_nr:
            return gate_raw
        return " ".join(tokens_uten_nr)

    df["sted"] = df["address"].apply(extract_sted)
    df["gate"] = df["address"].apply(extract_gate)

    # Gate+sted: f.eks. "Grefsenveien, Oslo"
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
    """
    Flask-side for 'Priser per sted'.

    Hovednivå (query-param 'nivaa'):
      - 'fylke'  (default): én rad per fylke
      - 'sted'   : topp N steder
      - 'gate'   : topp N gate+sted

    Hovedparametre:
      - nivaa = 'fylke' | 'sted' | 'gate'
      - sort  = kolonnenavn (f.eks. 'median_m2pris', 'median_totalpris', 'median_dager', 'antall')
      - order = 'asc' | 'desc'
      - top_n = antall rader for sted/gate (default 20)
      - min_n = minimum antall observasjoner per gruppe for sted/gate (default 5)

    Detaljvisning (alle boliger i valgt gruppe):
      - detalj_nivaa = 'fylke' | 'sted' | 'gate'
      - detalj_key   = verdien (fylkenavn, sted, gate_sted)
      - nybrukt      = 'alle' | 'nybygg' | 'brukt'
      - dsort        = kolonne i detaljtabell (f.eks. 'm2pris', 'totalpris', 'dager')
      - dorder       = 'asc' | 'desc'
    """
    df_raw = get_cached_bolig_df()
    if df_raw is None or df_raw.empty:
        return render_template(
            "bolig_priser_sted.html",
            error="Fant ingen boligdata å analysere.",
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
            nybrukt_filter="alle",
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
            nybrukt_filter="alle",
            dsort=None,
            dorder=None,
        )

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
                nybrukt_filter="alle",
                dsort=None,
                dorder=None,
            )

        agg = df.groupby("fylke").agg(
            antall=("M2-pris", "size"),
            median_m2pris=("M2-pris", "median"),
            gjennomsnitt_m2pris=("M2-pris", "mean"),
            median_totalpris=("totalpris", "median"),
            median_dager=("dager_paa_markedet", "median"),
        )
        agg = agg.reset_index()

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
        )
        agg = agg.reset_index()
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
        )
        agg = agg.reset_index()
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

    nybrukt_filter = request.args.get("nybrukt", "alle").lower()
    if nybrukt_filter not in {"alle", "nybygg", "brukt"}:
        nybrukt_filter = "alle"

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

        # Filtrer på nybygg / brukt
        if nybrukt_filter == "nybygg":
            df_det = df_det[df_det["NY/Brukt"] == "Nybygg"]
        elif nybrukt_filter == "brukt":
            df_det = df_det[df_det["NY/Brukt"] == "Brukt"]

        if not df_det.empty:
            df_det = df_det.copy()
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

            for _, r in df_det.iterrows():
                fk_str, fk_url = make_finn_info(r.get("finnkode"))
                detalj_rows.append(
                    {
                        "adresse": r.get("address") or "",
                        "fylke": r.get("fylke") or "",
                        "sted": r.get("sted") or "",
                        "gate_sted": r.get("gate_sted") or "",
                        "boligtype": r.get("boligtype") or "",
                        "nybrukt": r.get("NY/Brukt") or "",
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
            elif dsort in {"adresse", "fylke", "sted", "gate_sted", "boligtype", "nybrukt"}:
                detalj_rows.sort(key=lambda r: (r.get(dsort) or ""), reverse=reverse)

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
        if (
            "publisert_dato" in df_local.columns
            and "dager_på_markedet" not in df_local.columns
        ):
            publisert = pd.to_datetime(
                df_local["publisert_dato"], errors="coerce", utc=True
            )
            today = pd.Timestamp.now(tz="UTC").normalize()
            df_local["dager_på_markedet"] = (today - publisert).dt.days

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
            df_local.groupby(group_cols)["M2-pris"]
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

    # ---- Rensing og beregning ----------------------

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
        antall_kandidater=len(sub),
        map_html=map_html,
        table_html=table_html,
    )
