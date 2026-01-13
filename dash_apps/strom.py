# dash_apps/strom.py
# -*- coding: utf-8 -*-

import math
import json
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional, Set

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import (
    Dash, dcc, html, Input, Output, State, dash_table,
    callback_context, no_update, Patch
)


# -----------------------------
# FIL / KOLONNER
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = BASE_DIR / "static" / "data" / "kommuner.csv"
CSV_SEP = ";"

CSV_COLS = {
    "kommune": "KOMMUNE",
    "knr": "KOMMUNENUMMER",
    "bolig_np": "Bolig-Norgespris",
    "bolig_tot": "Bolig-alle",
    "fritid_np": "Fritid-Norgespris",
    "fritid_tot": "Fritid-alle",
}

CHANGE_ALIASES = {
    "oct": ["incr_oct", "INCR_OCT", "incr oct", "increase_oct", "increase oct"],
    "nov": ["incr_nov", "INCR_NOV", "incr nov", "increase_nov", "increase nov"],
    "dec": ["incr_dec", "INCR_DEC", "incr dec", "increase_dec", "increase dec"],
    "q4":  ["incr_Q4", "incr_q4", "INCR_Q4", "incr q4", "increase_q4", "increase q4"],
}


# -----------------------------
# HELPERS
# -----------------------------
def normalize_columns(cols):
    return (
        pd.Index(cols)
        .astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

def canon(s: str) -> str:
    s = str(s).strip().casefold()
    return "".join(ch for ch in s if ch.isalnum())

def find_col(actual_cols, aliases) -> Optional[str]:
    actual_cols = list(actual_cols)
    actual_c = {c: canon(c) for c in actual_cols}
    alias_c = [canon(a) for a in aliases]

    for c in actual_cols:
        if actual_c[c] in alias_c:
            return c

    for c in actual_cols:
        for a in alias_c:
            if a and a in actual_c[c]:
                return c

    return None

def normalize_kommunenr(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        s = s.zfill(4)
    return s

def fmt_pct(x: Optional[float]) -> str:
    """x er i prosentpoeng (2.3 -> '2,3%')"""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except Exception:
        return "—"

def to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False)
    s = s.str.replace("\u00a0", " ", regex=False)
    s = s.str.replace(" ", "", regex=False)

    mask_both = s.str.contains(r"\.", regex=True) & s.str.contains(",", regex=False)
    s.loc[mask_both] = s.loc[mask_both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    mask_comma = (~mask_both) & s.str.contains(",", regex=False)
    s.loc[mask_comma] = s.loc[mask_comma].str.replace(",", ".", regex=False)

    return pd.to_numeric(s, errors="coerce")

def pick_geojson_nr_key(properties: Dict) -> Optional[str]:
    for c in ["kommunenummer", "kommunenr", "KOMMUNENR", "KOMMUNE_NR", "KOMMUNENUMMER", "id", "ID"]:
        if c in properties:
            return c
    return None

def get_feature_bbox(feature: Dict) -> Tuple[float, float, float, float]:
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates") or []

    lons, lats = [], []

    def add_ring(ring: Iterable[Any]):
        for lon, lat in ring:
            lons.append(lon)
            lats.append(lat)

    if gtype == "Polygon":
        for ring in coords:
            add_ring(ring)
    elif gtype == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                add_ring(ring)

    if not lons or not lats:
        return (0.0, 0.0, 0.0, 0.0)

    return (min(lons), min(lats), max(lons), max(lats))

def feature_centroid(feature: Dict) -> Optional[Tuple[float, float]]:
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates") or []

    lons, lats = [], []

    def add_ring(ring):
        for lon, lat in ring:
            lons.append(lon)
            lats.append(lat)

    if gtype == "Polygon":
        for ring in coords:
            add_ring(ring)
    elif gtype == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                add_ring(ring)

    if not lons or not lats:
        return None
    return (float(sum(lons) / len(lons)), float(sum(lats) / len(lats)))

def bboxes_intersect(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    a_minx, a_miny, a_maxx, a_maxy = a
    b_minx, b_miny, b_maxx, b_maxy = b
    return not (a_maxx < b_minx or a_minx > b_maxx or a_maxy < b_miny or a_miny > b_maxy)

def viewport_bbox_from_relayout(relayout: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    if not relayout:
        return None

    derived = relayout.get("mapbox._derived")
    if isinstance(derived, dict):
        coords = derived.get("coordinates")
        if isinstance(coords, list) and coords and isinstance(coords[0], (list, tuple)) and len(coords[0]) == 2:
            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]
            return (min(lons), min(lats), max(lons), max(lats))

    coords2 = relayout.get("mapbox._derived.coordinates")
    if isinstance(coords2, list) and coords2 and isinstance(coords2[0], (list, tuple)) and len(coords2[0]) == 2:
        lons = [c[0] for c in coords2]
        lats = [c[1] for c in coords2]
        return (min(lons), min(lats), max(lons), max(lats))

    return None

def get_trigger_id() -> Optional[str]:
    if not callback_context.triggered:
        return None
    return callback_context.triggered[0]["prop_id"].split(".")[0]

def change_label(change_period: str) -> str:
    return {"oct": "Oktober", "nov": "November", "dec": "Desember", "q4": "Q4"}.get(change_period, "Q4")


# -----------------------------
# DATA-LOADING (én gang ved montering)
# -----------------------------
def load_resources() -> tuple[pd.DataFrame, dict, dict, list, str, dict, dict]:
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)
    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].map(normalize_kommunenr)

    change_cols_found = {k: find_col(df_raw.columns, aliases) for k, aliases in CHANGE_ALIASES.items()}

    GEOJSON_PATH = BASE_DIR / "static" / "geo" / "Kommuner-M.geojson"
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features") or []
    if not features or "properties" not in features[0]:
        raise RuntimeError("GeoJSON mangler forventet struktur (features/properties).")

    geo_nr_key = pick_geojson_nr_key(features[0]["properties"])
    if not geo_nr_key:
        raise RuntimeError("Fant ikke kommunenummer-felt i GeoJSON.")

    feature_bbox_by_nr: Dict[str, Tuple[float, float, float, float]] = {}
    centroid_by_nr: Dict[str, Tuple[float, float]] = {}

    for feat in features:
        knr = normalize_kommunenr(feat["properties"].get(geo_nr_key, ""))
        feature_bbox_by_nr[knr] = get_feature_bbox(feat)
        c = feature_centroid(feat)
        if knr and c:
            centroid_by_nr[knr] = c

    return df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr


# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr = load_resources()

    lon_by_nr = {k: v[0] for k, v in centroid_by_nr.items()}
    lat_by_nr = {k: v[1] for k, v in centroid_by_nr.items()}

    def build_df_once(mode_value: str) -> pd.DataFrame:
        df = df_raw.copy()

        if mode_value == "Bolig":
            np_col, tot_col = CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]
        else:
            np_col, tot_col = CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"]

        df["norgespris"] = to_number(df[np_col])
        df["total"] = to_number(df[tot_col])

        df["andel"] = df["norgespris"] / df["total"]
        df.loc[df["total"].isna() | (df["total"] == 0), "andel"] = pd.NA

        andel_pct = (df["andel"] * 100).round()
        df["andel_pct0"] = andel_pct.astype("Int64").astype(str).add("%")
        df.loc[df["andel"].isna(), "andel_pct0"] = "—"

        # endringskolonner (desimal i CSV: 0.023 = 2.3%)
        for _, col in change_cols_found.items():
            if col and col in df.columns:
                df[col] = to_number(df[col])

        df["knr_norm"] = df[CSV_COLS["knr"]].map(normalize_kommunenr)
        return df

    # Precompute for fart
    df_bolig = build_df_once("Bolig")
    df_fritid = build_df_once("Fritid")

    def get_df(mode_value: str) -> pd.DataFrame:
        return df_bolig if mode_value == "Bolig" else df_fritid

    DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

    def build_map_fig(
        df: pd.DataFrame,
        low: float,
        high: float,
        center: Dict[str, float],
        zoom: float,
        change_period: str,
        change_red_le: float,
        change_blue_ge: float,
        marker_scale_pct: float,
    ) -> go.Figure:
        low, high = sorted([float(low), float(high)])

        change_col = change_cols_found.get(change_period)
        period_label = change_label(change_period)

        keep = [CSV_COLS["kommune"], CSV_COLS["knr"], "knr_norm", "andel", "andel_pct0", "norgespris", "total"]
        dff = df[keep].copy()

        if change_col and change_col in df.columns:
            dff["change_pct"] = df[change_col] * 100.0
        else:
            dff["change_pct"] = float("nan")
        dff["change_pct_str"] = dff["change_pct"].map(fmt_pct)

        def kategori(share):
            if share is None or (isinstance(share, float) and math.isnan(share)):
                return "mangler"
            if share >= high:
                return f"≥ {int(round(high * 100))}%"
            if share <= low:
                return f"≤ {int(round(low * 100))}%"
            return "mellom"

        dff["kategori"] = dff["andel"].map(kategori)

        color_map = {
            f"≥ {int(round(high * 100))}%": "#1f77b4",
            f"≤ {int(round(low * 100))}%": "#d62728",
            "mellom": "#9e9e9e",
            "mangler": "#bdbdbd",
        }

        fig = px.choropleth_mapbox(
            dff,
            geojson=gj,
            locations=CSV_COLS["knr"],
            featureidkey=f"properties.{geo_nr_key}",
            color="kategori",
            color_discrete_map=color_map,
            hover_name=CSV_COLS["kommune"],
            custom_data=["andel_pct0", "change_pct_str", "norgespris", "total"],
            opacity=0.75,
        )

        fig.update_traces(
            hovertemplate=(
                f"<b>%{{hovertext}}</b><br>"
                f"Andel Norgespris: %{{customdata[0]}}<br>"
                f"Endring {period_label} (%): %{{customdata[1]}}<br>"
                f"Norgespris: %{{customdata[2]}}<br>"
                f"Total: %{{customdata[3]}}<extra></extra>"
            )
        )

        red_le = float(change_red_le)
        blue_ge = float(change_blue_ge)

        def change_color(x):
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return "#9e9e9e"
            if x <= red_le:
                return "#d62728"
            if x >= blue_ge:
                return "#1f77b4"
            return "#9e9e9e"

        dff2 = dff.copy()
        dff2["lon"] = dff2["knr_norm"].map(lon_by_nr)
        dff2["lat"] = dff2["knr_norm"].map(lat_by_nr)
        dff2 = dff2.dropna(subset=["lon", "lat", "change_pct"]).copy()
        dff2["chg_color"] = dff2["change_pct"].map(change_color)

        scale = max(0.1, float(marker_scale_pct))
        abs_chg = dff2["change_pct"].abs().clip(upper=scale)
        dff2["chg_size"] = 12 + 26 * (abs_chg / scale)

        fig.add_trace(
            go.Scattermapbox(
                lon=dff2["lon"],
                lat=dff2["lat"],
                mode="markers",
                marker=dict(size=dff2["chg_size"], color=dff2["chg_color"], opacity=0.85),
                hovertext=(
                    dff2[CSV_COLS["kommune"]].astype(str)
                    + "<br>Andel Norgespris: " + dff2["andel_pct0"].astype(str)
                    + f"<br>Endring {period_label} (%): " + dff2["change_pct"].map(fmt_pct)
                ),
                hoverinfo="text",
                showlegend=False,
            )
        )

        SHOW_LABELS_ZOOM = 5
        if zoom >= SHOW_LABELS_ZOOM:
            dff2["label"] = (
                dff2[CSV_COLS["kommune"]].astype(str)
                + "<br>Andel " + dff2["andel_pct0"].astype(str)
                + "<br>Forbruk " + dff2["change_pct"].map(fmt_pct)
            )
            fig.add_trace(
                go.Scattermapbox(
                    lon=dff2["lon"],
                    lat=dff2["lat"],
                    mode="text",
                    text=dff2["label"],
                    textposition="top center",
                    textfont=dict(size=12, color="black"),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

        fig.update_layout(
            mapbox_style="open-street-map",
            margin=dict(l=0, r=0, t=0, b=0),
            mapbox=dict(center=center, zoom=zoom),
            uirevision="keep",
        )
        return fig

    def build_scatter_fig(df: pd.DataFrame, change_period: str, visible_knr: Optional[Set[str]]) -> go.Figure:
        change_col = change_cols_found.get(change_period)
        period_label = change_label(change_period)

        keep = [CSV_COLS["kommune"], CSV_COLS["knr"], "andel", "andel_pct0", "norgespris", "total"]
        dff = df[keep].copy()

        if change_col and change_col in df.columns:
            dff["change_pct"] = df[change_col] * 100.0
        else:
            dff["change_pct"] = float("nan")

        if visible_knr is not None:
            dff = dff[dff[CSV_COLS["knr"]].isin(visible_knr)].copy()

        dff = dff.dropna(subset=["andel", "change_pct"]).copy()

        fig = px.scatter(
            dff,
            x="andel",
            y="change_pct",
            hover_name=CSV_COLS["kommune"],
            hover_data={"andel_pct0": True, "norgespris": True, "total": True},
            labels={"andel": "Andel Norgespris", "change_pct": f"Endring {period_label} (%)"},
        )
        fig.update_xaxes(tickformat=".0%")

        r2_text = "R²: —"
        if len(dff) >= 3:
            import numpy as np
            x = dff["andel"].astype(float).to_numpy()
            y = dff["change_pct"].astype(float).to_numpy()
            try:
                a, b = np.polyfit(x, y, 1)
                yhat = a * x + b
                ss_res = float(((y - yhat) ** 2).sum())
                ss_tot = float(((y - y.mean()) ** 2).sum())
                r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")
                if not math.isnan(r2):
                    r2_text = f"R²: {r2:.3f}".replace(".", ",")

                x_line = np.linspace(float(x.min()), float(x.max()), 60)
                y_line = a * x_line + b
                fig.add_trace(go.Scatter(x=x_line, y=y_line, mode="lines", name="Trendlinje", hoverinfo="skip"))
            except Exception:
                pass

        fig.update_layout(
            margin=dict(l=0, r=0, t=45, b=0),
            height=340,
            title=f"Sammenheng: Norgespris-andel vs endring i forbruk ({period_label}) — {r2_text}",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        return fig

    # -----------------------------
    # DASH APP
    # -----------------------------
    app = Dash(
        __name__,
        server=flask_server,
        url_base_pathname="/stromdash/",
        suppress_callback_exceptions=True,
        assets_folder=str(BASE_DIR / "assets"),
    )

    app.title = "Norgespris per kommune"

    input_box_style = {
        "width": "110px",
        "padding": "8px",
        "borderRadius": "10px",
        "border": "1px solid #d0d0d0",
        "fontSize": "14px",
    }

    TABLE_HEIGHT = "300px"
    table_style_table = {
        "height": TABLE_HEIGHT,
        "overflowY": "auto",
        "overflowX": "auto",
        "border": "1px solid #eee",
        "borderRadius": "10px",
    }
    table_style_cell = {
        "padding": "9px",
        "fontSize": "14px",
        "lineHeight": "1.25",
        "whiteSpace": "normal",
        "height": "auto",
    }
    table_style_header = {
        "fontWeight": "800",
        "fontSize": "14px",
        "position": "sticky",
        "top": 0,
        "zIndex": 2,
        "backgroundColor": "#fafafa",
        "borderBottom": "1px solid #e5e5e5",
    }

    def intro_layout():
        # Robust static-path: unngår “sort bakgrunn” pga feil URL
        BACKGROUND_IMAGE_URL = f"{flask_server.static_url_path}/background.jpg"

        btn_style = {
            "padding": "12px 18px",
            "borderRadius": "14px",
            "border": "1px solid rgba(255,255,255,0.18)",
            "background": "linear-gradient(180deg, rgba(255,255,255,0.16), rgba(255,255,255,0.06))",
            "color": "rgba(255,255,255,0.96)",
            "fontSize": "16px",
            "fontWeight": "800",
            "cursor": "pointer",
            "boxShadow": "0 10px 30px rgba(0,0,0,0.35)",
            "backdropFilter": "blur(10px)",
            "-webkit-backdrop-filter": "blur(10px)",
        }

        return html.Div(
            style={
                "minHeight": "100vh",
                "width": "100%",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "padding": "34px 16px",
                "fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
                "backgroundImage": (
                    "linear-gradient(rgba(0,0,0,0.45), rgba(0,0,0,0.45)), "
                    f"url('{BACKGROUND_IMAGE_URL}')"
                ),
                "backgroundSize": "cover",
                "backgroundPosition": "center",
                "backgroundRepeat": "no-repeat",
                "backgroundAttachment": "fixed",
                "backgroundColor": "#0b1220",
            },
            children=[
                html.Div(
                    style={
                        "maxWidth": "940px",
                        "width": "100%",
                        "padding": "28px 28px",
                        "borderRadius": "18px",
                        "background": "rgba(17, 24, 39, 0.52)",
                        "border": "1px solid rgba(255,255,255,0.14)",
                        "boxShadow": "0 18px 60px rgba(0,0,0,0.35)",
                        "backdropFilter": "blur(12px)",
                        "-webkit-backdrop-filter": "blur(12px)",
                        "color": "rgba(255,255,255,0.92)",
                    },
                    children=[
                        html.H1(
                            "Norgespris per kommune",
                            style={
                                "marginTop": 0,
                                "marginBottom": "12px",
                                "fontSize": "40px",
                                "letterSpacing": "-0.02em",
                            },
                        ),
                        html.P(
                            "Her kan du se hvor stor andel av strømforbrukerne i hver kommune som har valgt Norgespris. "
                            "Du kan også sammenligne forbruket i årets tre siste måneder med samme periode i fjor.",
                            style={
                                "margin": "0 0 10px 0",
                                "fontSize": "16px",
                                "lineHeight": "1.6",
                                "color": "rgba(255,255,255,0.86)",
                            },
                        ),
                        html.P(
                            "For at tallene skal være sammenlignbare, er det viktig å sammenligne kommuner med lignende klima. "
                            "Dataene kommer fra Elhub og er oppdatert per 11. januar 2026.",
                            style={
                                "margin": "0 0 18px 0",
                                "fontSize": "16px",
                                "lineHeight": "1.6",
                                "color": "rgba(255,255,255,0.86)",
                            },
                        ),
                        html.Div(
                            style={
                                "display": "flex",
                                "gap": "12px",
                                "alignItems": "center",
                                "flexWrap": "wrap",
                                "marginTop": "6px",
                            },
                            children=[
                                html.Button("Start kart", id="start-app", n_clicks=0, style=btn_style, className="strom-start-btn"),
                                html.Span(
                                    "Første gang kan det ta litt tid å generere kartet.",
                                    style={"fontSize": "13px", "color": "rgba(255,255,255,0.72)"},
                                ),
                            ],
                        ),
                    ],
                )
            ],
        )

    def main_layout():
        return html.Div(
            style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif", "padding": "12px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 10px 0"}),

                html.Div(
                    style={
                        "display": "flex",
                        "gap": "18px",
                        "alignItems": "center",
                        "marginBottom": "10px",
                        "padding": "10px 12px",
                        "border": "1px solid #e5e5e5",
                        "borderRadius": "12px",
                        "background": "#fafafa",
                        "flexWrap": "wrap",
                    },
                    children=[
                        html.Div(style={"minWidth": "220px"}, children=[
                            html.Label("Vis data for:", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Dropdown(
                                id="mode",
                                options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                                value="Bolig",
                                clearable=False,
                                style={"width": "220px", "maxWidth": "100%", "fontSize": "14px"},
                            ),
                        ]),
                        html.Div(style={"minWidth": "170px"}, children=[
                            html.Label("Endring i forbruk:", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Dropdown(
                                id="change_period",
                                options=[
                                    {"label": "Oktober", "value": "oct"},
                                    {"label": "November", "value": "nov"},
                                    {"label": "Desember", "value": "dec"},
                                    {"label": "Hele Q4", "value": "q4"},
                                ],
                                value="q4",
                                clearable=False,
                                style={"width": "170px", "maxWidth": "100%", "fontSize": "14px"},
                            ),
                        ]),
                        html.Div(children=[
                            html.Label("Rød ≤ (andel)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="low", type="number", value=0.20, step=0.01, min=0, max=1,
                                      debounce=True, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Blå ≥ (andel)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="high", type="number", value=0.50, step=0.01, min=0, max=1,
                                      debounce=True, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Rød ≤ (endring %)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="chg_red_le", type="number", value=0.0, step=0.1,
                                      debounce=True, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Blå ≥ (endring %)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="chg_blue_ge", type="number", value=0.0, step=0.1,
                                      debounce=True, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Prikk-skala (prosentpoeng)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="marker_scale_pct", type="number", value=10.0, step=0.5, min=0.1,
                                      debounce=True, style=input_box_style),
                        ]),
                    ],
                ),

                html.Div(
                    className="strom-grid",
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "1.12fr 1fr",
                        "gridTemplateRows": "auto auto",
                        "gap": "14px",
                        "alignItems": "start",
                    },
                    children=[
                        html.Div(
                            className="strom-map-col",
                            style={"gridColumn": "1", "gridRow": "1 / span 2"},
                            children=[
                                html.Div(
                                    style={"position": "relative"},
                                    children=[
                                        dcc.Loading(
                                            type="default",
                                            children=dcc.Graph(
                                                id="map",
                                                className="strom-map-graph",
                                                style={"height": "calc(100vh - 220px)", "minHeight": "720px"},
                                                config={"scrollZoom": True, "displayModeBar": True, "displaylogo": False},
                                            ),
                                        ),
                                        html.Div(
                                            style={
                                                "position": "absolute",
                                                "top": "12px",
                                                "left": "12px",
                                                "zIndex": 9999,
                                                "display": "flex",
                                                "flexDirection": "column",
                                                "gap": "6px",
                                            },
                                            children=[
                                                html.Button("+", id="zoom-in", n_clicks=0, style={
                                                    "width": "44px", "height": "44px", "fontSize": "26px", "fontWeight": "800",
                                                    "borderRadius": "10px", "border": "1px solid #ccc", "background": "white",
                                                    "cursor": "pointer", "boxShadow": "0 1px 6px rgba(0,0,0,0.15)",
                                                }),
                                                html.Button("–", id="zoom-out", n_clicks=0, style={
                                                    "width": "44px", "height": "44px", "fontSize": "26px", "fontWeight": "800",
                                                    "borderRadius": "10px", "border": "1px solid #ccc", "background": "white",
                                                    "cursor": "pointer", "boxShadow": "0 1px 6px rgba(0,0,0,0.15)",
                                                }),
                                                html.Button("⟲", id="zoom-reset", n_clicks=0, title="Tilbake til Norge", style={
                                                    "width": "44px", "height": "44px", "fontSize": "20px", "fontWeight": "800",
                                                    "borderRadius": "10px", "border": "1px solid #ccc", "background": "white",
                                                    "cursor": "pointer", "boxShadow": "0 1px 6px rgba(0,0,0,0.15)",
                                                }),
                                            ],
                                        ),
                                    ],
                                ),

                                html.Div(style={"margin": "8px 0", "color": "#555"}, id="debug-change"),

                                dcc.Store(id="relayout-store"),
                                dcc.Store(id="view-store", data=DEFAULT_VIEW),
                            ],
                        ),

                        html.Div(
                            className="strom-side-col",
                            style={"gridColumn": "2", "gridRow": "1"},
                            children=[
                                html.H3("Oversikt (synlig utsnitt)", style={"marginTop": "0"}),
                                html.Div(id="count", style={"color": "#555", "marginBottom": "8px"}),

                                html.H4("Høyest andel"),
                                dash_table.DataTable(
                                    id="top",
                                    page_size=15,
                                    style_table=table_style_table,
                                    style_cell=table_style_cell,
                                    style_header=table_style_header,
                                ),

                                html.H4("Lavest andel", style={"marginTop": "14px"}),
                                dash_table.DataTable(
                                    id="bottom",
                                    page_size=15,
                                    style_table=table_style_table,
                                    style_cell=table_style_cell,
                                    style_header=table_style_header,
                                ),
                            ],
                        ),

                        html.Div(
                            className="strom-scatter-col",
                            style={"gridColumn": "2", "gridRow": "2"},
                            children=[
                                dcc.Loading(
                                    type="default",
                                    children=dcc.Graph(
                                        id="scatter",
                                        style={"height": "340px"},
                                        config={"displayModeBar": True, "displaylogo": False},
                                    ),
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

    # Root layout (intro -> app)
    app.layout = html.Div(
        children=[
            dcc.Store(id="app-stage", data="intro"),
            html.Div(id="page"),
        ]
    )

    @app.callback(Output("page", "children"), Input("app-stage", "data"))
    def render_page(stage):
        if stage == "ready":
            return main_layout()
        return intro_layout()

    @app.callback(Output("app-stage", "data"), Input("start-app", "n_clicks"), prevent_initial_call=True)
    def start_app(n):
        return "ready"

    # -----------------------------
    # CALLBACKS
    # -----------------------------
    @app.callback(
        Output("map", "figure"),
        Input("mode", "value"),
        Input("low", "value"),
        Input("high", "value"),
        Input("change_period", "value"),
        Input("chg_red_le", "value"),
        Input("chg_blue_ge", "value"),
        Input("marker_scale_pct", "value"),
        State("view-store", "data"),
    )
    def update_map(mode_value, low, high, change_period, chg_red_le, chg_blue_ge, marker_scale_pct, view):
        df = get_df(mode_value)

        view = view or DEFAULT_VIEW
        center = {"lon": float(view.get("lon", DEFAULT_VIEW["lon"])), "lat": float(view.get("lat", DEFAULT_VIEW["lat"]))}
        zoom = float(view.get("zoom", DEFAULT_VIEW["zoom"]))

        low = 0.20 if low is None else float(low)
        high = 0.50 if high is None else float(high)
        chg_red_le = 0.0 if chg_red_le is None else float(chg_red_le)
        chg_blue_ge = 0.0 if chg_blue_ge is None else float(chg_blue_ge)
        marker_scale_pct = 10.0 if marker_scale_pct is None else float(marker_scale_pct)

        return build_map_fig(
            df=df,
            low=low,
            high=high,
            center=center,
            zoom=zoom,
            change_period=change_period,
            change_red_le=chg_red_le,
            change_blue_ge=chg_blue_ge,
            marker_scale_pct=marker_scale_pct,
        )

    @app.callback(
        Output("relayout-store", "data"),
        Output("view-store", "data"),
        Output("map", "figure", allow_duplicate=True),
        Input("map", "relayoutData"),
        Input("zoom-in", "n_clicks"),
        Input("zoom-out", "n_clicks"),
        Input("zoom-reset", "n_clicks"),
        State("view-store", "data"),
        prevent_initial_call=True,
    )
    def sync_view_and_buttons(relayout, zin, zout, zreset, view):
        view = view or DEFAULT_VIEW
        trig = get_trigger_id()

        new_view = dict(view)

        # Kart-interaksjon (pan/zoom)
        if trig == "map" and relayout:
            if "mapbox.zoom" in relayout:
                new_view["zoom"] = float(relayout["mapbox.zoom"])

            if "mapbox.center" in relayout and isinstance(relayout["mapbox.center"], dict):
                c = relayout["mapbox.center"]
                if "lon" in c:
                    new_view["lon"] = float(c["lon"])
                if "lat" in c:
                    new_view["lat"] = float(c["lat"])

            return (relayout or {}), new_view, no_update

        # Zoom-knapper: patch kun center/zoom (raskere)
        lon = float(new_view.get("lon", DEFAULT_VIEW["lon"]))
        lat = float(new_view.get("lat", DEFAULT_VIEW["lat"]))
        zoom = float(new_view.get("zoom", DEFAULT_VIEW["zoom"]))

        if trig == "zoom-in":
            zoom = min(18.0, zoom + 1.0)
        elif trig == "zoom-out":
            zoom = max(1.0, zoom - 1.0)
        elif trig == "zoom-reset":
            lon, lat, zoom = DEFAULT_VIEW["lon"], DEFAULT_VIEW["lat"], DEFAULT_VIEW["zoom"]
        else:
            return (relayout or {}), no_update, no_update

        patched = Patch()
        patched["layout"]["mapbox"]["center"] = {"lon": lon, "lat": lat}
        patched["layout"]["mapbox"]["zoom"] = zoom

        return (relayout or {}), {"lon": lon, "lat": lat, "zoom": zoom}, patched

    @app.callback(
        Output("top", "data"),
        Output("top", "columns"),
        Output("bottom", "data"),
        Output("bottom", "columns"),
        Output("count", "children"),
        Output("scatter", "figure"),
        Output("debug-change", "children"),
        Input("mode", "value"),
        Input("change_period", "value"),
        Input("relayout-store", "data"),
    )
    def update_tables_scatter_debug(mode_value, change_period, relayout):
        df = get_df(mode_value)

        period_label = change_label(change_period)
        change_col = change_cols_found.get(change_period)

        if change_col and change_col in df.columns:
            change_pct = df[change_col] * 100.0
        else:
            change_pct = pd.Series([float("nan")] * len(df), index=df.index)

        bbox = viewport_bbox_from_relayout(relayout or {})

        if bbox is None:
            visible = set(df[CSV_COLS["knr"]].tolist())
            count_text = "Viser alle kommuner (zoom/pan i kartet for å filtrere på synlig utsnitt)."
        else:
            visible: Set[str] = set()
            for knr, fb in feature_bbox_by_nr.items():
                if bboxes_intersect(fb, bbox):
                    visible.add(knr)
            count_text = f"Kommuner i synlig utsnitt: {len(visible)}"

        dff = df[[CSV_COLS["kommune"], CSV_COLS["knr"], "andel", "andel_pct0"]].copy()
        dff["change_pct"] = change_pct
        dff = dff[dff[CSV_COLS["knr"]].isin(visible)].copy()

        dff["andel_num"] = dff["andel"].fillna(-1)
        dff["Andel Norgespris"] = dff["andel_pct0"]
        dff[f"Endring {period_label} (%)"] = dff["change_pct"].map(fmt_pct)

        cols_show = [CSV_COLS["kommune"], "Andel Norgespris", f"Endring {period_label} (%)"]
        top_df = dff.sort_values("andel_num", ascending=False)[cols_show].head(15)
        bottom_df = dff.sort_values("andel_num", ascending=True)[cols_show].head(15)
        columns = [{"name": c, "id": c} for c in cols_show]

        scatter_fig = build_scatter_fig(df, change_period=change_period, visible_knr=visible)

        n_total = len(df)
        n_ok = int(pd.notna(change_pct).sum())
        mn = float(change_pct.min(skipna=True)) if n_ok else float("nan")
        mx = float(change_pct.max(skipna=True)) if n_ok else float("nan")

        missing_share = int(df["andel"].isna().sum())
        missing_change = int(pd.isna(change_pct).sum())
        total0 = int((df["total"].fillna(0) == 0).sum())

        debug = (
            f"Endringskolonne brukt: {change_col or 'IKKE FUNNET'} | "
            f"Gyldige endringsverdier: {n_ok}/{n_total} | "
            f"min={fmt_pct(mn)}, max={fmt_pct(mx)} | "
            f"Mangler andel: {missing_share} | Mangler endring: {missing_change} | Total=0: {total0}"
        )

        return (
            top_df.to_dict("records"), columns,
            bottom_df.to_dict("records"), columns,
            count_text,
            scatter_fig,
            debug,
        )

    return app
