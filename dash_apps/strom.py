# dash_apps/strom.py
# -*- coding: utf-8 -*-

import math
import json
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional, Set, List

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, no_update
from dash.exceptions import PreventUpdate


# -----------------------------
# FIL / KOLONNER
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # prosjektroten (prisanalyse/)
CSV_PATH = BASE_DIR / "static" / "data" / "kommuner.csv"
CSV_SEP = ";"

CSV_COLS = {
    "kommune": "KOMMUNE",
    "knr": "KOMMUNENUMMER",
    "fylke": "Fylke",
    "region": "Region",
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

def safe_div(n: float, d: float) -> Optional[float]:
    if d in (None, 0) or (isinstance(d, float) and math.isnan(d)):
        return None
    return float(n) / float(d)

def pct0(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{int(round(x * 100))}%"

def fmt_pct(x: Optional[float]) -> str:
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
    bbox = get_feature_bbox(feature)
    if bbox == (0.0, 0.0, 0.0, 0.0):
        return None
    minx, miny, maxx, maxy = bbox
    return ((minx + maxx) / 2.0, (miny + maxy) / 2.0)

def bboxes_intersect(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    a_minx, a_miny, a_maxx, a_maxy = a
    b_minx, b_miny, b_maxx, b_maxy = b
    return not (a_maxx < b_minx or a_minx > b_maxx or a_maxy < b_miny or a_miny > b_maxy)

def viewport_bbox_from_relayout(relayout: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    if not relayout:
        return None
    derived = relayout.get("mapbox._derived")
    if isinstance(derived, dict) and isinstance(derived.get("coordinates"), list):
        coords = derived.get("coordinates")
        if coords and isinstance(coords[0], (list, tuple)) and len(coords[0]) == 2:
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

def zoom_for_span(span: float) -> float:
    # enkel heuristikk for Norge-kart
    if span <= 0.5: return 8.0
    if span <= 1.0: return 7.0
    if span <= 2.0: return 6.2
    if span <= 4.0: return 5.4
    if span <= 8.0: return 4.6
    return 3.6

def clean_str_list(series: pd.Series) -> List[str]:
    out = []
    for x in series.dropna().tolist():
        v = str(x).strip()
        if v and v.lower() != "nan":
            out.append(v)
    return sorted(set(out))


# -----------------------------
# DATA-LOADING (én gang)
# -----------------------------
def load_resources():
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    # Normalisering
    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)
    df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip().str.upper()

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
        if knr:
            feature_bbox_by_nr[knr] = get_feature_bbox(feat)
            c = feature_centroid(feat)
            if c:
                centroid_by_nr[knr] = c

    return df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr


# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr = load_resources()

    # DEBUG som ALLTID dukker opp i Render-logg
    ALL_FYLKER = clean_str_list(df_raw[CSV_COLS["fylke"]])
    ALL_REGIONER = clean_str_list(df_raw[CSV_COLS["region"]])
    print(f"[stromdash] CSV_PATH={CSV_PATH}")
    print(f"[stromdash] Antall rader CSV={len(df_raw)}")
    print(f"[stromdash] ALL_FYLKER({len(ALL_FYLKER)}) eksempel={ALL_FYLKER[:15]}")
    print(f"[stromdash] ALL_REGIONER({len(ALL_REGIONER)}) eksempel={ALL_REGIONER[:15]}")
    print(f"[stromdash] change_cols_found={change_cols_found}")

    DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

    # Cache ferdigberegnet DF pr mode
    df_cache: Dict[str, pd.DataFrame] = {}

    def build_df(mode_value: str) -> pd.DataFrame:
        if mode_value in df_cache:
            return df_cache[mode_value]

        df = df_raw.copy()

        if mode_value == "Bolig":
            np_col, tot_col = CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]
        else:
            np_col, tot_col = CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"]

        df["norgespris"] = to_number(df[np_col])
        df["total"] = to_number(df[tot_col])
        df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
        df["andel_pct0"] = df["andel"].apply(pct0)

        for _, col in change_cols_found.items():
            if col and col in df.columns:
                df[col] = to_number(df[col])

        df["knr_norm"] = df[CSV_COLS["knr"]].apply(normalize_kommunenr)

        df_cache[mode_value] = df
        return df

    def compute_scope_view(scope_type: str, scope_id: str, mode_value: str) -> dict:
        """Returnerer view (lon,lat,zoom) for valgt scope."""
        df = build_df(mode_value)
        if scope_type == "country":
            return dict(DEFAULT_VIEW)

        if scope_type == "county":
            knr_list = df.loc[df[CSV_COLS["fylke"]] == scope_id, CSV_COLS["knr"]].tolist()
        else:  # region
            knr_list = df.loc[df[CSV_COLS["region"]] == scope_id, CSV_COLS["knr"]].tolist()

        bboxes = [feature_bbox_by_nr.get(normalize_kommunenr(k)) for k in knr_list]
        bboxes = [b for b in bboxes if b and b != (0.0, 0.0, 0.0, 0.0)]
        if not bboxes:
            return dict(DEFAULT_VIEW)

        minx = min(b[0] for b in bboxes); miny = min(b[1] for b in bboxes)
        maxx = max(b[2] for b in bboxes); maxy = max(b[3] for b in bboxes)

        lon = (minx + maxx) / 2.0
        lat = (miny + maxy) / 2.0
        span = max(maxx - minx, maxy - miny)
        zoom = zoom_for_span(span)

        return {"lon": float(lon), "lat": float(lat), "zoom": float(zoom)}

    def build_map_fig(df: pd.DataFrame, low: float, high: float, center: Dict[str, float], zoom: float,
                      change_period: str, marker_scale_pct: float) -> go.Figure:
        low, high = sorted([float(low), float(high)])
        change_col = change_cols_found.get(change_period)
        period_label = change_label(change_period)

        dff = df.copy()
        if change_col and change_col in dff.columns:
            dff["change_pct"] = dff[change_col]
        else:
            dff["change_pct"] = float("nan")

        def kategori(share):
            if share is None or (isinstance(share, float) and math.isnan(share)):
                return "mangler"
            if share >= high:
                return f"≥ {int(round(high * 100))}%"
            if share <= low:
                return f"≤ {int(round(low * 100))}%"
            return "mellom"

        dff["kategori"] = dff["andel"].apply(kategori)

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
            custom_data=["andel_pct0", "change_pct", "norgespris", "total"],
            opacity=0.75,
        )

        fig.update_traces(
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Andel Norgespris: %{{customdata[0]}}<br>"
                f"Endring {period_label} (%): %{{customdata[1]:.1f}}<br>"
                "Norgespris: %{{customdata[2]}}<br>"
                "Total: %{{customdata[3]}}<extra></extra>"
            )
        )

        # Prikker for endring (størrelse = abs endring)
        dff2 = dff.dropna(subset=["change_pct"]).copy()
        dff2["lon"] = dff2["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[0])
        dff2["lat"] = dff2["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[1])
        dff2 = dff2.dropna(subset=["lon", "lat"]).copy()

        scale = max(0.1, float(marker_scale_pct))
        abs_chg = dff2["change_pct"].abs().clip(upper=scale)
        dff2["chg_size"] = 10 + 24 * (abs_chg / scale)

        fig.add_trace(
            go.Scattermapbox(
                lon=dff2["lon"],
                lat=dff2["lat"],
                mode="markers",
                marker=dict(size=dff2["chg_size"], opacity=0.85),
                hovertext=(
                    dff2[CSV_COLS["kommune"]].astype(str)
                    + f"<br>Endring {period_label}: " + dff2["change_pct"].apply(fmt_pct)
                ),
                hoverinfo="text",
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

        dff = df.copy()
        if change_col and change_col in dff.columns:
            dff["change_pct"] = dff[change_col]
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
            labels={"andel": "Andel Norgespris", "change_pct": f"Endring {period_label} (%)"},
        )
        fig.update_xaxes(tickformat=".0%")
        fig.update_layout(height=340, margin=dict(l=0, r=0, t=40, b=0))
        return fig

    # -----------------------------
    # DASH APP
    # -----------------------------
    app = Dash(
        __name__,
        server=flask_server,
        routes_pathname_prefix="/stromdash/",
        requests_pathname_prefix="/stromdash/",
        suppress_callback_exceptions=True,
    )
    app.title = "Norgespris per kommune"

    # ---------- Layout-byggerne ----------
    def landing_layout():
        return html.Div(
            style={"maxWidth": "980px", "margin": "0 auto", "padding": "28px 18px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 18px 0"}),
                html.Div(
                    style={"background": "#fafafa", "padding": "18px", "borderRadius": "14px", "border": "1px solid #eee"},
                    children=[
                        html.Div("Velg geografisk nivå:", style={"fontWeight": "700", "marginBottom": "8px"}),
                        dcc.RadioItems(
                            id="scope-type",
                            options=[
                                {"label": "Hele landet", "value": "country"},
                                {"label": "Strømregion", "value": "region"},
                                {"label": "Fylke", "value": "county"},
                            ],
                            value="country",
                            labelStyle={"display": "block", "margin": "8px 0"},
                        ),
                        dcc.Dropdown(id="scope-id", placeholder="Velg...", clearable=False, style={"marginTop": "10px"}),

                        html.Hr(),

                        html.Div("Datakilde:", style={"fontWeight": "700", "marginBottom": "8px"}),
                        dcc.Dropdown(
                            id="landing-mode",
                            options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                            value="Bolig",
                            clearable=False,
                        ),

                        html.Button(
                            "Generer kart",
                            id="start-btn",
                            n_clicks=0,
                            style={
                                "marginTop": "14px",
                                "width": "100%",
                                "padding": "14px",
                                "background": "#2c3e50",
                                "color": "white",
                                "borderRadius": "12px",
                                "border": "0",
                                "cursor": "pointer",
                                "fontWeight": "700",
                            },
                        ),

                        html.Div(id="scope-live", style={"marginTop": "10px", "color": "#666", "fontSize": "13px"}),
                    ],
                ),
            ],
        )

    def main_layout():
        input_box_style = {
            "width": "110px",
            "padding": "8px",
            "borderRadius": "10px",
            "border": "1px solid #d0d0d0",
            "fontSize": "14px",
        }

        table_style_table = {
            "height": "300px",
            "overflowY": "auto",
            "overflowX": "auto",
            "border": "1px solid #eee",
            "borderRadius": "10px",
        }
        table_style_cell = {"padding": "9px", "fontSize": "14px", "whiteSpace": "normal", "height": "auto"}
        table_style_header = {
            "fontWeight": "800",
            "fontSize": "14px",
            "position": "sticky",
            "top": 0,
            "zIndex": 2,
            "backgroundColor": "#fafafa",
            "borderBottom": "1px solid #e5e5e5",
        }

        return html.Div(
            style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif", "padding": "12px"},
            children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
                    html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 10px 0"}),
                    html.Button("Bytt område", id="back-btn", n_clicks=0, style={"padding": "10px 12px"})
                ]),

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
                        html.Div(children=[
                            html.Label("Vis data for:", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Dropdown(
                                id="mode",
                                options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                                value="Bolig",
                                clearable=False,
                                style={"width": "220px", "fontSize": "14px"},
                            ),
                        ]),
                        html.Div(children=[
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
                                style={"width": "170px", "fontSize": "14px"},
                            ),
                        ]),
                        html.Div(children=[
                            html.Label("Rød ≤ (andel)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="low", type="number", value=0.20, step=0.01, min=0, max=1, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Blå ≥ (andel)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="high", type="number", value=0.50, step=0.01, min=0, max=1, style=input_box_style),
                        ]),
                        html.Div(children=[
                            html.Label("Prikk-skala (prosentpoeng)", style={"fontWeight": "700", "fontSize": "14px"}),
                            dcc.Input(id="marker_scale_pct", type="number", value=10.0, step=0.5, min=0.1, style=input_box_style),
                        ]),
                    ],
                ),

                html.Div(
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "1.12fr 1fr",
                        "gridTemplateRows": "auto auto",
                        "gap": "14px",
                        "alignItems": "start",
                    },
                    children=[
                        html.Div(
                            style={"gridColumn": "1", "gridRow": "1 / span 2"},
                            children=[
                                dcc.Graph(
                                    id="map",
                                    style={"height": "calc(100vh - 220px)", "minHeight": "720px"},
                                    config={"scrollZoom": True, "displayModeBar": True, "displaylogo": False},
                                ),
                                html.Div(style={"margin": "8px 0", "color": "#555"}, id="count"),
                            ],
                        ),

                        html.Div(
                            style={"gridColumn": "2", "gridRow": "1"},
                            children=[
                                html.H3("Oversikt (synlig utsnitt)", style={"marginTop": "0"}),
                                html.H4("Høyest andel"),
                                dash_table.DataTable(id="top", page_size=15, style_table=table_style_table,
                                                     style_cell=table_style_cell, style_header=table_style_header),
                                html.H4("Lavest andel", style={"marginTop": "14px"}),
                                dash_table.DataTable(id="bottom", page_size=15, style_table=table_style_table,
                                                     style_cell=table_style_cell, style_header=table_style_header),
                            ],
                        ),

                        html.Div(
                            style={"gridColumn": "2", "gridRow": "2"},
                            children=[dcc.Graph(id="scatter", style={"height": "340px"}, config={"displaylogo": False})],
                        ),
                    ],
                ),
            ],
        )

    # Root layout (én gang, stabile IDs)
    app.layout = html.Div(
        children=[
            dcc.Store(id="app-state", data={"stage": "landing"}),   # landing | main
            dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),  # valgt scope
            dcc.Store(id="view-store", data=DEFAULT_VIEW),          # map view
            html.Div(id="page", children=landing_layout()),
        ]
    )

    # -----------------------------
    # CALLBACKS (LANDING)
    # -----------------------------
    @app.callback(
        Output("scope-id", "options"),
        Output("scope-id", "value"),
        Input("scope-type", "value"),
    )
    def update_scope_options(scope_type):
        # NB: returnér ALLTID noe, ellers blir dropdown stående på gamle options.
        if scope_type == "country":
            return [{"label": "Norge", "value": "NO"}], "NO"

        if scope_type == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
        else:
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]

        if not opts:
            return [{"label": "Ingen alternativer (sjekk CSV)", "value": "__none__"}], "__none__"

        return opts, opts[0]["value"]

    @app.callback(
        Output("scope-live", "children"),
        Input("scope-type", "value"),
        Input("scope-id", "value"),
    )
    def live_scope(scope_type, scope_id):
        return f"scope-type={scope_type} | scope-id={scope_id}"

    @app.callback(
        Output("app-state", "data"),
        Output("scope-store", "data"),
        Output("view-store", "data"),
        Input("start-btn", "n_clicks"),
        State("scope-type", "value"),
        State("scope-id", "value"),
        State("landing-mode", "value"),
        prevent_initial_call=True,
    )
    def start_app(n, scope_type, scope_id, landing_mode):
        if not n:
            raise PreventUpdate

        scope_type = scope_type or "country"
        if scope_type == "country":
            scope_id = "NO"

        if not scope_id:
            raise PreventUpdate

        view = compute_scope_view(scope_type, str(scope_id), landing_mode or "Bolig")

        return (
            {"stage": "main", "landing_mode": landing_mode or "Bolig"},
            {"type": scope_type, "id": str(scope_id)},
            view,
        )

    @app.callback(
        Output("app-state", "data", allow_duplicate=True),
        Input("back-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def go_back(n):
        if not n:
            raise PreventUpdate
        return {"stage": "landing"}

    @app.callback(
        Output("page", "children"),
        Input("app-state", "data"),
    )
    def render_page(state):
        stage = (state or {}).get("stage", "landing")
        if stage == "main":
            return main_layout()
        return landing_layout()

    # -----------------------------
    # CALLBACKS (MAIN)
    # -----------------------------
    @app.callback(
        Output("mode", "value"),
        Input("app-state", "data"),
        prevent_initial_call=False,
    )
    def sync_mode_from_landing(state):
        # sett default mode når man går inn i main
        if not state:
            return "Bolig"
        if state.get("stage") != "main":
            return no_update
        return state.get("landing_mode") or "Bolig"

    @app.callback(
        Output("map", "figure"),
        Input("mode", "value"),
        Input("low", "value"),
        Input("high", "value"),
        Input("change_period", "value"),
        Input("marker_scale_pct", "value"),
        Input("view-store", "data"),
    )
    def update_map(mode_value, low, high, change_period, marker_scale_pct, view):
        mode_value = mode_value or "Bolig"
        df = build_df(mode_value)

        center = {"lon": float((view or {}).get("lon", DEFAULT_VIEW["lon"])),
                  "lat": float((view or {}).get("lat", DEFAULT_VIEW["lat"]))}
        zoom = float((view or {}).get("zoom", DEFAULT_VIEW["zoom"]))

        low = 0.20 if low is None else float(low)
        high = 0.50 if high is None else float(high)
        marker_scale_pct = 10.0 if marker_scale_pct is None else float(marker_scale_pct)

        return build_map_fig(
            df=df,
            low=low,
            high=high,
            center=center,
            zoom=zoom,
            change_period=change_period or "q4",
            marker_scale_pct=marker_scale_pct,
        )

    @app.callback(
        Output("top", "data"),
        Output("top", "columns"),
        Output("bottom", "data"),
        Output("bottom", "columns"),
        Output("count", "children"),
        Output("scatter", "figure"),
        Input("mode", "value"),
        Input("change_period", "value"),
        Input("map", "relayoutData"),
    )
    def update_tables_scatter(mode_value, change_period, relayout):
        mode_value = mode_value or "Bolig"
        df = build_df(mode_value)

        period_label = change_label(change_period or "q4")
        change_col = change_cols_found.get(change_period or "q4")

        if change_col and change_col in df.columns:
            df["change_pct"] = df[change_col]
        else:
            df["change_pct"] = float("nan")

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

        dff = df[df[CSV_COLS["knr"]].isin(visible)].copy()
        dff["andel_num"] = dff["andel"].fillna(-1)

        dff["Andel Norgespris"] = dff["andel_pct0"]
        dff[f"Endring {period_label} (%)"] = dff["change_pct"].apply(fmt_pct)

        cols_show = [CSV_COLS["kommune"], "Andel Norgespris", f"Endring {period_label} (%)"]
        top_df = dff.sort_values("andel_num", ascending=False)[cols_show].head(15)
        bottom_df = dff.sort_values("andel_num", ascending=True)[cols_show].head(15)
        columns = [{"name": c, "id": c} for c in cols_show]

        scatter_fig = build_scatter_fig(df, change_period=change_period or "q4", visible_knr=visible)

        return (
            top_df.to_dict("records"), columns,
            bottom_df.to_dict("records"), columns,
            count_text,
            scatter_fig,
        )

    return app
