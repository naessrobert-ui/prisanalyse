# -*- coding: utf-8 -*-

import math
import json
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional, Set

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, no_update

# -----------------------------
# FIL / KOLONNER / OPPSETT
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
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
    "dec": ["incr_dec", "INCR_DEC", "incr dec", "increase_dec", "increase_dec"],
    "q4": ["incr_Q4", "incr_q4", "INCR_Q4", "incr q4", "increase_q4", "increase q4"],
}


# -----------------------------
# HJELPEFUNKSJONER (LOGIKK & MATEMATIKK)
# -----------------------------
def normalize_columns(cols):
    return pd.Index(cols).astype(str).str.replace("\ufeff", "", regex=False).str.strip()


def canon(s: str) -> str:
    s = str(s).strip().casefold()
    return "".join(ch for ch in s if ch.isalnum())


def find_col(actual_cols, aliases) -> Optional[str]:
    actual_cols = list(actual_cols)
    actual_c = {c: canon(c) for c in actual_cols}
    alias_c = [canon(a) for a in aliases]
    for c in actual_cols:
        if actual_c[c] in alias_c: return c
    for c in actual_cols:
        for a in alias_c:
            if a and a in actual_c[c]: return c
    return None


def normalize_kommunenr(x) -> str:
    if x is None: return ""
    s = str(x).strip()
    if s.endswith(".0"): s = s[:-2]
    return s.zfill(4) if s.isdigit() else s


def safe_div(n: float, d: float) -> Optional[float]:
    if d in (None, 0) or (isinstance(d, float) and math.isnan(d)): return None
    return float(n) / float(d)


def pct0(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return "—"
    return f"{int(round(x * 100))}%"


def fmt_pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return "—"
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except:
        return "—"


def to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("%", "", regex=False).str.replace("\u00a0", "",
                                                                                     regex=False).str.replace(" ", "",
                                                                                                              regex=False)
    mask_both = s.str.contains(r"\.", regex=True) & s.str.contains(",", regex=False)
    s.loc[mask_both] = s.loc[mask_both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    mask_comma = (~mask_both) & s.str.contains(",", regex=False)
    s.loc[mask_comma] = s.loc[mask_comma].str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def pick_geojson_nr_key(properties: Dict) -> Optional[str]:
    for c in ["kommunenummer", "kommunenr", "KOMMUNENR", "KOMMUNE_NR", "KOMMUNENUMMER", "id", "ID"]:
        if c in properties: return c
    return None


def get_feature_bbox(feature: Dict) -> Tuple[float, float, float, float]:
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or []
    lons, lats = [], []

    def add_ring(ring):
        for lon, lat in ring:
            lons.append(lon);
            lats.append(lat)

    gtype = geom.get("type")
    if gtype == "Polygon":
        for r in coords: add_ring(r)
    elif gtype == "MultiPolygon":
        for p in coords:
            for r in p: add_ring(r)
    return (min(lons), min(lats), max(lons), max(lats)) if lons else (0.0, 0.0, 0.0, 0.0)


def feature_centroid(feature: Dict) -> Optional[Tuple[float, float]]:
    bbox = get_feature_bbox(feature)
    if bbox == (0.0, 0.0, 0.0, 0.0): return None
    return ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)


def bboxes_intersect(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def viewport_bbox_from_relayout(relayout: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    if not relayout: return None
    for key in ["mapbox._derived", "mapbox._derived.coordinates"]:
        coords = relayout.get(key)
        if isinstance(coords, list) and len(coords) > 0:
            lons = [c[0] for c in coords];
            lats = [c[1] for c in coords]
            return (min(lons), min(lats), max(lons), max(lats))
    return None


def get_trigger_id() -> Optional[str]:
    if not callback_context.triggered: return None
    return callback_context.triggered[0]["prop_id"].split(".")[0]


def change_label(change_period: str) -> str:
    return {"oct": "Oktober", "nov": "November", "dec": "Desember", "q4": "Q4"}.get(change_period, "Q4")


def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    if span <= 0.5: return 8.0
    if span <= 1.0: return 7.0
    if span <= 2.0: return 6.2
    if span <= 4.0: return 5.4
    if span <= 8.0: return 4.6
    return 3.6


# -----------------------------
# DATA LOADING
# -----------------------------
def load_resources():
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)
    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)
    df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip().str.upper()
    change_cols_found = {k: find_col(df_raw.columns, aliases) for k, aliases in CHANGE_ALIASES.items()}

    GEOJSON_PATH = BASE_DIR / "static" / "geo" / "Kommuner-M.geojson"
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    geo_nr_key = pick_geojson_nr_key(features[0]["properties"])

    feature_bbox_by_nr, centroid_by_nr, features_by_nr = {}, {}, {}
    for feat in features:
        knr = normalize_kommunenr(feat["properties"].get(geo_nr_key, ""))
        if not knr: continue
        features_by_nr[knr] = feat
        feature_bbox_by_nr[knr] = get_feature_bbox(feat)
        c = feature_centroid(feat)
        if c: centroid_by_nr[knr] = c

    return df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr, features_by_nr


# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr, features_by_nr = load_resources()

    def _clean_str_list(s: pd.Series) -> list[str]:
        out = []
        for x in s.dropna().tolist():
            v = str(x).strip()
            if v and v.lower() != "nan":
                out.append(v)
        return sorted(set(out))

    ALL_FYLKER = _clean_str_list(df_raw[CSV_COLS["fylke"]])
    ALL_REGIONER = _clean_str_list(df_raw[CSV_COLS["region"]])

    # --- Initialiser App ---
    app = Dash(__name__, server=flask_server,
               routes_pathname_prefix="/stromdash/",
               requests_pathname_prefix="/stromdash/",
               suppress_callback_exceptions=True)

    # --- Interne hjelpefunksjoner for DF-cache og Scope ---
    df_cache = {}

    def get_df_cached(mode):
        if mode not in df_cache:
            df = df_raw.copy()
            np_col, tot_col = (CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]) if mode == "Bolig" else (
                CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"])
            df["norgespris"] = to_number(df[np_col])
            df["total"] = to_number(df[tot_col])
            df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
            df["andel_pct0"] = df["andel"].apply(pct0)
            for k, col in change_cols_found.items():
                if col: df[col] = to_number(df[col])
            df["knr_norm"] = df[CSV_COLS["knr"]].apply(normalize_kommunenr)
            df_cache[mode] = df
        return df_cache[mode]

    # --- Layouts ---
    def landing_layout():
        return html.Div(style={"maxWidth": "980px", "margin": "auto", "padding": "40px"}, children=[
            html.H1("Norgespris per kommune – interaktivt kart"),
            html.Div(
                style={"background": "#fafafa", "padding": "25px", "borderRadius": "15px", "border": "1px solid #eee"},
                children=[
                    html.Label("Velg geografisk nivå:", style={"fontWeight": "bold"}),
                    dcc.RadioItems(id="scope-type", options=[
                        {"label": "Hele landet", "value": "country"},
                        {"label": "Strømregion", "value": "region"},
                        {"label": "Fylke", "value": "county"}
                    ], value="country", labelStyle={"display": "block", "margin": "10px 0"}),
                    dcc.Dropdown(id="scope-id", style={"marginTop": "10px"}),
                    html.Hr(),
                    html.Label("Datakilde:"),
                    dcc.Dropdown(id="landing-mode", options=[{"label": "Bolig", "value": "Bolig"},
                                                             {"label": "Fritidsbolig", "value": "Fritid"}],
                                 value="Bolig"),
                    html.Button("Generer Kart", id="start-btn", n_clicks=0,
                                style={"marginTop": "20px", "width": "100%", "padding": "15px", "background": "#2c3e50",
                                       "color": "white", "borderRadius": "10px", "cursor": "pointer"})
                ])
        ])

    def main_layout():
        return html.Div(children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "padding": "10px 20px",
                            "background": "white", "borderBottom": "1px solid #eee"}, children=[
                html.H2("Norgespris-analyse", style={"margin": 0}),
                html.Button("Bytt område", id="change-scope", n_clicks=0)
            ]),
            html.Div(style={"display": "flex", "padding": "15px", "gap": "15px", "flexWrap": "wrap",
                            "background": "#f8f9fa"}, children=[
                dcc.Dropdown(id="mode", options=[{"label": "Bolig", "value": "Bolig"},
                                                 {"label": "Fritidsbolig", "value": "Fritid"}], value="Bolig",
                             style={"width": "180px"}),
                dcc.Dropdown(id="change_period",
                             options=[{"label": "Q4", "value": "q4"}, {"label": "Okt", "value": "oct"},
                                      {"label": "Nov", "value": "nov"}, {"label": "Des", "value": "dec"}], value="q4",
                             style={"width": "150px"}),
                html.Div([html.Label("Filter Lav: "),
                          dcc.Input(id="low", type="number", value=0.2, step=0.05, style={"width": "60px"})]),
                html.Div([html.Label("Filter Høy: "),
                          dcc.Input(id="high", type="number", value=0.5, step=0.05, style={"width": "60px"})]),
                html.Div([html.Label("Prikk-skala: "),
                          dcc.Input(id="marker_scale_pct", type="number", value=10.0, style={"width": "60px"})]),
            ]),
            dcc.Loading(children=[
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1.2fr 1fr", "gap": "10px", "padding": "10px"},
                    children=[
                        dcc.Graph(id="map", style={"height": "750px"}),
                        html.Div(children=[
                            dcc.Graph(id="scatter-plot"),
                            html.Div(id="tables-container", style={"marginTop": "10px"})
                        ])
                    ])
            ])
        ])

    # Kritiske rettelser for Render: serve_layout definert her
    # ... def landing_layout()
    # ... def main_layout()

    def serve_layout():
        try:
            return html.Div(
                children=[
                    dcc.Store(id="app-state", data={"stage": "landing"}),
                    dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),
                    dcc.Store(id="relayout-store"),
                    dcc.Store(id="view-store", data=DEFAULT_VIEW),
                    html.Div(id="page", children=landing_layout()),
                ]
            )
        except Exception:
            logger.exception("Layout crash")
            return html.Pre("Layout crash:\n" + traceback.format_exc())


    app.layout = serve_layout  # <-- viktig: etter def, og uten parenteser

    # --- Callbacks ---
    @app.callback(
        [Output("scope-id", "options"), Output("scope-id", "value")],
        Input("scope-type", "value")
    )
    def update_scope_options(stype):
        if stype == "country": return [{"label": "Norge", "value": "NO"}], "NO"
        if stype == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
        else:
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]
        return opts, (opts[0]["value"] if opts else None)

    @app.callback(
        [Output("page-content", "children"), Output("app-state", "data"), Output("scope-store", "data")],
        [Input("start-btn", "n_clicks"), Input("change-scope", "n_clicks")],
        [State("scope-type", "value"), State("scope-id", "value"), State("landing-mode", "value")],
        prevent_initial_call=True
    )
    def toggle_view(n1, n2, stype, sid, lmode):
        trigger = get_trigger_id()
        if trigger == "start-btn":
            return main_layout(), "main", {"type": stype, "id": sid, "mode": lmode}
        return landing_layout(), "landing", None

    @app.callback(
        [Output("map", "figure"), Output("scatter-plot", "figure"), Output("tables-container", "children")],
        [Input("app-state", "data"), Input("mode", "value"), Input("change_period", "value"),
         Input("low", "value"), Input("high", "value"), Input("marker_scale_pct", "value"),
         Input("map", "relayoutData")],
        State("scope-store", "data")
    )
    def update_dashboard(state, mode, period, low, high, scale_pct, relayout, scope_data):
        if state != "main" or not scope_data: return no_update, no_update, no_update

        df = get_df_cached(mode)
        change_col = change_cols_found.get(period)
        df["change_val"] = df[change_col] if change_col else 0

        # Finn utsnitt for tabell-filtrering
        visible_df = df
        bbox = viewport_bbox_from_relayout(relayout)
        if bbox:
            visible_knrs = [k for k, b in feature_bbox_by_nr.items() if bboxes_intersect(b, bbox)]
            visible_df = df[df["knr_norm"].isin(visible_knrs)]

        # --- FIG 1: Map ---
        fig_map = px.choropleth_mapbox(
            df, geojson=gj, locations=CSV_COLS["knr"],
            featureidkey=f"properties.{geo_nr_key}",
            color="andel", color_continuous_scale="RdBu",
            range_color=[0, 1], mapbox_style="carto-positron",
            zoom=4.5, center={"lat": 64, "lon": 12}, opacity=0.6
        )

        # Legg til endrings-prikker (Scattermapbox)
        df_pts = df.dropna(subset=["change_val"]).copy()
        df_pts["lon"] = df_pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[0])
        df_pts["lat"] = df_pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[1])
        df_pts = df_pts.dropna(subset=["lon"])

        fig_map.add_trace(go.Scattermapbox(
            lon=df_pts["lon"], lat=df_pts["lat"], mode="markers",
            marker=dict(size=df_pts["change_val"].abs() * scale_pct,
                        color=df_pts["change_val"], colorscale="Picnic", showscale=False),
            hovertext=df_pts[CSV_COLS["kommune"]] + ": " + df_pts["change_val"].apply(fmt_pct)
        ))
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, uirevision="constant")

        # --- FIG 2: Scatter ---
        fig_scatter = px.scatter(visible_df, x="andel", y="change_val", trendline="ols",
                                 hover_name=CSV_COLS["kommune"], title="Andel vs Endring (Valgt utsnitt)")

        # --- Tabeller ---
        top_5 = visible_df.nlargest(5, "andel")[[CSV_COLS["kommune"], "andel_pct0"]]
        bot_5 = visible_df.nsmallest(5, "andel")[[CSV_COLS["kommune"], "andel_pct0"]]

        tables = html.Div(style={"display": "flex", "gap": "20px"}, children=[
            html.Div([html.B("Høyest andel"), dash_table.DataTable(top_5.to_dict('records'))]),
            html.Div([html.B("Lavest andel"), dash_table.DataTable(bot_5.to_dict('records'))])
        ])

        return fig_map, fig_scatter, tables

    return app