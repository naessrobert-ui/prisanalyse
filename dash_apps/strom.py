# dash_apps/strom.py
# -*- coding: utf-8 -*-

import logging
import math
import json
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, no_update, exceptions

logger = logging.getLogger(__name__)

# -----------------------------
# FIL / OPPSETT
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # prisanalyse/
CSV_PATH = BASE_DIR / "static" / "data" / "kommuner.csv"
CSV_SEP = ";"
GEOJSON_PATH = BASE_DIR / "static" / "geo" / "Kommuner-M.geojson"

# Viktige felt (vi finner "Fylke"/"Region" robust ved innlesing)
CSV_COLS_FIXED = {
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
        .str.replace("\ufeff", "", regex=False)  # BOM
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
        return (0.0, 0.0, 0.0, 0.0)
    return (min(lons), min(lats), max(lons), max(lats))

def bbox_union(bboxes: List[Tuple[float, float, float, float]]) -> Optional[Tuple[float, float, float, float]]:
    bboxes = [b for b in bboxes if b and b != (0.0, 0.0, 0.0, 0.0)]
    if not bboxes:
        return None
    return (
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    )

def bbox_center(b: Tuple[float, float, float, float]) -> Dict[str, float]:
    minx, miny, maxx, maxy = b
    return {"lon": (minx + maxx) / 2.0, "lat": (miny + maxy) / 2.0}

def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    if span <= 0.5: return 8.0
    if span <= 1.0: return 7.0
    if span <= 2.0: return 6.2
    if span <= 4.0: return 5.4
    if span <= 8.0: return 4.6
    return 3.6

def clean_str_list(s: pd.Series) -> list[str]:
    # robust: dropp NaN, "", "nan"
    s = s.dropna().astype(str).str.strip()
    s = s[(s != "") & (s.str.lower() != "nan")]
    return sorted(set(s.tolist()))

# -----------------------------
# DATA LOADING
# -----------------------------
def load_resources():
    # CSV
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    # Finn Fylke/Region robust (case/spacing)
    fylke_col = find_col(df_raw.columns, ["Fylke", "fylke", "FYLKE"])
    region_col = find_col(df_raw.columns, ["Region", "region", "REGION", "Strømregion", "Stromregion"])

    if not fylke_col:
        raise RuntimeError("Fant ikke kolonne for Fylke i kommuner.csv (sjekk kolonnenavn).")
    if not region_col:
        raise RuntimeError("Fant ikke kolonne for Region i kommuner.csv (sjekk kolonnenavn).")

    # Normaliser kommunenr
    knr_col = CSV_COLS_FIXED["knr"]
    if knr_col not in df_raw.columns:
        raise RuntimeError(f"Mangler forventet kolonne {knr_col} i kommuner.csv.")
    df_raw[knr_col] = df_raw[knr_col].apply(normalize_kommunenr)

    # Trim fylke/region
    df_raw[fylke_col] = df_raw[fylke_col].astype(str).str.strip()
    df_raw[region_col] = df_raw[region_col].astype(str).str.strip().str.upper()

    # Change-kolonner
    change_cols_found = {k: find_col(df_raw.columns, aliases) for k, aliases in CHANGE_ALIASES.items()}

    # GeoJSON
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
        if not knr:
            continue
        b = get_feature_bbox(feat)
        feature_bbox_by_nr[knr] = b
        if b != (0.0, 0.0, 0.0, 0.0):
            centroid_by_nr[knr] = ((b[0] + b[2]) / 2.0, (b[1] + b[3]) / 2.0)

    return df_raw, fylke_col, region_col, change_cols_found, gj, geo_nr_key, feature_bbox_by_nr, centroid_by_nr

# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, FYLKE_COL, REGION_COL, change_cols_found, gj, geo_nr_key, feature_bbox_by_nr, centroid_by_nr = load_resources()

    DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

    ALL_FYLKER = clean_str_list(df_raw[FYLKE_COL])
    ALL_REGIONER = clean_str_list(df_raw[REGION_COL])

    logger.warning("Loaded %d fylker, %d regioner", len(ALL_FYLKER), len(ALL_REGIONER))

    # Dash: bruk KUN url_base_pathname (ikke requests/routes samtidig)
    app = Dash(
        __name__,
        server=flask_server,
        url_base_pathname="/stromdash/",
        suppress_callback_exceptions=True,
    )
    app.title = "Norgespris per kommune"

    # -----------------------------
    # DF cache
    # -----------------------------
    df_cache: Dict[str, pd.DataFrame] = {}

    def get_df_cached(mode_value: str) -> pd.DataFrame:
        mode_value = "Bolig" if mode_value == "Bolig" else "Fritid"
        if mode_value in df_cache:
            return df_cache[mode_value]

        df = df_raw.copy()

        if mode_value == "Bolig":
            np_col, tot_col = CSV_COLS_FIXED["bolig_np"], CSV_COLS_FIXED["bolig_tot"]
        else:
            np_col, tot_col = CSV_COLS_FIXED["fritid_np"], CSV_COLS_FIXED["fritid_tot"]

        df["norgespris"] = to_number(df[np_col])
        df["total"] = to_number(df[tot_col])
        df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
        df["andel_pct0"] = df["andel"].apply(pct0)

        for _, col in change_cols_found.items():
            if col and col in df.columns:
                df[col] = to_number(df[col])

        df["knr_norm"] = df[CSV_COLS_FIXED["knr"]].apply(normalize_kommunenr)

        df_cache[mode_value] = df
        return df

    # -----------------------------
    # Scope helpers
    # -----------------------------
    def filter_df_by_scope(df: pd.DataFrame, scope: Dict[str, str]) -> pd.DataFrame:
        stype = (scope or {}).get("type", "country")
        sid = (scope or {}).get("id", "NO")

        if stype == "country":
            return df

        if stype == "county":
            return df[df[FYLKE_COL] == sid].copy()

        if stype == "region":
            sid2 = str(sid).strip().upper()
            return df[df[REGION_COL].astype(str).str.upper() == sid2].copy()

        return df

    def view_for_scope(df_scoped: pd.DataFrame) -> Dict[str, float]:
        knrs = df_scoped[CSV_COLS_FIXED["knr"]].astype(str).tolist()
        bbs = [feature_bbox_by_nr.get(normalize_kommunenr(k)) for k in knrs]
        ub = bbox_union([b for b in bbs if b is not None])
        if not ub:
            return DEFAULT_VIEW
        center = bbox_center(ub)
        lon_span = float(ub[2] - ub[0])
        lat_span = float(ub[3] - ub[1])
        zoom = zoom_for_bbox(lon_span, lat_span)
        return {"lon": float(center["lon"]), "lat": float(center["lat"]), "zoom": float(zoom)}

    # -----------------------------
    # Layouts
    # -----------------------------
    def landing_layout():
        return html.Div(
            style={"maxWidth": "980px", "margin": "auto", "padding": "40px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart"),
                html.Div(
                    style={
                        "background": "#fafafa",
                        "padding": "25px",
                        "borderRadius": "15px",
                        "border": "1px solid #eee",
                    },
                    children=[
                        html.Label("Velg geografisk nivå:", style={"fontWeight": "700"}),

                        dcc.RadioItems(
                            id="scope-type",
                            options=[
                                {"label": "Hele landet", "value": "country"},
                                {"label": "Strømregion", "value": "region"},
                                {"label": "Fylke", "value": "county"},
                            ],
                            value="country",
                            labelStyle={"display": "block", "margin": "10px 0"},
                        ),

                        # Viktig: default options/value slik at den aldri er tom ved start
                        dcc.Dropdown(
                            id="scope-id",
                            options=[{"label": "Norge", "value": "NO"}],
                            value="NO",
                            clearable=False,
                            style={"marginTop": "10px"},
                        ),

                        html.Hr(),

                        html.Label("Datakilde:", style={"fontWeight": "700"}),
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
                                "marginTop": "20px",
                                "width": "100%",
                                "padding": "15px",
                                "background": "#2c3e50",
                                "color": "white",
                                "borderRadius": "10px",
                                "cursor": "pointer",
                                "border": "0",
                                "fontWeight": "700",
                            },
                        ),

                        html.Div(id="debug-trigger", style={"marginTop": "10px", "color": "#666"}),
                    ],
                ),
            ],
        )

    def main_layout():
        return html.Div(
            children=[
                html.Div(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "center",
                        "padding": "10px 20px",
                        "background": "white",
                        "borderBottom": "1px solid #eee",
                    },
                    children=[
                        html.H2("Norgespris-analyse", style={"margin": 0}),
                        html.Button("Bytt område", id="change-scope", n_clicks=0),
                    ],
                ),

                html.Div(
                    style={"display": "flex", "padding": "15px", "gap": "15px", "flexWrap": "wrap", "background": "#f8f9fa"},
                    children=[
                        dcc.Dropdown(
                            id="mode",
                            options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                            value="Bolig",
                            style={"width": "200px"},
                            clearable=False,
                        ),
                        dcc.Dropdown(
                            id="change_period",
                            options=[
                                {"label": "Hele Q4", "value": "q4"},
                                {"label": "Oktober", "value": "oct"},
                                {"label": "November", "value": "nov"},
                                {"label": "Desember", "value": "dec"},
                            ],
                            value="q4",
                            style={"width": "180px"},
                            clearable=False,
                        ),
                        html.Div([html.Label("Rød ≤ andel"), dcc.Input(id="low", type="number", value=0.20, step=0.01, min=0, max=1)]),
                        html.Div([html.Label("Blå ≥ andel"), dcc.Input(id="high", type="number", value=0.50, step=0.01, min=0, max=1)]),
                        html.Div([html.Label("Prikk-skala (pp)"), dcc.Input(id="marker_scale_pct", type="number", value=10.0, step=0.5, min=0.1)]),
                    ],
                ),

                dcc.Loading(
                    children=[
                        html.Div(
                            style={"display": "grid", "gridTemplateColumns": "1.2fr 1fr", "gap": "10px", "padding": "10px"},
                            children=[
                                dcc.Graph(id="map", style={"height": "760px"}, config={"displaylogo": False}),
                                html.Div(
                                    children=[
                                        dcc.Graph(id="scatter-plot", style={"height": "340px"}, config={"displaylogo": False}),
                                        html.Div(id="tables-container", style={"marginTop": "10px"}),
                                    ]
                                ),
                            ],
                        )
                    ]
                ),
            ]
        )

    def serve_layout():
        return html.Div(
            style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif"},
            children=[
                dcc.Store(id="app-state", data={"stage": "landing"}),      # landing | ready
                dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),
                dcc.Store(id="view-store", data=DEFAULT_VIEW),
                html.Div(id="page", children=landing_layout()),
            ],
        )

    app.layout = serve_layout

    # -----------------------------
    # Callbacks
    # -----------------------------
    @app.callback(
        Output("scope-id", "options"),
        Output("scope-id", "value"),
        Input("scope-type", "value"),
    )
    def update_scope_options(stype):
        logger.warning("update_scope_options stype=%s", stype)

        if stype == "country":
            return [{"label": "Norge", "value": "NO"}], "NO"

        if stype == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
        else:
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]

        if not opts:
            # tydelig fallback hvis listene er tomme
            return [{"label": "Ingen treff (sjekk CSV)", "value": "__none__"}], "__none__"

        return opts, opts[0]["value"]

    @app.callback(
        Output("debug-trigger", "children"),
        Input("start-btn", "n_clicks"),
        Input("scope-type", "value"),
        Input("scope-id", "value"),
        Input("landing-mode", "value"),
    )
    def dbg(n, t, v, m):
        trig = callback_context.triggered[0]["prop_id"] if callback_context.triggered else "none"
        return f"trigger={trig} | start={n} | type={t} | id={v} | mode={m}"

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
    def on_start(n, scope_type, scope_id, landing_mode):
        if not n:
            raise exceptions.PreventUpdate

        scope_type = scope_type or "country"
        if scope_type == "country":
            scope_id = "NO"

        if not scope_id or scope_id == "__none__":
            raise exceptions.PreventUpdate

        df = get_df_cached(landing_mode)
        df_scoped = filter_df_by_scope(df, {"type": scope_type, "id": str(scope_id)})
        view = view_for_scope(df_scoped)

        return {"stage": "ready", "mode": landing_mode}, {"type": scope_type, "id": str(scope_id)}, view

    @app.callback(
        Output("app-state", "data"),
        Input("change-scope", "n_clicks"),
        prevent_initial_call=True,
    )
    def back_to_landing(n):
        if not n:
            raise exceptions.PreventUpdate
        return {"stage": "landing"}

    @app.callback(
        Output("page", "children"),
        Input("app-state", "data"),
    )
    def render_page(state):
        stage = (state or {}).get("stage", "landing")
        if stage != "ready":
            return landing_layout()
        return main_layout()

    @app.callback(
        Output("mode", "value"),
        Input("app-state", "data"),
        prevent_initial_call=True,
    )
    def sync_mode_from_landing(state):
        if (state or {}).get("stage") != "ready":
            raise exceptions.PreventUpdate
        return (state or {}).get("mode", "Bolig")

    @app.callback(
        Output("map", "figure"),
        Output("scatter-plot", "figure"),
        Output("tables-container", "children"),
        Input("app-state", "data"),
        Input("mode", "value"),
        Input("change_period", "value"),
        Input("low", "value"),
        Input("high", "value"),
        Input("marker_scale_pct", "value"),
        Input("view-store", "data"),
        State("scope-store", "data"),
    )
    def update_dashboard(state, mode_value, period, low, high, marker_scale_pct, view, scope):
        if (state or {}).get("stage") != "ready":
            return no_update, no_update, no_update

        df = get_df_cached(mode_value)
        df_scoped = filter_df_by_scope(df, scope or {"type": "country", "id": "NO"}).copy()

        marker_scale_pct = 10.0 if marker_scale_pct is None else float(marker_scale_pct)

        change_col = change_cols_found.get(period)
        df_scoped["change_val"] = df_scoped[change_col] if (change_col and change_col in df_scoped.columns) else np.nan

        # --- MAP ---
        fig_map = px.choropleth_mapbox(
            df_scoped,
            geojson=gj,
            locations=CSV_COLS_FIXED["knr"],
            featureidkey=f"properties.{geo_nr_key}",
            color="andel",
            color_continuous_scale="RdBu",
            range_color=[0, 1],
            mapbox_style="carto-positron",
            opacity=0.6,
            hover_name=CSV_COLS_FIXED["kommune"],
            hover_data={"andel_pct0": True, "norgespris": True, "total": True},
        )

        v = view or DEFAULT_VIEW
        fig_map.update_layout(
            mapbox=dict(center={"lon": float(v.get("lon", DEFAULT_VIEW["lon"])), "lat": float(v.get("lat", DEFAULT_VIEW["lat"]))},
                       zoom=float(v.get("zoom", DEFAULT_VIEW["zoom"]))),
            margin=dict(l=0, r=0, t=0, b=0),
            uirevision="keep",
        )

        # prikker for endring
        pts = df_scoped.dropna(subset=["change_val"]).copy()
        pts["lon"] = pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[0])
        pts["lat"] = pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[1])
        pts = pts.dropna(subset=["lon", "lat"])

        if not pts.empty:
            scale = max(0.1, float(marker_scale_pct))
            abs_chg = pts["change_val"].abs().clip(upper=scale)
            pts["size"] = 12 + 26 * (abs_chg / scale)

            fig_map.add_trace(
                go.Scattermapbox(
                    lon=pts["lon"],
                    lat=pts["lat"],
                    mode="markers",
                    marker=dict(size=pts["size"], color=pts["change_val"], colorscale="Picnic", opacity=0.85, showscale=False),
                    hovertext=pts[CSV_COLS_FIXED["kommune"]].astype(str) + "<br>Endring: " + pts["change_val"].apply(fmt_pct),
                    hoverinfo="text",
                    showlegend=False,
                )
            )

        # --- SCATTER ---
        scat = df_scoped.dropna(subset=["andel", "change_val"]).copy()
        fig_scatter = px.scatter(
            scat,
            x="andel",
            y="change_val",
            hover_name=CSV_COLS_FIXED["kommune"],
            labels={"andel": "Andel Norgespris", "change_val": "Endring (%)"},
        )
        fig_scatter.update_xaxes(tickformat=".0%")
        fig_scatter.update_layout(title="Sammenheng: andel vs endring", margin=dict(l=0, r=0, t=40, b=0), height=340)

        # trend med numpy (ingen statsmodels)
        if len(scat) >= 3:
            x = scat["andel"].astype(float).to_numpy()
            y = scat["change_val"].astype(float).to_numpy()
            try:
                a, b = np.polyfit(x, y, 1)
                xl = np.linspace(float(x.min()), float(x.max()), 60)
                yl = a * xl + b
                fig_scatter.add_trace(go.Scatter(x=xl, y=yl, mode="lines", name="Trend", hoverinfo="skip"))
            except Exception:
                pass

        # --- TABELLER ---
        tdf = df_scoped.copy()
        tdf["andel_num"] = tdf["andel"].fillna(-1)

        top_df = tdf.sort_values("andel_num", ascending=False).head(10)[[CSV_COLS_FIXED["kommune"], "andel_pct0"]]
        bot_df = tdf.sort_values("andel_num", ascending=True).head(10)[[CSV_COLS_FIXED["kommune"], "andel_pct0"]]

        tables = html.Div(
            style={"display": "flex", "gap": "20px"},
            children=[
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.B("Høyest andel"),
                        dash_table.DataTable(
                            data=top_df.to_dict("records"),
                            columns=[{"name": c, "id": c} for c in top_df.columns],
                            style_table={"border": "1px solid #eee", "borderRadius": "10px", "overflow": "hidden"},
                            style_cell={"padding": "8px", "fontSize": "14px"},
                            style_header={"fontWeight": "700", "backgroundColor": "#fafafa"},
                        ),
                    ],
                ),
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.B("Lavest andel"),
                        dash_table.DataTable(
                            data=bot_df.to_dict("records"),
                            columns=[{"name": c, "id": c} for c in bot_df.columns],
                            style_table={"border": "1px solid #eee", "borderRadius": "10px", "overflow": "hidden"},
                            style_cell={"padding": "8px", "fontSize": "14px"},
                            style_header={"fontWeight": "700", "backgroundColor": "#fafafa"},
                        ),
                    ],
                ),
            ],
        )

        return fig_map, fig_scatter, tables

    return app
