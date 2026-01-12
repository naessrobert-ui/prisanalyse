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

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, exceptions, no_update

logger = logging.getLogger(__name__)

# -----------------------------
# FIL / KOLONNER / OPPSETT
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = BASE_DIR / "static" / "data" / "kommuner.csv"
CSV_SEP = ";"

GEOJSON_PATH = BASE_DIR / "static" / "geo" / "Kommuner-M.geojson"

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
    "q4":  ["incr_q4", "incr_Q4", "INCR_Q4", "incr q4", "increase_q4", "increase q4"],
}

DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

# -----------------------------
# HJELPEFUNKSJONER
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
    return s.zfill(4) if s.isdigit() else s

def safe_div(n: float, d: float) -> Optional[float]:
    if d in (None, 0) or (isinstance(d, float) and math.isnan(d)):
        return None
    return float(n) / float(d)

def pct0_from_fraction(x: Optional[float]) -> str:
    # andel er fraksjon (0.47 -> 47%)
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{int(round(x * 100))}%"

def fmt_pp(x: Optional[float]) -> str:
    # endring er prosentpoeng (1.5 -> "1,5%")
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except Exception:
        return "—"

def to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False).str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False)

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
    coords = geom.get("coordinates") or []
    lons, lats = [], []

    def add_ring(ring):
        for lon, lat in ring:
            lons.append(lon)
            lats.append(lat)

    gtype = geom.get("type")
    if gtype == "Polygon":
        for r in coords:
            add_ring(r)
    elif gtype == "MultiPolygon":
        for p in coords:
            for r in p:
                add_ring(r)

    return (min(lons), min(lats), max(lons), max(lats)) if lons else (0.0, 0.0, 0.0, 0.0)

def feature_centroid(feature: Dict) -> Optional[Tuple[float, float]]:
    bbox = get_feature_bbox(feature)
    if bbox == (0.0, 0.0, 0.0, 0.0):
        return None
    return ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)

def bboxes_intersect(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])

def viewport_bbox_from_relayout(relayout: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    if not relayout:
        return None
    # plotly mapbox derived coords kan variere litt
    coords = relayout.get("mapbox._derived", None)
    if isinstance(coords, dict) and isinstance(coords.get("coordinates"), list):
        coords = coords["coordinates"]

    if isinstance(coords, list) and len(coords) > 0 and isinstance(coords[0], (list, tuple)):
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return (min(lons), min(lats), max(lons), max(lats))
    return None

def get_trigger_id() -> Optional[str]:
    if not callback_context.triggered:
        return None
    return callback_context.triggered[0]["prop_id"].split(".")[0]

def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    if span <= 0.5:
        return 8.0
    if span <= 1.0:
        return 7.0
    if span <= 2.0:
        return 6.2
    if span <= 4.0:
        return 5.4
    if span <= 8.0:
        return 4.6
    return 3.6

def union_bbox(bboxes: List[Tuple[float, float, float, float]]) -> Optional[Tuple[float, float, float, float]]:
    bboxes = [b for b in bboxes if b and b != (0.0, 0.0, 0.0, 0.0)]
    if not bboxes:
        return None
    minx = min(b[0] for b in bboxes)
    miny = min(b[1] for b in bboxes)
    maxx = max(b[2] for b in bboxes)
    maxy = max(b[3] for b in bboxes)
    return (minx, miny, maxx, maxy)

# -----------------------------
# DATA LOADING
# -----------------------------
def load_resources():
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    # Normaliser kommunenr
    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)

    # Normaliser fylke/region
    df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip().str.upper()

    change_cols_found = {k: find_col(df_raw.columns, aliases) for k, aliases in CHANGE_ALIASES.items()}
    logger.info("Change cols found: %s", change_cols_found)

    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    if not features:
        raise RuntimeError("GeoJSON mangler features")

    geo_nr_key = pick_geojson_nr_key(features[0].get("properties", {}))
    if not geo_nr_key:
        raise RuntimeError("Fant ikke kommunenr-key i GeoJSON properties")

    feature_bbox_by_nr, centroid_by_nr = {}, {}
    for feat in features:
        knr = normalize_kommunenr((feat.get("properties") or {}).get(geo_nr_key, ""))
        if not knr:
            continue
        feature_bbox_by_nr[knr] = get_feature_bbox(feat)
        c = feature_centroid(feat)
        if c:
            centroid_by_nr[knr] = c

    return df_raw, change_cols_found, gj, geo_nr_key, feature_bbox_by_nr, centroid_by_nr

# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, change_cols_found, gj, geo_nr_key, feature_bbox_by_nr, centroid_by_nr = load_resources()

    def clean_unique(series: pd.Series) -> List[str]:
        vals = []
        for x in series.dropna().tolist():
            v = str(x).strip()
            if v and v.lower() != "nan":
                vals.append(v)
        return sorted(set(vals))

    ALL_FYLKER = clean_unique(df_raw[CSV_COLS["fylke"]])
    ALL_REGIONER = clean_unique(df_raw[CSV_COLS["region"]])

    # Dash app – bruk kun routes+requests prefix (ikke url_base_pathname)
    app = Dash(
        __name__,
        server=flask_server,
        routes_pathname_prefix="/stromdash/",
        requests_pathname_prefix="/stromdash/",
        suppress_callback_exceptions=True,
        title="Norgespris per kommune",
    )

    # Cache per mode
    df_cache: Dict[str, pd.DataFrame] = {}

    def get_df_cached(mode: str) -> pd.DataFrame:
        if mode not in df_cache:
            df = df_raw.copy()

            np_col, tot_col = (CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]) if mode == "Bolig" else (CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"])
            df["norgespris"] = to_number(df[np_col])
            df["total"] = to_number(df[tot_col])
            df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
            df["andel_pct0"] = df["andel"].apply(pct0_from_fraction)

            # Endringer inn som råverdi (typisk 0.015 = 1.5 pp). Vi konverterer til prosentpoeng:
            for _, col in change_cols_found.items():
                if col and col in df.columns:
                    df[col] = to_number(df[col])

            df["knr_norm"] = df[CSV_COLS["knr"]].apply(normalize_kommunenr)

            df_cache[mode] = df
        return df_cache[mode]

    # -----------------------------
    # LAYOUTS
    # -----------------------------
    def landing_layout():
        return html.Div(
            style={"maxWidth": "980px", "margin": "auto", "padding": "40px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart"),
                html.Div(
                    style={"background": "#fafafa", "padding": "25px", "borderRadius": "15px", "border": "1px solid #eee"},
                    children=[
                        html.Label("Velg geografisk nivå:", style={"fontWeight": "bold"}),
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
                        dcc.Dropdown(id="scope-id", style={"marginTop": "10px"}, clearable=False),
                        html.Hr(),
                        html.Label("Datakilde:"),
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
                                "fontSize": "16px",
                                "fontWeight": "600",
                            },
                        ),
                        html.Div(id="debug-trigger", style={"marginTop": "10px", "color": "#666", "fontSize": "12px"}),
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
                        html.H2("Norgespris per kommune – interaktivt kart", style={"margin": 0}),
                        html.Button("Bytt område", id="change-scope", n_clicks=0),
                    ],
                ),
                html.Div(
                    style={"display": "flex", "gap": "12px", "padding": "10px 20px", "flexWrap": "wrap"},
                    children=[
                        html.Div(
                            style={"minWidth": "210px"},
                            children=[
                                html.Label("Geografi:", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Dropdown(
                                    id="scope-type-main",
                                    options=[
                                        {"label": "Hele landet", "value": "country"},
                                        {"label": "Strømregion", "value": "region"},
                                        {"label": "Fylke", "value": "county"},
                                    ],
                                    value="country",
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "240px"},
                            children=[
                                html.Label("Velg område:", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Dropdown(id="scope-id-main", clearable=False),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "170px"},
                            children=[
                                html.Label("Vis data for:", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Dropdown(
                                    id="mode",
                                    options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                                    value="Bolig",
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "170px"},
                            children=[
                                html.Label("Endring i forbruk:", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Dropdown(
                                    id="change_period",
                                    options=[
                                        {"label": "Hele Q4", "value": "q4"},
                                        {"label": "Oktober", "value": "oct"},
                                        {"label": "November", "value": "nov"},
                                        {"label": "Desember", "value": "dec"},
                                    ],
                                    value="q4",
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "140px"},
                            children=[
                                html.Label("Rød ≤ (andel)", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Input(id="low", type="number", value=0.2, step=0.05, style={"width": "100%"}),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "140px"},
                            children=[
                                html.Label("Blå ≥ (andel)", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Input(id="high", type="number", value=0.5, step=0.05, style={"width": "100%"}),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "170px"},
                            children=[
                                html.Label("Prikk-skala (prosentpoeng)", style={"fontSize": "12px", "color": "#333"}),
                                dcc.Input(id="marker_scale_pp", type="number", value=10.0, step=1, style={"width": "100%"}),
                            ],
                        ),
                    ],
                ),
                html.Div(style={"padding": "0 20px 10px 20px", "color": "#666", "fontSize": "12px"}, id="statusline"),
                dcc.Loading(
                    children=[
                        html.Div(
                            style={"display": "grid", "gridTemplateColumns": "1.25fr 1fr", "gap": "12px", "padding": "0 20px 20px 20px"},
                            children=[
                                dcc.Graph(id="map", style={"height": "760px"}),
                                html.Div(
                                    children=[
                                        html.H3("Oversikt (synlig utsnitt)", style={"marginTop": 0}),
                                        html.Div(style={"fontSize": "12px", "color": "#666", "marginBottom": "8px"},
                                                 children="Viser alle kommuner i valgt område (zoom/pan i kartet for å filtrere på synlig utsnitt)."),
                                        html.Div(id="tables-container"),
                                        html.H3("Sammenheng: Norgespris-andel vs endring i forbruk", style={"marginTop": "14px"}),
                                        dcc.Graph(id="scatter-plot", style={"height": "330px"}),
                                        html.H3("Valgt kommune", style={"marginTop": "14px"}),
                                        html.Div(id="selected-kommune", style={"padding": "10px", "border": "1px solid #eee", "borderRadius": "10px"}),
                                    ]
                                ),
                            ],
                        )
                    ]
                ),
            ]
        )

    import traceback

    def serve_layout():
        try:
            return html.Div(
                style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif"},
                children=[
                    dcc.Store(id="app-state", data={"stage": "landing"}),  # landing | ready
                    dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),
                    dcc.Store(id="mode-store", data="Bolig"),
                    html.Div(id="page", children=landing_layout()),
                ],
            )
        except Exception:
            logger.exception("Layout crash")
            return html.Pre("Layout crash:\n" + traceback.format_exc())

    app.layout = serve_layout

    # -----------------------------
    # CALLBACKS – LANDING: scope options
    # -----------------------------
    @app.callback(
        [Output("scope-id", "options"), Output("scope-id", "value")],
        Input("scope-type", "value"),
    )
    def update_scope_options_landing(stype):
        if stype == "country":
            return [{"label": "Norge", "value": "NO"}], "NO"
        if stype == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
        else:
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]
        return opts, (opts[0]["value"] if opts else None)

    @app.callback(
        Output("debug-trigger", "children"),
        Input("start-btn", "n_clicks"),
        State("scope-type", "value"),
        State("scope-id", "value"),
        State("landing-mode", "value"),
    )
    def dbg(n, t, v, m):
        trig = callback_context.triggered[0]["prop_id"] if callback_context.triggered else "none"
        return f"trigger={trig} | n_clicks={n} | scope-type={t} | scope-id={v} | mode={m}"

    # -----------------------------
    # START / BYTT OMRÅDE
    # -----------------------------
    @app.callback(
        [Output("app-state", "data"), Output("scope-store", "data"), Output("mode-store", "data")],
        Input("start-btn", "n_clicks"),
        State("scope-type", "value"),
        State("scope-id", "value"),
        State("landing-mode", "value"),
        prevent_initial_call=True,
    )
    def on_start(n, scope_type, scope_id, mode_value):
        if not n:
            raise exceptions.PreventUpdate

        scope_type = scope_type or "country"
        if scope_type == "country":
            scope_id = "NO"

        if not scope_id:
            raise exceptions.PreventUpdate

        return {"stage": "ready"}, {"type": scope_type, "id": str(scope_id)}, (mode_value or "Bolig")

    @app.callback(
        Output("page", "children"),
        Input("app-state", "data"),
    )
    def render_page(state):
        stage = (state or {}).get("stage", "landing")
        if stage != "ready":
            return landing_layout()
        return main_layout()

    # -----------------------------
    # MAIN: scope dropdowns
    # -----------------------------
    @app.callback(
        [Output("scope-id-main", "options"), Output("scope-id-main", "value")],
        Input("scope-type-main", "value"),
    )
    def update_scope_options_main(stype):
        if stype == "country":
            return [{"label": "Norge", "value": "NO"}], "NO"
        if stype == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
        else:
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]
        return opts, (opts[0]["value"] if opts else None)

    @app.callback(
        Output("scope-store", "data"),
        Input("scope-type-main", "value"),
        Input("scope-id-main", "value"),
        State("scope-store", "data"),
    )
    def sync_scope_store(stype, sid, old):
        if not stype:
            return old or {"type": "country", "id": "NO"}
        if stype == "country":
            return {"type": "country", "id": "NO"}
        if not sid:
            return old or {"type": stype, "id": None}
        return {"type": stype, "id": str(sid)}

    @app.callback(
        Output("app-state", "data", allow_duplicate=True),
        Input("change-scope", "n_clicks"),
        prevent_initial_call=True,
    )
    def back_to_landing(n):
        if not n:
            raise exceptions.PreventUpdate
        return {"stage": "landing"}

    # -----------------------------
    # DASHBOARD UPDATE
    # -----------------------------
    @app.callback(
        [
            Output("map", "figure"),
            Output("scatter-plot", "figure"),
            Output("tables-container", "children"),
            Output("selected-kommune", "children"),
            Output("statusline", "children"),
        ],
        [
            Input("app-state", "data"),
            Input("mode", "value"),
            Input("change_period", "value"),
            Input("low", "value"),
            Input("high", "value"),
            Input("marker_scale_pp", "value"),
            Input("map", "relayoutData"),
            Input("map", "clickData"),
            Input("scope-store", "data"),
        ],
        prevent_initial_call=False,
    )
    def update_dashboard(state, mode, period, low, high, scale_pp, relayout, clickData, scope_data):
        stage = (state or {}).get("stage", "landing")
        if stage != "ready":
            return no_update, no_update, no_update, no_update, ""

        mode = mode or "Bolig"
        period = period or "q4"
        scope_data = scope_data or {"type": "country", "id": "NO"}

        df = get_df_cached(mode).copy()

        # Scope-filter (data)
        stype = scope_data.get("type", "country")
        sid = scope_data.get("id", "NO")

        if stype == "county" and sid:
            df = df[df[CSV_COLS["fylke"]] == sid]
        elif stype == "region" and sid:
            df = df[df[CSV_COLS["region"]] == str(sid).upper()]
        # country -> ingen filter

        # Endringkolonne
        change_col = change_cols_found.get(period)
        if change_col and change_col in df.columns:
            # rå: 0.015 => 1.5 prosentpoeng
            df["change_pp"] = df[change_col] * 100.0
        else:
            df["change_pp"] = np.nan

        df["knr_norm"] = df["knr_norm"].apply(normalize_kommunenr)

        # Fitbounds/center for scope (geo)
        scope_bboxes = [feature_bbox_by_nr.get(k) for k in df["knr_norm"].dropna().unique().tolist()]
        ubb = union_bbox([b for b in scope_bboxes if b])
        if ubb:
            center = {"lon": (ubb[0] + ubb[2]) / 2, "lat": (ubb[1] + ubb[3]) / 2}
            zoom = zoom_for_bbox(ubb[2] - ubb[0], ubb[3] - ubb[1])
        else:
            center = {"lon": DEFAULT_VIEW["lon"], "lat": DEFAULT_VIEW["lat"]}
            zoom = DEFAULT_VIEW["zoom"]

        # Visible utsnitt (tabeller + scatter) basert på viewport
        visible_df = df
        bbox = viewport_bbox_from_relayout(relayout or {})
        if bbox:
            visible_knrs = [k for k, b in feature_bbox_by_nr.items() if bboxes_intersect(b, bbox)]
            visible_df = df[df["knr_norm"].isin(visible_knrs)]

        # Kart: choropleth på andel
        fig_map = px.choropleth_mapbox(
            df,
            geojson=gj,
            locations=CSV_COLS["knr"],
            featureidkey=f"properties.{geo_nr_key}",
            color="andel",
            color_continuous_scale="RdBu",
            range_color=[0, 1],
            mapbox_style="carto-positron",
            zoom=zoom,
            center=center,
            opacity=0.65,
            hover_name=CSV_COLS["kommune"],
            hover_data={
                "andel": ":.3f",
                "andel_pct0": True,
                "change_pp": True,
                CSV_COLS["knr"]: True,
            },
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, uirevision="constant")

        # Prikker: endring (prosentpoeng)
        df_pts = df.dropna(subset=["change_pp"]).copy()
        df_pts["lon"] = df_pts["knr_norm"].map(lambda k: (centroid_by_nr.get(k) or (None, None))[0])
        df_pts["lat"] = df_pts["knr_norm"].map(lambda k: (centroid_by_nr.get(k) or (None, None))[1])
        df_pts = df_pts.dropna(subset=["lon", "lat"])

        # Skala: hvis bruker skriver 10 -> 1pp gir 10px-ish
        scale_pp = float(scale_pp) if scale_pp not in (None, "") else 10.0
        marker_size = (df_pts["change_pp"].abs() * scale_pp).clip(lower=2, upper=40)

        fig_map.add_trace(
            go.Scattermapbox(
                lon=df_pts["lon"],
                lat=df_pts["lat"],
                mode="markers",
                marker=dict(
                    size=marker_size,
                    color=df_pts["change_pp"],
                    colorscale="Picnic",
                    showscale=False,
                    opacity=0.85,
                ),
                customdata=np.stack(
                    [
                        df_pts[CSV_COLS["kommune"]].astype(str),
                        df_pts["andel"].astype(float),
                        df_pts["change_pp"].astype(float),
                        df_pts[CSV_COLS["knr"]].astype(str),
                    ],
                    axis=1,
                ),
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Andel Norgespris: %{customdata[1]:.3f} (%{text})<br>"
                    "Endring: %{customdata[2]:.1f}%<br>"
                    "Knr: %{customdata[3]}<extra></extra>"
                ),
                text=df_pts["andel"].apply(pct0_from_fraction),
            )
        )

        # Scatter: andel vs endring
        fig_scatter = px.scatter(
            visible_df.dropna(subset=["andel"]),
            x="andel",
            y="change_pp",
            hover_name=CSV_COLS["kommune"],
            labels={"andel": "Andel Norgespris", "change_pp": f"Endring {period.upper()} (%)"},
        )
        fig_scatter.update_layout(margin={"l": 10, "r": 10, "t": 30, "b": 10})

        # Tabeller: topp/bunn andel (i synlig utsnitt)
        top_10 = visible_df.sort_values("andel", ascending=False).head(10)[[CSV_COLS["kommune"], "andel_pct0", "change_pp"]].copy()
        bot_10 = visible_df.sort_values("andel", ascending=True).head(10)[[CSV_COLS["kommune"], "andel_pct0", "change_pp"]].copy()
        top_10["change_pp"] = top_10["change_pp"].apply(fmt_pp)
        bot_10["change_pp"] = bot_10["change_pp"].apply(fmt_pp)

        tables = html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr", "gap": "14px"},
            children=[
                html.Div(
                    children=[
                        html.B("Høyest andel"),
                        dash_table.DataTable(
                            data=top_10.to_dict("records"),
                            columns=[
                                {"name": "KOMMUNE", "id": CSV_COLS["kommune"]},
                                {"name": "Andel Norgespris", "id": "andel_pct0"},
                                {"name": f"Endring {period.upper()} (%)", "id": "change_pp"},
                            ],
                            style_table={"overflowX": "auto"},
                            style_cell={"fontFamily": "system-ui", "fontSize": "12px", "padding": "6px"},
                            style_header={"fontWeight": "700"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.B("Lavest andel"),
                        dash_table.DataTable(
                            data=bot_10.to_dict("records"),
                            columns=[
                                {"name": "KOMMUNE", "id": CSV_COLS["kommune"]},
                                {"name": "Andel Norgespris", "id": "andel_pct0"},
                                {"name": f"Endring {period.upper()} (%)", "id": "change_pp"},
                            ],
                            style_table={"overflowX": "auto"},
                            style_cell={"fontFamily": "system-ui", "fontSize": "12px", "padding": "6px"},
                            style_header={"fontWeight": "700"},
                        ),
                    ]
                ),
            ],
        )

        # Klikk-valgt kommune (hent fra clickData)
        selected = "Klikk på en kommune i kartet for detaljer."
        if clickData and clickData.get("points"):
            pt = clickData["points"][0]
            # Hvis klikket på prikk-trace: vi har customdata
            cd = pt.get("customdata")
            if cd and len(cd) >= 4:
                kname = cd[0]
                andel = float(cd[1])
                chpp = float(cd[2])
                knr = cd[3]
                selected = html.Div(
                    children=[
                        html.Div([html.B(kname), html.Span(f" (Knr: {knr})", style={"color": "#666"})]),
                        html.Div(f"Andel Norgespris: {pct0_from_fraction(andel)}"),
                        html.Div(f"Endring ({period.upper()}): {fmt_pp(chpp)}"),
                    ]
                )
            else:
                # Klakk på choropleth: prøv å lese location
                loc = pt.get("location")
                if loc:
                    loc = normalize_kommunenr(loc)
                    row = df[df[CSV_COLS["knr"]].apply(normalize_kommunenr) == loc].head(1)
                    if len(row):
                        r = row.iloc[0]
                        selected = html.Div(
                            children=[
                                html.Div([html.B(str(r[CSV_COLS["kommune"]])), html.Span(f" (Knr: {r[CSV_COLS['knr']]})", style={"color": "#666"})]),
                                html.Div(f"Andel Norgespris: {pct0_from_fraction(r['andel'])}"),
                                html.Div(f"Endring ({period.upper()}): {fmt_pp(r['change_pp'])}"),
                            ]
                        )

        # Statusline for debugging tall
        used_col = change_col or "—"
        valid_change = int(df["change_pp"].notna().sum())
        missing_change = int(df["change_pp"].isna().sum())
        missing_andel = int(df["andel"].isna().sum())
        status = f"Endringskolonne brukt: {used_col} | Gyldige endringer: {valid_change} | Mangler endring: {missing_change} | Mangler andel: {missing_andel} | Antall kommuner i scope: {len(df)}"

        return fig_map, fig_scatter, tables, selected, status

    return app
