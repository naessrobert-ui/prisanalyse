# -*- coding: utf-8 -*-
import logging
import math
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, exceptions, no_update

logger = logging.getLogger(__name__)

# -----------------------------
# PATHS / KOLONNER
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
    "incr_oct": "incr_oct",
    "incr_nov": "incr_nov",
    "incr_dec": "incr_dec",
    "incr_q4": "incr_Q4",
}

# -----------------------------
# HJELP
# -----------------------------
def normalize_columns(cols):
    return pd.Index(cols).astype(str).str.replace("\ufeff", "", regex=False).str.strip()

def normalize_kommunenr(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4) if s.isdigit() else s

def safe_div(n: float, d: float) -> Optional[float]:
    try:
        if d in (None, 0) or (isinstance(d, float) and math.isnan(d)):
            return None
        return float(n) / float(d)
    except Exception:
        return None

def pct0(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{int(round(x * 100))}%"

def fmt_pct1(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:.1f}%".replace(".", ",")

def to_number(series: pd.Series) -> pd.Series:
    """
    Robust parser:
    - tåler '0,015' og '0.015'
    - tåler tusenskille '1 234 567' og NBSP
    - tåler både '.' og ',' i samme streng (da tolker vi '.' som tusen og ',' som desimal)
    """
    s = series.astype(str).str.strip()
    s = s.str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False)
    s = s.str.replace("%", "", regex=False)

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

    if not lons:
        return (0.0, 0.0, 0.0, 0.0)
    return (min(lons), min(lats), max(lons), max(lats))

def bbox_union(bboxes: List[Tuple[float, float, float, float]]) -> Tuple[float, float, float, float]:
    bboxes = [b for b in bboxes if b != (0.0, 0.0, 0.0, 0.0)]
    if not bboxes:
        return (0.0, 0.0, 0.0, 0.0)
    return (
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    )

def bbox_center(b: Tuple[float, float, float, float]) -> Dict[str, float]:
    return {"lon": (b[0] + b[2]) / 2, "lat": (b[1] + b[3]) / 2}

def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    if span <= 0.5: return 8.0
    if span <= 1.0: return 7.0
    if span <= 2.0: return 6.2
    if span <= 4.0: return 5.4
    if span <= 8.0: return 4.6
    return 3.7

def get_trigger_id() -> Optional[str]:
    if not callback_context.triggered:
        return None
    return callback_context.triggered[0]["prop_id"].split(".")[0]

# -----------------------------
# LOAD RESOURCES (CSV + GEOJSON)
# -----------------------------
def load_resources():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Fant ikke CSV: {CSV_PATH}")
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"Fant ikke GEOJSON: {GEOJSON_PATH}")

    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    # sjekk at forventede kolonner finnes
    for k, col in CSV_COLS.items():
        if col not in df_raw.columns:
            logger.warning("Kolonne mangler i CSV: %s (key=%s)", col, k)

    # normaliser kommunenr + fylke/region
    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)

    if CSV_COLS["fylke"] in df_raw.columns:
        df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    else:
        df_raw[CSV_COLS["fylke"]] = ""

    if CSV_COLS["region"] in df_raw.columns:
        df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip().str.upper()
    else:
        df_raw[CSV_COLS["region"]] = ""

    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    if not features:
        raise ValueError("GeoJSON har ingen features")

    geo_nr_key = pick_geojson_nr_key(features[0].get("properties", {}))
    if not geo_nr_key:
        raise ValueError("Fant ikke kommunenr-key i GeoJSON properties")

    # Bygg normalisert nøkkel i geojson for trygg matching
    feature_bbox_by_nr = {}
    centroid_by_nr = {}

    for feat in features:
        props = feat.setdefault("properties", {})
        knr_raw = props.get(geo_nr_key, "")
        knr_norm = normalize_kommunenr(knr_raw)
        props["_knr_norm"] = knr_norm

        bb = get_feature_bbox(feat)
        feature_bbox_by_nr[knr_norm] = bb
        if bb != (0.0, 0.0, 0.0, 0.0):
            centroid_by_nr[knr_norm] = ((bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2)

    return df_raw, gj, feature_bbox_by_nr, centroid_by_nr

# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    df_raw, gj, feature_bbox_by_nr, centroid_by_nr = load_resources()

    DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

    def _clean_list(series: pd.Series) -> List[str]:
        s = series.dropna().astype(str).str.strip()
        s = s[s.str.len() > 0]
        s = s[s.str.lower() != "nan"]
        return sorted(set(s.tolist()))

    ALL_FYLKER = _clean_list(df_raw[CSV_COLS["fylke"]])
    ALL_REGIONER = _clean_list(df_raw[CSV_COLS["region"]])

    logger.info("Fant %s fylker og %s regioner i CSV", len(ALL_FYLKER), len(ALL_REGIONER))

    # cache per mode
    df_cache = {}

    def get_df_cached(mode: str) -> pd.DataFrame:
        if mode in df_cache:
            return df_cache[mode]

        df = df_raw.copy()

        if mode == "Bolig":
            np_col, tot_col = CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]
        else:
            np_col, tot_col = CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"]

        df["norgespris"] = to_number(df.get(np_col, pd.Series([np.nan] * len(df))))
        df["total"] = to_number(df.get(tot_col, pd.Series([np.nan] * len(df))))
        df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
        df["andel_pct"] = df["andel"].apply(pct0)

        # endring (prosentpoeng/andel) fra csv
        for col in [CSV_COLS["incr_oct"], CSV_COLS["incr_nov"], CSV_COLS["incr_dec"], CSV_COLS["incr_q4"]]:
            if col in df.columns:
                df[col] = to_number(df[col])

        df["knr_norm"] = df[CSV_COLS["knr"]].apply(normalize_kommunenr)

        df_cache[mode] = df
        return df

    def landing_layout():
        return html.Div(
            style={"maxWidth": "980px", "margin": "auto", "padding": "32px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart"),
                html.Div(
                    style={
                        "background": "#fafafa",
                        "padding": "22px",
                        "borderRadius": "14px",
                        "border": "1px solid #eee",
                    },
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
                        dcc.Dropdown(id="scope-id", placeholder="Velg ...", style={"marginTop": "8px"}),
                        html.Hr(),
                        html.Label("Datakilde:", style={"fontWeight": "bold"}),
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
                                "marginTop": "18px",
                                "width": "100%",
                                "padding": "14px",
                                "background": "#2c3e50",
                                "color": "white",
                                "borderRadius": "10px",
                                "cursor": "pointer",
                                "border": "none",
                                "fontWeight": "600",
                            },
                        ),
                        html.Div(id="landing-hint", style={"marginTop": "10px", "color": "#666", "fontSize": "13px"}),
                    ],
                ),
            ],
        )

    def main_layout():
        return html.Div(
            style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif"},
            children=[
                html.Div(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "center",
                        "padding": "10px 16px",
                        "background": "white",
                        "borderBottom": "1px solid #eee",
                    },
                    children=[
                        html.H2("Norgespris per kommune – interaktivt kart", style={"margin": 0}),
                        html.Button("Bytt område", id="back-btn", n_clicks=0),
                    ],
                ),
                html.Div(
                    style={"display": "flex", "flexWrap": "wrap", "gap": "12px", "padding": "12px 16px", "background": "#f7f7f7"},
                    children=[
                        html.Div(style={"minWidth": "170px"}, children=[
                            html.Label("Vis data for:", style={"fontSize": "12px", "fontWeight": "600"}),
                            dcc.Dropdown(
                                id="mode",
                                options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                                value="Bolig",
                                clearable=False,
                            ),
                        ]),
                        html.Div(style={"minWidth": "180px"}, children=[
                            html.Label("Endring i forbruk:", style={"fontSize": "12px", "fontWeight": "600"}),
                            dcc.Dropdown(
                                id="period",
                                options=[
                                    {"label": "Hele Q4", "value": "q4"},
                                    {"label": "Oktober", "value": "oct"},
                                    {"label": "November", "value": "nov"},
                                    {"label": "Desember", "value": "dec"},
                                ],
                                value="q4",
                                clearable=False,
                            ),
                        ]),
                        html.Div(children=[
                            html.Label("Prikk-skala (prosentpoeng):", style={"fontSize": "12px", "fontWeight": "600"}),
                            dcc.Input(id="marker_scale", type="number", value=10, step=1, style={"width": "110px"}),
                        ]),
                        html.Div(id="debug-stats", style={"alignSelf": "end", "color": "#666", "fontSize": "12px"}),
                    ],
                ),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1.35fr 1fr", "gap": "12px", "padding": "12px 16px"},
                    children=[
                        dcc.Graph(id="map", style={"height": "720px"}),
                        html.Div(children=[
                            html.Div(id="click-info", style={"marginBottom": "10px"}),
                            dcc.Graph(id="scatter", style={"height": "320px"}),
                            html.Div(id="tables"),
                        ]),
                    ],
                ),
            ],
        )

    def serve_layout():
        # Stage: landing | main
        return html.Div(
            children=[
                dcc.Store(id="app-state", data={"stage": "landing"}),
                dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),
                html.Div(id="page", children=landing_layout()),
            ]
        )

    app = Dash(
        __name__,
        server=flask_server,
        routes_pathname_prefix="/stromdash/",
        requests_pathname_prefix="/stromdash/",
        suppress_callback_exceptions=True,
    )
    app.layout = serve_layout

    # -----------------------------
    # LANDING: scope dropdown options
    # -----------------------------
    @app.callback(
        Output("scope-id", "options"),
        Output("scope-id", "value"),
        Output("landing-hint", "children"),
        Input("scope-type", "value"),
    )
    def update_scope_options(scope_type):
        if scope_type == "country":
            return [{"label": "Norge", "value": "NO"}], "NO", ""

        if scope_type == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
            hint = "" if opts else "Fant ingen fylker i CSV. Sjekk at kolonnen heter 'Fylke' og har verdier."
            return opts, (opts[0]["value"] if opts else None), hint

        # region
        opts = [{"label": r, "value": r} for r in ALL_REGIONER]
        hint = "" if opts else "Fant ingen regioner i CSV. Sjekk at kolonnen heter 'Region' og har verdier (f.eks. NO1/NO2...)."
        return opts, (opts[0]["value"] if opts else None), hint

    # -----------------------------
    # NAV: landing <-> main
    # -----------------------------
    @app.callback(
        Output("app-state", "data"),
        Output("scope-store", "data"),
        Output("page", "children"),
        Input("start-btn", "n_clicks"),
        Input("back-btn", "n_clicks"),
        State("scope-type", "value"),
        State("scope-id", "value"),
        State("landing-mode", "value"),
        prevent_initial_call=True,
    )
    def goto_main(n_start, n_back, scope_type, scope_id, landing_mode):
        trig = get_trigger_id()

        if trig == "back-btn":
            return {"stage": "landing"}, {"type": "country", "id": "NO", "mode": "Bolig"}, landing_layout()

        # start
        scope_type = scope_type or "country"
        if scope_type == "country":
            scope_id = "NO"
        if not scope_id:
            raise exceptions.PreventUpdate

        return {"stage": "main"}, {"type": scope_type, "id": str(scope_id), "mode": landing_mode or "Bolig"}, main_layout()

    # -----------------------------
    # MAIN: sett mode fra landing
    # -----------------------------
    @app.callback(
        Output("mode", "value"),
        Input("scope-store", "data"),
        prevent_initial_call=False,
    )
    def sync_mode(scope_store):
        if not scope_store:
            return "Bolig"
        return scope_store.get("mode", "Bolig") or "Bolig"

    # -----------------------------
    # Hovedoppdatering
    # -----------------------------
    @app.callback(
        Output("map", "figure"),
        Output("scatter", "figure"),
        Output("tables", "children"),
        Output("click-info", "children"),
        Output("debug-stats", "children"),
        Input("app-state", "data"),
        Input("mode", "value"),
        Input("period", "value"),
        Input("marker_scale", "value"),
        Input("map", "clickData"),
        State("scope-store", "data"),
    )
    def update_all(state, mode, period, marker_scale, clickData, scope_store):
        if not state or state.get("stage") != "main":
            return no_update, no_update, no_update, no_update, no_update

        mode = mode or scope_store.get("mode", "Bolig") or "Bolig"
        df = get_df_cached(mode)

        # velg endringskolonne
        period = period or "q4"
        change_map = {
            "oct": CSV_COLS["incr_oct"],
            "nov": CSV_COLS["incr_nov"],
            "dec": CSV_COLS["incr_dec"],
            "q4": CSV_COLS["incr_q4"],
        }
        change_col = change_map.get(period, CSV_COLS["incr_q4"])
        if change_col not in df.columns:
            df["change_val"] = np.nan
        else:
            df["change_val"] = df[change_col]

        # scope-filter
        scope_type = (scope_store or {}).get("type", "country")
        scope_id = (scope_store or {}).get("id", "NO")

        dff = df
        if scope_type == "county":
            dff = df[df[CSV_COLS["fylke"]] == scope_id].copy()
        elif scope_type == "region":
            dff = df[df[CSV_COLS["region"]] == scope_id].copy()

        # Zoom/center etter scope
        bboxes = [feature_bbox_by_nr.get(k) for k in dff["knr_norm"].tolist() if k in feature_bbox_by_nr]
        b_union = bbox_union([b for b in bboxes if b])
        if b_union != (0.0, 0.0, 0.0, 0.0):
            center = bbox_center(b_union)
            zoom = zoom_for_bbox(b_union[2] - b_union[0], b_union[3] - b_union[1])
        else:
            center = {"lon": DEFAULT_VIEW["lon"], "lat": DEFAULT_VIEW["lat"]}
            zoom = DEFAULT_VIEW["zoom"]

        # Stats
        valid_change = dff["change_val"].dropna()
        stats_txt = (
            f"Endringskolonne: {change_col} | "
            f"Gyldige endringer: {len(valid_change)}/{len(dff)} | "
            f"Mangler andel: {int(dff['andel'].isna().sum())}"
        )
        if len(valid_change) > 0:
            stats_txt += f" | min={valid_change.min():.3f}, max={valid_change.max():.3f}"

        # Choropleth + hover med andel + endring
        # Bruk geojson properties._knr_norm
        fig_map = px.choropleth_mapbox(
            dff,
            geojson=gj,
            locations="knr_norm",
            featureidkey="properties._knr_norm",
            color="andel",
            color_continuous_scale="RdBu",
            range_color=(0, 1),
            mapbox_style="carto-positron",
            zoom=zoom,
            center={"lat": center["lat"], "lon": center["lon"]},
            opacity=0.65,
            hover_name=CSV_COLS["kommune"],
            hover_data={
                "knr_norm": True,
                "andel": False,  # vi viser formatert selv
                "andel_pct": True,
                "change_val": True,
            },
        )

        # Skreddersy hover-tekst (andel + endring)
        fig_map.update_traces(
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Kommunenr: %{customdata[0]}<br>"
                "Andel Norgespris: %{customdata[1]}<br>"
                "Endring: %{customdata[2]:.3f}<br>"
                "<extra></extra>"
            ),
            customdata=np.stack(
                [
                    dff["knr_norm"].fillna("").astype(str),
                    dff["andel_pct"].fillna("—").astype(str),
                    dff["change_val"].fillna(np.nan).astype(float),
                ],
                axis=-1,
            ),
        )

        # Prikker for endring (størrelse = abs(endring)*skala)
        mscale = float(marker_scale or 10.0)
        pts = dff.dropna(subset=["change_val"]).copy()
        pts["lon"] = pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[0])
        pts["lat"] = pts["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[1])
        pts = pts.dropna(subset=["lon", "lat"])

        if len(pts) > 0:
            fig_map.add_trace(
                go.Scattermapbox(
                    lon=pts["lon"],
                    lat=pts["lat"],
                    mode="markers",
                    marker=dict(
                        size=np.clip(pts["change_val"].abs() * mscale, 2, 40),
                        color=pts["change_val"],
                        colorscale="Picnic",
                        showscale=False,
                        opacity=0.85,
                    ),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Andel Norgespris: %{customdata[0]}<br>"
                        "Endring: %{customdata[1]:.3f}<br>"
                        "<extra></extra>"
                    ),
                    text=pts[CSV_COLS["kommune"]],
                    customdata=np.stack(
                        [
                            pts["andel_pct"].fillna("—").astype(str),
                            pts["change_val"].fillna(np.nan).astype(float),
                        ],
                        axis=-1,
                    ),
                    name="Endring",
                )
            )

        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

        # Scatter (andel vs endring)
        fig_scatter = px.scatter(
            dff,
            x="andel",
            y="change_val",
            hover_name=CSV_COLS["kommune"],
            labels={"andel": "Andel Norgespris", "change_val": "Endring"},
            title="Sammenheng: Norgespris-andel vs endring",
        )

        # Tabeller (top/bunn andel)
        top = dff.dropna(subset=["andel"]).nlargest(10, "andel")[[CSV_COLS["kommune"], "andel_pct", "change_val"]].copy()
        bot = dff.dropna(subset=["andel"]).nsmallest(10, "andel")[[CSV_COLS["kommune"], "andel_pct", "change_val"]].copy()

        def mk_table(df_in, title):
            df_show = df_in.copy()
            df_show["change"] = df_show["change_val"].apply(lambda v: ("—" if pd.isna(v) else f"{v:.3f}".replace(".", ",")))
            df_show = df_show.drop(columns=["change_val"])
            return html.Div(
                style={"marginTop": "10px"},
                children=[
                    html.B(title),
                    dash_table.DataTable(
                        data=df_show.to_dict("records"),
                        columns=[
                            {"name": "KOMMUNE", "id": CSV_COLS["kommune"]},
                            {"name": "Andel Norgespris", "id": "andel_pct"},
                            {"name": "Endring", "id": "change"},
                        ],
                        page_size=10,
                        style_table={"overflowX": "auto"},
                        style_cell={"padding": "6px", "fontFamily": "system-ui", "fontSize": "12px"},
                        style_header={"fontWeight": "700"},
                    ),
                ],
            )

        tables = html.Div(children=[mk_table(top, "Høyest andel"), mk_table(bot, "Lavest andel")])

        # Klikk-panel: vis andel + endring
        click_box = html.Div(
            style={"padding": "10px", "border": "1px solid #eee", "borderRadius": "10px", "background": "#fff"},
            children=[html.B("Klikk på en kommune i kartet for detaljer."), html.Div(style={"color": "#666"}, children="")],
        )

        if clickData and clickData.get("points"):
            p = clickData["points"][0]
            knr = None
            # choropleth har location i p.get("location")
            if "location" in p:
                knr = normalize_kommunenr(p["location"])
            # scattermapbox kan ha customdata, men vi prøver location først
            if knr:
                row = dff[dff["knr_norm"] == knr]
                if len(row) > 0:
                    r = row.iloc[0]
                    change_v = r.get("change_val", np.nan)
                    click_box = html.Div(
                        style={"padding": "10px", "border": "1px solid #eee", "borderRadius": "10px", "background": "#fff"},
                        children=[
                            html.B(str(r.get(CSV_COLS["kommune"], "Ukjent"))),
                            html.Div(f"Kommunenr: {knr}"),
                            html.Div(f"Andel Norgespris: {r.get('andel_pct', '—')}"),
                            html.Div(f"Endring: {('—' if pd.isna(change_v) else f'{change_v:.3f}'.replace('.', ','))}"),
                            html.Div(style={"color": "#666", "fontSize": "12px", "marginTop": "6px"},
                                     children="(Andel = Norgespris / Alle. Endring = valgt incr-kolonne.)"),
                        ],
                    )

        return fig_map, fig_scatter, tables, click_box, stats_txt

    return app
