# dash_apps/strom.py
# -*- coding: utf-8 -*-

import math
import json
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional, Set, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, dash_table, callback_context, no_update


# -----------------------------
# FIL / KOLONNER
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # prosjektroten (prisanalyse/)
CSV_PATH = BASE_DIR / "static" / "data" / "kommuner.csv"
CSV_SEP = ";"

CSV_COLS = {
    "kommune": "KOMMUNE",
    "knr": "KOMMUNENUMMER",
    "bolig_np": "Bolig-Norgespris",
    "bolig_tot": "Bolig-alle",
    "fritid_np": "Fritid-Norgespris",
    "fritid_tot": "Fritid-alle",
    "fylke": "Fylke",
    "region": "Region",
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
    # robust for "0,15" / "0.15" / "1 234,56" etc
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
    return ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)

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

def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    if span <= 0.5: return 8.0
    if span <= 1.0: return 7.0
    if span <= 2.0: return 6.2
    if span <= 4.0: return 5.4
    if span <= 8.0: return 4.6
    return 3.7

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


# -----------------------------
# DATA-LOADING (én gang når Dash monteres)
# -----------------------------
def load_resources() -> tuple[pd.DataFrame, dict, dict, list, str, dict, dict]:
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)

    # Fylke/Region fra CSV (brukes i dropdown)
    if CSV_COLS["fylke"] in df_raw.columns:
        df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    else:
        df_raw[CSV_COLS["fylke"]] = ""

    if CSV_COLS["region"] in df_raw.columns:
        df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip()
    else:
        df_raw[CSV_COLS["region"]] = ""

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

    # dropdown-lister
    def uniq_list(col: str) -> list[str]:
        s = df_raw[col].dropna().astype(str).str.strip()
        s = s[(s != "") & (s.str.lower() != "nan")]
        return sorted(set(s.tolist()))

    ALL_FYLKER = uniq_list(CSV_COLS["fylke"]) if CSV_COLS["fylke"] in df_raw.columns else []
    ALL_REGIONER = uniq_list(CSV_COLS["region"]) if CSV_COLS["region"] in df_raw.columns else []

    def build_df(mode_value: str) -> pd.DataFrame:
        df = df_raw.copy()

        if mode_value == "Bolig":
            np_col, tot_col = CSV_COLS["bolig_np"], CSV_COLS["bolig_tot"]
        else:
            np_col, tot_col = CSV_COLS["fritid_np"], CSV_COLS["fritid_tot"]

        df[np_col] = to_number(df[np_col])
        df[tot_col] = to_number(df[tot_col])

        df["norgespris"] = df[np_col]
        df["total"] = df[tot_col]
        df["andel"] = df.apply(lambda r: safe_div(r["norgespris"], r["total"]), axis=1)
        df["andel_pct0"] = df["andel"].apply(pct0)

        for _, col in change_cols_found.items():
            if col and col in df.columns:
                df[col] = to_number(df[col])

        df["knr_norm"] = df[CSV_COLS["knr"]].apply(normalize_kommunenr)
        return df

    def filter_scope(df: pd.DataFrame, geo_type: str, geo_value: str) -> pd.DataFrame:
        if geo_type == "country":
            return df
        if geo_type == "county":
            return df[df[CSV_COLS["fylke"]] == geo_value].copy()
        if geo_type == "region":
            return df[df[CSV_COLS["region"]] == geo_value].copy()
        return df

    def scope_view(df_scope: pd.DataFrame, default_view: Dict[str, float]) -> Dict[str, float]:
        knrs = df_scope["knr_norm"].dropna().astype(str).tolist()
        bbs = [feature_bbox_by_nr.get(k) for k in knrs if k in feature_bbox_by_nr]
        u = bbox_union([b for b in bbs if b])
        if not u:
            return dict(default_view)
        center_lon = (u[0] + u[2]) / 2
        center_lat = (u[1] + u[3]) / 2
        z = zoom_for_bbox(u[2] - u[0], u[3] - u[1])
        return {"lon": float(center_lon), "lat": float(center_lat), "zoom": float(z)}

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

        dff = df.copy()
        if change_col and change_col in dff.columns:
            dff["change_pct"] = dff[change_col]
        else:
            dff["change_pct"] = float("nan")
        dff["change_pct_str"] = dff["change_pct"].apply(fmt_pct)

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
            locations="knr_norm",
            featureidkey=f"properties.{geo_nr_key}",
            color="kategori",
            color_discrete_map=color_map,
            hover_name=CSV_COLS["kommune"],
            custom_data=["andel_pct0", "change_pct_str", "norgespris", "total", "knr_norm"],
            opacity=0.75,
        )

        fig.update_traces(
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Kommunenr: %{customdata[4]}<br>"
                "Andel Norgespris: %{customdata[0]}<br>"
                f"Endring {period_label} (%): %{customdata[1]}<br>"
                "Norgespris: %{customdata[2]}<br>"
                "Total: %{customdata[3]}<extra></extra>"
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
        dff2["lon"] = dff2["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[0])
        dff2["lat"] = dff2["knr_norm"].map(lambda k: centroid_by_nr.get(k, (None, None))[1])
        dff2 = dff2.dropna(subset=["lon", "lat", "change_pct"]).copy()
        dff2["chg_color"] = dff2["change_pct"].apply(change_color)

        scale = max(0.1, float(marker_scale_pct))
        abs_chg = dff2["change_pct"].abs().clip(upper=scale)
        dff2["chg_size"] = 12 + 26 * (abs_chg / scale)

        # legg customdata med andel + endring for klikk-info
        fig.add_trace(
            go.Scattermapbox(
                lon=dff2["lon"],
                lat=dff2["lat"],
                mode="markers",
                marker=dict(size=dff2["chg_size"], color=dff2["chg_color"], opacity=0.85),
                customdata=list(zip(dff2["andel_pct0"], dff2["change_pct_str"], dff2["knr_norm"], dff2[CSV_COLS["kommune"]])),
                hovertemplate=(
                    "<b>%{customdata[3]}</b><br>"
                    "Kommunenr: %{customdata[2]}<br>"
                    "Andel Norgespris: %{customdata[0]}<br>"
                    f"Endring {period_label} (%): %{customdata[1]}<extra></extra>"
                ),
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
            dff = dff[dff["knr_norm"].isin(visible_knr)].copy()

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

        fig.update_layout(
            margin=dict(l=0, r=0, t=45, b=0),
            height=340,
            title=f"Sammenheng: Norgespris-andel vs endring i forbruk ({period_label})",
        )
        return fig

    # -----------------------------
    # DASH APP
    # -----------------------------
    app = Dash(
        __name__,
        server=flask_server,
        routes_pathname_prefix="/stromdash/",
        requests_pathname_prefix="/stromdash/",
    )
    app.title = "Norgespris per kommune"

    DEFAULT_VIEW = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

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

    app.layout = html.Div(
        style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif", "padding": "12px"},
        children=[
            html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 10px 0"}),

            # --- NYTT: Geografi/område ---
            html.Div(
                style={
                    "display": "flex",
                    "gap": "14px",
                    "alignItems": "end",
                    "marginBottom": "10px",
                    "padding": "10px 12px",
                    "border": "1px solid #e5e5e5",
                    "borderRadius": "12px",
                    "background": "#fff",
                    "flexWrap": "wrap",
                },
                children=[
                    html.Div(children=[
                        html.Label("Geografi:", style={"fontWeight": "800", "fontSize": "14px"}),
                        dcc.Dropdown(
                            id="geo-type",
                            options=[
                                {"label": "Hele landet", "value": "country"},
                                {"label": "Fylke", "value": "county"},
                                {"label": "Strømregion", "value": "region"},
                            ],
                            value="country",
                            clearable=False,
                            style={"width": "180px"},
                        ),
                    ]),
                    html.Div(children=[
                        html.Label("Velg område:", style={"fontWeight": "800", "fontSize": "14px"}),
                        dcc.Dropdown(id="geo-value", style={"width": "240px"}),
                    ]),
                    html.Button("Tilbake til Norge", id="geo-reset", n_clicks=0, style={
                        "height": "40px", "padding": "0 14px", "borderRadius": "10px",
                        "border": "1px solid #ccc", "background": "#fafafa", "cursor": "pointer",
                        "fontWeight": "700"
                    }),
                ],
            ),

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
                        html.Label("Rød ≤ (endring %)", style={"fontWeight": "700", "fontSize": "14px"}),
                        dcc.Input(id="chg_red_le", type="number", value=0.0, step=0.1, style=input_box_style),
                    ]),
                    html.Div(children=[
                        html.Label("Blå ≥ (endring %)", style={"fontWeight": "700", "fontSize": "14px"}),
                        dcc.Input(id="chg_blue_ge", type="number", value=0.0, step=0.1, style=input_box_style),
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
                            html.Div(
                                style={"position": "relative"},
                                children=[
                                    dcc.Graph(
                                        id="map",
                                        style={"height": "calc(100vh - 260px)", "minHeight": "720px"},
                                        config={"scrollZoom": True, "displayModeBar": True, "displaylogo": False},
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
                        style={"gridColumn": "2", "gridRow": "1"},
                        children=[
                            html.H3("Valgt kommune", style={"marginTop": "0"}),
                            html.Div(
                                id="click-info",
                                style={"border": "1px solid #eee", "borderRadius": "10px", "padding": "10px", "background": "#fff", "marginBottom": "12px"},
                            ),

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
                        style={"gridColumn": "2", "gridRow": "2"},
                        children=[
                            dcc.Graph(
                                id="scatter",
                                style={"height": "340px"},
                                config={"displayModeBar": True, "displaylogo": False},
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    # -----------------------------
    # NYTT: geo dropdown options
    # -----------------------------
    @app.callback(
        Output("geo-value", "options"),
        Output("geo-value", "value"),
        Input("geo-type", "value"),
    )
    def update_geo_value_opts(geo_type):
        if geo_type == "country":
            return [{"label": "Norge", "value": "Norge"}], "Norge"
        if geo_type == "county":
            opts = [{"label": x, "value": x} for x in ALL_FYLKER]
            return opts, (opts[0]["value"] if opts else None)
        if geo_type == "region":
            opts = [{"label": x, "value": x} for x in ALL_REGIONER]
            return opts, (opts[0]["value"] if opts else None)
        return [], None

    # Når geo endres: sett view-store til riktig utsnitt (zoom til fylke/region)
    @app.callback(
        Output("view-store", "data"),
        Input("geo-type", "value"),
        Input("geo-value", "value"),
        Input("geo-reset", "n_clicks"),
        State("mode", "value"),
        prevent_initial_call=True,
    )
    def update_view_for_scope(geo_type, geo_value, n_reset, mode_value):
        trig = get_trigger_id()
        if trig == "geo-reset":
            return DEFAULT_VIEW

        if not geo_type or geo_type == "country":
            return DEFAULT_VIEW

        if not geo_value:
            return no_update

        df = build_df("Bolig" if mode_value == "Bolig" else "Fritid")
        df_scope = filter_scope(df, geo_type, geo_value)
        return scope_view(df_scope, DEFAULT_VIEW)

    # -----------------------------
    # Kart
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
        Input("view-store", "data"),
        Input("geo-type", "value"),
        Input("geo-value", "value"),
    )
    def update_map(mode_value, low, high, change_period, chg_red_le, chg_blue_ge, marker_scale_pct, view, geo_type, geo_value):
        df = build_df("Bolig" if mode_value == "Bolig" else "Fritid")
        df = filter_scope(df, geo_type or "country", geo_value or "Norge")

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

    # Håndter både kart-relayout og zoom-knapper
    @app.callback(
        Output("relayout-store", "data"),
        Output("view-store", "data"),
        Input("map", "relayoutData"),
        Input("zoom-in", "n_clicks"),
        Input("zoom-out", "n_clicks"),
        Input("zoom-reset", "n_clicks"),
        State("view-store", "data"),
        prevent_initial_call=True,
    )
    def sync_view_and_relayout(relayout, zin, zout, zreset, view):
        view = view or DEFAULT_VIEW
        trig = get_trigger_id()

        new_view = dict(view)

        if trig == "map" and relayout:
            if "mapbox.zoom" in relayout:
                new_view["zoom"] = float(relayout["mapbox.zoom"])

            if "mapbox.center" in relayout and isinstance(relayout["mapbox.center"], dict):
                c = relayout["mapbox.center"]
                if "lon" in c:
                    new_view["lon"] = float(c["lon"])
                if "lat" in c:
                    new_view["lat"] = float(c["lat"])

            return (relayout or {}), new_view

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
            return (relayout or {}), no_update

        return (relayout or {}), {"lon": lon, "lat": lat, "zoom": zoom}

    # -----------------------------
    # Tabeller + scatter + debug
    # -----------------------------
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
        Input("geo-type", "value"),
        Input("geo-value", "value"),
    )
    def update_tables_scatter_debug(mode_value, change_period, relayout, geo_type, geo_value):
        df = build_df("Bolig" if mode_value == "Bolig" else "Fritid")
        df = filter_scope(df, geo_type or "country", geo_value or "Norge")

        period_label = change_label(change_period)
        change_col = change_cols_found.get(change_period)

        if change_col and change_col in df.columns:
            df["change_pct"] = df[change_col]
        else:
            df["change_pct"] = float("nan")

        bbox = viewport_bbox_from_relayout(relayout or {})

        if bbox is None:
            visible = set(df["knr_norm"].tolist())
            count_text = "Viser alle kommuner i valgt område (zoom/pan i kartet for å filtrere på synlig utsnitt)."
        else:
            visible: Set[str] = set()
            for knr, fb in feature_bbox_by_nr.items():
                if bboxes_intersect(fb, bbox):
                    visible.add(knr)
            count_text = f"Kommuner i synlig utsnitt: {len(visible)}"

        dff = df[df["knr_norm"].isin(visible)].copy()
        dff["andel_num"] = dff["andel"].fillna(-1)

        dff["Andel Norgespris"] = dff["andel_pct0"]
        dff[f"Endring {period_label} (%)"] = dff["change_pct"].apply(fmt_pct)

        cols_show = [CSV_COLS["kommune"], "Andel Norgespris", f"Endring {period_label} (%)"]
        top_df = dff.sort_values("andel_num", ascending=False)[cols_show].head(15)
        bottom_df = dff.sort_values("andel_num", ascending=True)[cols_show].head(15)
        columns = [{"name": c, "id": c} for c in cols_show]

        scatter_fig = build_scatter_fig(df, change_period=change_period, visible_knr=visible)

        n_total = len(df)
        n_ok = int(df["change_pct"].notna().sum())
        mn = df["change_pct"].min(skipna=True)
        mx = df["change_pct"].max(skipna=True)

        missing_share = int(df["andel"].isna().sum())
        missing_change = int(df["change_pct"].isna().sum())
        total0 = int((df["total"].fillna(0) == 0).sum())

        debug = (
            f"Område-filter: {geo_type}:{geo_value} | "
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

    # -----------------------------
    # NYTT: Klikk-info (andel + endring)
    # -----------------------------
    @app.callback(
        Output("click-info", "children"),
        Input("map", "clickData"),
        Input("mode", "value"),
        Input("change_period", "value"),
        Input("geo-type", "value"),
        Input("geo-value", "value"),
    )
    def show_click_info(clickData, mode_value, change_period, geo_type, geo_value):
        period_label = change_label(change_period)

        if not clickData or not clickData.get("points"):
            return html.Div("Klikk på en kommune i kartet for å se detaljer.", style={"color": "#666"})

        p = clickData["points"][0]

        knr = None
        if "location" in p and p["location"]:
            knr = normalize_kommunenr(p["location"])

        # fallback: customdata fra prikk-trace
        if (not knr) and ("customdata" in p) and p["customdata"] and len(p["customdata"]) >= 3:
            knr = normalize_kommunenr(p["customdata"][2])

        if not knr:
            return html.Div("Klarte ikke å lese kommunenr fra klikket.", style={"color": "#b00"})

        df = build_df("Bolig" if mode_value == "Bolig" else "Fritid")
        df = filter_scope(df, geo_type or "country", geo_value or "Norge")

        change_col = change_cols_found.get(change_period)
        if change_col and change_col in df.columns:
            df["change_pct"] = df[change_col]
        else:
            df["change_pct"] = float("nan")

        row = df[df["knr_norm"] == knr]
        if row.empty:
            return html.Div(f"Fant ikke rad for kommunenr {knr} (etter område-filter).", style={"color": "#b00"})

        r = row.iloc[0]
        return html.Div(
            children=[
                html.Div(str(r.get(CSV_COLS["kommune"], "")), style={"fontWeight": "900", "fontSize": "16px"}),
                html.Div(f"Kommunenr: {knr}"),
                html.Hr(style={"margin": "8px 0"}),
                html.Div(f"Andel Norgespris: {r.get('andel_pct0', '—')}", style={"fontWeight": "700"}),
                html.Div(f"Endring {period_label} (%): {fmt_pct(r.get('change_pct'))}", style={"fontWeight": "700"}),
            ]
        )

    return app
