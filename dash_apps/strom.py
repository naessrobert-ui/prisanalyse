# dash_apps/strom.py
# -*- coding: utf-8 -*-

import math
import json
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional, Set

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

def zoom_for_bbox(lon_span: float, lat_span: float) -> float:
    span = max(lon_span, lat_span)
    # Grov, men stabil: større område -> lavere zoom
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
    if span <= 14.0:
        return 4.0
    return 3.6


# -----------------------------
# DATA-LOADING (én gang når Dash monteres)
# -----------------------------
def load_resources() -> tuple[pd.DataFrame, dict, dict, list, str, dict, dict, dict]:
    # CSV
    df_raw = pd.read_csv(CSV_PATH, sep=CSV_SEP, low_memory=False)
    df_raw.columns = normalize_columns(df_raw.columns)

    df_raw[CSV_COLS["knr"]] = df_raw[CSV_COLS["knr"]].apply(normalize_kommunenr)

    # Nytt: scope-felt
    df_raw[CSV_COLS["fylke"]] = df_raw[CSV_COLS["fylke"]].astype(str).str.strip()
    df_raw[CSV_COLS["region"]] = df_raw[CSV_COLS["region"]].astype(str).str.strip().str.upper()

    change_cols_found = {k: find_col(df_raw.columns, aliases) for k, aliases in CHANGE_ALIASES.items()}

    # GeoJSON
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
    features_by_nr: Dict[str, Dict[str, Any]] = {}

    for feat in features:
        knr = normalize_kommunenr(feat["properties"].get(geo_nr_key, ""))
        if not knr:
            continue
        features_by_nr[knr] = feat
        feature_bbox_by_nr[knr] = get_feature_bbox(feat)
        c = feature_centroid(feat)
        if c:
            centroid_by_nr[knr] = c

    return df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr, features_by_nr


# -----------------------------
# DASH FACTORY
# -----------------------------
def create_dash_app(flask_server):
    """
    Monter Dash inni Flask.
    Dash blir tilgjengelig på /stromdash/
    """
    df_raw, change_cols_found, gj, features, geo_nr_key, feature_bbox_by_nr, centroid_by_nr, features_by_nr = load_resources()

    # Scope-lister til landing
    ALL_FYLKER = sorted([x for x in df_raw[CSV_COLS["fylke"]].dropna().unique().tolist() if x and x != "nan"])
    ALL_REGIONER = sorted([x for x in df_raw[CSV_COLS["region"]].dropna().unique().tolist() if x and x != "nan"])

    # Cache: df per mode (Bolig/Fritid)
    df_cache: Dict[str, pd.DataFrame] = {}

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

    def get_df_cached(mode_value: str) -> pd.DataFrame:
        key = "Bolig" if mode_value == "Bolig" else "Fritid"
        if key not in df_cache:
            df_cache[key] = build_df(key)
        return df_cache[key]

    # Cache: geojson & kommune-sett per scope
    geojson_scope_cache: Dict[tuple, dict] = {}
    knr_scope_cache: Dict[tuple, Set[str]] = {}

    def get_scope_knr(scope_type: str, scope_id: str) -> Set[str]:
        key = (scope_type, scope_id)
        if key in knr_scope_cache:
            return knr_scope_cache[key]

        if scope_type == "country":
            s = set(df_raw[CSV_COLS["knr"]].astype(str).tolist())
        elif scope_type == "county":
            s = set(df_raw.loc[df_raw[CSV_COLS["fylke"]] == scope_id, CSV_COLS["knr"]].astype(str).tolist())
        elif scope_type == "region":
            sid = (scope_id or "").strip().upper()
            s = set(df_raw.loc[df_raw[CSV_COLS["region"]] == sid, CSV_COLS["knr"]].astype(str).tolist())
        else:
            s = set(df_raw[CSV_COLS["knr"]].astype(str).tolist())

        knr_scope_cache[key] = s
        return s

    def get_scope_geojson(scope_type: str, scope_id: str) -> dict:
        key = (scope_type, scope_id)
        if key in geojson_scope_cache:
            return geojson_scope_cache[key]

        knr_set = get_scope_knr(scope_type, scope_id)
        feats = [features_by_nr[k] for k in knr_set if k in features_by_nr]
        gj_sub = {"type": "FeatureCollection", "features": feats}
        geojson_scope_cache[key] = gj_sub
        return gj_sub

    def scope_bbox(scope_type: str, scope_id: str) -> Optional[Tuple[float, float, float, float]]:
        knr_set = get_scope_knr(scope_type, scope_id)
        boxes = [feature_bbox_by_nr.get(k) for k in knr_set if k in feature_bbox_by_nr]
        boxes = [b for b in boxes if b and b != (0.0, 0.0, 0.0, 0.0)]
        if not boxes:
            return None
        minx = min(b[0] for b in boxes)
        miny = min(b[1] for b in boxes)
        maxx = max(b[2] for b in boxes)
        maxy = max(b[3] for b in boxes)
        return (minx, miny, maxx, maxy)

    def default_view_for_scope(scope_type: str, scope_id: str) -> dict:
        # fallback Norge
        base = {"lon": 13.0, "lat": 65.0, "zoom": 4.0}

        if scope_type == "country":
            return base

        bb = scope_bbox(scope_type, scope_id)
        if not bb:
            # litt nærmere zoom enn landet hvis scope ikke kan beregnes
            return {"lon": base["lon"], "lat": base["lat"], "zoom": 4.8}

        minx, miny, maxx, maxy = bb
        lon = (minx + maxx) / 2
        lat = (miny + maxy) / 2
        z = zoom_for_bbox(maxx - minx, maxy - miny)

        # liten justering: fylke zoomes litt nærmere enn region
        if scope_type == "county":
            z = min(9.0, z + 0.4)
        elif scope_type == "region":
            z = min(8.5, z + 0.1)

        return {"lon": float(lon), "lat": float(lat), "zoom": float(z)}

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
        gj_scope: dict,
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
            geojson=gj_scope,
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
                "<b>%{hovertext}</b><br>"
                "Andel Norgespris: %{{customdata[0]}}<br>"
                f"Endring {period_label} (%): %{{customdata[1]}}<br>"
                "Norgespris: %{{customdata[2]}}<br>"
                "Total: %{{customdata[3]}}<extra></extra>"
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

        fig.add_trace(
            go.Scattermapbox(
                lon=dff2["lon"],
                lat=dff2["lat"],
                mode="markers",
                marker=dict(size=dff2["chg_size"], color=dff2["chg_color"], opacity=0.85),
                hovertext=(
                    dff2[CSV_COLS["kommune"]].astype(str)
                    + "<br>Andel Norgespris: " + dff2["andel_pct0"].astype(str)
                    + f"<br>Endring {period_label} (%): " + dff2["change_pct"].apply(fmt_pct)
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
                + "<br>Endring " + dff2["change_pct"].apply(fmt_pct)
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
        routes_pathname_prefix="/stromdash/",
        requests_pathname_prefix="/stromdash/",
        suppress_callback_exceptions=True,
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

    def landing_layout():
        return html.Div(
            style={"maxWidth": "980px"},
            children=[
                html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 8px 0"}),
                html.P(
                    "Kartet viser hvor stor andel av husholdningenes strømforbruk som omfattes av Norgespris i hver kommune, "
                    "og hvordan dette har endret seg siden forrige periode.",
                    style={"marginTop": 0, "color": "#333", "fontSize": "16px", "lineHeight": "1.45"},
                ),

                html.Div(
                    style={
                        "border": "1px solid #e5e5e5",
                        "borderRadius": "14px",
                        "padding": "14px",
                        "background": "#fafafa",
                        "display": "grid",
                        "gridTemplateColumns": "1fr 1fr",
                        "gap": "12px",
                        "alignItems": "end",
                    },
                    children=[
                        html.Div(children=[
                            html.Label("Start med område (gir raskere lasting):", style={"fontWeight": "800"}),
                            dcc.RadioItems(
                                id="scope-type",
                                options=[
                                    {"label": "Hele landet", "value": "country"},
                                    {"label": "Strømregion", "value": "region"},
                                    {"label": "Fylke", "value": "county"},
                                ],
                                value="country",
                                labelStyle={"display": "block", "margin": "6px 0"},
                                inputStyle={"marginRight": "8px"},
                            ),
                        ]),

                        html.Div(children=[
                            html.Label("Velg:", style={"fontWeight": "800"}),
                            dcc.Dropdown(
                                id="scope-id",
                                options=[{"label": "Norge", "value": "NO"}],
                                value="NO",
                                clearable=False,
                            ),
                            html.Div("Tips: du kan endre område senere.", style={"color": "#666", "fontSize": "13px", "marginTop": "6px"}),
                        ]),

                        html.Div(children=[
                            html.Label("Vis data for:", style={"fontWeight": "800"}),
                            dcc.Dropdown(
                                id="landing-mode",
                                options=[{"label": "Bolig", "value": "Bolig"}, {"label": "Fritidsbolig", "value": "Fritid"}],
                                value="Bolig",
                                clearable=False,
                            ),
                        ]),

                        html.Div(children=[
                            html.Button(
                                "Vis kart",
                                id="start-btn",
                                n_clicks=0,
                                style={
                                    "width": "100%",
                                    "padding": "12px 14px",
                                    "borderRadius": "12px",
                                    "border": "1px solid #ccc",
                                    "background": "white",
                                    "fontWeight": "800",
                                    "cursor": "pointer",
                                    "boxShadow": "0 1px 6px rgba(0,0,0,0.08)",
                                },
                            ),
                            html.Div("Kan ta noen sekunder å laste første gang.", style={"color": "#666", "fontSize": "13px", "marginTop": "8px"}),
                        ]),
                    ],
                ),

                html.Div(style={"marginTop": "14px"}, children=[
                    html.H3("Slik leser du kartet", style={"margin": "10px 0 6px 0"}),
                    html.Ul(style={"marginTop": 0, "color": "#333", "lineHeight": "1.5"}, children=[
                        html.Li("Farge viser nivå: rød = lav andel, blå = høy andel (terskler kan justeres)."),
                        html.Li("Prikkene viser endring: større prikk = større endring i prosentpoeng."),
                        html.Li("Tabellene viser høyest/lavest i utsnittet du ser (zoom/pan for å filtrere)."),
                    ]),
                ]),
            ],
        )

    def main_layout():
        return html.Div(
            children=[
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between", "gap": "10px", "alignItems": "baseline"},
                    children=[
                        html.H1("Norgespris per kommune – interaktivt kart", style={"margin": "0 0 10px 0"}),
                        html.Button(
                            "Endre område",
                            id="change-scope",
                            n_clicks=0,
                            style={
                                "padding": "10px 12px",
                                "borderRadius": "12px",
                                "border": "1px solid #d0d0d0",
                                "background": "white",
                                "cursor": "pointer",
                                "fontWeight": "700",
                            },
                        ),
                    ],
                ),

                html.Div(id="scope-label", style={"color": "#555", "margin": "0 0 10px 0"}),

                # Filtre (din opprinnelige filter-boks)
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

                # Tung del med loading overlay
                dcc.Loading(
                    type="default",
                    children=[
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
                                                    style={"height": "calc(100vh - 220px)", "minHeight": "720px"},
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
                                    ],
                                ),

                                html.Div(
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
                        )
                    ],
                ),
            ],
        )

    app.layout = serve_layout

    def serve_layout():
        return html.Div(
            style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif", "padding": "12px"},
            children=[
                dcc.Store(id="app-state", data={"stage": "landing"}),  # landing | ready
                dcc.Store(id="scope-store", data={"type": "country", "id": "NO"}),
                dcc.Store(id="relayout-store"),
                dcc.Store(id="view-store", data=DEFAULT_VIEW),
                html.Div(id="page", children=landing_layout()),  # default uten blank side
            ],
        )

    # -----------------------------
    # ROUTING / LANDING
    # -----------------------------
    @app.callback(
        Output("page", "children"),
        Input("app-state", "data"),
    )
    def render_page(state):
        stage = (state or {}).get("stage", "landing")
        if stage == "ready":
            return main_layout()
        return landing_layout()

    @app.callback(
        Output("scope-id", "options"),
        Output("scope-id", "value"),
        Input("scope-type", "value"),
    )
    def update_scope_options(scope_type):
        if scope_type == "country":
            return ([{"label": "Norge", "value": "NO"}], "NO")
        if scope_type == "region":
            opts = [{"label": r, "value": r} for r in ALL_REGIONER]
            return (opts, (ALL_REGIONER[0] if ALL_REGIONER else "NO1"))
        if scope_type == "county":
            opts = [{"label": f, "value": f} for f in ALL_FYLKER]
            return (opts, (ALL_FYLKER[0] if ALL_FYLKER else ""))
        return ([{"label": "Norge", "value": "NO"}], "NO")

    @app.callback(
        Output("app-state", "data"),
        Output("scope-store", "data"),
        Output("mode", "value"),
        Output("view-store", "data"),
        Input("start-btn", "n_clicks"),
        State("scope-type", "value"),
        State("scope-id", "value"),
        State("landing-mode", "value"),
        prevent_initial_call=True,
    )
    def start_app(n, scope_type, scope_id, landing_mode):
        if not n:
            return no_update, no_update, no_update, no_update
        scope_type = scope_type or "country"
        scope_id = scope_id or "NO"
        view = default_view_for_scope(scope_type, scope_id)
        return {"stage": "ready"}, {"type": scope_type, "id": scope_id}, landing_mode, view

    @app.callback(
        Output("app-state", "data"),
        Input("change-scope", "n_clicks"),
        prevent_initial_call=True,
    )
    def go_back(n):
        if not n:
            return no_update
        return {"stage": "landing"}

    @app.callback(
        Output("scope-label", "children"),
        Input("scope-store", "data"),
    )
    def scope_label(scope):
        scope = scope or {"type": "country", "id": "NO"}
        t = scope.get("type", "country")
        sid = scope.get("id", "NO")
        if t == "country":
            return "Viser: Hele landet"
        if t == "region":
            return f"Viser: Strømregion {sid}"
        if t == "county":
            return f"Viser: Fylke {sid}"
        return "Viser: Hele landet"

    # -----------------------------
    # CALLBACKS: MAP
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
        Input("scope-store", "data"),
    )
    def update_map(mode_value, low, high, change_period, chg_red_le, chg_blue_ge, marker_scale_pct, view, scope):
        mode_value = "Bolig" if mode_value == "Bolig" else "Fritid"
        df = get_df_cached(mode_value)

        scope = scope or {"type": "country", "id": "NO"}
        scope_type = scope.get("type", "country")
        scope_id = scope.get("id", "NO")

        knr_set = get_scope_knr(scope_type, scope_id)
        df = df[df[CSV_COLS["knr"]].isin(knr_set)].copy()

        gj_scope = get_scope_geojson(scope_type, scope_id)

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
            gj_scope=gj_scope,
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

        # Kart-interaksjon (pan/zoom med mus)
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

        # Zoom-knapper (bruk view-store som basis)
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
    # CALLBACKS: TABLES / SCATTER
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
        Input("scope-store", "data"),
    )
    def update_tables_scatter_debug(mode_value, change_period, relayout, scope):
        mode_value = "Bolig" if mode_value == "Bolig" else "Fritid"
        df = get_df_cached(mode_value).copy()

        scope = scope or {"type": "country", "id": "NO"}
        scope_type = scope.get("type", "country")
        scope_id = scope.get("id", "NO")

        knr_set = get_scope_knr(scope_type, scope_id)
        df = df[df[CSV_COLS["knr"]].isin(knr_set)].copy()

        period_label = change_label(change_period)
        change_col = change_cols_found.get(change_period)

        if change_col and change_col in df.columns:
            df["change_pct"] = df[change_col]
        else:
            df["change_pct"] = float("nan")

        bbox = viewport_bbox_from_relayout(relayout or {})

        if bbox is None:
            visible = set(df[CSV_COLS["knr"]].tolist())
            count_text = "Viser alle kommuner i valgt område (zoom/pan i kartet for å filtrere på synlig utsnitt)."
        else:
            visible: Set[str] = set()
            for knr in knr_set:
                fb = feature_bbox_by_nr.get(knr)
                if fb and bboxes_intersect(fb, bbox):
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

        scatter_fig = build_scatter_fig(df, change_period=change_period, visible_knr=visible)

        n_total = len(df)
        n_ok = int(df["change_pct"].notna().sum())
        mn = df["change_pct"].min(skipna=True)
        mx = df["change_pct"].max(skipna=True)

        missing_share = int(df["andel"].isna().sum())
        missing_change = int(df["change_pct"].isna().sum())
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
