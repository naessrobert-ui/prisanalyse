from pathlib import Path

import pandas as pd
import streamlit as st

from shipping_app.data import load_spot_rate_prediction_bundle
from shipping_app.ui import render_sidebar

st.set_page_config(
    page_title="Spot Rate Predictions",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)

render_sidebar()

st.title("Spot Rate Predictions")

model_toggle = st.radio(
    "Model",
    ["Slide models", "Schwartz-Smith"],
    horizontal=True,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
_DATA_DIR = Path(__file__).resolve().parent.parent / "shipping_app" / "Data"

bundle = load_spot_rate_prediction_bundle()
predictions_df = bundle["predictions"]
selected_models_df = bundle["selected_models"]


@st.cache_data
def load_ss_forecasts() -> pd.DataFrame:
    df = pd.read_excel(_DATA_DIR / "SS_forecasts.xlsx")
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


@st.cache_data
def load_tce_history() -> pd.DataFrame:
    df = pd.read_csv(_DATA_DIR / "monthly_tce_history.csv", parse_dates=["month"])
    return df.rename(columns={"month": "Date", "tce": "TCE", "vessel_type": "vessel"})


df_ss = load_ss_forecasts()
df_act = load_tce_history()

# Vessel name mappings
SLIDE_TO_SS_PREFIX = {
    "vlcc": "VLCC",
    "suezmax": "Suezmax",
    "aframax": "Aframax/LR2",
    "mr": "MR",
}
SLIDE_DISPLAY = {"vlcc": "VLCC", "suezmax": "Suezmax", "aframax": "Aframax/LR2", "mr": "MR"}

# ---------------------------------------------------------------------------
# Controls — shared
# ---------------------------------------------------------------------------
filter_col1, filter_col2, filter_col3 = st.columns(3)
selected_vessel = filter_col1.selectbox(
    "Vessel type",
    sorted(predictions_df["vessel_type"].astype(str).unique()),
    format_func=lambda v: SLIDE_DISPLAY.get(v, v.upper()),
)
_horizon_options = sorted(predictions_df["display_horizon_months"].astype(int).unique())
selected_horizon = filter_col2.selectbox(
    "Forecast horizon",
    _horizon_options,
    index=_horizon_options.index(12) if 12 in _horizon_options else len(_horizon_options) - 1,
    format_func=lambda h: f"{h}m",
)
history_years = filter_col3.selectbox(
    "History shown (years)",
    [1, 2, 3, 5, 10, 20],
    index=4,
)
history_cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(years=history_years)

ss_prefix = SLIDE_TO_SS_PREFIX[selected_vessel]

# Shared actuals anchor (same for both models)
hist_1m = predictions_df[
    (predictions_df["vessel_type"].astype(str) == selected_vessel)
    & (predictions_df["display_horizon_months"] == 1)
    & (predictions_df["actual_available"] == True)
].copy()
last_actual_row  = hist_1m[hist_1m["actual_tce"].notna()].iloc[-1] if not hist_1m.empty else None
last_actual_date = pd.Timestamp(last_actual_row["forecast_month"]) if last_actual_row is not None else None
last_actual_tce  = float(last_actual_row["actual_tce"])            if last_actual_row is not None else None
anchor_iso       = last_actual_date.isoformat() if last_actual_date else None

# ---------------------------------------------------------------------------
# Slide model view
# ---------------------------------------------------------------------------
if model_toggle == "Slide models":
    slide_df = predictions_df[
        (predictions_df["vessel_type"].astype(str) == selected_vessel)
        & (predictions_df["display_horizon_months"].astype(int) == selected_horizon)
    ].copy().sort_values("forecast_month")

    slide_hist = slide_df[slide_df["actual_available"] == True].copy()

    target_date   = last_actual_date + pd.DateOffset(months=selected_horizon) if last_actual_date else None
    slide_fwd_all = slide_df[slide_df["actual_available"] == False].copy()
    if target_date is not None and not slide_fwd_all.empty:
        slide_fwd_all["_delta"] = (pd.to_datetime(slide_fwd_all["forecast_month"]) - target_date).abs()
        best = slide_fwd_all.loc[slide_fwd_all["_delta"].idxmin()]
        slide_fwd = best if best["_delta"].days <= 20 else None
    else:
        slide_fwd = None

    residuals = (slide_hist["forecast_tce"] - slide_hist["actual_tce"]).dropna()
    s_p05 = float(residuals.quantile(0.05)) if len(residuals) >= 10 else 0.0
    s_p25 = float(residuals.quantile(0.25)) if len(residuals) >= 10 else 0.0
    s_p75 = float(residuals.quantile(0.75)) if len(residuals) >= 10 else 0.0
    s_p95 = float(residuals.quantile(0.95)) if len(residuals) >= 10 else 0.0

    selected_model_row = selected_models_df[
        (selected_models_df["vessel_type"].astype(str) == selected_vessel)
        & (selected_models_df["forecast_horizon_months"].astype(int) == selected_horizon)
    ]
    if not selected_model_row.empty:
        model_name = str(selected_model_row.iloc[0]["selected_model_name"])
        model_mae  = float(selected_model_row.iloc[0]["mae"])
        st.caption(
            f"Model: `{model_name}` — backtest MAE `{model_mae:,.0f}`"
        )

    slide_hist_vis = slide_hist[pd.to_datetime(slide_hist["forecast_month"]) >= history_cutoff]
    act_records = [
        {"Date": pd.Timestamp(r["forecast_month"]).isoformat(), "TCE": float(r["actual_tce"])}
        for _, r in slide_hist_vis[slide_hist_vis["actual_tce"].notna()].iterrows()
    ]
    hist_fc_records = [
        {"Date": pd.Timestamp(r["forecast_month"]).isoformat(), "TCE": float(r["forecast_tce"])}
        for _, r in slide_hist_vis.iterrows() if pd.notna(r["forecast_tce"])
    ]

    layers = [
        {
            "data": {"values": hist_fc_records},
            "mark": {"type": "line", "color": "#1f77b4", "strokeWidth": 1.2,
                     "opacity": 0.35, "strokeDash": [4, 3]},
            "encoding": {
                "x": {"field": "Date", "type": "temporal", "title": "Date"},
                "y": {"field": "TCE", "type": "quantitative", "title": "TCE ($/day)"},
                "tooltip": [{"field": "Date", "type": "temporal"},
                             {"field": "TCE", "type": "quantitative",
                              "title": "Historical forecast", "format": ",.0f"}],
            },
        },
        {
            "data": {"values": act_records},
            "mark": {"type": "line", "color": "#444444", "strokeWidth": 2},
            "encoding": {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
                "tooltip": [{"field": "Date", "type": "temporal"},
                             {"field": "TCE", "type": "quantitative",
                              "title": "Actual TCE", "format": ",.0f"}],
            },
        },
    ]

    if slide_fwd is not None and anchor_iso and last_actual_tce is not None:
        target_iso = pd.Timestamp(slide_fwd["forecast_month"]).isoformat()
        p50 = float(slide_fwd["forecast_tce"])
        for lo_off, hi_off, opacity in [(s_p05, s_p95, 0.15), (s_p25, s_p75, 0.28)]:
            layers.append({
                "data": {"values": [
                    {"Date": anchor_iso, "lo": last_actual_tce, "hi": last_actual_tce},
                    {"Date": target_iso,
                     "lo": min(p50 - lo_off, p50 - hi_off),
                     "hi": max(p50 - lo_off, p50 - hi_off)},
                ]},
                "mark": {"type": "area", "opacity": opacity, "color": "#1f77b4"},
                "encoding": {
                    "x": {"field": "Date", "type": "temporal"},
                    "y": {"field": "lo", "type": "quantitative"},
                    "y2": {"field": "hi"},
                },
            })
        layers.append({
            "data": {"values": [
                {"Date": anchor_iso, "TCE": last_actual_tce},
                {"Date": target_iso, "TCE": p50},
            ]},
            "mark": {"type": "line", "strokeDash": [4, 3], "strokeWidth": 1.5,
                     "color": "#1f77b4", "opacity": 0.9},
            "encoding": {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
            },
        })
        label = f"{selected_horizon}m: ${p50:,.0f}/day"
        for mark, dy in [("point", None), ("text", -12)]:
            enc = {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
            }
            m = {"type": mark, "color": "#1f77b4"}
            if mark == "point":
                m |= {"size": 90, "filled": True}
                enc["tooltip"] = [{"field": "Date", "type": "temporal"},
                                   {"field": "label", "type": "nominal", "title": "Forecast"}]
            else:
                m |= {"dy": dy, "fontSize": 11, "fontWeight": "bold"}
                enc["text"] = {"field": "label", "type": "nominal"}
            layers.append({"data": {"values": [{"Date": target_iso, "TCE": p50, "label": label}]},
                           "mark": m, "encoding": enc})

    st.vega_lite_chart(
        {"layer": layers, "resolve": {"scale": {"y": "shared"}},
         "width": "container", "height": 420},
            width="stretch",
    )
    anchor_str = last_actual_date.strftime("%Y-%m") if last_actual_date else "—"
    if len(residuals) >= 10:
        st.caption(
            f"Forward forecast is {selected_horizon}m from last actual ({anchor_str}). "
            f"Bands: empirical 50%/90% from {len(residuals)} historical forecast errors."
        )

    with st.expander("Full prediction history"):
        st.dataframe(slide_df.rename(columns={
            "forecast_month": "Forecast Month", "vessel_type": "Vessel Type",
            "forecast_tce": "Forecast TCE", "source_model_name": "Model",
            "source_origin_month": "Origin Month", "display_horizon_months": "Horizon (m)",
            "source_type": "Row Type", "actual_tce": "Actual TCE",
            "actual_available": "Actual Available",
        }), width="stretch", hide_index=True)

    with st.expander("Selected models by vessel and horizon"):
        st.dataframe(selected_models_df, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------
# Schwartz-Smith view
# ---------------------------------------------------------------------------
else:
    med_col = f"{ss_prefix}_predicted_values_{selected_horizon}m"
    p05_col, p25_col = f"{med_col}_p05", f"{med_col}_p25"
    p75_col, p95_col = f"{med_col}_p75", f"{med_col}_p95"

    ss_latest_origin = (
        df_act[df_act["vessel"] == selected_vessel]["Date"].max() + pd.offsets.MonthEnd(0)
    )
    ss_target = ss_latest_origin + pd.DateOffset(months=selected_horizon)
    ss_delta  = (df_ss["Date"] - ss_target).abs()
    ss_idx    = ss_delta.idxmin()
    ss_fwd    = df_ss.loc[ss_idx] if ss_delta[ss_idx].days <= 15 else None

    ss_hist_rows = df_ss[
        df_ss[med_col].notna() &
        (df_ss["Date"] >= history_cutoff) &
        (df_ss["Date"] <= (last_actual_date or "2026-01-01"))
    ].copy() if med_col in df_ss.columns else pd.DataFrame()

    act_records = [
        {"Date": r["Date"].isoformat(), "TCE": float(r["TCE"])}
        for _, r in df_act[
            (df_act["vessel"] == selected_vessel) & (df_act["Date"] >= history_cutoff)
        ].iterrows()
        if pd.notna(r["TCE"])
    ]

    layers = [
        {
            "data": {"values": act_records},
            "mark": {"type": "line", "color": "#444444", "strokeWidth": 2},
            "encoding": {
                "x": {"field": "Date", "type": "temporal", "title": "Date"},
                "y": {"field": "TCE", "type": "quantitative", "title": "TCE ($/day)"},
                "tooltip": [{"field": "Date", "type": "temporal"},
                             {"field": "TCE", "type": "quantitative",
                              "title": "Actual TCE", "format": ",.0f"}],
            },
        },
    ]

    if not ss_hist_rows.empty:
        layers.append({
            "data": {"values": [
                {"Date": r["Date"].isoformat(), "TCE": float(r[med_col])}
                for _, r in ss_hist_rows.iterrows()
            ]},
            "mark": {"type": "point", "color": "#ff7f0e", "size": 25,
                     "opacity": 0.3, "filled": True},
            "encoding": {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
                "tooltip": [{"field": "Date", "type": "temporal"},
                             {"field": "TCE", "type": "quantitative",
                              "title": "SS forecast (historical)", "format": ",.0f"}],
            },
        })

    if ss_fwd is not None and anchor_iso and last_actual_tce is not None:
        ss_target_iso = ss_fwd["Date"].isoformat()
        p50 = float(ss_fwd[med_col])
        p05 = float(ss_fwd[p05_col]) if pd.notna(ss_fwd.get(p05_col)) else None
        p25 = float(ss_fwd[p25_col]) if pd.notna(ss_fwd.get(p25_col)) else None
        p75 = float(ss_fwd[p75_col]) if pd.notna(ss_fwd.get(p75_col)) else None
        p95 = float(ss_fwd[p95_col]) if pd.notna(ss_fwd.get(p95_col)) else None

        for lo_val, hi_val, opacity in [(p05, p95, 0.15), (p25, p75, 0.28)]:
            if lo_val and hi_val:
                layers.append({
                    "data": {"values": [
                        {"Date": anchor_iso, "lo": last_actual_tce, "hi": last_actual_tce},
                        {"Date": ss_target_iso, "lo": lo_val, "hi": hi_val},
                    ]},
                    "mark": {"type": "area", "opacity": opacity, "color": "#ff7f0e"},
                    "encoding": {
                        "x": {"field": "Date", "type": "temporal"},
                        "y": {"field": "lo", "type": "quantitative"},
                        "y2": {"field": "hi"},
                    },
                })
        layers.append({
            "data": {"values": [
                {"Date": anchor_iso, "TCE": last_actual_tce},
                {"Date": ss_target_iso, "TCE": p50},
            ]},
            "mark": {"type": "line", "strokeDash": [4, 3], "strokeWidth": 1.5,
                     "color": "#ff7f0e", "opacity": 0.9},
            "encoding": {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
            },
        })
        label = f"{selected_horizon}m: ${p50:,.0f}/day"
        for mark, dy in [("point", None), ("text", -12)]:
            enc = {
                "x": {"field": "Date", "type": "temporal"},
                "y": {"field": "TCE", "type": "quantitative"},
            }
            m = {"type": mark, "color": "#ff7f0e"}
            if mark == "point":
                m |= {"size": 90, "filled": True}
                enc["tooltip"] = [{"field": "Date", "type": "temporal"},
                                   {"field": "label", "type": "nominal", "title": "SS forecast"}]
            else:
                m |= {"dy": dy, "fontSize": 11, "fontWeight": "bold"}
                enc["text"] = {"field": "label", "type": "nominal"}
            layers.append({"data": {"values": [{"Date": ss_target_iso, "TCE": p50, "label": label}]},
                           "mark": m, "encoding": enc})

        st.vega_lite_chart(
            {"layer": layers, "resolve": {"scale": {"y": "shared"}},
             "width": "container", "height": 420},
            width="stretch",
        )
        anchor_str = last_actual_date.strftime("%Y-%m") if last_actual_date else "—"
        lo_str = f"${p05:,.0f}" if p05 else "—"
        hi_str = f"${p95:,.0f}" if p95 else "—"
        st.caption(
            f"Origin: {ss_latest_origin.strftime('%Y-%m-%d')} · "
            f"Target: {pd.Timestamp(ss_fwd['Date']).strftime('%Y-%m')} · "
            f"Median: ${p50:,.0f} · 90% interval: {lo_str} – {hi_str}. "
            f"Bands: bootstrap FHS (block 6m, n=500)."
        )
    else:
        st.vega_lite_chart(
            {"layer": layers, "resolve": {"scale": {"y": "shared"}},
             "width": "container", "height": 420},
        width="stretch",
        )
        st.info("No SS forward forecast found for this vessel/horizon combination.")

    with st.expander("About the Schwartz-Smith model"):
        st.markdown(
            "**Schwartz-Smith two-factor model** — state-space model fitted on the freight "
            "term structure (spot + 1yr TC + 2yr TC). Two latent factors: short-term "
            "mean-reverting deviation (χ) and long-run stochastic level (ξ). "
            "Prediction intervals from bootstrap FHS (block size 6 months, n=500 paths). "
            "See `docs/SS_MODEL.md` for full details."
        )
