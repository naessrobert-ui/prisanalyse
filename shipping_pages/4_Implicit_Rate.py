from __future__ import annotations

from datetime import date, timedelta

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from shipping_datahandling._4_Datahandling_Implicit_Rate import (
    MissingFinancialsDataError,
    build_latest_implicit_rate_snapshot,
    load_fleet_ownership_periods,
    load_or_update_implicit_rate_output,
    load_or_update_vessel_value_defense_output,
    load_ship_values,
    load_stock_data,
    run_implicit_rate_sensitivity_grid,
)
from shipping_app.data import get_company_ticker
from shipping_app.ui import render_sidebar


NEWBUILD_LIFE = 20
WACC = 0.1
UTILIZATION = 0.98
DEFAULT_LOOKBACK_DAYS = 365
VESSEL_ORDER = ["VLCC", "Suezmax", "Aframax/LR2", "MR"]
VESSEL_COLORS = {
    "VLCC": "#0f766e",
    "Suezmax": "#1d4ed8",
    "Aframax/LR2": "#b45309",
    "MR": "#7c3aed",
}
SENSITIVITY_ASSUMPTIONS = {
    "wacc": {"label": "WACC", "base_column": "WACC"},
    "capex": {"label": "Capex", "base_column": "CapexAssumption"},
    "opex": {"label": "Opex", "base_column": "OpexAssumption"},
    "utilization": {"label": "Utilization", "base_column": "Utilization"},
    "ev_gav": {"label": "EV/GAV", "base_column": "EV_GAV"},
    "newbuild_price": {"label": "Newbuild Price", "base_column": "Price_0Y"},
}


st.set_page_config(
    page_title="Implicit Rate",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _ordered_vessel_types(values: list[str]) -> list[str]:
    present = [vessel for vessel in VESSEL_ORDER if vessel in values]
    extras = sorted([vessel for vessel in values if vessel not in VESSEL_ORDER])
    return present + extras


def _current_vessel_types_by_count(ticker: str, as_of_date: pd.Timestamp) -> list[str]:
    fleet_periods = load_fleet_ownership_periods(ticker).reset_index()
    fleet_periods["StartDate"] = pd.to_datetime(fleet_periods["StartDate"]).dt.normalize()
    fleet_periods["EndDate"] = pd.to_datetime(fleet_periods["EndDate"]).dt.normalize()

    active_fleet = fleet_periods.loc[
        fleet_periods["VesselType"].notna()
        & (fleet_periods["StartDate"] <= as_of_date)
        & (fleet_periods["EndDate"].isna() | (fleet_periods["EndDate"] >= as_of_date))
    ].copy()

    if active_fleet.empty:
        return []

    vessel_counts = (
        active_fleet.groupby("VesselType", as_index=False)
        .agg(ShipCount=("ID", "nunique"))
        .sort_values(["ShipCount", "VesselType"], ascending=[False, True])
    )
    return vessel_counts["VesselType"].tolist()


def _latest_and_previous(series_df: pd.DataFrame, value_col: str) -> tuple[pd.Series | None, pd.Series | None]:
    clean = series_df.dropna(subset=[value_col]).sort_values("Date")
    if clean.empty:
        return None, None

    latest = clean.iloc[-1]
    previous = clean.iloc[-2] if len(clean) > 1 else None
    return latest, previous


def _percent_delta(current: float | None, previous: float | None) -> str | None:
    if current is None or previous is None or pd.isna(current) or pd.isna(previous) or previous == 0:
        return None
    delta_pct = (current / previous - 1) * 100
    return f"{delta_pct:+.2f}%"


def _metric_delta_text(current: float | None, previous: float | None, previous_date) -> str | None:
    pct_text = _percent_delta(current, previous)
    if pct_text is None or previous_date is None:
        return None
    previous_date = pd.Timestamp(previous_date)
    return f"{pct_text} since {previous_date:%Y-%m-%d}"


def _format_dayrate(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"


def _format_money(value: float | None, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.0f}{suffix}"


def _format_ratio(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.2f}x"


def _format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.1%}"


def _format_assumption_value(assumption_key: str, value: float) -> str:
    if assumption_key in {"wacc", "utilization"}:
        return f"{value:.1%}"
    if assumption_key == "ev_gav":
        return f"{value:.2f}x"
    return f"${value:,.0f}"


def _parse_money_input(value: str) -> float | None:
    cleaned = value.replace("$", "").replace(",", "").replace(" ", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_ratio_assumption(assumption_key: str) -> bool:
    return assumption_key in {"wacc", "utilization", "ev_gav"}


def _assumption_input_step(assumption_key: str) -> float:
    if _is_ratio_assumption(assumption_key):
        return 0.01
    if assumption_key == "newbuild_price":
        return 1000000.0
    return 500.0


def _assumption_input_format(assumption_key: str) -> str:
    return "%.4f" if _is_ratio_assumption(assumption_key) else "%.0f"


def _assumption_range_input(
    label: str,
    *,
    value: float,
    assumption_key: str,
    key: str,
) -> float:
    if _is_ratio_assumption(assumption_key):
        return float(
            st.number_input(
                label,
                value=float(value),
                step=_assumption_input_step(assumption_key),
                format=_assumption_input_format(assumption_key),
                key=key,
            )
        )

    formatted_value = st.text_input(
        label,
        value=f"{value:,.0f}",
        key=key,
    )
    parsed_value = _parse_money_input(formatted_value)
    if parsed_value is None:
        st.warning(f"Enter a valid number for {label}.")
        return float(value)
    return float(parsed_value)


def _build_rate_chart(chart_df: pd.DataFrame) -> alt.Chart:
    color_domain = [vessel for vessel in VESSEL_ORDER if vessel in chart_df["VesselType"].unique()]
    color_range = [VESSEL_COLORS[vessel] for vessel in color_domain]
    return (
        alt.Chart(chart_df)
        .mark_line(strokeWidth=2.6)
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("ImplicitDayrate:Q", title="Implicit Rate"),
            color=alt.Color(
                "VesselType:N",
                scale=alt.Scale(domain=color_domain, range=color_range),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("Date:T", title="Date"),
                alt.Tooltip("VesselType:N", title="Vessel type"),
                alt.Tooltip("ImplicitDayrate:Q", title="Implicit rate", format=",.0f"),
            ],
        )
        .properties(height=380)
    )


def _build_ev_gav_chart(chart_df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(chart_df)
        .mark_line(color="#111827", strokeWidth=2.4)
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("EV_GAV:Q", title="EV/GAV"),
            tooltip=[
                alt.Tooltip("Date:T", title="Date"),
                alt.Tooltip("EV_GAV:Q", title="EV/GAV", format=".2f"),
            ],
        )
        .properties(height=220)
    )


def _default_range_for_assumption(assumption_key: str, base_value: float) -> tuple[float, float, int]:
    if assumption_key == "wacc":
        return max(0.01, base_value - 0.02), base_value, 3
    if assumption_key == "capex":
        return max(0.0, base_value - 2000), base_value + 2000, 5
    if assumption_key == "opex":
        return max(0.0, base_value - 2000), base_value + 2000, 5
    if assumption_key == "utilization":
        return max(0.7, base_value - 0.04), min(1.0, base_value + 0.02), 5
    if assumption_key == "ev_gav":
        return max(0.1, base_value - 0.2), base_value + 0.2, 5
    if assumption_key == "newbuild_price":
        return max(0.0, base_value * 0.8), base_value * 1.2, 5
    raise ValueError(f"Unsupported assumption '{assumption_key}'.")


def _build_assumption_grid(min_value: float, max_value: float, count: int, assumption_key: str) -> list[float]:
    if count <= 1 or np.isclose(min_value, max_value):
        values = np.array([min_value], dtype=float)
    else:
        values = np.linspace(min_value, max_value, int(count))

    if assumption_key in {"wacc", "utilization"}:
        rounded = np.round(values, 4)
    elif assumption_key == "ev_gav":
        rounded = np.round(values, 3)
    else:
        rounded = np.round(values, 0)

    deduped: list[float] = []
    for value in rounded.tolist():
        if not deduped or not np.isclose(deduped[-1], value):
            deduped.append(float(value))
    return deduped


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = color.lstrip("#")
    return tuple(int(color[i : i + 2], 16) for i in (0, 2, 4))


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def _blend_colors(start: str, end: str, weight: float) -> str:
    if pd.isna(weight):
        return "#ffffff"
    weight = min(max(float(weight), 0.0), 1.0)
    start_rgb = _hex_to_rgb(start)
    end_rgb = _hex_to_rgb(end)
    blended = tuple(
        int(round(start_channel + (end_channel - start_channel) * weight))
        for start_channel, end_channel in zip(start_rgb, end_rgb)
    )
    return _rgb_to_hex(blended)


def _delta_to_background(delta: float, min_delta: float, max_delta: float) -> str:
    if pd.isna(delta) or pd.isna(min_delta) or pd.isna(max_delta):
        return "#f8fafc"
    if np.isclose(min_delta, max_delta):
        return "#ffffff"

    if delta >= 0:
        scale = 0 if np.isclose(max_delta, 0) else min(delta / max_delta, 1.0)
        return _blend_colors("#fff7ed", "#fecaca", scale)

    scale = 0 if np.isclose(min_delta, 0) else min(abs(delta / min_delta), 1.0)
    return _blend_colors("#f0fdf4", "#bbf7d0", scale)


def _render_sensitivity_table(
    scenario_df: pd.DataFrame,
    *,
    row_values: list[float],
    column_values: list[float],
    row_assumption: str,
    column_assumption: str,
) -> None:
    if scenario_df.empty:
        st.info("No sensitivity scenarios available for the selected inputs.")
        return

    valid_deltas = scenario_df["DeltaDayrate"].dropna()
    delta_min = float(valid_deltas.min()) if not valid_deltas.empty else 0.0
    delta_max = float(valid_deltas.max()) if not valid_deltas.empty else 0.0
    row_labels = {
        float(row_value): _format_assumption_value(row_assumption, row_value)
        for row_value in row_values
    }
    column_labels = {
        float(column_value): _format_assumption_value(column_assumption, column_value)
        for column_value in column_values
    }

    display_df = pd.DataFrame(
        index=[row_labels[float(row_value)] for row_value in row_values],
        columns=[column_labels[float(column_value)] for column_value in column_values],
        dtype=object,
    )
    color_df = pd.DataFrame(
        "#f8fafc",
        index=display_df.index,
        columns=display_df.columns,
        dtype=object,
    )

    for row_value in row_values:
        for column_value in column_values:
            scenario_row = scenario_df.loc[
                np.isclose(scenario_df["RowValue"], row_value)
                & np.isclose(scenario_df["ColumnValue"], column_value)
            ]

            row_label = row_labels[float(row_value)]
            column_label = column_labels[float(column_value)]

            if scenario_row.empty:
                display_df.loc[row_label, column_label] = "N/A"
                continue

            scenario = scenario_row.iloc[0]
            value = float(scenario["ImplicitDayrate"])
            delta = float(scenario["DeltaDayrate"])

            if pd.isna(value) or pd.isna(delta):
                display_df.loc[row_label, column_label] = "N/A"
                continue

            display_df.loc[row_label, column_label] = (
                f"{_format_dayrate(value)} ({delta:+,.0f}/day)"
            )
            color_df.loc[row_label, column_label] = _delta_to_background(delta, delta_min, delta_max)

    def _apply_cell_background(_: pd.DataFrame) -> pd.DataFrame:
        return color_df.apply(
            lambda column: column.map(
                lambda color: f"background-color: {color}; text-align: center;"
            )
        )

    styler = (
        display_df.style
        .apply(_apply_cell_background, axis=None)
        .set_properties(**{"white-space": "nowrap"})
        .set_table_styles(
            [
                {"selector": "th.col_heading", "props": [("text-align", "center")]},
                {"selector": "th.row_heading", "props": [("text-align", "left")]},
            ]
        )
    )
    st.dataframe(styler, width='stretch')


def _render_ev_gav_sensitivity_table(
    scenario_df: pd.DataFrame,
    *,
    row_values: list[float],
    column_values: list[float],
    row_assumption: str,
    column_assumption: str,
) -> None:
    if scenario_df.empty:
        return

    valid_deltas = scenario_df["DeltaEV_GAV"].dropna()
    delta_min = float(valid_deltas.min()) if not valid_deltas.empty else 0.0
    delta_max = float(valid_deltas.max()) if not valid_deltas.empty else 0.0
    row_labels = {
        float(row_value): _format_assumption_value(row_assumption, row_value)
        for row_value in row_values
    }
    column_labels = {
        float(column_value): _format_assumption_value(column_assumption, column_value)
        for column_value in column_values
    }

    display_df = pd.DataFrame(
        index=[row_labels[float(row_value)] for row_value in row_values],
        columns=[column_labels[float(column_value)] for column_value in column_values],
        dtype=object,
    )
    color_df = pd.DataFrame(
        "#f8fafc",
        index=display_df.index,
        columns=display_df.columns,
        dtype=object,
    )

    for row_value in row_values:
        for column_value in column_values:
            scenario_row = scenario_df.loc[
                np.isclose(scenario_df["RowValue"], row_value)
                & np.isclose(scenario_df["ColumnValue"], column_value)
            ]

            row_label = row_labels[float(row_value)]
            column_label = column_labels[float(column_value)]

            if scenario_row.empty:
                display_df.loc[row_label, column_label] = "N/A"
                continue

            scenario = scenario_row.iloc[0]
            value = float(scenario["EV_GAV"])
            delta = float(scenario["DeltaEV_GAV"])

            if pd.isna(value) or pd.isna(delta):
                display_df.loc[row_label, column_label] = "N/A"
                continue

            display_df.loc[row_label, column_label] = (
                f"{_format_ratio(value)} ({delta:+.2f}x)"
            )
            color_df.loc[row_label, column_label] = _delta_to_background(delta, delta_min, delta_max)

    def _apply_cell_background(_: pd.DataFrame) -> pd.DataFrame:
        return color_df.apply(
            lambda column: column.map(
                lambda color: f"background-color: {color}; text-align: center;"
            )
        )

    styler = (
        display_df.style
        .apply(_apply_cell_background, axis=None)
        .set_properties(**{"white-space": "nowrap"})
        .set_table_styles(
            [
                {"selector": "th.col_heading", "props": [("text-align", "center")]},
                {"selector": "th.row_heading", "props": [("text-align", "left")]},
            ]
        )
    )
    st.dataframe(styler, width='stretch')


def render_overview_tab(
    *,
    company: str,
    ticker: str,
    dashboard_df: pd.DataFrame,
    defense_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    ship_values_df: pd.DataFrame,
    available_vessels: list[str],
    latest_date: pd.Timestamp,
    latest_snapshot: pd.DataFrame,
    current_company_vessels: list[str],
) -> None:
    latest_defense_date = defense_df["Date"].max() if not defense_df.empty else None
    latest_defense_snapshot = (
        defense_df.loc[defense_df["Date"] == latest_defense_date]
        .sort_values("VesselType")
        .reset_index(drop=True)
        if latest_defense_date is not None
        else pd.DataFrame()
    )
    latest_stock_date = stock_df.index.max() if not stock_df.empty else None
    latest_ship_value_date = ship_values_df.index.max() if not ship_values_df.empty else None

    rate_vessels = [vessel for vessel in current_company_vessels if vessel in available_vessels]
    rate_columns = st.columns(max(len(rate_vessels), 1))

    for column, vessel in zip(rate_columns, rate_vessels):
        vessel_history = dashboard_df.loc[dashboard_df["VesselType"] == vessel, ["Date", "ImplicitDayrate"]]
        latest_row, previous_row = _latest_and_previous(vessel_history, "ImplicitDayrate")
        current_value = float(latest_row["ImplicitDayrate"]) if latest_row is not None else None
        previous_value = float(previous_row["ImplicitDayrate"]) if previous_row is not None else None

        with column:
            st.metric(
                label=f"{vessel} Rate",
                value=_format_dayrate(current_value),
                delta=_metric_delta_text(
                    current_value,
                    previous_value,
                    previous_row["Date"] if previous_row is not None else None,
                ),
            )

    if not latest_defense_snapshot.empty and current_company_vessels:
        st.markdown("##### Defense Rates")

        defense_vessels = [vessel for vessel in current_company_vessels if vessel in latest_defense_snapshot["VesselType"].tolist()]
        defense_columns = st.columns(max(len(defense_vessels), 1))

        for column, vessel in zip(defense_columns, defense_vessels):
            vessel_row = latest_defense_snapshot.loc[latest_defense_snapshot["VesselType"] == vessel].iloc[0]
            defense_value = float(vessel_row["DefenseDayrate"])
            with column:
                st.markdown(
                    f"""
                    <div style="border:1px solid #e5e7eb; border-radius:14px; padding:0.8rem 0.9rem; background:#f8fafc; min-height:104px;">
                        <div style="font-size:0.8rem; font-weight:400; letter-spacing:0.02em; color:#475569; text-transform:uppercase; margin-bottom:0.35rem;">{vessel}</div>
                        <div style="font-size:1.35rem; font-weight:400; color:#0f172a; margin-bottom:0.25rem;">{_format_dayrate(defense_value)}</div>
                        <div style="font-size:0.8rem; font-weight:400; color:#64748b;">Rate needed to defend ship values.</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    if latest_stock_date is not None and latest_ship_value_date is not None and latest_ship_value_date < latest_stock_date:
        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
        st.info(
            f"Ship values currently run through {latest_ship_value_date:%Y-%m-%d}, while stock prices run through "
            f"{latest_stock_date:%Y-%m-%d}. Later dates are using the latest available ship-value curve."
        )

    st.markdown(
        """
        <div style="margin: 1rem 0 1.35rem 0; border-top: 1px solid #e5e7eb;"></div>
        """,
        unsafe_allow_html=True,
    )

    ev_gav_series = dashboard_df[["Date", "EV_GAV"]].drop_duplicates().sort_values("Date")
    ev_gav_latest, ev_gav_previous = _latest_and_previous(ev_gav_series, "EV_GAV")
    current_ev_gav = float(ev_gav_latest["EV_GAV"]) if ev_gav_latest is not None else None
    previous_ev_gav = float(ev_gav_previous["EV_GAV"]) if ev_gav_previous is not None else None

    ev_col, summary_col = st.columns((1, 2))

    with ev_col:
        st.metric(
            "Current EV/GAV",
            _format_ratio(current_ev_gav),
            delta=_metric_delta_text(
                current_ev_gav,
                previous_ev_gav,
                ev_gav_previous["Date"] if ev_gav_previous is not None else None,
            ),
        )

    with summary_col:
        st.markdown(
            f"""
            **Interpretation**

            The cards above show the freight rates the market is currently implying for {company}'s main vessel classes.
            """
        )

    st.markdown(
        """
        <div style="margin: 1rem 0 1.35rem 0; border-top: 1px solid #e5e7eb;"></div>
        """,
        unsafe_allow_html=True,
    )

    st.subheader("Model Assumptions")
    global_cols = st.columns(3)
    global_cols[0].metric("WACC", f"{WACC:.0%}")
    global_cols[1].metric("Utilization", f"{UTILIZATION:.0%}")
    global_cols[2].metric("Newbuild Life", f"{NEWBUILD_LIFE} years")

    st.caption("Global inputs at the top, followed by vessel-specific operating assumptions from the latest available date.")

    assumption_vessels = _ordered_vessel_types(latest_snapshot["VesselType"].tolist())
    assumption_cols = st.columns(max(len(assumption_vessels), 1))
    for column, vessel in zip(assumption_cols, assumption_vessels):
        vessel_row = latest_snapshot.loc[latest_snapshot["VesselType"] == vessel].iloc[0]
        with column:
            st.markdown(
                f"""
                <div style="border:1px solid #e5e7eb; border-radius:18px; padding:1rem 1rem 0.85rem 1rem; background:#ffffff; min-height:190px;">
                    <div style="font-size:1rem; font-weight:700; color:#111827; margin-bottom:0.8rem;">{vessel}</div>
                    <div style="font-size:0.78rem; color:#6b7280;">Opex <span style="font-size:0.72rem;">(USD/day)</span></div>
                    <div style="font-size:1.15rem; font-weight:600; color:#111827; margin-bottom:0.55rem;">{_format_money(vessel_row["OpexAssumption"], "/day")}</div>
                    <div style="font-size:0.78rem; color:#6b7280;">G&amp;A <span style="font-size:0.72rem;">(USD/day)</span></div>
                    <div style="font-size:1.15rem; font-weight:600; color:#111827; margin-bottom:0.55rem;">{_format_money(vessel_row["GnaAssumption"], "/day")}</div>
                    <div style="font-size:0.78rem; color:#6b7280;">Capex <span style="font-size:0.72rem;">(USD/year)</span></div>
                    <div style="font-size:1.15rem; font-weight:600; color:#111827;">{_format_money(vessel_row["CapexAssumption"], "/year")}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()
    st.subheader("Implicit Rate History")

    default_start = max(dashboard_df["Date"].min().date(), (latest_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)).date())
    quick_ranges = {
        "3M": latest_date.date() - timedelta(days=90),
        "6M": latest_date.date() - timedelta(days=180),
        "1Y": latest_date.date() - timedelta(days=365),
        "3Y": latest_date.date() - timedelta(days=365 * 3),
        "Max": dashboard_df["Date"].min().date(),
    }

    control_col1, control_col2, control_col3 = st.columns((1.2, 1.15, 0.9))

    with control_col1:
        selected_vessels = st.multiselect(
            "Visible rate series",
            options=available_vessels,
            default=rate_vessels if rate_vessels else available_vessels[:3],
        )

    with control_col2:
        selected_range = st.radio(
            "Quick range",
            options=list(quick_ranges.keys()),
            index=2,
            horizontal=True,
        )

    with control_col3:
        date_range = st.date_input(
            "Date range",
            value=(
                max(dashboard_df["Date"].min().date(), quick_ranges[selected_range]),
                latest_date.date(),
            ),
            min_value=dashboard_df["Date"].min().date(),
            max_value=latest_date.date(),
        )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        chart_start, chart_end = date_range
    else:
        chart_start, chart_end = default_start, latest_date.date()

    chart_df = dashboard_df.loc[
        dashboard_df["VesselType"].isin(selected_vessels)
        & (dashboard_df["Date"] >= pd.Timestamp(chart_start))
        & (dashboard_df["Date"] <= pd.Timestamp(chart_end)),
        ["Date", "VesselType", "ImplicitDayrate"],
    ].copy()

    if chart_df.empty:
        st.info("No data available for the selected filters.")
    else:
        st.altair_chart(_build_rate_chart(chart_df), width='stretch')

    ev_chart_df = ev_gav_series.loc[
        (ev_gav_series["Date"] >= pd.Timestamp(chart_start))
        & (ev_gav_series["Date"] <= pd.Timestamp(chart_end))
    ].copy()

    if not ev_chart_df.empty:
        st.subheader("EV/GAV History")
        st.altair_chart(_build_ev_gav_chart(ev_chart_df), width='stretch')

    download_df = dashboard_df.loc[
        dashboard_df["VesselType"].isin(selected_vessels)
        & (dashboard_df["Date"] >= pd.Timestamp(chart_start))
        & (dashboard_df["Date"] <= pd.Timestamp(chart_end))
    ].sort_values(["Date", "VesselType"])
    st.download_button(
        "Download filtered history",
        data=download_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{ticker}_implicit_rate_history.csv",
        mime="text/csv",
    )

    with st.expander("Methodology"):
        st.markdown(
            """
            The model works backward from market valuation to implied freight rates.

            - `EV`: built from share price, debt, and cash
            - `Fleet asset value`: built from vessel counts, ages, ship-value curves, and G&A
            - `EV/GAV`: links market valuation to modeled fleet value
            - `Implicit dayrate`: the revenue level needed to support that valuation
            - `Defense dayrate`: the generic vessel-level rate needed to defend current ship values
            """
        )

    with st.expander("Show raw implicit-rate data"):
        st.dataframe(
            dashboard_df.sort_values(["Date", "VesselType"]),
            width='stretch',
            hide_index=True,
        )

    with st.expander("Show raw vessel defense-rate data"):
        st.dataframe(
            defense_df.sort_values(["Date", "VesselType"]),
            width='stretch',
            hide_index=True,
        )


def render_sensitivity_tab(
    *,
    sensitivity_snapshot_df: pd.DataFrame,
    current_company_vessels: list[str],
) -> None:
    valid_snapshot_df = sensitivity_snapshot_df.dropna(
        subset=[
            "ImplicitDayrate",
            "WACC",
            "EV_GAV",
            "CapexAssumption",
            "OpexAssumption",
            "GnaAssumption",
            "Utilization",
            "Price_0Y",
            "NewbuildLife",
            "RemainingLife",
            "Count",
            "SubFleetValue",
            "TotalGnaPval",
            "SubFleetAssetValue",
        ]
    ).copy()
    if valid_snapshot_df.empty:
        st.info("Sensitivity analysis is unavailable because the latest snapshot is missing one or more required model inputs.")
        return

    sensitivity_vessel_options = [
        vessel
        for vessel in current_company_vessels
        if vessel in valid_snapshot_df["VesselType"].dropna().tolist()
    ]
    if not sensitivity_vessel_options:
        sensitivity_vessel_options = _ordered_vessel_types(
            valid_snapshot_df["VesselType"].dropna().drop_duplicates().tolist()
        )

    st.subheader("Sensitivity Analysis")
    st.caption("Latest snapshot only. The table shows how the implied dayrate moves when two assumptions are changed while all other inputs are held constant.")

    control_col1, control_col2, control_col3 = st.columns((1.1, 1, 1))

    with control_col1:
        selected_vessel = st.selectbox(
            "Vessel type",
            options=sensitivity_vessel_options,
            index=0,
        )

    base_row = valid_snapshot_df.loc[
        valid_snapshot_df["VesselType"] == selected_vessel
    ].iloc[0]

    with control_col2:
        row_assumption = st.selectbox(
            "Row axis",
            options=list(SENSITIVITY_ASSUMPTIONS.keys()),
            index=0,
            format_func=lambda key: SENSITIVITY_ASSUMPTIONS[key]["label"],
            key=f"{selected_vessel}_row_assumption",
        )

    column_options = list(SENSITIVITY_ASSUMPTIONS.keys())
    default_column_index = column_options.index("capex") if "capex" in column_options else 0
    with control_col3:
        column_assumption = st.selectbox(
            "Column axis",
            options=column_options,
            index=default_column_index,
            format_func=lambda key: SENSITIVITY_ASSUMPTIONS[key]["label"],
            key=f"{selected_vessel}_column_assumption",
        )

    if column_assumption == row_assumption:
        st.info("Choose two different assumptions for the row and column axes.")
        return

    base_dayrate = float(base_row["ImplicitDayrate"])
    base_ev_gav = float(base_row["EV_GAV"])
    base_wacc = float(base_row["WACC"])
    base_utilization = float(base_row["Utilization"])
    base_capex = float(base_row["CapexAssumption"])
    base_opex = float(base_row["OpexAssumption"])
    base_newbuild_price = float(base_row["Price_0Y"])

    summary_items = [
        ("Base Dayrate", _format_dayrate(base_dayrate)),
        ("EV/GAV", _format_ratio(base_ev_gav)),
        ("WACC", _format_percent(base_wacc)),
        ("Newbuild Price", _format_money(base_newbuild_price)),
        ("Capex", _format_money(base_capex, "/year")),
        ("Opex", _format_money(base_opex, "/day")),
        ("Utilization", _format_percent(base_utilization)),
    ]
    summary_cards = "\n".join(
        f"""
        <div style="border:1px solid #e5e7eb; border-radius:8px; padding:0.75rem 0.85rem; background:#ffffff; min-height:96px;">
            <div style="font-size:0.82rem; color:#475569; margin-bottom:0.35rem; overflow-wrap:anywhere;">{label}</div>
            <div style="font-size:clamp(0.9rem, 1.2vw, 1.15rem); font-weight:650; color:#0f172a; line-height:1.25; overflow-wrap:anywhere;">{value}</div>
        </div>
        """
        for label, value in summary_items
    )
    st.markdown(
        f"""
        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(155px, 1fr)); gap:0.75rem; margin-bottom:0.85rem;">
            {summary_cards}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="margin:0.35rem 0 1rem 0; color:#475569; font-size:0.9rem;">
            Snapshot date: <strong>{pd.Timestamp(base_row['Date']):%Y-%m-%d}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )

    row_base_value = float(base_row[SENSITIVITY_ASSUMPTIONS[row_assumption]["base_column"]])
    column_base_value = float(base_row[SENSITIVITY_ASSUMPTIONS[column_assumption]["base_column"]])
    row_default_min, row_default_max, row_default_count = _default_range_for_assumption(row_assumption, row_base_value)
    column_default_min, column_default_max, column_default_count = _default_range_for_assumption(column_assumption, column_base_value)

    row_settings_col, column_settings_col = st.columns(2)

    with row_settings_col:
        st.markdown(f"**{SENSITIVITY_ASSUMPTIONS[row_assumption]['label']} range**")
        row_min = _assumption_range_input(
            f"{SENSITIVITY_ASSUMPTIONS[row_assumption]['label']} min",
            value=float(row_default_min),
            assumption_key=row_assumption,
            key=f"{selected_vessel}_{row_assumption}_min",
        )
        row_max = _assumption_range_input(
            f"{SENSITIVITY_ASSUMPTIONS[row_assumption]['label']} max",
            value=float(row_default_max),
            assumption_key=row_assumption,
            key=f"{selected_vessel}_{row_assumption}_max",
        )
        row_count = st.number_input(
            f"{SENSITIVITY_ASSUMPTIONS[row_assumption]['label']} steps",
            min_value=2,
            max_value=9,
            value=int(row_default_count),
            step=1,
            key=f"{selected_vessel}_{row_assumption}_count",
        )

    with column_settings_col:
        st.markdown(f"**{SENSITIVITY_ASSUMPTIONS[column_assumption]['label']} range**")
        column_min = _assumption_range_input(
            f"{SENSITIVITY_ASSUMPTIONS[column_assumption]['label']} min",
            value=float(column_default_min),
            assumption_key=column_assumption,
            key=f"{selected_vessel}_{column_assumption}_min",
        )
        column_max = _assumption_range_input(
            f"{SENSITIVITY_ASSUMPTIONS[column_assumption]['label']} max",
            value=float(column_default_max),
            assumption_key=column_assumption,
            key=f"{selected_vessel}_{column_assumption}_max",
        )
        column_count = st.number_input(
            f"{SENSITIVITY_ASSUMPTIONS[column_assumption]['label']} steps",
            min_value=2,
            max_value=9,
            value=int(column_default_count),
            step=1,
            key=f"{selected_vessel}_{column_assumption}_count",
        )

    row_values = _build_assumption_grid(min(row_min, row_max), max(row_min, row_max), int(row_count), row_assumption)
    column_values = _build_assumption_grid(
        min(column_min, column_max),
        max(column_min, column_max),
        int(column_count),
        column_assumption,
    )

    scenario_df = run_implicit_rate_sensitivity_grid(
        valid_snapshot_df,
        selected_vessel,
        row_assumption=row_assumption,
        row_values=row_values,
        column_assumption=column_assumption,
        column_values=column_values,
    )

    st.markdown(
        f"""
        **Base case for {selected_vessel}**

        {SENSITIVITY_ASSUMPTIONS[row_assumption]['label']}: `{_format_assumption_value(row_assumption, row_base_value)}` |
        {SENSITIVITY_ASSUMPTIONS[column_assumption]['label']}: `{_format_assumption_value(column_assumption, column_base_value)}`
        """
    )

    _render_sensitivity_table(
        scenario_df,
        row_values=row_values,
        column_values=column_values,
        row_assumption=row_assumption,
        column_assumption=column_assumption,
    )

    st.markdown("**EV/GAV Sensitivity**")
    st.caption("EV/GAV is recalculated from the full company fleet value unless EV/GAV is selected as a manual axis override.")
    _render_ev_gav_sensitivity_table(
        scenario_df,
        row_values=row_values,
        column_values=column_values,
        row_assumption=row_assumption,
        column_assumption=column_assumption,
    )

    with st.expander("Show sensitivity data"):
        display_df = scenario_df.copy()
        display_df["RowValue"] = display_df["RowValue"].map(lambda value: _format_assumption_value(row_assumption, value))
        display_df["ColumnValue"] = display_df["ColumnValue"].map(lambda value: _format_assumption_value(column_assumption, value))
        display_df["ImplicitDayrate"] = display_df["ImplicitDayrate"].map(_format_dayrate)
        display_df["DeltaDayrate"] = display_df["DeltaDayrate"].map(lambda value: f"{value:+,.0f}/day")
        display_df["EV_GAV"] = display_df["EV_GAV"].map(_format_ratio)
        display_df["DeltaEV_GAV"] = display_df["DeltaEV_GAV"].map(lambda value: f"{value:+.2f}x")
        st.dataframe(display_df, width='stretch', hide_index=True)


company = render_sidebar()
ticker = get_company_ticker(company)

try:
    dashboard_df = load_or_update_implicit_rate_output(
        ticker,
        start_date=date(2015, 1, 2),
        newbuild_life=NEWBUILD_LIFE,
        wacc=WACC,
        utilization=UTILIZATION,
    )
    defense_df = load_or_update_vessel_value_defense_output(
        end_date=None,
        newbuild_life=NEWBUILD_LIFE,
        wacc=WACC,
        utilization=UTILIZATION,
    )
    sensitivity_snapshot_df = build_latest_implicit_rate_snapshot(
        ticker,
        newbuild_life=NEWBUILD_LIFE,
        wacc=WACC,
        utilization=UTILIZATION,
    )
except MissingFinancialsDataError as exc:
    st.error(
        "Financials data is not ready for this company yet. "
        f"{exc} Open `Data/{ticker}_Financials.xlsx` and add at least one reporting period."
    )
    st.stop()
except FileNotFoundError as exc:
    st.error(f"Required data file is missing for {company}: {exc}")
    st.stop()
except Exception as exc:
    st.error(f"Could not load implicit-rate data for {company}: {exc}")
    st.stop()

dashboard_df["Date"] = pd.to_datetime(dashboard_df["Date"]).dt.normalize()
defense_df["Date"] = pd.to_datetime(defense_df["Date"]).dt.normalize()
sensitivity_snapshot_df["Date"] = pd.to_datetime(sensitivity_snapshot_df["Date"]).dt.normalize()
available_vessels = _ordered_vessel_types(sorted(dashboard_df["VesselType"].dropna().unique()))
stock_df = load_stock_data(ticker)
ship_values_df = load_ship_values()

latest_date = dashboard_df["Date"].max()
latest_snapshot = (
    dashboard_df.loc[dashboard_df["Date"] == latest_date]
    .sort_values("VesselType")
    .reset_index(drop=True)
)
current_company_vessels = [
    vessel
    for vessel in _current_vessel_types_by_count(ticker, latest_date)
    if vessel in latest_snapshot["VesselType"].dropna().tolist()
]
if not current_company_vessels:
    current_company_vessels = latest_snapshot["VesselType"].dropna().drop_duplicates().tolist()

st.title(f"{company} Implicit Rate")
st.caption(f"As of {latest_date:%Y-%m-%d}")

overview_tab, sensitivity_tab = st.tabs(["Overview", "Sensitivity Analysis"])

with overview_tab:
    render_overview_tab(
        company=company,
        ticker=ticker,
        dashboard_df=dashboard_df,
        defense_df=defense_df,
        stock_df=stock_df,
        ship_values_df=ship_values_df,
        available_vessels=available_vessels,
        latest_date=latest_date,
        latest_snapshot=latest_snapshot,
        current_company_vessels=current_company_vessels,
    )

with sensitivity_tab:
    render_sensitivity_tab(
        sensitivity_snapshot_df=sensitivity_snapshot_df,
        current_company_vessels=current_company_vessels,
    )

st.stop()


company = render_sidebar()
ticker = get_company_ticker(company)

dashboard_df = load_or_update_implicit_rate_output(
    ticker,
    start_date=date(2015, 1, 2),
    newbuild_life=NEWBUILD_LIFE,
    wacc=WACC,
    utilization=UTILIZATION,
)
defense_df = load_or_update_vessel_value_defense_output(
    end_date=None,
    newbuild_life=NEWBUILD_LIFE,
    wacc=WACC,
    utilization=UTILIZATION,
)

dashboard_df["Date"] = pd.to_datetime(dashboard_df["Date"]).dt.normalize()
defense_df["Date"] = pd.to_datetime(defense_df["Date"]).dt.normalize()
available_vessels = _ordered_vessel_types(sorted(dashboard_df["VesselType"].dropna().unique()))
stock_df = load_stock_data(ticker)
ship_values_df = load_ship_values()

latest_date = dashboard_df["Date"].max()
latest_snapshot = (
    dashboard_df.loc[dashboard_df["Date"] == latest_date]
    .sort_values("VesselType")
    .reset_index(drop=True)
)
current_company_vessels = [
    vessel
    for vessel in _current_vessel_types_by_count(ticker, latest_date)
    if vessel in latest_snapshot["VesselType"].dropna().tolist()
]
if not current_company_vessels:
    current_company_vessels = latest_snapshot["VesselType"].dropna().drop_duplicates().tolist()
latest_defense_date = defense_df["Date"].max() if not defense_df.empty else None
latest_defense_snapshot = (
    defense_df.loc[defense_df["Date"] == latest_defense_date]
    .sort_values("VesselType")
    .reset_index(drop=True)
    if latest_defense_date is not None
    else pd.DataFrame()
)
latest_stock_date = stock_df.index.max() if not stock_df.empty else None
latest_ship_value_date = ship_values_df.index.max() if not ship_values_df.empty else None

st.title(f"{company} Implicit Rate")
#st.markdown(f"Freight rates implied by {company}’s current market valuation.")
st.caption(f"As of {latest_date:%Y-%m-%d}")

rate_vessels = [vessel for vessel in current_company_vessels if vessel in available_vessels]
rate_columns = st.columns(max(len(rate_vessels), 1))

for column, vessel in zip(rate_columns, rate_vessels):
    vessel_history = dashboard_df.loc[dashboard_df["VesselType"] == vessel, ["Date", "ImplicitDayrate"]]
    latest_row, previous_row = _latest_and_previous(vessel_history, "ImplicitDayrate")
    current_value = float(latest_row["ImplicitDayrate"]) if latest_row is not None else None
    previous_value = float(previous_row["ImplicitDayrate"]) if previous_row is not None else None

    with column:
        st.metric(
            label=f"{vessel} Rate",
            value=_format_dayrate(current_value),
            delta=_metric_delta_text(
                current_value,
                previous_value,
                previous_row["Date"] if previous_row is not None else None,
            ),
        )

if not latest_defense_snapshot.empty and current_company_vessels:
    st.markdown("##### Defense Rates")

    defense_vessels = [vessel for vessel in current_company_vessels if vessel in latest_defense_snapshot["VesselType"].tolist()]
    defense_columns = st.columns(max(len(defense_vessels), 1))

    for column, vessel in zip(defense_columns, defense_vessels):
        vessel_row = latest_defense_snapshot.loc[latest_defense_snapshot["VesselType"] == vessel].iloc[0]
        defense_value = float(vessel_row["DefenseDayrate"])
        with column:
            st.markdown(
                f"""
                <div style="border:1px solid #e5e7eb; border-radius:14px; padding:0.8rem 0.9rem; background:#f8fafc; min-height:104px;">
                    <div style="font-size:0.8rem; font-weight:400; letter-spacing:0.02em; color:#475569; text-transform:uppercase; margin-bottom:0.35rem;">{vessel}</div>
                    <div style="font-size:1.35rem; font-weight:400; color:#0f172a; margin-bottom:0.25rem;">{_format_dayrate(defense_value)}</div>
                    <div style="font-size:0.8rem; font-weight:400; color:#64748b;">Rate needed to defend ship values.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

if latest_stock_date is not None and latest_ship_value_date is not None and latest_ship_value_date < latest_stock_date:
    st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
    st.info(
        f"Ship values currently run through {latest_ship_value_date:%Y-%m-%d}, while stock prices run through "
        f"{latest_stock_date:%Y-%m-%d}. Later dates are using the latest available ship-value curve."
    )

st.markdown(
    """
    <div style="margin: 1rem 0 1.35rem 0; border-top: 1px solid #e5e7eb;"></div>
    """,
    unsafe_allow_html=True,
)

ev_gav_series = dashboard_df[["Date", "EV_GAV"]].drop_duplicates().sort_values("Date")
ev_gav_latest, ev_gav_previous = _latest_and_previous(ev_gav_series, "EV_GAV")
current_ev_gav = float(ev_gav_latest["EV_GAV"]) if ev_gav_latest is not None else None
previous_ev_gav = float(ev_gav_previous["EV_GAV"]) if ev_gav_previous is not None else None


ev_col, summary_col = st.columns((1, 2))

with ev_col:
    st.metric(
        "Current EV/GAV",
        _format_ratio(current_ev_gav),
        delta=_metric_delta_text(
            current_ev_gav,
            previous_ev_gav,
            ev_gav_previous["Date"] if ev_gav_previous is not None else None,
        ),
    )

with summary_col:
    st.markdown(
        f"""
        **Interpretation**

        The cards above show the freight rates the market is currently implying for {company}'s main vessel classes.
        """
    )

st.markdown(
    """
    <div style="margin: 1rem 0 1.35rem 0; border-top: 1px solid #e5e7eb;"></div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Model Assumptions")
global_cols = st.columns(3)
global_cols[0].metric("WACC", f"{WACC:.0%}")
global_cols[1].metric("Utilization", f"{UTILIZATION:.0%}")
global_cols[2].metric("Newbuild Life", f"{NEWBUILD_LIFE} years")

st.caption("Global inputs at the top, followed by vessel-specific operating assumptions from the latest available date.")

assumption_vessels = _ordered_vessel_types(latest_snapshot["VesselType"].tolist())
assumption_cols = st.columns(max(len(assumption_vessels), 1))
for column, vessel in zip(assumption_cols, assumption_vessels):
    vessel_row = latest_snapshot.loc[latest_snapshot["VesselType"] == vessel].iloc[0]
    with column:
        st.markdown(
            f"""
            <div style="border:1px solid #e5e7eb; border-radius:18px; padding:1rem 1rem 0.85rem 1rem; background:#ffffff; min-height:190px;">
                <div style="font-size:1rem; font-weight:700; color:#111827; margin-bottom:0.8rem;">{vessel}</div>
                <div style="font-size:0.78rem; color:#6b7280;">Opex <span style="font-size:0.72rem;">(USD/day)</span></div>
                <div style="font-size:1.15rem; font-weight:600; color:#111827; margin-bottom:0.55rem;">{_format_money(vessel_row["OpexAssumption"], "/day")}</div>
                <div style="font-size:0.78rem; color:#6b7280;">G&amp;A <span style="font-size:0.72rem;">(USD/day)</span></div>
                <div style="font-size:1.15rem; font-weight:600; color:#111827; margin-bottom:0.55rem;">{_format_money(vessel_row["GnaAssumption"], "/day")}</div>
                <div style="font-size:0.78rem; color:#6b7280;">Capex <span style="font-size:0.72rem;">(USD/year)</span></div>
                <div style="font-size:1.15rem; font-weight:600; color:#111827;">{_format_money(vessel_row["CapexAssumption"], "/year")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.divider()
st.subheader("Implicit Rate History")

default_start = max(dashboard_df["Date"].min().date(), (latest_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)).date())
quick_ranges = {
    "3M": latest_date.date() - timedelta(days=90),
    "6M": latest_date.date() - timedelta(days=180),
    "1Y": latest_date.date() - timedelta(days=365),
    "3Y": latest_date.date() - timedelta(days=365 * 3),
    "Max": dashboard_df["Date"].min().date(),
}

control_col1, control_col2, control_col3 = st.columns((1.2, 1.15, 0.9))

with control_col1:
    selected_vessels = st.multiselect(
        "Visible rate series",
        options=available_vessels,
        default=rate_vessels if rate_vessels else available_vessels[:3],
    )

with control_col2:
    selected_range = st.radio(
        "Quick range",
        options=list(quick_ranges.keys()),
        index=2,
        horizontal=True,
    )

with control_col3:
    date_range = st.date_input(
        "Date range",
        value=(
            max(dashboard_df["Date"].min().date(), quick_ranges[selected_range]),
            latest_date.date(),
        ),
        min_value=dashboard_df["Date"].min().date(),
        max_value=latest_date.date(),
    )

if isinstance(date_range, tuple) and len(date_range) == 2:
    chart_start, chart_end = date_range
else:
    chart_start, chart_end = default_start, latest_date.date()

chart_df = dashboard_df.loc[
    dashboard_df["VesselType"].isin(selected_vessels)
    & (dashboard_df["Date"] >= pd.Timestamp(chart_start))
    & (dashboard_df["Date"] <= pd.Timestamp(chart_end)),
    ["Date", "VesselType", "ImplicitDayrate"],
].copy()

if chart_df.empty:
    st.info("No data available for the selected filters.")
else:
    st.altair_chart(_build_rate_chart(chart_df), width='stretch')

ev_chart_df = ev_gav_series.loc[
    (ev_gav_series["Date"] >= pd.Timestamp(chart_start))
    & (ev_gav_series["Date"] <= pd.Timestamp(chart_end))
].copy()

if not ev_chart_df.empty:
    st.subheader("EV/GAV History")
    st.altair_chart(_build_ev_gav_chart(ev_chart_df), width='stretch')

download_df = dashboard_df.loc[
    dashboard_df["VesselType"].isin(selected_vessels)
    & (dashboard_df["Date"] >= pd.Timestamp(chart_start))
    & (dashboard_df["Date"] <= pd.Timestamp(chart_end))
].sort_values(["Date", "VesselType"])
st.download_button(
    "Download filtered history",
    data=download_df.to_csv(index=False).encode("utf-8"),
    file_name=f"{ticker}_implicit_rate_history.csv",
    mime="text/csv",
)

with st.expander("Methodology"):
    st.markdown(
        """
        The model works backward from market valuation to implied freight rates.

        - `EV`: built from share price, debt, and cash
        - `Fleet asset value`: built from vessel counts, ages, ship-value curves, and G&A
        - `EV/GAV`: links market valuation to modeled fleet value
        - `Implicit dayrate`: the revenue level needed to support that valuation
        - `Defense dayrate`: the generic vessel-level rate needed to defend current ship values
        """
    )

with st.expander("Show raw implicit-rate data"):
    st.dataframe(
        dashboard_df.sort_values(["Date", "VesselType"]),
        width='stretch',
        hide_index=True,
    )

with st.expander("Show raw vessel defense-rate data"):
    st.dataframe(
        defense_df.sort_values(["Date", "VesselType"]),
        width='stretch',
        hide_index=True,
    )
