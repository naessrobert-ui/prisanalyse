import streamlit as st
import plotly.express as px
import pandas as pd

from shipping_app.data import get_company_profile
from shipping_app.ui import render_sidebar
from shipping_datahandling._3_Datahandling_Fleet_Valuation import (
    PRICE_COLUMN_MAPPING,
    add_ship_to_fleet,
    build_fleet_table,
    end_ship_ownership,
    get_vessel_type_counts,
    load_fleet_values,
    run_fleet_valuation_model,
)

st.set_page_config(
    page_title="Fleet & Valuation",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)

company = render_sidebar()
profile = get_company_profile(company)
ticker = profile.ticker

current_owned_df, vessel_type_counts = get_vessel_type_counts(ticker)
fleet_values_df = load_fleet_values()
fleet_table = build_fleet_table(ticker)

st.title(f"{profile.name} Fleet & Valuation")
st.caption("Fleet composition, operating profile, and an asset-value bridge.")

st.subheader("Current fleet overview")

col_left, col_right = st.columns((0.9, 1.6))

with col_left:
    st.metric("Number of ships currently owned", len(current_owned_df))
    st.markdown("**Ships by vessel type**")
    st.dataframe(
        vessel_type_counts,
        width="stretch",
        hide_index=True,
    )

with col_right:
    if not vessel_type_counts.empty:
        fig_pie = px.pie(
            vessel_type_counts,
            names="VesselType",
            values="Count",
            title="Current owned fleet mix",
            color="VesselType",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hole=0.4,
            marker=dict(line=dict(color="#000000", width=1)),
        )
        fig_pie.update_layout(
            title_x=0.25,
            showlegend=True,
            legend_title="Vessel Type",
            margin=dict(t=50, b=20, l=20, r=20),
        )
        st.plotly_chart(fig_pie, width="stretch")
    else:
        st.warning(f"No currently owned vessels found for {profile.name}.")

st.divider()
details_header, edit_action = st.columns([1, 0.18])

with details_header:
    st.subheader("Fleet details")

with edit_action:
    if st.button("Edit fleet", width="stretch"):
        st.session_state["show_fleet_editor"] = True

if st.session_state.get("show_fleet_editor", False):
    with st.expander("Edit fleet", expanded=True):
        add_tab, end_tab = st.tabs(["Add ship", "End period"])

        with add_tab:
            with st.form("add_ship_form", clear_on_submit=True):
                name_col, type_col = st.columns(2)

                with name_col:
                    new_vessel = st.text_input("Ship name")

                with type_col:
                    vessel_type_options = ["VLCC", "Suezmax", "Aframax/LR2", "LR1", "MR"]
                    new_vessel_type = st.selectbox("Type", vessel_type_options)

                built_col, dwt_col, start_col = st.columns(3)

                with built_col:
                    new_built = st.number_input(
                        "Built year",
                        min_value=1950,
                        max_value=pd.Timestamp.today().year + 5,
                        value=pd.Timestamp.today().year,
                        step=1,
                    )

                with dwt_col:
                    new_dwt = st.number_input("DWT", min_value=0, value=0, step=1000)

                with start_col:
                    new_start_date = st.date_input("Start date")

                owned_col, flag_col = st.columns([0.35, 0.65])

                with owned_col:
                    new_owned = st.checkbox("Owned", value=True)

                with flag_col:
                    new_flag = st.text_input("Flag")

                employment_col, rate_col = st.columns(2)

                with employment_col:
                    new_employment = st.text_input("Employment")

                with rate_col:
                    new_time_charter_rate = st.number_input(
                        "Time charter rate",
                        min_value=0,
                        value=0,
                        step=500,
                    )

                add_submitted = st.form_submit_button("Add ship")

                if add_submitted:
                    try:
                        vessel_id = add_ship_to_fleet(
                            ticker=ticker,
                            vessel=new_vessel,
                            vessel_type=new_vessel_type,
                            built=int(new_built),
                            dwt=None if new_dwt == 0 else new_dwt,
                            start_date=new_start_date,
                            owned=new_owned,
                            flag=new_flag,
                            employment=new_employment,
                            time_charter_rate=(
                                None if new_time_charter_rate == 0 else new_time_charter_rate
                            ),
                        )
                        st.success(f"Added {new_vessel} with ID {vessel_id}.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not add ship: {exc}")

        with end_tab:
            active_vessels = current_owned_df.copy()

            if active_vessels.empty:
                st.info("There are no currently active ships to end.")
            else:
                active_vessels["Display"] = (
                    active_vessels["Vessel"].astype(str)
                    + " - "
                    + active_vessels["VesselType"].astype(str)
                    + " ("
                    + active_vessels["ID"].astype(str)
                    + ")"
                )

                with st.form("end_ship_form"):
                    selected_display = st.selectbox(
                        "Ship",
                        active_vessels["Display"].tolist(),
                    )
                    end_date = st.date_input("End date")
                    end_submitted = st.form_submit_button("End ownership period")

                    if end_submitted:
                        selected_id = active_vessels.loc[
                            active_vessels["Display"] == selected_display,
                            "ID",
                        ].iloc[0]

                        try:
                            updated_rows = end_ship_ownership(
                                ticker=ticker,
                                vessel_id=selected_id,
                                end_date=end_date,
                            )
                            st.success(
                                f"Ended {updated_rows} active period for {selected_display}."
                            )
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Could not end period: {exc}")

if fleet_table.empty:
    st.warning(f"No fleet details found for {profile.name}.")
else:
    st.dataframe(fleet_table, width="stretch", hide_index=True)

st.divider()
st.subheader("Fleet valuation model")

if fleet_values_df.empty:
    st.warning("No FleetValues.xlsx data found; valuation model unavailable.")
else:
    control_cols = st.columns(3)

    with control_cols[0]:
        vessel_types = sorted(list(PRICE_COLUMN_MAPPING.keys()))
        selected_vessel_types = st.multiselect(
            "Vessel type filter",
            options=vessel_types,
            default=vessel_types,
        )

    with control_cols[1]:
        depreciation_curve_param = st.slider(
            "Depreciation curve",
            min_value=0.2,
            max_value=1.5,
            value=0.65,
            step=0.05,
            help="Concavity control for age/value interpolation between 0y, 5y, 15y anchors.",
        )

    with control_cols[2]:
        economic_life = st.number_input(
            "Economic life",
            min_value=15,
            max_value=35,
            value=20,
            step=1,
        )

    model_output = run_fleet_valuation_model(
        ticker=ticker,
        start_date=None,
        end_date=None,
        selected_vessel_types=selected_vessel_types,
        depreciation_curve_param=depreciation_curve_param,
        economic_life=int(economic_life),
    )

    for warning in model_output["warnings"]:
        st.warning(warning)

    vessel_values = model_output["vessel_values"]
    fleet_total = model_output["fleet_total"]
    fleet_by_type = model_output["fleet_by_type"]
    nav_bridge = model_output["nav_bridge"]

    st.markdown("#### Vessel-level valuation table")

    if vessel_values.empty:
        st.info("No vessel-level valuation results for selected filters.")
    else:
        valuation_dates = sorted(vessel_values["Date"].dt.date.unique())

        valuation_date = st.selectbox(
            "Valuation date",
            options=valuation_dates,
            index=max(0, len(valuation_dates) - 1),
        )

        vessel_slice = vessel_values[
            vessel_values["Date"].dt.date == valuation_date
        ].copy()

        vessel_slice = vessel_slice.sort_values(["VesselType", "Vessel"])

        st.dataframe(
            vessel_slice[
                [
                    "Vessel",
                    "VesselType",
                    "Built",
                    "Age",
                    "AnchorCurrent",
                    "Anchor5Y",
                    "Anchor15Y",
                    "Value",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    st.markdown("#### Fleet-level valuation over time")

    if fleet_total.empty:
        st.info("No fleet-level valuation history available.")
    else:
        fig_total = px.line(
            fleet_total,
            x="Date",
            y="TotalFleetValue",
            title="Total fleet value",
        )
        fig_total.update_layout(
            xaxis_title=None,
            yaxis_title="Fleet value",
            margin=dict(l=10, r=10, t=50, b=10),
        )
        st.plotly_chart(fig_total, width="stretch")

    st.markdown("#### Fleet value by vessel type")

    if fleet_by_type.empty:
        st.info("No vessel-type breakdown available.")
    else:
        fig_type = px.area(
            fleet_by_type,
            x="Date",
            y="FleetValue",
            color="VesselType",
            title="Fleet value by vessel type",
        )
        fig_type.update_layout(
            xaxis_title=None,
            yaxis_title="Fleet value",
            margin=dict(l=10, r=10, t=50, b=10),
        )
        st.plotly_chart(fig_type, width="stretch")

    show_nav = st.checkbox("Show NAV bridge (optional)", value=True)

    if show_nav:
        st.markdown("#### NAV bridge")
        st.caption("NAV = FleetValue + Cash - TotalDebt")

        if nav_bridge.empty:
            st.info("NAV bridge unavailable with current financial data.")
        else:

            fig_nav = px.line(
                nav_bridge,
                x="Date",
                y=["NAV", "TotalFleetValue"],
                title="NAV bridge history",
            )
            fig_nav.update_layout(
                xaxis_title=None,
                yaxis_title="USD",
                margin=dict(l=10, r=10, t=50, b=10),
            )
            st.plotly_chart(fig_nav, width="stretch")
