from pathlib import Path

import pandas as pd
import streamlit as st

from shipping_app.data import (
    CompanyProfile,
    add_company_profile,
    get_company_names,
    remove_company_profile,
)
from shipping_app.price_refresh import refresh_all_company_prices
from shipping_app.ui import render_sidebar


st.set_page_config(
    page_title="Shipping Sector Analysis",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "shipping_app" / "Data"
OUTPUT_DIR = BASE_DIR / "shipping_app" / "Output"


def format_date(value) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return pd.to_datetime(value).strftime("%d %b %Y")


def latest_file_update(paths: list[Path]) -> str:
    existing_paths = [path for path in paths if path.exists()]
    if not existing_paths:
        return "Missing"
    latest = max(path.stat().st_mtime for path in existing_paths)
    return format_date(pd.to_datetime(latest, unit="s"))


@st.cache_data(show_spinner=False)
def latest_price_observation(data_dir: Path) -> str:
    latest_date = None
    for path in data_dir.glob("*_Prices.csv"):
        prices = pd.read_csv(path, usecols=lambda column: column == "Date")
        if "Date" not in prices.columns:
            continue
        dates = pd.to_datetime(prices["Date"], errors="coerce").dropna()
        if dates.empty:
            continue
        file_latest = dates.max()
        if latest_date is None or file_latest > latest_date:
            latest_date = file_latest
    return format_date(latest_date)


def render_status_card(title: str, caption: str) -> None:
    st.markdown(
        f"""
        <div style="
            border: 1px solid rgba(49, 51, 63, 0.18);
            border-radius: 0.5rem;
            padding: 1.05rem 1.25rem;
            min-height: 8.9rem;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        ">
            <div style="font-weight: 700; font-size: 1.25rem; color: #31333f;">{title}</div>
            <div style="font-size: 1rem; line-height: 1.55; color: rgba(49, 51, 63, 0.68);">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


render_sidebar()
spot_predictions_file = OUTPUT_DIR / "Spot_Rate_Pred" / "spot_rate_predictions_from_2015.xlsx"
implicit_output_files = list((OUTPUT_DIR / "Implicit_Rate").glob("*")) if (OUTPUT_DIR / "Implicit_Rate").exists() else []

st.title("Shipping Sector Financial Analysis")
st.caption(
    "A decision dashboard for freight forecasts, company fundamentals, asset values, "
    "implied rates, and trading signals."
)
st.markdown(
    """
    Move from market outlook to company-level investment view: forecast spot rates,
    review fleet exposure, compare market pricing against implied rates, and
    pressure-test the resulting trading signal.
    """
)

st.divider()

st.subheader("Workflow")

market_col, company_col, trading_col = st.columns(3)

with market_col:
    with st.container(border=True):
        st.markdown("**Market View**")
        st.caption("Start with freight-market direction and screening.")
        st.page_link(
            "shipping_pages/1_Spot_Rate_Predictions.py",
            label="Spot Rate Predictions",
            icon=":material/show_chart:",
        )
        st.page_link(
            "shipping_pages/1b_Market_Screener.py",
            label="Stock Screener",
            icon=":material/query_stats:",
        )

with company_col:
    with st.container(border=True):
        st.markdown("**Company View**")
        st.caption("Translate the market view into company fundamentals.")
        st.page_link(
            "shipping_pages/2_Company_Overview.py",
            label="Company Overview",
            icon=":material/domain:",
        )
        st.page_link(
            "shipping_pages/3_Fleet_Valuation.py",
            label="Fleet & Valuation",
            icon=":material/directions_boat:",
        )

with trading_col:
    with st.container(border=True):
        st.markdown("**Trading View**")
        st.caption("Compare forecasts with equity-implied expectations.")
        st.page_link(
            "shipping_pages/4_Implicit_Rate.py",
            label="Implicit Rate",
            icon=":material/functions:",
        )
        st.page_link(
            "shipping_pages/5_Trading_Algorithm.py",
            label="Trading Algorithm",
            icon=":material/auto_graph:",
        )

st.divider()

st.subheader("Data status")

auto_refresh_error = st.session_state.get("auto_price_refresh_error")
auto_refresh_status = st.session_state.get("auto_price_refresh_status")
if auto_refresh_error:
    st.warning(f"Automatic price refresh did not run: {auto_refresh_error}")
elif auto_refresh_status:
    st.caption(f"Automatic price refresh: {auto_refresh_status}")

refresh_col, refresh_note_col = st.columns([0.26, 0.74])
with refresh_col:
    if st.button("Refresh all prices", icon=":material/sync:", width="stretch"):
        try:
            with st.spinner("Refreshing full price histories..."):
                summary = refresh_all_company_prices(full_refresh=True)
            st.cache_data.clear()
            st.success(f"Full price refresh complete: {summary.status_text()}.")
            st.rerun()
        except Exception as exc:
            st.error(f"Full price refresh failed: {exc}")

with refresh_note_col:
    st.caption(
        "The app opens from local data so the dashboard is available even if market-data "
        "providers are slow or blocked. Use full refresh when you want to rebuild the "
        "local price history."
    )

with st.expander("Add company", icon=":material/add_business:"):
    st.caption(
        "This saves a new company profile to the JSON-backed company store used by the app. "
        "It will appear in the sidebar immediately after saving."
    )
    with st.form("add_company_form", clear_on_submit=True):
        form_col_left, form_col_right = st.columns(2)
        with form_col_left:
            company_name = st.text_input("Company name")
            ticker = st.text_input("Ticker")
            fleet_size = st.number_input("Fleet size", min_value=0, step=1, value=0)
        with form_col_right:
            description = st.text_area(
                "Description",
                value="New company profile added from the webpage.",
                height=100,
            )

        submitted = st.form_submit_button("Save company", width="stretch")

    if submitted:
        cleaned_name = company_name.strip()
        cleaned_ticker = ticker.strip().upper()

        if not cleaned_name:
            st.error("Company name is required.")
        elif not cleaned_ticker:
            st.error("Ticker is required.")
        else:
            try:
                profile = CompanyProfile(
                    name=cleaned_name,
                    ticker=cleaned_ticker,
                    fleet_size=int(fleet_size),
                    description=description.strip() or "New company profile added from the webpage.",
                )
                add_company_profile(profile)
                st.session_state["pending_selected_company"] = cleaned_name
                st.success(f"Saved company profile for {cleaned_name}.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Could not save company: {exc}")

with st.expander("Remove company", icon=":material/delete:"):
    company_names = get_company_names()
    st.caption(
        "This removes the company profile from the JSON-backed company store. "
        "Use this for test companies or names you no longer want in the app."
    )

    if len(company_names) <= 1:
        st.info("At least one company must remain in the app.")
    else:
        with st.form("remove_company_form"):
            company_to_remove = st.selectbox(
                "Company to remove",
                options=company_names,
                key="remove_company_name",
            )
            confirm_remove = st.checkbox(
                "I understand this removes the company from the shared company list.",
                key="confirm_remove_company",
            )
            remove_submitted = st.form_submit_button(
                "Remove company",
                width="stretch",
                type="primary",
            )

        if remove_submitted:
            if not confirm_remove:
                st.error("Please confirm the removal before deleting a company.")
            else:
                try:
                    remaining_companies = [name for name in company_names if name != company_to_remove]
                    remove_company_profile(company_to_remove)
                    if st.session_state.get("selected_company") == company_to_remove:
                        st.session_state["pending_selected_company"] = remaining_companies[0]
                    st.success(f"Removed company profile for {company_to_remove}.")
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))
                except Exception as exc:
                    st.error(f"Could not remove company: {exc}")

fleet_files = list(DATA_DIR.glob("*_Fleet.xlsx"))
financial_files = list(DATA_DIR.glob("*_Financials.xlsx"))

status_cols = st.columns(4)
status_cols[0].metric("Companies", len(get_company_names()))
status_cols[1].metric("Latest price data", latest_price_observation(DATA_DIR))
status_cols[2].metric("Latest fleet update", latest_file_update(fleet_files))
status_cols[3].metric("Latest model output", latest_file_update([spot_predictions_file] + implicit_output_files))

source_cols = st.columns(3)
with source_cols[0]:
    render_status_card(
        "Company inputs",
        f"{len(fleet_files)} fleet files and {len(financial_files)} financial workbooks found.",
    )

with source_cols[1]:
    render_status_card(
        "Market data",
        f"{len(list(DATA_DIR.glob('*_Prices.csv')))} price histories found.",
    )

with source_cols[2]:
    output_count = int(spot_predictions_file.exists()) + len(implicit_output_files)
    render_status_card(
        "Model outputs",
        f"{output_count} generated output files found.",
    )

st.divider()
st.markdown(
    """
    <div style="text-align: center; color: #6b7280; padding: 0.75rem 0 0.25rem 0;">
        <div style="font-weight: 600; margin-bottom: 0.35rem;">A product by</div>
        <div>Markus Dybevold Simonsen, August Wille Vage, Mathias Lillejord, Reidar Greve, Sindre Froestad and Simen Lippestad</div>
    </div>
    """,
    unsafe_allow_html=True,
)
