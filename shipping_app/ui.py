from __future__ import annotations

import streamlit as st
from shipping_app.data import get_company_names
from shipping_app.price_refresh import refresh_all_company_prices


SELECTED_COMPANY_KEY = "selected_company"
COMPANY_SELECTBOX_KEY = "_selected_company"


def ensure_incremental_price_refresh() -> None:
    """Run one lightweight stale-price check per Streamlit session."""
    if st.session_state.get("auto_price_refresh_checked"):
        return

    st.session_state["auto_price_refresh_checked"] = True
    try:
        summary = refresh_all_company_prices(full_refresh=False)
    except Exception as exc:
        st.session_state["auto_price_refresh_error"] = str(exc)
        return

    st.session_state["auto_price_refresh_status"] = summary.status_text()


def _sync_selected_company() -> None:
    """Persist the sidebar widget choice across Streamlit pages."""
    selected = st.session_state.get(COMPANY_SELECTBOX_KEY)
    if selected:
        st.session_state[SELECTED_COMPANY_KEY] = selected


def _load_selected_company_widget(selected_company: str) -> None:
    st.session_state[COMPANY_SELECTBOX_KEY] = selected_company


def render_sidebar() -> str:
    ensure_incremental_price_refresh()

    companies = get_company_names()
    default_company = "Frontline" if "Frontline" in companies else companies[0]

    pending_selected_company = st.session_state.pop("pending_selected_company", None)
    if pending_selected_company in companies:
        st.session_state[SELECTED_COMPANY_KEY] = pending_selected_company

    selected_company = st.session_state.get(SELECTED_COMPANY_KEY, default_company)
    if selected_company not in companies:
        selected_company = default_company
    st.session_state[SELECTED_COMPANY_KEY] = selected_company
    _load_selected_company_widget(selected_company)

    with st.sidebar:
        st.subheader("Navigation")
        st.page_link("app.py", label="Home", icon=":material/home:")
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

        st.subheader("Analysis Controls")

        selected = st.selectbox(
            "Select company",
            companies,
            key=COMPANY_SELECTBOX_KEY,
            on_change=_sync_selected_company,
        )
        st.session_state[SELECTED_COMPANY_KEY] = selected

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

    return selected
