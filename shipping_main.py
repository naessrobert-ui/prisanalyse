"""Streamlit entrypoint for the shipping analysis app.

Registers all pages with st.navigation so that st.page_link targets in
shipping_app/ui.py and shipping_home.py resolve. The built-in sidebar
navigation is hidden because the app renders its own via render_sidebar().
"""

import streamlit as st

pages = [
    st.Page(
        "shipping_home.py",
        title="Home",
        icon=":material/home:",
        default=True,
    ),
    st.Page(
        "shipping_pages/1_Spot_Rate_Predictions.py",
        title="Spot Rate Predictions",
        icon=":material/show_chart:",
    ),
    st.Page(
        "shipping_pages/1b_Market_Screener.py",
        title="Stock Screener",
        icon=":material/query_stats:",
    ),
    st.Page(
        "shipping_pages/2_Company_Overview.py",
        title="Company Overview",
        icon=":material/domain:",
    ),
    st.Page(
        "shipping_pages/3_Fleet_Valuation.py",
        title="Fleet & Valuation",
        icon=":material/directions_boat:",
    ),
    st.Page(
        "shipping_pages/4_Implicit_Rate.py",
        title="Implicit Rate",
        icon=":material/functions:",
    ),
    st.Page(
        "shipping_pages/5_Trading_Algorithm.py",
        title="Trading Algorithm",
        icon=":material/auto_graph:",
    ),
]

st.navigation(pages, position="hidden").run()
