import plotly.express as px
import streamlit as st

from datahandling._2_Datahandling_Company_Overview import (
    PRICE_RANGE_LABELS,
    filter_price_history,
    load_price_history,
)
from shipping_app.data import get_company_profile
from shipping_app.ui import render_sidebar


st.set_page_config(
    page_title="Company Overview",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)
company = render_sidebar()
profile = get_company_profile(company)
price_history = load_price_history(profile.ticker)

latest_price = None
if not price_history.empty:
    latest_price = price_history.iloc[-1]["Close"]

st.title(profile.name)
st.metric("Share price", f"${latest_price:,.2f}" if latest_price else "N/A")

st.divider()

st.subheader("Share price history")

selected_range = st.segmented_control(
    "Range",
    PRICE_RANGE_LABELS,
    default="1Y",
)
selected_range = selected_range or "1Y"

if price_history.empty:
    st.warning(f"No valid price history found for {profile.ticker}.")
else:
    chart_history = filter_price_history(price_history, selected_range)
    fig = px.line(
        chart_history,
        x="Date",
        y="Close",
        title=f"{profile.ticker} close price",
    )
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="Close price",
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, width="stretch")
