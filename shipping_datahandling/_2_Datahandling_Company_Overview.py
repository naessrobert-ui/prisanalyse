from pathlib import Path

import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Data"

PRICE_RANGE_LABELS = ("3M", "6M", "1Y", "3Y", "Max")
PRICE_RANGE_OFFSETS = {
    "3M": pd.DateOffset(months=3),
    "6M": pd.DateOffset(months=6),
    "1Y": pd.DateOffset(years=1),
    "3Y": pd.DateOffset(years=3),
}


@st.cache_data(show_spinner=False)
def load_price_history(ticker: str) -> pd.DataFrame:
    path = DATA_DIR / f"{ticker}_Prices.csv"
    if not path.exists():
        return pd.DataFrame()

    prices = pd.read_csv(path)
    if "Date" not in prices.columns or "Close" not in prices.columns:
        return pd.DataFrame()

    prices = prices[["Date", "Close"]].copy()
    prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
    prices["Close"] = pd.to_numeric(prices["Close"], errors="coerce")
    return prices.dropna(subset=["Date", "Close"]).sort_values("Date")


def filter_price_history(price_history: pd.DataFrame, selected_range: str) -> pd.DataFrame:
    if price_history.empty or selected_range == "Max":
        return price_history

    if selected_range not in PRICE_RANGE_OFFSETS:
        return price_history

    latest_date = price_history["Date"].max()
    start_date = latest_date - PRICE_RANGE_OFFSETS[selected_range]
    return price_history[price_history["Date"] >= start_date]
