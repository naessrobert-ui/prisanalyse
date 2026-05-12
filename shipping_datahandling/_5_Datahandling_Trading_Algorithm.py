import json
from pathlib import Path

import pandas as pd

from datahandling._4_Datahandling_Implicit_Rate import (
    load_fleet_master,
    load_fleet_ownership_periods,
    load_or_update_implicit_rate_output,
    fleet_stats_over_time,
)
from shipping_app.data import get_company_profile, load_spot_rate_prediction_bundle


BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "Output" / "Trading_Algorithm"
TRADING_CACHE_SCHEMA_VERSION = 2
VESSEL_ORDER = ["Aframax/LR2", "MR", "Suezmax", "VLCC"]
HORIZONS = ("3m", "6m", "12m")
SPOT_TO_TRADING_VESSEL_MAP = {
    "aframax": "Aframax/LR2",
    "mr": "MR",
    "suezmax": "Suezmax",
    "vlcc": "VLCC",
}


def _safe_company_name(company: str) -> str:
    return company.lower().replace(" ", "_")


def _cache_paths(company: str, start_date: str) -> tuple[Path, Path]:
    safe_company = _safe_company_name(company)
    safe_start = str(start_date).replace("-", "")
    base_name = f"{safe_company}_trading_model_data_{safe_start}"
    return OUTPUT_DIR / f"{base_name}.pkl", OUTPUT_DIR / f"{base_name}.meta.json"


def _load_cached_trading_model_data(company: str, start_date: str) -> tuple[pd.DataFrame | None, dict | None]:
    data_path, meta_path = _cache_paths(company, start_date)
    if not data_path.exists() or not meta_path.exists():
        return None, None

    cached_df = pd.read_pickle(data_path)
    cached_df["Date"] = pd.to_datetime(cached_df["Date"], errors="coerce")
    with meta_path.open("r", encoding="utf-8") as fh:
        meta = json.load(fh)
    return cached_df, meta


def _save_cached_trading_model_data(
    company: str,
    start_date: str,
    df: pd.DataFrame,
    price_col: str,
) -> tuple[pd.DataFrame, dict]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    data_path, meta_path = _cache_paths(company, start_date)

    cached_df = df.copy()
    cached_df["Date"] = pd.to_datetime(cached_df["Date"], errors="coerce")
    cached_df.to_pickle(data_path)
    data_mtime_ns = data_path.stat().st_mtime_ns

    meta = {
        "schema_version": TRADING_CACHE_SCHEMA_VERSION,
        "company": company,
        "start_date": str(start_date),
        "price_col": price_col,
        "row_count": int(len(cached_df)),
        "cached_from": cached_df["Date"].min().strftime("%Y-%m-%d") if not cached_df.empty else None,
        "cached_through": cached_df["Date"].max().strftime("%Y-%m-%d") if not cached_df.empty else None,
        "data_path": str(data_path),
        "data_mtime_ns": data_mtime_ns,
    }
    with meta_path.open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)

    return cached_df, meta


def get_trading_data_paths(company: str) -> dict[str, str]:
    """Get file paths for trading model data based on company name."""
    ticker = get_company_profile(company).ticker
    return {
        "prices": str(BASE_DIR / "Data" / f"{ticker}_Prices.csv"),
    }


def _load_implicit_rates_wide(ticker: str, start_date: str) -> pd.DataFrame:
    implicit = load_or_update_implicit_rate_output(
        ticker,
        start_date=start_date,
    ).copy()
    implicit["Date"] = pd.to_datetime(implicit["Date"])
    implicit = implicit.loc[implicit["Date"] >= start_date, ["Date", "VesselType", "ImplicitDayrate"]]

    implicit_wide = (
        implicit.pivot_table(
            index="Date",
            columns="VesselType",
            values="ImplicitDayrate",
            aggfunc="last",
        )
    )
    for vessel in VESSEL_ORDER:
        if vessel not in implicit_wide.columns:
            implicit_wide[vessel] = pd.NA

    implicit_wide = (
        implicit_wide[VESSEL_ORDER]
        .rename(columns=lambda vessel: f"{vessel}_ImplicitDayrate")
        .reset_index()
    )
    implicit_wide["implicit_rate_date"] = implicit_wide["Date"]
    return implicit_wide


def _load_fleet_counts(company: str, start_date: str) -> pd.DataFrame:
    ticker = get_company_profile(company).ticker
    fleet_master = load_fleet_master(ticker)
    fleet_timeline = load_fleet_ownership_periods(ticker)
    fleet_stats = fleet_stats_over_time(
        fleet_timeline,
        fleet_master,
        start_date=start_date,
    )

    counts_wide = (
        fleet_stats.pivot_table(
            index="Date",
            columns="VesselType",
            values="Count",
            aggfunc="last",
        )
    )
    for vessel in VESSEL_ORDER:
        if vessel not in counts_wide.columns:
            counts_wide[vessel] = 0

    return (
        counts_wide[VESSEL_ORDER]
        .rename(columns=lambda vessel: f"{vessel}_Count")
        .reset_index()
    )


def _load_predictions(start_date: str) -> pd.DataFrame:
    bundle = load_spot_rate_prediction_bundle()
    pred = bundle["predictions"].copy()
    pred["Date"] = pd.to_datetime(pred["forecast_month"])
    pred["VesselType"] = pred["vessel_type"].astype(str).str.lower().map(SPOT_TO_TRADING_VESSEL_MAP)
    pred["Horizon"] = pred["display_horizon_months"].astype("Int64").astype(str) + "m"

    pred = pred.loc[
        (pred["Date"] >= pd.Timestamp(start_date))
        & pred["VesselType"].notna()
        & pred["Horizon"].isin(HORIZONS),
        ["Date", "VesselType", "Horizon", "forecast_tce"],
    ].copy()

    pred_wide = (
        pred.pivot_table(
            index="Date",
            columns=["VesselType", "Horizon"],
            values="forecast_tce",
            aggfunc="last",
        )
        .sort_index(axis=1)
    )

    expected_columns: list[tuple[str, str]] = [
        (vessel, horizon)
        for vessel in VESSEL_ORDER
        for horizon in HORIZONS
    ]
    pred_wide = pred_wide.reindex(columns=pd.MultiIndex.from_tuples(expected_columns))
    pred_wide.columns = [
        f"{vessel}_predicted_values_{horizon}"
        for vessel, horizon in pred_wide.columns.to_list()
    ]
    pred_wide = pred_wide.reset_index()
    for horizon in HORIZONS:
        horizon_cols = [
            f"{vessel}_predicted_values_{horizon}"
            for vessel in VESSEL_ORDER
        ]
        pred_wide[f"prediction_date_{horizon}"] = pred_wide["Date"].where(
            pred_wide[horizon_cols].notna().any(axis=1)
        )

    required_cols = ["Date"] + [
        f"{vessel}_predicted_values_{horizon}"
        for vessel in VESSEL_ORDER
        for horizon in HORIZONS
    ] + [f"prediction_date_{horizon}" for horizon in HORIZONS]
    missing = [col for col in required_cols if col not in pred_wide.columns]
    if missing:
        raise KeyError(f"Missing prediction columns in refreshed spot-rate bundle: {missing}")

    return pred_wide[required_cols].copy()


def _merge_latest_available(base_df: pd.DataFrame, other_df: pd.DataFrame) -> pd.DataFrame:
    left = base_df.copy()
    right = other_df.copy()
    left["Date"] = pd.to_datetime(left["Date"]).astype("datetime64[ns]")
    right["Date"] = pd.to_datetime(right["Date"]).astype("datetime64[ns]")

    return pd.merge_asof(
        left.sort_values("Date"),
        right.sort_values("Date"),
        on="Date",
        direction="backward",
    )


def load_trading_model_data(company: str, start_date: str = "2015-01-01") -> tuple[pd.DataFrame, str]:
    """
    Load and merge trading model data (implicit rates, fleet counts, prices, predictions)
    for a company. Returns a tuple of (merged DataFrame, price_col name).
    """
    paths = get_trading_data_paths(company)
    ticker = get_company_profile(company).ticker

    prices = pd.read_csv(paths["prices"])
    prices["Date"] = pd.to_datetime(prices["Date"])
    for c in ["Adj Close", "Close", "Open", "High", "Low", "Volume"]:
        if c in prices.columns:
            prices[c] = pd.to_numeric(prices[c], errors="coerce")
    price_col = "Adj Close" if "Adj Close" in prices.columns else "Close"
    prices["stock_return_1d"] = prices[price_col].pct_change().shift(-1)
    prices["stock_price_date"] = prices["Date"]
    prices = prices[prices["Date"] >= start_date].copy()
    prices = prices.sort_values("Date").reset_index(drop=True)

    implicit = _load_implicit_rates_wide(ticker, start_date)
    counts = _load_fleet_counts(company, start_date)
    pred = _load_predictions(start_date)

    df = (
        prices[["Date", price_col, "stock_return_1d", "stock_price_date"]]
        .pipe(_merge_latest_available, implicit)
        .pipe(_merge_latest_available, counts)
        .pipe(_merge_latest_available, pred)
        .sort_values("Date")
        .reset_index(drop=True)
    )

    return df, price_col


def load_cached_or_refresh_trading_model_data(
    company: str,
    start_date: str = "2015-01-01",
    *,
    force_refresh: bool = False,
) -> tuple[pd.DataFrame, str, dict]:
    if not force_refresh:
        cached_df, cached_meta = _load_cached_trading_model_data(company, start_date)
        if (
            cached_df is not None
            and cached_meta is not None
            and cached_meta.get("schema_version") == TRADING_CACHE_SCHEMA_VERSION
            and cached_meta.get("price_col")
        ):
            return cached_df, str(cached_meta["price_col"]), cached_meta

    df, price_col = load_trading_model_data(company, start_date=start_date)
    cached_df, meta = _save_cached_trading_model_data(company, start_date, df, price_col)
    return cached_df, price_col, meta
