from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd
import yfinance as yf

from shipping_datahandling._4_Datahandling_Implicit_Rate import build_latest_implicit_rate_snapshot, load_financials
from shipping_dataingestion import IngestionPipeline, YFinanceProvider
from shipping_app.data import (
    CompanyProfile,
    add_company_profile,
    get_company_names,
    get_company_profile,
    remove_company_profile,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "shipping_app" / "Data"
_IMPLICIT_VESSEL_TO_FIELD = {
    "VLCC": "implicit_rate_vlcc",
    "Suezmax": "implicit_rate_suezmax",
    "Aframax/LR2": "implicit_rate_aframax_lr2",
    "MR": "implicit_rate_mr",
}
_YFINANCE_PROVIDER = YFinanceProvider()


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_price_series(history: pd.DataFrame, field: str) -> pd.Series:
    if field in history.columns:
        selected = history[field]
        if isinstance(selected, pd.DataFrame):
            numeric_frame = selected.apply(pd.to_numeric, errors="coerce")
            return numeric_frame.bfill(axis=1).iloc[:, 0]
        return pd.to_numeric(selected, errors="coerce")

    if isinstance(history.columns, pd.MultiIndex):
        for column in history.columns.to_flat_index():
            parts = [str(part).strip().lower() for part in column if str(part).strip()]
            if field.lower() in parts:
                return pd.to_numeric(history[column], errors="coerce")

    return pd.Series(dtype=float)


def _trend_from_close(close_series: pd.Series) -> str:
    close = close_series.dropna()
    if len(close) < 22:
        return "Stable"

    baseline = close.iloc[-22]
    if baseline <= 0:
        return "Stable"

    change = (close.iloc[-1] / baseline) - 1.0
    if change >= 0.05:
        return "Rising"
    if change <= -0.05:
        return "Falling"
    return "Stable"


def _extract_market_cap(info: dict[str, object], fast_info: dict[str, object]) -> float | None:
    # yfinance varies key names across API surfaces and versions.
    candidates = [
        fast_info.get("marketCap"),
        fast_info.get("market_cap"),
        fast_info.get("lastMarketCap"),
        fast_info.get("last_market_cap"),
        info.get("marketCap"),
        info.get("market_cap"),
    ]
    for candidate in candidates:
        value = _safe_float(candidate)
        if value is not None and value > 0:
            return value
    return None


def _market_cap_from_shares(info: dict[str, object], close_series: pd.Series) -> float | None:
    shares_candidates = [
        info.get("sharesOutstanding"),
        info.get("shares_outstanding"),
        info.get("impliedSharesOutstanding"),
        info.get("implied_shares_outstanding"),
    ]
    shares = None
    for candidate in shares_candidates:
        parsed = _safe_float(candidate)
        if parsed is not None and parsed > 0:
            shares = parsed
            break

    if shares is None:
        return None

    close = close_series.dropna()
    if close.empty:
        return None

    return float(close.iloc[-1]) * float(shares)


def _market_cap_from_financials(ticker: str, latest_price: float, latest_date: pd.Timestamp | None) -> float | None:
    if latest_price <= 0:
        return None

    try:
        financials_df = load_financials(ticker)
    except Exception:
        return None

    if financials_df.empty or "ReportmentDate" not in financials_df.columns or "SharesOutstanding" not in financials_df.columns:
        return None

    financials = financials_df[["ReportmentDate", "SharesOutstanding"]].copy()
    financials["ReportmentDate"] = pd.to_datetime(financials["ReportmentDate"], errors="coerce").dt.normalize()
    financials["SharesOutstanding"] = pd.to_numeric(financials["SharesOutstanding"], errors="coerce")
    financials = financials.dropna(subset=["ReportmentDate", "SharesOutstanding"]).sort_values("ReportmentDate")
    if financials.empty:
        return None

    reference_date = latest_date.normalize() if latest_date is not None and pd.notna(latest_date) else None
    if reference_date is not None:
        eligible = financials.loc[financials["ReportmentDate"] <= reference_date]
        if eligible.empty:
            return None
        latest_shares_outstanding = float(eligible.iloc[-1]["SharesOutstanding"])
    else:
        latest_shares_outstanding = float(financials.iloc[-1]["SharesOutstanding"])

    if latest_shares_outstanding <= 0:
        return None

    # Financial workbook shares are stored in thousands in the same way the implicit-rate model expects.
    return latest_price * latest_shares_outstanding * 1000.0


def _load_local_price_history(ticker: str) -> pd.DataFrame:
    path = DATA_DIR / f"{ticker}_Prices.csv"
    if not path.exists():
        return pd.DataFrame()

    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

    if frame.empty:
        return frame

    if "Date" in frame.columns:
        frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    return frame


def _normalize_ticker(token: str) -> str:
    cleaned = token.strip().upper()
    return re.sub(r"[^A-Z0-9.\-]", "", cleaned)


def parse_ticker_input(raw_tickers: str) -> list[str]:
    tokens = [_normalize_ticker(token) for token in raw_tickers.split(",")]
    return [token for token in tokens if token]


def custom_tickers_signature() -> tuple[str, int]:
    # Kept for backward compatibility with callers that still include this
    # signature in cache keys. Universe now comes from company profiles.
    profiles_path = DATA_DIR / "company_profiles.json"
    if not profiles_path.exists():
        return ("missing", 0)
    return (profiles_path.name, profiles_path.stat().st_mtime_ns)


def _fetch_yfinance_snapshot(ticker: str) -> tuple[dict[str, object], pd.DataFrame]:
    source_ticker = _YFINANCE_PROVIDER.resolve_ticker(ticker)
    history = yf.download(
        tickers=source_ticker,
        period="1y",
        interval="1d",
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=False,
        group_by="column",
    )
    if history.empty:
        history = _load_local_price_history(ticker)
        if history.empty:
            raise ValueError(f"No yfinance price history returned for {ticker}.")

    info: dict[str, object] = {}
    fast_info: dict[str, object] = {}
    yf_ticker = yf.Ticker(source_ticker)

    try:
        fast_info = dict(yf_ticker.fast_info)
    except Exception:
        fast_info = {}

    try:
        info = dict(yf_ticker.info)
    except Exception:
        info = {}

    payload = {
        "source_ticker": source_ticker,
        "info": info,
        "fast_info": fast_info,
    }
    return payload, history


def build_screener_stock_row(ticker: str) -> dict[str, object]:
    ticker = _normalize_ticker(ticker)
    if not ticker:
        raise ValueError("Ticker cannot be empty.")

    payload, history = _fetch_yfinance_snapshot(ticker)
    info = payload["info"]
    fast_info = payload["fast_info"]

    close = _extract_price_series(history, "Close")
    latest_price = float(close.dropna().iloc[-1]) if not close.dropna().empty else 0.0
    latest_date = None
    if not close.dropna().empty:
        if "Date" in history.columns:
            latest_date = pd.to_datetime(history["Date"], errors="coerce").dropna().max()
        else:
            latest_date = pd.to_datetime(close.dropna().index[-1], errors="coerce")

    market_cap_value = _market_cap_from_financials(ticker, latest_price, latest_date)
    if market_cap_value is None:
        market_cap_value = _extract_market_cap(info, fast_info)
    if market_cap_value is None:
        market_cap_value = _market_cap_from_shares(info, close)
    market_cap_usd_bn = (market_cap_value or 0.0) / 1_000_000_000

    company_name = str(info.get("longName") or info.get("shortName") or ticker)

    return {
        "ticker": ticker,
        "company_name": company_name,
        "stock_price": latest_price,
        "market_cap_usd_bn": float(market_cap_usd_bn),
        "implicit_rate_vlcc": 0.0,
        "implicit_rate_suezmax": 0.0,
        "implicit_rate_aframax_lr2": 0.0,
        "implicit_rate_mr": 0.0,
        "signal_3m": "-",
        "signal_6m": "-",
        "signal_12m": "-",
        "diff_3m": 0.0,
        "diff_6m": 0.0,
        "diff_12m": 0.0,
    }


def _latest_implicit_rates_by_vessel(ticker: str) -> dict[str, float]:
    rates_by_field: dict[str, float] = {
        field_name: 0.0
        for field_name in _IMPLICIT_VESSEL_TO_FIELD.values()
    }

    try:
        snapshot_df = build_latest_implicit_rate_snapshot(ticker)
    except Exception:
        return rates_by_field

    if snapshot_df.empty or "VesselType" not in snapshot_df.columns or "ImplicitDayrate" not in snapshot_df.columns:
        return rates_by_field

    snapshot = snapshot_df.copy()
    snapshot["Date"] = pd.to_datetime(snapshot.get("Date"), errors="coerce")
    latest_date = snapshot["Date"].dropna().max()
    if pd.notna(latest_date):
        latest_snapshot = snapshot.loc[snapshot["Date"] == latest_date].copy()
    else:
        latest_snapshot = snapshot

    latest_snapshot["VesselType"] = latest_snapshot["VesselType"].astype(str)
    latest_snapshot["ImplicitDayrate"] = pd.to_numeric(latest_snapshot["ImplicitDayrate"], errors="coerce")

    for vessel_type, field_name in _IMPLICIT_VESSEL_TO_FIELD.items():
        vessel_rows = latest_snapshot.loc[
            latest_snapshot["VesselType"].str.lower() == vessel_type.lower(),
            "ImplicitDayrate",
        ].dropna()
        if not vessel_rows.empty:
            rates_by_field[field_name] = float(vessel_rows.iloc[-1])

    return rates_by_field


def add_tickers_from_input(raw_tickers: str) -> dict[str, list[str]]:
    requested = parse_ticker_input(raw_tickers)
    if not requested:
        return {"added": [], "already_present": [], "invalid": []}

    existing_profiles: dict[str, CompanyProfile] = {
        company: get_company_profile(company)
        for company in get_company_names()
    }
    profile_tickers = {profile.ticker.upper() for profile in existing_profiles.values()}

    added: list[str] = []
    already_present: list[str] = []
    invalid: list[str] = []

    pipeline = IngestionPipeline(YFinanceProvider())

    for ticker in requested:
        if ticker in profile_tickers:
            already_present.append(ticker)
            continue

        try:
            row = build_screener_stock_row(ticker)
            pipeline.refresh_company_prices(ticker, full_refresh=False, dry_run=False, max_age_days=0)
        except Exception:
            invalid.append(ticker)
            continue

        base_name = str(row.get("company_name") or ticker).strip() or ticker
        company_name = base_name
        suffix = 2
        while company_name in existing_profiles:
            company_name = f"{base_name} ({suffix})"
            suffix += 1

        profile = CompanyProfile(
            name=company_name,
            ticker=ticker,
            fleet_size=0,
            description="Added from Market Screener.",
        )
        add_company_profile(profile)
        existing_profiles[company_name] = profile
        profile_tickers.add(ticker)
        added.append(ticker)

    return {
        "added": sorted(added),
        "already_present": sorted(set(already_present)),
        "invalid": sorted(set(invalid)),
    }


def remove_ticker_from_screener(ticker: str) -> str:
    normalized_ticker = _normalize_ticker(ticker)
    for company_name in get_company_names():
        profile = get_company_profile(company_name)
        if profile.ticker.upper() == normalized_ticker:
            remove_company_profile(company_name)
            return company_name

    raise ValueError(f"Ticker {normalized_ticker} does not exist in company profiles.")


def build_screener_universe(
    latest_signals_by_horizon: dict[str, dict[str, dict[str, object]]]
) -> tuple[list[dict[str, object]], dict[str, object]]:
    profiles = [get_company_profile(company) for company in get_company_names()]
    row_cache_by_ticker: dict[str, dict[str, object]] = {}
    implicit_cache_by_ticker: dict[str, dict[str, float]] = {}

    stocks: list[dict[str, object]] = []
    for profile in profiles:
        ticker = profile.ticker.upper()
        try:
            if ticker not in row_cache_by_ticker:
                row_cache_by_ticker[ticker] = build_screener_stock_row(ticker)
            row = dict(row_cache_by_ticker[ticker])
            # Company profiles are the source of truth for list membership and names.
            row["company_name"] = profile.name
        except Exception:
            row = {
                "ticker": ticker,
                "company_name": profile.name,
                "stock_price": 0.0,
                "market_cap_usd_bn": 0.0,
                "implicit_rate_vlcc": 0.0,
                "implicit_rate_suezmax": 0.0,
                "implicit_rate_aframax_lr2": 0.0,
                "implicit_rate_mr": 0.0,
                "signal_3m": "-",
                "signal_6m": "-",
                "signal_12m": "-",
                "diff_3m": 0.0,
                "diff_6m": 0.0,
                "diff_12m": 0.0,
            }

        if ticker not in implicit_cache_by_ticker:
            implicit_cache_by_ticker[ticker] = _latest_implicit_rates_by_vessel(ticker)
        row.update(implicit_cache_by_ticker[ticker])

        horizons = latest_signals_by_horizon.get(ticker, {})
        for horizon in ("3m", "6m", "12m"):
            horizon_snapshot = horizons.get(horizon, {})
            row[f"signal_{horizon}"] = str(horizon_snapshot.get("action") or "-")
            row[f"diff_{horizon}"] = float(horizon_snapshot.get("spread") or 0.0)
        stocks.append(row)

    metadata = {
        "source": "yfinance",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "profile_stock_count": len(profiles),
        "custom_stock_count": 0,
        "total_stock_count": len(stocks),
    }
    return stocks, metadata
