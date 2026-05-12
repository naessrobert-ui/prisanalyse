from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from dataingestion.providers import ProviderResult, StubProvider
from dataingestion.schemas import PRICE_COLUMNS


def _flatten_yfinance_columns(frame: pd.DataFrame, source_ticker: str) -> pd.DataFrame:
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame

    columns = frame.columns
    level_values = [
        {str(value) for value in columns.get_level_values(level)}
        for level in range(columns.nlevels)
    ]
    source_ticker = source_ticker.upper()

    ticker_level = None
    for level, values in enumerate(level_values):
        if source_ticker in {value.upper() for value in values}:
            ticker_level = level
            break

    if ticker_level is None:
        flattened = frame.copy()
        flattened.columns = [
            " ".join(str(part) for part in column if str(part))
            for column in flattened.columns.to_flat_index()
        ]
        return flattened

    field_level = 1 - ticker_level if columns.nlevels == 2 else 0
    flattened = frame.copy()
    flattened.columns = [
        str(column[field_level])
        for column in flattened.columns.to_flat_index()
    ]
    return flattened


def format_yfinance_price_history(raw_history: pd.DataFrame, source_ticker: str) -> pd.DataFrame:
    """Convert yfinance output to the app's existing {TICKER}_Prices.csv format."""
    if raw_history.empty:
        raise ValueError(f"yfinance returned no price history for {source_ticker}.")

    history = _flatten_yfinance_columns(raw_history, source_ticker).copy()
    history = history.reset_index()

    date_column = "Date" if "Date" in history.columns else history.columns[0]
    history = history.rename(columns={date_column: "Date"})
    history["Date"] = pd.to_datetime(history["Date"], errors="coerce").dt.tz_localize(None)

    for column in PRICE_COLUMNS:
        if column not in history.columns:
            history[column] = 0.0 if column in {"Dividends", "Stock Splits"} else pd.NA

    if history["Adj Close"].isna().all() and "Close" in history.columns:
        history["Adj Close"] = history["Close"]

    numeric_columns = [column for column in PRICE_COLUMNS if column != "Date"]
    for column in numeric_columns:
        history[column] = pd.to_numeric(history[column], errors="coerce")

    return (
        history.loc[:, PRICE_COLUMNS]
        .dropna(subset=["Date", "Close"])
        .sort_values("Date")
        .reset_index(drop=True)
    )


@dataclass(frozen=True)
class YFinanceProvider(StubProvider):
    """yfinance-backed provider for daily stock price history.

    Only stock prices are implemented. Other provider methods inherit the
    explicit NotImplementedError behavior from StubProvider.
    """

    ticker_aliases: dict[str, str] = field(default_factory=dict)
    start: str | None = "2000-01-01"
    end: str | None = None
    period: str | None = None
    interval: str = "1d"

    name = "yfinance"

    def resolve_ticker(self, ticker: str) -> str:
        requested = ticker.upper()
        return self.ticker_aliases.get(requested, requested)

    def fetch_price_history(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        period: str | None = None,
    ) -> ProviderResult:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise ImportError(
                "The YFinanceProvider requires the optional 'yfinance' package. "
                "Install it in the project environment before running this ingestion."
            ) from exc

        source_ticker = self.resolve_ticker(ticker)
        download_kwargs = {
            "tickers": source_ticker,
            "interval": self.interval,
            "auto_adjust": False,
            "actions": True,
            "progress": False,
            "threads": False,
            "group_by": "column",
        }
        requested_period = self.period if period is None else period
        requested_start = self.start if start is None else start
        requested_end = self.end if end is None else end

        if requested_period is not None:
            download_kwargs["period"] = requested_period
        else:
            download_kwargs["start"] = requested_start
            download_kwargs["end"] = requested_end

        raw_history = yf.download(**download_kwargs)
        formatted = format_yfinance_price_history(raw_history, source_ticker)

        source = f"yfinance:{source_ticker}"
        if source_ticker != ticker.upper():
            source = f"{source} requested_as:{ticker.upper()}"

        return ProviderResult(
            data=formatted,
            source=source,
            fetched_at_utc=datetime.now(timezone.utc).isoformat(),
        )
