from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from shipping_dataingestion.providers import MarketDataProvider
from shipping_dataingestion.schemas import DATASET_SPECS, DatasetSpec, company_dataset_specs
from shipping_dataingestion.transforms import transform_provider_result
from shipping_dataingestion.writers import write_dataset


@dataclass(frozen=True)
class IngestionOutcome:
    dataset: str
    target_path: Path
    source: str
    written: bool
    skipped: bool = False
    message: str = ""
    rows: int | None = None


def _latest_price_date(path: Path) -> pd.Timestamp | None:
    if not path.exists():
        return None

    prices = pd.read_csv(path, usecols=lambda column: column == "Date")
    if "Date" not in prices.columns:
        return None

    dates = pd.to_datetime(prices["Date"], errors="coerce").dropna()
    if dates.empty:
        return None

    return pd.Timestamp(dates.max()).normalize()


def _merge_price_history(existing_path: Path, new_prices: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if existing_path.exists():
        frames.append(pd.read_csv(existing_path))
    frames.append(new_prices)

    combined = pd.concat(frames, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce").dt.normalize()
    combined = combined.dropna(subset=["Date", "Close"])
    return (
        combined.sort_values("Date")
        .drop_duplicates(subset=["Date"], keep="last")
        .reset_index(drop=True)
    )


class IngestionPipeline:
    """Coordinates provider fetches, transformations, validation, and writes."""

    def __init__(self, provider: MarketDataProvider) -> None:
        self.provider = provider

    def target_catalog(self, tickers: list[str] | None = None) -> dict[str, DatasetSpec]:
        catalog = dict(DATASET_SPECS)
        for ticker in tickers or []:
            for key, spec in company_dataset_specs(ticker).items():
                catalog[f"{ticker.upper()}_{key}"] = spec
        return catalog

    def describe_targets(self, tickers: list[str] | None = None) -> list[dict[str, str]]:
        """Show what each data type should become in the Data folder."""
        descriptions = []
        for key, spec in self.target_catalog(tickers).items():
            descriptions.append(
                {
                    "dataset_key": key,
                    "target_path": str(spec.target_path),
                    "file_type": spec.file_type,
                    "provider_method": spec.provider_method or "",
                    "description": spec.description,
                    "example_source": spec.example_source or "",
                }
            )
        return descriptions

    def ingest_dataset(
        self,
        spec: DatasetSpec,
        ticker: str | None = None,
        dry_run: bool = True,
    ) -> IngestionOutcome:
        if spec.provider_method is None:
            raise ValueError(f"{spec.name} does not define a provider method.")

        provider_method = getattr(self.provider, spec.provider_method)
        if ticker is None:
            result = provider_method()
        else:
            result = provider_method(ticker)

        transformed = transform_provider_result(result, spec)
        if not dry_run:
            write_dataset(transformed, spec)

        return IngestionOutcome(
            dataset=spec.name,
            target_path=spec.target_path,
            source=result.source,
            written=not dry_run,
        )

    def ingest_shared_dataset(self, dataset_key: str, dry_run: bool = True) -> IngestionOutcome:
        return self.ingest_dataset(DATASET_SPECS[dataset_key], dry_run=dry_run)

    def ingest_company_dataset(
        self,
        ticker: str,
        dataset_key: str,
        dry_run: bool = True,
    ) -> IngestionOutcome:
        specs = company_dataset_specs(ticker)
        return self.ingest_dataset(specs[dataset_key], ticker=ticker.upper(), dry_run=dry_run)

    def refresh_company_prices(
        self,
        ticker: str,
        *,
        full_refresh: bool = False,
        dry_run: bool = False,
        max_age_days: int = 1,
        overlap_days: int = 7,
        full_start: str = "2000-01-01",
    ) -> IngestionOutcome:
        """Refresh one {TICKER}_Prices.csv file.

        Incremental refresh reads the local file, skips recent files, and only
        fetches from latest local date minus overlap_days when stale.
        """
        ticker = ticker.upper()
        spec = company_dataset_specs(ticker)["company_prices"]
        latest_local_date = None if full_refresh else _latest_price_date(spec.target_path)

        today = pd.Timestamp.today().normalize()
        if latest_local_date is not None:
            stale_before = today - pd.Timedelta(days=max_age_days)
            if latest_local_date >= stale_before:
                return IngestionOutcome(
                    dataset=spec.name,
                    target_path=spec.target_path,
                    source="local-file",
                    written=False,
                    skipped=True,
                    message=f"{ticker} prices are current through {latest_local_date:%Y-%m-%d}.",
                )

        fetch_start = full_start
        if latest_local_date is not None:
            fetch_start = (latest_local_date - pd.Timedelta(days=overlap_days)).strftime("%Y-%m-%d")

        provider_method = getattr(self.provider, "fetch_price_history")
        result = provider_method(ticker, start=fetch_start)
        transformed = transform_provider_result(result, spec)
        if not isinstance(transformed, pd.DataFrame):
            raise TypeError("Price refresh expected a DataFrame provider result.")

        output = transformed if full_refresh or latest_local_date is None else _merge_price_history(
            spec.target_path,
            transformed,
        )

        if not dry_run:
            write_dataset(output, spec)

        action = "fully refreshed" if full_refresh or latest_local_date is None else "incrementally refreshed"
        return IngestionOutcome(
            dataset=spec.name,
            target_path=spec.target_path,
            source=result.source,
            written=not dry_run,
            skipped=False,
            message=f"{ticker} prices {action} from {fetch_start}.",
            rows=len(output),
        )
