# Data ingestion

This folder is the bridge between future API providers and the app's current `Data/` files.

The existing pages and `datahandling/` modules already expect concrete files such as `Data/FRO_Prices.csv`, `Data/FRO_Fleet.xlsx`, `Data/monthly_tce_history.csv`, and `Data/Ship_Values.xlsx`. The ingestion layer should therefore not introduce a new storage format yet. Its job is:

1. Fetch raw data from a provider.
2. Transform the raw provider shape into the existing app schema.
3. Validate required columns and workbook sheets.
4. Write the result into `Data/` using the same filenames the app already reads.

## Folder map

- `schemas.py`: dataset contracts, target paths, required columns, expected sheets, and likely source systems.
- `providers.py`: provider interface plus `StubProvider`, where future Bloomberg/yfinance/etc. implementations should plug in.
- `yfinance_provider.py`: yfinance implementation for stock-price history.
- `transforms.py`: normalization and validation before anything is written.
- `writers.py`: CSV/XLSX writers for the current `Data/` format.
- `pipeline.py`: orchestration helpers for describing targets and running one dataset ingestion.

## Data-type targets

Shared datasets:

- `monthly_tce_history` -> `Data/monthly_tce_history.csv`
  - Columns: `month`, `vessel_type`, `tce`
  - Used by: spot-rate prediction model.
  - Future provider hook: `fetch_monthly_tce_history()`

- `shipping_index` -> `Data/Shipping_Index.csv`
  - Columns: `Date`, `Adj Close`, `Close`, `High`, `Low`, `Open`, `Volume`
  - Future provider hook: `fetch_shipping_index()`

- `spot_rates` -> `Data/Spot_Rates.xlsx`
  - Sheet: `Sheet1`
  - Columns: `Date`, `Dagrater`
  - Future provider hook: `fetch_spot_rates()`

- `ship_values` -> `Data/Ship_Values.xlsx`
  - Sheet: `FleetValues`
  - Contains vessel value columns for VLCC, Suezmax, Aframax/LR2, LR1, and MR.
  - Used by: implicit-rate model.
  - Future provider hook: `fetch_vessel_values()`

- `capex_assumptions` -> `Data/Capex_Assumptions.xlsx`
  - Sheet: `Sheet1`
  - Columns: `Date`, `VLCC`, `Suezmax`, `Aframax`, `MR`
  - Future provider hook: `fetch_capex_assumptions()`

- `opex_assumptions` -> `Data/Opex_Assumptions.xlsx`
  - Sheet: `Opex`
  - Contains vessel-specific OPEX columns.
  - Future provider hook: `fetch_opex_assumptions()`

- `gna_assumptions` -> `Data/GnA_Assumptions.csv`
  - Columns: `Date`, `VLCC`, `Suezmax`, `Aframax/LR2`, `MR`
  - Future provider hook: `fetch_gna_assumptions()`

Company datasets:

- `company_prices` -> `Data/{TICKER}_Prices.csv`
  - Columns: `Date`, `Adj Close`, `Close`, `Dividends`, `High`, `Low`, `Open`, `Stock Splits`, `Volume`
  - Future provider hook: `fetch_price_history(ticker)`

- `company_financials` -> `Data/{TICKER}_Financials.xlsx`
  - Sheet: `Sheet1`
  - Existing app expects financial metrics as rows and reporting periods as columns.
  - Future provider hook: `fetch_financials(ticker)`

- `company_fleet` -> `Data/{TICKER}_Fleet.xlsx`
  - Sheets: `Master`, `OwnershipPeriods`
  - Future provider hook: `fetch_fleet(ticker)`

## Example usage

Describe all targets without calling any API:

```python
from shipping_dataingestion import IngestionPipeline, StubProvider

pipeline = IngestionPipeline(StubProvider())
targets = pipeline.describe_targets(tickers=["FRO"])
for target in targets:
    print(target["dataset_key"], "->", target["target_path"])
```

Later, when a provider exists:

```python
from shipping_dataingestion.pipeline import IngestionPipeline

provider = YourYFinanceOrBloombergProvider()
pipeline = IngestionPipeline(provider)

# dry_run=True validates transformed data without overwriting Data/FRO_Prices.csv.
pipeline.ingest_company_dataset("FRO", "company_prices", dry_run=True)

# dry_run=False writes the current app-compatible output.
pipeline.ingest_company_dataset("FRO", "company_prices", dry_run=False)
```

Using the included yfinance provider for stock prices:

```python
from shipping_dataingestion import IngestionPipeline, YFinanceProvider

provider = YFinanceProvider()
pipeline = IngestionPipeline(provider)

# Writes Data/FRO_Prices.csv from the listed FRO ticker.
pipeline.ingest_company_dataset("FRO", "company_prices", dry_run=False)
```

For app use, prefer incremental refresh:

```python
pipeline.refresh_company_prices("FRO")
```

This reads the existing local CSV, skips the API call when the file is recent,
and otherwise fetches from the latest local date minus a 7-day overlap. If the
file is missing, it fetches from `2000-01-01`.

`YFinanceProvider` only implements `fetch_price_history(ticker)` for now. Other datasets still raise `NotImplementedError` until their providers are added.

## Adding a real provider

Create a new provider class that implements one or more methods from `MarketDataProvider`.

```python
from datetime import datetime, timezone

import pandas as pd

from shipping_dataingestion.providers import ProviderResult


class ExamplePriceProvider:
    name = "example-price-provider"

    def fetch_price_history(self, ticker: str) -> ProviderResult:
        # Replace this with yfinance, Bloomberg, or another source later.
        raw = pd.DataFrame(
            columns=[
                "Date",
                "Adj Close",
                "Close",
                "Dividends",
                "High",
                "Low",
                "Open",
                "Stock Splits",
                "Volume",
            ]
        )
        return ProviderResult(
            data=raw,
            source=f"{self.name}:{ticker}",
            fetched_at_utc=datetime.now(timezone.utc).isoformat(),
        )
```

The expected end result is still a normal file in `Data/`, for example `Data/FRO_Prices.csv`, so the existing Streamlit pages do not need to care where the data came from.
