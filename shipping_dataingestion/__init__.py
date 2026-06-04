"""Data ingestion scaffolding for the shipping app.

The package is intentionally API-neutral. Provider classes will later fetch
data from Bloomberg, yfinance, Clarksons, internal files, or other sources,
while the dataset contracts here keep outputs compatible with the existing
Data folder.
"""

from shipping_dataingestion.pipeline import IngestionPipeline
from shipping_dataingestion.providers import StubProvider
from shipping_dataingestion.schemas import DATASET_SPECS, DatasetSpec
from shipping_dataingestion.yfinance_provider import YFinanceProvider

__all__ = [
    "DATASET_SPECS",
    "DatasetSpec",
    "IngestionPipeline",
    "StubProvider",
    "YFinanceProvider",
]
