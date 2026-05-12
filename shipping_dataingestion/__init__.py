"""Data ingestion scaffolding for the shipping app.

The package is intentionally API-neutral. Provider classes will later fetch
data from Bloomberg, yfinance, Clarksons, internal files, or other sources,
while the dataset contracts here keep outputs compatible with the existing
Data folder.
"""

from dataingestion.pipeline import IngestionPipeline
from dataingestion.providers import StubProvider
from dataingestion.schemas import DATASET_SPECS, DatasetSpec
from dataingestion.yfinance_provider import YFinanceProvider

__all__ = [
    "DATASET_SPECS",
    "DatasetSpec",
    "IngestionPipeline",
    "StubProvider",
    "YFinanceProvider",
]
