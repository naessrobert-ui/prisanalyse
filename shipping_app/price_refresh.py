from __future__ import annotations

from dataclasses import dataclass

from dataingestion import IngestionPipeline, YFinanceProvider
from dataingestion.pipeline import IngestionOutcome
from shipping_app.data import get_company_names, get_company_profile


@dataclass(frozen=True)
class PriceRefreshSummary:
    outcomes: list[IngestionOutcome]

    @property
    def refreshed_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.written and not outcome.skipped)

    @property
    def skipped_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.skipped)

    def status_text(self) -> str:
        if not self.outcomes:
            return "No companies found for price refresh."

        parts = []
        if self.refreshed_count:
            parts.append(f"{self.refreshed_count} refreshed")
        if self.skipped_count:
            parts.append(f"{self.skipped_count} already current")
        return ", ".join(parts) if parts else "No price files changed."


def company_price_tickers() -> list[str]:
    return [get_company_profile(company).ticker for company in get_company_names()]


def refresh_all_company_prices(
    *,
    full_refresh: bool = False,
    max_age_days: int = 1,
    overlap_days: int = 7,
) -> PriceRefreshSummary:
    pipeline = IngestionPipeline(YFinanceProvider())
    outcomes = [
        pipeline.refresh_company_prices(
            ticker,
            full_refresh=full_refresh,
            max_age_days=max_age_days,
            overlap_days=overlap_days,
        )
        for ticker in company_price_tickers()
    ]
    return PriceRefreshSummary(outcomes=outcomes)
