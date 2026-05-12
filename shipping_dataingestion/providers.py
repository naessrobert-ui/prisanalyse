from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class ProviderResult:
    """Raw provider payload plus lightweight provenance."""

    data: pd.DataFrame | dict[str, pd.DataFrame]
    source: str
    fetched_at_utc: str | None = None


class MarketDataProvider(Protocol):
    """Interface future API-specific providers should implement."""

    name: str

    def fetch_price_history(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        period: str | None = None,
    ) -> ProviderResult:
        ...

    def fetch_financials(self, ticker: str) -> ProviderResult:
        ...

    def fetch_fleet(self, ticker: str) -> ProviderResult:
        ...

    def fetch_monthly_tce_history(self) -> ProviderResult:
        ...

    def fetch_shipping_index(self) -> ProviderResult:
        ...

    def fetch_spot_rates(self) -> ProviderResult:
        ...

    def fetch_vessel_values(self) -> ProviderResult:
        ...

    def fetch_capex_assumptions(self) -> ProviderResult:
        ...

    def fetch_opex_assumptions(self) -> ProviderResult:
        ...

    def fetch_gna_assumptions(self) -> ProviderResult:
        ...


class StubProvider:
    """Provider placeholder.

    Replace one method at a time when an API is chosen. For example, a future
    YFinanceProvider can implement fetch_price_history while the rest still
    raise NotImplementedError.
    """

    name = "stub"

    def _not_implemented(self, method_name: str) -> ProviderResult:
        raise NotImplementedError(
            f"{method_name} is not wired to a real API yet. "
            "Implement it in a provider class and return ProviderResult."
        )

    def fetch_price_history(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        period: str | None = None,
    ) -> ProviderResult:
        return self._not_implemented("fetch_price_history")

    def fetch_financials(self, ticker: str) -> ProviderResult:
        return self._not_implemented("fetch_financials")

    def fetch_fleet(self, ticker: str) -> ProviderResult:
        return self._not_implemented("fetch_fleet")

    def fetch_monthly_tce_history(self) -> ProviderResult:
        return self._not_implemented("fetch_monthly_tce_history")

    def fetch_shipping_index(self) -> ProviderResult:
        return self._not_implemented("fetch_shipping_index")

    def fetch_spot_rates(self) -> ProviderResult:
        return self._not_implemented("fetch_spot_rates")

    def fetch_vessel_values(self) -> ProviderResult:
        return self._not_implemented("fetch_vessel_values")

    def fetch_capex_assumptions(self) -> ProviderResult:
        return self._not_implemented("fetch_capex_assumptions")

    def fetch_opex_assumptions(self) -> ProviderResult:
        return self._not_implemented("fetch_opex_assumptions")

    def fetch_gna_assumptions(self) -> ProviderResult:
        return self._not_implemented("fetch_gna_assumptions")
