from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from shipping_dataingestion.paths import data_path


@dataclass(frozen=True)
class DatasetSpec:
    """Contract for one output that the existing app already knows how to read."""

    name: str
    target_path: Path
    file_type: str
    description: str
    required_columns: tuple[str, ...] = ()
    sheets: dict[str, tuple[str, ...]] | None = None
    provider_method: str | None = None
    example_source: str | None = None


PRICE_COLUMNS = (
    "Date",
    "Adj Close",
    "Close",
    "Dividends",
    "High",
    "Low",
    "Open",
    "Stock Splits",
    "Volume",
)

FLEET_MASTER_COLUMNS = (
    "ID",
    "Vessel",
    "Built",
    "DWT",
    "Flag",
    "VesselType",
    "Owned",
    "CharteredFrom",
    "Construction",
    "Yard",
    "BuilderCountry",
    "Class",
    "Notes",
)

FLEET_OWNERSHIP_COLUMNS = (
    "ID",
    "Vessel",
    "VesselType",
    "Owned",
    "StartDate",
    "EndDate",
    "Employment",
    "TimeCharterRate",
)

VESSEL_TYPES = ("VLCC", "Suezmax", "Aframax/LR2", "MR")


def company_dataset_specs(ticker: str) -> dict[str, DatasetSpec]:
    """Return ticker-specific contracts such as prices, financials, and fleet."""
    ticker = ticker.upper()
    return {
        "company_prices": DatasetSpec(
            name="company_prices",
            target_path=data_path(f"{ticker}_Prices.csv"),
            file_type="csv",
            description="Daily OHLCV stock price history used by company overview, implicit rate, and trading model.",
            required_columns=PRICE_COLUMNS,
            provider_method="fetch_price_history",
            example_source="yfinance, Bloomberg PX_LAST/PX_OPEN/PX_HIGH/PX_LOW/VOLUME",
        ),
        "company_financials": DatasetSpec(
            name="company_financials",
            target_path=data_path(f"{ticker}_Financials.xlsx"),
            file_type="xlsx",
            description="Quarterly financial statement workbook. Existing app expects metrics as rows and quarters as columns.",
            sheets={
                "Sheet1": (
                    "Unnamed: 0",
                    "Q1-2025",
                    "Q2-2025",
                )
            },
            provider_method="fetch_financials",
            example_source="Bloomberg fundamentals, company filings, manually maintained model export",
        ),
        "company_fleet": DatasetSpec(
            name="company_fleet",
            target_path=data_path(f"{ticker}_Fleet.xlsx"),
            file_type="xlsx",
            description="Company fleet workbook with vessel master data and ownership periods.",
            sheets={
                "Master": FLEET_MASTER_COLUMNS,
                "OwnershipPeriods": FLEET_OWNERSHIP_COLUMNS,
            },
            provider_method="fetch_fleet",
            example_source="Company fleet list, VesselsValue, Clarksons, manual analyst input",
        ),
    }


DATASET_SPECS: dict[str, DatasetSpec] = {
    "monthly_tce_history": DatasetSpec(
        name="monthly_tce_history",
        target_path=data_path("monthly_tce_history.csv"),
        file_type="csv",
        description="Monthly tanker TCE history used as the input to spot-rate prediction models.",
        required_columns=("month", "vessel_type", "tce"),
        provider_method="fetch_monthly_tce_history",
        example_source="Clarksons/Baltic Exchange/Bloomberg shipping rate series",
    ),
    "shipping_index": DatasetSpec(
        name="shipping_index",
        target_path=data_path("Shipping_Index.csv"),
        file_type="csv",
        description="Daily shipping equity/index price history.",
        required_columns=("Date", "Adj Close", "Close", "High", "Low", "Open", "Volume"),
        provider_method="fetch_shipping_index",
        example_source="yfinance, Bloomberg index history",
    ),
    "spot_rates": DatasetSpec(
        name="spot_rates",
        target_path=data_path("Spot_Rates.xlsx"),
        file_type="xlsx",
        description="Spot-rate workbook used by exploratory pages and legacy notebooks.",
        sheets={"Sheet1": ("Date", "Dagrater")},
        provider_method="fetch_spot_rates",
        example_source="Broker quotes, Baltic Exchange, Clarksons",
    ),
    "ship_values": DatasetSpec(
        name="ship_values",
        target_path=data_path("Ship_Values.xlsx"),
        file_type="xlsx",
        description="Historical vessel value curves used by implicit-rate calculations.",
        sheets={
            "FleetValues": (
                "Date",
                "Vessel Price for Current VLCC",
                "Vessel Price for 5 Year Old VL",
                "Vessel Price for 15 Year Old V",
                "Vessel Price for Current Suezm",
                "Vessel Price for 5 Year Old Su",
                "Vessel Price for 15 Year Old S",
                "Vessel Price for Current Afram",
                "Vessel Price for 5 Year Old Af",
                "Vessel Price for 15 Year Old A",
                "Vessel Price for Current LR1 T",
                "Vessel Price for 5 Year Old LR",
                "Vessel Price for 15 Year Old L",
                "Vessel Price for Current MR Ta",
                "Vessel Price for 5 Year Old MR",
                "Vessel Price for 15 Year Old M",
            )
        },
        provider_method="fetch_vessel_values",
        example_source="VesselsValue, Clarksons, broker valuation series",
    ),
    "capex_assumptions": DatasetSpec(
        name="capex_assumptions",
        target_path=data_path("Capex_Assumptions.xlsx"),
        file_type="xlsx",
        description="Newbuild or capex assumptions by vessel type.",
        sheets={"Sheet1": ("Date", "VLCC", "Suezmax", "Aframax", "MR")},
        provider_method="fetch_capex_assumptions",
        example_source="Analyst model, shipyard quotes, broker reports",
    ),
    "opex_assumptions": DatasetSpec(
        name="opex_assumptions",
        target_path=data_path("Opex_Assumptions.xlsx"),
        file_type="xlsx",
        description="Operating cost assumptions by vessel segment.",
        sheets={
            "Opex": (
                "Date",
                "VLCC Operating Costs (OPEX)",
                "Suezmax Operating Costs (OPEX)",
                "Aframax/LR2 Operating Costs (OPEX)",
                "MR/Handy Tanker Operating Costs (OPEX)",
            )
        },
        provider_method="fetch_opex_assumptions",
        example_source="Analyst model, company disclosures, broker reports",
    ),
    "gna_assumptions": DatasetSpec(
        name="gna_assumptions",
        target_path=data_path("GnA_Assumptions.csv"),
        file_type="csv",
        description="G&A assumptions by vessel type.",
        required_columns=("Date", *VESSEL_TYPES),
        provider_method="fetch_gna_assumptions",
        example_source="Analyst model, company disclosures",
    ),
}
