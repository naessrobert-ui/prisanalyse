from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CompanyProfile:
    name: str
    ticker: str
    fleet_size: int
    description: str
    segment: str | None = None
    market_cap_usd_bn: float | None = None
    net_asset_value_usd_bn: float | None = None


BASE_DIR = Path(__file__).resolve().parents[0]
COMPANY_PROFILES_PATH = BASE_DIR / "Data" / "company_profiles.json"
DATA_DIR = BASE_DIR / "Data"
SPOT_RATE_SCRIPT_PATH = BASE_DIR.parent / "shipping_datahandling" / "_1_Datahandling_Spot_Rate_Pred.py"
SPOT_RATE_WORKBOOK_PATH = (
    BASE_DIR / "Output" / "Spot_Rate_Pred" / "spot_rate_predictions_from_2015.xlsx"
)
SPOT_RATE_METADATA_PATH = (
    BASE_DIR / "Output" / "Spot_Rate_Pred" / "spot_rate_predictions_from_2015.meta.json"
)
FINANCIALS_TEMPLATE_METRICS = [
    "ReportmentDate",
    "SharesOutstanding",
    "Cash",
    "Capex",
    "G&A",
    "Finance Income",
    "Finance Expenses",
    "AnnualizedFinanceExpenses",
    "DebtRepayment",
    "TotalDebt",
    "CurrentPortionLTDebt",
    "TaxRate",
    "EBITDA",
    "TotalCAPEX",
    "Revenue",
    "NetIncome",
    "DividendPayment",
]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _profile_from_dict(raw: dict[str, object]) -> CompanyProfile:
    return CompanyProfile(
        name=str(raw["name"]),
        ticker=str(raw["ticker"]),
        fleet_size=int(raw["fleet_size"]),
        description=str(raw["description"]),
        segment=_optional_str(raw.get("segment")),
        market_cap_usd_bn=_optional_float(raw.get("market_cap_usd_bn")),
        net_asset_value_usd_bn=_optional_float(raw.get("net_asset_value_usd_bn")),
    )


def _profile_to_dict(profile: CompanyProfile) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": profile.name,
        "ticker": profile.ticker,
        "fleet_size": profile.fleet_size,
        "description": profile.description,
    }
    if profile.segment is not None:
        payload["segment"] = profile.segment
    if profile.market_cap_usd_bn is not None:
        payload["market_cap_usd_bn"] = profile.market_cap_usd_bn
    if profile.net_asset_value_usd_bn is not None:
        payload["net_asset_value_usd_bn"] = profile.net_asset_value_usd_bn
    return payload


def _load_company_profiles_from_disk() -> dict[str, CompanyProfile]:
    if not COMPANY_PROFILES_PATH.exists():
        raise FileNotFoundError(f"Company profiles file not found: {COMPANY_PROFILES_PATH}")

    raw_profiles = json.loads(COMPANY_PROFILES_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw_profiles, list):
        raise ValueError(
            f"Company profiles file must contain a JSON list of profiles: {COMPANY_PROFILES_PATH}"
        )

    profiles: dict[str, CompanyProfile] = {}
    for raw in raw_profiles:
        profile = _profile_from_dict(raw)
        profiles[profile.name] = profile
    return profiles


def save_company_profiles(profiles: dict[str, CompanyProfile]) -> None:
    COMPANY_PROFILES_PATH.parent.mkdir(parents=True, exist_ok=True)
    serializable = [
        _profile_to_dict(profile)
        for _, profile in sorted(profiles.items(), key=lambda item: item[0].lower())
    ]
    COMPANY_PROFILES_PATH.write_text(
        json.dumps(serializable, indent=2),
        encoding="utf-8",
    )


def reload_company_profiles() -> dict[str, CompanyProfile]:
    global COMPANY_PROFILES
    COMPANY_PROFILES = _load_company_profiles_from_disk()
    return COMPANY_PROFILES


def _financials_workbook_path(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}_Financials.xlsx"


def ensure_financials_workbook_exists(ticker: str) -> Path:
    financials_path = _financials_workbook_path(ticker)
    if financials_path.exists():
        return financials_path

    financials_path.parent.mkdir(parents=True, exist_ok=True)
    template_df = pd.DataFrame({"Metric": FINANCIALS_TEMPLATE_METRICS})
    readme_df = pd.DataFrame(
        {
            "Note": [
                "Add quarterly financial periods as new columns.",
                "Keep the first column as metric names so the app can parse the workbook.",
            ]
        }
    )
    with pd.ExcelWriter(financials_path, engine="openpyxl", mode="w") as writer:
        template_df.to_excel(writer, sheet_name="Financials", index=False)
        readme_df.to_excel(writer, sheet_name="Readme", index=False)

    return financials_path


def add_company_profile(profile: CompanyProfile, *, overwrite: bool = False) -> CompanyProfile:
    profiles = reload_company_profiles().copy()
    if profile.name in profiles and not overwrite:
        raise ValueError(f"Company already exists: {profile.name}")

    profiles[profile.name] = profile
    save_company_profiles(profiles)
    ensure_financials_workbook_exists(profile.ticker)
    reload_company_profiles()
    return profile


def remove_company_profile(company_name: str) -> None:
    profiles = reload_company_profiles().copy()
    if company_name not in profiles:
        raise ValueError(f"Company does not exist: {company_name}")
    if len(profiles) <= 1:
        raise ValueError("Cannot remove the last remaining company.")

    del profiles[company_name]
    save_company_profiles(profiles)
    reload_company_profiles()


COMPANY_PROFILES = _load_company_profiles_from_disk()


def get_company_names() -> list[str]:
    return list(reload_company_profiles().keys())


def get_company_ticker(company):
    return get_company_profile(company).ticker

def get_company_profile(company: str) -> CompanyProfile:
    profiles = reload_company_profiles()
    return profiles[company]


def refresh_spot_rate_predictions() -> str:
    if not SPOT_RATE_SCRIPT_PATH.exists():
        raise FileNotFoundError(f"Spot-rate refresh script not found: {SPOT_RATE_SCRIPT_PATH}")

    result = subprocess.run(
        [sys.executable, str(SPOT_RATE_SCRIPT_PATH)],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "No stderr captured."
        raise RuntimeError(f"Spot-rate refresh failed:\n{stderr}")

    return result.stdout.strip()


def get_spot_rate_predictions() -> pd.DataFrame:
    refresh_spot_rate_predictions()

    if not SPOT_RATE_WORKBOOK_PATH.exists():
        raise FileNotFoundError(f"Spot-rate workbook not found: {SPOT_RATE_WORKBOOK_PATH}")

    workbook = pd.ExcelFile(SPOT_RATE_WORKBOOK_PATH)
    prediction_sheets = [
        sheet_name for sheet_name in workbook.sheet_names if sheet_name.endswith("_predictions")
    ]
    frames: list[pd.DataFrame] = []

    for sheet_name in prediction_sheets:
        frame = pd.read_excel(workbook, sheet_name=sheet_name)
        frame["forecast_month"] = pd.to_datetime(frame["forecast_month"], errors="coerce")
        frame["source_origin_month"] = pd.to_datetime(
            frame["source_origin_month"], errors="coerce"
        )
        frame["vessel_label"] = frame["vessel_type"].astype(str).str.upper()
        frame["horizon_label"] = (
            frame["display_horizon_months"].astype("Int64").astype(str) + "m"
        )
        frame["series_label"] = frame["vessel_label"] + " (" + frame["horizon_label"] + ")"
        frames.append(frame)

    predictions = pd.concat(frames, ignore_index=True).sort_values(
        ["display_horizon_months", "vessel_type", "forecast_month"]
    ).reset_index(drop=True)
    return predictions


def get_spot_rate_prediction_metadata() -> dict[str, object]:
    bundle = load_spot_rate_prediction_bundle()
    return bundle["metadata"]


def get_spot_rate_selected_models() -> pd.DataFrame:
    bundle = load_spot_rate_prediction_bundle()
    return bundle["selected_models"]


def load_spot_rate_prediction_bundle() -> dict[str, object]:
    refresh_spot_rate_predictions()

    if not SPOT_RATE_WORKBOOK_PATH.exists():
        raise FileNotFoundError(f"Spot-rate workbook not found: {SPOT_RATE_WORKBOOK_PATH}")

    workbook = pd.ExcelFile(SPOT_RATE_WORKBOOK_PATH)
    prediction_sheets = [
        sheet_name for sheet_name in workbook.sheet_names if sheet_name.endswith("_predictions")
    ]
    frames: list[pd.DataFrame] = []

    for sheet_name in prediction_sheets:
        frame = pd.read_excel(workbook, sheet_name=sheet_name)
        frame["forecast_month"] = pd.to_datetime(frame["forecast_month"], errors="coerce")
        frame["source_origin_month"] = pd.to_datetime(
            frame["source_origin_month"], errors="coerce"
        )
        frame["vessel_label"] = frame["vessel_type"].astype(str).str.upper()
        frame["horizon_label"] = (
            frame["display_horizon_months"].astype("Int64").astype(str) + "m"
        )
        frame["series_label"] = frame["vessel_label"] + " (" + frame["horizon_label"] + ")"
        frames.append(frame)

    predictions = pd.concat(frames, ignore_index=True).sort_values(
        ["display_horizon_months", "vessel_type", "forecast_month"]
    ).reset_index(drop=True)

    selected_models = pd.read_excel(workbook, sheet_name="selected_models").sort_values(
        ["forecast_horizon_months", "vessel_type"]
    ).reset_index(drop=True)

    metadata: dict[str, object] = {}
    if SPOT_RATE_METADATA_PATH.exists():
        metadata = json.loads(SPOT_RATE_METADATA_PATH.read_text(encoding="utf-8"))

    return {
        "predictions": predictions,
        "selected_models": selected_models,
        "metadata": metadata,
    }


def get_fleet_data(company: str) -> pd.DataFrame:
    fleet_map = {
        "Frontline": pd.DataFrame(
            [
                {"vessel_name": "Front Eagle", "segment": "VLCC", "age": 6, "capacity_dwt": 320000, "status": "Spot"},
                {"vessel_name": "Front Orion", "segment": "Suezmax", "age": 8, "capacity_dwt": 160000, "status": "Spot"},
                {"vessel_name": "Front Nova", "segment": "Aframax", "age": 4, "capacity_dwt": 115000, "status": "Time Charter"},
                {"vessel_name": "Front Quest", "segment": "VLCC", "age": 9, "capacity_dwt": 300000, "status": "Spot"},
            ]
        ),
    }
    return fleet_map[company]


def get_company_overview_data(company: str) -> dict[str, object]:
    overview_map = {
        "Frontline": {
            "share_price": 21.4,
            "nav_discount_pct": -12.7,
            "spot_exposure_pct": 78,
            "dividend_yield_pct": 9.5,
            "investment_view": "Equity looks highly geared to tanker spot rates and fleet values.",
        },
    }
    return overview_map[company]


def get_valuation_bridge(company: str) -> pd.DataFrame:
    valuation_map = {
        "Frontline": pd.DataFrame(
            [
                {"component": "Fleet market value", "value_usd_bn": 6.6},
                {"component": "Other assets", "value_usd_bn": 0.4},
                {"component": "Net debt", "value_usd_bn": -1.5},
                {"component": "Equity NAV", "value_usd_bn": 5.5},
            ]
        ),
    }
    return valuation_map[company]


def get_implicit_rate_inputs(company: str) -> dict[str, float]:
    inputs_map = {
        "Frontline": {
            "share_price_usd": 21.4,
            "shares_outstanding_m": 223.0,
            "net_debt_usd_bn": 1.5,
            "fleet_value_usd_bn": 6.6,
            "opex_per_day": 10500,
            "breakeven_rate": 23500,
            "implied_rate": 38100,
        },
    }
    return inputs_map[company]


def get_trading_signal_data(company: str) -> dict[str, object]:
    signal_map = {
        "Frontline": {
            "predicted_spot_rate": 44500,
            "implicit_rate": 38100,
            "signal_strength": 6400,
            "signal_label": "Long bias",
            "rationale": (
                "The forecasted spot market is above the equity-implied rate, suggesting the stock may not "
                "fully reflect the freight environment."
            ),
        },
    }
    return signal_map[company]
