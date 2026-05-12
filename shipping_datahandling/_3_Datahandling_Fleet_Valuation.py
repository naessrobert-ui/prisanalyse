from __future__ import annotations

from pathlib import Path
import re

import numpy as np
import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Data"

FLEET_VALUES_FILE = DATA_DIR / "FleetValues.xlsx"

TYPE_ALIASES = {
    "VLCC": "VLCC",
    "SUEZMAX": "Suezmax",
    "AFRAMAX": "Aframax/LR2",
    "AFRAMAX/LR2": "Aframax/LR2",
    "LR2": "Aframax/LR2",
    "LR1": "LR1",
    "MR": "MR",
}

PRICE_COLUMN_MAPPING = {
    "VLCC": (
        "Vessel Price for Current VLCC",
        "Vessel Price for 5 Year Old VL",
        "Vessel Price for 15 Year Old V",
    ),
    "Suezmax": (
        "Vessel Price for Current Suezm",
        "Vessel Price for 5 Year Old Su",
        "Vessel Price for 15 Year Old S",
    ),
    "Aframax/LR2": (
        "Vessel Price for Current Afram",
        "Vessel Price for 5 Year Old Af",
        "Vessel Price for 15 Year Old A",
    ),
    "LR1": (
        "Vessel Price for Current LR1 T",
        "Vessel Price for 5 Year Old LR",
        "Vessel Price for 15 Year Old L",
    ),
    "MR": (
        "Vessel Price for Current MR Ta",
        "Vessel Price for 5 Year Old MR",
        "Vessel Price for 15 Year Old M",
    ),
}


def get_fleet_file(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}_Fleet.xlsx"


def clear_fleet_cache() -> None:
    load_current_owned_fleet.clear()
    load_fleet_master.clear()
    load_current_owned_vessels.clear()
    build_fleet_table.clear()
    load_fleet_values.clear()
    load_financials_for_nav.clear()


def _read_fleet_workbook(ticker: str) -> dict[str, pd.DataFrame]:
    fleet_file = get_fleet_file(ticker)
    if not fleet_file.exists():
        raise FileNotFoundError(f"Fleet file not found: {fleet_file}")

    workbook = pd.ExcelFile(fleet_file)
    return {
        sheet_name: pd.read_excel(fleet_file, sheet_name=sheet_name)
        for sheet_name in workbook.sheet_names
    }


def _write_fleet_workbook(ticker: str, sheets: dict[str, pd.DataFrame]) -> None:
    fleet_file = get_fleet_file(ticker)
    with pd.ExcelWriter(fleet_file, engine="openpyxl", mode="w") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)

    clear_fleet_cache()


def _build_empty_fleet_workbook() -> dict[str, pd.DataFrame]:
    master_columns = [
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
    ]
    ownership_columns = [
        "ID",
        "Vessel",
        "VesselType",
        "Owned",
        "StartDate",
        "EndDate",
        "Employment",
        "TimeCharterRate",
    ]
    return {
        "Master": pd.DataFrame(columns=master_columns),
        "OwnershipPeriods": pd.DataFrame(columns=ownership_columns),
        "Readme": pd.DataFrame(),
    }


def ensure_fleet_workbook_exists(ticker: str) -> Path:
    fleet_file = get_fleet_file(ticker)
    if fleet_file.exists():
        return fleet_file

    fleet_file.parent.mkdir(parents=True, exist_ok=True)
    _write_fleet_workbook(ticker, _build_empty_fleet_workbook())
    return fleet_file


def _next_vessel_id(
    ticker: str,
    vessel: str,
    vessel_type: str,
    built: int,
    existing_ids: pd.Series,
) -> str:
    type_prefix = "".join(word[0] for word in re.findall(r"[A-Za-z0-9]+", vessel_type))
    vessel_slug = re.sub(r"[^A-Z0-9]", "", vessel.upper())[:6] or "VESSEL"
    base_id = f"{type_prefix[:2].upper()}-{ticker.upper()}{vessel_slug}-{built}"

    used_ids = set(existing_ids.dropna().astype(str))
    candidate = base_id
    suffix = 1
    while candidate in used_ids:
        suffix += 1
        candidate = f"{base_id}-{suffix}"

    return candidate


def _normalize_vessel_type(value: str | None) -> str | None:
    if value is None or pd.isna(value):
        return None

    cleaned = str(value).strip()
    upper = cleaned.upper().replace(" ", "")

    if "AFRAMAX" in upper or upper in {"LR2", "AFR/LR2", "AFRAMAXLR2"}:
        return "Aframax/LR2"
    if "SUEZ" in upper:
        return "Suezmax"
    if "VLCC" in upper:
        return "VLCC"
    if upper == "LR1":
        return "LR1"
    if upper in {"MR", "MRTANKER", "MRT"}:
        return "MR"

    return TYPE_ALIASES.get(cleaned.upper(), cleaned)


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y"}


def _merge_with_master(ownership_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df.empty:
        return ownership_df.copy()

    merged = ownership_df.merge(
        master_df[["ID", "Vessel", "Built", "DWT", "VesselType", "Flag"]],
        on="ID",
        how="left",
        suffixes=("", "_master"),
    )

    if "Vessel_master" in merged.columns:
        merged["Vessel"] = merged["Vessel"].fillna(merged["Vessel_master"])

    if "VesselType_master" in merged.columns:
        merged["VesselType"] = merged["VesselType"].fillna(merged["VesselType_master"])

    return merged


def add_ship_to_fleet(
    ticker: str,
    vessel: str,
    vessel_type: str,
    built: int,
    dwt: float | int | None,
    start_date,
    owned: bool = True,
    flag: str | None = None,
    employment: str | None = None,
    time_charter_rate: float | int | None = None,
) -> str:
    ensure_fleet_workbook_exists(ticker)
    sheets = _read_fleet_workbook(ticker)
    master_df = sheets.get("Master", pd.DataFrame()).copy()
    ownership_df = sheets.get("OwnershipPeriods", pd.DataFrame()).copy()

    vessel = str(vessel).strip()
    vessel_type = str(vessel_type).strip()
    if not vessel:
        raise ValueError("Ship name is required.")
    if not vessel_type:
        raise ValueError("Vessel type is required.")
    if pd.isna(built):
        raise ValueError("Built year is required.")

    built = int(built)
    dwt_value = np.nan if dwt is None or pd.isna(dwt) else float(dwt)
    start_timestamp = pd.to_datetime(start_date, errors="coerce")
    if pd.isna(start_timestamp):
        raise ValueError("Start date is required.")

    vessel_id = _next_vessel_id(
        ticker=ticker,
        vessel=vessel,
        vessel_type=vessel_type,
        built=built,
        existing_ids=master_df.get("ID", pd.Series(dtype=str)),
    )

    master_defaults = {
        "ID": vessel_id,
        "Vessel": vessel,
        "Built": built,
        "DWT": dwt_value,
        "Flag": (flag or "").strip() or np.nan,
        "VesselType": vessel_type,
        "Owned": bool(owned),
        "CharteredFrom": np.nan,
        "Construction": np.nan,
        "Yard": np.nan,
        "BuilderCountry": np.nan,
        "Class": np.nan,
        "Notes": np.nan,
    }
    for column in master_defaults:
        if column not in master_df.columns:
            master_df[column] = np.nan
    master_df = pd.concat(
        [
            master_df,
            pd.DataFrame(
                [{col: master_defaults.get(col, np.nan) for col in master_df.columns}]
            ),
        ],
        ignore_index=True,
    )

    ownership_defaults = {
        "ID": vessel_id,
        "Vessel": vessel,
        "VesselType": vessel_type,
        "Owned": bool(owned),
        "StartDate": start_timestamp,
        "EndDate": pd.NaT,
        "Employment": (employment or "").strip() or np.nan,
        "TimeCharterRate": (
            np.nan
            if time_charter_rate is None or pd.isna(time_charter_rate)
            else float(time_charter_rate)
        ),
    }
    for column in ownership_defaults:
        if column not in ownership_df.columns:
            ownership_df[column] = np.nan
    ownership_df = pd.concat(
        [
            ownership_df,
            pd.DataFrame(
                [{col: ownership_defaults.get(col, np.nan) for col in ownership_df.columns}]
            ),
        ],
        ignore_index=True,
    )

    sheets["Master"] = master_df
    sheets["OwnershipPeriods"] = ownership_df
    _write_fleet_workbook(ticker, sheets)

    return vessel_id


def end_ship_ownership(ticker: str, vessel_id: str, end_date) -> int:
    sheets = _read_fleet_workbook(ticker)
    ownership_df = sheets.get("OwnershipPeriods", pd.DataFrame()).copy()
    if ownership_df.empty or "ID" not in ownership_df.columns:
        raise ValueError("No ownership periods found.")

    ownership_df["StartDate"] = pd.to_datetime(
        ownership_df["StartDate"], dayfirst=True, errors="coerce"
    )
    ownership_df["EndDate"] = pd.to_datetime(
        ownership_df["EndDate"], dayfirst=True, errors="coerce"
    )

    end_timestamp = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(end_timestamp):
        raise ValueError("End date is required.")

    active_mask = (ownership_df["ID"].astype(str) == str(vessel_id)) & ownership_df[
        "EndDate"
    ].isna()
    if not active_mask.any():
        raise ValueError("Selected ship is not currently active.")

    invalid_dates = ownership_df.loc[active_mask, "StartDate"] > end_timestamp
    if invalid_dates.any():
        raise ValueError("End date cannot be before the ship's start date.")

    ownership_df.loc[active_mask, "EndDate"] = end_timestamp
    sheets["OwnershipPeriods"] = ownership_df
    _write_fleet_workbook(ticker, sheets)

    return int(active_mask.sum())


@st.cache_data
def load_current_owned_fleet(ticker: str) -> pd.DataFrame:
    fleet_file = get_fleet_file(ticker)
    if not fleet_file.exists():
        return pd.DataFrame()

    df = pd.read_excel(fleet_file, sheet_name="OwnershipPeriods")

    df["StartDate"] = pd.to_datetime(df["StartDate"], dayfirst=True, errors="coerce")
    df["EndDate"] = pd.to_datetime(df["EndDate"], dayfirst=True, errors="coerce")

    owned_mask = df.get("Owned", True).map(_coerce_bool) if "Owned" in df.columns else True
    current_owned = df[owned_mask & df["EndDate"].isna()].copy()
    current_owned = current_owned.dropna(subset=["VesselType"])
    current_owned["VesselType"] = current_owned["VesselType"].map(_normalize_vessel_type)

    return current_owned


@st.cache_data
def load_fleet_master(ticker: str) -> pd.DataFrame:
    fleet_file = get_fleet_file(ticker)
    if not fleet_file.exists():
        return pd.DataFrame()

    df = pd.read_excel(fleet_file, sheet_name="Master")

    for column in ["Built", "DWT"]:
        if column in df.columns:
            df[column] = df[column].astype(str).str.replace(",", "", regex=False)
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "VesselType" in df.columns:
        df["VesselType"] = df["VesselType"].map(_normalize_vessel_type)

    return df


@st.cache_data
def load_current_owned_vessels(ticker: str) -> pd.DataFrame:
    fleet_file = get_fleet_file(ticker)
    if not fleet_file.exists():
        return pd.DataFrame()

    ownership_df = pd.read_excel(fleet_file, sheet_name="OwnershipPeriods")
    master_df = load_fleet_master(ticker)

    ownership_df["StartDate"] = pd.to_datetime(
        ownership_df["StartDate"], dayfirst=True, errors="coerce"
    )
    ownership_df["EndDate"] = pd.to_datetime(
        ownership_df["EndDate"], dayfirst=True, errors="coerce"
    )
    if "VesselType" in ownership_df.columns:
        ownership_df["VesselType"] = ownership_df["VesselType"].map(_normalize_vessel_type)

    owned_mask = (
        ownership_df.get("Owned", True).map(_coerce_bool)
        if "Owned" in ownership_df.columns
        else True
    )
    current_owned = ownership_df[owned_mask & ownership_df["EndDate"].isna()].copy()

    merged = _merge_with_master(current_owned, master_df)
    merged["VesselType"] = merged["VesselType"].map(_normalize_vessel_type)
    return merged


@st.cache_data
def load_fleet_values() -> pd.DataFrame:
    if not FLEET_VALUES_FILE.exists():
        return pd.DataFrame()

    df = pd.read_excel(FLEET_VALUES_FILE, sheet_name="FleetValues")
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce").dt.normalize()
    df = df.dropna(subset=["Date"]).sort_values("Date")

    value_columns = [col for col in df.columns if col != "Date"]
    for column in value_columns:
        df[column] = df[column].astype(str).str.replace(",", "", regex=False)
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


@st.cache_data
def load_financials_for_nav(ticker: str) -> pd.DataFrame:
    financials_df, _ = _load_financials_for_nav_with_warnings(ticker)
    return financials_df


def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _standardize_financials_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    date_candidates = ["ReportmentDate", "Date", "Quarter", "QuarterEnd"]
    date_col = next((col for col in date_candidates if col in df.columns), None)

    fields = {
        "Cash": ["Cash", "CashAndCashEquivalents"],
        "TotalDebt": ["TotalDebt", "Debt", "GrossDebt"],
        "SharesOutstanding": ["SharesOutstanding", "ShareCount", "Shares"],
    }

    if date_col is None:
        warnings.append("Missing date column in financials data.")
        return pd.DataFrame(), warnings

    out = pd.DataFrame()
    out["Date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()

    missing_fields: list[str] = []
    for target, aliases in fields.items():
        source = next((col for col in aliases if col in df.columns), None)
        if source is None:
            missing_fields.append(target)
            out[target] = np.nan
            continue
        out[target] = _coerce_numeric_series(df[source])

    if missing_fields:
        warnings.append(f"Missing financial fields: {', '.join(missing_fields)}.")

    # Scaling assumption:
    # FRO financial workbook values are conventionally stored in thousands (USD and shares).
    # We scale to absolute units for NAV comparability with fleet values.
    for column in ["Cash", "TotalDebt", "SharesOutstanding"]:
        out[column] = out[column] * 1000

    out = out.dropna(subset=["Date"]).sort_values("Date")
    return out[["Date", "Cash", "TotalDebt", "SharesOutstanding"]], warnings


def _looks_transposed_financials(df: pd.DataFrame) -> bool:
    if df.empty or df.shape[1] < 2:
        return False

    first_col = df.columns[0]
    metric_names = (
        df[first_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .tolist()
    )
    required_markers = {"reportmentdate", "cash", "totaldebt", "sharesoutstanding"}
    return required_markers.issubset(set(metric_names))


def _load_financials_for_nav_with_warnings(ticker: str) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    path = DATA_DIR / f"{ticker}_Financials.xlsx"
    if not path.exists():
        return pd.DataFrame(), [f"Financial file not found: {path.name}."]

    df = pd.read_excel(path)
    if _looks_transposed_financials(df):
        first_col = df.columns[0]
        metrics = df[first_col].astype(str).str.strip()
        transposed = df.drop(columns=[first_col]).copy()
        transposed.index = metrics
        normalized_index = transposed.index.str.lower()
        transposed.index = normalized_index
        transposed = transposed.transpose().reset_index(drop=True)

        rename_map = {
            "reportmentdate": "ReportmentDate",
            "cash": "Cash",
            "totaldebt": "TotalDebt",
            "sharesoutstanding": "SharesOutstanding",
        }
        available_cols = {c: rename_map[c] for c in rename_map if c in transposed.columns}
        transposed = transposed.rename(columns=available_cols)
        standardized, local_warnings = _standardize_financials_frame(transposed)
        warnings.extend(local_warnings)
        return standardized, warnings

    standardized, local_warnings = _standardize_financials_frame(df)
    warnings.extend(local_warnings)
    return standardized, warnings


def get_vessel_type_counts(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    current_owned_df = load_current_owned_fleet(ticker)

    if current_owned_df.empty or "VesselType" not in current_owned_df.columns:
        return pd.DataFrame(), pd.DataFrame(columns=["VesselType", "Count"])

    vessel_type_counts = (
        current_owned_df["VesselType"]
        .value_counts()
        .rename_axis("VesselType")
        .reset_index(name="Count")
    )

    return current_owned_df, vessel_type_counts


def _interpolate_ship_value(
    age_years: float,
    price_0: float,
    price_5: float,
    price_15: float,
    economic_life: float,
    depreciation_curve_param: float,
    scrap_value: float = 0.0,
) -> float:
    if pd.isna(age_years) or pd.isna(price_0) or pd.isna(price_5) or pd.isna(price_15):
        return np.nan

    if age_years <= 0:
        return float(price_0)

    bounded_age = min(max(float(age_years), 0.0), float(economic_life))

    ages = np.array([0.0, 5.0, 15.0, float(economic_life)], dtype=float)
    prices = np.array([price_0, price_5, price_15, scrap_value], dtype=float)

    transformed_ages = np.power(ages / float(economic_life), float(depreciation_curve_param))
    transformed_query = np.power(
        bounded_age / float(economic_life), float(depreciation_curve_param)
    )

    return float(np.interp(transformed_query, transformed_ages, prices))


def _build_active_fleet(ticker: str) -> pd.DataFrame:
    fleet_file = get_fleet_file(ticker)
    if not fleet_file.exists():
        return pd.DataFrame()

    ownership_df = pd.read_excel(fleet_file, sheet_name="OwnershipPeriods")
    master_df = load_fleet_master(ticker)

    ownership_df["StartDate"] = pd.to_datetime(
        ownership_df["StartDate"], dayfirst=True, errors="coerce"
    ).dt.normalize()
    ownership_df["EndDate"] = pd.to_datetime(
        ownership_df["EndDate"], dayfirst=True, errors="coerce"
    ).dt.normalize()

    if "Owned" in ownership_df.columns:
        ownership_df = ownership_df[ownership_df["Owned"].map(_coerce_bool)]

    if "VesselType" in ownership_df.columns:
        ownership_df["VesselType"] = ownership_df["VesselType"].map(_normalize_vessel_type)

    merged = _merge_with_master(ownership_df, master_df)
    merged["VesselType"] = merged["VesselType"].map(_normalize_vessel_type)
    merged["Built"] = pd.to_numeric(merged.get("Built"), errors="coerce")

    return merged


def run_fleet_valuation_model(
    ticker: str,
    start_date,
    end_date,
    selected_vessel_types: list[str] | None = None,
    depreciation_curve_param: float = 0.65,
    economic_life: int = 20,
) -> dict[str, pd.DataFrame | list[str]]:
    warnings: list[str] = []

    fleet_values_df = load_fleet_values()
    fleet_df = _build_active_fleet(ticker)

    if fleet_df.empty:
        warnings.append(f"No fleet ownership data available for {ticker}.")
        empty = pd.DataFrame()
        return {
            "vessel_values": empty,
            "fleet_total": empty,
            "fleet_by_type": empty,
            "nav_bridge": empty,
            "warnings": warnings,
        }

    if fleet_values_df.empty:
        warnings.append("FleetValues.xlsx could not be loaded or has no valid dates.")
        empty = pd.DataFrame()
        return {
            "vessel_values": empty,
            "fleet_total": empty,
            "fleet_by_type": empty,
            "nav_bridge": empty,
            "warnings": warnings,
        }

    model_dates = fleet_values_df["Date"]

    if model_dates.empty:
        empty = pd.DataFrame()
        return {
            "vessel_values": empty,
            "fleet_total": empty,
            "fleet_by_type": empty,
            "nav_bridge": empty,
            "warnings": warnings,
        }

    if selected_vessel_types:
        fleet_df = fleet_df[fleet_df["VesselType"].isin(selected_vessel_types)]

    if fleet_df.empty:
        warnings.append("No vessels remain after applying the vessel type filter.")
        empty = pd.DataFrame()
        return {
            "vessel_values": empty,
            "fleet_total": empty,
            "fleet_by_type": empty,
            "nav_bridge": empty,
            "warnings": warnings,
        }

    fleet_values_indexed = fleet_values_df.set_index("Date")
    rows: list[dict] = []

    unknown_type_ids: set[str] = set()
    missing_anchor_ids: set[str] = set()
    missing_built_ids: set[str] = set()

    for valuation_date in model_dates:
        active = fleet_df.loc[
            (fleet_df["StartDate"] <= valuation_date)
            & (fleet_df["EndDate"].isna() | (fleet_df["EndDate"] >= valuation_date))
        ].copy()

        if active.empty:
            continue

        curve_row = fleet_values_indexed.loc[valuation_date]

        for _, vessel in active.iterrows():
            vessel_type = vessel.get("VesselType")
            built = vessel.get("Built")
            vessel_id = str(vessel.get("ID", ""))

            if vessel_type not in PRICE_COLUMN_MAPPING:
                unknown_type_ids.add(vessel_id)
                continue

            if pd.isna(built):
                missing_built_ids.add(vessel_id)
                continue

            col_0, col_5, col_15 = PRICE_COLUMN_MAPPING[vessel_type]
            price_0 = curve_row.get(col_0, np.nan)
            price_5 = curve_row.get(col_5, np.nan)
            price_15 = curve_row.get(col_15, np.nan)

            if pd.isna(price_0) or pd.isna(price_5) or pd.isna(price_15):
                missing_anchor_ids.add(vessel_id)
                continue

            age = (valuation_date - pd.Timestamp(int(built), 1, 1)).days / 365.25
            vessel_value = _interpolate_ship_value(
                age_years=age,
                price_0=price_0,
                price_5=price_5,
                price_15=price_15,
                economic_life=economic_life,
                depreciation_curve_param=depreciation_curve_param,
                scrap_value=0.0,
            )

            rows.append(
                {
                    "Date": valuation_date,
                    "ID": vessel.get("ID"),
                    "Vessel": vessel.get("Vessel"),
                    "VesselType": vessel_type,
                    "Built": built,
                    "Age": age,
                    "Value": vessel_value,
                    "AnchorCurrent": price_0,
                    "Anchor5Y": price_5,
                    "Anchor15Y": price_15,
                    "EconomicLife": economic_life,
                }
            )

    vessel_values = pd.DataFrame(rows)

    if unknown_type_ids:
        warnings.append(
            "Some vessel types could not be mapped to valuation curves and were excluded "
            f"({len(unknown_type_ids)} vessels)."
        )
    if missing_built_ids:
        warnings.append(
            f"Missing built year for {len(missing_built_ids)} vessels; they were excluded."
        )
    if missing_anchor_ids:
        warnings.append(
            f"Missing market benchmark prices for {len(missing_anchor_ids)} vessels on some dates."
        )

    if vessel_values.empty:
        warnings.append("No vessel-level values could be calculated with current inputs.")
        empty = pd.DataFrame()
        return {
            "vessel_values": empty,
            "fleet_total": empty,
            "fleet_by_type": empty,
            "nav_bridge": empty,
            "warnings": warnings,
        }

    fleet_total = (
        vessel_values.groupby("Date", as_index=False)
        .agg(TotalFleetValue=("Value", "sum"), ActiveVessels=("ID", "nunique"))
        .sort_values("Date")
    )

    fleet_by_type = (
        vessel_values.groupby(["Date", "VesselType"], as_index=False)
        .agg(FleetValue=("Value", "sum"), Vessels=("ID", "nunique"))
        .sort_values(["Date", "VesselType"])
    )

    nav_bridge, nav_warnings = build_nav_bridge(ticker, fleet_total)
    warnings.extend(nav_warnings)

    return {
        "vessel_values": vessel_values,
        "fleet_total": fleet_total,
        "fleet_by_type": fleet_by_type,
        "nav_bridge": nav_bridge,
        "warnings": warnings,
    }


def build_nav_bridge(ticker: str, fleet_total: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    if fleet_total.empty:
        return pd.DataFrame(), warnings

    financials, load_warnings = _load_financials_for_nav_with_warnings(ticker)
    warnings.extend(load_warnings)
    if financials.empty:
        warnings.append(
            "NAV bridge unavailable: unable to parse financials into Date/Cash/TotalDebt/SharesOutstanding."
        )
        return pd.DataFrame(), warnings

    required_cols = {"Cash", "TotalDebt", "SharesOutstanding"}
    if not required_cols.issubset(set(financials.columns)):
        warnings.append(
            "NAV bridge unavailable: missing financial inputs (Cash, TotalDebt, or SharesOutstanding)."
        )
        return pd.DataFrame(), warnings

    left = fleet_total[["Date", "TotalFleetValue"]].sort_values("Date")
    right = financials[["Date", "Cash", "TotalDebt", "SharesOutstanding"]].sort_values("Date")

    merged = pd.merge_asof(left, right, on="Date", direction="backward")
    merged = merged.dropna(subset=["Cash", "TotalDebt", "SharesOutstanding"]).copy()
    if merged.empty:
        warnings.append("NAV bridge unavailable: no overlapping dates after as-of merge.")
        return pd.DataFrame(), warnings

    merged["NAV"] = merged["TotalFleetValue"] + merged["Cash"] - merged["TotalDebt"]
    merged["NAVPerShare"] = merged["NAV"] / merged["SharesOutstanding"]

    return merged, warnings


def build_total_fleet_value_series(ticker: str) -> pd.DataFrame:
    result = run_fleet_valuation_model(
        ticker=ticker,
        start_date=None,
        end_date=None,
        selected_vessel_types=None,
    )
    return result["fleet_total"]


@st.cache_data
def build_fleet_table(ticker: str) -> pd.DataFrame:
    df = load_current_owned_vessels(ticker)

    if df.empty:
        return pd.DataFrame()

    fleet_table = df.copy()
    fleet_table["Built"] = pd.to_numeric(fleet_table["Built"], errors="coerce")
    if "DWT" in fleet_table.columns:
        fleet_table["DWT"] = pd.to_numeric(fleet_table["DWT"], errors="coerce")

    current_year = pd.Timestamp.today().year
    fleet_table["Age"] = current_year - fleet_table["Built"]

    columns = ["ID", "Vessel", "VesselType", "Built", "Age"]
    if "DWT" in fleet_table.columns:
        columns.append("DWT")
    if "Flag" in fleet_table.columns:
        columns.append("Flag")

    fleet_table = fleet_table[columns]

    rename_map = {
        "ID": "ID",
        "Vessel": "Ship name",
        "VesselType": "Type",
        "Built": "Built year",
        "DWT": "DWT",
        "Flag": "Flag",
        "Age": "Age",
    }

    fleet_table = fleet_table.rename(columns=rename_map)
    return fleet_table.sort_values("Age", ascending=False)
