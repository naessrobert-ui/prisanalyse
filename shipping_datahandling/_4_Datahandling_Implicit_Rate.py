from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import numpy_financial as npf
import pandas as pd


_BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = _BASE_DIR / "shipping_app" / "Data"
OUTPUT_DIR = _BASE_DIR / "shipping_app" / "Output" / "Implicit_Rate"
CACHE_SCHEMA_VERSION = 3
DEFENSE_CACHE_SCHEMA_VERSION = 2
INCREMENTAL_OVERLAP_DAYS = 31


class MissingFinancialsDataError(ValueError):
    """Raised when a company workbook exists but has no usable financials yet."""


def _normalize_date(value: date | str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    return pd.Timestamp(value).normalize()


def _source_paths(ticker: str) -> dict[str, Path]:
    return {
        "prices": DATA_DIR / f"{ticker}_Prices.csv",
        "financials": DATA_DIR / f"{ticker}_Financials.xlsx",
        "fleet": DATA_DIR / f"{ticker}_Fleet.xlsx",
        "ship_values": DATA_DIR / "Ship_Values.xlsx",
        "gna": DATA_DIR / "GnA_Assumptions.csv",
        "capex": DATA_DIR / "Capex_Assumptions.xlsx",
        "opex": DATA_DIR / "Opex_Assumptions.xlsx",
    }


def _cache_paths(ticker: str) -> tuple[Path, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return (
        OUTPUT_DIR / f"{ticker}_Implicit_Rates.pkl",
        OUTPUT_DIR / f"{ticker}_Implicit_Rates.meta.json",
    )


def _defense_cache_paths() -> tuple[Path, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return (
        OUTPUT_DIR / "Vessel_Value_Defense_Rates.pkl",
        OUTPUT_DIR / "Vessel_Value_Defense_Rates.meta.json",
    )


def _source_signature(ticker: str) -> dict[str, dict[str, int]]:
    signature: dict[str, dict[str, int]] = {}
    for name, path in _source_paths(ticker).items():
        if not path.exists():
            if name == "financials":
                raise MissingFinancialsDataError(
                    f"{ticker} is missing a financials workbook. "
                    f"Create {path.as_posix()} or re-add the company from the home page."
                )
            raise FileNotFoundError(f"Required source file not found for {ticker}: {path.as_posix()}")
        stat = path.stat()
        signature[name] = {
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return signature


def _defense_source_signature() -> dict[str, dict[str, int]]:
    signature: dict[str, dict[str, int]] = {}
    shared_paths = {
        "ship_values": DATA_DIR / "Ship_Values.xlsx",
        "gna": DATA_DIR / "GnA_Assumptions.csv",
        "capex": DATA_DIR / "Capex_Assumptions.xlsx",
        "opex": DATA_DIR / "Opex_Assumptions.xlsx",
    }
    for name, path in shared_paths.items():
        stat = path.stat()
        signature[name] = {
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return signature


def _load_cached_implicit_output(ticker: str) -> tuple[pd.DataFrame | None, dict | None]:
    data_path, meta_path = _cache_paths(ticker)
    if not data_path.exists() or not meta_path.exists():
        return None, None

    try:
        cached_df = pd.read_pickle(data_path)
        cached_df["Date"] = pd.to_datetime(cached_df["Date"]).dt.normalize()

        with meta_path.open("r", encoding="utf-8") as handle:
            meta = json.load(handle)
    except Exception:
        # Stale/incompatible cache (for example after pandas upgrades):
        # treat as cache miss so callers rebuild fresh output.
        return None, None

    return cached_df, meta


def _save_cached_implicit_output(ticker: str, implicit_df: pd.DataFrame) -> pd.DataFrame:
    implicit_df = implicit_df.copy()
    implicit_df["Date"] = pd.to_datetime(implicit_df["Date"]).dt.normalize()
    implicit_df = (
        implicit_df.sort_values(["Date", "VesselType"])
        .drop_duplicates(subset=["Date", "VesselType"], keep="last")
        .reset_index(drop=True)
    )

    data_path, meta_path = _cache_paths(ticker)
    implicit_df.to_pickle(data_path)

    meta = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "ticker": ticker,
        "cached_from": implicit_df["Date"].min().strftime("%Y-%m-%d") if not implicit_df.empty else None,
        "cached_through": implicit_df["Date"].max().strftime("%Y-%m-%d") if not implicit_df.empty else None,
        "source_signature": _source_signature(ticker),
    }

    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2)

    return implicit_df


def _load_cached_defense_output() -> tuple[pd.DataFrame | None, dict | None]:
    data_path, meta_path = _defense_cache_paths()
    if not data_path.exists() or not meta_path.exists():
        return None, None

    try:
        cached_df = pd.read_pickle(data_path)
        cached_df["Date"] = pd.to_datetime(cached_df["Date"]).dt.normalize()

        with meta_path.open("r", encoding="utf-8") as handle:
            meta = json.load(handle)
    except Exception:
        # Stale/incompatible cache (for example after pandas upgrades):
        # treat as cache miss so callers rebuild fresh output.
        return None, None

    return cached_df, meta


def _save_cached_defense_output(defense_df: pd.DataFrame) -> pd.DataFrame:
    defense_df = defense_df.copy()
    defense_df["Date"] = pd.to_datetime(defense_df["Date"]).dt.normalize()
    defense_df = (
        defense_df.sort_values(["Date", "VesselType"])
        .drop_duplicates(subset=["Date", "VesselType"], keep="last")
        .reset_index(drop=True)
    )

    data_path, meta_path = _defense_cache_paths()
    defense_df.to_pickle(data_path)

    meta = {
        "schema_version": DEFENSE_CACHE_SCHEMA_VERSION,
        "cached_from": defense_df["Date"].min().strftime("%Y-%m-%d") if not defense_df.empty else None,
        "cached_through": defense_df["Date"].max().strftime("%Y-%m-%d") if not defense_df.empty else None,
        "source_signature": _defense_source_signature(),
    }

    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2)

    return defense_df


def _merge_cache_frames(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"]).dt.normalize()
    return (
        combined.sort_values(["Date", "VesselType"])
        .drop_duplicates(subset=["Date", "VesselType"], keep="last")
        .reset_index(drop=True)
    )


# ----- Load the data -----
def load_stock_data(ticker: str, start_date=None, end_date=None):
    df = pd.read_csv(DATA_DIR / f"{ticker}_Prices.csv", parse_dates=[0], index_col=0).sort_index()
    df.index = pd.to_datetime(df.index).normalize()
    df.index.name = "Date"

    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)

    if start_date is not None:
        df = df[df.index >= start_date]
    if end_date is not None:
        df = df[df.index <= end_date]

    return df


def load_financials(ticker, date_col="ReportmentDate"):
    raw = pd.read_excel(DATA_DIR / f"{ticker}_Financials.xlsx", index_col=0)
    df = raw.T.copy()
    df.index.name = "Period"

    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")

    def to_num(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).strip()
        if s in {"â€”", "-", ""}:
            return np.nan
        s = s.replace(",", "").replace(" ", "")
        return pd.to_numeric(s, errors="coerce")

    for column in df.columns:
        if column != date_col:
            df[column] = df[column].map(to_num)

    return df


def _require_usable_financials(ticker: str, financials_df: pd.DataFrame) -> pd.DataFrame:
    required_columns = ["ReportmentDate", "SharesOutstanding", "TotalDebt", "Cash"]
    missing_columns = [column for column in required_columns if column not in financials_df.columns]
    if missing_columns:
        raise MissingFinancialsDataError(
            f"{ticker} financials workbook is missing required fields: {', '.join(missing_columns)}."
        )

    financials = financials_df[required_columns].copy()
    if financials.empty:
        raise MissingFinancialsDataError(
            f"{ticker} financials workbook exists, but it does not contain any reporting periods yet."
        )

    financials["ReportmentDate"] = pd.to_datetime(financials["ReportmentDate"], errors="coerce")
    has_core_values = financials[["SharesOutstanding", "TotalDebt", "Cash"]].notna().any(axis=1)
    usable_rows = financials["ReportmentDate"].notna() & has_core_values
    if not usable_rows.any():
        raise MissingFinancialsDataError(
            f"{ticker} financials workbook exists, but it does not contain any usable financial data yet."
        )

    return financials_df


def _fleet_workbook(ticker: str) -> pd.ExcelFile:
    return pd.ExcelFile(DATA_DIR / f"{ticker}_Fleet.xlsx")


def _load_fleet_single_sheet(ticker: str) -> pd.DataFrame:
    workbook = _fleet_workbook(ticker)
    return pd.read_excel(workbook, sheet_name=workbook.sheet_names[0], index_col="ID")


def load_fleet_master(ticker: str):
    workbook = _fleet_workbook(ticker)
    if "Master" in workbook.sheet_names:
        return pd.read_excel(workbook, sheet_name="Master", index_col="ID")
    return _load_fleet_single_sheet(ticker)


def load_fleet_ownership_periods(ticker: str):
    workbook = _fleet_workbook(ticker)
    if "OwnershipPeriods" in workbook.sheet_names:
        return pd.read_excel(
            workbook,
            sheet_name="OwnershipPeriods",
            index_col="ID",
            parse_dates=["StartDate", "EndDate"],
        )

    fleet_df = _load_fleet_single_sheet(ticker).reset_index()

    if "Owned" in fleet_df.columns:
        owned_flag = pd.to_numeric(fleet_df["Owned"], errors="coerce").fillna(0)
        fleet_df = fleet_df.loc[owned_flag > 0].copy()

    built_year = pd.to_numeric(fleet_df.get("Built"), errors="coerce").round().astype("Int64")
    fleet_df["StartDate"] = pd.to_datetime(built_year.astype(str) + "-01-01", errors="coerce")
    fleet_df["EndDate"] = pd.NaT

    return fleet_df.set_index("ID")


def load_capex_assumptions():
    df = pd.read_excel(DATA_DIR / "Capex_Assumptions.xlsx")
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    df.set_index("Date", inplace=True)
    return df


def load_opex_assumptions():
    df = pd.read_excel(DATA_DIR / "Opex_Assumptions.xlsx")
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    df.set_index("Date", inplace=True)
    return df


def load_gna_assumptions():
    return pd.read_csv(DATA_DIR / "GnA_Assumptions.csv")


def load_ship_values(start_date=None, end_date=None):
    ship_values_df = pd.read_excel(
        DATA_DIR / "Ship_Values.xlsx",
        sheet_name="FleetValues",
        index_col="Date",
    )

    ship_values_df.index = pd.to_datetime(ship_values_df.index).normalize()

    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)

    if start_date is not None:
        ship_values_df = ship_values_df.loc[ship_values_df.index >= start_date]
    if end_date is not None:
        ship_values_df = ship_values_df.loc[ship_values_df.index <= end_date]

    ship_values_df.rename(
        columns={
            "Vessel Price for Current VLCC": "VLCC_0",
            "Vessel Price for 5 Year Old VL": "VLCC_5",
            "Vessel Price for 15 Year Old V": "VLCC_15",
            "Vessel Price for Current Suezm": "Suez_0",
            "Vessel Price for 5 Year Old Su": "Suez_5",
            "Vessel Price for 15 Year Old S": "Suez_15",
            "Vessel Price for Current Afram": "Afram_0",
            "Vessel Price for 5 Year Old Af": "Afram_5",
            "Vessel Price for 15 Year Old A": "Afram_15",
            "Vessel Price for Current LR1 T": "LR1_0",
            "Vessel Price for 5 Year Old LR": "LR1_5",
            "Vessel Price for 15 Year Old L": "LR1_15",
            "Vessel Price for Current MR Ta": "MR_0",
            "Vessel Price for 5 Year Old MR": "MR_5",
            "Vessel Price for 15 Year Old M": "MR_15",
        },
        inplace=True,
    )

    return ship_values_df


# ----- Handle the data -----
def add_financials(stock_df, financials_df, price_col="Close"):
    prices = stock_df[[price_col]].sort_index().reset_index()
    prices["Date"] = pd.to_datetime(prices["Date"]).dt.normalize()

    financials = financials_df[["ReportmentDate", "SharesOutstanding", "TotalDebt", "Cash"]].copy()
    financials["ReportmentDate"] = pd.to_datetime(financials["ReportmentDate"]).dt.normalize()
    financials = financials.sort_values("ReportmentDate")

    merged = pd.merge_asof(
        prices,
        financials,
        left_on="Date",
        right_on="ReportmentDate",
        direction="backward",
    )
    merged["SharesOutstanding"] = merged["SharesOutstanding"] * 1000
    merged["TotalDebt"] = merged["TotalDebt"] * 1000
    merged["Cash"] = merged["Cash"] * 1000

    merged["MarketCap"] = merged[price_col] * merged["SharesOutstanding"]
    merged["EV"] = merged["MarketCap"] + merged["TotalDebt"] - merged["Cash"]

    return merged.set_index("Date")


def fleet_stats_over_time(
    fleet_timeline_df,
    fleet_overview_df,
    freq="D",
    start_date=None,
    end_date=None,
    newbuild_life=20,
):
    if freq != "D":
        raise ValueError("fleet_stats_over_time currently supports daily frequency only.")

    df = fleet_timeline_df.copy()
    if "Built" not in df.columns:
        df = df.merge(
            fleet_overview_df[["Built"]],
            left_index=True,
            right_index=True,
            how="left",
        )
    else:
        built_lookup = fleet_overview_df[["Built"]].rename(columns={"Built": "Built_master"})
        df = df.merge(built_lookup, left_index=True, right_index=True, how="left")
        df["Built"] = df["Built"].fillna(df["Built_master"])
        df = df.drop(columns=["Built_master"])

    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.normalize()
    df["EndDate"] = pd.to_datetime(df["EndDate"]).dt.normalize()
    df["Built"] = pd.to_numeric(df["Built"], errors="coerce")

    start = _normalize_date(start_date) if start_date is not None else df["StartDate"].min()
    end = _normalize_date(end_date) if end_date is not None else pd.Timestamp.today().normalize()

    active_end = df["EndDate"].fillna(end + pd.Timedelta(days=1)) - pd.Timedelta(days=1)
    df["ActiveStart"] = df["StartDate"].clip(lower=start)
    df["ActiveEnd"] = active_end.clip(upper=end)

    df = df.loc[
        df["VesselType"].notna()
        & df["Built"].notna()
        & (df["ActiveStart"] <= df["ActiveEnd"])
    ].copy()

    df["Date"] = df.apply(
        lambda row: pd.date_range(row["ActiveStart"], row["ActiveEnd"], freq=freq),
        axis=1,
    )

    expanded = df[["VesselType", "Built", "Date"]].explode("Date", ignore_index=True)
    expanded["Date"] = pd.to_datetime(expanded["Date"]).dt.normalize()
    expanded["Age"] = expanded["Date"].dt.year - expanded["Built"]

    fleet_stats = (
        expanded.groupby(["Date", "VesselType"], as_index=False)
        .agg(
            Count=("VesselType", "size"),
            AvgAge=("Age", "mean"),
        )
        .sort_values(["Date", "VesselType"])
        .reset_index(drop=True)
    )

    fleet_stats["AvgAge"] = fleet_stats["AvgAge"].round(2)
    fleet_stats["RemainingLife"] = np.maximum(0, newbuild_life - fleet_stats["AvgAge"])

    return fleet_stats


def add_ship_value(fleet_stats, ship_values_df):
    fs = fleet_stats.copy()
    fs["Date"] = pd.to_datetime(fs["Date"]).dt.normalize().astype("datetime64[ns]")

    sv = ship_values_df.copy()
    if "Date" not in sv.columns:
        sv = sv.reset_index()
    sv["Date"] = pd.to_datetime(sv["Date"]).dt.normalize().astype("datetime64[ns]")
    sv = sv.sort_values("Date")

    vessel_map = {
        "VLCC": ["VLCC_0", "VLCC_5", "VLCC_15"],
        "Suezmax": ["Suez_0", "Suez_5", "Suez_15"],
        "MR": ["MR_0", "MR_5", "MR_15"],
        "Aframax/LR2": ["Afram_0", "Afram_5", "Afram_15"],
    }

    parts = []

    for vessel_type, price_cols in vessel_map.items():
        left = fs[fs["VesselType"] == vessel_type].sort_values("Date").copy()

        right = (
            sv[["Date", *price_cols]]
            .rename(
                columns={
                    price_cols[0]: "Price_0Y",
                    price_cols[1]: "Price_5Y",
                    price_cols[2]: "Price_15Y",
                }
            )
            .sort_values("Date")
            .copy()
        )

        merged = pd.merge_asof(left, right, on="Date", direction="backward")
        parts.append(merged)

    return (
        pd.concat(parts, ignore_index=True)
        .sort_values(["Date", "VesselType"])
        .reset_index(drop=True)
    )


def add_newbuild_parity(df, newbuild_life):
    out = df.copy()

    age = out["AvgAge"]
    p0 = out["Price_0Y"]
    remaining_life = newbuild_life - age

    out["NewbuildParity"] = p0 / newbuild_life * remaining_life
    out["SubFleetValue"] = out["Count"] * out["NewbuildParity"]

    return out


def add_gna(fleet_stats_df, gna):
    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["Year"] = pd.to_datetime(fleet_stats_df["Date"]).dt.year

    gna_long = (
        gna.melt(id_vars="Date", var_name="VesselType", value_name="GnaAssumption")
        .rename(columns={"Date": "Year"})
    )

    df_w_gna = fleet_stats_df.merge(
        gna_long,
        on=["Year", "VesselType"],
        how="left",
    )

    df_w_gna["TotalGnaAssumption"] = df_w_gna["Count"] * df_w_gna["GnaAssumption"] * 365

    return df_w_gna


def calculate_gna_pv(fleet_stats_df, newbuild_life, wacc):
    remaining_life = np.round(np.maximum(0, newbuild_life - fleet_stats_df["AvgAge"]), 2)

    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["TotalGnaPval"] = -npf.pv(
        rate=wacc,
        nper=remaining_life,
        pmt=fleet_stats_df["TotalGnaAssumption"],
        fv=0,
        when="end",
    )
    return fleet_stats_df


def calculate_subfleet_asset_value(fleet_stats_df):
    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["SubFleetAssetValue"] = fleet_stats_df["SubFleetValue"] - fleet_stats_df["TotalGnaPval"]
    return fleet_stats_df


def calculate_total_fleet_value(fleet_stats_df):
    total_fleet_value_df = (
        fleet_stats_df.groupby("Date", as_index=False)[["SubFleetValue", "TotalGnaPval"]]
        .sum()
        .rename(columns={"SubFleetValue": "TotalFleetValue"})
    )

    total_fleet_value_df["AssetValue"] = (
        total_fleet_value_df["TotalFleetValue"] - total_fleet_value_df["TotalGnaPval"]
    )
    total_fleet_value_df.set_index("Date", inplace=True)
    return total_fleet_value_df


def calculate_ev_gav(financials_df, total_fleet_value_df):
    financials_df = financials_df.copy()
    total_fleet_value_df = total_fleet_value_df.copy()

    financials_df.index = pd.to_datetime(financials_df.index).normalize()
    total_fleet_value_df.index = pd.to_datetime(total_fleet_value_df.index).normalize()

    ev_gav_df = (
        financials_df[["EV"]]
        .join(total_fleet_value_df[["TotalFleetValue", "TotalGnaPval", "AssetValue"]], how="inner")
    )
    ev_gav_df["EV_GAV"] = ev_gav_df["EV"] / ev_gav_df["AssetValue"]
    return ev_gav_df


def add_ev_gav(fleet_stats_df, ev_gav_df):
    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["Date"] = pd.to_datetime(fleet_stats_df["Date"]).dt.normalize()

    ev_gav_daily = ev_gav_df[["EV_GAV"]].copy()
    ev_gav_daily.index = pd.to_datetime(ev_gav_daily.index).normalize()

    all_dates = pd.Index(sorted(fleet_stats_df["Date"].unique()), name="Date")

    ev_gav_daily = ev_gav_daily.reindex(all_dates).ffill().reset_index()

    fleet_stats_df = fleet_stats_df.merge(ev_gav_daily, on="Date", how="left")
    fleet_stats_df.set_index("Date", inplace=True)
    return fleet_stats_df


def implicit_calculations(fleet_stats_df, wacc):
    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["ImpliedAssetValue"] = fleet_stats_df["SubFleetAssetValue"] * fleet_stats_df["EV_GAV"]
    fleet_stats_df["ImplicitNewbuildParity"] = fleet_stats_df["NewbuildParity"] * fleet_stats_df["EV_GAV"]
    fleet_stats_df["RequiredCashFlow"] = -npf.pmt(
        rate=wacc,
        nper=fleet_stats_df["RemainingLife"],
        pv=fleet_stats_df["ImplicitNewbuildParity"],
        when="end",
    )
    return fleet_stats_df


def add_capex_and_opex(capex, opex, fleet_stats_df):
    capex = capex.rename(columns={"Aframax": "Aframax/LR2"})
    opex = opex.rename(
        columns={
            "VLCC Operating Costs (OPEX)": "VLCC",
            "Suezmax Operating Costs (OPEX)": "Suezmax",
            "Aframax/LR2 Operating Costs (OPEX)": "Aframax/LR2",
            "MR/Handy Tanker Operating Costs (OPEX)": "MR",
        }
    )

    capex = capex[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]
    opex = opex[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]

    capex_long = (
        capex.reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="CapexAssumption")
    )
    opex_long = (
        opex.reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="OpexAssumption")
    )

    fleet_stats_df = fleet_stats_df.merge(
        capex_long,
        on=["Date", "VesselType"],
        how="left",
    )
    fleet_stats_df = fleet_stats_df.merge(
        opex_long,
        on=["Date", "VesselType"],
        how="left",
    )

    fleet_stats_df = fleet_stats_df.sort_values(["VesselType", "Date"])

    fleet_stats_df["CapexAssumption"] = (
        fleet_stats_df.groupby("VesselType")["CapexAssumption"].ffill()
    )
    fleet_stats_df["OpexAssumption"] = (
        fleet_stats_df.groupby("VesselType")["OpexAssumption"].ffill()
    )
    fleet_stats_df["CapexAssumption"] = (
        fleet_stats_df.groupby("VesselType")["CapexAssumption"].bfill()
    )
    fleet_stats_df["OpexAssumption"] = (
        fleet_stats_df.groupby("VesselType")["OpexAssumption"].bfill()
    )

    fleet_stats_df["Capex"] = fleet_stats_df["CapexAssumption"]
    fleet_stats_df["Opex"] = fleet_stats_df["OpexAssumption"] * 365

    return fleet_stats_df.sort_values(["Date", "VesselType"]).reset_index(drop=True)


def add_capex_and_opex_asof(capex, opex, fleet_stats_df):
    capex = capex.rename(columns={"Aframax": "Aframax/LR2"})
    opex = opex.rename(
        columns={
            "VLCC Operating Costs (OPEX)": "VLCC",
            "Suezmax Operating Costs (OPEX)": "Suezmax",
            "Aframax/LR2 Operating Costs (OPEX)": "Aframax/LR2",
            "MR/Handy Tanker Operating Costs (OPEX)": "MR",
        }
    )

    capex = capex[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]
    opex = opex[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]

    capex_long = (
        capex.reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="CapexAssumption")
        .sort_values(["VesselType", "Date"])
    )
    opex_long = (
        opex.reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="OpexAssumption")
        .sort_values(["VesselType", "Date"])
    )

    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["Date"] = pd.to_datetime(fleet_stats_df["Date"]).dt.normalize().astype("datetime64[ns]")
    capex_long["Date"] = pd.to_datetime(capex_long["Date"]).dt.normalize().astype("datetime64[ns]")
    opex_long["Date"] = pd.to_datetime(opex_long["Date"]).dt.normalize().astype("datetime64[ns]")

    parts = []
    vessel_types = sorted(fleet_stats_df["VesselType"].dropna().unique())

    for vessel_type in vessel_types:
        left = fleet_stats_df.loc[fleet_stats_df["VesselType"] == vessel_type].sort_values("Date").copy()
        capex_right = capex_long.loc[
            capex_long["VesselType"] == vessel_type,
            ["Date", "CapexAssumption"],
        ].sort_values("Date")
        opex_right = opex_long.loc[
            opex_long["VesselType"] == vessel_type,
            ["Date", "OpexAssumption"],
        ].sort_values("Date")

        merged = pd.merge_asof(left, capex_right, on="Date", direction="backward")
        merged = pd.merge_asof(merged.sort_values("Date"), opex_right, on="Date", direction="backward")
        parts.append(merged)

    fleet_stats_df = pd.concat(parts, ignore_index=True).sort_values(["Date", "VesselType"]).reset_index(drop=True)
    fleet_stats_df["Capex"] = fleet_stats_df["CapexAssumption"]
    fleet_stats_df["Opex"] = fleet_stats_df["OpexAssumption"] * 365
    return fleet_stats_df


def calculate_implicit_rate(fleet_stats_df, utilization):
    fleet_stats_df = fleet_stats_df.copy()
    fleet_stats_df["EBITDA"] = fleet_stats_df["RequiredCashFlow"] + fleet_stats_df["Capex"]
    fleet_stats_df["RevenueBasis365"] = fleet_stats_df["EBITDA"] + fleet_stats_df["Opex"]
    fleet_stats_df["Revenue"] = fleet_stats_df["RevenueBasis365"] / utilization
    fleet_stats_df["ImplicitDayrate"] = fleet_stats_df["Revenue"] / 365
    return fleet_stats_df


def retrieve_implicit_rate(fleet_stats_df):
    implicit_df = fleet_stats_df[["Date", "VesselType", "ImplicitDayrate"]].copy()
    implicit_df["Date"] = pd.to_datetime(implicit_df["Date"]).dt.normalize()
    return implicit_df


def retrieve_dashboard_output(fleet_stats_df):
    dashboard_columns = [
        "Date",
        "VesselType",
        "ImplicitDayrate",
        "EV_GAV",
        "CapexAssumption",
        "OpexAssumption",
        "GnaAssumption",
    ]
    dashboard_df = fleet_stats_df[dashboard_columns].copy()
    dashboard_df["Date"] = pd.to_datetime(dashboard_df["Date"]).dt.normalize()
    return dashboard_df


def build_latest_implicit_rate_snapshot(
    ticker: str,
    *,
    newbuild_life=20,
    wacc=0.1,
    utilization=0.98,
):
    stock_df = load_stock_data(ticker)
    if stock_df.empty:
        raise ValueError(f"No stock data available for {ticker}.")

    latest_date = pd.Timestamp(stock_df.index.max()).normalize()
    stock_snapshot = stock_df.loc[[latest_date]]
    financials_df = load_financials(ticker)
    _require_usable_financials(ticker, financials_df)
    implicit_foundation_df = add_financials(stock_snapshot, financials_df)

    fleet_overview_df = load_fleet_master(ticker)
    fleet_timeline_df = load_fleet_ownership_periods(ticker)
    ship_values_df = load_ship_values(end_date=latest_date)
    gna_df = load_gna_assumptions()
    capex_df = load_capex_assumptions()
    opex_df = load_opex_assumptions()

    fleet_stats_df = fleet_stats_over_time(
        fleet_timeline_df,
        fleet_overview_df,
        freq="D",
        start_date=latest_date,
        end_date=latest_date,
        newbuild_life=newbuild_life,
    )
    fleet_stats_df = add_ship_value(fleet_stats_df, ship_values_df)
    fleet_stats_df = add_newbuild_parity(fleet_stats_df, newbuild_life=newbuild_life)
    fleet_stats_df = add_gna(fleet_stats_df, gna_df)
    fleet_stats_df = calculate_gna_pv(fleet_stats_df, newbuild_life=newbuild_life, wacc=wacc)
    fleet_stats_df = calculate_subfleet_asset_value(fleet_stats_df)

    total_fleet_value_df = calculate_total_fleet_value(fleet_stats_df)
    ev_gav_df = calculate_ev_gav(implicit_foundation_df, total_fleet_value_df)

    fleet_stats_df = add_ev_gav(fleet_stats_df, ev_gav_df).reset_index()
    fleet_stats_df = implicit_calculations(fleet_stats_df, wacc=wacc)
    fleet_stats_df = add_capex_and_opex_asof(capex_df, opex_df, fleet_stats_df)
    fleet_stats_df = calculate_implicit_rate(fleet_stats_df, utilization=utilization)

    fleet_stats_df["Date"] = pd.to_datetime(fleet_stats_df["Date"]).dt.normalize()
    fleet_stats_df["WACC"] = float(wacc)
    fleet_stats_df["Utilization"] = float(utilization)
    fleet_stats_df["NewbuildLife"] = float(newbuild_life)

    company_value_row = ev_gav_df.loc[latest_date]
    fleet_stats_df["EV"] = float(company_value_row["EV"])
    fleet_stats_df["CompanyTotalFleetValue"] = float(company_value_row["TotalFleetValue"])
    fleet_stats_df["CompanyTotalGnaPval"] = float(company_value_row["TotalGnaPval"])
    fleet_stats_df["CompanyAssetValue"] = float(company_value_row["AssetValue"])

    snapshot_columns = [
        "Date",
        "VesselType",
        "Count",
        "AvgAge",
        "RemainingLife",
        "Price_0Y",
        "NewbuildParity",
        "SubFleetValue",
        "GnaAssumption",
        "TotalGnaAssumption",
        "TotalGnaPval",
        "SubFleetAssetValue",
        "EV_GAV",
        "ImpliedAssetValue",
        "ImplicitNewbuildParity",
        "RequiredCashFlow",
        "CapexAssumption",
        "OpexAssumption",
        "Capex",
        "Opex",
        "ImplicitDayrate",
        "WACC",
        "Utilization",
        "NewbuildLife",
        "EV",
        "CompanyTotalFleetValue",
        "CompanyTotalGnaPval",
        "CompanyAssetValue",
    ]
    return fleet_stats_df[snapshot_columns].sort_values("VesselType").reset_index(drop=True)


def run_implicit_rate_sensitivity_grid(
    snapshot_df: pd.DataFrame,
    vessel_type: str,
    *,
    row_assumption: str,
    row_values,
    column_assumption: str,
    column_values,
):
    assumption_to_column = {
        "wacc": "WACC",
        "capex": "CapexAssumption",
        "opex": "OpexAssumption",
        "gna": "GnaAssumption",
        "utilization": "Utilization",
        "ev_gav": "EV_GAV",
        "newbuild_price": "Price_0Y",
    }

    if row_assumption not in assumption_to_column:
        raise ValueError(f"Unsupported row assumption '{row_assumption}'.")
    if column_assumption not in assumption_to_column:
        raise ValueError(f"Unsupported column assumption '{column_assumption}'.")

    vessel_snapshot = snapshot_df.loc[snapshot_df["VesselType"] == vessel_type].copy()
    if vessel_snapshot.empty:
        raise ValueError(f"No snapshot data available for vessel type '{vessel_type}'.")

    base_row = vessel_snapshot.iloc[0]
    base_company_asset_value = float(snapshot_df["SubFleetAssetValue"].sum())
    base_ev = float(base_row.get("EV", base_row["EV_GAV"] * base_company_asset_value))
    ev_gav_is_manual_override = row_assumption == "ev_gav" or column_assumption == "ev_gav"

    def _scenario_row(overrides: dict[str, float]) -> dict[str, float | str | pd.Timestamp]:
        scenario_df = snapshot_df.copy()
        selected_mask = scenario_df["VesselType"] == vessel_type

        for assumption_key, value in overrides.items():
            scenario_df.loc[selected_mask, assumption_to_column[assumption_key]] = float(value)

        selected_index = scenario_df.index[selected_mask][0]
        scenario_df.loc[selected_index, "NewbuildParity"] = (
            float(scenario_df.loc[selected_index, "Price_0Y"])
            / float(scenario_df.loc[selected_index, "NewbuildLife"])
            * float(scenario_df.loc[selected_index, "RemainingLife"])
        )
        scenario_df.loc[selected_index, "SubFleetValue"] = (
            float(scenario_df.loc[selected_index, "Count"])
            * float(scenario_df.loc[selected_index, "NewbuildParity"])
        )
        scenario_df.loc[selected_index, "TotalGnaAssumption"] = (
            float(scenario_df.loc[selected_index, "Count"])
            * float(scenario_df.loc[selected_index, "GnaAssumption"])
            * 365
        )
        scenario_df.loc[selected_index, "TotalGnaPval"] = -npf.pv(
            rate=float(scenario_df.loc[selected_index, "WACC"]),
            nper=float(scenario_df.loc[selected_index, "RemainingLife"]),
            pmt=float(scenario_df.loc[selected_index, "TotalGnaAssumption"]),
            fv=0,
            when="end",
        )
        scenario_df.loc[selected_index, "SubFleetAssetValue"] = (
            float(scenario_df.loc[selected_index, "SubFleetValue"])
            - float(scenario_df.loc[selected_index, "TotalGnaPval"])
        )

        company_total_fleet_value = float(scenario_df["SubFleetValue"].sum())
        company_total_gna_pval = float(scenario_df["TotalGnaPval"].sum())
        company_asset_value = company_total_fleet_value - company_total_gna_pval

        if not ev_gav_is_manual_override:
            scenario_df["EV_GAV"] = base_ev / company_asset_value

        scenario_df.loc[selected_index, "ImpliedAssetValue"] = (
            float(scenario_df.loc[selected_index, "SubFleetAssetValue"])
            * float(scenario_df.loc[selected_index, "EV_GAV"])
        )
        scenario_df.loc[selected_index, "ImplicitNewbuildParity"] = (
            float(scenario_df.loc[selected_index, "NewbuildParity"])
            * float(scenario_df.loc[selected_index, "EV_GAV"])
        )
        scenario_df.loc[selected_index, "RequiredCashFlow"] = -npf.pmt(
            rate=float(scenario_df.loc[selected_index, "WACC"]),
            nper=float(scenario_df.loc[selected_index, "RemainingLife"]),
            pv=float(scenario_df.loc[selected_index, "ImplicitNewbuildParity"]),
            when="end",
        )
        scenario_df.loc[selected_index, "Capex"] = float(scenario_df.loc[selected_index, "CapexAssumption"])
        scenario_df.loc[selected_index, "Opex"] = float(scenario_df.loc[selected_index, "OpexAssumption"]) * 365
        scenario_df.loc[selected_index, "EBITDA"] = (
            float(scenario_df.loc[selected_index, "RequiredCashFlow"])
            + float(scenario_df.loc[selected_index, "Capex"])
        )
        scenario_df.loc[selected_index, "RevenueBasis365"] = (
            float(scenario_df.loc[selected_index, "EBITDA"])
            + float(scenario_df.loc[selected_index, "Opex"])
        )
        scenario_df.loc[selected_index, "Revenue"] = (
            float(scenario_df.loc[selected_index, "RevenueBasis365"])
            / float(scenario_df.loc[selected_index, "Utilization"])
        )
        scenario_df.loc[selected_index, "ImplicitDayrate"] = (
            float(scenario_df.loc[selected_index, "Revenue"]) / 365
        )

        scenario = scenario_df.loc[selected_index]

        return {
            "Date": pd.Timestamp(scenario["Date"]).normalize(),
            "VesselType": str(scenario["VesselType"]),
            "RowAssumption": row_assumption,
            "RowValue": float(overrides.get(row_assumption, scenario[assumption_to_column[row_assumption]])),
            "ColumnAssumption": column_assumption,
            "ColumnValue": float(overrides.get(column_assumption, scenario[assumption_to_column[column_assumption]])),
            "WACC": float(scenario["WACC"]),
            "CapexAssumption": float(scenario["CapexAssumption"]),
            "OpexAssumption": float(scenario["OpexAssumption"]),
            "GnaAssumption": float(scenario["GnaAssumption"]),
            "Utilization": float(scenario["Utilization"]),
            "EV_GAV": float(scenario["EV_GAV"]),
            "Price_0Y": float(scenario["Price_0Y"]),
            "NewbuildParity": float(scenario["NewbuildParity"]),
            "SubFleetValue": float(scenario["SubFleetValue"]),
            "SubFleetAssetValue": float(scenario["SubFleetAssetValue"]),
            "EV": base_ev,
            "CompanyTotalFleetValue": company_total_fleet_value,
            "CompanyTotalGnaPval": company_total_gna_pval,
            "CompanyAssetValue": company_asset_value,
            "ImplicitDayrate": float(scenario["ImplicitDayrate"]),
            "DeltaDayrate": float(scenario["ImplicitDayrate"]) - float(base_row["ImplicitDayrate"]),
            "DeltaEV_GAV": float(scenario["EV_GAV"]) - float(base_row["EV_GAV"]),
        }

    scenarios: list[dict[str, float | str | pd.Timestamp]] = []
    for row_value in row_values:
        for column_value in column_values:
            scenarios.append(
                _scenario_row(
                    {
                        row_assumption: row_value,
                        column_assumption: column_value,
                    }
                )
            )

    return pd.DataFrame(scenarios).sort_values(["RowValue", "ColumnValue"]).reset_index(drop=True)


def _build_vessel_value_defense_output(
    end_date=None,
    *,
    newbuild_life=20,
    wacc=0.1,
    utilization=0.98,
    tax_rate=0.0,
):
    end_date = _normalize_date(end_date) or pd.Timestamp.today().normalize()

    ship_values_df = load_ship_values(end_date=end_date).reset_index()
    gna_df = load_gna_assumptions()
    capex_df = load_capex_assumptions().rename(columns={"Aframax": "Aframax/LR2"})
    opex_df = load_opex_assumptions().rename(
        columns={
            "VLCC Operating Costs (OPEX)": "VLCC",
            "Suezmax Operating Costs (OPEX)": "Suezmax",
            "Aframax/LR2 Operating Costs (OPEX)": "Aframax/LR2",
            "MR/Handy Tanker Operating Costs (OPEX)": "MR",
        }
    )

    vessel_types = ["VLCC", "Suezmax", "Aframax/LR2", "MR"]
    scaffold = pd.DataFrame(
        [
            {
                "Date": row.Date,
                "VesselType": vessel_type,
                "Count": 1,
                "AvgAge": 0,
                "RemainingLife": newbuild_life,
            }
            for row in ship_values_df.itertuples(index=False)
            for vessel_type in vessel_types
        ]
    )

    defense_df = add_ship_value(scaffold, ship_values_df.set_index("Date"))[
        ["Date", "VesselType", "Price_0Y"]
    ].copy()
    defense_df["WACC"] = float(wacc)
    defense_df["Utilization"] = float(utilization)
    defense_df["NewbuildLife"] = float(newbuild_life)
    defense_df["TaxRate"] = float(tax_rate)
    defense_df["NewbuildValue"] = defense_df["Price_0Y"]
    defense_df["RequiredCashFlow"] = -npf.pmt(
        rate=wacc,
        nper=defense_df["NewbuildLife"],
        pv=defense_df["NewbuildValue"],
        when="end",
    )

    gna_long = (
        gna_df.melt(id_vars="Date", var_name="VesselType", value_name="GnaAssumption")
        .rename(columns={"Date": "Year"})
    )
    defense_df["Year"] = pd.to_datetime(defense_df["Date"]).dt.year
    defense_df = defense_df.merge(gna_long, on=["Year", "VesselType"], how="left")

    capex_long = (
        capex_df[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]
        .reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="CapexAssumption")
    )
    opex_long = (
        opex_df[["VLCC", "Suezmax", "Aframax/LR2", "MR"]]
        .reset_index()
        .melt(id_vars="Date", var_name="VesselType", value_name="OpexAssumption")
    )
    defense_df["Date"] = pd.to_datetime(defense_df["Date"]).dt.normalize().astype("datetime64[ns]")
    capex_long["Date"] = pd.to_datetime(capex_long["Date"]).dt.normalize().astype("datetime64[ns]")
    opex_long["Date"] = pd.to_datetime(opex_long["Date"]).dt.normalize().astype("datetime64[ns]")

    capex_parts = []
    opex_parts = []
    for vessel_type in vessel_types:
        left = defense_df.loc[defense_df["VesselType"] == vessel_type].sort_values("Date").copy()
        capex_right = capex_long.loc[capex_long["VesselType"] == vessel_type, ["Date", "CapexAssumption"]].sort_values("Date")
        opex_right = opex_long.loc[opex_long["VesselType"] == vessel_type, ["Date", "OpexAssumption"]].sort_values("Date")

        capex_parts.append(pd.merge_asof(left, capex_right, on="Date", direction="backward"))
        opex_parts.append(pd.merge_asof(left, opex_right, on="Date", direction="backward"))

    defense_df = pd.concat(capex_parts, ignore_index=True).sort_values(["Date", "VesselType"]).reset_index(drop=True)
    opex_joined = pd.concat(opex_parts, ignore_index=True).sort_values(["Date", "VesselType"]).reset_index(drop=True)
    defense_df["OpexAssumption"] = opex_joined["OpexAssumption"]

    defense_df["Capex"] = defense_df["CapexAssumption"]
    defense_df["Opex"] = defense_df["OpexAssumption"] * 365
    defense_df["Taxes"] = defense_df["TaxRate"] * 0
    defense_df["EBITDA"] = defense_df["RequiredCashFlow"] + defense_df["Capex"] + defense_df["Taxes"]
    defense_df["RevenueBasis365"] = defense_df["EBITDA"] + defense_df["Opex"]
    defense_df["Revenue"] = defense_df["RevenueBasis365"] / defense_df["Utilization"]
    defense_df["DefenseDayrate"] = defense_df["Revenue"] / 365

    return defense_df[
        [
            "Date",
            "VesselType",
            "WACC",
            "GnaAssumption",
            "CapexAssumption",
            "OpexAssumption",
            "Utilization",
            "NewbuildLife",
            "NewbuildValue",
            "RequiredCashFlow",
            "Capex",
            "TaxRate",
            "Taxes",
            "EBITDA",
            "Opex",
            "RevenueBasis365",
            "Revenue",
            "DefenseDayrate",
        ]
    ].sort_values(["Date", "VesselType"]).reset_index(drop=True)


def load_or_update_vessel_value_defense_output(
    end_date=None,
    *,
    newbuild_life=20,
    wacc=0.1,
    utilization=0.98,
    tax_rate=0.0,
):
    end_date = _normalize_date(end_date) or pd.Timestamp.today().normalize()
    cached_df, cached_meta = _load_cached_defense_output()
    current_signature = _defense_source_signature()

    cache_is_valid = (
        cached_df is not None
        and cached_meta is not None
        and cached_meta.get("schema_version") == DEFENSE_CACHE_SCHEMA_VERSION
        and cached_meta.get("source_signature") == current_signature
    )

    if not cache_is_valid:
        rebuilt_df = _build_vessel_value_defense_output(
            end_date=end_date,
            newbuild_life=newbuild_life,
            wacc=wacc,
            utilization=utilization,
            tax_rate=tax_rate,
        )
        cached_df = _save_cached_defense_output(rebuilt_df)

    return (
        cached_df.loc[cached_df["Date"] <= end_date]
        .sort_values(["Date", "VesselType"])
        .reset_index(drop=True)
    )


def retrieve_lates_rates(implicit_df):
    implicit_df = implicit_df.copy()
    implicit_df["Date"] = pd.to_datetime(implicit_df["Date"]).dt.normalize()

    latest_date = implicit_df["Date"].max()
    latest_dayrates = (
        implicit_df.loc[
            (implicit_df["Date"] == latest_date) & (implicit_df["ImplicitDayrate"].notna()),
            ["VesselType", "ImplicitDayrate"],
        ]
        .sort_values("VesselType")
        .reset_index(drop=True)
    )
    return latest_date, latest_dayrates


def _run_implicit_rate_pipeline(
    ticker: str,
    start_date,
    end_date=None,
    stock_lookback_start=None,
    newbuild_life=20,
    wacc=0.1,
    utilization=0.98,
):
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date) or pd.Timestamp.today().normalize()
    stock_lookback_start = _normalize_date(stock_lookback_start) or start_date

    stock_df = load_stock_data(ticker, start_date=stock_lookback_start, end_date=end_date)
    financials_df = load_financials(ticker)
    _require_usable_financials(ticker, financials_df)
    implicit_foundation_df = add_financials(stock_df, financials_df)

    fleet_overview_df = load_fleet_master(ticker)
    fleet_timeline_df = load_fleet_ownership_periods(ticker)
    ship_values_df = load_ship_values(end_date=end_date)
    gna_df = load_gna_assumptions()

    fleet_stats_df = fleet_stats_over_time(
        fleet_timeline_df,
        fleet_overview_df,
        freq="D",
        start_date=start_date,
        end_date=end_date,
        newbuild_life=newbuild_life,
    )
    fleet_stats_df = add_ship_value(fleet_stats_df, ship_values_df)
    fleet_stats_df = add_newbuild_parity(fleet_stats_df, newbuild_life=newbuild_life)
    fleet_stats_df = add_gna(fleet_stats_df, gna_df)
    fleet_stats_df = calculate_gna_pv(fleet_stats_df, newbuild_life=newbuild_life, wacc=wacc)
    fleet_stats_df = calculate_subfleet_asset_value(fleet_stats_df)

    total_fleet_value_df = calculate_total_fleet_value(fleet_stats_df)
    ev_gav_df = calculate_ev_gav(implicit_foundation_df, total_fleet_value_df)

    fleet_stats_df = add_ev_gav(fleet_stats_df, ev_gav_df)
    fleet_stats_df = implicit_calculations(fleet_stats_df, wacc=wacc)

    capex_df = load_capex_assumptions()
    opex_df = load_opex_assumptions()

    fleet_stats_df = add_capex_and_opex(capex_df, opex_df, fleet_stats_df)
    fleet_stats_df = calculate_implicit_rate(fleet_stats_df, utilization=utilization)

    dashboard_df = retrieve_dashboard_output(fleet_stats_df)
    return dashboard_df.loc[
        (dashboard_df["Date"] >= start_date) & (dashboard_df["Date"] <= end_date)
    ].reset_index(drop=True)


def _latest_available_on_or_before(dates: pd.Index, target_date: pd.Timestamp) -> pd.Timestamp | None:
    valid_dates = pd.to_datetime(dates)
    valid_dates = valid_dates[valid_dates <= target_date]
    if len(valid_dates) == 0:
        return None
    return pd.Timestamp(valid_dates.max()).normalize()


def _latest_available_before(dates: pd.Index, target_date: pd.Timestamp) -> pd.Timestamp | None:
    valid_dates = pd.to_datetime(dates)
    valid_dates = valid_dates[valid_dates < target_date]
    if len(valid_dates) == 0:
        return None
    return pd.Timestamp(valid_dates.max()).normalize()


def _determine_incremental_start(ticker: str, start_date: pd.Timestamp, cached_max: pd.Timestamp) -> pd.Timestamp:
    anchors = [cached_max - pd.Timedelta(days=INCREMENTAL_OVERLAP_DAYS)]

    stock_df = load_stock_data(ticker, end_date=cached_max)
    stock_anchor = _latest_available_on_or_before(stock_df.index, cached_max)
    if stock_anchor is not None:
        anchors.append(stock_anchor)

    ship_values_df = load_ship_values(end_date=cached_max)
    ship_anchor = _latest_available_on_or_before(ship_values_df.index, cached_max)
    if ship_anchor is not None:
        anchors.append(ship_anchor)

    capex_df = load_capex_assumptions()
    capex_anchor = _latest_available_on_or_before(capex_df.index, cached_max)
    if capex_anchor is not None:
        anchors.append(capex_anchor)

    opex_df = load_opex_assumptions()
    opex_anchor = _latest_available_on_or_before(opex_df.index, cached_max)
    if opex_anchor is not None:
        anchors.append(opex_anchor)

    return max(start_date, min(anchors))


def load_or_update_implicit_rate_output(
    ticker: str,
    start_date,
    end_date=None,
    *,
    newbuild_life=20,
    wacc=0.1,
    utilization=0.98,
    overlap_days=INCREMENTAL_OVERLAP_DAYS,
):
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date) or pd.Timestamp.today().normalize()

    cached_df, cached_meta = _load_cached_implicit_output(ticker)
    current_signature = _source_signature(ticker)

    cache_is_valid = (
        cached_df is not None
        and cached_meta is not None
        and cached_meta.get("schema_version") == CACHE_SCHEMA_VERSION
        and cached_meta.get("source_signature") == current_signature
    )

    if not cache_is_valid:
        rebuilt_df = _run_implicit_rate_pipeline(
            ticker,
            start_date=start_date,
            end_date=end_date,
            stock_lookback_start=start_date,
            newbuild_life=newbuild_life,
            wacc=wacc,
            utilization=utilization,
        )
        cached_df = _save_cached_implicit_output(ticker, rebuilt_df)
        return cached_df[cached_df["Date"] >= start_date].reset_index(drop=True)

    cached_df = cached_df.sort_values(["Date", "VesselType"]).reset_index(drop=True)
    cached_min = cached_df["Date"].min()
    cached_max = cached_df["Date"].max()

    updated_df = cached_df

    if start_date < cached_min:
        prepend_df = _run_implicit_rate_pipeline(
            ticker,
            start_date=start_date,
            end_date=cached_min - pd.Timedelta(days=1),
            stock_lookback_start=start_date,
            newbuild_life=newbuild_life,
            wacc=wacc,
            utilization=utilization,
        )
        updated_df = _merge_cache_frames(prepend_df, updated_df)
        cached_min = updated_df["Date"].min()

    if end_date > cached_max:
        incremental_start = _determine_incremental_start(ticker, start_date, cached_max)
        stock_df = load_stock_data(ticker, end_date=incremental_start)
        stock_lookback_start = _latest_available_before(stock_df.index, incremental_start)
        if stock_lookback_start is None:
            stock_lookback_start = incremental_start
        pipeline_start = stock_lookback_start

        tail_df = _run_implicit_rate_pipeline(
            ticker,
            start_date=pipeline_start,
            end_date=end_date,
            stock_lookback_start=stock_lookback_start,
            newbuild_life=newbuild_life,
            wacc=wacc,
            utilization=utilization,
        )
        updated_df = _merge_cache_frames(updated_df, tail_df)

    if not updated_df.equals(cached_df):
        updated_df = _save_cached_implicit_output(ticker, updated_df)

    return (
        updated_df.loc[(updated_df["Date"] >= start_date) & (updated_df["Date"] <= end_date)]
        .sort_values(["Date", "VesselType"])
        .reset_index(drop=True)
    )
