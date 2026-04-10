from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any

import pandas as pd

PORTFOLIO_SHEET_KEYWORDS = ("portfolio", "portef", "behold", "holding")
FUND_NAME_ALIASES = ("fund", "fond", "instrument", "ticker", "name", "navn")
WEIGHT_ALIASES = ("weight", "vekt", "allocation", "andel")


@dataclass
class PortfolioHolding:
    portfolio: str
    fund: str
    weight: float


class RebalancerError(ValueError):
    pass


def _normalize_col(col: str) -> str:
    return str(col or "").strip().lower().replace("%", "").replace("_", " ")


def detect_portfolio_sheets(workbook: dict[str, pd.DataFrame]) -> list[str]:
    detected: list[str] = []
    for sheet_name, df in workbook.items():
        cols = {_normalize_col(c) for c in df.columns}
        has_fund = any(any(alias in c for alias in FUND_NAME_ALIASES) for c in cols)
        has_weight = any(any(alias in c for alias in WEIGHT_ALIASES) for c in cols)
        by_name = any(k in sheet_name.lower() for k in PORTFOLIO_SHEET_KEYWORDS)
        if (has_fund and has_weight) or by_name:
            detected.append(sheet_name)
    return detected


def _pick_column(columns: list[str], aliases: tuple[str, ...]) -> str | None:
    normalized = {_normalize_col(c): c for c in columns}
    for ncol, source in normalized.items():
        if any(alias in ncol for alias in aliases):
            return source
    return None


def extract_holdings(workbook: dict[str, pd.DataFrame], sheets: list[str]) -> pd.DataFrame:
    rows: list[PortfolioHolding] = []
    for sheet in sheets:
        df = workbook[sheet]
        fund_col = _pick_column(list(df.columns), FUND_NAME_ALIASES)
        weight_col = _pick_column(list(df.columns), WEIGHT_ALIASES)
        if not fund_col or not weight_col:
            continue

        subset = df[[fund_col, weight_col]].copy()
        subset[fund_col] = subset[fund_col].astype(str).str.strip()
        subset[weight_col] = pd.to_numeric(subset[weight_col], errors="coerce")
        subset = subset.dropna(subset=[weight_col])
        subset = subset[subset[fund_col] != ""]

        for _, row in subset.iterrows():
            rows.append(
                PortfolioHolding(
                    portfolio=sheet,
                    fund=str(row[fund_col]).strip(),
                    weight=float(row[weight_col]),
                )
            )

    if not rows:
        raise RebalancerError("Fant ingen fond/vekter i valgte ark.")

    holdings = pd.DataFrame([r.__dict__ for r in rows])
    totals = holdings.groupby("portfolio", as_index=False)["weight"].sum()
    merged = holdings.merge(totals, on="portfolio", suffixes=("", "_total"))
    merged["weight"] = merged["weight"] / merged["weight_total"]
    return merged[["portfolio", "fund", "weight"]]


def classify_holdings(holdings: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    classified = holdings.copy()
    classified["asset_class"] = classified["fund"].map(mapping).fillna("Unclassified")
    return classified


def compute_actual_exposures(classified_holdings: pd.DataFrame) -> pd.DataFrame:
    return (
        classified_holdings
        .groupby(["portfolio", "asset_class"], as_index=False)["weight"]
        .sum()
        .rename(columns={"weight": "actual_weight"})
    )


def compute_benchmark_exposures(settings: pd.DataFrame) -> pd.DataFrame:
    required = {
        "portfolio",
        "equity_share",
        "norway_share_within_equity",
        "em_share_within_international_equity",
    }
    missing = required - set(settings.columns)
    if missing:
        raise RebalancerError(f"Mangler benchmark-kolonner: {', '.join(sorted(missing))}")

    rows: list[dict[str, Any]] = []
    for _, row in settings.iterrows():
        portfolio = str(row["portfolio"])
        equity_share = float(row["equity_share"])
        norway_share = float(row["norway_share_within_equity"])
        em_share = float(row["em_share_within_international_equity"])

        norway_equity = equity_share * norway_share
        intl_equity = max(0.0, equity_share - norway_equity)
        em_equity = intl_equity * em_share
        developed_equity = max(0.0, intl_equity - em_equity)
        fixed_income = max(0.0, 1.0 - equity_share)

        rows.extend(
            [
                {"portfolio": portfolio, "asset_class": "Equity Norway", "benchmark_weight": norway_equity},
                {"portfolio": portfolio, "asset_class": "Equity International DM", "benchmark_weight": developed_equity},
                {"portfolio": portfolio, "asset_class": "Equity EM", "benchmark_weight": em_equity},
                {"portfolio": portfolio, "asset_class": "Fixed Income", "benchmark_weight": fixed_income},
            ]
        )

    return pd.DataFrame(rows)


def compute_target_exposures(
    benchmark_exposures: pd.DataFrame,
    rebalance_mode: str,
    active_bets: pd.DataFrame | None = None,
    absolute_targets: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build target exposures based on mode.

    Modes:
    - go_to_benchmark: target = benchmark
    - reference_plus_active_bets: target = benchmark + configured active bets
    - absolute_targets: target = supplied absolute targets
    """
    if rebalance_mode == "go_to_benchmark":
        target = benchmark_exposures.copy()
        target = target.rename(columns={"benchmark_weight": "target_weight"})
        return target[["portfolio", "asset_class", "target_weight"]]

    if rebalance_mode == "reference_plus_active_bets":
        if active_bets is None:
            raise RebalancerError("active_bets må oppgis for mode=reference_plus_active_bets.")
        required = {"portfolio", "asset_class", "active_bet_adjustment"}
        missing = required - set(active_bets.columns)
        if missing:
            raise RebalancerError(f"Mangler active bet-kolonner: {', '.join(sorted(missing))}")

        target = benchmark_exposures.merge(
            active_bets[["portfolio", "asset_class", "active_bet_adjustment"]],
            on=["portfolio", "asset_class"],
            how="left",
        ).fillna({"active_bet_adjustment": 0.0})
        target["target_weight"] = target["benchmark_weight"] + target["active_bet_adjustment"]
        return target[["portfolio", "asset_class", "target_weight"]]

    if rebalance_mode == "absolute_targets":
        if absolute_targets is None:
            raise RebalancerError("absolute_targets må oppgis for mode=absolute_targets.")
        required = {"portfolio", "asset_class", "target_weight"}
        missing = required - set(absolute_targets.columns)
        if missing:
            raise RebalancerError(f"Mangler absolute target-kolonner: {', '.join(sorted(missing))}")
        return absolute_targets[["portfolio", "asset_class", "target_weight"]].copy()

    raise RebalancerError(
        "Ukjent rebalance_mode. Bruk go_to_benchmark, reference_plus_active_bets eller absolute_targets."
    )


def compute_active_bets(
    actual_exposures: pd.DataFrame,
    benchmark_exposures: pd.DataFrame,
    target_exposures: pd.DataFrame,
    minimum_trade_threshold: float,
) -> pd.DataFrame:
    merged = actual_exposures.merge(
        benchmark_exposures,
        on=["portfolio", "asset_class"],
        how="outer",
    ).merge(
        target_exposures,
        on=["portfolio", "asset_class"],
        how="outer",
    ).fillna(0.0)

    merged["active_bet"] = merged["actual_weight"] - merged["benchmark_weight"]
    merged["raw_trade"] = merged["target_weight"] - merged["actual_weight"]
    merged["suggested_trade"] = merged["raw_trade"].where(
        merged["raw_trade"].abs() >= float(minimum_trade_threshold),
        0.0,
    )

    return merged.sort_values(["portfolio", "asset_class"]).reset_index(drop=True)


def workbook_to_frames(file_bytes: bytes) -> dict[str, pd.DataFrame]:
    try:
        xls = pd.ExcelFile(BytesIO(file_bytes))
    except ImportError as exc:
        raise RebalancerError(
            "Excel-støtte mangler på serveren (openpyxl er ikke installert). "
            "Be drift installere openpyxl."
        ) from exc
    return {sheet: pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names}


def export_results_to_excel(result_frames: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for sheet_name, frame in result_frames.items():
                frame.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    except ImportError as exc:
        raise RebalancerError(
            "Excel-eksport er ikke tilgjengelig (openpyxl mangler på serveren). "
            "Be drift installere openpyxl."
        ) from exc
    output.seek(0)
    return output.read()
