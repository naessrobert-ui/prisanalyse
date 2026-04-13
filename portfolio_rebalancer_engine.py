from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any

import pandas as pd

PORTFOLIO_SHEET_KEYWORDS = ("portfolio", "portef", "behold", "holding")
FUND_NAME_ALIASES = (
    "fund",
    "fond",
    "instrument",
    "ticker",
    "name",
    "navn",
    "security",
    "security name",
    "model portfolio",
)
WEIGHT_ALIASES = (
    "weight",
    "vekt",
    "allocation",
    "andel",
    "exposure",
    "expo",
    "lev. expo",
    "lev expo",
    "lev",
)
MV_ALIASES = ("mv", "market value", "markedsverdi")


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


def _parse_weight_value(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace("%", "").replace("\u00a0", "").replace(" ", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    else:
        text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def extract_holdings(workbook: dict[str, pd.DataFrame], sheets: list[str]) -> pd.DataFrame:
    rows: list[PortfolioHolding] = []
    attempted: list[str] = []
    for sheet in sheets:
        df = workbook[sheet]
        columns = list(df.columns)
        normalized_to_source = {_normalize_col(c): c for c in columns}

        # Prefer Nordea-style explicit columns if present.
        fund_col = normalized_to_source.get("security name")
        preferred_weight = None
        for candidate in (
            "lev. expo. distr. (pf)",
            "lev. expo. distr.pre-sim.",
            "lev. expo. distr., sim.",
        ):
            if candidate in normalized_to_source:
                preferred_weight = normalized_to_source[candidate]
                break

        if not fund_col:
            fund_col = _pick_column(columns, FUND_NAME_ALIASES)
        weight_col = preferred_weight or _pick_column(columns, WEIGHT_ALIASES)
        if not fund_col or not weight_col:
            attempted.append(f"{sheet}: columns={list(df.columns)}")
            continue

        mv_col = _pick_column(columns, MV_ALIASES)
        model_portfolio_col = normalized_to_source.get("model portfolio")
        selected_cols = [fund_col, weight_col] + ([mv_col] if mv_col else []) + ([model_portfolio_col] if model_portfolio_col else [])
        subset = df[selected_cols].copy()
        subset[fund_col] = subset[fund_col].astype(str).str.strip()
        subset[weight_col] = subset[weight_col].apply(_parse_weight_value)
        if mv_col:
            subset[mv_col] = subset[mv_col].apply(_parse_weight_value)
        subset = subset[subset[fund_col] != ""]
        # Nordea-ark: behold kun faktiske holdings (Model portfolio utfylt).
        if model_portfolio_col:
            subset = subset[subset[model_portfolio_col].astype(str).str.strip() != ""]
        # Hvis vekt mangler men MV finnes, fyll med MV-andel.
        if mv_col and subset[mv_col].notna().any():
            mv_total = float(subset[mv_col].fillna(0.0).sum())
            if mv_total > 0:
                mv_share = subset[mv_col].fillna(0.0) / mv_total
                subset[weight_col] = subset[weight_col].where(subset[weight_col].notna(), mv_share)
        subset = subset.dropna(subset=[weight_col])

        for _, row in subset.iterrows():
            rows.append(
                PortfolioHolding(
                    portfolio=sheet,
                    fund=str(row[fund_col]).strip(),
                    weight=float(row[weight_col]),
                )
            )

    if not rows:
        details = "; ".join(attempted[:3])
        raise RebalancerError(
            "Fant ingen fond/vekter i valgte ark. Sjekk at arbeidsboken har kolonner som "
            "f.eks. 'Fund'/'Security name' og 'Weight'/'Exposure'. "
            f"Debug: {details}"
        )

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
        cash_target = float(row.get("cash_target", 0.0) or 0.0)
        allocation_target = float(row.get("allocation_target", 0.0) or 0.0)
        fi_norway_within_fi = float(row.get("fi_norway_within_fi", 0.5) or 0.5)
        fi_long_within_norway_fi = float(row.get("fi_long_within_norway_fi", 0.5) or 0.5)

        norway_equity = equity_share * norway_share
        intl_equity = max(0.0, equity_share - norway_equity)
        em_equity = intl_equity * em_share
        developed_equity = max(0.0, intl_equity - em_equity)
        fi_total = max(0.0, 1.0 - equity_share - cash_target - allocation_target)
        fi_norway_total = fi_total * fi_norway_within_fi
        fi_global = max(0.0, fi_total - fi_norway_total)
        fi_norway_long = fi_norway_total * fi_long_within_norway_fi
        fi_norway_short = max(0.0, fi_norway_total - fi_norway_long)

        rows.extend(
            [
                {"portfolio": portfolio, "asset_class": "cash", "benchmark_weight": cash_target},
                {"portfolio": portfolio, "asset_class": "allocation", "benchmark_weight": allocation_target},
                {"portfolio": portfolio, "asset_class": "equity_norway", "benchmark_weight": norway_equity},
                {"portfolio": portfolio, "asset_class": "equity_global_developed", "benchmark_weight": developed_equity},
                {"portfolio": portfolio, "asset_class": "equity_global_em", "benchmark_weight": em_equity},
                {"portfolio": portfolio, "asset_class": "fi_norway_short", "benchmark_weight": fi_norway_short},
                {"portfolio": portfolio, "asset_class": "fi_norway_long", "benchmark_weight": fi_norway_long},
                {"portfolio": portfolio, "asset_class": "fi_global", "benchmark_weight": fi_global},
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
    frames: dict[str, pd.DataFrame] = {}
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        normalized_cols = [_normalize_col(c) for c in df.columns]
        if all(col.startswith("unnamed:") or col == "" for col in normalized_cols):
            df_alt = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=1)
            alt_cols = [_normalize_col(c) for c in df_alt.columns]
            if any(any(alias in c for alias in FUND_NAME_ALIASES + WEIGHT_ALIASES) for c in alt_cols):
                df = df_alt
        frames[sheet] = df
    return frames


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
