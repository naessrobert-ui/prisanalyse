"""
portfolio_rebalancer_engine.py
------------------------------
Bridge module that exposes the API expected by portfolio_rebalancer_routes.py
while delegating business logic to portfolio_rebalancer.py.

Key design: workbook_to_frames() stores raw openpyxl rows (not DataFrames)
to avoid pandas column-deduplication issues with None/duplicate headers.
"""

from __future__ import annotations

import io
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook as openpyxl_load_workbook

import portfolio_rebalancer as _rb


# ---------------------------------------------------------------------------
# 1. Workbook I/O  — raw rows, no pandas deduplication
# ---------------------------------------------------------------------------

def workbook_to_frames(raw_bytes: bytes) -> Dict[str, Any]:
    """
    Returns {sheet_name: {"headers": list, "rows": list[list]}}
    Uses raw openpyxl values so None/duplicate column headers never cause
    pandas column-renaming issues.
    """
    wb = openpyxl_load_workbook(io.BytesIO(raw_bytes), data_only=True)
    frames: Dict[str, Any] = {}
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            frames[sheet_name] = {"headers": [], "rows": []}
        else:
            frames[sheet_name] = {
                "headers": list(all_rows[0]),
                "rows": [list(r) for r in all_rows[1:]],
            }
    return frames


def detect_portfolio_sheets(frames: Dict[str, Any]) -> List[str]:
    """Return sheet names that contain the required portfolio columns."""
    required = {"portfolio", "security name", "mv"}
    detected = []
    for name, frame in frames.items():
        cols = {_rb.normalize_text(h).lower() for h in frame["headers"] if h is not None}
        if required.issubset(cols):
            detected.append(name)
    return detected


# ---------------------------------------------------------------------------
# 2. Holdings extraction
# ---------------------------------------------------------------------------

def extract_holdings(
    frames: Dict[str, Any],
    sheets: List[str],
    exposure_column: str = "Lev. expo. distr. (PF)",
) -> pd.DataFrame:
    """
    Extract holding rows from raw frames dict.
    Mirrors read_workbook_holdings() logic from portfolio_rebalancer.py
    but works on in-memory raw rows instead of a file path.
    """
    exposure_col_norm = _rb.normalize_text(exposure_column).lower()
    records: List[Dict[str, Any]] = []

    for sheet_name in sheets:
        frame = frames.get(sheet_name)
        if not frame or not frame["headers"]:
            continue

        raw_headers = frame["headers"]
        # Build normalized_name -> column_index mapping
        col_map: Dict[str, int] = {}
        for idx, h in enumerate(raw_headers):
            norm = _rb.normalize_text(h)
            if norm:  # skip empty/None headers
                col_map[norm.lower()] = idx

        required = ["portfolio", "security name", "mv", exposure_col_norm]
        missing = [r for r in required if r not in col_map]
        if missing:
            raise _rb.DataError(
                f"Fanen '{sheet_name}' mangler kolonner: {missing}. "
                f"Fant: {[_rb.normalize_text(h) for h in raw_headers if h is not None]}"
            )

        portfolio_idx = col_map["portfolio"]
        security_idx = col_map["security name"]
        mv_idx = col_map["mv"]
        expo_idx = col_map[exposure_col_norm]
        model_idx = col_map.get("model portfolio")
        country_idx = col_map.get("country")

        for row_number, row in enumerate(frame["rows"], start=2):
            def get(idx):
                return row[idx] if idx is not None and idx < len(row) else None

            expo = get(expo_idx)
            if expo is None:
                continue

            security_name = _rb.normalize_text(get(security_idx))
            if not security_name:
                continue

            model_portfolio = _rb.normalize_text(get(model_idx)) if model_idx is not None else ""
            is_cash_position = security_name.startswith("PB Norway:")
            is_security_holding = bool(model_portfolio)
            if not (is_cash_position or is_security_holding):
                continue

            records.append({
                "sheet_name": sheet_name,
                "portfolio": sheet_name,
                "internal_portfolio_name": _rb.normalize_text(get(portfolio_idx)),
                "row_number": row_number,
                "security_name": security_name,
                "country": _rb.normalize_text(get(country_idx)) if country_idx is not None else "",
                "model_portfolio": model_portfolio,
                "mv": _rb.safe_float(get(mv_idx)),
                "exposure_pct": _rb.safe_float(expo),
            })

    if not records:
        raise _rb.DataError("Fant ingen beholdningsrader i arbeidsboken.")

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 3. Classification
# ---------------------------------------------------------------------------

def classify_holdings(holdings: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Classify using a flat {security_name: category} dict from the UI."""
    config = {"security_categories": mapping}
    return _rb.classify_holdings(holdings, config)


# ---------------------------------------------------------------------------
# 4. Exposure computations
# ---------------------------------------------------------------------------

def compute_actual_exposures(classified: pd.DataFrame) -> pd.DataFrame:
    return _rb.summarize_current_weights(classified)


def compute_benchmark_exposures(benchmark_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert benchmark_json rows to per-portfolio reference weights.
    Input columns: portfolio, equity_share, norway_share_within_equity,
                   em_share_within_international_equity
    Output columns: portfolio, category, reference_weight_pct
    """
    rows: List[Dict[str, Any]] = []
    for _, r in benchmark_df.iterrows():
        portfolio = str(r["portfolio"])
        equity_share = float(r.get("equity_share", 0.60))
        norway_share = float(r.get("norway_share_within_equity", 0.20))
        em_share = float(r.get("em_share_within_international_equity", 0.15))

        equity_total = equity_share * 100.0
        equity_norway = equity_total * norway_share
        international_equity = equity_total - equity_norway
        equity_em = international_equity * em_share
        equity_dm = international_equity - equity_em
        fi_total = 100.0 - equity_total
        fi_norway = fi_total * 0.5
        fi_global = fi_total - fi_norway

        rows.extend([
            {"portfolio": portfolio, "category": "cash",                   "reference_weight_pct": 0.0},
            {"portfolio": portfolio, "category": "allocation",             "reference_weight_pct": 0.0},
            {"portfolio": portfolio, "category": "equity_norway",          "reference_weight_pct": equity_norway},
            {"portfolio": portfolio, "category": "equity_global_developed","reference_weight_pct": equity_dm},
            {"portfolio": portfolio, "category": "equity_global_em",       "reference_weight_pct": equity_em},
            {"portfolio": portfolio, "category": "fi_norway_short",        "reference_weight_pct": fi_norway * 0.5},
            {"portfolio": portfolio, "category": "fi_norway_long",         "reference_weight_pct": fi_norway * 0.5},
            {"portfolio": portfolio, "category": "fi_global",              "reference_weight_pct": fi_global},
        ])
    return pd.DataFrame(rows)


def compute_target_exposures(
    benchmark_exposures: pd.DataFrame,
    rebalance_mode: str = "go_to_benchmark",
    active_bets: Optional[pd.DataFrame] = None,
    absolute_targets: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for portfolio in benchmark_exposures["portfolio"].unique():
        ref = (
            benchmark_exposures[benchmark_exposures["portfolio"] == portfolio]
            .set_index("category")["reference_weight_pct"]
            .to_dict()
        )
        target = deepcopy(ref)

        if rebalance_mode == "reference_plus_active" and active_bets is not None and not active_bets.empty:
            port_bets = active_bets[active_bets["portfolio"] == portfolio] if "portfolio" in active_bets.columns else active_bets
            for _, bet_row in port_bets.iterrows():
                cat = str(bet_row.get("category", ""))
                val = float(bet_row.get("active_bet_pct", 0.0))
                if cat in target:
                    target[cat] = target.get(cat, 0.0) + val
            residual = 100.0 - sum(target.values())
            if abs(residual) > 1e-9:
                target["fi_global"] = target.get("fi_global", 0.0) + residual

        elif rebalance_mode == "absolute_targets" and absolute_targets is not None and not absolute_targets.empty:
            port_abs = absolute_targets[absolute_targets["portfolio"] == portfolio] if "portfolio" in absolute_targets.columns else absolute_targets
            target = {str(r["category"]): float(r["target_weight_pct"]) for _, r in port_abs.iterrows()}

        for cat, weight in target.items():
            rows.append({"portfolio": portfolio, "category": cat, "target_weight_pct": weight})

    return pd.DataFrame(rows)


def compute_active_bets(
    actual: pd.DataFrame,
    benchmark: pd.DataFrame,
    target: pd.DataFrame,
    minimum_trade_threshold: float = 0.01,
) -> pd.DataFrame:
    merged = (
        actual
        .merge(benchmark[["portfolio", "category", "reference_weight_pct"]], on=["portfolio", "category"], how="outer")
        .merge(target[["portfolio", "category", "target_weight_pct"]], on=["portfolio", "category"], how="outer")
    )
    merged["current_weight_pct"]   = merged["current_weight_pct"].fillna(0.0)
    merged["reference_weight_pct"] = merged["reference_weight_pct"].fillna(0.0)
    merged["target_weight_pct"]    = merged["target_weight_pct"].fillna(0.0)
    merged["active_bet_vs_reference_pct"] = merged["current_weight_pct"] - merged["reference_weight_pct"]
    merged["trade_weight_pct"]     = merged["target_weight_pct"] - merged["current_weight_pct"]
    merged["skip_due_to_minimum"]  = merged["trade_weight_pct"].abs() < minimum_trade_threshold
    return merged.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 5. Excel export
# ---------------------------------------------------------------------------

def export_results_to_excel(frames: Dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in frames.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    buf.seek(0)
    return buf.read()
