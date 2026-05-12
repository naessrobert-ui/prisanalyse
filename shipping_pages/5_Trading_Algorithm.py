import warnings

warnings.filterwarnings("ignore")

import hashlib
import json
import pickle
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from numpy.linalg import LinAlgError

from shipping_datahandling._4_Datahandling_Implicit_Rate import MissingFinancialsDataError
from shipping_datahandling._5_Datahandling_Trading_Algorithm import load_cached_or_refresh_trading_model_data
from shipping_app.data import get_company_ticker
from shipping_app.ui import render_sidebar


DEFAULT_START_DATE = "2015-01-01"
DEFAULT_TRAIN_END = "2023-12-31"
DEFAULT_HORIZONS = ["3m", "6m", "12m"]
DEFAULT_HORIZON_SHIFTS = {"3m": 0, "6m": 0, "12m": 0}
DEFAULT_SIGNAL_LOOKBACKS = {"3m": 126, "6m": 84, "12m": 252}
TRANSACTION_COST_BPS = 5
TRANSACTION_COST_RATE = TRANSACTION_COST_BPS / 10000
TRADING_MODEL_SIGNAL_VERSION = 10
_SHIPPING_BASE = Path(__file__).resolve().parent.parent / "shipping_app"
SHIPPING_INDEX_PATH = _SHIPPING_BASE / "Data" / "Shipping_Index.csv"
TRADING_OUTPUT_DIR = _SHIPPING_BASE / "Output" / "Trading_Algorithm"
TRADING_MODEL_CACHE_DIR = TRADING_OUTPUT_DIR / "Model_Snapshots"
SPREAD_MODE_OPTIONS = {
    "ratio": "Ratio",
    "log": "Log",
    "residual": "Residual",
}


def _normalize_signal_lookbacks(value: int | float | dict | None, horizons: list[str] | None = None) -> dict[str, int]:
    horizons = horizons if horizons is not None else DEFAULT_HORIZONS
    if isinstance(value, dict):
        fallback = int(value.get("6m", DEFAULT_SIGNAL_LOOKBACKS["6m"]))
        return {
            horizon: int(value.get(horizon, DEFAULT_SIGNAL_LOOKBACKS.get(horizon, fallback)))
            for horizon in horizons
        }
    if value is None:
        return {horizon: int(DEFAULT_SIGNAL_LOOKBACKS.get(horizon, DEFAULT_SIGNAL_LOOKBACKS["6m"])) for horizon in horizons}
    shared_value = int(value)
    return {horizon: shared_value for horizon in horizons}


RECOMMENDED_RESEARCH_SETTINGS = {
    "spread_mode": "residual",
    "signal_lookbacks": DEFAULT_SIGNAL_LOOKBACKS.copy(),
    "half_life_lookback": 252,
    "min_half_life": 10,
    "max_half_life": 60,
    "threshold_min": 0.2,
    "threshold_max": 1.2,
    "threshold_step": 0.1,
    "state_threshold_buffer": 0.2,
}
TUNING_CANDIDATES = {
    "spread_mode": ["residual", "log", "ratio"],
    "half_life_lookback": [126, 252],
    "min_half_life": [5, 10],
    "max_half_life": [45, 60, 90],
}


class TradingModel:
    def __init__(
        self,
        df: pd.DataFrame,
        price_col: str,
        train_end: str = DEFAULT_TRAIN_END,
        threshold_grid: np.ndarray | None = None,
        horizons: list[str] | None = None,
        horizon_shifts: dict[str, int] | None = None,
        spread_mode: str = "ratio",
        signal_lookbacks: int | dict[str, int] | None = None,
        half_life_lookback: int = 252,
        min_half_life: int = 5,
        max_half_life: int = 60,
        state_threshold_buffer: float = 0.0,
    ):
        self.train_end = train_end
        self.threshold_grid = threshold_grid if threshold_grid is not None else np.arange(0.0, 2.0, 0.01)
        self.horizons = horizons if horizons is not None else DEFAULT_HORIZONS
        self.horizon_shifts = horizon_shifts if horizon_shifts is not None else DEFAULT_HORIZON_SHIFTS
        self.spread_mode = spread_mode
        self.signal_lookbacks = _normalize_signal_lookbacks(signal_lookbacks, self.horizons)
        self.half_life_lookback = half_life_lookback
        self.min_half_life = min_half_life
        self.max_half_life = max_half_life
        self.state_threshold_buffer = max(0.0, float(state_threshold_buffer))

        self.df = df.copy()
        self.price_col = price_col
        self.train_df: pd.DataFrame | None = None
        self.test_df: pd.DataFrame | None = None
        self.best_thresholds: dict[str, dict[str, float]] = {}
        self.bt_results: dict[str, dict[str, pd.DataFrame]] = {}
        self.spread_models: dict[str, dict[str, float | str]] = {}

    def build_fleet_rates(self) -> None:
        count_cols = {
            "Aframax/LR2": "Aframax/LR2_Count",
            "MR": "MR_Count",
            "Suezmax": "Suezmax_Count",
            "VLCC": "VLCC_Count",
        }
        implicit_cols = {
            "Aframax/LR2": "Aframax/LR2_ImplicitDayrate",
            "MR": "MR_ImplicitDayrate",
            "Suezmax": "Suezmax_ImplicitDayrate",
            "VLCC": "VLCC_ImplicitDayrate",
        }
        pred_cols = {
            "3m": {
                "Aframax/LR2": "Aframax/LR2_predicted_values_3m",
                "MR": "MR_predicted_values_3m",
                "Suezmax": "Suezmax_predicted_values_3m",
                "VLCC": "VLCC_predicted_values_3m",
            },
            "6m": {
                "Aframax/LR2": "Aframax/LR2_predicted_values_6m",
                "MR": "MR_predicted_values_6m",
                "Suezmax": "Suezmax_predicted_values_6m",
                "VLCC": "VLCC_predicted_values_6m",
            },
            "12m": {
                "Aframax/LR2": "Aframax/LR2_predicted_values_12m",
                "MR": "MR_predicted_values_12m",
                "Suezmax": "Suezmax_predicted_values_12m",
                "VLCC": "VLCC_predicted_values_12m",
            },
        }

        self.df["fleet_count"] = sum(self.df[c].fillna(0) for c in count_cols.values())

        implicit_num = 0
        for vessel, col in implicit_cols.items():
            implicit_num += self.df[col].fillna(0) * self.df[count_cols[vessel]].fillna(0)
        self.df["fleet_implicit"] = implicit_num / self.df["fleet_count"]

        for horizon in self.horizons:
            pred_num = 0
            for vessel, col in pred_cols[horizon].items():
                pred_num += self.df[col].fillna(0) * self.df[count_cols[vessel]].fillna(0)
            self.df[f"fleet_pred_{horizon}"] = pred_num / self.df["fleet_count"]
            self.df[f"fleet_implicit_{horizon}"] = self.df["fleet_implicit"].shift(self.horizon_shifts[horizon])

    @staticmethod
    def _fit_residual_model(train_pred: pd.Series, train_implicit: pd.Series) -> tuple[float, float]:
        x = pd.to_numeric(train_implicit, errors="coerce").to_numpy(dtype=float)
        y = pd.to_numeric(train_pred, errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(x) & np.isfinite(y)
        if mask.sum() < 2:
            return 0.0, 1.0

        try:
            beta, alpha = np.polyfit(x[mask], y[mask], deg=1)
        except (LinAlgError, ValueError):
            return 0.0, 1.0

        return float(alpha), float(beta)

    def build_spreads(self) -> None:
        train_mask = self.df["Date"] <= pd.Timestamp(self.train_end)

        for horizon in self.horizons:
            pred_col = f"fleet_pred_{horizon}"
            implicit_col = f"fleet_implicit_{horizon}"
            spread_col = f"spread_{horizon}"

            pred = pd.to_numeric(self.df[pred_col], errors="coerce")
            implicit = pd.to_numeric(self.df[implicit_col], errors="coerce")

            if self.spread_mode == "ratio":
                spread = pred / implicit - 1
                self.spread_models[horizon] = {"mode": "ratio"}
            elif self.spread_mode == "log":
                spread = np.log(pred.clip(lower=1.0)) - np.log(implicit.clip(lower=1.0))
                self.spread_models[horizon] = {"mode": "log"}
            elif self.spread_mode == "residual":
                alpha, beta = self._fit_residual_model(
                    pred.loc[train_mask],
                    implicit.loc[train_mask],
                )
                spread = pred - (alpha + beta * implicit)
                self.spread_models[horizon] = {
                    "mode": "residual",
                    "alpha": alpha,
                    "beta": beta,
                }
            else:
                raise ValueError(f"Unsupported spread mode: {self.spread_mode}")

            self.df[spread_col] = spread.replace([np.inf, -np.inf], np.nan)

    def build_signals(self) -> None:
        for horizon in self.horizons:
            pred_col = f"fleet_pred_{horizon}"
            spread_col = f"spread_{horizon}"
            shifted_spread = self.df[spread_col].shift(1)
            signal_lookback = self.signal_lookbacks[horizon]
            min_periods = max(20, signal_lookback // 4)
            rolling_mean = shifted_spread.rolling(signal_lookback, min_periods=min_periods).mean()
            rolling_std = shifted_spread.rolling(signal_lookback, min_periods=min_periods).std()
            rolling_std = rolling_std.replace(0, np.nan)
            self.df[f"signal_{horizon}"] = (self.df[spread_col] - rolling_mean) / rolling_std
            self.df[f"prediction_change_{horizon}"] = self.df[pred_col].diff()
            self.df[f"spread_change_{horizon}"] = self.df[spread_col].diff()
            self.df[f"signal_fresh_{horizon}"] = self.df[spread_col].notna() & self.df[spread_col].ne(self.df[spread_col].shift(1))

    @staticmethod
    def _estimate_half_life(window: pd.Series) -> float:
        clean = pd.to_numeric(window, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(clean) < 20:
            return np.nan

        lagged = clean.shift(1).dropna()
        current = clean.loc[lagged.index]
        if len(lagged) < 20:
            return np.nan

        try:
            phi, _ = np.polyfit(lagged.to_numpy(dtype=float), current.to_numpy(dtype=float), deg=1)
        except (LinAlgError, ValueError):
            return np.nan

        if not np.isfinite(phi) or phi <= 0 or phi >= 1:
            return np.nan

        return float(-np.log(2) / np.log(phi))

    def build_half_life_filters(self) -> None:
        min_periods = max(20, self.half_life_lookback // 4)

        for horizon in self.horizons:
            spread_col = f"spread_{horizon}"
            half_life_col = f"half_life_{horizon}"
            allowed_col = f"trade_allowed_{horizon}"

            half_life_series = (
                self.df[spread_col]
                .shift(1)
                .rolling(self.half_life_lookback, min_periods=min_periods)
                .apply(self._estimate_half_life, raw=False)
            )
            self.df[half_life_col] = half_life_series
            self.df[allowed_col] = half_life_series.between(
                self.min_half_life,
                self.max_half_life,
                inclusive="both",
            )

        train_mask = self.df["Date"] <= pd.Timestamp(self.train_end)
        self.train_df = self.df.loc[train_mask].copy()
        self.test_df = self.df.loc[~train_mask].copy()

    @staticmethod
    def performance_stats(returns: pd.Series) -> pd.Series:
        r = returns.dropna().copy()
        if len(r) == 0:
            return pd.Series(
                {
                    "total_return": np.nan,
                    "annual_return": np.nan,
                    "annual_vol": np.nan,
                    "sharpe": np.nan,
                    "max_drawdown": np.nan,
                    "days": 0,
                }
            )

        equity = (1 + r).cumprod()
        total_return = equity.iloc[-1] - 1
        annual_return = (1 + total_return) ** (252 / len(r)) - 1
        annual_vol = r.std() * np.sqrt(252)
        sharpe = (r.mean() / r.std() * np.sqrt(252)) if r.std() != 0 else np.nan
        max_drawdown = (equity / equity.cummax() - 1).min()

        return pd.Series(
            {
                "total_return": total_return,
                "annual_return": annual_return,
                "annual_vol": annual_vol,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
                "days": len(r),
            }
        )

    @staticmethod
    def trade_count(position: pd.Series) -> int:
        previous_position = position.shift(1).fillna(0)
        return int((position != previous_position).sum())

    @staticmethod
    def build_positions(
        signal: pd.Series,
        upper_bound: float,
        lower_bound: float,
        state_threshold_buffer: float = 0.0,
        trade_allowed: pd.Series | None = None,
        signal_fresh: pd.Series | None = None,
        signal_change: pd.Series | None = None,
    ) -> pd.Series:
        positions = np.zeros(len(signal), dtype=float)
        current_position = 0.0
        buffer = max(0.0, float(state_threshold_buffer))
        for idx, value in enumerate(signal):
            allowed = True if trade_allowed is None else bool(trade_allowed.iloc[idx])
            fresh = True if signal_fresh is None else bool(signal_fresh.iloc[idx])
            change = np.nan if signal_change is None else float(signal_change.iloc[idx])
            can_trade = allowed and fresh
            effective_upper, effective_lower = TradingModel.state_adjusted_bounds(
                upper_bound,
                lower_bound,
                current_position,
                buffer,
            )
            if can_trade and change > 0 and value >= effective_upper:
                current_position = 1.0
            elif can_trade and change < 0 and value <= effective_lower:
                current_position = 0.0
            positions[idx] = current_position
        return pd.Series(positions, index=signal.index, name="position")

    @staticmethod
    def state_adjusted_bounds(upper: float, lower: float, current_state: float, buffer: float) -> tuple[float, float]:
        buffer = max(0.0, float(buffer))
        if current_state >= 0.75:
            return upper + buffer, lower + buffer
        if current_state <= 0.25:
            return upper - buffer, lower - buffer
        return upper, lower

    @staticmethod
    def backtest_signal(
        data: pd.DataFrame,
        signal_col: str,
        upper_bound: float,
        lower_bound: float,
        state_threshold_buffer: float = 0.0,
        trade_allowed_col: str | None = None,
        signal_fresh_col: str | None = None,
        signal_change_col: str | None = None,
    ) -> pd.DataFrame:
        out = data.copy()
        trade_allowed = out[trade_allowed_col] if trade_allowed_col and trade_allowed_col in out.columns else None
        signal_fresh = out[signal_fresh_col] if signal_fresh_col and signal_fresh_col in out.columns else None
        signal_change = out[signal_change_col] if signal_change_col and signal_change_col in out.columns else None
        out["position"] = TradingModel.build_positions(
            out[signal_col],
            upper_bound,
            lower_bound,
            state_threshold_buffer=state_threshold_buffer,
            trade_allowed=trade_allowed,
            signal_fresh=signal_fresh,
            signal_change=signal_change,
        )
        out["signal_position"] = out["position"]
        out["decision_state"] = out["signal_position"].shift(1).fillna(0)
        adjusted_bounds = out["decision_state"].apply(
            lambda state: TradingModel.state_adjusted_bounds(
                upper_bound,
                lower_bound,
                state,
                state_threshold_buffer,
            )
        )
        out["effective_upper_bound"] = adjusted_bounds.apply(lambda bounds: bounds[0])
        out["effective_lower_bound"] = adjusted_bounds.apply(lambda bounds: bounds[1])
        out["position"] = out["signal_position"].shift(1).fillna(0)
        previous_position = out["position"].shift(1).fillna(0)
        out["trade_flag"] = (out["position"] != previous_position).astype(int)
        out["transaction_cost"] = out["trade_flag"] * TRANSACTION_COST_RATE
        out["gross_strategy_return"] = out["position"] * out["stock_return_1d"]
        out["strategy_return"] = out["gross_strategy_return"] - out["transaction_cost"]
        out["trading_algorithm_curve"] = (1 + out["strategy_return"].fillna(0)).cumprod()
        out["buy_hold_curve"] = (1 + out["stock_return_1d"].fillna(0)).cumprod()
        return out

    def tune_thresholds(self, train_data: pd.DataFrame, signal_col: str, upper_grid: np.ndarray) -> pd.DataFrame:
        rows = []
        horizon = signal_col.replace("signal_", "")
        trade_allowed_col = f"trade_allowed_{horizon}"
        signal_fresh_col = f"signal_fresh_{horizon}"
        signal_change_col = f"spread_change_{horizon}"

        for upper_bound in upper_grid:
            for lower_magnitude in upper_grid:
                lower_bound = -float(lower_magnitude)

                bt = self.backtest_signal(
                    train_data,
                    signal_col,
                    upper_bound,
                    lower_bound,
                    state_threshold_buffer=self.state_threshold_buffer,
                    trade_allowed_col=trade_allowed_col,
                    signal_fresh_col=signal_fresh_col,
                    signal_change_col=signal_change_col,
                )
                stats = self.performance_stats(bt["strategy_return"])
                active_days = int((bt["position"] != 0).sum())
                rows.append(
                    {
                        "upper_bound": upper_bound,
                        "lower_bound": lower_bound,
                        "band_width": upper_bound + lower_magnitude,
                        "active_days": active_days,
                        "trades": self.trade_count(bt["position"]),
                        **stats.to_dict(),
                    }
                )

        tuned = pd.DataFrame(rows)
        if tuned.empty:
            return tuned

        tuned["valid_candidate"] = (
            tuned["sharpe"].notna()
            & np.isfinite(tuned["sharpe"])
            & (tuned["trades"] >= 3)
            & (tuned["active_days"] >= 20)
        )
        tuned["fallback_candidate"] = tuned["sharpe"].notna() & np.isfinite(tuned["sharpe"])
        tuned["candidate_rank"] = np.select(
            [tuned["valid_candidate"], tuned["fallback_candidate"]],
            [2, 1],
            default=0,
        )
        tuned = tuned.sort_values(
            ["candidate_rank", "sharpe", "total_return", "band_width"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        return tuned

    def run_tuning(self) -> None:
        for horizon in self.horizons:
            tuning = self.tune_thresholds(self.train_df, f"signal_{horizon}", self.threshold_grid)
            if tuning.empty:
                raise ValueError(f"No threshold candidates available for horizon {horizon}.")
            self.best_thresholds[horizon] = {
                "upper": float(tuning.iloc[0]["upper_bound"]),
                "lower": float(tuning.iloc[0]["lower_bound"]),
            }

    def run_backtests(self) -> None:
        for horizon in self.horizons:
            bt_train = self.backtest_signal(
                self.train_df,
                f"signal_{horizon}",
                self.best_thresholds[horizon]["upper"],
                self.best_thresholds[horizon]["lower"],
                state_threshold_buffer=self.state_threshold_buffer,
                trade_allowed_col=f"trade_allowed_{horizon}",
                signal_fresh_col=f"signal_fresh_{horizon}",
                signal_change_col=f"spread_change_{horizon}",
            )
            bt_test = self.backtest_signal(
                self.test_df,
                f"signal_{horizon}",
                self.best_thresholds[horizon]["upper"],
                self.best_thresholds[horizon]["lower"],
                state_threshold_buffer=self.state_threshold_buffer,
                trade_allowed_col=f"trade_allowed_{horizon}",
                signal_fresh_col=f"signal_fresh_{horizon}",
                signal_change_col=f"spread_change_{horizon}",
            )
            bt_full = self.backtest_signal(
                self.df,
                f"signal_{horizon}",
                self.best_thresholds[horizon]["upper"],
                self.best_thresholds[horizon]["lower"],
                state_threshold_buffer=self.state_threshold_buffer,
                trade_allowed_col=f"trade_allowed_{horizon}",
                signal_fresh_col=f"signal_fresh_{horizon}",
                signal_change_col=f"spread_change_{horizon}",
            )
            self.bt_results[horizon] = {"train": bt_train, "test": bt_test, "full": bt_full}

    def run(self) -> None:
        self.build_fleet_rates()
        self.build_spreads()
        self.build_signals()
        self.build_half_life_filters()
        self.run_tuning()
        self.run_backtests()


def _format_pct(value: float) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{value:.2%}"


def _format_num(value: float) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}"


def _build_threshold_grid(min_value: float, max_value: float, step: float) -> np.ndarray:
    return np.arange(min_value, max_value + step / 2, step)


def load_shipping_index_curve(start_date: pd.Timestamp | str | None = None) -> pd.Series | None:
    if not SHIPPING_INDEX_PATH.exists():
        return None

    shipping_index = pd.read_csv(SHIPPING_INDEX_PATH, skiprows=[1])
    if "Date" not in shipping_index.columns or "Adj Close" not in shipping_index.columns:
        return None

    shipping_index["Date"] = pd.to_datetime(shipping_index["Date"], errors="coerce")
    shipping_index["Adj Close"] = pd.to_numeric(shipping_index["Adj Close"], errors="coerce")
    shipping_index = shipping_index.dropna(subset=["Date", "Adj Close"]).sort_values("Date").copy()

    if start_date is not None:
        shipping_index = shipping_index.loc[shipping_index["Date"] >= pd.Timestamp(start_date)].copy()

    if shipping_index.empty:
        return None

    base_value = shipping_index["Adj Close"].iloc[0]
    if pd.isna(base_value) or base_value == 0:
        return None

    shipping_index["shipping_index_curve"] = shipping_index["Adj Close"] / base_value
    return shipping_index.set_index("Date")["shipping_index_curve"]


def _run_model(
    df: pd.DataFrame,
    price_col: str,
    *,
    train_end: str,
    spread_mode: str,
    signal_lookbacks: dict[str, int],
    half_life_lookback: int,
    min_half_life: int,
    max_half_life: int,
    state_threshold_buffer: float,
    threshold_grid: np.ndarray,
) -> TradingModel:
    model = TradingModel(
        df=df,
        price_col=price_col,
        train_end=train_end,
        threshold_grid=threshold_grid,
        spread_mode=spread_mode,
        signal_lookbacks=signal_lookbacks,
        half_life_lookback=half_life_lookback,
        min_half_life=min_half_life,
        max_half_life=max_half_life,
        state_threshold_buffer=state_threshold_buffer,
    )
    model.run()
    return model


def _controls_snapshot_payload(
    *,
    train_end: str,
    spread_mode: str,
    signal_lookbacks: dict[str, int],
    half_life_lookback: int,
    min_half_life: int,
    max_half_life: int,
    state_threshold_buffer: float,
    threshold_grid: np.ndarray,
) -> dict[str, object]:
    return {
        "signal_version": TRADING_MODEL_SIGNAL_VERSION,
        "train_end": str(train_end),
        "spread_mode": str(spread_mode),
        "signal_lookbacks": {horizon: int(value) for horizon, value in signal_lookbacks.items()},
        "half_life_lookback": int(half_life_lookback),
        "min_half_life": int(min_half_life),
        "max_half_life": int(max_half_life),
        "state_threshold_buffer": float(state_threshold_buffer),
        "threshold_grid": [float(value) for value in threshold_grid.tolist()],
    }


def _model_cache_dependency(cache_meta: dict | None) -> dict[str, object]:
    if not cache_meta:
        return {}
    return {
        "schema_version": cache_meta.get("schema_version"),
        "price_col": cache_meta.get("price_col"),
        "row_count": cache_meta.get("row_count"),
        "cached_from": cache_meta.get("cached_from"),
        "cached_through": cache_meta.get("cached_through"),
        "data_mtime_ns": cache_meta.get("data_mtime_ns"),
    }


def _model_snapshot_path(company: str, controls_payload: dict[str, object]) -> Path:
    TRADING_MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe_company = company.lower().replace(" ", "_")
    digest = hashlib.sha256(json.dumps(controls_payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return TRADING_MODEL_CACHE_DIR / f"{safe_company}_{digest}.pkl"


def _restore_model_from_snapshot(snapshot: dict) -> TradingModel:
    controls = snapshot["controls"]
    model = TradingModel(
        df=snapshot["df"],
        price_col=snapshot["price_col"],
        train_end=controls["train_end"],
        threshold_grid=np.array(controls["threshold_grid"], dtype=float),
        spread_mode=controls["spread_mode"],
        signal_lookbacks=_normalize_signal_lookbacks(
            controls.get("signal_lookbacks", controls.get("signal_lookback")),
            DEFAULT_HORIZONS,
        ),
        half_life_lookback=int(controls["half_life_lookback"]),
        min_half_life=int(controls["min_half_life"]),
        max_half_life=int(controls["max_half_life"]),
        state_threshold_buffer=float(controls.get("state_threshold_buffer", 0.0)),
    )
    model.train_df = snapshot["train_df"]
    model.test_df = snapshot["test_df"]
    model.best_thresholds = snapshot["best_thresholds"]
    model.bt_results = snapshot["bt_results"]
    model.spread_models = snapshot["spread_models"]
    return model


def load_cached_or_run_model(
    *,
    company: str,
    df: pd.DataFrame,
    price_col: str,
    train_end: str,
    spread_mode: str,
    signal_lookbacks: dict[str, int],
    half_life_lookback: int,
    min_half_life: int,
    max_half_life: int,
    state_threshold_buffer: float,
    threshold_grid: np.ndarray,
    source_cache_meta: dict | None = None,
    force_refresh: bool = False,
) -> tuple[TradingModel, bool, Path]:
    controls_payload = _controls_snapshot_payload(
        train_end=train_end,
        spread_mode=spread_mode,
        signal_lookbacks=signal_lookbacks,
        half_life_lookback=half_life_lookback,
        min_half_life=min_half_life,
        max_half_life=max_half_life,
        state_threshold_buffer=state_threshold_buffer,
        threshold_grid=threshold_grid,
    )
    dependency = _model_cache_dependency(source_cache_meta)
    snapshot_path = _model_snapshot_path(company, controls_payload)

    if not force_refresh and snapshot_path.exists():
        with snapshot_path.open("rb") as fh:
            snapshot = pickle.load(fh)
        if snapshot.get("controls") == controls_payload and snapshot.get("source_dependency") == dependency:
            return _restore_model_from_snapshot(snapshot), True, snapshot_path

    model = _run_model(
        df,
        price_col,
        train_end=train_end,
        spread_mode=spread_mode,
        signal_lookbacks=signal_lookbacks,
        half_life_lookback=half_life_lookback,
        min_half_life=min_half_life,
        max_half_life=max_half_life,
        state_threshold_buffer=state_threshold_buffer,
        threshold_grid=threshold_grid,
    )

    snapshot = {
        "controls": controls_payload,
        "source_dependency": dependency,
        "price_col": price_col,
        "df": model.df,
        "train_df": model.train_df,
        "test_df": model.test_df,
        "best_thresholds": model.best_thresholds,
        "bt_results": model.bt_results,
        "spread_models": model.spread_models,
    }
    with snapshot_path.open("wb") as fh:
        pickle.dump(snapshot, fh)

    return model, False, snapshot_path


def _safe_metric(stats: pd.Series, key: str) -> float:
    value = stats.get(key, np.nan)
    if pd.isna(value) or not np.isfinite(value):
        return np.nan
    return float(value)


def _score_model(model: TradingModel) -> dict[str, float]:
    sharpe_values = []
    total_return_values = []
    trade_counts = []
    active_days = []

    for horizon in model.horizons:
        test_result = model.bt_results[horizon]["test"]
        stats = model.performance_stats(test_result["strategy_return"])
        sharpe = _safe_metric(stats, "sharpe")
        total_return = _safe_metric(stats, "total_return")
        trades = model.trade_count(test_result["position"])
        active = int((test_result["position"] != 0).sum())

        if np.isfinite(sharpe):
            sharpe_values.append(sharpe)
        if np.isfinite(total_return):
            total_return_values.append(total_return)
        trade_counts.append(trades)
        active_days.append(active)

    if not sharpe_values:
        return {
            "score": -np.inf,
            "avg_sharpe": np.nan,
            "avg_total_return": np.nan,
            "total_trades": int(sum(trade_counts)),
            "active_days": int(sum(active_days)),
        }

    avg_sharpe = float(np.mean(sharpe_values))
    avg_total_return = float(np.mean(total_return_values)) if total_return_values else np.nan
    total_trades = int(sum(trade_counts))
    total_active_days = int(sum(active_days))

    trade_penalty = 0.0
    if total_trades < 6:
        trade_penalty = 0.25
    if total_active_days < 60:
        trade_penalty += 0.25

    score = avg_sharpe - trade_penalty
    return {
        "score": score,
        "avg_sharpe": avg_sharpe,
        "avg_total_return": avg_total_return,
        "total_trades": total_trades,
        "active_days": total_active_days,
    }


def tune_research_parameters(df: pd.DataFrame, price_col: str, train_end: str) -> tuple[dict, pd.DataFrame]:
    threshold_grid = _build_threshold_grid(
        RECOMMENDED_RESEARCH_SETTINGS["threshold_min"],
        RECOMMENDED_RESEARCH_SETTINGS["threshold_max"],
        RECOMMENDED_RESEARCH_SETTINGS["threshold_step"],
    )
    rows = []

    signal_lookbacks = RECOMMENDED_RESEARCH_SETTINGS["signal_lookbacks"]

    for spread_mode, half_life_lookback, min_half_life, max_half_life in product(
        TUNING_CANDIDATES["spread_mode"],
        TUNING_CANDIDATES["half_life_lookback"],
        TUNING_CANDIDATES["min_half_life"],
        TUNING_CANDIDATES["max_half_life"],
    ):
        if max_half_life <= min_half_life:
            continue

        model = _run_model(
            df,
            price_col,
            train_end=train_end,
            spread_mode=spread_mode,
            signal_lookbacks=signal_lookbacks,
            half_life_lookback=half_life_lookback,
            min_half_life=min_half_life,
            max_half_life=max_half_life,
            state_threshold_buffer=RECOMMENDED_RESEARCH_SETTINGS["state_threshold_buffer"],
            threshold_grid=threshold_grid,
        )
        score = _score_model(model)
        rows.append(
            {
                "spread_mode": spread_mode,
                "signal_lookbacks": "/".join(str(signal_lookbacks[horizon]) for horizon in DEFAULT_HORIZONS),
                "half_life_lookback": half_life_lookback,
                "min_half_life": min_half_life,
                "max_half_life": max_half_life,
                "avg_sharpe": score["avg_sharpe"],
                "avg_total_return": score["avg_total_return"],
                "total_trades": score["total_trades"],
                "active_days": score["active_days"],
                "score": score["score"],
            }
        )

    leaderboard = pd.DataFrame(rows).sort_values(
        ["score", "avg_sharpe", "avg_total_return"],
        ascending=False,
    ).reset_index(drop=True)
    best_row = leaderboard.iloc[0]
    best_settings = {
        "spread_mode": best_row["spread_mode"],
        "signal_lookbacks": signal_lookbacks.copy(),
        "half_life_lookback": int(best_row["half_life_lookback"]),
        "min_half_life": int(best_row["min_half_life"]),
        "max_half_life": int(best_row["max_half_life"]),
        "threshold_min": RECOMMENDED_RESEARCH_SETTINGS["threshold_min"],
        "threshold_max": RECOMMENDED_RESEARCH_SETTINGS["threshold_max"],
        "threshold_step": RECOMMENDED_RESEARCH_SETTINGS["threshold_step"],
        "state_threshold_buffer": RECOMMENDED_RESEARCH_SETTINGS["state_threshold_buffer"],
    }
    return best_settings, leaderboard


def _state_adjusted_bounds(upper: float, lower: float, current_state: float, buffer: float) -> tuple[float, float]:
    return TradingModel.state_adjusted_bounds(upper, lower, current_state, buffer)


def get_current_signal_summary(model: TradingModel, horizon: str, current_state: float) -> dict | None:
    signal_col = f"signal_{horizon}"
    pred_col = f"fleet_pred_{horizon}"
    implicit_col = f"fleet_implicit_{horizon}"
    spread_col = f"spread_{horizon}"
    signal_fresh_col = f"signal_fresh_{horizon}"
    prediction_change_col = f"prediction_change_{horizon}"
    spread_change_col = f"spread_change_{horizon}"

    latest_daily = model.df.dropna(subset=[signal_col, pred_col, implicit_col, spread_col]).tail(1)
    latest_signal = latest_daily
    if signal_fresh_col in model.df.columns:
        latest_fresh = model.df.loc[model.df[signal_fresh_col]].dropna(subset=[signal_col, pred_col, implicit_col, spread_col]).tail(1)
        if not latest_fresh.empty:
            latest_signal = latest_fresh
    if latest_signal.empty:
        return None

    row = latest_signal.iloc[0]
    source_row = latest_daily.iloc[0] if not latest_daily.empty else row
    upper = model.best_thresholds[horizon]["upper"]
    lower = model.best_thresholds[horizon]["lower"]
    adjusted_upper, adjusted_lower = _state_adjusted_bounds(
        upper,
        lower,
        current_state,
        model.state_threshold_buffer,
    )
    signal_value = float(row[signal_col])
    prediction_change = float(row[prediction_change_col]) if prediction_change_col in model.df.columns and pd.notna(row[prediction_change_col]) else np.nan
    spread_change = float(row[spread_change_col]) if spread_change_col in model.df.columns and pd.notna(row[spread_change_col]) else np.nan
    stock_price_date = pd.to_datetime(source_row.get("stock_price_date", source_row["Date"]), errors="coerce")
    implicit_rate_date = pd.to_datetime(source_row.get("implicit_rate_date", source_row["Date"]), errors="coerce")
    prediction_date = pd.to_datetime(row.get(f"prediction_date_{horizon}", row["Date"]), errors="coerce")
    source_dates = [date for date in [stock_price_date, implicit_rate_date] if pd.notna(date)]
    reference_date = min(source_dates) if source_dates else pd.to_datetime(row["Date"])
    if pd.notna(prediction_date) and prediction_date.to_period("M") < reference_date.to_period("M"):
        source_dates.append(prediction_date)
    bottleneck_date = min(source_dates) if source_dates else pd.to_datetime(row["Date"])

    if not bool(row[f"trade_allowed_{horizon}"]):
        next_state = current_state
        action = "Hold"
        reason = "Mean-reversion filter blocks new trades, so the previous state is kept."
    elif signal_fresh_col in model.df.columns and not bool(row[signal_fresh_col]):
        next_state = current_state
        action = "Hold"
        reason = "No fresh valuation-gap update, so the previous state is kept."
    elif signal_value >= adjusted_upper:
        if spread_change > 0:
            if current_state >= 0.75:
                next_state = current_state
                action = "Hold"
                reason = "Buy trigger is active, but the assumed state is already Overweight."
            else:
                next_state = 1.0
                action = "Buy"
                reason = "The valuation gap moved upward and is unusually high versus its recent history."
        else:
            next_state = current_state
            action = "Hold"
            reason = "Spread is high, but the latest valuation-gap update was not upward."
    elif signal_value <= adjusted_lower:
        if spread_change < 0:
            if current_state <= 0.25:
                next_state = current_state
                action = "Hold"
                reason = "Sell trigger is active, but the assumed state is already Underweight."
            else:
                next_state = 0.0
                action = "Sell"
                reason = "The valuation gap moved downward and is unusually low versus its recent history."
        else:
            next_state = current_state
            action = "Hold"
            reason = "Spread is low, but the latest valuation-gap update was not downward."
    else:
        next_state = current_state
        action = "Hold"
        reason = "Signal is neutral, so the previous state is kept."

    return {
        "date": bottleneck_date.strftime("%Y-%m-%d"),
        "row_date": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
        "stock_price_date": stock_price_date.strftime("%Y-%m-%d") if pd.notna(stock_price_date) else None,
        "implicit_rate_date": implicit_rate_date.strftime("%Y-%m-%d") if pd.notna(implicit_rate_date) else None,
        "prediction_date": prediction_date.strftime("%Y-%m-%d") if pd.notna(prediction_date) else None,
        "action": action,
        "reason": reason,
        "current_state": current_state,
        "next_state": next_state,
        "predicted_spot": float(row[pred_col]),
        "prediction_change": prediction_change,
        "implicit_rate": float(row[implicit_col]),
        "spread": float(row[spread_col]),
        "spread_change": spread_change,
        "signal": signal_value,
        "upper": upper,
        "lower": lower,
        "adjusted_upper": adjusted_upper,
        "adjusted_lower": adjusted_lower,
        "state_threshold_buffer": model.state_threshold_buffer,
        "half_life": float(row[f"half_life_{horizon}"]) if pd.notna(row[f"half_life_{horizon}"]) else None,
        "trade_allowed": bool(row[f"trade_allowed_{horizon}"]),
        "signal_fresh": bool(row[signal_fresh_col]) if signal_fresh_col in model.df.columns else True,
    }


def build_signal_comparison_chart(plot_df: pd.DataFrame, horizon: str) -> tuple[pd.DataFrame, dict]:
    chart_df = plot_df.copy()
    chart_df["Date"] = pd.to_datetime(chart_df["Date"]).dt.strftime("%Y-%m-%d")

    marker_position_col = "signal_position" if "signal_position" in chart_df.columns else "position"
    prev_position = chart_df[marker_position_col].shift(1).fillna(0)
    buy_signals = chart_df[(chart_df[marker_position_col] == 1) & (prev_position != 1)]
    sell_signals = chart_df[(chart_df[marker_position_col] == 0) & (prev_position == 1)]

    line_df = chart_df[["Date", f"fleet_pred_{horizon}", f"fleet_implicit_{horizon}"]].rename(
        columns={
            f"fleet_pred_{horizon}": f"Predicted spot ({horizon})",
            f"fleet_implicit_{horizon}": "Implicit rate",
        }
    )
    line_df = line_df.melt("Date", var_name="series", value_name="rate")

    signal_frames = []
    if not buy_signals.empty:
        signal_frames.append(
            pd.DataFrame(
                {
                    "Date": buy_signals["Date"],
                    "rate": buy_signals[f"fleet_pred_{horizon}"],
                    "signal": "Buy signal",
                }
            )
        )
    if not sell_signals.empty:
        signal_frames.append(
            pd.DataFrame(
                {
                    "Date": sell_signals["Date"],
                    "rate": sell_signals[f"fleet_pred_{horizon}"],
                    "signal": "Sell signal",
                }
            )
        )

    signal_df = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame(columns=["Date", "rate", "signal"])

    chart_spec = {
        "title": f"Predicted Spot vs Implicit Rate ({horizon})",
        "layer": [
            {
                "mark": {"type": "line", "strokeWidth": 2},
                "encoding": {
                    "x": {"field": "Date", "type": "temporal", "title": "Date"},
                    "y": {"field": "rate", "type": "quantitative", "title": "Rate"},
                    "color": {
                        "field": "series",
                        "type": "nominal",
                        "scale": {
                            "domain": [f"Predicted spot ({horizon})", "Implicit rate"],
                            "range": ["#1f77b4", "#444444"],
                        },
                        "legend": {"title": None, "orient": "top"},
                    },
                    "tooltip": [
                        {"field": "Date", "type": "temporal"},
                        {"field": "series", "type": "nominal", "title": "Series"},
                        {"field": "rate", "type": "quantitative", "title": "Rate", "format": ".2f"},
                    ],
                },
            },
            {
                "data": {"values": signal_df.to_dict("records")},
                "transform": [{"filter": "datum.signal === 'Buy signal'"}],
                "mark": {"type": "text", "fontSize": 20, "fontWeight": "bold"},
                "encoding": {
                    "x": {"field": "Date", "type": "temporal"},
                    "y": {"field": "rate", "type": "quantitative"},
                    "text": {"value": "\u25B2"},
                    "color": {"value": "#00A651"},
                    "tooltip": [
                        {"field": "Date", "type": "temporal"},
                        {"field": "signal", "type": "nominal", "title": "Signal"},
                        {"field": "rate", "type": "quantitative", "title": "Rate", "format": ".2f"},
                    ],
                },
            },
            {
                "data": {"values": signal_df.to_dict("records")},
                "transform": [{"filter": "datum.signal === 'Sell signal'"}],
                "mark": {"type": "text", "fontSize": 20, "fontWeight": "bold"},
                "encoding": {
                    "x": {"field": "Date", "type": "temporal"},
                    "y": {"field": "rate", "type": "quantitative"},
                    "text": {"value": "\u25BC"},
                    "color": {"value": "#D62728"},
                    "tooltip": [
                        {"field": "Date", "type": "temporal"},
                        {"field": "signal", "type": "nominal", "title": "Signal"},
                        {"field": "rate", "type": "quantitative", "title": "Rate", "format": ".2f"},
                    ],
                },
            },
        ],
    }
    return line_df, chart_spec


def build_backtest_summary_table(model: TradingModel, horizon: str) -> pd.DataFrame:
    rows = []

    test_result = model.bt_results[horizon]["test"]
    test_stats = model.performance_stats(test_result["strategy_return"])
    rows.append(
        {
            "Period": "Test",
            "Total Return": test_stats["total_return"],
            "Annual Return": test_stats["annual_return"],
            "Annual Volatility": test_stats["annual_vol"],
            "Sharpe": test_stats["sharpe"],
            "Max Drawdown": test_stats["max_drawdown"],
            "Trades": model.trade_count(test_result["position"]),
            "Days": int(test_stats["days"]),
        }
    )

    buy_hold_stats = model.performance_stats(test_result["stock_return_1d"])
    rows.append(
        {
            "Period": "Buy & Hold",
            "Total Return": buy_hold_stats["total_return"],
            "Annual Return": buy_hold_stats["annual_return"],
            "Annual Volatility": buy_hold_stats["annual_vol"],
            "Sharpe": buy_hold_stats["sharpe"],
            "Max Drawdown": buy_hold_stats["max_drawdown"],
            "Trades": 1 if buy_hold_stats["days"] > 0 else 0,
            "Days": int(buy_hold_stats["days"]),
        }
    )

    return pd.DataFrame(rows)


def build_signal_diagnostics_table(model: TradingModel, horizon: str) -> pd.DataFrame:
    test_df = model.bt_results[horizon]["test"].copy()
    signal_col = f"signal_{horizon}"
    half_life_col = f"half_life_{horizon}"
    allowed_col = f"trade_allowed_{horizon}"
    fresh_col = f"signal_fresh_{horizon}"
    change_col = f"spread_change_{horizon}"
    signal = pd.to_numeric(test_df[signal_col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()

    if signal.empty:
        return pd.DataFrame([{"Metric": "Signal observations", "Value": "0"}])

    upper = model.best_thresholds[horizon]["upper"]
    lower = model.best_thresholds[horizon]["lower"]
    half_life = pd.to_numeric(test_df[half_life_col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    allowed_share = test_df[allowed_col].mean()
    fresh_mask = test_df[fresh_col].fillna(False) if fresh_col in test_df.columns else pd.Series(True, index=test_df.index)
    allowed_mask = test_df[allowed_col].fillna(False) if allowed_col in test_df.columns else pd.Series(True, index=test_df.index)
    spread_change = pd.to_numeric(test_df[change_col], errors="coerce") if change_col in test_df.columns else pd.Series(np.nan, index=test_df.index)
    if "effective_upper_bound" not in test_df.columns or "effective_lower_bound" not in test_df.columns:
        decision_state = test_df["signal_position"].shift(1).fillna(0) if "signal_position" in test_df.columns else test_df["position"].shift(1).fillna(0)
        adjusted_bounds = decision_state.apply(
            lambda state: TradingModel.state_adjusted_bounds(
                upper,
                lower,
                state,
                model.state_threshold_buffer,
            )
        )
        test_df["effective_upper_bound"] = adjusted_bounds.apply(lambda bounds: bounds[0])
        test_df["effective_lower_bound"] = adjusted_bounds.apply(lambda bounds: bounds[1])
    effective_upper = pd.to_numeric(test_df["effective_upper_bound"], errors="coerce")
    effective_lower = pd.to_numeric(test_df["effective_lower_bound"], errors="coerce")
    fresh_signal = pd.to_numeric(test_df.loc[fresh_mask, signal_col], errors="coerce").replace(
        [np.inf, -np.inf],
        np.nan,
    ).dropna()
    buy_trigger = fresh_mask & allowed_mask & spread_change.gt(0) & pd.to_numeric(test_df[signal_col], errors="coerce").ge(effective_upper)
    sell_trigger = fresh_mask & allowed_mask & spread_change.lt(0) & pd.to_numeric(test_df[signal_col], errors="coerce").le(effective_lower)

    diagnostics = [
        {"Metric": "Signal observations", "Value": f"{len(signal):,}"},
        {"Metric": "Fresh spread observations", "Value": f"{len(fresh_signal):,}"},
        {"Metric": "Signal mean", "Value": f"{signal.mean():.2f}"},
        {"Metric": "Signal std", "Value": f"{signal.std():.2f}"},
        {"Metric": "Signal 5% / 95%", "Value": f"{signal.quantile(0.05):.2f} / {signal.quantile(0.95):.2f}"},
        {"Metric": "Raw upper / lower", "Value": f"{upper:.2f} / {lower:.2f}"},
        {"Metric": "Fresh signals above raw upper", "Value": f"{int((fresh_signal > upper).sum()):,}"},
        {"Metric": "Fresh signals below raw lower", "Value": f"{int((fresh_signal < lower).sum()):,}"},
        {"Metric": "Actual buy triggers", "Value": f"{int(buy_trigger.sum()):,}"},
        {"Metric": "Actual sell triggers", "Value": f"{int(sell_trigger.sum()):,}"},
        {"Metric": "Filter pass rate", "Value": f"{allowed_share:.1%}" if pd.notna(allowed_share) else "N/A"},
        {"Metric": "Median spread half-life", "Value": f"{half_life.median():.1f}d" if half_life.notna().any() else "N/A"},
    ]
    return pd.DataFrame(diagnostics)


def build_latest_signals_output(
    model: TradingModel,
    company: str,
    current_state: float,
    controls: dict[str, float | int | str],
) -> pd.DataFrame:
    rows = []
    for horizon in model.horizons:
        summary = get_current_signal_summary(model, horizon, current_state)
        if summary is None:
            continue

        rows.append(
            {
                "company": company,
                "date": summary["date"],
                "row_date": summary["row_date"],
                "stock_price_date": summary["stock_price_date"],
                "implicit_rate_date": summary["implicit_rate_date"],
                "prediction_date": summary["prediction_date"],
                "horizon": horizon,
                "action": summary["action"],
                "reason": summary["reason"],
                "current_state": summary["current_state"],
                "next_state": summary["next_state"],
                "current_state_label": format_state_label(summary["current_state"]),
                "next_state_label": format_state_label(summary["next_state"]),
                "signal": summary["signal"],
                "predicted_spot": summary["predicted_spot"],
                "prediction_change": summary["prediction_change"],
                "implicit_rate": summary["implicit_rate"],
                "spread": summary["spread"],
                "spread_change": summary["spread_change"],
                "upper_bound": summary["upper"],
                "lower_bound": summary["lower"],
                "adjusted_upper_bound": summary["adjusted_upper"],
                "adjusted_lower_bound": summary["adjusted_lower"],
                "state_threshold_buffer": summary["state_threshold_buffer"],
                "half_life": summary["half_life"],
                "trade_allowed": summary["trade_allowed"],
                "signal_fresh": summary["signal_fresh"],
                "spread_mode": controls["spread_mode"],
                "signal_lookback": controls["signal_lookbacks"][horizon],
                "signal_lookback_3m": controls["signal_lookbacks"]["3m"],
                "signal_lookback_6m": controls["signal_lookbacks"]["6m"],
                "signal_lookback_12m": controls["signal_lookbacks"]["12m"],
                "half_life_lookback": controls["half_life_lookback"],
                "min_half_life": controls["min_half_life"],
                "max_half_life": controls["max_half_life"],
                "threshold_min": controls["threshold_min"],
                "threshold_max": controls["threshold_max"],
                "threshold_step": controls["threshold_step"],
                "control_state_threshold_buffer": controls["state_threshold_buffer"],
            }
        )

    return pd.DataFrame(rows)


def export_latest_signals(latest_signals: pd.DataFrame, company: str) -> Path | None:
    if latest_signals.empty:
        return None

    TRADING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_company = company.lower().replace(" ", "_")
    output_path = TRADING_OUTPUT_DIR / f"{safe_company}_latest_trading_signals.csv"
    latest_signals.to_csv(output_path, index=False)

    generic_output_path = TRADING_OUTPUT_DIR / "latest_trading_signals.csv"
    latest_signals.to_csv(generic_output_path, index=False)
    return output_path


def format_state_label(value: float) -> str:
    if value >= 0.99:
        return "Overweight"
    if value >= 0.49:
        return "Market Weight"
    return "Underweight"


def render_current_signal_card(horizon: str, summary: dict) -> None:
    if summary["action"] == "Buy":
        accent = "#0f9d58"
        action_color = "#0f9d58"
    elif summary["action"] == "Sell":
        accent = "#d93025"
        action_color = "#d93025"
    elif summary["action"] == "Hold":
        accent = "#1a73e8"
        action_color = "#1a73e8"
    else:
        accent = "#5f6368"
        action_color = "#202124"

    current_state_label = format_state_label(summary["current_state"])
    next_state_label = format_state_label(summary["next_state"])
    spread_change_label = "N/A" if pd.isna(summary["spread_change"]) else f"{summary['spread_change']:+.2f}"
    st.markdown(
        f"""
        <div style="border: 1px solid #e5e7eb; border-radius: 18px; padding: 1.1rem 1.2rem; background: white; min-height: 350px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);">
            <div style="font-size: 0.9rem; font-weight: 700; color: #5f6368; letter-spacing: 0.02em; margin-bottom: 0.6rem;">{horizon} horizon</div>
            <div style="font-size: 2.1rem; font-weight: 800; color: {action_color}; margin-bottom: 0.35rem;">{summary["action"]}</div>
            <div style="font-size: 0.95rem; color: #5f6368; margin-bottom: 1rem;">{summary["reason"]}</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.95rem 1.1rem;">
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Bottleneck date</div>
                    <div style="font-size: 1.05rem; font-weight: 600; color: #202124;">{summary["date"]}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Current state</div>
                    <div style="font-size: 1.05rem; font-weight: 600; color: #202124;">{current_state_label}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Signal</div>
                    <div style="font-size: 1.7rem; font-weight: 700; color: {accent};">{summary["signal"]:.2f}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Next state</div>
                    <div style="font-size: 1.05rem; font-weight: 600; color: #202124;">{next_state_label}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Predicted spot</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{summary["predicted_spot"]:.2f}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Spread change</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{spread_change_label}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Upper trigger</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{summary["adjusted_upper"]:.2f}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Lower trigger</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{summary["adjusted_lower"]:.2f}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">Spread half-life</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{"N/A" if summary["half_life"] is None else f"{summary['half_life']:.1f}d"}</div>
                </div>
                <div>
                    <div style="font-size: 0.78rem; color: #80868b;">MR filter</div>
                    <div style="font-size: 1.02rem; font-weight: 600; color: #202124;">{"On" if summary["trade_allowed"] else "Blocked"}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_bounds_card(horizon: str, bounds: dict[str, float]) -> None:
    band_width = bounds["upper"] - bounds["lower"]
    st.markdown(
        f"""
        <div style="border-radius: 20px; padding: 1.1rem 1.15rem; background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%); border: 1px solid #dbe4ee; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06); min-height: 210px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.9rem;">
                <div style="font-size: 1.1rem; font-weight: 800; color: #0f172a;">{horizon} horizon</div>
                <div style="font-size: 0.78rem; font-weight: 700; color: #475569; background: #e2e8f0; padding: 0.28rem 0.55rem; border-radius: 999px;">Tuned thresholds</div>
            </div>
            <div style="display: flex; gap: 0.75rem; margin-bottom: 0.95rem;">
                <div style="flex: 1; background: #ecfdf3; border: 1px solid #bbf7d0; border-radius: 16px; padding: 0.8rem 0.85rem;">
                    <div style="font-size: 0.76rem; color: #166534; text-transform: uppercase; letter-spacing: 0.04em;">Upper bound</div>
                    <div style="font-size: 1.55rem; font-weight: 800; color: #166534;">{bounds["upper"]:.2f}</div>
                    <div style="font-size: 0.82rem; color: #166534;">Raw buy threshold</div>
                </div>
                <div style="flex: 1; background: #fef2f2; border: 1px solid #fecaca; border-radius: 16px; padding: 0.8rem 0.85rem;">
                    <div style="font-size: 0.76rem; color: #991b1b; text-transform: uppercase; letter-spacing: 0.04em;">Lower bound</div>
                    <div style="font-size: 1.55rem; font-weight: 800; color: #991b1b;">{bounds["lower"]:.2f}</div>
                    <div style="font-size: 0.82rem; color: #991b1b;">Raw sell threshold</div>
                </div>
            </div>
            <div style="background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 16px; padding: 0.8rem 0.85rem;">
                <div style="font-size: 0.76rem; color: #1d4ed8; text-transform: uppercase; letter-spacing: 0.04em;">Neutral band width</div>
                <div style="font-size: 1.3rem; font-weight: 800; color: #1e3a8a;">{band_width:.2f}</div>
                <div style="font-size: 0.84rem; color: #1d4ed8;">Signals inside the raw band keep the prior state unless state adjustment moves the trigger</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _initialize_state() -> None:
    defaults = {
        "trading_research_mode": False,
        "assumed_state_label": "Underweight",
        "ui_spread_mode": RECOMMENDED_RESEARCH_SETTINGS["spread_mode"],
        "ui_signal_lookback_3m": RECOMMENDED_RESEARCH_SETTINGS["signal_lookbacks"]["3m"],
        "ui_signal_lookback_6m": RECOMMENDED_RESEARCH_SETTINGS["signal_lookbacks"]["6m"],
        "ui_signal_lookback_12m": RECOMMENDED_RESEARCH_SETTINGS["signal_lookbacks"]["12m"],
        "ui_half_life_lookback": RECOMMENDED_RESEARCH_SETTINGS["half_life_lookback"],
        "ui_min_half_life": RECOMMENDED_RESEARCH_SETTINGS["min_half_life"],
        "ui_max_half_life": RECOMMENDED_RESEARCH_SETTINGS["max_half_life"],
        "ui_threshold_min": RECOMMENDED_RESEARCH_SETTINGS["threshold_min"],
        "ui_threshold_max": RECOMMENDED_RESEARCH_SETTINGS["threshold_max"],
        "ui_threshold_step": RECOMMENDED_RESEARCH_SETTINGS["threshold_step"],
        "ui_state_threshold_buffer": RECOMMENDED_RESEARCH_SETTINGS["state_threshold_buffer"],
        "research_controls": RECOMMENDED_RESEARCH_SETTINGS.copy(),
        "tuned_settings": None,
        "tuning_preview": None,
        "pending_research_controls": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _queue_research_controls(settings: dict[str, float | int | str]) -> None:
    st.session_state["pending_research_controls"] = settings.copy()


def _apply_queued_research_controls() -> None:
    pending = st.session_state.get("pending_research_controls")
    if not pending:
        return

    st.session_state["research_controls"] = pending.copy()
    st.session_state["ui_spread_mode"] = pending["spread_mode"]
    signal_lookbacks = _normalize_signal_lookbacks(
        pending.get("signal_lookbacks", pending.get("signal_lookback")),
        DEFAULT_HORIZONS,
    )
    st.session_state["ui_signal_lookback_3m"] = signal_lookbacks["3m"]
    st.session_state["ui_signal_lookback_6m"] = signal_lookbacks["6m"]
    st.session_state["ui_signal_lookback_12m"] = signal_lookbacks["12m"]
    st.session_state["ui_half_life_lookback"] = pending["half_life_lookback"]
    st.session_state["ui_min_half_life"] = pending["min_half_life"]
    st.session_state["ui_max_half_life"] = pending["max_half_life"]
    st.session_state["ui_threshold_min"] = pending["threshold_min"]
    st.session_state["ui_threshold_max"] = pending["threshold_max"]
    st.session_state["ui_threshold_step"] = pending["threshold_step"]
    st.session_state["ui_state_threshold_buffer"] = pending.get(
        "state_threshold_buffer",
        RECOMMENDED_RESEARCH_SETTINGS["state_threshold_buffer"],
    )
    st.session_state["pending_research_controls"] = None


def _collect_research_controls_from_widgets() -> dict[str, float | int | str]:
    controls = {
        "spread_mode": st.session_state["ui_spread_mode"],
        "signal_lookbacks": {
            "3m": int(st.session_state["ui_signal_lookback_3m"]),
            "6m": int(st.session_state["ui_signal_lookback_6m"]),
            "12m": int(st.session_state["ui_signal_lookback_12m"]),
        },
        "half_life_lookback": int(st.session_state["ui_half_life_lookback"]),
        "min_half_life": int(st.session_state["ui_min_half_life"]),
        "max_half_life": int(st.session_state["ui_max_half_life"]),
        "threshold_min": float(st.session_state["ui_threshold_min"]),
        "threshold_max": float(st.session_state["ui_threshold_max"]),
        "threshold_step": float(st.session_state["ui_threshold_step"]),
        "state_threshold_buffer": float(st.session_state["ui_state_threshold_buffer"]),
    }
    st.session_state["research_controls"] = controls.copy()
    return controls


def _apply_tuned_controls(settings: dict[str, float | int | str]) -> None:
    _queue_research_controls(settings)
    st.session_state["tuned_settings"] = settings.copy()


st.set_page_config(
    page_title="Trading Algorithm",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)

company = render_sidebar()
ticker = get_company_ticker(company)
_initialize_state()
_apply_queued_research_controls()

st.title(f"{company} Trading Algorithm")

main_col, controls_col = st.columns((3, 1))

with controls_col:
    st.header("Model controls")
    refresh_cached_data = st.button(
        "Refresh Cached Data",
        width="stretch",
        help="Rebuild the trading dataset from source files and overwrite the local cache file.",
    )
    train_end = st.text_input("Train End Date", DEFAULT_TRAIN_END)
    research_mode = st.toggle(
        "Research mode",
        key="trading_research_mode",
        help="Show advanced parameter controls and enable parameter tuning.",
    )

    if not research_mode:
        controls = RECOMMENDED_RESEARCH_SETTINGS.copy()
        st.info("Standard view uses the recommended settings to keep the page simple for normal use.")
        st.caption(
            f"Recommended setup: {SPREAD_MODE_OPTIONS[RECOMMENDED_RESEARCH_SETTINGS['spread_mode']]} spread, "
            "signal-lookback 3m/6m/12m = "
            f"{RECOMMENDED_RESEARCH_SETTINGS['signal_lookbacks']['3m']}/"
            f"{RECOMMENDED_RESEARCH_SETTINGS['signal_lookbacks']['6m']}/"
            f"{RECOMMENDED_RESEARCH_SETTINGS['signal_lookbacks']['12m']}d, "
            f"{RECOMMENDED_RESEARCH_SETTINGS['half_life_lookback']}d half-life-lookback, "
            f"half-life filter {RECOMMENDED_RESEARCH_SETTINGS['min_half_life']}-"
            f"{RECOMMENDED_RESEARCH_SETTINGS['max_half_life']} days and threshold grid "
            f"{RECOMMENDED_RESEARCH_SETTINGS['threshold_min']:.1f}-"
            f"{RECOMMENDED_RESEARCH_SETTINGS['threshold_max']:.1f}, "
            f"state-buffer {RECOMMENDED_RESEARCH_SETTINGS['state_threshold_buffer']:.1f}."
        )
    else:
        st.warning("Research mode is for tuning and deeper testing. `Tune parameters` may take a little while.")
        st.selectbox(
            "Spread mode",
            options=list(SPREAD_MODE_OPTIONS.keys()),
            key="ui_spread_mode",
            format_func=lambda key: SPREAD_MODE_OPTIONS[key],
            help="Choose how predicted spot and implicit rate are compared before z-score normalization.",
        )
        st.slider("3m signal lookback", 63, 504, key="ui_signal_lookback_3m", step=21)
        st.slider("6m signal lookback", 63, 504, key="ui_signal_lookback_6m", step=21)
        st.slider("12m signal lookback", 63, 504, key="ui_signal_lookback_12m", step=21)
        st.slider("Half-life lookback", 63, 504, key="ui_half_life_lookback", step=21)
        st.slider(
            "Min half-life",
            1,
            max(60, int(RECOMMENDED_RESEARCH_SETTINGS["min_half_life"])),
            key="ui_min_half_life",
        )
        st.slider(
            "Max half-life",
            10,
            max(180, int(RECOMMENDED_RESEARCH_SETTINGS["max_half_life"])),
            key="ui_max_half_life",
        )
        st.slider("Threshold Min", 0.0, 1.0, key="ui_threshold_min", step=0.1)
        st.slider("Threshold Max", 1.0, 3.0, key="ui_threshold_max", step=0.1)
        st.slider("Threshold Step", 0.1, 0.5, key="ui_threshold_step", step=0.1)
        st.slider(
            "State threshold buffer",
            0.0,
            1.0,
            key="ui_state_threshold_buffer",
            step=0.1,
            help="Adjusts triggers by current state: Underweight lowers the buy trigger and lowers the sell trigger; Overweight raises the buy trigger and raises the sell trigger.",
        )

        controls = _collect_research_controls_from_widgets()

        if st.button("Use Recommended Settings", width="stretch"):
            _queue_research_controls(RECOMMENDED_RESEARCH_SETTINGS)
            st.rerun()

        tune_parameters = st.button("Tune Parameters", width="stretch")

    if not research_mode:
        tune_parameters = False

    assumed_state_label = st.radio(
        "Current State",
        ["Underweight", "Market Weight", "Overweight"],
        key="assumed_state_label",
    )
    assumed_state_map = {
        "Underweight": 0.0,
        "Market Weight": 0.5,
        "Overweight": 1.0,
    }
    assumed_state = assumed_state_map[assumed_state_label]
    threshold_grid = _build_threshold_grid(
        controls["threshold_min"],
        controls["threshold_max"],
        controls["threshold_step"],
    )
    run_backtest = st.button("Run Backtest")

with main_col:
    try:
        with st.spinner("Refreshing cached trading data..." if refresh_cached_data else "Loading trading data from cache..."):
            df, price_col, cache_meta = load_cached_or_refresh_trading_model_data(
                company,
                start_date=DEFAULT_START_DATE,
                force_refresh=refresh_cached_data,
            )

        cache_status = (
            "Cache refreshed from source data."
            if refresh_cached_data
            else "Loaded from cached trading dataset."
        )
        st.caption(
            f"{cache_status} Cache window: {cache_meta.get('cached_from', 'N/A')} to "
            f"{cache_meta.get('cached_through', 'N/A')} "
            f"({cache_meta.get('row_count', 0):,} rows)."
        )

        if tune_parameters:
            with st.spinner("Tuning parameters on the latest available data. This can take a while..."):
                best_settings, leaderboard = tune_research_parameters(df, price_col, train_end)
            _apply_tuned_controls(best_settings)
            st.session_state["tuning_preview"] = leaderboard.head(10).to_dict("records")
            st.rerun()

        model_spinner_label = (
            "Refreshing cached model snapshot..." if refresh_cached_data else "Loading model snapshot..."
        )
        with st.spinner(model_spinner_label):
            model, model_loaded_from_cache, model_cache_path = load_cached_or_run_model(
                company=company,
                df=df,
                price_col=price_col,
                train_end=train_end,
                spread_mode=controls["spread_mode"],
                signal_lookbacks=controls["signal_lookbacks"],
                half_life_lookback=controls["half_life_lookback"],
                min_half_life=controls["min_half_life"],
                max_half_life=controls["max_half_life"],
                state_threshold_buffer=controls["state_threshold_buffer"],
                threshold_grid=threshold_grid,
                source_cache_meta=cache_meta,
                force_refresh=refresh_cached_data,
            )

        model_cache_status = "Loaded cached model snapshot." if model_loaded_from_cache else "Computed and cached model snapshot."
        st.caption(f"{model_cache_status} Snapshot: `{model_cache_path.as_posix()}`.")
        latest_signals_output = build_latest_signals_output(model, company, assumed_state, controls)
        latest_signals_path = export_latest_signals(latest_signals_output, company)

        st.markdown(
            f"""
            <div style="border-radius: 24px; padding: 1.35rem 1.4rem 1rem 1.4rem; background: linear-gradient(135deg, #111827 0%, #1f2937 100%); color: white; margin-bottom: 1rem;">
                <div style="font-size: 2.5rem; font-weight: 800; margin-bottom: 0.4rem;">Current Trading Signal</div>
                <div style="font-size: 1.05rem; color: #d1d5db; margin-bottom: 1rem;">Live recommendation overview for {company}, using the latest available model inputs.</div>
                <div style="display: flex; gap: 2.5rem; flex-wrap: wrap;">
                    <div>
                        <div style="font-size: 0.82rem; color: #9ca3af;">Company</div>
                        <div style="font-size: 2rem; font-weight: 700;">{company}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.82rem; color: #9ca3af;">Assumed current state</div>
                        <div style="font-size: 2rem; font-weight: 700;">{assumed_state_label}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.82rem; color: #9ca3af;">Spread mode</div>
                        <div style="font-size: 2rem; font-weight: 700;">{SPREAD_MODE_OPTIONS[controls["spread_mode"]]}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.82rem; color: #9ca3af;">Signal lookback 3m / 6m / 12m in days</div>
                        <div style="font-size: 2rem; font-weight: 700;">{controls["signal_lookbacks"]["3m"]} / {controls["signal_lookbacks"]["6m"]} / {controls["signal_lookbacks"]["12m"]}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.82rem; color: #9ca3af;">Half-life lookback in days</div>
                        <div style="font-size: 2rem; font-weight: 700;">{controls["half_life_lookback"]}</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if latest_signals_path is not None:
            st.caption(f"Latest trading signals exported to `{latest_signals_path.as_posix()}`.")

        if research_mode:
            tuned_settings = st.session_state.get("tuned_settings")
            if tuned_settings:
                st.success(
                    "Latest tuning found the best score with "
                    f"{SPREAD_MODE_OPTIONS[tuned_settings['spread_mode']]}, "
                    "signal lookback "
                    f"{tuned_settings['signal_lookbacks']['3m']}/"
                    f"{tuned_settings['signal_lookbacks']['6m']}/"
                    f"{tuned_settings['signal_lookbacks']['12m']}, "
                    f"half-life lookback {tuned_settings['half_life_lookback']} and "
                    f"half-life range {tuned_settings['min_half_life']}-{tuned_settings['max_half_life']}. "
                    f"State buffer is {tuned_settings.get('state_threshold_buffer', 0.0):.1f}."
                )

            preview_rows = st.session_state.get("tuning_preview")
            if preview_rows:
                display_results = pd.DataFrame(preview_rows).copy()
                display_results["spread_mode"] = display_results["spread_mode"].map(SPREAD_MODE_OPTIONS)
                display_results["avg_sharpe"] = display_results["avg_sharpe"].map(_format_num)
                display_results["avg_total_return"] = display_results["avg_total_return"].map(_format_pct)
                display_results["score"] = display_results["score"].map(_format_num)
                st.subheader("Tuning leaderboard")
                st.dataframe(display_results, width="stretch", hide_index=True)

        signal_cols = st.columns(len(model.horizons))
        for idx, horizon in enumerate(model.horizons):
            with signal_cols[idx]:
                summary = get_current_signal_summary(model, horizon, assumed_state)
                if summary is None:
                    st.warning(f"{horizon}: Not enough data to compute a current signal yet.")
                else:
                    render_current_signal_card(horizon, summary)

        if run_backtest:
            st.header("Tuned Signal Thresholds")
            st.caption(
                f"Bounds are tuned on the training window up to {train_end}. Search grid: "
                f"+/-{controls['threshold_min']:.2f} to +/-{controls['threshold_max']:.2f} in steps of {controls['threshold_step']:.2f}. "
                "Signals use horizon-specific rolling z-scores of the valuation spread "
                f"({controls['signal_lookbacks']['3m']}/{controls['signal_lookbacks']['6m']}/{controls['signal_lookbacks']['12m']} days for 3m/6m/12m) and only trade on "
                f"fresh spread updates when estimated half-life is between {controls['min_half_life']} and "
                f"{controls['max_half_life']} days. The state buffer is {controls['state_threshold_buffer']:.2f}: "
                "Underweight makes buy easier and sell stricter, Overweight makes buy stricter and sell easier, Market Weight uses standard bounds."
            )
            bound_cols = st.columns(len(model.horizons))
            for idx, horizon in enumerate(model.horizons):
                with bound_cols[idx]:
                    render_bounds_card(horizon, model.best_thresholds[horizon])

            st.header("Backtest Results")
            for horizon in model.horizons:
                st.subheader(f"{horizon} horizon")
                summary = build_backtest_summary_table(model, horizon)
                styled_summary = summary.copy()
                for col in ["Total Return", "Annual Return", "Annual Volatility", "Max Drawdown"]:
                    styled_summary[col] = styled_summary[col].map(_format_pct)
                styled_summary["Sharpe"] = styled_summary["Sharpe"].map(_format_num)

                st.dataframe(
                    styled_summary,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Period": st.column_config.TextColumn("Period"),
                        "Total Return": st.column_config.TextColumn("Total Return"),
                        "Annual Return": st.column_config.TextColumn("Annual Return"),
                        "Annual Volatility": st.column_config.TextColumn("Annual Volatility"),
                        "Sharpe": st.column_config.TextColumn("Sharpe"),
                        "Max Drawdown": st.column_config.TextColumn("Max Drawdown"),
                        "Trades": st.column_config.NumberColumn("Trades", format="%d"),
                        "Days": st.column_config.NumberColumn("Days", format="%d"),
                    },
                    column_order=[
                        "Period",
                        "Total Return",
                        "Annual Return",
                        "Annual Volatility",
                        "Sharpe",
                        "Max Drawdown",
                        "Trades",
                        "Days",
                    ],
                )

                plot_df = model.bt_results[horizon]["test"].copy()
                plot_df = plot_df[plot_df["Date"] > pd.Timestamp(train_end)].copy()

                equity_df = plot_df.set_index("Date")
                shipping_index_curve = load_shipping_index_curve(plot_df["Date"].min())
                if "trading_algorithm_curve" not in equity_df.columns and "equity_curve" in equity_df.columns:
                    equity_df["trading_algorithm_curve"] = equity_df["equity_curve"]
                chart_df = equity_df[["trading_algorithm_curve", "buy_hold_curve"]].copy()
                if shipping_index_curve is not None:
                    chart_df = chart_df.join(shipping_index_curve, how="left")
                    chart_df["shipping_index_curve"] = chart_df["shipping_index_curve"].ffill()

                line_columns = ["trading_algorithm_curve", "buy_hold_curve"]
                if "shipping_index_curve" in chart_df.columns and chart_df["shipping_index_curve"].notna().any():
                    line_columns.append("shipping_index_curve")

                visible_chart_df = chart_df[line_columns].copy()
                first_values = visible_chart_df.apply(lambda col: col.dropna().iloc[0] if col.notna().any() else np.nan)
                first_values = first_values.replace(0, np.nan)
                visible_chart_df = visible_chart_df.divide(first_values)
                visible_chart_df = visible_chart_df.reset_index().melt("Date", var_name="series", value_name="value")
                visible_chart_df["series"] = visible_chart_df["series"].map(
                    {
                        "trading_algorithm_curve": "Trading algorithm",
                        "buy_hold_curve": "Buy & Hold",
                        "shipping_index_curve": "Shipping index",
                    }
                )
                equity_chart_spec = {
                    "mark": {"type": "line", "strokeWidth": 2.5},
                    "encoding": {
                        "x": {"field": "Date", "type": "temporal", "title": "Date"},
                        "y": {"field": "value", "type": "quantitative", "title": "Rebased value"},
                        "color": {
                            "field": "series",
                            "type": "nominal",
                            "scale": {
                                "domain": ["Trading algorithm", "Buy & Hold", "Shipping index"],
                                "range": ["#60a5fa", "#1d4ed8", "#dc2626"],
                            },
                            "legend": {"title": None, "orient": "top"},
                        },
                        "tooltip": [
                            {"field": "Date", "type": "temporal"},
                            {"field": "series", "type": "nominal", "title": "Series"},
                            {"field": "value", "type": "quantitative", "title": "Value", "format": ".2f"},
                        ],
                    },
                }
                st.vega_lite_chart(visible_chart_df, equity_chart_spec, width="stretch")

                comparison_df, comparison_spec = build_signal_comparison_chart(plot_df, horizon)
                st.vega_lite_chart(comparison_df, comparison_spec, width="stretch")
                st.caption("Markers show position changes, plotted on the predicted spot level.")

                diagnostics_df = build_signal_diagnostics_table(model, horizon)
                with st.expander("Signal diagnostics"):
                    st.dataframe(
                        diagnostics_df,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "Metric": st.column_config.TextColumn("Signal diagnostics"),
                            "Value": st.column_config.TextColumn("Value"),
                        },
                    )
    except MissingFinancialsDataError as e:
        st.error(
            "Financials data is not ready for this company yet. "
            f"{e} Open `Data/{ticker}_Financials.xlsx` and add at least one reporting period."
        )
    except FileNotFoundError as e:
        st.error(f"Data file not found for {company}: {e}")
    except Exception as e:
        st.error(f"Error running backtest: {e}")
