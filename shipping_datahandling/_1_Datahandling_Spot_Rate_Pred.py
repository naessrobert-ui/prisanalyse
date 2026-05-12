from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "Data" / "monthly_tce_history.csv"
OUTPUT_DIR = BASE_DIR / "Output" / "Spot_Rate_Pred"
OUTPUT_PATH = OUTPUT_DIR / "spot_rate_predictions_from_2015.xlsx"
METADATA_PATH = OUTPUT_DIR / "spot_rate_predictions_from_2015.meta.json"
START_FORECAST_MONTH = pd.Timestamp("2015-01-01")
DISPLAY_HORIZONS: tuple[int, ...] = (1, 3, 6, 12)
REQUIRED_COLUMNS: tuple[str, ...] = ("month", "vessel_type", "tce")
SUPPORTED_HORIZONS: tuple[int, ...] = DISPLAY_HORIZONS
LOG_FLOOR = 1.0


@dataclass(frozen=True)
class SlideModelSpec:
    name: str
    scope: str


SLIDE_MODEL_SPECS: tuple[SlideModelSpec, ...] = (
    SlideModelSpec(name="ornstein_uhlenbeck", scope="univariate"),
    SlideModelSpec(name="nonlinear_mean_reversion", scope="univariate"),
    SlideModelSpec(name="multivariate_mean_reversion", scope="panel"),
)

MODEL_SCOPE_BY_NAME = {spec.name: spec.scope for spec in SLIDE_MODEL_SPECS}

# Best-model table copied from Reidar2: outputs/forecasts/slide_models_selected.csv
SELECTED_MODEL_ROWS = [
    {
        "vessel_type": "aframax",
        "forecast_horizon_months": 1,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 8544.2246474562,
        "rmse": 12947.239953475359,
        "mape": 30.141163572079712,
        "bias": -3409.80654659824,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "aframax",
        "forecast_horizon_months": 3,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 12525.708782016254,
        "rmse": 18834.429103471564,
        "mape": 51.100163113659136,
        "bias": -5983.8119862135,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "aframax",
        "forecast_horizon_months": 6,
        "selected_model_name": "ornstein_uhlenbeck",
        "mae": 14967.29376319128,
        "rmse": 20683.73449143409,
        "mape": 81.23984306987774,
        "bias": -4223.5534661363945,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "aframax",
        "forecast_horizon_months": 12,
        "selected_model_name": "ornstein_uhlenbeck",
        "mae": 17384.68107089591,
        "rmse": 23189.717758697807,
        "mape": 89.16642977863721,
        "bias": -6735.452875741655,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "mr",
        "forecast_horizon_months": 1,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 3053.4231492138492,
        "rmse": 4443.843334694688,
        "mape": 18.70975796994593,
        "bias": -397.72328289586363,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "mr",
        "forecast_horizon_months": 3,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 4705.340071411474,
        "rmse": 7003.887015722923,
        "mape": 28.129652520101743,
        "bias": -1386.4405327755844,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "mr",
        "forecast_horizon_months": 6,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 6495.6193741589705,
        "rmse": 9275.495476089567,
        "mape": 40.943642099864505,
        "bias": -2477.381938120304,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "mr",
        "forecast_horizon_months": 12,
        "selected_model_name": "ornstein_uhlenbeck",
        "mae": 7611.165320969473,
        "rmse": 10435.24032950796,
        "mape": 52.36775213279715,
        "bias": -1896.1259534205446,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "suezmax",
        "forecast_horizon_months": 1,
        "selected_model_name": "nonlinear_mean_reversion",
        "mae": 8654.529495287448,
        "rmse": 13760.006724541698,
        "mape": 33.247221057972695,
        "bias": -2212.981539670153,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "suezmax",
        "forecast_horizon_months": 3,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 12878.761692613067,
        "rmse": 18689.973242421762,
        "mape": 56.55990108425781,
        "bias": -3074.883169223393,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "suezmax",
        "forecast_horizon_months": 6,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 15055.554979911154,
        "rmse": 20979.25521335564,
        "mape": 75.38817424952556,
        "bias": -4840.436273765531,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "suezmax",
        "forecast_horizon_months": 12,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 19161.1758859334,
        "rmse": 25768.56518857016,
        "mape": 96.64226003338148,
        "bias": -8411.752873909325,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "vlcc",
        "forecast_horizon_months": 1,
        "selected_model_name": "nonlinear_mean_reversion",
        "mae": 12510.455031401987,
        "rmse": 21736.122588972146,
        "mape": 109.97573388537737,
        "bias": -2503.1754785451844,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "vlcc",
        "forecast_horizon_months": 3,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 17533.63875617947,
        "rmse": 26483.8932471429,
        "mape": 212.90836949711905,
        "bias": -2155.851340566849,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "vlcc",
        "forecast_horizon_months": 6,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 19880.31681053673,
        "rmse": 29468.77769030485,
        "mape": 326.10603278859253,
        "bias": -2618.481603832482,
        "n_forecasts": 122,
    },
    {
        "vessel_type": "vlcc",
        "forecast_horizon_months": 12,
        "selected_model_name": "multivariate_mean_reversion",
        "mae": 22525.18988948853,
        "rmse": 33557.779761616956,
        "mape": 642.4006476498791,
        "bias": -5594.978807956106,
        "n_forecasts": 122,
    },
]


def selected_models_frame() -> pd.DataFrame:
    return pd.DataFrame.from_records(SELECTED_MODEL_ROWS).sort_values(
        ["forecast_horizon_months", "vessel_type"]
    ).reset_index(drop=True)


def validate_required_columns(
    frame: pd.DataFrame,
    required_columns: tuple[str, ...] = REQUIRED_COLUMNS,
) -> None:
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")


def validate_horizon(horizon_months: int) -> None:
    if horizon_months not in SUPPORTED_HORIZONS:
        supported = ", ".join(str(value) for value in SUPPORTED_HORIZONS)
        raise ValueError(
            f"Unsupported forecast horizon: {horizon_months}. Supported horizons are: {supported}."
        )


def validate_monthly_continuity(
    frame: pd.DataFrame,
    date_column: str = "month",
    group_column: str = "vessel_type",
) -> None:
    for vessel_type, vessel_history in frame.groupby(group_column, sort=True):
        observed_months = vessel_history[date_column].reset_index(drop=True)
        expected_months = pd.Series(
            pd.date_range(
                start=observed_months.iloc[0],
                periods=len(observed_months),
                freq="MS",
            )
        )
        if not observed_months.equals(expected_months):
            raise ValueError(
                f"Monthly history for vessel_type '{vessel_type}' has missing months."
            )


def prepare_monthly_history(
    frame: pd.DataFrame,
    date_column: str = "month",
    group_column: str = "vessel_type",
    target_column: str = "tce",
) -> pd.DataFrame:
    validate_required_columns(frame, (date_column, group_column, target_column))
    if frame.empty:
        raise ValueError("Input history is empty.")

    prepared = frame.copy()
    prepared[date_column] = pd.to_datetime(prepared[date_column], errors="coerce")

    if prepared[date_column].isna().any():
        raise ValueError(f"Column '{date_column}' contains invalid or missing dates.")
    if prepared[group_column].isna().any():
        raise ValueError(f"Column '{group_column}' contains missing values.")
    if prepared[target_column].isna().any():
        raise ValueError(f"Column '{target_column}' contains missing values.")
    if not pd.api.types.is_numeric_dtype(prepared[target_column]):
        raise TypeError(f"Column '{target_column}' must be numeric.")

    duplicate_count = int(prepared.duplicated(subset=[group_column, date_column]).sum())
    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate rows for '{group_column}' and '{date_column}'."
        )

    prepared[target_column] = prepared[target_column].astype("float64")
    prepared = prepared.sort_values([group_column, date_column]).reset_index(drop=True)
    validate_monthly_continuity(prepared, date_column=date_column, group_column=group_column)
    return prepared


def _future_months(last_observed_month: pd.Timestamp, horizon_months: int) -> pd.DatetimeIndex:
    start_month = last_observed_month + pd.offsets.MonthBegin(1)
    return pd.date_range(start=start_month, periods=horizon_months, freq="MS")


def _fit_ar1(series: pd.Series) -> tuple[float, float]:
    values = series.astype("float64").to_numpy()
    if len(values) < 2:
        latest = float(values[-1])
        return latest, 0.0

    lagged = values[:-1]
    current = values[1:]
    design = np.column_stack([np.ones(len(lagged)), lagged])
    coefficients, *_ = np.linalg.lstsq(design, current, rcond=None)
    intercept = float(coefficients[0])
    beta = float(np.clip(coefficients[1], -0.99, 0.99))
    return intercept, beta


def _fit_log_ar1(series: pd.Series) -> tuple[float, float, pd.Series]:
    log_series = np.log(series.astype("float64").clip(lower=LOG_FLOOR))
    intercept, beta = _fit_ar1(log_series)
    return intercept, beta, log_series


def _format_forecast_frame(
    vessel_type: str,
    history_end_month: pd.Timestamp,
    horizon_months: int,
    model_name: str,
    forecast_values: list[float],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "vessel_type": vessel_type,
            "history_end_month": history_end_month,
            "forecast_month": _future_months(history_end_month, horizon_months),
            "forecast_horizon_months": horizon_months,
            "horizon": range(1, horizon_months + 1),
            "model_name": model_name,
            "forecast_tce": forecast_values,
        }
    )


def ornstein_uhlenbeck_forecast(
    frame: pd.DataFrame,
    horizon_months: int,
    date_column: str = "month",
    group_column: str = "vessel_type",
    target_column: str = "tce",
) -> pd.DataFrame:
    validate_horizon(horizon_months)
    prepared = prepare_monthly_history(frame, date_column, group_column, target_column)
    forecast_frames: list[pd.DataFrame] = []

    for vessel_type, vessel_history in prepared.groupby(group_column, sort=True):
        series = vessel_history[target_column].reset_index(drop=True)
        intercept, beta = _fit_ar1(series)
        last_value = float(series.iloc[-1])
        forecasts: list[float] = []

        for _ in range(horizon_months):
            last_value = intercept + beta * last_value
            forecasts.append(float(max(last_value, 1.0)))

        forecast_frames.append(
            _format_forecast_frame(
                vessel_type=str(vessel_type),
                history_end_month=vessel_history[date_column].iloc[-1],
                horizon_months=horizon_months,
                model_name="ornstein_uhlenbeck",
                forecast_values=forecasts,
            )
        )

    return pd.concat(forecast_frames, ignore_index=True).sort_values(
        ["vessel_type", "forecast_month"]
    ).reset_index(drop=True)


def nonlinear_mean_reversion_forecast(
    frame: pd.DataFrame,
    horizon_months: int,
    date_column: str = "month",
    group_column: str = "vessel_type",
    target_column: str = "tce",
) -> pd.DataFrame:
    validate_horizon(horizon_months)
    prepared = prepare_monthly_history(frame, date_column, group_column, target_column)
    forecast_frames: list[pd.DataFrame] = []

    for vessel_type, vessel_history in prepared.groupby(group_column, sort=True):
        series = np.log(
            vessel_history[target_column].astype("float64").clip(lower=LOG_FLOOR)
        ).reset_index(drop=True)
        theta = float(series.mean())
        deltas = series - theta
        x = deltas[:-1].to_numpy()
        y = deltas[1:].to_numpy()
        threshold = float(np.quantile(np.abs(x), 0.7)) if len(x) else 0.0
        large_mask = np.abs(x) > threshold

        def _fit_beta(mask: np.ndarray) -> float:
            if mask.sum() == 0:
                return 0.0
            denom = float(np.dot(x[mask], x[mask]))
            if denom == 0.0:
                return 0.0
            return float(np.dot(x[mask], y[mask]) / denom)

        beta_small = float(np.clip(_fit_beta(~large_mask), -0.99, 0.99))
        beta_large = float(np.clip(_fit_beta(large_mask), -0.99, 0.99))
        if beta_small == 0.0 and beta_large == 0.0:
            beta_small = 0.5
            beta_large = 0.2

        current_log = float(series.iloc[-1])
        forecasts: list[float] = []

        for _ in range(horizon_months):
            deviation = current_log - theta
            beta = beta_large if abs(deviation) > threshold else beta_small
            current_log = theta + beta * deviation
            forecasts.append(float(max(np.exp(current_log), 1.0)))

        forecast_frames.append(
            _format_forecast_frame(
                vessel_type=str(vessel_type),
                history_end_month=vessel_history[date_column].iloc[-1],
                horizon_months=horizon_months,
                model_name="nonlinear_mean_reversion",
                forecast_values=forecasts,
            )
        )

    return pd.concat(forecast_frames, ignore_index=True).sort_values(
        ["vessel_type", "forecast_month"]
    ).reset_index(drop=True)


def multivariate_mean_reversion_forecast(
    frame: pd.DataFrame,
    horizon_months: int,
    date_column: str = "month",
    group_column: str = "vessel_type",
    target_column: str = "tce",
) -> pd.DataFrame:
    validate_horizon(horizon_months)
    prepared = prepare_monthly_history(frame, date_column, group_column, target_column)
    wide = prepared.pivot(index=date_column, columns=group_column, values=target_column).sort_index()

    if wide.isna().any().any():
        raise ValueError("Multivariate mean reversion requires a complete monthly panel.")

    log_wide = np.log(wide.astype("float64").clip(lower=LOG_FLOOR))
    lagged = log_wide.shift(1).dropna()
    current = log_wide.loc[lagged.index]
    design = np.column_stack([np.ones(len(lagged)), lagged.to_numpy()])
    coefficients, *_ = np.linalg.lstsq(design, current.to_numpy(), rcond=None)
    intercept = coefficients[0]
    transition = coefficients[1:]
    current_vector = log_wide.iloc[-1].to_numpy(dtype="float64")
    forecast_months = _future_months(log_wide.index[-1], horizon_months)
    records: list[dict[str, object]] = []

    for step_number, forecast_month in enumerate(forecast_months, start=1):
        current_vector = intercept + current_vector.dot(transition)
        for vessel_index, vessel_type in enumerate(log_wide.columns):
            records.append(
                {
                    "vessel_type": str(vessel_type),
                    "history_end_month": log_wide.index[-1],
                    "forecast_month": forecast_month,
                    "forecast_horizon_months": horizon_months,
                    "horizon": step_number,
                    "model_name": "multivariate_mean_reversion",
                    "forecast_tce": float(max(np.exp(current_vector[vessel_index]), 1.0)),
                }
            )

    return pd.DataFrame.from_records(records).sort_values(
        ["vessel_type", "forecast_month"]
    ).reset_index(drop=True)


def run_slide_model_forecast(
    frame: pd.DataFrame,
    model_name: str,
    horizon_months: int,
) -> pd.DataFrame:
    if model_name == "ornstein_uhlenbeck":
        return ornstein_uhlenbeck_forecast(frame, horizon_months)
    if model_name == "nonlinear_mean_reversion":
        return nonlinear_mean_reversion_forecast(frame, horizon_months)
    if model_name == "multivariate_mean_reversion":
        return multivariate_mean_reversion_forecast(frame, horizon_months)
    raise ValueError(f"Unsupported slide model: {model_name}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_history(path: Path) -> pd.DataFrame:
    history = pd.read_csv(path)
    history["month"] = pd.to_datetime(history["month"], errors="coerce")
    if history["month"].isna().any():
        raise ValueError("monthly_tce_history.csv contains invalid values in 'month'.")
    return history


def _current_fingerprint(history_path: Path, script_path: Path) -> dict[str, object]:
    history = _load_history(history_path)
    latest_month = pd.Timestamp(history["month"].max()).date().isoformat()
    return {
        "history_path": str(history_path),
        "history_sha256": _sha256(history_path),
        "history_rows": int(len(history)),
        "history_latest_month": latest_month,
        "script_path": str(script_path),
        "script_sha256": _sha256(script_path),
    }


def _load_metadata(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _needs_refresh(force: bool, fingerprint: dict[str, object]) -> bool:
    if force or not OUTPUT_PATH.exists() or not METADATA_PATH.exists():
        return True
    existing = _load_metadata(METADATA_PATH)
    if existing is None:
        return True
    for key, value in fingerprint.items():
        if existing.get(key) != value:
            return True
    return False


def _history_by_vessel(history: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        str(vessel_type): vessel_history.reset_index(drop=True)
        for vessel_type, vessel_history in history.groupby("vessel_type", sort=True)
    }


def _selection_lookup_for_horizon(horizon_months: int) -> dict[str, str]:
    selected = selected_models_frame()
    filtered = selected.loc[
        selected["forecast_horizon_months"] == horizon_months,
        ["vessel_type", "selected_model_name"],
    ]
    if filtered.empty:
        raise ValueError(f"No selected models found for horizon {horizon_months}.")
    return {
        str(row.vessel_type): str(row.selected_model_name)
        for row in filtered.itertuples(index=False)
    }


def _min_train_size_for_horizon(
    vessel_histories: dict[str, pd.DataFrame],
    horizon_months: int,
) -> int:
    first_origin_month = START_FORECAST_MONTH - pd.DateOffset(months=horizon_months)
    train_sizes: list[int] = []

    for vessel_type, vessel_history in vessel_histories.items():
        matching_rows = vessel_history.index[vessel_history["month"] == first_origin_month].tolist()
        if len(matching_rows) != 1:
            raise ValueError(
                f"Could not find first origin month {first_origin_month.date()} for "
                f"vessel_type '{vessel_type}'."
            )
        train_sizes.append(int(matching_rows[0]) + 1)

    return min(train_sizes)


def _build_exact_horizon_series(
    history: pd.DataFrame,
    horizon_months: int,
    min_train_size: int,
) -> pd.DataFrame:
    selected_lookup = _selection_lookup_for_horizon(horizon_months)
    vessel_histories = _history_by_vessel(history)
    history_length = min(len(vessel_history) for vessel_history in vessel_histories.values())
    origins = list(range(min_train_size - 1, history_length))

    if not origins:
        raise ValueError(
            f"No forecast origins available for horizon {horizon_months}. Check the history file."
        )

    records: list[dict[str, object]] = []

    for origin_index in origins:
        model_forecasts: dict[str, pd.DataFrame] = {}

        for model_name in sorted(set(selected_lookup.values())):
            selected_vessels = sorted(
                vessel_type
                for vessel_type, selected_model_name in selected_lookup.items()
                if selected_model_name == model_name
            )
            model_scope = MODEL_SCOPE_BY_NAME[model_name]

            if model_scope == "panel":
                training_frame = pd.concat(
                    [
                        vessel_history.iloc[: origin_index + 1].copy()
                        for vessel_history in vessel_histories.values()
                    ],
                    ignore_index=True,
                )
            else:
                training_frame = pd.concat(
                    [
                        vessel_histories[vessel_type].iloc[: origin_index + 1].copy()
                        for vessel_type in selected_vessels
                    ],
                    ignore_index=True,
                )

            model_forecasts[model_name] = run_slide_model_forecast(
                frame=training_frame,
                model_name=model_name,
                horizon_months=horizon_months,
            )

        for vessel_type, model_name in selected_lookup.items():
            vessel_history = vessel_histories[vessel_type]
            origin_month = pd.Timestamp(vessel_history.iloc[origin_index]["month"])
            forecast_month = origin_month + pd.offsets.MonthBegin(horizon_months)
            forecast_frame = model_forecasts[model_name]
            exact_row = forecast_frame.loc[
                (forecast_frame["vessel_type"].astype(str) == vessel_type)
                & (pd.to_numeric(forecast_frame["horizon"], errors="coerce") == horizon_months)
                & (pd.to_datetime(forecast_frame["forecast_month"], errors="coerce") == forecast_month)
            ]

            if len(exact_row) != 1:
                raise ValueError(
                    "Did not find exactly one exact-horizon forecast row for "
                    f"vessel_type={vessel_type!r}, model={model_name!r}, "
                    f"origin={origin_month.date()}, horizon={horizon_months}."
                )

            actual_index = origin_index + horizon_months
            actual_available = actual_index < len(vessel_history)
            actual_tce = (
                float(vessel_history.iloc[actual_index]["tce"]) if actual_available else None
            )

            records.append(
                {
                    "forecast_month": forecast_month,
                    "vessel_type": vessel_type,
                    "forecast_tce": float(exact_row.iloc[0]["forecast_tce"]),
                    "source_model_name": model_name,
                    "source_origin_month": origin_month,
                    "display_horizon_months": horizon_months,
                    "source_horizon_months": horizon_months,
                    "source_type": (
                        "walk_forward_history" if actual_available else "future_vintage_exact"
                    ),
                    "actual_tce": actual_tce,
                    "actual_available": actual_available,
                }
            )

    exact_series = pd.DataFrame.from_records(records)
    exact_series["forecast_month"] = pd.to_datetime(exact_series["forecast_month"], errors="coerce")
    exact_series["source_origin_month"] = pd.to_datetime(
        exact_series["source_origin_month"], errors="coerce"
    )
    exact_series = exact_series.sort_values(["vessel_type", "forecast_month"]).reset_index(drop=True)

    duplicate_count = int(
        exact_series.duplicated(subset=["vessel_type", "forecast_month"]).sum()
    )
    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate exact-series rows for horizon {horizon_months}."
        )

    return exact_series


def _write_workbook(
    exact_series_by_horizon: dict[int, pd.DataFrame],
    metadata: dict[str, object],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    selected_models = selected_models_frame()

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        for horizon_months, frame in exact_series_by_horizon.items():
            frame.to_excel(writer, sheet_name=f"{horizon_months}m_predictions", index=False)

        selected_models.to_excel(writer, sheet_name="selected_models", index=False)
        pd.DataFrame([metadata]).to_excel(writer, sheet_name="run_metadata", index=False)


def _refresh_predictions(force: bool = False) -> bool:
    script_path = Path(__file__).resolve()
    fingerprint = _current_fingerprint(history_path=DATA_PATH, script_path=script_path)

    if not _needs_refresh(force=force, fingerprint=fingerprint):
        print(f"No new data detected. Reusing existing workbook: {OUTPUT_PATH}")
        return False

    history = _load_history(DATA_PATH)
    prepared_history = prepare_monthly_history(history)
    vessel_histories = _history_by_vessel(prepared_history)

    exact_series_by_horizon: dict[int, pd.DataFrame] = {}
    for horizon_months in DISPLAY_HORIZONS:
        min_train_size = _min_train_size_for_horizon(vessel_histories, horizon_months)
        exact_series_by_horizon[horizon_months] = _build_exact_horizon_series(
            history=prepared_history,
            horizon_months=horizon_months,
            min_train_size=min_train_size,
        )

    metadata = {
        **fingerprint,
        "selected_models_source": "Reidar2/outputs/forecasts/slide_models_selected.csv",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_path": str(OUTPUT_PATH),
        "horizons": list(DISPLAY_HORIZONS),
    }

    _write_workbook(exact_series_by_horizon=exact_series_by_horizon, metadata=metadata)
    METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Wrote forecast workbook to: {OUTPUT_PATH}")
    print(f"Wrote refresh metadata to: {METADATA_PATH}")
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh exact-horizon spot-rate predictions from monthly_tce_history.csv "
            "only when the source data or this script changed."
        )
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force a refresh even if the source data fingerprint has not changed.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    _refresh_predictions(force=args.force)


if __name__ == "__main__":
    main()
