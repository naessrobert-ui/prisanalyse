from __future__ import annotations

import pandas as pd

from dataingestion.providers import ProviderResult
from dataingestion.schemas import DatasetSpec


def validate_frame(frame: pd.DataFrame, required_columns: tuple[str, ...], dataset_name: str) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing required columns: {', '.join(missing)}")


def validate_workbook(
    sheets: dict[str, pd.DataFrame],
    required_sheets: dict[str, tuple[str, ...]],
    dataset_name: str,
) -> None:
    for sheet_name, required_columns in required_sheets.items():
        if sheet_name not in sheets:
            raise ValueError(f"{dataset_name} is missing required sheet: {sheet_name}")
        validate_frame(sheets[sheet_name], required_columns, f"{dataset_name}.{sheet_name}")


def normalize_dates(frame: pd.DataFrame, column: str = "Date") -> pd.DataFrame:
    if column not in frame.columns:
        return frame
    normalized = frame.copy()
    normalized[column] = pd.to_datetime(normalized[column], errors="coerce").dt.normalize()
    return normalized.dropna(subset=[column]).sort_values(column).reset_index(drop=True)


def transform_provider_result(result: ProviderResult, spec: DatasetSpec) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """Normalize a provider result into the exact shape expected by Data files.

    This is intentionally conservative today. When API providers are added,
    create dataset-specific branches here or split them into separate
    transformer functions.
    """
    data = result.data

    if isinstance(data, pd.DataFrame):
        frame = normalize_dates(data, "Date")
        if "month" in frame.columns:
            frame = normalize_dates(frame, "month")
        validate_frame(frame, spec.required_columns, spec.name)
        return frame

    if isinstance(data, dict):
        if spec.sheets is None:
            raise ValueError(f"{spec.name} does not define workbook sheets.")
        normalized_sheets = {
            sheet_name: normalize_dates(frame, "Date")
            for sheet_name, frame in data.items()
        }
        validate_workbook(normalized_sheets, spec.sheets, spec.name)
        return normalized_sheets

    raise TypeError(f"{spec.name} provider returned unsupported data type: {type(data)!r}")
