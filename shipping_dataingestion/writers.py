from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataingestion.paths import ensure_data_dir
from dataingestion.schemas import DatasetSpec


def write_csv(frame: pd.DataFrame, path: Path) -> Path:
    ensure_data_dir()
    frame.to_csv(path, index=False)
    return path


def write_workbook(sheets: dict[str, pd.DataFrame], path: Path) -> Path:
    ensure_data_dir()
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
    return path


def write_dataset(data: pd.DataFrame | dict[str, pd.DataFrame], spec: DatasetSpec) -> Path:
    if spec.file_type == "csv":
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"{spec.name} expects a DataFrame for CSV output.")
        return write_csv(data, spec.target_path)

    if spec.file_type == "xlsx":
        if not isinstance(data, dict):
            raise TypeError(f"{spec.name} expects a sheet dictionary for XLSX output.")
        return write_workbook(data, spec.target_path)

    raise ValueError(f"Unsupported file type for {spec.name}: {spec.file_type}")
