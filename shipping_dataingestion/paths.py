from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1] / "shipping_app"
DATA_DIR = BASE_DIR / "Data"


def data_path(*parts: str) -> Path:
    """Return a path inside the existing Data folder."""
    return DATA_DIR.joinpath(*parts)


def ensure_data_dir() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR
