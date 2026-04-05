#!/usr/bin/env python3
from __future__ import annotations

from datetime import date as _date
from html import escape
from typing import Optional


_ALLOWED_MODES = {"observed", "forecast"}
_ALLOWED_PERIODS = {"hour", "day", "month"}
_ALLOWED_METRICS = {"avg", "gust", "peak"}
_ALLOWED_REGIONS = {"all", "south", "mid", "north"}


def _sanitize_mode(value: str) -> str:
    return value if value in _ALLOWED_MODES else "observed"


def _sanitize_period(value: str) -> str:
    return value if value in _ALLOWED_PERIODS else "hour"


def _sanitize_metric(value: str) -> str:
    return value if value in _ALLOWED_METRICS else "avg"


def _sanitize_region(value: str) -> str:
    return value if value in _ALLOWED_REGIONS else "all"


def build_wind_map_html(
    *,
    mode: str = "observed",
    period: str = "hour",
    metric: str = "avg",
    date_str: Optional[str] = None,
    forecast_hours: int = 24,
    region: str = "all",
    bbox: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    show_heatmap: bool = True,
    top_n: int = 600,
) -> str:
    """
    Render-safe fallback for wind map output.

    Denne funksjonen returnerer en enkel HTML-side slik at /ver/vind-kart alltid
    svarer uten import-feil. Den beholder samme signatur som ruten bruker.
    """
    safe_mode = _sanitize_mode(mode)
    safe_period = _sanitize_period(period)
    safe_metric = _sanitize_metric(metric)
    safe_region = _sanitize_region(region)

    selected_date = date_str or _date.today().isoformat()
    hours = max(6, min(int(forecast_hours or 24), 168))

    details = {
        "modus": safe_mode,
        "periode": safe_period,
        "måling": safe_metric,
        "dato": selected_date,
        "forecast_hours": str(hours),
        "region": safe_region,
        "bbox": bbox or "(ingen)",
        "zoom": z or "(ukjent)",
        "senter": f"{clat or '?'} , {clon or '?'}",
        "heatmap": "på" if show_heatmap else "av",
        "top_n": str(max(1, int(top_n or 1))),
    }

    rows = "".join(
        f"<tr><th>{escape(k)}</th><td>{escape(v)}</td></tr>" for k, v in details.items()
    )

    return f"""<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vindkart</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; padding: 24px; background: #0b1220; color: #e5e7eb; }}
    .card {{ max-width: 980px; margin: 0 auto; border: 1px solid #243041; border-radius: 12px; background: #0f172a; padding: 20px; }}
    h1 {{ margin: 0 0 10px; font-size: 1.3rem; }}
    p {{ margin: 0 0 14px; color: #93a3b8; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #1f2a3a; text-align: left; font-size: 0.95rem; }}
    th {{ width: 220px; color: #9fb0c7; font-weight: 600; }}
    .note {{ margin-top: 16px; padding: 10px 12px; background: #111b2e; border: 1px solid #22324c; border-radius: 8px; font-size: 0.92rem; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Vindkart er tilgjengelig</h1>
    <p>Tjenesten er oppe. Kartvisning kan bygges videre uten at deploy stopper.</p>
    <table>
      <tbody>{rows}</tbody>
    </table>
    <div class="note">Denne fallback-visningen er lagt inn for å sikre stabil oppstart i Render.</div>
  </div>
</body>
</html>
"""
