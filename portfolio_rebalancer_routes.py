from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from io import StringIO

import pandas as pd
from flask import Blueprint, make_response, render_template, request, session
from werkzeug.exceptions import RequestEntityTooLarge

import portfolio_rebalancer_engine as engine

portfolio_rebalancer_bp = Blueprint(
    "portfolio_rebalancer",
    __name__,
    url_prefix="/internal/portfolio-rebalancer",
)
_UPLOAD_LIMIT_MB = max(1, int((os.environ.get("HANDLER_DB_UPLOAD_MAX_MB", "300") or "300").strip() or "300"))

DEFAULT_MAPPING = {
    "PB Norway: Balanced Global 10%EQ + 90%FI": "cash",
    "Nordea PB Norsk Aksje Portefolje B Acc NOK": "equity_norway",
    "Nordea 1 - Global Portfolio Fund BF-NOK": "equity_global_developed",
    "Nordea 2 - BetaPlus Enhan Global Eq Fd BF-NOK": "equity_global_developed",
    "Nordea Discretionary Global Equity C Acc NOK": "equity_global_developed",
    "Nordea Emerging Market Equities Fund C Acc NOK": "equity_global_em",
    "Nordea FRN Pensjon C Acc NOK": "fi_norway_short",
    "Nordea Kort Obligasjon Pluss C Acc NOK": "fi_norway_short",
    "Nordea Obligasjon III C Acc NOK": "fi_norway_long",
    "Nordea 1 - European Corporate Bond Fund HBCN-NOK": "fi_global",
    "Nordea 1 - Global High Yield Bond Fund HBCN-NOK": "fi_global",
    "Nordea 1 - US Corporate Bond Fund HBC-NOK": "fi_global",
    "Nordea Allokeringsfond Fund C Acc NOK": "allocation",
}


@dataclass
class UiState:
    mapping_json: str
    benchmark_json: str
    active_bets_json: str
    absolute_targets_json: str
    min_trade_threshold: float
    rebalance_mode: str


def _default_benchmark(portfolios: list[str]) -> list[dict[str, float | str]]:
    return [
        {
            "portfolio": portfolio,
            "equity_share": 0.60,
            "norway_share_within_equity": 0.20,
            "em_share_within_international_equity": 0.15,
        }
        for portfolio in portfolios
    ]


@portfolio_rebalancer_bp.errorhandler(RequestEntityTooLarge)
def handle_too_large(_err):
    return render_template(
        "portfolio_rebalancer.html",
        detected_sheets=[],
        results_preview=[],
        holdings_preview=[],
        portfolio_overview=[],
        export_ready=False,
        error_message=f"Filen er for stor for opplasting via appen (grense: {_UPLOAD_LIMIT_MB} MB).",
        ui_state=asdict(
            UiState(
                mapping_json=json.dumps(DEFAULT_MAPPING, indent=2, ensure_ascii=False),
                benchmark_json=json.dumps([], indent=2),
                active_bets_json=json.dumps([], indent=2),
                absolute_targets_json=json.dumps([], indent=2),
                min_trade_threshold=0.01,
                rebalance_mode="go_to_benchmark",
            )
        ),
    ), 413


@portfolio_rebalancer_bp.route("/", methods=["GET", "POST"])
def index():
    detected_sheets: list[str] = []
    results_preview: list[dict[str, object]] = []
    holdings_preview: list[dict[str, object]] = []
    portfolio_overview: list[dict[str, object]] = []
    error_message = ""
    export_ready = False

    ui_state = UiState(
        mapping_json=json.dumps(DEFAULT_MAPPING, indent=2, ensure_ascii=False),
        benchmark_json=json.dumps([], indent=2),
        active_bets_json=json.dumps([], indent=2),
        absolute_targets_json=json.dumps([], indent=2),
        min_trade_threshold=0.01,
        rebalance_mode="go_to_benchmark",
    )

    if request.method == "POST":
        ui_state.mapping_json = (request.form.get("mapping_json") or ui_state.mapping_json).strip()
        ui_state.benchmark_json = (request.form.get("benchmark_json") or ui_state.benchmark_json).strip()
        ui_state.active_bets_json = (request.form.get("active_bets_json") or ui_state.active_bets_json).strip()
        ui_state.absolute_targets_json = (request.form.get("absolute_targets_json") or ui_state.absolute_targets_json).strip()
        ui_state.min_trade_threshold = float(request.form.get("min_trade_threshold") or ui_state.min_trade_threshold)
        ui_state.rebalance_mode = (request.form.get("rebalance_mode") or "go_to_benchmark").strip() or "go_to_benchmark"

        upload = request.files.get("workbook")
        if not upload or not upload.filename:
            error_message = "Velg en Excel-fil (.xlsx) før beregning."
        elif not upload.filename.lower().endswith((".xlsx", ".xlsm")):
            error_message = "Ugyldig filtype. Bruk .xlsx eller .xlsm."
        else:
            try:
                workbook = engine.workbook_to_frames(upload.read())
                detected_sheets = engine.detect_portfolio_sheets(workbook)
                if not detected_sheets:
                    detected_sheets = list(workbook.keys())

                holdings = engine.extract_holdings(workbook, detected_sheets)
                mapping = json.loads(ui_state.mapping_json)
                benchmark_rows = json.loads(ui_state.benchmark_json) if ui_state.benchmark_json else []
                if not benchmark_rows:
                    benchmark_rows = _default_benchmark(sorted(holdings["portfolio"].unique().tolist()))
                    ui_state.benchmark_json = json.dumps(benchmark_rows, indent=2, ensure_ascii=False)

                classified = engine.classify_holdings(holdings, mapping)
                actual = engine.compute_actual_exposures(classified)
                benchmark = engine.compute_benchmark_exposures(pd.DataFrame(benchmark_rows))
                active_bets = pd.DataFrame(json.loads(ui_state.active_bets_json)) if ui_state.active_bets_json else pd.DataFrame()
                absolute_targets = pd.DataFrame(json.loads(ui_state.absolute_targets_json)) if ui_state.absolute_targets_json else pd.DataFrame()
                target = engine.compute_target_exposures(
                    benchmark_exposures=benchmark,
                    rebalance_mode=ui_state.rebalance_mode,
                    active_bets=active_bets if not active_bets.empty else None,
                    absolute_targets=absolute_targets if not absolute_targets.empty else None,
                )
                active = engine.compute_active_bets(
                    actual,
                    benchmark,
                    target,
                    minimum_trade_threshold=ui_state.min_trade_threshold,
                )

                session["portfolio_rebalancer_export"] = {
                    "holdings": holdings.to_json(orient="records"),
                    "classified": classified.to_json(orient="records"),
                    "actual": actual.to_json(orient="records"),
                    "benchmark": benchmark.to_json(orient="records"),
                    "target": target.to_json(orient="records"),
                    "active": active.to_json(orient="records"),
                }
                export_ready = True
                results_preview = active.round(4).to_dict(orient="records")
                holdings_preview = (
                    classified.sort_values(["portfolio", "asset_class", "fund"])
                    .round(4)
                    .to_dict(orient="records")
                )
                overview = (
                    active.groupby("portfolio", as_index=False)
                    .agg(
                        antall_aktivaklasser=("asset_class", "count"),
                        sum_abs_active=("active_bet", lambda s: float(s.abs().sum())),
                        sum_abs_trade=("suggested_trade", lambda s: float(s.abs().sum())),
                    )
                    .sort_values("portfolio")
                )
                portfolio_overview = overview.round(4).to_dict(orient="records")
            except Exception as exc:  # noqa: BLE001
                error_message = f"Kunne ikke behandle workbook: {exc}"

    return render_template(
        "portfolio_rebalancer.html",
        detected_sheets=detected_sheets,
        results_preview=results_preview,
        holdings_preview=holdings_preview,
        portfolio_overview=portfolio_overview,
        error_message=error_message,
        export_ready=export_ready,
        ui_state=asdict(ui_state),
    )


@portfolio_rebalancer_bp.route("/export.xlsx", methods=["GET"])
def export_xlsx():
    cached = session.get("portfolio_rebalancer_export")
    if not cached:
        resp = make_response("Ingen resultater å eksportere.", 400)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return resp

    frames = {
        name: pd.read_json(StringIO(payload))
        for name, payload in cached.items()
    }
    payload = engine.export_results_to_excel(frames)
    resp = make_response(payload)
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    resp.headers["Content-Disposition"] = "attachment; filename=portfolio_rebalancer_result.xlsx"
    return resp
