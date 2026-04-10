# Portfolio Rebalancer (internal)

## Scope (phase 1)
- Upload multi-sheet Excel workbook.
- Detect likely portfolio sheets.
- Read fund holdings and current weights.
- Classify funds to asset classes using configurable mapping.
- Compute actual exposures per portfolio.
- Compute benchmark exposures from:
  - equity share by portfolio
  - Norway share within equity
  - EM share within international equity
- Compute active bets vs benchmark.
- Apply minimum trade threshold and rebalance mode:
  - `go_to_benchmark`
  - `reference_plus_active_bets`
  - `absolute_targets`
- Export results to Excel.

## Routes
- UI: `/internal/portfolio-rebalancer/`
- Export: `/internal/portfolio-rebalancer/export.xlsx`

## Architecture
- Calculation engine is isolated in `portfolio_rebalancer_engine.py`.
- UI + request handling is in `portfolio_rebalancer_routes.py`.
- Template is in `templates/portfolio_rebalancer.html`.
- The rebalance mode design is inspired by the suggested standalone script (benchmark-only, benchmark+active bets, and absolute targets), adapted to this repo's Flask workflow.

## Environment-specific settings
- Global upload size is controlled via `HANDLER_DB_UPLOAD_MAX_MB` in app runtime environment (used in `app.py` as `MAX_CONTENT_LENGTH`).
- App-level environment defaults and deployment-specific values are managed through existing environment mechanisms (`.env` / hosting environment), with examples in `config.example.py`.
- Excel import/export requires `openpyxl` to be installed in the runtime environment.
