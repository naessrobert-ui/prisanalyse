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

## Excel parsing notes
- The parser now accepts several common column names for holdings and weights, including `Fund`, `Security name`, `Weight`, `Exposure`, and `Lev. expo...`.
- Only rows with a non-empty `Security name` are treated as holdings; subtotal/header rows are ignored.
- For Nordea-style files, rows must also have a value in `Model portfolio` to be included as holdings.
- `Lev. expo. distr. (PF)` is used as primary weight source, with optional fallback to `MV`-share when exposure values are missing.
- Rows/funds classified as `Unclassified` (or `nan`/empty fund names) are excluded from final exposure/trade tables.
- If no portfolio sheet is auto-detected, the tool attempts all workbook sheets.
- Supported file types: `.xlsx` and `.xlsm`.
- If upload fails with file-size error, check `HANDLER_DB_UPLOAD_MAX_MB` and any proxy limits in front of Flask.
- Standard category-set uses granular reference buckets: `cash`, `allocation`, `equity_norway`, `equity_global_developed`, `equity_global_em`, `fi_norway_short`, `fi_norway_long`, `fi_global`.
- Default benchmark presets now infer equity share from portfolio code (e.g. `G1090` => 10%, `G3070` => 30%), and apply portfolio-specific allocation targets (`G1090`=7%, `G3070/G5050/G6535/G8020`=10%, `EG`=0%).
- UI displays human-friendly Norwegian category labels and percentage values with two decimals in result/holding tables.

## Architecture
- Calculation engine is isolated in `portfolio_rebalancer_engine.py`.
- UI + request handling is in `portfolio_rebalancer_routes.py`.
- Template is in `templates/portfolio_rebalancer.html`.
- The rebalance mode design is inspired by the suggested standalone script (benchmark-only, benchmark+active bets, and absolute targets), adapted to this repo's Flask workflow.

## Environment-specific settings
- Global upload size is controlled via `HANDLER_DB_UPLOAD_MAX_MB` in app runtime environment (used in `app.py` as `MAX_CONTENT_LENGTH`).
- App-level environment defaults and deployment-specific values are managed through existing environment mechanisms (`.env` / hosting environment), with examples in `config.example.py`.
- Excel import/export requires `openpyxl` to be installed in the runtime environment.
