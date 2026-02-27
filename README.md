# Forecast Accuracy Report

This folder contains the scripts and input files used to generate the monthly Forecast Accuracy Report, including a DB-capable migration path.

## What it does
- Builds Excel dashboards for totals, product family, marketing manager, and product-level accuracy.
- Produces a validation workbook with rollups and raw data.
- Applies formatting, highlights winners, and hides specified sheets/rows/columns.

## Key files
- `build_forecast_accuracy_report.py` - legacy report generator
- `build_forecast_accuracy_report_db.py` - DB/copy2026-capable report generator with dual-run comparison support
- `build_forecast_accuracy_trend_report_db.py` - DB-backed rolling trend report (12-month default)
- `run_forecast_accuracy_db_monthly.py` - DB-first monthly run orchestrator (loaders + report + comparison)
- `Marketing Forecast Data.xlsx` - marketing forecast input for both legacy and 2026 migration flow (`2026 Data`, starts at `A6` for migration parser)
- `all_products_actuals_and_bookings.xlsx` - actuals input
- `stats_model_forecasts_YYYY-Mon.xlsx` - stats model input for the month
- `product_catalog_master.xlsx` - product catalog lookup
- `forecast_accuracy_source_to_db_mapping.md` - source-to-target mapping for migration
- `db_monthly_control_checklist.md` - operating SOP and monthly control checklist

## Folder layout
- Root folder: source scripts + core input files
- `outputs/reports/`: generated report and validation workbooks
- `outputs/comparisons/`: dual-run comparison workbooks
- `Archive/26-02_cutover-db-testing/analysis/`: archived pre-sync + post-fix cutover analysis packages

## Default run mode (DB-first)

Run from this folder:

```powershell
python run_forecast_accuracy_db_monthly.py `
  --month YYYY-MM `
  --server "(localdb)\MSSQLLocalDB" `
  --database "Forecast_Database" `
  --compare-baseline legacy `
  --dq-mode fail
```

Optional skip flags:
- `--skip-actuals-load`
- `--skip-marketing-load`
- `--skip-catalog-load`
- `--skip-trend-report`

Trend options:
- `--trend-window-months` (default `12`)
- `--trend-top-n-products` (default `10`)

## One-command run (Essbase refresh + DB monthly run)

This mode refreshes `Marketing Forecast Data.xlsx` from Smart View/Eessbase first, validates parser shape with a dry-run, then executes the DB monthly runbook.

```powershell
powershell -ExecutionPolicy Bypass -File .\run_forecast_accuracy_db_monthly_with_refresh.ps1 `
  -Month YYYY-MM `
  -Server "(localdb)\MSSQLLocalDB" `
  -Database "Forecast_Database"
```

Required:
- `ESSBASE_PASSWORD` env var set, or pass `-EssbasePassword`.
  If neither is provided, the script now prompts for a hidden password entry at runtime (used for that run).

Optional:
- `-SkipActualsLoad`
- `-SkipCatalogLoad`
- `-CompareBaseline legacy|none|copy2026|db`
- `-DQMode fail|warn|off`
- `-VisibleExcel`

## Load marketing 2026 data to SQL (v4)

From repo root:

```powershell
python "Demand Planning Projects/Python Modeling/demand_forecast_prototype_v4/scripts/sql/load_marketing_forecast_2026_to_sql.py" `
  --server "YOUR_SQL_SERVER" `
  --source-file "Reporting/Ad Hoc Reports/Forecast Reports/Marketing Forecast Data.xlsx"
```

Defaults:
- `--sheet` = `2026 Data`
- `--start-row` = `6` (data starts at `A6`)

## Load BU-grain product catalog attributes to SQL (v4)

This supports product+BU reporting joins and resolves multi-BU mapping gaps that `dim_product` alone cannot represent.

```powershell
python "Demand Planning Projects/Python Modeling/demand_forecast_prototype_v4/scripts/sql/load_product_catalog_bu_to_sql.py" `
  --server "(localdb)\MSSQLLocalDB" `
  --catalog-file "Reporting/Ad Hoc Reports/Forecast Reports/product_catalog_master.xlsx"
```

## Run the report (legacy)
From this folder:

```powershell
python build_forecast_accuracy_report.py
```

Optional:

```powershell
python build_forecast_accuracy_report.py --month YYYY-MM --output "Custom Name.xlsx"
```

Notes:
- `--month` defaults to the previous month.
- Output is written to this folder.

## Run DB/copy2026 migration path

Recommended (copy2026 source, compare against legacy):

```powershell
python build_forecast_accuracy_report_db.py --month YYYY-MM --data-source copy2026 --compare-baseline legacy
```

DB-backed source:

```powershell
python build_forecast_accuracy_report_db.py `
  --month YYYY-MM `
  --data-source db `
  --server "YOUR_SQL_SERVER" `
  --database "Forecast_Database" `
  --compare-baseline legacy
```

Key options:
- `--data-source`: `copy2026`, `legacy`, or `db`
- `--compare-baseline`: `none`, `legacy`, `copy2026`, or `db`
- `--marketing-copy-file`, `--marketing-copy-sheet`, `--start-row`
- `--dq-mode`: `off`, `warn`, or `fail`
- `--dq-log`: optional DQ JSON output path

## Run rolling trend report (DB)

```powershell
python build_forecast_accuracy_trend_report_db.py `
  --month YYYY-MM `
  --server "(localdb)\MSSQLLocalDB" `
  --database "Forecast_Database" `
  --window-months 12 `
  --top-n-products 10 `
  --dq-mode fail
```

Behavior:
- Rolling window defaults to 12 months ending at the anchor month (`--month`, or previous month if omitted).
- Product Top 10 is ranked from the latest month in the rolling window (stable tie-break by product code).

## Output
- `Mon Forecast Accuracy Report.xlsx` - main report
- `Mon Forecast Accuracy Validation.xlsx` - validation workbook
- `Mon Forecast Accuracy Comparison (source vs baseline).xlsx` - dual-run variance workbook (when enabled)
- `outputs/comparisons/Mon Forecast Accuracy DQ (source).json` - DQ check results and gate outcome
- `Mon Forecast Accuracy Trend Report.xlsx` - rolling trend workbook
- `outputs/comparisons/Mon Forecast Accuracy Trend DQ (db).json` - trend DQ checks and gate outcome

## Monthly sequence (DB-only operation)
1. Refresh source files:
   - `Demand Planning Projects/Python Modeling/demand_forecast_prototype_v4/data/raw/all_products_actuals_and_bookings.xlsx`
   - `Marketing Forecast Data.xlsx`
   - `product_catalog_master.xlsx` (if product attribute changes exist)
2. Run DB monthly orchestrator.
3. Review DQ JSON results and address critical failures.
4. Review comparison workbook deltas and document accepted variances.
5. Archive outputs under `outputs/reports` and `outputs/comparisons`.
6. Update Mission Control dossier checklist/status.

## Parser validation tests

From this folder:

```powershell
pytest test_marketing_copy_2026_parser.py
```
