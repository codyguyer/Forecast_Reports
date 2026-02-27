# Forecast Accuracy Source-to-DB Mapping

## Goal
Map each Forecast Accuracy report input to database-backed lineage for the DB migration workstream.

## Input Mapping

| Report Input | Legacy Source | DB Target |
|---|---|---|
| Marketing Forecast | `Marketing Forecast Data.xlsx` / `Tableau Data Pull` | `dbo.fact_marketing_forecast_raw` -> `dbo.vw_marketing_forecast_monthly_2026` |
| Actuals | `all_products_actuals_and_bookings.xlsx` | `dbo.fact_actuals_monthly` |
| Stats Forecast | `stats_model_forecasts_YYYY-Mon.xlsx` / `Forecast_Library` | `dbo.fact_forecast_monthly` |
| Product Catalog | `product_catalog_master.xlsx` | `dbo.dim_product` + `dbo.dim_business_unit` |

## Key Transform Rules

1. Marketing source for this phase is only `Marketing Forecast Data (copy).xlsx` sheet `2026 Data`, starting at row `A6`.
2. Month token (`Jan`..`Dec`) + fiscal year (`FY26`) maps to `forecast_month` month-start date.
3. `Division` BU value is normalized to `D200`.
4. Geography for report mapping remains `AMERICAS`.
5. D200 casework split behavior is preserved with location rules:
- `Loc1020` -> `ARTISAN CASEWORK`
- `Loc1080` -> `SYNTHESIS CASEWORK`

## Output Contract

DB-backed report path must produce the same workbook/tab structure as the current report generator:
- Main report workbook (`Mon Forecast Accuracy Report.xlsx`)
- Validation workbook (`Mon Forecast Accuracy Validation.xlsx`)

Dual-run comparison workbook is added for signoff:
- `Mon Forecast Accuracy Comparison (source vs baseline).xlsx`

## Known Delta Artifacts (Jan 2026)

Generated dual-run workbooks for documented variance review:
- `Jan Forecast Accuracy Comparison (copy2026 vs legacy).xlsx`
- `Jan Forecast Accuracy Comparison (db vs legacy).xlsx`

These files are the variance evidence set for stakeholder review and cutover signoff.
