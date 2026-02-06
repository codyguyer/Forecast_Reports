# Forecast Accuracy Report

This folder contains the Python script and input files used to generate the monthly Forecast Accuracy Report.

## What it does
- Builds Excel dashboards for totals, product family, marketing manager, and product-level accuracy.
- Produces a validation workbook with rollups and raw data.
- Applies formatting, highlights winners, and hides specified sheets/rows/columns.

## Key files
- `build_forecast_accuracy_report.py` — main report generator
- `Marketing Forecast Data.xlsx` — marketing forecast input
- `all_products_actuals_and_bookings.xlsx` — actuals input
- `stats_model_forecasts_YYYY-Mon.xlsx` — stats model input for the month
- `product_catalog_master.xlsx` — product catalog lookup

## Run the report
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

## Output
- `Mon Forecast Accuracy Report.xlsx` — main report
- `Mon Forecast Accuracy Validation.xlsx` — validation workbook

## GitHub setup (first time)
If this folder is not yet a Git repo:

```powershell
git init
git add README.md build_forecast_accuracy_report.py
git commit -m "Add forecast accuracy report generator"
```

Then add a remote and push:

```powershell
git remote add origin <your-github-repo-url>
git branch -M main
git push -u origin main
```

## Updating and pushing changes

```powershell
git add README.md build_forecast_accuracy_report.py
git commit -m "Update report logic"
git push
```
