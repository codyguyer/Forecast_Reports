# Forecast Accuracy DB Monthly Control Checklist

Use this checklist for each monthly DB-first run.

## Pre-Run
- [ ] Confirm run month (`YYYY-MM`) and owner.
- [ ] Confirm SQL target: `(localdb)\MSSQLLocalDB` / `Forecast_Database` (or approved non-local target).
- [ ] Confirm source files are updated:
  - [ ] `demand_forecast_prototype_v4/data/raw/all_products_actuals_and_bookings.xlsx`
  - [ ] `Marketing Forecast Data (copy).xlsx` (`2026 Data`, data starts at `A6`)
  - [ ] `product_catalog_master.xlsx` (if product mapping/BU attributes changed)

## Execute
- [ ] Run:
  ```powershell
  python run_forecast_accuracy_db_monthly.py `
    --month YYYY-MM `
    --server "(localdb)\MSSQLLocalDB" `
    --database "Forecast_Database" `
    --compare-baseline legacy `
    --dq-mode fail
  ```

## Data Quality Gate
- [ ] Open DQ JSON in `outputs/comparisons/`.
- [ ] Verify `critical_failed = 0`.
- [ ] Review any warning checks and document disposition.

## Reconciliation
- [ ] Open DB-vs-legacy comparison workbook in `outputs/comparisons/`.
- [ ] Review total, BU, product family, manager, and product tabs.
- [ ] Document material variances and reasons.
- [ ] Confirm accepted vs non-accepted deltas with stakeholders.

## Signoff + Recordkeeping
- [ ] Save final report workbook under `outputs/reports/`.
- [ ] Save final comparison workbook under `outputs/comparisons/`.
- [ ] Update Mission Control dossier:
  - [ ] Last updated date
  - [ ] Next action
  - [ ] Checklist status updates
  - [ ] Progress %
- [ ] Record stakeholder signoff decision (or rollback decision).
