from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import build_forecast_accuracy_report as legacy


ROOT = Path(__file__).resolve().parent
SALES_OPS_ROOT = ROOT.parents[2]
V4_ROOT = SALES_OPS_ROOT / "Demand Planning Projects" / "Python Modeling" / "demand_forecast_prototype_v4"

ACTUALS_LOADER = V4_ROOT / "scripts" / "sql" / "load_actuals_only_to_sql.py"
MARKETING_LOADER = V4_ROOT / "scripts" / "sql" / "load_marketing_forecast_2026_to_sql.py"
CATALOG_LOADER = V4_ROOT / "scripts" / "sql" / "load_product_catalog_bu_to_sql.py"
REPORT_RUNNER = ROOT / "build_forecast_accuracy_report_db.py"

DEFAULT_ACTUALS_FILE = V4_ROOT / "data" / "raw" / "all_products_actuals_and_bookings.xlsx"
DEFAULT_MARKETING_FILE = ROOT / "Marketing Forecast Data (copy).xlsx"
DEFAULT_CATALOG_FILE = ROOT / "product_catalog_master.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monthly DB-first Forecast Accuracy runbook executor.")
    parser.add_argument("--month", type=str, default=None, help="Report month in YYYY-MM (defaults to previous month).")
    parser.add_argument("--server", type=str, required=True)
    parser.add_argument("--database", type=str, default="Forecast_Database")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    parser.add_argument("--actuals-file", type=str, default=str(DEFAULT_ACTUALS_FILE))
    parser.add_argument("--marketing-file", type=str, default=str(DEFAULT_MARKETING_FILE))
    parser.add_argument("--catalog-file", type=str, default=str(DEFAULT_CATALOG_FILE))
    parser.add_argument("--compare-baseline", choices=["none", "legacy", "copy2026", "db"], default="legacy")
    parser.add_argument("--dq-mode", choices=["off", "warn", "fail"], default="fail")
    parser.add_argument("--skip-actuals-load", action="store_true")
    parser.add_argument("--skip-marketing-load", action="store_true")
    parser.add_argument("--skip-catalog-load", action="store_true")
    return parser.parse_args()


def run_step(cmd: list[str], cwd: Path) -> None:
    print(f"[RUN] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd))
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def resolve_month(month_arg: str | None) -> date:
    return legacy.resolve_report_month(month_arg)


def main() -> None:
    args = parse_args()
    report_month = resolve_month(args.month)
    month_text = legacy.month_label(report_month)

    if not args.skip_actuals_load:
        run_step(
            [
                sys.executable,
                str(ACTUALS_LOADER),
                "--server",
                args.server,
                "--database",
                args.database,
                "--driver",
                args.driver,
                "--actuals-file",
                args.actuals_file,
                "--mode",
                "upsert_all",
            ],
            cwd=V4_ROOT,
        )

    if not args.skip_marketing_load:
        run_step(
            [
                sys.executable,
                str(MARKETING_LOADER),
                "--server",
                args.server,
                "--database",
                args.database,
                "--driver",
                args.driver,
                "--source-file",
                args.marketing_file,
                "--sheet",
                "2026 Data",
                "--start-row",
                "6",
            ],
            cwd=V4_ROOT,
        )

    if not args.skip_catalog_load:
        run_step(
            [
                sys.executable,
                str(CATALOG_LOADER),
                "--server",
                args.server,
                "--database",
                args.database,
                "--driver",
                args.driver,
                "--catalog-file",
                args.catalog_file,
            ],
            cwd=V4_ROOT,
        )

    report_name = f"{month_text} Forecast Accuracy Report db-monthly-run.xlsx"
    run_step(
        [
            sys.executable,
            str(REPORT_RUNNER),
            "--month",
            report_month.strftime("%Y-%m"),
            "--data-source",
            "db",
            "--server",
            args.server,
            "--database",
            args.database,
            "--driver",
            args.driver,
            "--compare-baseline",
            args.compare_baseline,
            "--dq-mode",
            args.dq_mode,
            "--output",
            f"outputs/reports/{report_name}",
        ],
        cwd=ROOT,
    )

    if args.compare_baseline != "none":
        compare_name = f"{month_text} Forecast Accuracy Comparison (db vs {args.compare_baseline}).xlsx"
        src = ROOT / "outputs" / "reports" / compare_name
        dst = ROOT / "outputs" / "comparisons" / f"{month_text} Forecast Accuracy Comparison (db vs {args.compare_baseline}) db-monthly-run.xlsx"
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            print(f"[MOVE] {src} -> {dst}")

    print("[DONE] Monthly DB-first Forecast Accuracy run completed.")


if __name__ == "__main__":
    main()
