from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import build_forecast_accuracy_report as legacy

try:
    import pyodbc
except ImportError:
    pyodbc = None


ROOT = legacy.ROOT
MARKETING_COPY_FILE = ROOT / "Marketing Forecast Data.xlsx"


@dataclass
class SqlConfig:
    server: str
    database: str
    driver: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Forecast Accuracy report with legacy/copy2026/DB input paths and optional dual-run comparison."
    )
    parser.add_argument("--month", type=str, default=None, help="Report month in YYYY-MM (defaults to previous month).")
    parser.add_argument("--output", type=str, default=None, help="Output XLSX filename.")
    parser.add_argument(
        "--data-source",
        choices=["copy2026", "legacy", "db"],
        default="copy2026",
        help="Input source mode: copy2026 (recommended), legacy Tableau pull file, or SQL DB.",
    )
    parser.add_argument("--marketing-copy-file", type=str, default=str(MARKETING_COPY_FILE))
    parser.add_argument("--marketing-copy-sheet", type=str, default="2026 Data")
    parser.add_argument("--start-row", type=int, default=6, help="1-based start row for 2026 Data input.")
    parser.add_argument("--server", type=str, default="localhost")
    parser.add_argument("--database", type=str, default="Forecast_Database")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    parser.add_argument(
        "--compare-baseline",
        choices=["none", "legacy", "copy2026", "db"],
        default="legacy",
        help="Optional baseline source for dual-run comparison workbook.",
    )
    parser.add_argument(
        "--dq-mode",
        choices=["off", "warn", "fail"],
        default="fail",
        help="Data-quality enforcement mode. 'fail' stops report generation on critical failed checks.",
    )
    parser.add_argument(
        "--dq-log",
        type=str,
        default=None,
        help="Optional JSON path for DQ results. Defaults under outputs/comparisons.",
    )
    return parser.parse_args()


def connect_sql(cfg: SqlConfig):
    if pyodbc is None:
        raise ImportError("pyodbc is required for --data-source db. Install with: pip install pyodbc")
    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def parse_marketing_copy_2026(path: Path, sheet: str = "2026 Data", start_row: int = 6) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    start_idx = start_row - 1
    if len(raw) <= start_idx:
        raise ValueError(f"Sheet {sheet!r} has no data at row {start_row}.")

    data = raw.iloc[start_idx:].copy()
    if data.shape[1] < 11:
        raise ValueError(f"Sheet {sheet!r} expected at least 11 columns from row {start_row}.")
    data = data.iloc[:, :11]
    data.columns = [
        "fiscal_year",
        "BU",
        "Location",
        "Geography",
        "Product",
        "Period",
        "Budget (Dollars)",
        "Forecast (Dollars)",
        "_blank",
        "Budget (Quantity)",
        "Forecast (Quantity)",
    ]
    data = data.drop(columns=["_blank"])

    for c in ["fiscal_year", "BU", "Location", "Geography", "Product", "Period"]:
        data[c] = data[c].astype(str).str.strip()
    for c in ["Budget (Dollars)", "Forecast (Dollars)", "Budget (Quantity)", "Forecast (Quantity)"]:
        data[c] = pd.to_numeric(data[c], errors="coerce")

    month_map = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }
    data = data[data["Period"].str.upper().isin(month_map.keys())].copy()
    if data.empty:
        raise ValueError("No month rows found in marketing 2026 data after parsing.")

    year = (
        pd.to_numeric(data["fiscal_year"].str.upper().str.extract(r"FY(\d{2})")[0], errors="coerce").fillna(26).astype(int) + 2000
    )
    data["Date"] = pd.to_datetime(
        {
            "year": year,
            "month": data["Period"].str.upper().map(month_map),
            "day": 1,
        },
        errors="coerce",
    )
    data = data[data["Date"].notna()].copy()
    return data[
        [
            "BU",
            "Location",
            "Geography",
            "Product",
            "Date",
            "Forecast (Dollars)",
            "Forecast (Quantity)",
            "Budget (Dollars)",
            "Budget (Quantity)",
        ]
    ]


def validate_marketing_copy_2026(df: pd.DataFrame) -> dict[str, Any]:
    month_counts = df["Date"].dt.month.nunique()
    if month_counts != 12:
        raise ValueError(f"Expected 12 months in parsed marketing data, found {month_counts}.")
    return {
        "rows": int(len(df)),
        "months": sorted(df["Date"].dt.month.unique().tolist()),
        "distinct_bu": sorted(df["BU"].dropna().astype(str).str.strip().str.upper().unique().tolist()),
        "distinct_location": sorted(df["Location"].dropna().astype(str).str.strip().str.upper().unique().tolist()),
    }


def _as_records(df: pd.DataFrame, limit: int = 15) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.head(limit).to_dict("records")


def run_dq_checks(
    report_month: date,
    source: str,
    marketing_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    actuals_df: pd.DataFrame,
) -> dict[str, Any]:
    month_start = pd.Timestamp(report_month).to_period("M").to_timestamp()
    checks: list[dict[str, Any]] = []

    def add_check(name: str, severity: str, passed: bool, details: dict[str, Any]) -> None:
        checks.append(
            {
                "name": name,
                "severity": severity,
                "passed": bool(passed),
                "details": details,
            }
        )

    add_check(
        "marketing_rows_present",
        "critical",
        not marketing_df.empty,
        {"row_count": int(len(marketing_df))},
    )
    add_check(
        "catalog_rows_present",
        "critical",
        not catalog_df.empty,
        {"row_count": int(len(catalog_df))},
    )
    add_check(
        "stats_rows_present_for_month",
        "critical",
        not stats_df.empty,
        {"row_count": int(len(stats_df)), "month": str(month_start.date())},
    )
    add_check(
        "actuals_rows_present_for_month",
        "critical",
        not actuals_df.empty,
        {"row_count": int(len(actuals_df)), "month": str(month_start.date())},
    )

    if not marketing_df.empty:
        key_nulls = marketing_df[["BU", "Location", "Product", "Date"]].isna().any(axis=1).sum()
        add_check(
            "marketing_key_nulls",
            "critical",
            int(key_nulls) == 0,
            {"null_key_rows": int(key_nulls)},
        )
        dupes = marketing_df.duplicated(subset=["BU", "Location", "Geography", "Product", "Date"]).sum()
        add_check(
            "marketing_duplicate_keys",
            "warning",
            int(dupes) == 0,
            {"duplicate_rows": int(dupes)},
        )
        geo = marketing_df["Geography"].astype(str).str.upper().str.strip()
        non_amer = marketing_df[geo != "AMERICAS"][["BU", "Location", "Geography", "Product", "Date"]]
        add_check(
            "marketing_geography_filter_behavior",
            "warning",
            non_amer.empty,
            {"non_americas_rows": int(len(non_amer)), "sample": _as_records(non_amer)},
        )

    if not actuals_df.empty:
        act_nulls = actuals_df[["Product", "Division", "Month"]].isna().any(axis=1).sum()
        add_check(
            "actuals_key_nulls",
            "critical",
            int(act_nulls) == 0,
            {"null_key_rows": int(act_nulls)},
        )
        act_dupes = actuals_df.duplicated(subset=["Product", "Division", "Month"]).sum()
        add_check(
            "actuals_duplicate_keys",
            "warning",
            int(act_dupes) == 0,
            {"duplicate_rows": int(act_dupes)},
        )

    if not stats_df.empty:
        st_nulls = stats_df[["product_id", "bu_id", "forecast_month", "model_type"]].isna().any(axis=1).sum()
        add_check(
            "stats_key_nulls",
            "critical",
            int(st_nulls) == 0,
            {"null_key_rows": int(st_nulls)},
        )
        st_dupes = stats_df.duplicated(subset=["product_id", "bu_id", "forecast_month", "model_type", "run_id"]).sum()
        add_check(
            "stats_duplicate_keys",
            "warning",
            int(st_dupes) == 0,
            {"duplicate_rows": int(st_dupes)},
        )

    if not catalog_df.empty:
        cat_nulls = catalog_df[["group_key", "business_unit_code", "sku_list"]].isna().any(axis=1).sum()
        add_check(
            "catalog_key_nulls",
            "critical",
            int(cat_nulls) == 0,
            {"null_key_rows": int(cat_nulls)},
        )

    if source == "copy2026":
        m = marketing_df.copy()
        m["month_num"] = pd.to_datetime(m["Date"], errors="coerce").dt.month
        months_per_key = (
            m.groupby(["BU", "Location", "Geography", "Product"], dropna=False)["month_num"]
            .nunique()
            .reset_index(name="month_count")
        )
        incomplete = months_per_key[months_per_key["month_count"] != 12]
        add_check(
            "marketing_12_month_completeness_per_key",
            "warning",
            incomplete.empty,
            {"incomplete_key_count": int(len(incomplete)), "sample": _as_records(incomplete)},
        )

    summary = {
        "report_month": str(month_start.date()),
        "source": source,
        "checks_total": len(checks),
        "checks_failed": int(sum(0 if c["passed"] else 1 for c in checks)),
        "critical_failed": int(sum(0 if (c["passed"] or c["severity"] != "critical") else 1 for c in checks)),
        "warning_failed": int(sum(0 if (c["passed"] or c["severity"] != "warning") else 1 for c in checks)),
        "checks": checks,
    }
    return summary


def load_from_db(report_month: date, sql_cfg: SqlConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    start = pd.Timestamp(report_month)
    end = (start + pd.offsets.MonthBegin(1)).date()
    start_date = start.date()

    with connect_sql(sql_cfg) as conn:
        marketing = pd.read_sql(
            """
            SELECT
                bu_code AS [BU],
                location_code AS [Location],
                geography AS [Geography],
                product_code_raw AS [Product],
                forecast_month AS [Date],
                forecast_dollars AS [Forecast (Dollars)],
                forecast_quantity AS [Forecast (Quantity)]
            FROM dbo.vw_marketing_forecast_monthly_2026
            WHERE forecast_month >= ? AND forecast_month < ?
            """,
            conn,
            params=[start_date, end],
        )
        # Prefer BU-grain catalog attributes when available.
        try:
            catalog = pd.read_sql(
                """
                SELECT
                    group_key,
                    business_unit_code,
                    business_unit_name,
                    sku_list,
                    product_family,
                    marketing_manager,
                    salesforce_feature_mode
                FROM dbo.vw_product_catalog_bu_reporting
                """,
                conn,
            )
        except Exception:
            catalog = pd.read_sql(
                """
                SELECT
                    p.product_code AS group_key,
                    p.business_unit_code AS business_unit_code,
                    COALESCE(b.bu_name, p.business_unit_code) AS business_unit_name,
                    p.sku_list,
                    p.product_family,
                    p.marketing_manager,
                    p.salesforce_feature_mode
                FROM dbo.dim_product p
                LEFT JOIN dbo.dim_business_unit b
                    ON b.bu_code = p.business_unit_code
                """,
                conn,
            )
        stats = pd.read_sql(
            """
            SELECT
                product_code AS product_id,
                bu_code AS bu_id,
                forecast_month,
                model_type,
                forecast_value,
                recommended_model,
                run_id
            FROM dbo.fact_forecast_monthly
            WHERE forecast_month >= ? AND forecast_month < ?
            """,
            conn,
            params=[start_date, end],
        )
        actuals = pd.read_sql(
            """
            SELECT
                product_code AS [Product],
                bu_code AS [Division],
                month_start AS [Month],
                actuals AS [Actuals],
                bookings AS [Bookings]
            FROM dbo.fact_actuals_monthly
            WHERE month_start >= ? AND month_start < ?
            """,
            conn,
            params=[start_date, end],
        )

    marketing["Date"] = pd.to_datetime(marketing["Date"], errors="coerce")
    stats["forecast_month"] = pd.to_datetime(stats["forecast_month"], errors="coerce")
    actuals["Month"] = pd.to_datetime(actuals["Month"], errors="coerce")
    return marketing, catalog, stats, actuals


def load_frames(
    source: str,
    report_month: date,
    marketing_copy_file: Path,
    marketing_copy_sheet: str,
    start_row: int,
    sql_cfg: SqlConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    stats_file = ROOT / legacy.stats_model_filename(report_month)
    if source == "legacy":
        return (
            legacy.load_marketing_data(legacy.MARKETING_FILE),
            legacy.load_product_catalog(legacy.PRODUCT_CATALOG_FILE),
            legacy.load_stats_model(stats_file),
            legacy.load_actuals_data(legacy.ACTUALS_FILE),
        )
    if source == "copy2026":
        marketing = parse_marketing_copy_2026(marketing_copy_file, marketing_copy_sheet, start_row)
        validate_marketing_copy_2026(marketing)
        return (
            marketing,
            legacy.load_product_catalog(legacy.PRODUCT_CATALOG_FILE),
            legacy.load_stats_model(stats_file),
            legacy.load_actuals_data(legacy.ACTUALS_FILE),
        )
    if source == "db":
        return load_from_db(report_month, sql_cfg)
    raise ValueError(f"Unsupported source: {source}")


def build_report_from_frames(
    report_month: date,
    output_path: Path,
    marketing_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    actuals_df: pd.DataFrame,
) -> None:
    with tempfile.TemporaryDirectory(prefix="forecast_accuracy_db_") as td:
        tmp = Path(td)
        marketing_file = tmp / "marketing.xlsx"
        catalog_file = tmp / "catalog.xlsx"
        stats_file = tmp / "stats.xlsx"
        actuals_file = tmp / "actuals.xlsx"

        with pd.ExcelWriter(marketing_file, engine="openpyxl") as writer:
            marketing_df.to_excel(writer, sheet_name="Tableau Data Pull", index=False)
        catalog_df.to_excel(catalog_file, index=False)
        with pd.ExcelWriter(stats_file, engine="openpyxl") as writer:
            stats_df.to_excel(writer, sheet_name="Forecast_Library", index=False)
        actuals_df.to_excel(actuals_file, index=False)

        original_actuals = legacy.ACTUALS_FILE
        try:
            legacy.ACTUALS_FILE = actuals_file
            cfg = legacy.ReportConfig(
                report_month=report_month,
                marketing_file=marketing_file,
                product_catalog_file=catalog_file,
                stats_model_file=stats_file,
                output_file=output_path,
            )
            legacy.write_report(cfg)
        finally:
            legacy.ACTUALS_FILE = original_actuals


def build_comparison_workbook(
    report_month: date,
    compare_path: Path,
    source_name: str,
    baseline_name: str,
    source_raw: pd.DataFrame,
    baseline_raw: pd.DataFrame,
) -> None:
    def summarize(raw: pd.DataFrame) -> dict[str, pd.DataFrame]:
        return {
            "Totals": legacy.build_totals_dashboard(raw),
            "Prod Fam": legacy.build_prod_fam_dashboard(raw),
            "Prod Fam WAPE": legacy.build_prod_fam_wape_dashboard(raw),
            "Mkt Mgr": legacy.build_marketing_manager_dashboard(raw),
            "Product": legacy.build_product_dashboard(raw),
        }

    def compare_df(name: str, left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        key_cols = [c for c in left.columns if c not in {"Stats Model", "Marketing"} and c in right.columns]
        merged = left.merge(right, on=key_cols, how="outer", suffixes=(f" ({source_name})", f" ({baseline_name})"), indicator=True)
        s_left = f"Stats Model ({source_name})"
        s_right = f"Stats Model ({baseline_name})"
        m_left = f"Marketing ({source_name})"
        m_right = f"Marketing ({baseline_name})"
        if s_left in merged.columns and s_right in merged.columns:
            merged["Stats Delta"] = pd.to_numeric(merged[s_left], errors="coerce") - pd.to_numeric(merged[s_right], errors="coerce")
        if m_left in merged.columns and m_right in merged.columns:
            merged["Marketing Delta"] = pd.to_numeric(merged[m_left], errors="coerce") - pd.to_numeric(merged[m_right], errors="coerce")
        return merged

    source_sets = summarize(source_raw)
    baseline_sets = summarize(baseline_raw)
    with pd.ExcelWriter(compare_path, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {"report_month": pd.Timestamp(report_month), "source": source_name, "baseline": baseline_name},
            ]
        ).to_excel(writer, sheet_name="Summary", index=False)
        for sheet_name, src_df in source_sets.items():
            out = compare_df(sheet_name, src_df, baseline_sets[sheet_name])
            out.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def main() -> None:
    args = parse_args()
    report_month = legacy.resolve_report_month(args.month)
    month_text = legacy.month_label(report_month)
    output_name = args.output or f"{month_text} Forecast Accuracy Report.xlsx"
    output_path = ROOT / output_name

    sql_cfg = SqlConfig(server=args.server, database=args.database, driver=args.driver)
    source_frames = load_frames(
        source=args.data_source,
        report_month=report_month,
        marketing_copy_file=Path(args.marketing_copy_file),
        marketing_copy_sheet=args.marketing_copy_sheet,
        start_row=args.start_row,
        sql_cfg=sql_cfg,
    )
    dq_results = run_dq_checks(report_month, args.data_source, *source_frames)
    dq_log_name = args.dq_log or f"outputs/comparisons/{month_text} Forecast Accuracy DQ ({args.data_source}).json"
    dq_log_path = ROOT / dq_log_name
    dq_log_path.parent.mkdir(parents=True, exist_ok=True)
    dq_log_path.write_text(json.dumps(dq_results, indent=2, default=str), encoding="utf-8")
    print(f"DQ written: {dq_log_path}")
    if args.dq_mode != "off":
        for check in dq_results["checks"]:
            if not check["passed"]:
                level = "ERROR" if check["severity"] == "critical" else "WARN"
                print(f"[{level}] {check['name']}: {check['details']}")
    if args.dq_mode == "fail" and dq_results["critical_failed"] > 0:
        raise SystemExit("Critical DQ checks failed. Report generation aborted.")

    build_report_from_frames(report_month, output_path, *source_frames)
    print(f"Report written: {output_path}")

    if args.compare_baseline != "none" and args.compare_baseline != args.data_source:
        baseline_frames = load_frames(
            source=args.compare_baseline,
            report_month=report_month,
            marketing_copy_file=Path(args.marketing_copy_file),
            marketing_copy_sheet=args.marketing_copy_sheet,
            start_row=args.start_row,
            sql_cfg=sql_cfg,
        )
        source_raw, *_ = legacy.build_raw_data(*source_frames, report_month)
        baseline_raw, *_ = legacy.build_raw_data(*baseline_frames, report_month)
        compare_path = output_path.parent / f"{month_text} Forecast Accuracy Comparison ({args.data_source} vs {args.compare_baseline}).xlsx"
        build_comparison_workbook(report_month, compare_path, args.data_source, args.compare_baseline, source_raw, baseline_raw)
        print(f"Comparison written: {compare_path}")


if __name__ == "__main__":
    main()
