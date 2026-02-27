from pathlib import Path

from build_forecast_accuracy_report_db import parse_marketing_copy_2026, validate_marketing_copy_2026


ROOT = Path(r"c:\Users\cguyer\OneDrive - Midmark Corporation\Documents\Sales Ops\Reporting\Ad Hoc Reports\Forecast Reports")
SOURCE = ROOT / "Marketing Forecast Data (copy).xlsx"


def test_parse_2026_data_from_a6() -> None:
    df = parse_marketing_copy_2026(SOURCE, sheet="2026 Data", start_row=6)
    assert not df.empty
    assert "Date" in df.columns
    assert "Forecast (Dollars)" in df.columns
    assert "Forecast (Quantity)" in df.columns


def test_validation_requires_12_months() -> None:
    df = parse_marketing_copy_2026(SOURCE, sheet="2026 Data", start_row=6)
    summary = validate_marketing_copy_2026(df)
    assert summary["months"] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
