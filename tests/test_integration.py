import json
import shutil
import pandas as pd
from pathlib import Path

from app.file_process.sales_monitor import SalesMonitor


def test_full_integration_flow(tmp_path, caplog):
    caplog.set_level("INFO")

    # -----------------------------------------------------
    # 1) CREATE FAKE DIRECTORY WITH INITIAL CSV FILES
    # -----------------------------------------------------
    directory = tmp_path / "sales"
    directory.mkdir()

    file1 = directory / "2025-01-01-sales.csv"
    file2 = directory / "2025-01-02-sales.csv"

    df1 = pd.DataFrame({
        "Date": ["2025-01-01"],
        "Region": ["North"],
        "Product Category": ["AA"],
        "Product": ["Cola"],
        "Sales": [100],
    })
    df2 = pd.DataFrame({
        "Date": ["2025-01-02"],
        "Region": ["South"],
        "Product Category": ["AB"],
        "Product": ["Oat"],
        "Sales": [200],
    })

    df1.to_csv(file1, index=False)
    df2.to_csv(file2, index=False)

    # -----------------------------------------------------
    # 2) CREATE RECORD FILE PATH (initially empty)
    # -----------------------------------------------------
    record_file = tmp_path / "last.txt"

    # -----------------------------------------------------
    # 3) RUN FULL SalesMonitor INITIALIZATION
    # -----------------------------------------------------
    monitor = SalesMonitor(
        directory=str(directory),
        record_file_path=str(record_file)
    )

    # After fill()
    assert monitor.report_generator is not None
    assert record_file.exists()

    # -----------------------------------------------------
    # 4) CHECK INITIAL REPORT EXISTS (IN PROJECT ROOT — NOT IN DIRECTORY)
    # -----------------------------------------------------
    report_path = Path.cwd() / "sales_report.json"
    assert report_path.exists()  # this is where your code writes

    # Also check report content
    with open(report_path) as f:
        report = json.load(f)

    assert "region_report_mean" in report
    assert "beverage_report_total" in report

    # -----------------------------------------------------
    # 5) ADD A NEW FILE
    # -----------------------------------------------------
    new_file = directory / "2025-01-03-sales.csv"
    pd.DataFrame({
        "Date": ["2025-01-03"],
        "Region": ["East"],
        "Product Category": ["AC"],
        "Product": ["Milkshake"],
        "Sales": [300],
    }).to_csv(new_file, index=False)

    # -----------------------------------------------------
    # 6) PROCESS NEW FILES
    # -----------------------------------------------------
    monitor.process_new_files()

    # archive folder inside sales/
    assert (directory / "report_archive").exists()

    # last_processed updated
    with open(record_file) as f:
        text = f.read()
        assert "2025-01-03" in text

    # -----------------------------------------------------
    # 7) CLEANUP — remove generated files so pytest leaves no trash
    # -----------------------------------------------------
    cleanup_files = [
        Path("sales_report.json"),
        Path("sales_report.csv"),
        Path("last.txt"),
    ]

    for fp in cleanup_files:
        try:
            if fp.exists():
                fp.unlink()
        except Exception:
            pass  # ignore filesystem race issues
