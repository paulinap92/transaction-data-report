import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

from app.file_process.sales_monitor import SalesMonitor


# ==========================================================
#  extract_date_from_filename
# ==========================================================
def test_extract_date_from_filename_valid():
    assert SalesMonitor.extract_date_from_filename("test_2024-12-31.csv") == datetime(2024, 12, 31)


def test_extract_date_from_filename_invalid():
    assert SalesMonitor.extract_date_from_filename("xxx.csv") is None


# ==========================================================
#  _get_files_since — directory missing → WARNING
# ==========================================================
def test_get_files_since_missing_directory(caplog):
    caplog.set_level("WARNING")

    monitor = SalesMonitor(directory="NO_SUCH_DIR", record_file_path="dummy.txt")
    result = monitor._get_files_since(None)

    assert result == {}
    assert "Directory not found" in caplog.text


# ==========================================================
#  _get_files_since — generic error → ERROR
# ==========================================================
def test_get_files_since_generic_error(caplog, tmp_path, monkeypatch):
    caplog.set_level("ERROR")

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path="dummy.txt")

    def broken_listdir(_):
        raise Exception("boom")

    monkeypatch.setattr("os.listdir", broken_listdir)

    result = monitor._get_files_since(None)

    assert result == {}
    assert "Error reading directory" in caplog.text


# ==========================================================
#  fill — no CSV files → WARNING
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
def test_fill_no_files(caplog, tmp_path):
    caplog.set_level("WARNING")

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))
    monitor.fill()

    assert "No CSV files found in the directory" in caplog.text


# ==========================================================
#  fill — successful initial run
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
@patch("app.file_process.sales_monitor.FileManager.set_last_processed_file")
@patch("app.file_process.sales_monitor.ReportGenerator")
def test_fill_success(mock_rg, mock_set, tmp_path, caplog):
    caplog.set_level("INFO")

    # create test CSV
    csv_file = tmp_path / "2025-01-01.csv"
    pd.DataFrame({"x": [1]}).to_csv(csv_file, index=False)

    mock_instance = MagicMock()
    mock_rg.create_first_dataframe.return_value = mock_instance

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))
    monitor.fill()

    mock_rg.create_first_dataframe.assert_called_once()
    mock_instance.generate_report.assert_called_once()
    mock_instance.save_report.assert_called_once()


# ==========================================================
#  fill — ReportGenerator throws → ERROR
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
@patch("app.file_process.sales_monitor.ReportGenerator.create_first_dataframe")
def test_fill_generator_exception(mock_create, tmp_path, caplog):
    caplog.set_level("ERROR")

    csv_file = tmp_path / "2025-01-01.csv"
    pd.DataFrame({"x": [1]}).to_csv(csv_file, index=False)

    mock_create.side_effect = Exception("fail")

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))
    monitor.fill()

    assert "Error during initial report generation" in caplog.text


# ==========================================================
#  process_new_files — no new files
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
@patch("app.file_process.sales_monitor.FileManager.get_last_processed_file", return_value="2025-01-01.csv")
def test_process_no_new_files(mock_last, tmp_path, caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({"x": [1]})
    df.to_csv(tmp_path / "2025-01-01.csv", index=False)

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))
    monitor.report_generator = MagicMock()

    monitor.process_new_files()

    assert "No new files detected" in caplog.text


# ==========================================================
#  process_new_files — found new files
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
@patch("app.file_process.sales_monitor.FileManager.get_last_processed_file", return_value="2025-01-01.csv")
@patch("app.file_process.sales_monitor.FileManager.set_last_processed_file")
def test_process_new_files_success(mock_set, mock_last, tmp_path, caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({"x": [1]})
    df.to_csv(tmp_path / "2025-01-01.csv", index=False)
    df.to_csv(tmp_path / "2025-01-10.csv", index=False)

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))
    mock_rg = MagicMock()
    monitor.report_generator = mock_rg

    monitor.process_new_files()

    assert "Found new files" in caplog.text
    mock_rg.update_dataframe.assert_called_once()
    mock_rg.generate_report.assert_called_once()
    mock_rg.save_report.assert_called_once()


# ==========================================================
#  process_new_files — update_dataframe raises → ERROR
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
@patch("app.file_process.sales_monitor.FileManager.get_last_processed_file", return_value="2025-01-01.csv")
def test_process_update_dataframe_exception(mock_last, tmp_path, caplog):
    caplog.set_level("ERROR")

    df = pd.DataFrame({"x": [1]})
    df.to_csv(tmp_path / "2025-01-01.csv", index=False)
    df.to_csv(tmp_path / "2025-01-10.csv", index=False)

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path=str(tmp_path / "rec.txt"))

    mock_rg = MagicMock()
    mock_rg.update_dataframe.side_effect = Exception("fail")
    monitor.report_generator = mock_rg

    monitor.process_new_files()

    assert "Error while processing new files" in caplog.text


# ==========================================================
#  _archive_old_report — success (with cwd fix)
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
def test_archive_old_report_success(tmp_path, caplog, monkeypatch):
    caplog.set_level("INFO")

    json_file = tmp_path / "sales_report.json"
    csv_file = tmp_path / "sales_report.csv"
    json_file.write_text("ok")
    csv_file.write_text("ok")

    # Ensure existence of expected working directory
    monkeypatch.chdir(tmp_path)

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path="rec.txt")
    monitor._archive_old_report()

    archive = tmp_path / "report_archive"
    assert archive.exists()
    assert any(f.suffix == ".json" for f in archive.iterdir())


# ==========================================================
#  _archive_old_report — copy fails → WARNING
# ==========================================================
@patch("app.file_process.sales_monitor.SalesMonitor.__post_init__", lambda x: None)
def test_archive_old_report_copy_exception(tmp_path, caplog, monkeypatch):
    caplog.set_level("WARNING")

    json_file = tmp_path / "sales_report.json"
    json_file.write_text("ok")

    monkeypatch.chdir(tmp_path)

    monitor = SalesMonitor(directory=str(tmp_path), record_file_path="rec.txt")

    with patch("shutil.copy2", side_effect=Exception("boom")):
        monitor._archive_old_report()

    assert "Could not archive previous report" in caplog.text
import pytest
from unittest.mock import patch
from app.file_process.sales_monitor import SalesMonitor

def test_sales_monitor_initialization_exception(caplog):
    """Covers: except Exception during SalesMonitor.__post_init__."""
    caplog.set_level("ERROR")

    # Force fill() to throw an exception
    with patch.object(SalesMonitor, "fill", side_effect=Exception("init failure")):
        monitor = SalesMonitor(directory="dummy", record_file_path="dummy.txt")

    # Assertions
    assert "Error during SalesMonitor initialization" in caplog.text
