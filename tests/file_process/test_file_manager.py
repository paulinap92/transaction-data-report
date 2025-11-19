import os
import tempfile
import pytest
from app.file_process.file_manager import FileManager   # adjust import if needed
import logging

@pytest.fixture
def temp_dir():
    """Creates a temporary working directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# ---------- GET TESTS ----------

def test_get_last_processed_file_returns_filename(temp_dir):
    """Should return the stored filename when the file exists and contains text."""
    file_path = os.path.join(temp_dir, "record.txt")
    with open(file_path, "w") as f:
        f.write("data_2025_11.csv")

    result = FileManager.get_last_processed_file(file_path)
    assert result == "data_2025_11.csv"


def test_get_last_processed_file_empty_file(temp_dir, caplog):
    """Should return an empty string and log info when the file exists but is empty."""
    file_path = os.path.join(temp_dir, "record.txt")
    open(file_path, "w").close()  # create empty file
    caplog.set_level(logging.INFO)
    result = FileManager.get_last_processed_file(file_path)
    assert result == ""
    assert "exists but is empty" in caplog.text


def test_get_last_processed_file_file_not_found(temp_dir, caplog):
    """Should return None when the record file does not exist."""
    file_path = os.path.join(temp_dir, "missing.txt")

    result = FileManager.get_last_processed_file(file_path)
    assert result is None
    assert "not found" in caplog.text


def test_get_last_processed_file_oserror(monkeypatch):
    monkeypatch.setattr("os.path.exists", lambda _: True)

    def mock_open(*args, **kwargs):
        raise OSError("Test error")

    monkeypatch.setattr("builtins.open", mock_open)

    with pytest.raises(RuntimeError):
        FileManager.get_last_processed_file("fake_path.txt")



# ---------- SET TESTS ----------

def test_set_last_processed_file_creates_file(temp_dir):
    """Should successfully write a valid CSV filename into the record file."""
    file_path = os.path.join(temp_dir, "record.txt")
    FileManager.set_last_processed_file("data.csv", file_path)

    with open(file_path) as f:
        content = f.read()

    assert content == "data.csv"


def test_set_last_processed_file_invalid_filename(temp_dir):
    """Should raise ValueError when filename does not match the expected CSV pattern."""
    file_path = os.path.join(temp_dir, "record.txt")

    with pytest.raises(ValueError, match="Invalid filename format"):
        FileManager.set_last_processed_file("invalid.txt", file_path)


def test_set_last_processed_file_creates_missing_dir(temp_dir):
    """Should automatically create the directory if it doesn't exist."""
    sub_dir = os.path.join(temp_dir, "nested")
    file_path = os.path.join(sub_dir, "record.txt")

    FileManager.set_last_processed_file("data.csv", file_path)

    assert os.path.exists(sub_dir)
    assert os.path.isfile(file_path)


def test_set_last_processed_file_oserror(monkeypatch, temp_dir):
    """Should raise OSError when writing to the file fails."""
    def mock_open(*args, **kwargs):
        raise OSError("Test write error")

    monkeypatch.setattr("builtins.open", mock_open)

    file_path = os.path.join(temp_dir, "record.txt")

    with pytest.raises(OSError, match="Error writing file"):
        FileManager.set_last_processed_file("data.csv", file_path)
