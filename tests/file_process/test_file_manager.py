# import os
# import tempfile
# import pytest
# from app.file_process.file_manager import FileManager
#
#
# class TestFileManager:
#     def setup_method(self):
#         self.test_file = tempfile.NamedTemporaryFile(delete=False)
#         self.test_file_path = self.test_file.name
#         self.test_file.close()
#
#     def teardown_method(self):
#         if os.path.exists(self.test_file_path):
#             os.remove(self.test_file_path)
#
#
#     def test_get_last_processed_file_existing_file(self):
#         content = "test_file.csv"
#         with open(self.test_file_path, "w") as f:
#             f.write(content)
#         result = FileManager.get_last_processed_file(self.test_file_path)
#         assert result == content
#
#     def test_get_last_processed_file_no_file(self):
#         os.remove(self.test_file_path)
#         with pytest.raises(RuntimeError):
#             FileManager.get_last_processed_file(self.test_file_path)
#
#     def test_set_last_processed_file_valid(self):
#         filename = "processed_data.csv"
#         FileManager.set_last_processed_file(filename, self.test_file_path)
#         with open(self.test_file_path, "r") as f:
#             saved_content = f.read().strip()
#         assert saved_content == filename
#
#
#     def test_set_last_processed_file_with_newline_character(self):
#         with pytest.raises(ValueError, match="Invalid filename format"):
#             FileManager.set_last_processed_file("bad_name\n.csv", self.test_file_path)
#
#     def test_set_last_processed_file_invalid_format(self):
#         with pytest.raises(ValueError, match="Invalid filename format"):
#             FileManager.set_last_processed_file("bad name.txt", self.test_file_path)
#
#     def test_set_last_processed_file_wrong_extension(self):
#         with pytest.raises(ValueError, match="Invalid filename format"):
#             FileManager.set_last_processed_file("file.json", self.test_file_path)
#
#     def test_set_last_processed_file_custom_pattern_json(self):
#         pattern = r"^[\w\-]+\.json$"
#         valid = "data_ok.json"
#         FileManager.set_last_processed_file(valid, self.test_file_path, filename_pattern=pattern)
#         with open(self.test_file_path, "r") as f:
#             assert f.read().strip() == valid
#
#         invalid = "data.bad.csv"
#         with pytest.raises(ValueError):
#             FileManager.set_last_processed_file(invalid, self.test_file_path, filename_pattern=pattern)
#
#
#     def test_get_last_processed_file_directory_path(self):
#         dir_path = tempfile.mkdtemp()
#         with pytest.raises((ValueError, RuntimeError)) as exc_info:
#             FileManager.get_last_processed_file(dir_path)
#         assert "does not point to a valid file" in str(exc_info.value) or "Permission denied" in str(exc_info.value)
#         os.rmdir(dir_path)
#
#     def test_get_last_processed_file_nonexistent_file(self):
#         nonexistent_path = os.path.join(tempfile.gettempdir(), "definitely_nonexistent_file.txt")
#         if os.path.exists(nonexistent_path):
#             os.remove(nonexistent_path)
#         with pytest.raises(RuntimeError) as exc_info:
#             FileManager.get_last_processed_file(nonexistent_path)
#         assert "Error reading file" in str(exc_info.value)
#
#     def test_set_last_processed_file_invalid_path(self):
#         invalid_path = "/invalid_directory/test_file.csv"
#
#         with pytest.raises(OSError) as exc_info:
#             FileManager.set_last_processed_file("test_file.csv", invalid_path)
#
#         assert exc_info.type is OSError
#         assert "Error writing file" in str(exc_info.value)
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
