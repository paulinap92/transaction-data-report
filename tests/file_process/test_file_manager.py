import os
import tempfile
import pytest
from app.file_process.file_manager import FileManager


class TestFileManager:
    def setup_method(self):
        self.test_file = tempfile.NamedTemporaryFile(delete=False)
        self.test_file_path = self.test_file.name
        self.test_file.close()

    def teardown_method(self):
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)


    def test_get_last_processed_file_existing_file(self):
        content = "test_file.csv"
        with open(self.test_file_path, "w") as f:
            f.write(content)
        result = FileManager.get_last_processed_file(self.test_file_path)
        assert result == content

    def test_get_last_processed_file_no_file(self):
        os.remove(self.test_file_path)
        with pytest.raises(RuntimeError):
            FileManager.get_last_processed_file(self.test_file_path)

    def test_set_last_processed_file_valid(self):
        filename = "processed_data.csv"
        FileManager.set_last_processed_file(filename, self.test_file_path)
        with open(self.test_file_path, "r") as f:
            saved_content = f.read().strip()
        assert saved_content == filename


    def test_set_last_processed_file_with_newline_character(self):
        with pytest.raises(ValueError, match="Invalid filename format"):
            FileManager.set_last_processed_file("bad_name\n.csv", self.test_file_path)

    def test_set_last_processed_file_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid filename format"):
            FileManager.set_last_processed_file("bad name.txt", self.test_file_path)

    def test_set_last_processed_file_wrong_extension(self):
        with pytest.raises(ValueError, match="Invalid filename format"):
            FileManager.set_last_processed_file("file.json", self.test_file_path)

    def test_set_last_processed_file_custom_pattern_json(self):
        pattern = r"^[\w\-]+\.json$"
        valid = "data_ok.json"
        FileManager.set_last_processed_file(valid, self.test_file_path, filename_pattern=pattern)
        with open(self.test_file_path, "r") as f:
            assert f.read().strip() == valid

        invalid = "data.bad.csv"
        with pytest.raises(ValueError):
            FileManager.set_last_processed_file(invalid, self.test_file_path, filename_pattern=pattern)


    def test_get_last_processed_file_directory_path(self):
        dir_path = tempfile.mkdtemp()
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            FileManager.get_last_processed_file(dir_path)
        assert "does not point to a valid file" in str(exc_info.value) or "Permission denied" in str(exc_info.value)
        os.rmdir(dir_path)

    def test_get_last_processed_file_nonexistent_file(self):
        nonexistent_path = os.path.join(tempfile.gettempdir(), "definitely_nonexistent_file.txt")
        if os.path.exists(nonexistent_path):
            os.remove(nonexistent_path)
        with pytest.raises(RuntimeError) as exc_info:
            FileManager.get_last_processed_file(nonexistent_path)
        assert "Error reading file" in str(exc_info.value)

    def test_set_last_processed_file_invalid_path(self):
        invalid_path = "/invalid_directory/test_file.csv"

        with pytest.raises(OSError) as exc_info:
            FileManager.set_last_processed_file("test_file.csv", invalid_path)

        assert exc_info.type is OSError
        assert "Error writing file" in str(exc_info.value)
