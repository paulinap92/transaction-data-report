import os
import re
import logging

logger = logging.getLogger(__name__)


class FileManager:
    """A class for storing and retrieving a filename in a specified file path."""

    @staticmethod
    def get_last_processed_file(file_path: str) -> str | None:
        """Reads and returns the content (typically a filename) from the specified file path.

        Args:
            file_path (str): Path to the file that stores the filename.

        Returns:
            str | None: The stored filename (stripped of whitespace), or raises an exception on error.

        Raises:
            RuntimeError: If there is a problem reading the file (e.g., it doesn't exist or is inaccessible).
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"⚠️ Last processed file record not found at: {file_path}")
                return None

            with open(file_path, "r") as f:
                filename = f.read().strip()

            if filename:
                logger.info(f"📖 Loaded last processed file: {filename}")
            else:
                logger.info(f"🕳️ File {file_path} exists but is empty.")
            return filename

        except (OSError, IOError) as e:
            logger.error(f"❌ Error reading file {file_path}: {e}")
            raise RuntimeError(f"Error reading file {file_path}: {e}") from e

    @staticmethod
    def set_last_processed_file(filename: str, file_path: str, filename_pattern: str = r"^[\w\-.]+\.csv$") -> None:
        """Writes the provided filename to the specified file path.

        Validates that the filename matches the expected pattern (default: .csv filenames).

        Args:
            filename (str): The name to store (e.g., the name of a processed file).
            file_path (str): The path of the file where the name should be saved.
            filename_pattern (str): Optional regex pattern to validate the filename (default: CSV format).

        Raises:
            ValueError: If the filename format is invalid.
            OSError: If there is a problem writing to the file.
        """
        if not re.fullmatch(filename_pattern, filename):
            logger.error(f"🚫 Invalid filename format: {filename}. Must match pattern: {filename_pattern}")
            raise ValueError(f"Invalid filename format: {filename}. Must match pattern: {filename_pattern}")

        try:
            dir_path = os.path.dirname(file_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                logger.info(f"📂 Created directory for record file: {dir_path}")

            with open(file_path, "w") as f:
                f.write(filename)

            logger.info(f"📝 Updated last processed file: {filename} → {file_path}")

        except OSError as e:
            logger.exception(f"❌ Error writing to file {file_path}: {e}")
            raise OSError(f"Error writing file {file_path}: {e}") from e
