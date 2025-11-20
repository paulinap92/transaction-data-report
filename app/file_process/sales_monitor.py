from app.file_process.file_manager import FileManager
from app.file_process.report_generator import ReportGenerator
from dataclasses import dataclass, field
import re
import shutil
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

LAST_FILE_RECORD = "data/Sabores Ibéricos Company Transaction Data/last_processed.txt"


@dataclass
class SalesMonitor:
    """Monitors the sales data directory, processes new files, and generates sales reports.

    This class is responsible for:
    - Monitoring a specific directory for new CSV files.
    - Processing new files by filtering and mapping categories.
    - Generating aggregated sales reports.
    - Saving and sending the generated reports.
    """

    directory: str
    record_file_path: str
    all_files: dict = field(init=False, default_factory=dict)
    report_generator: ReportGenerator = None

    def __post_init__(self):
        """Initializes the SalesMonitor instance by loading all files and setting up the report generator."""
        logger.info("🚀 Initializing SalesMonitor...")
        try:
            self.fill()
            logger.info(f"📁 All files found: {len(self.all_files)}")
            last_file = FileManager.get_last_processed_file(self.record_file_path)
            logger.info(f"🕐 Last processed file: {last_file}")
        except Exception as e:
            logger.exception(f"❌ Error during SalesMonitor initialization: {e}")

    @staticmethod
    def extract_date_from_filename(filename: str) -> datetime | None:
        """Extracts the date from a filename in the format 'YYYY-MM-DD'."""
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if match:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return None

    def _get_files_since(self, date: datetime | None) -> dict:
        """Fetches files in the directory that are newer than the provided date."""
        try:
            files = {
                f: self.extract_date_from_filename(f)
                for f in os.listdir(self.directory)
                if f.endswith(".csv") and self.extract_date_from_filename(f) is not None
            }
            files = dict(sorted(files.items(), key=lambda item: item[1]))

            if date:
                files = {f: d for f, d in files.items() if d > date}

            logger.info(f"📂 Found {len(files)} CSV files in directory: {self.directory}")
            return files
        except FileNotFoundError:
            logger.warning(f"⚠️ Directory not found: {self.directory}")
            return {}
        except Exception as e:
            logger.exception(f"❌ Error reading directory {self.directory}: {e}")
            return {}

    def process_new_files(self):
        """Processes any new files that were added to the directory after the last processed file."""
        logger.info("🔍 Checking for new files to process...")

        try:
            last_processed_file = FileManager.get_last_processed_file(self.record_file_path)
            logger.info(f"🕐 Last processed file: {last_processed_file}")

            last_date = self.extract_date_from_filename(last_processed_file) if last_processed_file else None
            new_files = self._get_files_since(last_date)

            if new_files:
                logger.info(f"🆕 Found new files: {list(new_files.keys())}")
                self.all_files.update(new_files)

                logger.info("🧾 Archiving previous report before generating a new one...")
                self._archive_old_report()

                logger.info("📊 Updating DataFrame with new data...")
                self.report_generator.update_dataframe(self.directory, list(new_files.keys()))

                logger.info("🧮 Generating new sales report...")
                self.report_generator.generate_report()

                logger.info("💾 Saving updated report...")
                self.report_generator.save_report()

                last_processed_file = list(new_files.keys())[-1]
                FileManager.set_last_processed_file(last_processed_file, self.record_file_path)
                logger.info(f"✅ Updated last processed file: {last_processed_file}")
                logger.info("🏁 Report generation and update completed successfully.")
            else:
                logger.info("ℹ️ No new files detected. Everything is up to date.")

        except Exception as e:
            logger.exception(f"❌ Error while processing new files: {e}")

    def fill(self):
        """Initializes the monitor by loading all files and generating the first report."""
        logger.info("📂 Loading all CSV files for initialization...")
        try:
            self.all_files = self._get_files_since(None)

            if self.all_files:
                last_processed_file = list(self.all_files.keys())[-1]
                FileManager.set_last_processed_file(last_processed_file, self.record_file_path)
                logger.info(f"🕒 Set last processed file: {last_processed_file}")

                logger.info("📈 Creating initial DataFrame and generating first report...")
                self.report_generator = ReportGenerator.create_first_dataframe(self.directory, list(self.all_files.keys()))
                self.report_generator.generate_report()
                self.report_generator.save_report()
                logger.info("🎉 Initial report generated successfully.")
            else:
                logger.warning("⚠️ No CSV files found in the directory during initialization.")
        except Exception as e:
            logger.exception(f"❌ Error during initial report generation: {e}")

    def _archive_old_report(self):
        """Creates a timestamped copy of the current report before it's overwritten."""
        try:
            report_json = "sales_report.json"
            report_csv = "sales_report.csv"
            archive_dir = os.path.join(self.directory, "report_archive")
            os.makedirs(archive_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            if os.path.exists(report_json):
                dst_json = os.path.join(archive_dir, f"sales_report_{timestamp}.json")
                shutil.copy2(report_json, dst_json)
                logger.info(f"🧾 Archived previous JSON report → {dst_json}")

            if os.path.exists(report_csv):
                dst_csv = os.path.join(archive_dir, f"sales_report_{timestamp}.csv")
                shutil.copy2(report_csv, dst_csv)
                logger.info(f"📊 Archived previous CSV report → {dst_csv}")

        except Exception as e:
            logger.warning(f"⚠️ Could not archive previous report: {e}")
