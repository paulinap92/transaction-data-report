import os
import pandas as pd
import json
from dataclasses import dataclass, field
from typing import Self, Callable
import joblib
from pathlib import Path
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

CATEGORY_MAPPING = {
    "AA": "Carbonated Drink",
    "AB": "Plant-Based Drink",
    "AC": "Milkshake",
    "AD": "Fruit Juice",
    "AE": "Diet Drink",
    "AF": "Functional Beverage"
}

REPORT_FILE = "sales_report.json"


class DataProcessor:
    """🧩 Handles preprocessing, filtering and category mapping of sales data."""

    @staticmethod
    def filter_data(df: pd.DataFrame, filter_func: Callable[[pd.DataFrame], pd.DataFrame] | None) -> pd.DataFrame:
        """Applies an optional filtering function.

        Args:
            df (pd.DataFrame): Input data.
            filter_func (Callable[[pd.DataFrame], pd.DataFrame] | None): Function to filter rows.

        Returns:
            pd.DataFrame: Filtered data.
        """
        if filter_func:
            logger.info("🧹 Applying custom data filter...")
            return filter_func(df)
        logger.info("ℹ️ No filter function provided, returning original dataset.")
        return df

    @staticmethod
    def map_categories(df: pd.DataFrame) -> pd.DataFrame:
        """Maps product categories (AA, AB...) to descriptive names."""
        df["Product Category Mapped"] = df["Product Category"].map(CATEGORY_MAPPING)
        logger.info("🔤 Product categories mapped to descriptive names.")
        return df


@dataclass
class ReportGenerator:
    """📊 Generates, updates, and saves structured sales reports including anomaly detection."""

    report_dataframe: pd.DataFrame
    report_dict: dict[str, pd.DataFrame] = field(default_factory=dict)

    # ================================================================
    # INITIAL DATAFRAME CREATION
    # ================================================================
    @classmethod
    def create_first_dataframe(cls, directory_path: str, file_names: list) -> Self:
        """Creates the initial DataFrame and immediately runs anomaly detection."""
        data_frames = []
        for file_name in file_names:
            file_path = os.path.join(directory_path, file_name)
            logger.info(f"📥 Loading initial file: {file_name}")
            df = pd.read_csv(file_path)
            df = DataProcessor.map_categories(df)
            data_frames.append(df)

        report_dataframe = pd.concat(data_frames, ignore_index=True)
        logger.info(f"✅ Created initial report DataFrame with shape: {report_dataframe.shape}")

        instance = ReportGenerator(report_dataframe)

        try:
            logger.info("🧠 Running initial anomaly detection...")
            instance.generate_report(run_anomaly_detection=True)
            logger.info("✅ Initial anomaly detection completed successfully.")
        except Exception as e:
            logger.exception(f"❌ Error during initial anomaly detection: {e}")

        return instance

    # ================================================================
    # DATA UPDATE
    # ================================================================
    def update_dataframe(self, directory_path: str, file_names: list) -> None:
        """Updates the DataFrame with new files and recalculates anomalies."""
        data_frames = []
        for file_name in file_names:
            file_path = os.path.join(directory_path, file_name)
            logger.info(f"📥 Loading update file: {file_name}")
            df = pd.read_csv(file_path)
            df = DataProcessor.map_categories(df)
            data_frames.append(df)

        if not data_frames:
            logger.warning("⚠️ No new data files found for update.")
            return

        new_data = pd.concat(data_frames, ignore_index=True)
        self.report_dataframe = pd.concat([self.report_dataframe, new_data], ignore_index=True)
        logger.info(f"📈 Updated report DataFrame shape: {self.report_dataframe.shape}")

        try:
            logger.info("🧠 Recalculating anomalies after update...")
            self.generate_report(run_anomaly_detection=True)
            logger.info("✅ Anomaly recalculation after update completed successfully.")
        except Exception as e:
            logger.exception(f"❌ Error during anomaly recalculation after update: {e}")

    # ================================================================
    # ANOMALY DETECTION
    # ================================================================
    def _detect_anomalies(self, model_path: str | Path | None = None) -> None:
        """Loads pretrained IsolationForest models and flags anomalies."""
        df = self.report_dataframe.copy()

        if model_path is None: #pragma: no cover
            model_path = Path(__file__).resolve().parent.parent / "app" / "models" / "category_anomaly_models.pkl"

        path = Path(model_path).resolve()
        logger.info(f"📂 Checking model path: {path}")

        if not path.exists():
            logger.error(f"❌ Model file not found at: {path}")
            df["is_anomaly"] = 0
            self.report_dataframe = df
            return

        try:
            models = joblib.load(path)
            if not isinstance(models, dict) or len(models) == 0:
                logger.warning(f"⚠️ Loaded model file is empty or invalid: {path}")
                df["is_anomaly"] = 0
                self.report_dataframe = df
                return
            logger.info(f"✅ Loaded {len(models)} category models: {list(models.keys())}")
        except Exception as e:
            logger.exception(f"❌ Error loading model from {path}: {e}")
            df["is_anomaly"] = 0
            self.report_dataframe = df
            return

        if "is_anomaly" not in df.columns:
            df["is_anomaly"] = 0

        if "Product Category Mapped" not in df.columns:
            logger.warning("⚠️ Missing column 'Product Category Mapped' — mapping required before detection.")
            self.report_dataframe = df
            return

        total_anomalies = 0
        for category, idx in df.groupby("Product Category Mapped").groups.items():
            model = models.get(category)
            if model is None:
                logger.warning(f"⚠️ No model found for category '{category}'")
                continue
            try:
                preds = model.predict(df.loc[idx, ["Sales"]])
                anomalies = int((preds == -1).sum())
                total_anomalies += anomalies
                logger.info(f"🧠 {category:25s} — {anomalies} anomalies detected.")
                df.loc[idx, "is_anomaly"] = (preds == -1).astype(int)
            except Exception as e:
                logger.exception(f"❌ Error predicting anomalies for {category}: {e}")

        logger.info(f"📊 Total anomalies detected: {total_anomalies}")
        self.report_dataframe = df

    # ================================================================
    # REPORT GENERATION
    # ================================================================
    def generate_report(self, *, model_path: str = "app/models/category_anomaly_models.pkl",
                        run_anomaly_detection: bool = True) -> dict[str, pd.DataFrame]:
        """Generates aggregated reports and identifies top anomalies for last 30 days.

        Args:
            model_path (str): Path to pretrained models.
            run_anomaly_detection (bool): Whether to perform anomaly detection before reporting.

        Returns:
            dict[str, pd.DataFrame]: Aggregated reports with anomaly summary.
        """
        if run_anomaly_detection:
            self._detect_anomalies(model_path=model_path)

        df = self.report_dataframe.copy()
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # --- 🔄 Filter last 30 days
        today = datetime.now().date()
        cutoff = today - timedelta(days=30)

        # Upewniamy się, że Date jest datą (bez Timestamp)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        df_30 = df[df["Date"] >= cutoff]
        logger.info(f"📅 Filtered last 30 days: {df_30.shape[0]} records remaining.")
        # --- 📊 Aggregate reports
        self.report_dict = {
            "region_report_mean": df_30.groupby(["Region"])["Sales"].mean().round(2).to_dict(),
            "beverage_report_total": df_30.groupby(["Product Category Mapped"])["Sales"].sum().astype(int).to_dict(),
            "beverage_report_mean": df_30.groupby(["Product Category Mapped"])["Sales"].mean().round(2).to_dict(),
        }

        # --- 🧠 Anomalies: only today's anomalies detected by the model
        today_anoms = df_30[(df_30["is_anomaly"] == 1) & (df_30["Date"] == today)]

        if not today_anoms.empty:
            total_anomalies = len(today_anoms)

            logger.info(f"📈 Today's anomalies detected by model: {total_anomalies}")
            logger.info(
                f"🧾 Full list of anomalies:\n{today_anoms[['Date', 'Region', 'Product', 'Sales']].to_string(index=False)}")

            self.report_dict["today_anomalies"] = {
                "total_anomalies": total_anomalies,
                "records": today_anoms.to_dict(orient="records"),
            }
        else:
            logger.info("✅ No anomalies detected today.")
            self.report_dict["today_anomalies"] = {
                "total_anomalies": 0,
                "records": [],
            }

        logger.info("✅ Report generation completed successfully.")
        return self.report_dict

    # ================================================================
    # REPORT SAVING
    # ================================================================
    def save_report(self, file_name: str = REPORT_FILE) -> None:
        """Saves the generated report as JSON."""
        logger.info(f"💾 PRINT DICT: {self.report_dict}")
        json_object = json.dumps(self.report_dict, indent=4, default=str)
        with open(file_name, "w") as outfile:
            outfile.write(json_object)
        logger.info(f"💾 Report successfully saved to: {file_name}")

