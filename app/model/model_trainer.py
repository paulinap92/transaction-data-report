# app/file_process/model_trainer.py
# ============================================================
# 🧠 Model Trainer — Isolation Forest retraining for Sales Anomaly Detection
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import IsolationForest
import joblib
import logging
from datetime import datetime, timedelta
from app.file_process.report_generator import DataProcessor, CATEGORY_MAPPING

logger = logging.getLogger(__name__)

class ModelTrainer:
    """Handles retraining of Isolation Forest models for each product category.

    Loads recent sales data (last 30 days) from CSV files, applies category mapping,
    trains a separate IsolationForest model per mapped product category, and saves
    them to `app/models/category_anomaly_models.pkl`.
    """

    def __init__(self, data_dir: str = "app/data/Sabores Ibéricos Company Transaction Data"):
        self.data_dir = Path(data_dir)
        self.pattern = "sales_*.csv"
        self.models = {}
        self.df = None

    # ============================================================
    # 1️⃣ Load last 30 days of sales data
    # ============================================================
    def load_data(self) -> pd.DataFrame:
        """Loads only CSV files from the last 30 days into a single DataFrame."""
        csv_paths = sorted(self.data_dir.glob(self.pattern))
        if not csv_paths:
            raise FileNotFoundError(f"No CSV files matching {self.pattern} in {self.data_dir}")

        # Extract dates from file names like 'sales_2025-03-01.csv'
        date_files = []
        for path in csv_paths:
            try:
                file_date = datetime.strptime(path.stem.split("_")[1], "%Y-%m-%d").date()
                date_files.append((file_date, path))
            except Exception:
                logger.warning(f"⚠️ Skipping file with invalid date format: {path.name}")

        if not date_files:
            raise ValueError("No valid dated files found in directory.")

        # Sort by date and determine cutoff
        date_files.sort(key=lambda x: x[0])
        latest_date = date_files[-1][0]
        cutoff = latest_date - timedelta(days=30)

        # Select only last 30 days
        recent_files = [p for d, p in date_files if d >= cutoff]
        logger.info(f"📅 Found {len(recent_files)} files from last 30 days "
                    f"({cutoff} → {latest_date}).")

        # Load CSVs
        df_list = []
        for path in recent_files:
            temp_df = pd.read_csv(path)
            temp_df["source_file"] = path.name
            df_list.append(temp_df)

        df = pd.concat(df_list, ignore_index=True)
        df = DataProcessor.map_categories(df)
        logger.info(f"✅ Loaded {len(df)} rows from {len(recent_files)} files.")
        self.df = df
        return df

    # ============================================================
    # 2️⃣ Train IsolationForest models per category
    # ============================================================
    def retrain_models(self) -> None:
        """Retrains Isolation Forest models for each product category."""
        if self.df is None:
            self.load_data()

        df = self.df
        df["is_anomaly"] = 0
        models = {}

        for cat, group in df.groupby("Product Category Mapped"):
            if len(group) < 5:
                logger.warning(f"⚠️ Skipping category '{cat}' — not enough samples ({len(group)} rows).")
                continue

            model = IsolationForest(contamination=0.05, random_state=42)
            preds = model.fit_predict(group[["Sales"]])
            df.loc[group.index, "is_anomaly"] = (preds == -1).astype(int)
            models[cat] = model

            logger.info(f"✅ {cat:25s} — {(preds == -1).sum()} anomalies "
                        f"({100*(preds == -1).mean():.2f}%) detected.")

        self.models = models
        self.df = df
        logger.info(f"📊 Global anomalies: {df['is_anomaly'].sum()} "
                    f"({100*df['is_anomaly'].mean():.2f}%) detected.")

    # ============================================================
    # 3️⃣ Save results
    # ============================================================
    def save_models(self, output_path: str = "app/models/category_anomaly_models.pkl") -> None:
        """Saves trained models to disk."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.models, path)
        logger.info(f"💾 Saved {len(self.models)} category models to {path}")

    def save_annotated_data(self, output_path: str = "app/data/sales_with_category_anomalies.csv") -> None:
        """Saves DataFrame with anomaly annotations."""
        if self.df is None:
            raise ValueError("No data to save — please call retrain_models() first.")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(output_path, index=False)
        logger.info(f"💾 Annotated sales data saved to {output_path}")

    # ============================================================
    # 4️⃣ Full retraining pipeline
    # ============================================================
    def retrain_all_categories(self) -> None:
        """Full pipeline: load → train → save models and annotated data."""
        logger.info("🔁 Starting model retraining pipeline...")
        self.load_data()
        self.retrain_models()
        self.save_models()
        self.save_annotated_data()
        logger.info("✅ Model retraining completed successfully.")


# ============================================================
# Optional: manual run for debugging
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    trainer = ModelTrainer()
    trainer.retrain_all_categories()
