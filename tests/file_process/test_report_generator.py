import os
import json
import pytest
import pandas as pd
import joblib
import numpy as np
from datetime import datetime, timedelta, date
from unittest.mock import patch

from app.file_process.report_generator import (
    ReportGenerator,
    DataProcessor,
    CATEGORY_MAPPING,
)

# ================================================================================
#  GLOBAL DUMMY MODELS (Picklable — MUST be defined at module level)
# ================================================================================

class DummySuccessModel:
    """
    Fully safe, picklable model that ALWAYS returns a numpy array.
    Ensures preds == -1 returns a vector, never a scalar.
    """

    def predict(self, X):
        values = X.values.reshape(-1)   # always 1D
        return np.array([-1 if float(v) > 500 else 1 for v in values], dtype=int)


class FakeModel:
    """Used for 'no model found' branch — also picklable."""
    def predict(self, X):
        return np.ones(len(X))


class RaisingModel:
    """Used for prediction-exception branch — must be picklable."""
    def predict(self, X):
        raise Exception("predict failed")


# ================================================================================
#  DataProcessor tests
# ================================================================================

def test_filter_data_applies_function(caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({"A": [1, 2, 3]})
    out = DataProcessor.filter_data(df, lambda d: d[d["A"] > 1])

    assert out["A"].tolist() == [2, 3]
    assert "Applying custom data filter" in caplog.text


def test_filter_data_no_function(caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({"A": [1, 2, 3]})
    out = DataProcessor.filter_data(df, None)

    assert out.equals(df)
    assert "No filter function provided" in caplog.text


def test_map_categories_creates_mapped_column(caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({"Product Category": ["AA", "AB"]})
    out = DataProcessor.map_categories(df)

    assert out["Product Category Mapped"].tolist() == [
        CATEGORY_MAPPING["AA"],
        CATEGORY_MAPPING["AB"],
    ]
    assert "Product categories mapped" in caplog.text


# ================================================================================
#  Fixtures for CSV files
# ================================================================================

@pytest.fixture
def csv_files(tmp_path):
    d = tmp_path
    f1 = d / "f1.csv"
    f2 = d / "f2.csv"

    pd.DataFrame({
        "Date": ["2025-01-01"],
        "Region": ["North"],
        "Product Category": ["AA"],
        "Product": ["Cola"],
        "Sales": [100],
    }).to_csv(f1, index=False)

    pd.DataFrame({
        "Date": ["2025-01-02"],
        "Region": ["South"],
        "Product Category": ["AB"],
        "Product": ["Oat"],
        "Sales": [200],
    }).to_csv(f2, index=False)

    return str(d), ["f1.csv", "f2.csv"]


# ================================================================================
#  create_first_dataframe (success + failing path)
# ================================================================================

def test_create_first_dataframe_success(csv_files, caplog):
    caplog.set_level("INFO")

    directory, files = csv_files
    rg = ReportGenerator.create_first_dataframe(directory, files)

    assert rg.report_dataframe.shape[0] == 2
    assert "Product Category Mapped" in rg.report_dataframe.columns
    assert "Created initial report DataFrame" in caplog.text


def test_create_first_dataframe_initial_anomaly_detection_exception(tmp_path, caplog):
    caplog.set_level("ERROR")

    csv_file = tmp_path / "init.csv"
    pd.DataFrame({
        "Date": ["2025-01-01"],
        "Region": ["N"],
        "Product Category": ["AA"],
        "Product": ["Cola"],
        "Sales": [100],
    }).to_csv(csv_file, index=False)

    with patch.object(ReportGenerator, "generate_report", side_effect=Exception("boom")):
        ReportGenerator.create_first_dataframe(str(tmp_path), ["init.csv"])

    assert "Error during initial anomaly detection" in caplog.text


# ================================================================================
#  update_dataframe tests
# ================================================================================

def test_update_dataframe_success(tmp_path, caplog):
    caplog.set_level("INFO")

    initial = pd.DataFrame({
        "Date": ["2025-01-01"],
        "Region": ["North"],
        "Product Category": ["AA"],
        "Product Category Mapped": ["Carbonated Drink"],
        "Product": ["Cola"],
        "Sales": [100],
        "is_anomaly": [0],
    })

    rg = ReportGenerator(initial)

    new_file = tmp_path / "new.csv"
    pd.DataFrame({
        "Date": ["2025-01-03"],
        "Region": ["South"],
        "Product Category": ["AB"],
        "Product": ["Oat"],
        "Sales": [200],
    }).to_csv(new_file, index=False)

    rg.update_dataframe(str(tmp_path), ["new.csv"])

    assert rg.report_dataframe.shape[0] == 2
    assert "Updated report DataFrame" in caplog.text


def test_update_dataframe_no_new_files(tmp_path, caplog):
    caplog.set_level("WARNING")

    rg = ReportGenerator(
        pd.DataFrame({"Date": ["2025-01-01"], "Sales": [10], "Product Category": ["AA"]})
    )

    rg.update_dataframe(str(tmp_path), [])

    assert "No new data files found for update" in caplog.text


def test_update_dataframe_anomaly_recalc_exception(tmp_path, caplog):
    caplog.set_level("ERROR")

    initial = pd.DataFrame({
        "Date": ["2025-01-01"],
        "Region": ["North"],
        "Product Category": ["AA"],
        "Product Category Mapped": ["Carbonated Drink"],
        "Product": ["Cola"],
        "Sales": [100],
    })

    rg = ReportGenerator(initial)

    new_file = tmp_path / "new.csv"
    pd.DataFrame({
        "Date": ["2025-01-02"],
        "Region": ["South"],
        "Product Category": ["AB"],
        "Product": ["Tea"],
        "Sales": [200],
    }).to_csv(new_file, index=False)

    with patch.object(ReportGenerator, "generate_report", side_effect=Exception("boom")):
        rg.update_dataframe(str(tmp_path), ["new.csv"])

    assert "Error during anomaly recalculation after update" in caplog.text


# ================================================================================
#  _detect_anomalies tests — all branches
# ================================================================================

def test_detect_anomalies_missing_model(tmp_path, caplog):
    caplog.set_level("ERROR")

    df = pd.DataFrame({
        "Product Category Mapped": ["Carbonated Drink"],
        "Sales": [100],
        "Date": ["2025-01-01"],
    })

    rg = ReportGenerator(df)
    rg._detect_anomalies(str(tmp_path / "nope.pkl"))

    assert "Model file not found" in caplog.text
    assert rg.report_dataframe["is_anomaly"].tolist() == [0]


def test_detect_anomalies_empty_model_file(tmp_path, caplog):
    caplog.set_level("WARNING")

    df = pd.DataFrame({
        "Product Category Mapped": ["Milkshake"],
        "Sales": [200],
        "Date": ["2025-01-01"],
    })

    rg = ReportGenerator(df)

    model_path = tmp_path / "empty.pkl"
    joblib.dump({}, model_path)

    rg._detect_anomalies(str(model_path))

    assert "invalid" in caplog.text.lower()
    assert rg.report_dataframe["is_anomaly"].tolist() == [0]


def test_detect_anomalies_missing_category_mapping(tmp_path, caplog):
    caplog.set_level("WARNING")

    df = pd.DataFrame({"Sales": [10], "Date": ["2025-01-01"]})
    rg = ReportGenerator(df)

    model_path = tmp_path / "dummy.pkl"
    joblib.dump({"AA": DummySuccessModel()}, model_path)

    rg._detect_anomalies(str(model_path))

    assert "Missing column 'Product Category Mapped'" in caplog.text


def test_detect_anomalies_prediction_exception(tmp_path, caplog):
    caplog.set_level("ERROR")

    df = pd.DataFrame({
        "Product Category Mapped": ["Fruit Juice"],
        "Sales": [50],
        "Date": ["2025-01-01"],
    })

    rg = ReportGenerator(df)

    model_path = tmp_path / "bad.pkl"
    joblib.dump({"Fruit Juice": RaisingModel()}, model_path)

    rg._detect_anomalies(str(model_path))

    assert "Error predicting anomalies" in caplog.text
    assert rg.report_dataframe["is_anomaly"].tolist() == [0]


def test_detect_anomalies_no_model_for_category(tmp_path, caplog):
    caplog.set_level("WARNING")

    df = pd.DataFrame({
        "Product Category Mapped": ["Carbonated Drink"],
        "Sales": [150],
        "Date": ["2025-01-01"],
    })
    rg = ReportGenerator(df)

    model_path = tmp_path / "model.pkl"
    joblib.dump({"SomeOtherCategory": FakeModel()}, model_path)

    rg._detect_anomalies(str(model_path))

    assert "No model found for category 'Carbonated Drink'" in caplog.text


def test_detect_anomalies_success(tmp_path, caplog):
    caplog.set_level("INFO")

    df = pd.DataFrame({
        "Product Category Mapped": ["Carbonated Drink", "Carbonated Drink"],
        "Sales": [10, 999],
        "Date": ["2025-01-01", "2025-01-01"],
    })

    rg = ReportGenerator(df)

    model_path = tmp_path / "good.pkl"
    joblib.dump({"Carbonated Drink": DummySuccessModel()}, model_path)

    rg._detect_anomalies(str(model_path))

    assert rg.report_dataframe["is_anomaly"].tolist() == [0, 1]
    assert "Total anomalies detected" in caplog.text


# ================================================================================
#  generate_report tests
# ================================================================================

def test_generate_report_no_anomalies(caplog):
    caplog.set_level("INFO")

    today = date.today()
    df = pd.DataFrame({
        "Date": [today],
        "Region": ["N"],
        "Product Category Mapped": ["Carbonated Drink"],
        "Sales": [123],
        "is_anomaly": [0],
    })

    rg = ReportGenerator(df)
    report = rg.generate_report(run_anomaly_detection=False)

    assert "No anomalies detected today" in caplog.text
    assert report["today_anomalies"]["total_anomalies"] == 0


def test_generate_report_detects_anomalies(caplog):
    caplog.set_level("INFO")

    today = date.today()
    df = pd.DataFrame({
        "Date": [today],
        "Region": ["S"],
        "Product Category Mapped": ["Fruit Juice"],
        "Product": ["X"],
        "Sales": [500],
        "is_anomaly": [1],
    })

    rg = ReportGenerator(df)
    report = rg.generate_report(run_anomaly_detection=False)

    assert report["today_anomalies"]["total_anomalies"] == 1
    assert "Today's anomalies detected" in caplog.text



# ================================================================================
#  save_report tests
# ================================================================================

def test_save_report_success(tmp_path):
    rg = ReportGenerator(pd.DataFrame({"x": [1]}))
    rg.report_dict = {"ok": True}

    out = tmp_path / "out.json"
    rg.save_report(str(out))

    assert out.exists()
    with open(out) as f:
        assert json.load(f)["ok"] is True


def test_save_report_failure(tmp_path):
    rg = ReportGenerator(pd.DataFrame({"x": [1]}))
    rg.report_dict = {"fail": True}

    with patch("builtins.open", side_effect=Exception("boom")):
        with pytest.raises(Exception):
            rg.save_report(str(tmp_path / "no.json"))

def test_detect_anomalies_model_load_error(tmp_path, caplog):
    """Covers: except Exception during joblib.load (model file exists)."""
    caplog.set_level("ERROR")

    # --- dataframe ---
    df = pd.DataFrame({
        "Product Category Mapped": ["Carbonated Drink"],
        "Sales": [100],
        "Date": ["2025-01-01"]
    })
    rg = ReportGenerator(df)

    # --- create dummy placeholder file so path.exists() == True ---
    model_path = tmp_path / "broken_model.pkl"
    model_path.write_text("dummy content")

    # --- force joblib.load to fail WHILE file exists ---
    with patch("joblib.load", side_effect=Exception("corrupted file")):
        rg._detect_anomalies(str(model_path))

    # --- assertions ---
    assert "Error loading model from" in caplog.text
    assert rg.report_dataframe["is_anomaly"].tolist() == [0]
