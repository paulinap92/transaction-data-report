import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
from app.file_process.report_generator import DataProcessor, ReportGenerator, CATEGORY_MAPPING

class TestDataProcessor(unittest.TestCase):

    def test_map_categories(self):

        df = pd.DataFrame({
            "Product Category": ["A", "B", "C", "E"]
        })
        expected = {
            "Product Category": ["A", "B", "C", "E"],
            "Product Category Mapped": ["Electronics", "Furniture", "Clothing", "Food"]
        }
        df = DataProcessor.map_categories(df)
        pd.testing.assert_frame_equal(df, pd.DataFrame(expected))

    def test_filter_data_with_filter_func(self):
        df = pd.DataFrame({
            "Sale Amount": [100, 200, 300],
            "Region": ["North", "South", "East"]
        })
        filter_func = lambda df: df[df["Sale Amount"] > 150]
        filtered_df = DataProcessor.filter_data(df, filter_func)
        self.assertEqual(filtered_df.shape[0], 2)

    def test_filter_data_without_filter_func(self):
        df = pd.DataFrame({
            "Sale Amount": [100, 200, 300],
            "Region": ["North", "South", "East"]
        })
        filtered_df = DataProcessor.filter_data(df, None)
        self.assertEqual(filtered_df.shape[0], 3)

class TestReportGenerator(unittest.TestCase):

    @patch("pandas.read_csv")
    def test_create_first_dataframe(self, mock_read_csv):

        mock_read_csv.return_value = pd.DataFrame({
            "Product Category": ["A", "B", "C"],
            "Sale Amount": [100, 200, 300],
            "Region": ["North", "South", "East"]
        })

        directory_path = "data"
        file_names = ["file1.csv", "file2.csv"]

        report_generator = ReportGenerator.create_first_dataframe(directory_path, file_names)

        self.assertEqual(report_generator.report_dataframe.shape[0], 6)  # Suma wierszy z dwóch plików

    @patch("pandas.read_csv")
    def test_update_dataframe(self, mock_read_csv):
        # Przygotowanie danych testowych
        mock_read_csv.return_value = pd.DataFrame({
            "Product Category": ["A", "B", "C"],
            "Sale Amount": [100, 200, 300],
            "Region": ["North", "South", "East"]
        })

        directory_path = "data"
        file_names = ["file1.csv", "file2.csv"]
        report_generator = ReportGenerator.create_first_dataframe(directory_path, file_names)

        new_file_names = ["file3.csv", "file4.csv"]
        report_generator.update_dataframe(directory_path, new_file_names)

        self.assertEqual(report_generator.report_dataframe.shape[0], 12)  # 6 z poprzednich plików + 6 nowych

    def test_generate_report(self):
        df = pd.DataFrame({
            "Region": ["North", "South", "East", "North", "South", "East"],
            "Sale Amount": [100, 200, 300, 150, 250, 350],
            "Product Category": ["A", "B", "C", "A", "B", "C"]
        })
        report_generator = ReportGenerator(df)

        report = report_generator.generate_report()

        self.assertIn("region_report_mean", report)
        self.assertIn("category_report", report)
        self.assertIn("category_report_mean", report)

    @patch("builtins.open", mock_open())
    def test_save_report(self):
        df = pd.DataFrame({
            "Region": ["North", "South", "East"],
            "Sale Amount": [100, 200, 300],
            "Product Category": ["A", "B", "C"]
        })
        report_generator = ReportGenerator(df)

        with patch("builtins.open", mock_open()) as mock_file:
            report_generator.save_report("test_report.json")
            mock_file.assert_called_with("test_report.json", "w")
            mock_file().write.assert_called()

if __name__ == "__main__":
    unittest.main()
