import os
import pandas as pd
import json
from dataclasses import dataclass, field
from typing import Self, Callable


CATEGORY_MAPPING = {
    "AA": "Carbonated Drink",      # Coca-Cola, Fanta, Sprite, Pepsi, 7Up, Schweppes
    "AB": "Plant-Based Drink",     # Golden Milk, Almond Drink, Oat Latte
    "AC": "Milkshake",             # Strawberry Shake, Chocolate Shake, Vanilla Shake
    "AD": "Fruit Juice",           # Orange Juice, Apple Juice, Mango Juice, etc.
    "AE": "Diet Drink",            # Cola Zero, Pepsi Max, Sprite Zero
    "AF": "Functional Beverage"    # Isotonic Drink, Coconut Water, Kombucha
}

REPORT_FILE = "sales_report.json"


class DataProcessor:
    """A class responsible for data processing such as filtering and category mapping."""

    @staticmethod
    def filter_data(df: pd.DataFrame, filter_func: Callable[[pd.DataFrame], pd.DataFrame] | None) -> pd.DataFrame:
        """Filters the given DataFrame based on the provided filter function.

        Args:
            df (pd.DataFrame): The DataFrame to filter.
            filter_func (Callable[[pd.DataFrame], pd.DataFrame] | None): The function used to filter the DataFrame. If None, no filtering is applied.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        if filter_func:
            return filter_func(df)
        return df

    @staticmethod
    def map_categories(df: pd.DataFrame) -> pd.DataFrame:
        """Maps product categories to more readable names.

        Args:
            df (pd.DataFrame): The DataFrame with the 'Product Category' column.

        Returns:
            pd.DataFrame: The DataFrame with an additional 'Product Category Mapped' column.
        """
        df["Product Category Mapped"] = df["Product Category"].map(CATEGORY_MAPPING)
        return df


@dataclass
class ReportGenerator:
    """A class for generating and saving sales reports."""

    report_dataframe: pd.DataFrame
    report_dict: dict[str, pd.DataFrame] = field(default_factory=dict)

    @classmethod
    def create_first_dataframe(cls, directory_path: str, file_names: list) -> Self:
        """Creates the first DataFrame by reading and processing CSV files.

        Args:
            directory_path (str): The path to the directory containing the CSV files.
            file_names (list): The list of CSV filenames to process.

        Returns:
            ReportGenerator: An instance of the ReportGenerator with the processed DataFrame.
        """
        data_frames = []
        for file_name in file_names:
            file_path = os.path.join(directory_path, file_name)
            df = pd.read_csv(file_path)
            df = DataProcessor.map_categories(df)  # Map categories
            data_frames.append(df)

        report_dataframe = pd.concat(data_frames, ignore_index=True)
        return ReportGenerator(report_dataframe)

    def update_dataframe(self, directory_path: str, file_names: list) -> None:
        """Updates the DataFrame with new files.

        Args:
            directory_path (str): The path to the directory containing the new CSV files.
            file_names (list): The list of new CSV filenames to process.
        """
        data_frames = []
        for file_name in file_names:
            file_path = os.path.join(directory_path, file_name)
            df = pd.read_csv(file_path)
            df = DataProcessor.map_categories(df)  # Map categories
            data_frames.append(df)

        if data_frames:
            new_data = pd.concat(data_frames, ignore_index=True)
            self.report_dataframe = pd.concat([self.report_dataframe, new_data], ignore_index=True)

    def generate_report(self) -> dict[str, pd.DataFrame]:
        """Generates the sales report with aggregations by region and product category.

        Returns:
            dict[str, pd.DataFrame]: A dictionary with sales reports by region and product category.
        """
        self.report_dict = self.report_dict = {
            'region_report_mean': self.report_dataframe.groupby(['Region'])['Sales'].mean().round(2).to_dict(),
            'beverage_report_total': self.report_dataframe.groupby(['Product Category Mapped'])['Sales'].sum().astype(int).to_dict(),
            'beverage_report_mean': self.report_dataframe.groupby(['Product Category Mapped'])['Sales'].mean().round(2).to_dict()
    }
        return self.report_dict

    def save_report(self, file_name: str = REPORT_FILE) -> None:
        """Saves the generated report to a JSON file.

        Args:
            file_name (str, optional): The name of the file to save the report. Defaults to "sales_report.json".
        """
        # Serializing JSON
        json_object = json.dumps(self.report_dict, indent=4)

        # Writing to file
        with open(file_name, "w") as outfile:
            outfile.write(json_object)
