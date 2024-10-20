# src/data_validation.py

import pandas as pd
import great_expectations as ge
from great_expectations.data_context import DataContext
from typing import Dict

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes
        self.context = DataContext()

    def validate_data(self):
        for file_name, df in self.dataframes.items():
            try:
                batch = ge.from_pandas(df)
                expectation_suite_name = "my_expectation_suite"

                # Ensure the expectation suite exists
                try:
                    self.context.get_expectation_suite(expectation_suite_name)
                except ge.exceptions.DataContextError:
                    self.context.create_expectation_suite(expectation_suite_name)

                # Validate the batch
                results = self.context.run_validation_operator(
                    "action_list_operator",
                    assets_to_validate=[batch],
                    run_id={"run_name": file_name}
                )

                print(f"Validation result for {file_name}: {results}")

            except Exception as e:
                print(f"Validation error for {file_name}: {e}")

if __name__ == "__main__":
    from data_ingestion import DataIngestion

    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data('data/')

    data_validation = DataValidation(dataframes)
    data_validation.validate_data()
