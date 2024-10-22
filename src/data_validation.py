import pandas as pd
from typing import Dict

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes

    def validate_data(self):
        """Performs validation on all loaded DataFrames."""
        for file_name, df in self.dataframes.items():
            try:
                # Check if DataFrame is empty
                if df.empty:
                    raise ValueError(f"The DataFrame from file '{file_name}' is empty.")

                # Validate and correct headers if necessary
                df = self.validate_headers(df, file_name)

                # Check for null values
                if df.isnull().values.any():
                    print(f"Warning: The DataFrame from file '{file_name}' contains null values.")
                else:
                    print(f"The DataFrame from file '{file_name}' passed all validations.")
            except Exception as e:
                print(f"Error validating file '{file_name}': {e}")

    def validate_headers(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Validates headers and corrects them if they are numeric."""
        # Check if all headers are numbers or integers
        if all(isinstance(col, (int, float)) or str(col).isdigit() for col in df.columns):
            print(f"Numeric headers detected in file '{file_name}'. Assigning generic headers.")
            num_columns = df.shape[1]
            generic_headers = [f'column{i+1}' for i in range(num_columns)]
            df.columns = generic_headers  # Assign generic headers
        else:
            # Ensure all headers are strings
            df.columns = df.columns.map(str)
        
        return df