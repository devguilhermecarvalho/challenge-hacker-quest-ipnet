# src/data_ingestion.py

import pandas as pd
import os
from typing import Dict, Callable

class DataIngestion:
    def __init__(self):
        self.readers: Dict[str, Callable[[str], pd.DataFrame]] = {
            '.csv': pd.read_csv,
            '.tsv': lambda path: pd.read_csv(path, sep='\t'),
            '.txt': pd.read_csv,
            '.xlsx': pd.read_excel,
            '.xls': pd.read_excel
        }

    def read_data(self, path: str) -> Dict[str, pd.DataFrame]:
        """Reads data from files in the specified directory."""
        dataframes = {}
        files_in_directory = os.listdir(path)

        for file_name in files_in_directory:
            file_path = os.path.join(path, file_name)

            if os.path.isfile(file_path):
                extension = os.path.splitext(file_name)[1].lower()
                if extension in self.readers:
                    try:
                        dataframes[file_name] = self.readers[extension](file_path)
                        print(f"The file '{file_name}' loaded successfully.")
                    except (pd.errors.ParserError, FileNotFoundError, pd.errors.EmptyDataError) as e:
                        print(f"Error loading file '{file_name}': {e}")
                else:
                    print(f"Unsupported file extension: '{extension}'")
        return dataframes

if __name__ == "__main__":
    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data('data/')

