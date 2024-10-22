import pandas as pd
import os
from typing import Dict
from abc import ABC, abstractmethod
from google.cloud import storage
import yaml
import io

# Load configurations
with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

file_delimiter_mapping = configs.get('file_delimiter_mapping', {})

class Reader(ABC):
    @abstractmethod
    def read(self, blob_content, delimiter=None) -> pd.DataFrame:
        pass

class CSVReader(Reader):
    def read(self, blob_content, delimiter=None) -> pd.DataFrame:
        try:
            if delimiter:
                df = pd.read_csv(blob_content, sep=delimiter, engine='python', header=None)
            else:
                df = pd.read_csv(blob_content, sep=None, engine='python', header=None)
            df = self._apply_generic_headers(df)
            return df
        except Exception as e:
            print(f"Error reading file: {e}")
            raise e

    def _apply_generic_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        num_columns = df.shape[1]
        generic_headers = [f'column{i+1}' for i in range(num_columns)]
        df.columns = generic_headers
        return df

class TSVReader(Reader):
    def read(self, blob_content, delimiter=None) -> pd.DataFrame:
        return pd.read_csv(blob_content, sep='\t', header=None)

class ExcelReader(Reader):
    def read(self, blob_content, delimiter=None) -> pd.DataFrame:
        return pd.read_excel(blob_content, header=None)

class ReaderFactory:
    _readers = {
        '.csv': CSVReader(),
        '.tsv': TSVReader(),
        '.txt': CSVReader(),
        '.xlsx': ExcelReader(),
        '.xls': ExcelReader()
    }

    @classmethod
    def get_reader(cls, extension: str) -> Reader:
        reader = cls._readers.get(extension)
        if reader is None:
            raise ValueError(f"No reader found for extension '{extension}'")
        return reader

class DataIngestion:
    def __init__(self, storage_client, bucket_name, bronze_layer_path):
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.bronze_layer_path = bronze_layer_path
        self.bucket = self.storage_client.bucket(bucket_name)

    def read_data(self) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        blobs = self.bucket.list_blobs(prefix=self.bronze_layer_path)

        for blob in blobs:
            if blob.name.endswith('/'):
                continue  # Skip folders
            file_name = blob.name.replace(self.bronze_layer_path, '', 1)
            extension = os.path.splitext(file_name)[1].lower()
            try:
                reader = ReaderFactory.get_reader(extension)
                delimiter = file_delimiter_mapping.get(file_name, None)
                content = blob.download_as_bytes()
                blob_content = io.BytesIO(content)
                df = reader.read(blob_content, delimiter=delimiter)
                dataframes[file_name] = df
                print(f"File '{file_name}' loaded successfully.")
                print(f"Columns: {df.columns.tolist()}")
            except Exception as e:
                print(f"Error loading file '{file_name}': {e}")
        return dataframes