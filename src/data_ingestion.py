# src/data_ingestion.py
import pandas as pd
import os
from typing import Dict
from abc import ABC, abstractmethod
import yaml
from google.cloud import storage

with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

class Reader(ABC):
    @abstractmethod
    def read(self, file_path: str, content: bytes, delimiter=None) -> pd.DataFrame:
        pass

class CSVReader(Reader):
    def read(self, file_path: str, content: bytes, delimiter=None) -> pd.DataFrame:
        try:
            if delimiter:
                df = pd.read_csv(pd.compat.BytesIO(content), sep=delimiter, engine='python', header=None)
            else:
                df = pd.read_csv(pd.compat.BytesIO(content), sep=None, engine='python', header=None)
            df = self._apply_generic_headers(df)
            return df
        except Exception as e:
            print(f"Erro ao ler o arquivo {file_path}: {e}")
            raise e

    def _apply_generic_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        num_columns = df.shape[1]
        generic_headers = [f'column{i+1}' for i in range(num_columns)]
        df.columns = generic_headers
        return df

class TSVReader(Reader):
    def read(self, file_path: str, content: bytes, delimiter=None) -> pd.DataFrame:
        return pd.read_csv(pd.compat.BytesIO(content), sep='\t', header=None)

class ExcelReader(Reader):
    def read(self, file_path: str, content: bytes, delimiter=None) -> pd.DataFrame:
        return pd.read_excel(pd.compat.BytesIO(content), header=None)

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
            raise ValueError(f"Nenhum leitor encontrado para a extensão '{extension}'")
        return reader

class DataIngestion:
    def __init__(self, bucket_name, silver_folder):
        self.bucket_name = bucket_name
        self.silver_folder = silver_folder
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def read_data(self) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        blobs = self.client.list_blobs(self.bucket_name, prefix=self.silver_folder + '/')
        file_delimiter_mapping = configs.get('file_delimiter_mapping', {})

        for blob in blobs:
            file_name = os.path.basename(blob.name)
            if not file_name:
                continue  # Skip directories
            extension = os.path.splitext(file_name)[1].lower()
            try:
                reader = ReaderFactory.get_reader(extension)
                delimiter = file_delimiter_mapping.get(file_name, None)
                content = blob.download_as_bytes()
                df = reader.read(blob.name, content, delimiter=delimiter)
                dataframes[file_name] = df
                print(f"O arquivo '{file_name}' foi carregado com sucesso.")
                print(f"Colunas: {df.columns.tolist()}")
            except Exception as e:
                print(f"Erro ao carregar o arquivo '{file_name}': {e}")
        return dataframes