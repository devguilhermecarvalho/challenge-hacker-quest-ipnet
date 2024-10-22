# src/data_ingestion.py
import pandas as pd
import os
from typing import Dict
from abc import ABC, abstractmethod
import yaml
from google.cloud import storage

with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

bucket_name = configs['bucket_name']
silver_folder = configs['silver_folder']

class DataIngestion:
    def __init__(self, bucket_name: str, folder_name: str):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def read_data(self) -> Dict[str, pd.DataFrame]:
        """Reads data from the specified folder in GCS."""
        dataframes = {}
        blobs = list(self.client.list_blobs(self.bucket_name, prefix=f"{self.folder_name}/"))

        for blob in blobs:
            file_name = os.path.basename(blob.name)
            if not file_name:
                continue  # Skip directories
            extension = os.path.splitext(file_name)[1].lower()

            try:
                reader = ReaderFactory.get_reader(extension)
                content = blob.download_as_bytes()
                delimiter = configs.get('file_delimiter_mapping', {}).get(file_name, None)
                df = reader.read(blob.name, content, delimiter=delimiter)

                dataframes[file_name] = df
                print(f"O arquivo '{file_name}' foi carregado com sucesso.")
            except Exception as e:
                print(f"Erro ao carregar o arquivo '{file_name}': {e}")

        return dataframes
