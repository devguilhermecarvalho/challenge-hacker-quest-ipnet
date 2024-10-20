# src/data_ingestion.py

import pandas as pd
import os
from typing import Dict
from abc import ABC, abstractmethod

class Reader(ABC):
    @abstractmethod
    def read(self, file_path: str) -> pd.DataFrame:
        pass

class CSVReader(Reader):
    def read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)

class TSVReader(Reader):
    def read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path, sep='\t')

class ExcelReader(Reader):
    def read(self, file_path: str) -> pd.DataFrame:
        return pd.read_excel(file_path, header=0)
    
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
    def read_data(self, path: str) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        files_in_directory = os.listdir(path)

        for file_name in files_in_directory:
            file_path = os.path.join(path, file_name)
            if os.path.isfile(file_path):
                extension = os.path.splitext(file_name)[1].lower()
                try:
                    reader = ReaderFactory.get_reader(extension)
                    df = reader.read(file_path)

                    # Garantir que os nomes das colunas sejam strings
                    df.columns = df.columns.map(str)

                    dataframes[file_name] = df
                    print(f"O arquivo '{file_name}' foi carregado com sucesso.")
                except Exception as e:
                    print(f"Erro ao carregar o arquivo '{file_name}': {e}")
        return dataframes

if __name__ == "__main__":
    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data('data/raw/')
