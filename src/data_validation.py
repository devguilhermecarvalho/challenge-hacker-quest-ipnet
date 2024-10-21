# src/data_validation.py
import pandas as pd
from typing import Dict
import yaml

with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

data_validation_directory = configs['data_validation_directory']

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes

    def validate_data(self):
        for file_name, df in self.dataframes.items():
            try:
                if df.empty:
                    raise ValueError(f"O DataFrame do arquivo '{file_name}' está vazio.")
                df = self.validate_headers(df, file_name)
                if df.isnull().values.any():
                    print(f"Aviso: O DataFrame do arquivo '{file_name}' contém valores nulos.")
                else:
                    print(f"O DataFrame do arquivo '{file_name}' passou nas validações.")
            except Exception as e:
                print(f"Erro na validação do arquivo '{file_name}': {e}")

    def validate_headers(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        if all(isinstance(col, (int, float)) or str(col).isdigit() for col in df.columns):
            print(f"Headers numéricos detectados no arquivo '{file_name}'. Atribuindo headers genéricos.")
            num_columns = df.shape[1]
            generic_headers = [f'column{i+1}' for i in range(num_columns)]
            df.columns = generic_headers
        else:
            df.columns = df.columns.map(str)
        return df