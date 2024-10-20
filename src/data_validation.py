import pandas as pd
from typing import Dict

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes

    def validate_data(self):
        for file_name, df in self.dataframes.items():
            try:
                # Exemplo de validações simples
                if df.empty:
                    raise ValueError(f"O DataFrame do arquivo '{file_name}' está vazio.")
                if df.isnull().values.any():
                    print(f"Aviso: O DataFrame do arquivo '{file_name}' contém valores nulos.")
                else:
                    print(f"O DataFrame do arquivo '{file_name}' passou nas validações.")
            except Exception as e:
                print(f"Erro na validação do arquivo '{file_name}': {e}")

if __name__ == "__main__":
    from data_ingestion import DataIngestion

    data_ingestion = DataInestion()
    dataframes = data_ingestion.read_data('data/raw/')

    data_validation = DataValidation(dataframes)
    data_validation.validate_data()
