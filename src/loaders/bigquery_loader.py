# src/loaders/bigquery_loader.py

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

class BigQueryLoader:
    def __init__(self, credentials_info: dict, project_id: str):
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=project_id)

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida e ajusta tipos de dados e nomes de colunas para compatibilidade com BigQuery."""
        # Garantir que os nomes das colunas sejam strings
        df.columns = df.columns.map(str)

        # Remover espaços em branco nos nomes das colunas
        df.columns = df.columns.str.strip()

        # Substituir espaços por underscores nos nomes das colunas
        df.columns = df.columns.str.replace(' ', '_')

        # Verificar tipos de dados
        for column in df.columns:
            if pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].astype(str)
            elif pd.api.types.is_integer_dtype(df[column]):
                df[column] = df[column].astype('Int64')  # Tipo compatível com BigQuery
            elif pd.api.types.is_float_dtype(df[column]):
                df[column] = df[column].astype(float)
            elif pd.api.types.is_bool_dtype(df[column]):
                df[column] = df[column].astype(bool)
            else:
                # Converte outros tipos para string por segurança
                df[column] = df[column].astype(str)
        return df

    def load_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        """Carrega o DataFrame no BigQuery."""
        df = self.validate_dataframe(df)  # Valida e ajusta tipos de dados

        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        print(f"Iniciando carga para a tabela '{table_ref}'...")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Aguarda o término do job
        print(f"Carregado {len(df)} linhas na tabela '{table_ref}'.")
