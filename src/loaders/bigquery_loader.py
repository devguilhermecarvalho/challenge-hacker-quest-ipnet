from google.cloud import bigquery
import pandas as pd

class BigQueryLoader:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def create_dataset_if_not_exists(self, dataset_id: str):
        """Cria o dataset no BigQuery caso não exista."""
        dataset_ref = f"{self.client.project}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            print(f"Dataset '{dataset_id}' já existe no projeto '{self.client.project}'.")
        except Exception as e:
            print(f"Dataset '{dataset_id}' não encontrado. Criando...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"Dataset '{dataset_id}' criado com sucesso.")

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.map(str)
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace(',', '_')  # Substituir vírgulas nos nomes das colunas

        for column in df.columns:
            if pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].astype(str)
            elif pd.api.types.is_integer_dtype(df[column]):
                df[column] = df[column].astype('Int64')
            elif pd.api.types.is_float_dtype(df[column]):
                df[column] = df[column].astype(float)
            elif pd.api.types.is_bool_dtype(df[column]):
                df[column] = df[column].astype(bool)
            else:
                df[column] = df[column].astype(str)
        return df

    def load_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        df = self.validate_dataframe(df)

        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        print(f"Iniciando carga para a tabela '{table_ref}'...")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,  # Permitir que o BigQuery autodetecte o esquema
        )

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Carregado {len(df)} linhas na tabela '{table_ref}'.")