from google.cloud import bigquery
import pandas as pd

class BigQueryLoader:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def create_dataset_if_not_exists(self, dataset_id: str):
        """Creates the dataset in BigQuery if it does not exist."""
        dataset_ref = f"{self.client.project}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            print(f"Dataset '{dataset_id}' already exists in project '{self.client.project}'.")
        except Exception:
            print(f"Dataset '{dataset_id}' not found. Creating...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"Dataset '{dataset_id}' created successfully.")

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.map(str)
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace(',', '_')  # Replace commas in column names

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
        print(f"Starting load to table '{table_ref}'...")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,  # Allow BigQuery to autodetect the schema
        )

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {len(df)} rows into table '{table_ref}'.")
