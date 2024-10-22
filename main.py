import yaml
import os
import io
import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation
from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.cloudstorage_loader import CloudStorageLoader

@functions_framework.http
def main(request):
    # Load configurations
    with open('config/configs.yml', 'r') as f:
        configs = yaml.safe_load(f)

    project_id = configs['project_id']
    dataset_id = configs['dataset_id']
    bucket_name = configs['bucket_name']
    bronze_layer_path = configs['bronze_layer_path']
    silver_layer_path = configs['silver_layer_path']

    # Initialize storage loader
    storage_loader = CloudStorageLoader(bucket_name)

    # Ensure bucket and folders exist
    storage_loader.verify_and_create_bucket_and_folders([bronze_layer_path, silver_layer_path])

    # Data ingestion
    data_ingestion = DataIngestion(storage_loader.client, bucket_name, bronze_layer_path)
    dataframes = data_ingestion.read_data()

    # Data validation
    data_validation = DataValidation(dataframes)
    data_validation.validate_data()

    # Load data into BigQuery
    bq_loader = BigQueryLoader(project_id)
    bq_loader.create_dataset_if_not_exists(dataset_id)

    for file_name, df in dataframes.items():
        table_id = os.path.splitext(file_name)[0]
        bq_loader.load_dataframe(df, dataset_id, table_id)
        print(f"Table '{table_id}' loaded successfully.")

    # Save validated dataframes to silver_layer in Cloud Storage
    for file_name, df in dataframes.items():
        storage_loader.save_dataframe_to_storage(df, silver_layer_path, file_name)