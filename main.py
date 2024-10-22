import yaml
import os
import io
from google.cloud import storage
from google.cloud import bigquery
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation
from src.loaders.bigquery_loader import BigQueryLoader

# Load configurations
with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

project_id = configs['project_id']
dataset_id = configs['dataset_id']
bucket_name = configs['bucket_name']
bronze_layer_path = configs['bronze_layer_path']
silver_layer_path = configs['silver_layer_path']

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=project_id)

# Ensure bucket and folders exist
def create_bucket_and_folders():
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except Exception:
        print(f"Bucket '{bucket_name}' not found. Creating...")
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")

    def create_folder_if_not_exists(folder_path):
        blobs = list(bucket.list_blobs(prefix=folder_path))
        if not blobs:
            print(f"Folder '{folder_path}' not found. Creating...")
            blob = bucket.blob(f"{folder_path}/.placeholder")
            blob.upload_from_string('')
            print(f"Folder '{folder_path}' created successfully.")
        else:
            print(f"Folder '{folder_path}' already exists.")

    create_folder_if_not_exists(bronze_layer_path)
    create_folder_if_not_exists(silver_layer_path)

create_bucket_and_folders()

# Data ingestion
data_ingestion = DataIngestion(storage_client, bucket_name, bronze_layer_path)
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
bucket = storage_client.bucket(bucket_name)
for file_name, df in dataframes.items():
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    blob_name = silver_layer_path + file_name
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"File '{file_name}' saved to '{blob_name}' in bucket '{bucket_name}'.")