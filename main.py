# main.py
import os
import yaml
from flask import Flask
from src.file_validation import FileValidation
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation
from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.cloudstorage_loader import CloudStorageLoader
import subprocess

app = Flask(__name__)

def run_etl():
    with open('config/configs.yml', 'r') as f:
        configs = yaml.safe_load(f)

    project_id = configs['project_id']
    dataset_id = configs['dataset_id']
    data_raw_directory = configs['data_raw_directory']
    data_validation_directory = configs['data_validation_directory']
    bucket_name = configs['bucket_name']

    validator = FileValidation()
    validator.validate_and_process_files()

    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data(data_validation_directory)

    data_validation = DataValidation(dataframes)
    data_validation.validate_data()

    bq_loader = BigQueryLoader(project_id)
    bq_loader.create_dataset_if_not_exists(dataset_id)

    for file_name, df in dataframes.items():
        table_id = file_name.split('.')[0]
        bq_loader.load_dataframe(df, dataset_id, table_id)
        print(f"Tabela '{table_id}' carregada com sucesso.")

    cloud_storage_loader = CloudStorageLoader(bucket_name)
    cloud_storage_loader.verify_folder_exists('raw_validated/')
    cloud_storage_loader.upload_files(data_validation_directory)

    # Run dbt commands
    subprocess.run(['dbt', 'deps'], cwd='dbt_validations')
    subprocess.run(['dbt', 'seed'], cwd='dbt_validations')
    subprocess.run(['dbt', 'run'], cwd='dbt_validations')
    subprocess.run(['dbt', 'test'], cwd='dbt_validations')

@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    run_etl()
    return 'ETL process completed', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
