# src/file_validation.py
import os
import pandas as pd
import yaml
from google.cloud import storage

with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

class FileValidation:
    def __init__(self, bucket_name, bronze_folder, silver_folder):
        self.bucket_name = bucket_name
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.type_files = ['.csv', '.tsv', '.txt', '.xlsx', '.xls']
        self.count_names = {}
            
    def validate_and_process_files(self):
        blobs = self.client.list_blobs(self.bucket_name, prefix=self.bronze_folder + '/')
        processed_files = set()

        for blob in blobs:
            file_name = os.path.basename(blob.name)
            extension = os.path.splitext(file_name)[1].lower()
            if extension not in self.type_files:
                continue
            if file_name in processed_files:
                continue

            processed_files.add(file_name)
            file_size = blob.size

            if file_size == 0:
                print(f'O arquivo {file_name} está vazio.')
                continue

            # Copy the file to the silver_folder
            destination_blob_name = f"{self.silver_folder}/{file_name}"
            new_blob = self.bucket.copy_blob(blob, self.bucket, destination_blob_name)
            print(f'O arquivo {file_name} foi copiado para o diretório de validação.')

            if extension == '.txt':
                self.convert_txt_to_csv(destination_blob_name)

    def convert_txt_to_csv(self, blob_name):
        try:
            # Download the txt file
            blob = self.bucket.blob(blob_name)
            contents = blob.download_as_text()

            # Convert to DataFrame
            df = pd.read_csv(pd.compat.StringIO(contents), sep=None, engine='python')

            # Save as CSV back to GCS
            csv_blob_name = os.path.splitext(blob_name)[0] + '.csv'
            csv_data = df.to_csv(index=False)
            csv_blob = self.bucket.blob(csv_blob_name)
            csv_blob.upload_from_string(csv_data, content_type='text/csv')

            print(f'Convertido {os.path.basename(blob_name)} para o formato CSV.')
        except Exception as e:
            print(f'Falha ao converter {os.path.basename(blob_name)} para CSV: {e}')