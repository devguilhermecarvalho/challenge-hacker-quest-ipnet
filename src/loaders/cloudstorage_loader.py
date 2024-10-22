# src/loaders/cloudstorage_loader.py
from google.cloud import storage

class CloudStorageLoader:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = None

    def verify_bucket_exists(self):
        """Verifies if the bucket exists; creates it if it doesn't."""
        try:
            self.bucket = self.client.get_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' já existe.")
        except Exception:
            print(f"Bucket '{self.bucket_name}' não encontrado. Criando...")
            self.bucket = self.client.create_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' criado com sucesso.")

    def verify_folder_exists(self, folder_name: str):
        """Ensures that the folder exists in the bucket."""
        blobs = list(self.bucket.list_blobs(prefix=folder_name + '/'))
        if not blobs:
            print(f"Pasta '{folder_name}' está vazia ou não existe. Criando...")
            blob = self.bucket.blob(f"{folder_name}/.placeholder")
            blob.upload_from_string('')
            print(f"Pasta '{folder_name}' criada com sucesso.")
        else:
            print(f"Pasta '{folder_name}' já existe.")