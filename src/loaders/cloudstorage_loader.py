from google.cloud import storage
import os

class CloudStorageLoader:
    def __init__(self, credentials, bucket_name: str):
        self.client = storage.Client(credentials=credentials)
        self.bucket_name = bucket_name
        self.bucket = self.get_or_create_bucket()

    def get_or_create_bucket(self):
        """Verifica se o bucket existe; se não, cria."""
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' já existe.")
        except Exception as e:
            print(f"Bucket '{self.bucket_name}' não encontrado. Criando...")
            bucket = self.client.create_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' criado com sucesso.")
        return bucket

    def upload_files(self, source_directory: str, destination_folder: str = 'raw_validated/'):
        """Faz upload de arquivos do diretório local para o GCS na pasta de destino."""
        for root, _, files in os.walk(source_directory):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                blob_path = os.path.join(destination_folder, file_name)

                blob = self.bucket.blob(blob_path)
                blob.upload_from_filename(local_path)

                print(f"Upload de {file_name} para gs://{self.bucket_name}/{blob_path}")

    def verify_folder_exists(self, folder_name: str = 'raw_validated/'):
        """Garante que a pasta exista no bucket."""
        blobs = list(self.bucket.list_blobs(prefix=folder_name))
        if not blobs:
            print(f"Pasta '{folder_name}' está vazia ou não existe. Criando...")
            # Criar um arquivo placeholder para garantir que a pasta exista
            blob = self.bucket.blob(f"{folder_name}/.placeholder")
            blob.upload_from_string('')
            print(f"Pasta '{folder_name}' criada com sucesso.")