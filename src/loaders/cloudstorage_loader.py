from google.cloud import storage
import io

class CloudStorageLoader:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.get_or_create_bucket()

    def get_or_create_bucket(self):
        """Verifies if the bucket exists; creates it if not."""
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' already exists.")
        except Exception:
            print(f"Bucket '{self.bucket_name}' not found. Creating...")
            bucket = self.client.create_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' created successfully.")
        return bucket

    def verify_and_create_bucket_and_folders(self, folders):
        """Ensures that the required folders exist within the bucket."""
        for folder in folders:
            self.create_folder_if_not_exists(folder)

    def create_folder_if_not_exists(self, folder_path: str):
        """Creates a folder in the bucket if it doesn't exist."""
        blobs = list(self.bucket.list_blobs(prefix=folder_path))
        if not blobs:
            print(f"Folder '{folder_path}' not found. Creating...")
            blob = self.bucket.blob(f"{folder_path}/.placeholder")
            blob.upload_from_string('')
            print(f"Folder '{folder_path}' created successfully.")
        else:
            print(f"Folder '{folder_path}' already exists.")

    def save_dataframe_to_storage(self, df, folder_path: str, file_name: str):
        """Saves a DataFrame as a CSV file in the specified Cloud Storage folder."""
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        blob_name = f"{folder_path}{file_name}"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"File '{file_name}' saved to '{blob_name}' in bucket '{self.bucket_name}'.")