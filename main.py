import yaml
from google.oauth2 import service_account
from src.file_validation import FileValidation
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation
from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.cloudstorage_loader import CloudStorageLoader

# Carregar configurações
with open('config/configs.yml', 'r') as f:
    configs = yaml.safe_load(f)

project_id = configs['project_id']
dataset_id = configs['dataset_id']
credentials_path = configs['credentials_path']
data_raw_directory = configs['data_raw_directory']
data_validation_directory = configs['data_validation_directory']
bucket_name = configs['bucket_name']

# Executar validação de arquivos
validator = FileValidation()
validator.validate_and_process_files()

# Carregar credenciais do arquivo de chave da conta de serviço
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Inicializar DataIngestion
data_ingestion = DataIngestion()
dataframes = data_ingestion.read_data(data_validation_directory)

# Validar dados
data_validation = DataValidation(dataframes)
data_validation.validate_data()

# Inicializar BigQueryLoader com as credenciais
bq_loader = BigQueryLoader(credentials, project_id)

# Criar o dataset caso não exista usando o método dentro do BigQueryLoader
bq_loader.create_dataset_if_not_exists(dataset_id)

# Carregar dataframes no BigQuery
for file_name, df in dataframes.items():
    table_id = file_name.split('.')[0]
    bq_loader.load_dataframe(df, dataset_id, table_id)
    print(f"Tabela '{table_id}' carregada com sucesso.")

# Inicializar CloudStorageLoader e fazer upload dos arquivos validados
cloud_storage_loader = CloudStorageLoader(credentials, bucket_name)
cloud_storage_loader.verify_folder_exists('raw_validated/')
cloud_storage_loader.upload_files(data_validation_directory)