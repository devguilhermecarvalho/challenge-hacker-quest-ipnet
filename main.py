from google.cloud import bigquery
from google.oauth2 import service_account
from config.gcp_secrets import get_service_account_key
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation
from src.loaders.bigquery_loader import BigQueryLoader

project_number = "1089961646630"
secret_name = "dbt-service-account-key"
project_id = "etl-hacker-quest-ipnet"
dataset_id = "etl_hackerquest"
credentials_path = "/path/to/key.json"  # Substituir pelo caminho correto

# Obter credenciais
credentials_info = get_service_account_key(project_number, secret_name)

# Inicializar DataIngestion
data_ingestion = DataIngestion()
dataframes = data_ingestion.read_data('data/raw/')

# Validar dados
data_validation = DataValidation(dataframes)
data_validation.validate_data()

def create_dataset_if_not_exists(client, dataset_id, project_id):
    """Cria o dataset no BigQuery caso ele não exista."""
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)  # Verifica se o dataset existe
        print(f"Dataset '{dataset_id}' já existe no projeto '{project_id}'.")
    except Exception as e:
        print(f"Dataset '{dataset_id}' não encontrado. Criando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Defina a localização conforme necessário
        client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' criado com sucesso.")

# Inicializar BigQueryLoader
bq_loader = BigQueryLoader(credentials_info, project_id)

# Criar o dataset caso não exista
create_dataset_if_not_exists(bq_loader.client, dataset_id, project_id)

# Carregar dataframes no BigQuery
for file_name, df in dataframes.items():
    table_id = file_name.split('.')[0]  # Nome da tabela é o nome do arquivo sem extensão
    print(f"Carregando {len(df)} linhas na tabela '{table_id}'...")
    bq_loader.load_dataframe(df, dataset_id, table_id)
    print(f"Tabela '{table_id}' carregada com sucesso.")
