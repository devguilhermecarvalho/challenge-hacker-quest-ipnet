from data_ingestion import DataIngestion
from data_validation import DataValidation

if __name__ == "__main__":
    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data('data/')
    data_validation = DataValidation(dataframes)
    data_validation.validate_data()
