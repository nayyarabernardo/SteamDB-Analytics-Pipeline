import os
import pandas as pd
import numpy as np
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

# Funções para credenciais e inicialização de serviços
def create_credentials(key_file_path):
    """Create and return credentials from a JSON key file."""
    return service_account.Credentials.from_service_account_file(key_file_path)

def initialize_storage_client(credentials, project_id):
    """Initialize and return a Google Cloud Storage client."""
    return storage.Client(credentials=credentials, project=project_id)

def initialize_bigquery_client(credentials, project_id):
    """Initialize and return a BigQuery client."""
    return bigquery.Client(credentials=credentials, project=project_id)

def initialize_sheets_service(credentials):
    """Initialize and return the Google Sheets service."""
    return build('sheets', 'v4', credentials=credentials)

def get_bucket(client, bucket_name):
    """Get and return a specific bucket from the storage client."""
    return client.get_bucket(bucket_name)

# Funções para processamento de arquivos
def generate_file_paths(base_path):
    """Generate file paths for raw, processed, and trusted data."""
    return {
        'raw': f'{base_path}/raw/raw_steamdb_sales_{datetime.now().strftime("%Y%m%d")}.csv',
        'processed': f'{base_path}/processed/processed_steamdb_sales_{datetime.now().strftime("%Y%m%d")}.csv',
        'trusted': f'{base_path}/trusted/trusted_steamdb_sales.csv'  # Fix name for trusted file
    }

def upload_file_if_exists(bucket, local_path, blob_name):
    """Upload a file to GCS if it exists locally."""
    if os.path.exists(local_path):
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        print(f"File uploaded to gs://{bucket.name}/{blob_name}")
        return True
    else:
        print(f"File not found: {local_path}")
        return False

def append_gcs_to_bigquery(bigquery_client, project_id, bucket_name, blob_name, dataset_id, table_id):
    """Append data from a CSV file in GCS to an existing BigQuery table."""
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Check if table exists
    try:
        bigquery_client.get_table(table_ref)
        print(f"Table {table_id} exists, appending data.")
    except:
        print(f"Table {table_id} does not exist, it will be created.")

    # Configure job for incremental load (append)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Append mode

    uri = f"gs://{bucket_name}/{blob_name}"

    load_job = bigquery_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete

    print(f"Appended {blob_name} to {project_id}.{dataset_id}.{table_id}")

# Funções para manipulação de dados no Google Sheets
def clean_data(df):
    """Clean the DataFrame by replacing NaN values and cleaning text."""
    df = df.replace({np.nan: '', 'NaN': ''})
    for col in df.columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.replace('\n', ' ').str.replace('\r', '')
    return df

def get_existing_data(service, spreadsheet_id, range_name):
    """Get existing data from Google Sheets."""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    if not values:
        return pd.DataFrame()
    return pd.DataFrame(values[1:], columns=values[0])

def upload_to_google_sheets(service, spreadsheet_id, range_name, csv_file):
    """Upload new records from a CSV file to Google Sheets incrementally."""
    existing_df = get_existing_data(service, spreadsheet_id, range_name)
    new_df = pd.read_csv(csv_file, na_values=['', 'NaN', 'NULL'])
    new_df = clean_data(new_df)

    id_column = new_df.columns[0]

    # Identificar novos registros
    if not existing_df.empty:
        new_records = new_df[~new_df[id_column].isin(existing_df[id_column])]
    else:
        new_records = new_df

    if new_records.empty:  # Aqui foi feita a correção
        print("Nenhum novo registro para adicionar.")
        return

    # Preparar novos dados para upload
    values = new_records.values.tolist()

    # Corpo da requisição
    body = {
        'values': values
    }

    # Executar o upload incremental
    result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, 
        range=range_name,
        valueInputOption='RAW', 
        body=body,
        insertDataOption='INSERT_ROWS'
    ).execute()

    print(f"{result.get('updates').get('updatedRows')} novas linhas adicionadas.")


# Função principal
def main():
    # Configuration
    key_file_path = '/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/configs/data-analytics.json'
    project_id = 'data-analytics-433518'
    bucket_name = 'steamdb_sales'
    base_path = '/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data'
    dataset_id = 'steamdb'
    spreadsheet_id = '1siFjaCa92INpVe-cp2kr8vIAiNvs5thCLkI4VxBTjC8'
    range_name = 'sales!A:Z'  # Modificado para incluir todas as colunas

    # Create credentials and initialize clients
    credentials = create_credentials(key_file_path)
    storage_client = initialize_storage_client(credentials, project_id)
    bigquery_client = initialize_bigquery_client(credentials, project_id)
    sheets_service = initialize_sheets_service(credentials)

    # Get the bucket
    bucket = get_bucket(storage_client, bucket_name)

    # Generate file paths
    file_paths = generate_file_paths(base_path)

    # Mapping of file types to their respective BigQuery table names
    table_names = {
        'raw': 'raw_steamdb_sales',
        'processed': 'processed_steamdb_sales',
        'trusted': 'trusted_steamdb_sales'
    }

    # Upload files
    for file_type, local_path in file_paths.items():
        blob_name = f'{file_type}/{os.path.basename(local_path)}'
        
        if upload_file_if_exists(bucket, local_path, blob_name):
            if file_type == 'trusted':
                # Apenas para a tabela 'trusted', carrega no BigQuery
                table_id = table_names[file_type]
                append_gcs_to_bigquery(bigquery_client, project_id, bucket_name, blob_name, dataset_id, table_id)

                # E carrega também no Google Sheets
                upload_to_google_sheets(sheets_service, spreadsheet_id, range_name, local_path)

if __name__ == "__main__":
    main()

