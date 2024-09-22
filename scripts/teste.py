import os
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

def create_credentials(key_file_path):
    """Create and return credentials from a JSON key file."""
    return service_account.Credentials.from_service_account_file(key_file_path)

def initialize_storage_client(credentials, project_id):
    """Initialize and return a Google Cloud Storage client."""
    return storage.Client(credentials=credentials, project=project_id)

def get_bucket(client, bucket_name):
    """Get and return a specific bucket from the storage client."""
    return client.get_bucket(bucket_name)

def generate_file_paths(base_path, today_str):
    """Generate file paths for raw, processed, and trusted data."""
    return {
        'raw': f'{base_path}/raw/raw_steamdb_sales_{today_str}.csv',
        'processed': f'{base_path}/processed/processed_steamdb_sales_{today_str}.csv',
        'trusted': f'{base_path}/trusted/trusted_steamdb_sales_{today_str}.csv'
    }

def upload_file_if_exists(bucket, local_path, blob_name):
    """Upload a file to GCS if it exists locally."""
    if os.path.exists(local_path):
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        print(f"File uploaded to gs://{bucket.name}/{blob_name}")
    else:
        print(f"File not found: {local_path}")

def main():
    # Configuration
    key_file_path = '/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/configs/data-analytics.json'
    project_id = 'data-analytics-433518'
    bucket_name = 'steamdb_sales'
    base_path = '/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data'

    # Get today's date string
    today_str = datetime.now().strftime("%Y%m%d")

    # Create credentials and initialize client
    credentials = create_credentials(key_file_path)
    client = initialize_storage_client(credentials, project_id)

    # Get the bucket
    bucket = get_bucket(client, bucket_name)

    # Generate file paths
    file_paths = generate_file_paths(base_path, today_str)

    # Upload files
    for file_type, local_path in file_paths.items():
        blob_name = f'{file_type}/{os.path.basename(local_path)}'
        upload_file_if_exists(bucket, local_path, blob_name)

if __name__ == "__main__":
    main()