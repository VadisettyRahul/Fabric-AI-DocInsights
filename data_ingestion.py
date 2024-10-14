import os
import pandas as pd
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def get_secret(vault_url, secret_name):
    """Retrieve a secret from Azure Key Vault."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    retrieved_secret = client.get_secret(secret_name)
    return retrieved_secret.value

def initialize_storage_account(storage_account_name, storage_account_key):
    """Initialize the Azure Data Lake Storage account."""
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=storage_account_key
        )
        return service_client
    except Exception as e:
        print(f"Failed to initialize storage account: {e}")
        return None

def upload_sensor_data(service_client, file_system_name, directory_name, file_name, data):
    """Upload sensor data to Azure Data Lake Storage."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(directory=directory_name)
        
        # Create directory if it doesn't exist
        try:
            directory_client.get_directory_properties()
        except:
            directory_client.create_directory()
            print(f"Created directory: {directory_name}")
        
        file_client = directory_client.create_file(file_name)
        file_contents = data.to_csv(index=False)
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print(f"Uploaded {file_name} to {directory_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload sensor data: {e}")

def generate_dummy_sensor_data(num_records=1000):
    """Generate synthetic sensor data for demonstration purposes."""
    np.random.seed(42)
    timestamps = pd.date_range(start=datetime.now(), periods=num_records, freq='T')
    sensor_1 = np.random.normal(loc=50, scale=5, size=num_records)
    sensor_2 = np.random.normal(loc=30, scale=3, size=num_records)
    sensor_3 = np.random.normal(loc=100, scale=10, size=num_records)
    
    data = pd.DataFrame({
        'timestamp': timestamps,
        'sensor_1': sensor_1,
        'sensor_2': sensor_2,
        'sensor_3': sensor_3
    })
    return data

if __name__ == "__main__":
    # Configuration
    VAULT_URL = 'https://your-key-vault-name.vault.azure.net/'
    STORAGE_ACCOUNT_NAME = 'your_storage_account_name'
    STORAGE_ACCOUNT_KEY_SECRET = 'storage-account-key'  # Name of the secret in Key Vault
    FILE_SYSTEM_NAME = 'sensor-data'
    DIRECTORY_NAME = datetime.now().strftime('%Y-%m-%d')
    FILE_NAME = 'sensor_readings.csv'
    
    # Retrieve Storage Account Key from Key Vault
    STORAGE_ACCOUNT_KEY = get_secret(VAULT_URL, STORAGE_ACCOUNT_KEY_SECRET)
    
    # Initialize Storage Account
    service_client = initialize_storage_account(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    
    if service_client:
        # Generate and upload sensor data
        sensor_data = generate_dummy_sensor_data()
        upload_sensor_data(service_client, FILE_SYSTEM_NAME, DIRECTORY_NAME, FILE_NAME, sensor_data)
