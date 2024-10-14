import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from sklearn.ensemble import IsolationForest
import joblib
import io
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azureml.core import Workspace, Dataset, Experiment
from azureml.core.run import Run

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

def download_sensor_data(service_client, file_system_name, directory_name, file_name):
    """Download sensor data from Azure Data Lake Storage."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        file_client = file_system_client.get_file_client(f"{directory_name}/{file_name}")
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        data = pd.read_csv(io.StringIO(downloaded_bytes.decode('utf-8')))
        print(f"Downloaded {file_name} from {directory_name}/{file_name}")
        return data
    except Exception as e:
        print(f"Failed to download sensor data: {e}")
        return None

def connect_to_workspace(vault_url, workspace_secret_name):
    """Connect to Azure ML Workspace using secrets from Key Vault."""
    subscription_id = get_secret(vault_url, 'azure-subscription-id')
    resource_group = get_secret(vault_url, 'azure-resource-group')
    workspace_name = get_secret(vault_url, 'azure-ml-workspace-name')
    
    ws = Workspace(subscription_id, resource_group, workspace_name)
    return ws

def train_or_load_model(data, model_path='isolation_forest_model.joblib'):
    """Train a new Isolation Forest model or load an existing one."""
    if os.path.exists(model_path):
        model = joblib.load(model_path)
        print("Loaded pre-trained Isolation Forest model.")
    else:
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        features = ['sensor_1', 'sensor_2', 'sensor_3']
        model.fit(data[features])
        joblib.dump(model, model_path)
        print("Trained and saved Isolation Forest model.")
    return model

def detect_anomalies(data, model):
    """Detect anomalies in the sensor data using the Isolation Forest model."""
    data['anomaly'] = model.predict(data[['sensor_1', 'sensor_2', 'sensor_3']])
    # -1 indicates anomaly, 1 indicates normal
    data['anomaly'] = data['anomaly'].apply(lambda x: 'Anomaly' if x == -1 else 'Normal')
    return data

def upload_anomaly_results(service_client, file_system_name, directory_name, file_name, data):
    """Upload anomaly detection results to Azure Data Lake Storage."""
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
        print(f"Uploaded anomaly results to {directory_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload anomaly results: {e}")

def register_model(ws, model_path, model_name='isolation_forest_model'):
    """Register the Isolation Forest model with Azure ML Workspace."""
    from azureml.core.model import Model
    model = Model.register(workspace=ws,
                           model_path=model_path,
                           model_name=model_name)
    print(f"Registered model: {model.name}, version: {model.version}")
    return model

def main():
    # Configuration
    VAULT_URL = 'https://your-key-vault-name.vault.azure.net/'
    STORAGE_ACCOUNT_NAME = 'your_storage_account_name'
    STORAGE_ACCOUNT_KEY_SECRET = 'storage-account-key'  # Name of the secret in Key Vault
    FILE_SYSTEM_NAME = 'sensor-data'
    RAW_DATA_DIRECTORY = datetime.now().strftime('%Y-%m-%d')  # Assuming data ingested today
    RAW_FILE_NAME = 'sensor_readings.csv'
    
    PROCESSED_DATA_DIRECTORY = 'anomaly-results'
    PROCESSED_FILE_NAME = 'sensor_readings_with_anomalies.csv'
    
    # Retrieve Storage Account Key from Key Vault
    STORAGE_ACCOUNT_KEY = get_secret(VAULT_URL, STORAGE_ACCOUNT_KEY_SECRET)
    
    # Initialize Storage Account
    service_client = initialize_storage_account(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    
    if service_client:
        # Download sensor data
        sensor_data = download_sensor_data(service_client, FILE_SYSTEM_NAME, RAW_DATA_DIRECTORY, RAW_FILE_NAME)
        
        if sensor_data is not None:
            # Connect to Azure ML Workspace
            ws = connect_to_workspace(VAULT_URL, 'azure-ml-workspace-secret')
            
            # Train or load Isolation Forest model
            model = train_or_load_model(sensor_data)
            
            # Optionally, register the model with Azure ML
            model = register_model(ws, 'isolation_forest_model.joblib')
            
            # Detect anomalies
            processed_data = detect_anomalies(sensor_data, model)
            
            # Upload anomaly results
            upload_anomaly_results(service_client, FILE_SYSTEM_NAME, PROCESSED_DATA_DIRECTORY, PROCESSED_FILE_NAME, processed_data)

if __name__ == "__main__":
    main()
