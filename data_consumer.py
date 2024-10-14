import json
import os
import pandas as pd
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest
import joblib
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sensor-data'
GROUP_ID = 'anomaly-detector-group'
MODEL_PATH = 'isolation_forest_model.joblib'

VAULT_URL = 'https://your-key-vault-name.vault.azure.net/'
STORAGE_ACCOUNT_NAME = 'your_storage_account_name'
STORAGE_ACCOUNT_KEY_SECRET = 'storage-account-key'  # Name of the secret in Key Vault
FILE_SYSTEM_NAME = 'sensor-data'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_serializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

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

def download_model(model_path=MODEL_PATH):
    """Load the Isolation Forest model."""
    if os.path.exists(model_path):
        model = joblib.load(model_path)
        print("Loaded pre-trained Isolation Forest model.")
    else:
        # If model does not exist, initialize a new one (for demo purposes)
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        # In a real scenario, you would train the model with historical data
        print("Initialized a new Isolation Forest model.")
    return model

def detect_anomaly(model, data_point):
    """Detect anomaly in a single data point."""
    features = [[data_point['sensor_1'], data_point['sensor_2'], data_point['sensor_3']]]
    prediction = model.predict(features)
    return 'Anomaly' if prediction[0] == -1 else 'Normal'

def upload_anomaly_result(service_client, directory_name, file_name, data):
    """Upload anomaly detection result to Azure Data Lake Storage."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)
        directory_client = file_system_client.get_directory_client(directory=directory_name)
        
        # Create directory if it doesn't exist
        try:
            directory_client.get_directory_properties()
        except:
            directory_client.create_directory()
            print(f"Created directory: {directory_name}")
        
        file_client = directory_client.create_file(file_name)
        file_contents = data.to_json()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print(f"Uploaded anomaly result to {directory_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload anomaly result: {e}")

def main():
    # Retrieve Storage Account Key from Key Vault
    STORAGE_ACCOUNT_KEY = get_secret(VAULT_URL, STORAGE_ACCOUNT_KEY_SECRET)
    
    # Initialize Storage Account
    service_client = initialize_storage_account(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    
    if not service_client:
        print("Storage account initialization failed. Exiting.")
        return
    
    # Load or initialize the model
    model = download_model()
    
    print("Starting to consume messages from Kafka...")
    
    for message in consumer:
        data = message.value
        # Assuming sensor data contains sensor_1, sensor_2, sensor_3
        # For demonstration, we need to map incoming data to these features
        # Here, we simulate feature extraction
        sensor_features = {
            'sensor_1': random.uniform(20.0, 100.0),
            'sensor_2': random.uniform(20.0, 100.0),
            'sensor_3': random.uniform(20.0, 100.0)
        }
        data_point = {
            'timestamp': data['timestamp'],
            'sensor_1': sensor_features['sensor_1'],
            'sensor_2': sensor_features['sensor_2'],
            'sensor_3': sensor_features['sensor_3']
        }
        
        # Detect anomaly
        status = detect_anomaly(model, data_point)
        data_point['status'] = status
        
        print(f"Data Point: {data_point}")
        
        # Upload result to Azure Data Lake
        directory_name = datetime.utcnow().strftime('%Y-%m-%d')
        file_name = f"anomaly_{datetime.utcnow().isoformat()}.json"
        upload_anomaly_result(service_client, directory_name, file_name, data_point)
        
        # Trigger real-time alert if anomaly is detected
        if status == 'Anomaly':
            # Import the alerting module and send alert
            from alerting import send_alert
            send_alert(data_point)

if __name__ == "__main__":
    main()
