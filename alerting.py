import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configuration
VAULT_URL = 'https://your-key-vault-name.vault.azure.net/'
ALERT_EMAIL_SECRET = 'alert-email'          # Secret name for alert email address
ALERT_EMAIL_PASSWORD_SECRET = 'alert-email-password'  # Secret name for email password
SMTP_SERVER_SECRET = 'smtp-server'          # Secret name for SMTP server
SMTP_PORT_SECRET = 'smtp-port'              # Secret name for SMTP port

def get_secret(vault_url, secret_name):
    """Retrieve a secret from Azure Key Vault."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    retrieved_secret = client.get_secret(secret_name)
    return retrieved_secret.value

def send_alert(data_point):
    """Send an email alert for the detected anomaly."""
    # Retrieve email credentials from Key Vault
    alert_email = get_secret(VAULT_URL, ALERT_EMAIL_SECRET)
    alert_email_password = get_secret(VAULT_URL, ALERT_EMAIL_PASSWORD_SECRET)
    smtp_server = get_secret(VAULT_URL, SMTP_SERVER_SECRET)
    smtp_port = int(get_secret(VAULT_URL, SMTP_PORT_SECRET))
    
    # Email content
    subject = "🚨 Anomaly Detected in Manufacturing Sensor Data"
    body = f"""
    An anomaly has been detected in the manufacturing process.

    Details:
    - Timestamp: {data_point['timestamp']}
    - Sensor ID: {data_point['sensor_id']}
    - Sensor 1 Value: {data_point['sensor_1']}
    - Sensor 2 Value: {data_point['sensor_2']}
    - Sensor 3 Value: {data_point['sensor_3']}
    - Status: {data_point['status']}

    Please investigate the issue immediately.
    """
    
    # Create the email message
    message = MIMEMultipart()
    message['From'] = alert_email
    message['To'] = alert_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))
    
    try:
        # Connect to SMTP server and send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(alert_email, alert_email_password)
        server.send_message(message)
        server.quit()
        print(f"Alert email sent to {alert_email}.")
    except Exception as e:
        print(f"Failed to send alert email: {e}")
