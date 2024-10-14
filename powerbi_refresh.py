import requests
import msal
import json

def get_access_token(client_id, client_secret, tenant_id):
    """Obtain an access token from Azure AD for Power BI API."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    scopes = ["https://analysis.windows.net/powerbi/api/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    
    if "access_token" in result:
        return result['access_token']
    else:
        raise Exception(f"Failed to obtain access token: {result.get('error_description')}")

def trigger_refresh(workspace_id, dataset_id, access_token):
    """Trigger a dataset refresh in Power BI."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    payload = {
        "notifyOption": "NoNotification"
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 202:
        print("Refresh triggered successfully.")
    else:
        print(f"Failed to trigger refresh: {response.status_code} - {response.text}")

def main():
    # Configuration
    CLIENT_ID = 'your-azure-ad-app-client-id'
    CLIENT_SECRET = 'your-azure-ad-app-client-secret'
    TENANT_ID = 'your-tenant-id'
    WORKSPACE_ID = 'your-powerbi-workspace-id'
    DATASET_ID = 'your-powerbi-dataset-id'
    
    # Get Access Token
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
    
    # Trigger Dataset Refresh
    trigger_refresh(WORKSPACE_ID, DATASET_ID, access_token)

if __name__ == "__main__":
    main()
