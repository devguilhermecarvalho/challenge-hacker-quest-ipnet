# config/gcp_secrets.py

import json
from google.cloud import secretmanager

def get_service_account_key(project_number, secret_name, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_number}/secrets/{secret_name}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))
