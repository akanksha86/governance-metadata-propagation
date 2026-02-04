import requests
import google.auth
import google.auth.transport.requests
import json
import os

def get_raw_link():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent")
    location = "europe-west1"
    fqn = f"bigquery:{project_id}.retail_syn_data.transactions"
    
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token = credentials.token
    
    parent = f"projects/{project_id}/locations/{location}"
    url = f"https://{location}-datalineage.googleapis.com/v1/{parent}:searchLinks"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    body = {
        "target": {
            "fullyQualifiedName": fqn
        }
    }
    
    response = requests.post(url, headers=headers, json=body)
    print(json.dumps(response.json(), indent=2))

if __name__ == "__main__":
    get_raw_link()
