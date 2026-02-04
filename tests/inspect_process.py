import requests
import google.auth
import google.auth.transport.requests
import json
import os

def get_process_details():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent")
    location = "europe-west1"
    
    # Selected a process ID from the previous dump
    process_id = "dec7bf3002ccadb7ee69d2f20f39c955" 
    
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token = credentials.token
    
    url = f"https://{location}-datalineage.googleapis.com/v1/projects/{project_id}/locations/{location}/processes/{process_id}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    else:
        print(response.text)

if __name__ == "__main__":
    get_process_details()
