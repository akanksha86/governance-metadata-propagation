import os
from typing import Optional, Any
import time
import json
from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.protobuf.json_format import MessageToDict

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
DATASET_ID = "retail_syn_data"

class DescriptionPropagator:
    """Helper class to load and serve Dataset Insights."""
    def __init__(self, json_path=None):
        self.json_path = json_path
        self.knowledge_json = {}
        if json_path:
            self._load_insights()

    def _load_insights(self):
        try:
            with open(self.json_path, 'r') as f:
                self.knowledge_json = json.load(f)
                # Normalize structure if needed, or propagate_metadata will handle it.
                # propagate_metadata expects 'relationships' key based on my previous code,
                # BUT lineage_propagation expects 'datasetResult' -> 'schemaRelationships'.
                # I should align them.
                
                # If using the 'API Structure' (datasetResult), let's map it to what propagate_metadata might want 
                # OR update propagate_metadata to use the raw API structure too.
                # For now, I'll just expose the raw json.
        except Exception as e:
            print(f"Failed to load insights from {self.json_path}: {e}")

def update_bq_dataset_labels(dataset_id, scan_id, credentials: Optional[Any] = None):
    """Updates BigQuery dataset labels to enable Dataplex Insights publishing."""
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_ref)
        labels = dataset.labels or {}
        
        # Labels required for publishing scan results to BigQuery
        updates = {
            "dataplex-data-documentation-published-scan": scan_id,
            "dataplex-data-documentation-published-project": PROJECT_ID,
            "dataplex-data-documentation-published-location": LOCATION
        }
        
        # Only update if changes are needed
        needs_update = False
        for k, v in updates.items():
            if labels.get(k) != v:
                labels[k] = v
                needs_update = True
                
        if needs_update:
            dataset.labels = labels
            client.update_dataset(dataset, ["labels"])
            print(f"[{dataset_id}] Updated labels for BigQuery publishing.")
        else:
            print(f"[{dataset_id}] Labels already configured for publishing.")
            
    except Exception as e:
        print(f"[{dataset_id}] Failed to update BQ dataset labels: {e}")

def create_and_start_dataset_scan(credentials: Optional[Any] = None):
    """Creates (if needed) and starts a Dataset-level Data Documentation scan."""
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    # Dataset scan ID
    scan_id = f"doc-scan-dataset-{DATASET_ID.replace('_', '-')}"
    scan_name = f"{parent}/dataScans/{scan_id}"
    
    # 1. Create Scan if missing
    try:
        client.get_data_scan(name=scan_name)
    except NotFound:
        print(f"[{DATASET_ID}] Creating Dataset Data Documentation scan...")
        data_scan = dataplex_v1.DataScan()
        # Pointing to the Dataset resource, not a specific table
        data_scan.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}"
        data_scan.execution_spec.trigger.on_demand = {}
        data_scan.type_ = dataplex_v1.DataScanType.DATA_DOCUMENTATION
        data_scan.data_documentation_spec = {}
        
        operation = client.create_data_scan(
            parent=parent,
            data_scan=data_scan,
            data_scan_id=scan_id
        )
        operation.result()
    
    # 2. Configure BigQuery Dataset Labels for Publishing
    update_bq_dataset_labels(DATASET_ID, scan_id, credentials=credentials)

    # 3. Run Scan
    print(f"[{DATASET_ID}] Starting scan...")
    try:
        run_response = client.run_data_scan(name=scan_name)
        return run_response.job.name
    except Exception as e:
        print(f"[{DATASET_ID}] Failed to start scan: {e}")
        return None

def extract_and_save_insights(job, output_file="knowledge_insights.json"):
    """Extracts Data Documentation results (insights) and saves to JSON."""
    try:
        # Convert Protobuf to Dict
        job_dict = MessageToDict(job._pb)
        
        # Navigate to dataDocumentationResult
        result = job_dict.get("dataDocumentationResult", {})
        
        if result:
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"[{DATASET_ID}] Insights saved to {output_file}")
            
            # Print summary of relationships found
            relationships = result.get("datasetResult", {}).get("schemaRelationships", [])
            print(f"[{DATASET_ID}] Found {len(relationships)} schema relationships.")
        else:
            print(f"[{DATASET_ID}] No Data Documentation results found in job.")
            print(f"DEBUG: Job Dict Keys: {job_dict.keys()}")
            
    except Exception as e:
        print(f"[{DATASET_ID}] Failed to save insights: {e}")

def wait_for_job(job_name, credentials: Optional[Any] = None):
    """Waits for the job to complete and returns the full job object."""
    if not job_name:
        return None
        
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)
    print(f"Waiting for scan job {job_name} to complete...")
    
    while True:
        # Request FULL view to get results
        request = dataplex_v1.GetDataScanJobRequest(
            name=job_name,
            view=dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL
        )
        job = client.get_data_scan_job(request=request)
        
        if job.state in [dataplex_v1.DataScanJob.State.SUCCEEDED, dataplex_v1.DataScanJob.State.FAILED, dataplex_v1.DataScanJob.State.CANCELLED]:
            print(f"[{DATASET_ID}] Scan finished with state: {job.state.name}")
            return job
        time.sleep(10)

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
        
    job_name = create_and_start_dataset_scan()
    job = wait_for_job(job_name)
    
    # if job and job.state == dataplex_v1.DataScanJob.State.SUCCEEDED:
    #     extract_and_save_insights(job)
        
    print("Done.")
