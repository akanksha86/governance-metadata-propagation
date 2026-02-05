import os
import time
import concurrent.futures
from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
DATASET_ID = "retail_syn_data"

def update_bq_labels(table_name, scan_id, credentials=None):
    """Updates BigQuery table labels to enable Dataplex Insights publishing."""
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    try:
        table = client.get_table(table_ref)
        labels = table.labels or {}
        
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
            table.labels = labels
            client.update_table(table, ["labels"])
            print(f"[{table_name}] Updated labels for BigQuery publishing.")
        else:
            print(f"[{table_name}] Labels already configured for publishing.")
            
    except Exception as e:
        print(f"[{table_name}] Failed to update BQ labels: {e}")

def create_and_start_scan(table_name, credentials=None):
    """Creates (if needed) and starts a Data Documentation scan."""
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    scan_id = f"doc-scan-{table_name.replace('_', '-')}"
    scan_name = f"{parent}/dataScans/{scan_id}"
    
    # 1. Create Scan if missing
    try:
        client.get_data_scan(name=scan_name)
    except NotFound:
        print(f"[{table_name}] Creating Data Documentation scan...")
        data_scan = dataplex_v1.DataScan()
        data_scan.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{table_name}"
        data_scan.execution_spec.trigger.on_demand = {}
        data_scan.type_ = dataplex_v1.DataScanType.DATA_DOCUMENTATION
        data_scan.data_documentation_spec = {}
        
        operation = client.create_data_scan(
            parent=parent,
            data_scan=data_scan,
            data_scan_id=scan_id
        )
        operation.result()
    
    # 2. Configure BigQuery Labels for Publishing
    # This must be done so Dataplex knows where to push the results
    update_bq_labels(table_name, scan_id, credentials=credentials)

    # 3. Run Scan
    print(f"[{table_name}] Starting scan...")
    try:
        run_response = client.run_data_scan(name=scan_name)
        return table_name, run_response.job.name
    except Exception as e:
        print(f"[{table_name}] Failed to start scan: {e}")
        return table_name, None

def wait_for_jobs(jobs_map, credentials=None):
    """Waits for all jobs to complete."""
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)
    pending = list(jobs_map.keys())
    
    while pending:
        print(f"Waiting for {len(pending)} scans to complete...")
        time.sleep(10)
        
        still_pending = []
        for table_name in pending:
            job_name = jobs_map[table_name]
            if not job_name:
                continue
                
            job = client.get_data_scan_job(name=job_name)
            if job.state in [dataplex_v1.DataScanJob.State.SUCCEEDED, dataplex_v1.DataScanJob.State.FAILED, dataplex_v1.DataScanJob.State.CANCELLED]:
                print(f"[{table_name}] Scan finished with state: {job.state.name}")
            else:
                still_pending.append(table_name)
        pending = still_pending

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
        
    tables = ["raw_customers", "raw_products", "raw_orders", "raw_transactions"]
    
    # 1. Trigger Scans in Parallel
    jobs_map = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(create_and_start_scan, table): table for table in tables}
        for future in concurrent.futures.as_completed(futures):
            table, job_name = future.result()
            if job_name:
                jobs_map[table] = job_name
    
    # 2. Wait for completion
    wait_for_jobs(jobs_map)
        
    print("Done.")
