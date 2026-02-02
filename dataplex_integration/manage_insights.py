import os
import time
from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.protobuf import struct_pb2

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
DATASET_ID = "retail_syn_data"

def create_and_run_scan(table_name, scan_type):
    """Creates and runs a Dataplex Data Scan (PROFILE or DATA_DOCUMENTATION)."""
    client = dataplex_v1.DataScanServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    scan_id = f"{scan_type.lower().replace('_', '-')}-{table_name}"
    
    # Check if scan already exists
    try:
        existing_scan = client.get_data_scan(name=f"{parent}/dataScans/{scan_id}")
        print(f"Scan {scan_id} already exists.")
        scan_name = existing_scan.name
    except Exception:
        # Create new scan
        data_scan = dataplex_v1.DataScan()
        data_scan.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{table_name}"
        data_scan.execution_spec.trigger.on_demand = {}
        data_scan.type_ = scan_type
        
        if scan_type == "DATA_DOCUMENTATION":
            data_scan.data_documentation_spec = {}
        
        try:
            operation = client.create_data_scan(
                parent=parent,
                data_scan=data_scan,
                data_scan_id=scan_id
            )
            print(f"Creating {scan_type} scan for {table_name}...")
            result = operation.result()
            scan_name = result.name
            print(f"{scan_type} scan created: {scan_name}")
        except Exception as e:
            print(f"Failed to create {scan_type} scan for {table_name}: {e}")
            return None

    # Run the scan
    try:
        run_operation = client.run_data_scan(name=scan_name)
        job_id = run_operation.job.id if hasattr(run_operation, 'job') else 'unknown'
        print(f"Started {scan_type} scan for {table_name}. Job ID: {job_id}")
        return scan_id
    except Exception as e:
        print(f"Failed to run {scan_type} scan for {table_name}: {e}")
        return scan_id # Return scan_id anyway as it exists

def publish_scan_results(table_name, scan_id, scan_type):
    """Publishes scan results to BigQuery table by adding labels."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = bq_client.dataset(DATASET_ID).table(table_name)
    table = bq_client.get_table(table_ref)
    
    if scan_type == "DATA_PROFILE":
        prefix = "dataplex-dp-published"
    elif scan_type == "DATA_DOCUMENTATION":
        prefix = "dataplex-data-documentation-published"
    else:
        print(f"Unknown scan type for publishing: {scan_type}")
        return

    labels = table.labels or {}
    labels[f"{prefix}-scan"] = scan_id
    labels[f"{prefix}-project"] = PROJECT_ID
    labels[f"{prefix}-location"] = LOCATION
    
    table.labels = labels
    try:
        bq_client.update_table(table, ["labels"])
        print(f"Published {scan_type} results to {table_name} via labels.")
    except Exception as e:
        print(f"Failed to publish {scan_type} results to {table_name}: {e}")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
    
    tables = ["customers", "products"]
    
    for table in tables:
        print(f"\nProcessing table: {table}")
        
        # 1. Create and Run Data Documentation Scan (For Insights)
        doc_scan_id = create_and_run_scan(table, "DATA_DOCUMENTATION")
        if doc_scan_id:
            publish_scan_results(table, doc_scan_id, "DATA_DOCUMENTATION")
        
        print(f"Finished processing {table}. Scans are running in background.")
