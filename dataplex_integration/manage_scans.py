import os
from google.cloud import dataplex_v1
from google.cloud.dataplex_v1 import DataScanServiceClient
from google.api_core import exceptions

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1" # For Scans, must be a supported region
DATASET_ID = "retail_syn_data"

def create_dq_scan(table_name):
    client = dataplex_v1.DataScanServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    scan_id = f"dq-scan-{table_name}"
    
    # Define rules based on table
    rules = []
    if table_name == "customers":
        rules = [
            dataplex_v1.DataQualityRule(column="email", non_null_expectation={}, dimension="COMPLETENESS"),
            dataplex_v1.DataQualityRule(column="phone", regex_expectation={"regex": r"^\+?[0-9\s\-()]+$"}, dimension="VALIDITY")
        ]
    elif table_name == "products":
        rules = [
            dataplex_v1.DataQualityRule(column="price", non_null_expectation={}, dimension="COMPLETENESS"),
            dataplex_v1.DataQualityRule(column="price", range_expectation={"min_value": "0", "max_value": "1000"}, dimension="VALIDITY")
        ]
    
    if not rules:
        print(f"No DQ rules defined for {table_name}, skipping.")
        return
    
    data_quality_spec = dataplex_v1.DataQualitySpec(rules=rules)
    
    data_scan = dataplex_v1.DataScan()
    data_scan.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{table_name}"
    data_scan.data_quality_spec = data_quality_spec
    
    # Results are automatically published to Dataplex Catalog.
    
    try:
        operation = client.create_data_scan(
            parent=parent,
            data_scan=data_scan,
            data_scan_id=scan_id
        )
        print(f"Creating DQ scan for {table_name}...")
        operation.result()
        print(f"DQ scan created for {table_name}")
    except exceptions.AlreadyExists:
        print(f"DQ scan {scan_id} already exists, skipping creation.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"DQ scan failed for {table_name}: {e}")


def create_profiling_scan(table_name):
    client = dataplex_v1.DataScanServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    scan_id = f"profile-{table_name}"
    
    data_scan = dataplex_v1.DataScan()
    data_scan.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{table_name}"
    data_scan.data_profile_spec = dataplex_v1.DataProfileSpec()
    data_scan.type_ = "DATA_PROFILE"
    
    # Results are automatically published to Dataplex Catalog.
    # For Data Profiling, BigQuery export is also available in some regions/versions.
    # data_scan.data_profile_spec.post_scan_actions.bigquery_export.results_table = ...
    
    try:
        operation = client.create_data_scan(
            parent=parent,
            data_scan=data_scan,
            data_scan_id=scan_id
        )
        print(f"Creating Profiling scan for {table_name}...")
        operation.result()
        print(f"Profiling scan created for {table_name}")
    except exceptions.AlreadyExists:
        print(f"Profiling scan {scan_id} already exists, skipping creation.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Profiling scan failed for {table_name}: {e}")

def run_scan(scan_id):
    client = dataplex_v1.DataScanServiceClient()
    name = f"projects/{PROJECT_ID}/locations/{LOCATION}/dataScans/{scan_id}"
    try:
        client.run_data_scan(name=name)
        print(f"Triggered scan {scan_id}")
    except Exception as e:
        print(f"Failed to run scan {scan_id}: {e}")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
    
    # First pass: Create profiling scans for all tables
    for table in ["customers", "products"]:
        create_profiling_scan(table)
        run_scan(f"profile-{table}")
    
    # Second pass: Create DQ scans for all tables
    for table in ["customers", "products"]:
        create_dq_scan(table)
        run_scan(f"dq-scan-{table}")
