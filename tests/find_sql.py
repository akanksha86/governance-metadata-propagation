from google.cloud import bigquery
import os
import json

def find_transformation_sql(table_id):
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent")
    client = bigquery.Client(project=project_id)
    
    # Query jobs that touch this table
    # We look for jobs where this table was a destination
    query = f"""
    SELECT 
        query,
        job_id,
        creation_time,
        user_email
    FROM `{project_id}.region-europe-west1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE destination_table.table_id = '{table_id}'
    AND statement_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'UPDATE')
    ORDER BY creation_time DESC
    LIMIT 1
    """
    
    print(f"Searching for SQL for table: {table_id}...")
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        
        if results:
            row = results[0]
            print(f"Found Job ID: {row.job_id}")
            print(f"Creation Time: {row.creation_time}")
            print(f"SQL:\n{row.query}")
        else:
            print("No matching BigQuery jobs found in Information Schema.")
    except Exception as e:
        print(f"Error querying Information Schema: {e}")

if __name__ == "__main__":
    find_transformation_sql("transactions")
