from google.cloud import datacatalog_lineage_v1
from google.api_core import exceptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LineageGraphTraverser:
    def __init__(self, project_id, location):
        self.project_id = project_id
        self.location = location
        self.client = datacatalog_lineage_v1.LineageClient()

    def get_column_lineage(self, target_entry_name, target_columns):
        """
        Fetches upstream column lineage for a given target entry (BigQuery table) and specific columns.
        Returns a dictionary mapping target columns to upstream (source_table, source_column) tuples.
        """
        import google.auth
        import google.auth.transport.requests
        import requests
        import json

        logger.info(f"Searching column lineage for {target_entry_name}...")
        
        credentials, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        token = credentials.token

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8"
        }

        parent = f"projects/{self.project_id}/locations/{self.location}"
        url = f"https://{self.location}-datalineage.googleapis.com/v1/{parent}:searchLinks"
        
        column_mappings = {}

        for col in target_columns:
            # The API currently supports one column at a time for CLL
            body = {
                "target": {
                    "fullyQualifiedName": target_entry_name,
                    "field": [col]
                }
            }
            
            try:
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
                
                # Parse links
                for link in data.get("links", []):
                    source = link.get("source", {})
                    source_fqn = source.get("fullyQualifiedName")
                    source_fields = source.get("field", [])
                    
                    # We expect source_fields to have the upstream column
                    if source_fqn and source_fields:
                        # Assuming 1:1 for now as per API current limitation of single element array
                        upstream_col = source_fields[0]
                        
                        # We map Target Col -> (Source Table, Source Col)
                        # Source FQN is usually "bigquery:project.dataset.table" or similar
                        # We might need to parse it to just "project.dataset.table" or keep as is.
                        # For `propagate_metadata.py`, we need strictly table name for Knowledge Engine lookup if needed, 
                        # but unique ID is better.
                        
                        # Store the first valid upstream found (or collect all?)
                        # For now, store first.
                        column_mappings[col] = {
                            "source_fqn": source_fqn, # full "bigquery:..."
                            "source_entity": source_fqn.split(':')[-1] if ':' in source_fqn else source_fqn, # simple name
                            "source_column": upstream_col
                        }
                        logger.info(f"Lineage found: {col} <- {source_fqn}.{upstream_col}")
                        break # Found a source for this column

            except Exception as e:
                logger.warning(f"Failed to fetch lineage for column {col}: {e}")
        
        return column_mappings

    def get_downstream_lineage(self, source_entry_name, source_columns):
        """
        Fetches downstream column lineage for a given source entry (BigQuery table) and specific columns.
        Returns a dictionary mapping source columns to list of (target_fqn, target_column) tuples.
        """
        import google.auth
        import google.auth.transport.requests
        import requests
        
        logger.info(f"Searching downstream lineage for {source_entry_name}...")
        
        credentials, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        token = credentials.token

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8"
        }

        parent = f"projects/{self.project_id}/locations/{self.location}"
        url = f"https://{self.location}-datalineage.googleapis.com/v1/{parent}:searchLinks"
        
        downstream_mappings = {} # Source Col -> List of Targets

        for col in source_columns:
            # Search for links where THIS column is the SOURCE
            body = {
                "source": {
                    "fullyQualifiedName": source_entry_name,
                    "field": [col]
                }
            }
            
            try:
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
                
                targets = []
                for link in data.get("links", []):
                    target = link.get("target", {})
                    target_fqn = target.get("fullyQualifiedName")
                    target_fields = target.get("field", [])
                    
                    if target_fqn and target_fields:
                        target_col = target_fields[0]
                        targets.append({
                            "target_fqn": target_fqn,
                            "target_table": target_fqn.split(':')[-1] if ':' in target_fqn else target_fqn,
                            "target_column": target_col
                        })
                        logger.info(f"Downstream found: {col} -> {target_fqn}.{target_col}")
                
                if targets:
                    downstream_mappings[col] = targets

            except Exception as e:
                logger.warning(f"Failed to fetch downstream lineage for column {col}: {e}")
        
        return downstream_mappings

class DerivationIdentifier:
    @staticmethod
    def identify_pattern(source_col, target_col, logic=None):
        """
        Identifies if it's DIRECT_COPY, RENAME, or TRANSFORM.
        """
        if logic:
            return "TRANSFORM"
        
        if source_col == target_col:
            return "DIRECT_COPY"
        
        # Heuristic: if names are similar?
        return "RENAME"
