import json
import logging
import requests
import google.auth
import google.auth.transport.requests
from google.cloud import datacatalog_lineage_v1
from google.api_core import exceptions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LineageGraphTraverser:
    def __init__(self, project_id, location):
        self.project_id = project_id
        self.location = location
        self.client = datacatalog_lineage_v1.LineageClient()
        self.knowledge_insights = []

    def load_knowledge_insights(self, json_path):
        """Loads Knowledge Engine insights (schema relationships) from JSON."""
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                # Navigate to schemaRelationships
                # data -> datasetResult -> schemaRelationships
                self.knowledge_insights = data.get("datasetResult", {}).get("schemaRelationships", [])
                logger.info(f"Loaded {len(self.knowledge_insights)} schema relationships from {json_path}")
        except FileNotFoundError:
            logger.warning(f"Insights file {json_path} not found. Skipping.")
        except Exception as e:
            logger.warning(f"Failed to load insights from {json_path}: {e}")

    def _normalize_fqn(self, fqn):
        """
        Normalizes various FQN formats to 'project.dataset.table'.
        """
        if not fqn:
            return ""
        if fqn.startswith("//bigquery.googleapis.com/"):
            # //bigquery.googleapis.com/projects/P/datasets/D/tables/T
            parts = fqn.split("/")
            # parts indices: 4=project, 6=dataset, 8=table
            if len(parts) >= 9:
                return f"{parts[4]}.{parts[6]}.{parts[8]}"
        elif fqn.startswith("bigquery:"):
            return fqn.replace("bigquery:", "")
        return fqn

    def get_column_lineage(self, target_entry_name, target_columns):
        """
        Fetches upstream column lineage for a given target entry.
        """
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
        
        # NOTE: Using v1 searchLinks
        url = f"https://{self.location}-datalineage.googleapis.com/v1/{parent}:searchLinks"
        
        column_mappings = {}

        for col in target_columns:
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
                found_upstream = False
                for link in data.get("links", []):
                    source = link.get("source", {})
                    source_fqn = source.get("fullyQualifiedName")
                    source_fields = source.get("field", [])
                    
                    if source_fqn and source_fields:
                        upstream_col = source_fields[0]
                        column_mappings[col] = {
                            "source_fqn": source_fqn,
                            "source_entity": source_fqn.split(':')[-1] if ':' in source_fqn else source_fqn,
                            "source_column": upstream_col
                        }
                        logger.info(f"Lineage found: {col} <- {source_fqn}.{upstream_col}")
                        found_upstream = True
                        break 
                
                if not found_upstream:
                    logger.debug(f"No upstream found for {col} via API.")

            except Exception as e:
                logger.warning(f"Failed to fetch lineage for column {col}: {e}")
        
        return column_mappings

    def get_downstream_lineage(self, source_entry_name, source_columns):
        """
        Fetches downstream column lineage, optionally using Knowledge Engine insights.
        """
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

        # Normalize source entry for matching
        norm_source = self._normalize_fqn(source_entry_name)

        for col in source_columns:
            # 1. Standard Lineage API
            targets = []
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
                        logger.info(f"Downstream found (API): {col} -> {target_fqn}.{target_col}")

            except Exception as e:
                logger.warning(f"Failed to fetch downstream lineage for column {col}: {e}")
            
            # 2. Check Knowledge Engine Insights
            if self.knowledge_insights:
                for rel in self.knowledge_insights:
                    # check left -> right
                    left_fqn = self._normalize_fqn(rel.get("leftSchemaPaths", {}).get("tableFqn", ""))
                    right_fqn = self._normalize_fqn(rel.get("rightSchemaPaths", {}).get("tableFqn", ""))
                    
                    left_cols = rel.get("leftSchemaPaths", {}).get("paths", [])
                    right_cols = rel.get("rightSchemaPaths", {}).get("paths", [])
                    
                    # If Source is Left, Target is Right
                    if left_fqn == norm_source and col in left_cols:
                        # Find corresponding column index? usually just one matching pair or cross join
                        # Assuming index 0 for now if lists are matched, or all to all?
                        # Relationships usually mean "Join ON left.col = right.col" so 1:1 usually
                        
                        if right_cols: 
                            # Simplification: Map index 0 to index 0
                            # Real extraction might need more logic if multiple cols
                            target_col = right_cols[0] 
                            
                            # Avoid duplicates
                            existing = any(t['target_table'] == right_fqn and t['target_column'] == target_col for t in targets)
                            if not existing:
                                targets.append({
                                    "target_fqn": right_fqn, # Use normalized as FQN or reconstruct?
                                    "target_table": right_fqn,
                                    "target_column": target_col,
                                    "source": "KNOWLEDGE_ENGINE"
                                })
                                logger.info(f"Downstream found (KnowledgeEngine): {col} -> {right_fqn}.{target_col}")

            if targets:
                downstream_mappings[col] = targets
        
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
        
        return "RENAME"
