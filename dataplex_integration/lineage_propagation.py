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

class TransformationEnricher:
    @staticmethod
    def enrich_description(target_col, source_col, source_desc):
        """
        Enriches the description based on transformation patterns.
        """
        explanation = ""
        
        # Pattern matching for common fields
        target_lower = target_col.lower()
        source_lower = source_col.lower()
        
        if any(kw in target_lower for kw in ['amount', 'price', 'cost', 'discount', 'tax']):
            explanation = "Monetary value of the transaction."
        elif any(kw in target_lower for kw in ['date', 'timestamp', 'time']):
            explanation = "Temporal attribute of the event."
        elif any(kw in target_lower for kw in ['category', 'type', 'status']):
            explanation = "Classification or status indicator."
            
        if explanation:
            if source_desc and explanation.lower() in source_desc.lower():
                # Already present, don't repeat
                pass
            else:
                if source_col != target_col:
                    return f"{explanation} Derived from {source_col}. {source_desc or ''}".strip()
                return f"{explanation} {source_desc or ''}".strip()
            
        return source_desc

    @staticmethod
    def check_semantic_mismatch(target_col, source_col):
        """
        Returns a penalty score if the columns are semantically incompatible.
        """
        t = target_col.lower()
        s = source_col.lower()
        
        # ID vs Date/Value mismatch
        is_t_id = 'id' in t or t.endswith('_id')
        is_s_id = 'id' in s or s.endswith('_id')
        
        is_t_date = any(kw in t for kw in ['date', 'time', 'timestamp'])
        is_s_date = any(kw in s for kw in ['date', 'time', 'timestamp'])
        
        if is_t_date and is_s_id:
            return 0.4 # Significant penalty: Date should not map to ID
            
        if is_t_id and not is_s_id:
             # Target is ID but source isn't - suspicious but maybe a rename?
             # Let's check name similarity
             if t.replace("_id", "") in s or s in t.replace("_id", ""):
                 return 1.0
             return 0.6
             
        return 1.0

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

    def _search_links(self, fqn, fields=None, search_type="target"):
        """
        Helper to call Data Lineage API searchLinks.
        search_type: "target" for upstream, "source" for downstream.
        """
        # Try to get token from context first
        try:
            from context import get_oauth_token
            token = get_oauth_token()
        except ImportError:
            token = None

        if not token:
            # Fallback to ADC
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
        
        body = {
            search_type: {
                "fullyQualifiedName": fqn
            }
        }
        if fields:
            body[search_type]["field"] = fields

        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        return response.json().get("links", [])

    def get_column_lineage(self, target_entry_name, target_columns):
        """
        Fetches upstream column lineage for a given target entry.
        """
        logger.info(f"Searching upstream column lineage for {target_entry_name}...")
        column_mappings = {}

        for col in target_columns:
            try:
                links = self._search_links(target_entry_name, [col], "target")
                if not links:
                    continue

                best_match = None
                max_score = -1.0

                for link in links:
                    source = link.get("source", {})
                    source_fqn = source.get("fullyQualifiedName")
                    source_fields = source.get("field", [])
                    
                    if not source_fqn or not source_fields:
                        continue

                    for src_field in source_fields:
                        score = 0.5 
                        if src_field == col: score = 1.0
                        elif src_field.lower() == col.lower(): score = 0.95
                        elif src_field.replace("_", "") == col.replace("_", ""): score = 0.9
                        elif col in src_field or src_field in col: score = 0.8
                        elif len(source_fields) == 1: score = 0.7

                        penalty = TransformationEnricher.check_semantic_mismatch(col, src_field)
                        score = score * penalty

                        if score > max_score:
                            max_score = score
                            best_match = {
                                "source_fqn": source_fqn,
                                "source_entity": source_fqn.split(':')[-1] if ':' in source_fqn else source_fqn,
                                "source_column": src_field,
                                "confidence": round(score, 2),
                                "semantic_penalty": True if penalty < 1.0 else False
                            }
                
                if best_match:
                    column_mappings[col] = best_match

            except Exception as e:
                logger.warning(f"Failed to fetch upstream lineage for column {col}: {e}")
        
        return column_mappings

    def get_downstream_lineage(self, source_entry_name, source_columns):
        """
        Fetches downstream column lineage, optionally using Knowledge Engine insights.
        """
        logger.info(f"Searching downstream lineage for {source_entry_name}...")
        downstream_mappings = {} # Source Col -> List of Targets
        norm_source = self._normalize_fqn(source_entry_name)

        for col in source_columns:
            targets = []
            try:
                # 1. Standard Lineage API
                links = self._search_links(source_entry_name, [col], "source")
                for link in links:
                    target = link.get("target", {})
                    target_fqn = target.get("fullyQualifiedName")
                    target_fields = target.get("field", [])
                    
                    if target_fqn and target_fields:
                        targets.append({
                            "target_fqn": target_fqn,
                            "target_entity": target_fqn.split(':')[-1] if ':' in target_fqn else target_fqn,
                            "target_column": target_fields[0]
                        })

            except Exception as e:
                logger.warning(f"Failed to fetch downstream lineage for column {col}: {e}")
            
            # 2. Check Knowledge Engine Insights
            if self.knowledge_insights:
                for rel in self.knowledge_insights:
                    left_fqn = self._normalize_fqn(rel.get("leftSchemaPaths", {}).get("tableFqn", ""))
                    right_fqn = self._normalize_fqn(rel.get("rightSchemaPaths", {}).get("tableFqn", ""))
                    left_cols = rel.get("leftSchemaPaths", {}).get("paths", [])
                    right_cols = rel.get("rightSchemaPaths", {}).get("paths", [])
                    
                    if left_fqn == norm_source and col in left_cols:
                        if right_cols: 
                            target_col = right_cols[0] 
                            existing = any(t['target_entity'] == right_fqn and t['target_column'] == target_col for t in targets)
                            if not existing:
                                targets.append({
                                    "target_fqn": right_fqn,
                                    "target_entity": right_fqn.split('.')[-1],
                                    "target_column": target_col,
                                    "source": "KNOWLEDGE_ENGINE"
                                })

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
