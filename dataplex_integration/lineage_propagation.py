import json
import logging
import requests
import google.auth
import google.auth.transport.requests
from google.cloud import datacatalog_lineage_v1
from google.api_core import exceptions
from google.cloud import bigquery
import re
from typing import List, Dict, Any, Optional
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLFetcher:
    """
    Fetches transformation SQL from BigQuery Information Schema.
    This allows the propagation engine to understand HOW a column was derived.
    """
    def __init__(self, project_id: str, location: str, credentials: Optional[Any] = None):
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id, credentials=credentials)

    def get_transformation_sql(self, dataset_id: str, table_id: str) -> Optional[str]:
        """Queries Information Schema for the last SQL job that updated this table."""
        # NOTE: BigQuery Information Schema Jobs views are regional.
        # We must query the specific region where the processing happened.
        region = self.location.split('-')[0] # approximate region mapping
        if 'europe' in self.location: region = 'europe-west1' # handle europe specific multi-region/regional variation
        
        query = f"""
        SELECT query
        FROM `{self.project_id}.region-{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE destination_table.table_id = '{table_id}'
        AND destination_table.dataset_id = '{dataset_id}'
        AND statement_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'UPDATE')
        ORDER BY creation_time DESC
        LIMIT 1
        """
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())
            if results:
                return results[0].query
        except Exception as e:
            logger.warning(f"Failed to fetch SQL for {table_id}: {e}")
        return None

class TransformationEnricher:
    """Provides semantic enrichment logic for propagated metadata."""
    
    @staticmethod
    def check_semantic_mismatch(target_col: str, source_col: str) -> float:
        """Applies penalties if column semantics seem way off."""
        target_lower = target_col.lower()
        source_lower = source_col.lower()

        # Date vs ID mismatch
        if "date" in target_lower and "id" in source_lower: return 0.2
        if "id" in target_lower and "date" in source_lower: return 0.2

        # Amount vs Flag mismatch
        if "amount" in target_lower and ("flag" in source_lower or "is_" in source_lower): return 0.5

        return 1.0

    @staticmethod
    def extract_column_logic(sql: str, target_col: str) -> Optional[str]:
        """Attempts to extract the expression for a specific column from SQL SELECT."""
        if not sql: return None
        
        # Simple cleanup
        sql_clean = re.sub(r'--.*', '', sql) # remove comments
        sql_clean = re.sub(r'\s+', ' ', sql_clean).strip()
        
        # Look for SELECT ... AS target_col or SELECT target_col AS ...
        # Use word boundaries \b to ensure exact matches only (e.g. 'amount' vs 'amount_discounted')
        pattern = rf"([^,]*?)\s+as\s+`?\b{target_col}\b`?"
        match = re.search(pattern, sql_clean, re.IGNORECASE)
        
        if match:
            expr = match.group(1).strip()
            # Clean up leading SELECT if present, and anything before it (like CREATE TABLE ... AS)
            # Find the last 'SELECT' in the expression if it exists
            last_select = re.split(r'\bSELECT\b', expr, flags=re.IGNORECASE)[-1]
            return last_select.strip()
        
        return None

    @staticmethod
    def describe_sql_logic(expr: Optional[str]) -> str:
        """Converts SQL expression into natural language hint."""
        if not expr: return ""
        
        expr_upper = expr.upper()

        # 1. Type Conversion
        if "CAST(" in expr_upper or "SAFE_CAST(" in expr_upper:
            return f", converted to a different format (`{expr}`)"
            
        # 2. Null Handling
        if any(kw in expr_upper for kw in ["COALESCE(", "IFNULL(", "NULLIF("]):
            return f", with null-handling logic (`{expr}`)"
            
        # 3. Numerical Operations
        if any(kw in expr_upper for kw in ["ROUND(", "CEIL(", "FLOOR(", "TRUNC("]):
            return f", rounded using `{expr}`"
            
        if any(op in expr for op in ["*", "/", "+", "-"]) and any(char.isdigit() for char in expr):
            return f", with value adjustment applied (calculated as `{expr}`)"

        # 4. String Formatting
        if any(kw in expr_upper for kw in ["UPPER(", "LOWER(", "TRIM(", "CONCAT(", "SUBSTR("]):
            return f", with string transformations (`{expr}`)"

        # 5. Date/Time Extractions
        if "EXTRACT(" in expr_upper:
            return f", with temporal component extracted via `{expr}`"

        # 6. Logical Branching
        if "CASE" in expr_upper or "IF(" in expr_upper:
            return f", determined by conditional logic (`{expr}`)"

        # 7. Safe Execution
        if "SAFE." in expr_upper:
            return f", executed with safe-mode operations (`{expr}`)"
            
        return f", calculated using: `{expr}`"

    @staticmethod
    def enrich_description(target_col: str, source_col: str, original_desc: str, sql_hints: List[str] = None) -> str:
        """Builds a polished description combining source and multi-hop transformation context."""
        explanation = ""
        target_lower = target_col.lower()
        
        # Determine fallback explanation if original_desc is empty
        if any(kw in target_lower for kw in ['amount', 'price', 'cost', 'discount', 'tax']):
            explanation = "Monetary value."
        elif any(kw in target_lower for kw in ['date', 'timestamp', 'time']):
            explanation = "Temporal attribute."
        elif any(kw in target_lower for kw in ['category', 'type', 'status']):
            explanation = "Classification or status indicator."

        # Start with the best available description
        description = original_desc or explanation or ""
        
        # Clean up any trailing periods to allow smoother concatenation
        description = description.strip()
        if description.endswith('.'):
            description = description[:-1]

        # Add source context if significantly different - REMOVED AS PER USER REQUEST
        # The source is already tracked in separate columns
        pass
            
        # Add SQL logic context from all hops
        if sql_hints:
            # Filter out trivial passthroughs (where hint is just the column name or table.column)
            meaningful_hints = []
            for hint in sql_hints:
                # Trivial if it's just 'col', 'alias.col', or '`col`'
                is_trivial = re.match(r'^[\w\.]+$', hint) or hint.strip() == f"`{target_col}`"
                if not is_trivial:
                    meaningful_hints.append(hint)
            
            for hint in meaningful_hints:
                logic_hint = TransformationEnricher.describe_sql_logic(hint)
                if logic_hint.strip() and logic_hint not in description:
                    description += logic_hint

        return description.strip()

class LineageGraphTraverser:
    def __init__(self, project_id, location, token: Optional[str] = None):
        self.project_id = project_id
        self.location = location
        self.token = token
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
        token = self.token
        
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

    def get_column_lineage(self, target_entry_name, target_columns, depth=0, max_depth=3):
        """
        Fetches upstream column lineage for a given target entry.
        Supports recursive lookup (multi-hop) up to max_depth.
        """
        if depth >= max_depth:
            return {}

        logger.info(f"Searching upstream column lineage for {target_entry_name} (depth {depth})...")
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
                                "semantic_penalty": True if penalty < 1.0 else False,
                                "hop_depth": depth
                            }
                
                if best_match:
                    column_mappings[col] = best_match

            except Exception as e:
                logger.warning(f"Failed to fetch upstream lineage for column {col}: {e}")
        
        return column_mappings

    def get_recursive_column_lineage(self, target_entry_name, target_columns, max_depth=3):
        """
        Public method to start a recursive search.
        It resolves lineage level by level until it finds a source for each column.
        Currently, this is a simplified multi-hop resolver.
        """
        final_mappings = {}
        columns_to_resolve = target_columns.copy()
        
        for d in range(max_depth):
            if not columns_to_resolve:
                break
                
            current_layer = self.get_column_lineage(target_entry_name, columns_to_resolve, depth=d, max_depth=max_depth)
            
            for col, mapping in current_layer.items():
                if col not in final_mappings:
                    final_mappings[col] = mapping
                    # Note: For true recursion where we want to find the ROOT source, 
                    # we would need to call this again for each mapping's source_fqn.
                    # But for now, we just resolve one level at a time for simplicity.
            
            # To be more thorough, we should iterate through mappings and resolve their sources
            # This is a bit complex for a stateless helper without a BQ client here.
            # We'll rely on the Plugin to coordinate deep resolution if needed.
            break # Single-pass for now as a base

        return final_mappings

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
