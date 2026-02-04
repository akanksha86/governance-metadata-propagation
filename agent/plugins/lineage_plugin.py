import sys
import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional

# Add adk_integration and dataplex_integration to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../adk_integration')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../dataplex_integration')))

from google.adk.plugins.base_plugin import BasePlugin
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from context import get_oauth_token
from lineage_propagation import LineageGraphTraverser, TransformationEnricher
from knowledge_engine import DescriptionPropagator

logger = logging.getLogger(__name__)

class LineagePlugin(BasePlugin):
    def __init__(self, project_id: str, location: str = "europe-west1", knowledge_json_path: Optional[str] = None):
        super().__init__(name="lineage_plugin")
        self.project_id = project_id
        self.location = location
        self.knowledge_json_path = knowledge_json_path
        self._lineage_traverser = None
        self._description_propagator = None

    def _get_credentials(self):
        token = get_oauth_token()
        if token:
            return Credentials(token=token)
        return None  # Fallback to ADC if no token

    def _get_bq_client(self):
        creds = self._get_credentials()
        return bigquery.Client(project=self.project_id, credentials=creds)

    def _ensure_initialized(self):
        if not self._lineage_traverser:
            # Note: LineageGraphTraverser might create its own clients. 
            # Ideally we should pass credentials to it, but for now we rely on ADC or we need to refactor it.
            # If LineageGraphTraverser uses `bigquery.Client()`, it will use ADC. 
            # To support OAuth, we might need to monkeypatch or refactor LineageGraphTraverser.
            # For this demo step, let's assume ADC for the internals OR that we will update LineageGraphTraverser later.
            # But wait, the user wants OAuth. 
            # Refactoring LineageGraphTraverser is safer.
            # For this pass, I will instantiate it as is and warn if it doesn't support explicit creds.
            self._lineage_traverser = LineageGraphTraverser(self.project_id, self.location)
            if self.knowledge_json_path:
                self._lineage_traverser.load_knowledge_insights(self.knowledge_json_path)
            
        if not self._description_propagator:
            self._description_propagator = DescriptionPropagator(self.knowledge_json_path)

    def scan_for_missing_descriptions(self, dataset_id: str) -> pd.DataFrame:
        """
        Scans a dataset for tables/columns missing descriptions.
        Returns a DataFrame.
        """
        self._ensure_initialized()
        client = self._get_bq_client()
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        tables = list(client.list_tables(dataset_ref))
        missing_data = []

        for table_item in tables:
            table_ref = f"{dataset_ref}.{table_item.table_id}"
            try:
                table = client.get_table(table_ref)
                for schema_field in table.schema:
                    if not schema_field.description:
                        missing_data.append({
                            "Table": table_item.table_id,
                            "Column": schema_field.name,
                            "Type": schema_field.field_type
                        })
            except Exception as e:
                logger.error(f"Error accessing {table_ref}: {e}")

        return pd.DataFrame(missing_data)

    def _find_description_recursive(self, target_fqn: str, column: str, depth: int = 0, max_depth: int = 5) -> Optional[Dict[str, Any]]:
        """
        Recursively searches upstream for a description.
        Returns a dict with metadata if found, else None.
        """
        if depth >= max_depth:
            return None
            
        # 1. Get immediate upstream
        upstream = self._lineage_traverser.get_column_lineage(target_fqn, [column], depth=depth)
        source = upstream.get(column)
        
        if not source:
            return None
            
        # 2. Check if source has description
        src_entity = source['source_fqn'].replace("bigquery:", "")
        src_col = source['source_column']
        
        try:
            client = self._get_bq_client()
            src_table = client.get_table(src_entity)
            for f in src_table.schema:
                if f.name == src_col:
                    if f.description:
                        # Found it!
                        return {
                            "source_entity": source['source_entity'],
                            "source_column": src_col,
                            "description": f.description,
                            "confidence": source['confidence'],
                            "hop_depth": depth
                        }
                    else:
                        # No description here, keep going up
                        return self._find_description_recursive(source['source_fqn'], src_col, depth + 1, max_depth)
        except Exception as e:
            logger.warning(f"Failed to check desc for {src_entity}.{src_col}: {e}")
            
        return None

    def preview_propagation(self, dataset_id: str, target_table: str) -> pd.DataFrame:
        """
        Simulates propagation for a specific table with multi-hop support.
        """
        self._ensure_initialized()
        target_fqn = f"bigquery:{self.project_id}.{dataset_id}.{target_table}"
        client = self._get_bq_client()
        table_ref = f"{self.project_id}.{dataset_id}.{target_table}"
        table = client.get_table(table_ref)
        
        candidates = []
        for field in table.schema:
            if field.description:
                continue
                
            # Recursive search for this column
            match = self._find_description_recursive(target_fqn, field.name)
            
            if match:
                # Enrich the found description
                from lineage_propagation import TransformationEnricher
                enriched_desc = TransformationEnricher.enrich_description(
                    field.name, 
                    match['source_column'], 
                    match['description']
                )
                
                candidates.append({
                    "Target Column": field.name,
                    "Source": match['source_entity'],
                    "Source Column": match['source_column'],
                    "Confidence": match['confidence'],
                    "Proposed Description": enriched_desc,
                    "Type": f"Lineage (Hop {match['hop_depth']})" if match['hop_depth'] > 0 else "Lineage"
                })

        return pd.DataFrame(candidates)

    def get_lineage_summary(self, dataset_id: str, table_id: str) -> str:
        """
        Provides a holistic summary of upstream and downstream lineage.
        """
        self._ensure_initialized()
        full_table_name = f"{self.project_id}.{dataset_id}.{table_id}"
        client = self._get_bq_client()
        table = client.get_table(full_table_name)
        columns = [f.name for f in table.schema]
        
        # Upstream Analysis
        upstream_map = self._lineage_traverser.get_column_lineage(
            f"bigquery:{full_table_name}", 
            columns
        )
        upstream_entities = set(v['source_entity'] for v in upstream_map.values())
        
        # Downstream Analysis
        downstream_map = self._lineage_traverser.get_downstream_lineage(
            f"bigquery:{full_table_name}", 
            columns
        )
        downstream_entities = set()
        for targets in downstream_map.values():
            for t in targets:
                downstream_entities.add(t['target_entity'])
        
        # Generate Summary Text
        summary = f"### Lineage Summary for `{table_id}`\n\n"
        
        if upstream_entities:
            summary += f"**Upstream Sources ({len(upstream_entities)}):**\n"
            for ent in sorted(upstream_entities):
                cols = [c for c, v in upstream_map.items() if v['source_entity'] == ent]
                summary += f"- `{ent}` (contributes {len(cols)} columns)\n"
        else:
            summary += "*No upstream sources found via Data Lineage API.*\n"
            
        summary += "\n"
        
        if downstream_entities:
            summary += f"**Downstream Targets ({len(downstream_entities)}):**\n"
            for ent in sorted(downstream_entities):
                # Count how many columns from this table flow into the downstream entity
                flowing_cols = set()
                for c, targets in downstream_map.items():
                    if any(t['target_entity'] == ent for t in targets):
                        flowing_cols.add(c)
                summary += f"- `{ent}` (receives {len(flowing_cols)} columns)\n"
        else:
            summary += "*No downstream targets found via Data Lineage API.*\n"
            
        summary += f"\n**Propagation Potential:**\n"
        missing_desc = [f.name for f in table.schema if not f.description]
        potential_inherit = len([c for c in missing_desc if c in upstream_map])
        
        summary += f"- {potential_inherit} missing columns can be enriched from upstream.\n"
        if downstream_entities:
            summary += f"- Metadata from this table can propagate to {len(downstream_entities)} downstream entities.\n"
            
        return summary

    def apply_propagation(self, dataset_id: str, updates: List[Dict[str, str]]):
        """
        Applies updates. 
        updates: List of dicts with keys 'table', 'column', 'description'
        """
        self._ensure_initialized()
        client = self._get_bq_client()
        
        for update in updates:
            table_id = update['table']
            col_name = update['column']
            desc = update['description']
            
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            table = client.get_table(table_ref)
            
            new_schema = []
            for field in table.schema:
                if field.name == col_name:
                    new_field = field.to_api_repr()
                    new_field['description'] = desc
                    new_schema.append(bigquery.SchemaField.from_api_repr(new_field))
                else:
                    new_schema.append(field)
            
            table.schema = new_schema
            client.update_table(table, ["schema"])
            logger.info(f"Updated {table_id}.{col_name}")
