import sys
import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional

# Add adk_integration and dataplex_integration to relative path for plugin execution
PLUGIN_DIR = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(PLUGIN_DIR, '../adk_integration')))
sys.path.append(os.path.abspath(os.path.join(PLUGIN_DIR, '../../dataplex_integration')))

from google.adk.plugins.base_plugin import BasePlugin
from google.cloud import bigquery
from context import get_credentials, get_oauth_token
from lineage_propagation import LineageGraphTraverser, TransformationEnricher, SQLFetcher

logger = logging.getLogger(__name__)

class PolicyTagPlugin(BasePlugin):
    def __init__(self, project_id: str, location: str = "europe-west1"):
        super().__init__(name="policy_tag_plugin")
        self.project_id = project_id
        self.location = location
        self._lineage_traverser = None
        self._sql_fetcher = None

    def _get_credentials(self):
        return get_credentials(self.project_id)

    def _get_bq_client(self):
        creds = self._get_credentials()
        return bigquery.Client(project=self.project_id, credentials=creds)

    def _ensure_initialized(self):
        creds = self._get_credentials()
        token = get_oauth_token()
        
        if not self._lineage_traverser:
            self._lineage_traverser = LineageGraphTraverser(self.project_id, self.location, token=token)
            
        if not self._sql_fetcher:
            self._sql_fetcher = SQLFetcher(self.project_id, self.location, credentials=creds)

    def scan_for_policy_tags(self, dataset_id: str) -> pd.DataFrame:
        """
        Scans a dataset for tables/columns with policy tags.
        """
        self._ensure_initialized()
        client = self._get_bq_client()
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        tables = list(client.list_tables(dataset_ref))
        policy_tags_data = []

        for table_item in tables:
            table_ref = f"{dataset_ref}.{table_item.table_id}"
            try:
                table = client.get_table(table_ref)
                for field in table.schema:
                    if field.policy_tags:
                        policy_tags_data.append({
                            "Table": table_item.table_id,
                            "Column": field.name,
                            "Policy Tags": ", ".join(field.policy_tags.names)
                        })
            except Exception as e:
                logger.error(f"Error accessing {table_ref}: {e}")

        return pd.DataFrame(policy_tags_data)

    def preview_policy_tag_propagation(self, dataset_id: str, target_table: str) -> pd.DataFrame:
        """
        Recommends policy tag propagation based on lineage.
        """
        self._ensure_initialized()
        target_fqn = f"bigquery:{self.project_id}.{dataset_id}.{target_table}"
        client = self._get_bq_client()
        table_ref = f"{self.project_id}.{dataset_id}.{target_table}"
        table = client.get_table(table_ref)
        
        recommendations = []
        
        for field in table.schema:
            # We check even if it already has a policy tag, to see if it matches or needs update
            # But usually we look for missing ones.
            
            logger.info(f"Searching source for column '{field.name}'...")
            # For policy tags, we might only care about direct upstream or a few hops
            lineage = self._lineage_traverser.get_column_lineage(target_fqn, [field.name], depth=0)
            sources = lineage.get(field.name, [])
            
            if not sources:
                continue
                
            for source in sources:
                src_entity = source['source_entity']
                src_col = source['source_column']
                
                try:
                    src_table = client.get_table(src_entity)
                    for src_field in src_table.schema:
                        if src_field.name == src_col and src_field.policy_tags:
                            # Found a source with policy tags
                            
                            # Check for transformation
                            is_straight_pull = (src_col == field.name)
                            logic = None
                            try:
                                sql = self._sql_fetcher.get_transformation_sql(dataset_id, target_table)
                                if sql:
                                    logic = TransformationEnricher.extract_column_logic(sql, field.name)
                                    if logic and logic.strip() != src_col and logic.strip() != f"`{src_col}`":
                                        is_straight_pull = False
                            except Exception as e:
                                logger.debug(f"SQL check failed: {e}")

                            recommendation = "Propagate" if is_straight_pull else "Review Required (Transformed)"
                            
                            # Check if the target field already has this tag
                            src_tag_names = src_field.policy_tags.names
                            if field.policy_tags and set(src_tag_names).issubset(set(field.policy_tags.names)):
                                logger.info(f"Skipping recommendation for {field.name} - tag already applied.")
                                continue

                            recommendations.append({
                                "Target Column": field.name,
                                "Source Table": src_entity,
                                "Source Column": src_col,
                                "Policy Tags": ", ".join(src_tag_names),
                                "Recommendation": recommendation,
                                "Logic": logic or "Straight Pull"
                            })
                except Exception as e:
                    logger.warning(f"Failed to check source {src_entity}: {e}")

        return pd.DataFrame(recommendations)

    def apply_policy_tags(self, dataset_id: str, updates: List[Dict[str, str]]):
        """
        Applies policy tags to specified columns.
        updates: List of dicts with keys 'table', 'column', 'policy_tag'
        """
        self._ensure_initialized()
        client = self._get_bq_client()
        
        for update in updates:
            table_id = update['table']
            col_name = update['column']
            tag_name = update['policy_tag']
            
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            try:
                table = client.get_table(table_ref)
                
                new_schema = []
                found = False
                for field in table.schema:
                    if field.name == col_name:
                        field_dict = field.to_api_repr()
                        field_dict['policyTags'] = {'names': [tag_name]}
                        new_schema.append(bigquery.SchemaField.from_api_repr(field_dict))
                        found = True
                    else:
                        new_schema.append(field)
                
                if found:
                    table.schema = new_schema
                    client.update_table(table, ["schema"])
                    logger.info(f"Successfully applied policy tag to {table_id}.{col_name}")
                else:
                    logger.warning(f"Column {col_name} not found in {table_id}")
            except Exception as e:
                logger.error(f"Failed to apply policy tag to {table_ref}.{col_name}: {e}")
