import sys
import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional

# Add paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../dataplex_integration')))

from google.adk.plugins.base_plugin import BasePlugin
from google.cloud import bigquery, dataplex_v1
from glossary_management import GlossaryClient
from similarity_engine import SimilarityEngine
from context import get_credentials

logger = logging.getLogger(__name__)

class GlossaryPlugin(BasePlugin):
    def __init__(self, project_id: str, location: str = "europe-west1"):
        super().__init__(name="glossary_plugin")
        self.project_id = project_id
        self.location = location
        self._glossary_client = None
        self._similarity_engine = None
        self._bq_client = None

    def _ensure_initialized(self):
        creds = get_credentials(self.project_id)
        if not self._glossary_client:
            self._glossary_client = GlossaryClient(self.project_id, self.location, credentials=creds)
        if not self._similarity_engine:
            self._similarity_engine = SimilarityEngine()
        if not self._bq_client:
            self._bq_client = bigquery.Client(project=self.project_id, credentials=creds)

    def recommend_terms_for_table(self, dataset_id: str, table_id: str) -> pd.DataFrame:
        """
        Fetches recommendations for all columns in a table.
        """
        self._ensure_initialized()
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self._bq_client.get_table(table_ref)
        
        all_terms = self._glossary_client.get_all_terms()
        if not all_terms:
            logger.warning("No glossary terms found to recommend.")
            return pd.DataFrame()

        recommendations = []
        for field in table.schema:
            col_meta = {
                "name": field.name,
                "description": field.description or "",
                "type": field.field_type
            }
            
            suggestions = self._similarity_engine.get_ranked_suggestions(col_meta, all_terms)
            
            for sug in suggestions:
                recommendations.append({
                    "Column": field.name,
                    "Suggested Term": sug['display_name'],
                    "Confidence": sug['confidence'],
                    "Rationale": f"Lexical: {sug['signals']['lexical']}, Semantic: {sug['signals']['semantic']}",
                    "Term ID": sug['term_name']
                })

        return pd.DataFrame(recommendations)

    def _get_entry_name(self, dataset_id: str, table_id: str):
        entry_id = f"bigquery.googleapis.com/projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}"
        # Harvested entries are in the @bigquery group at the same location as the BQ dataset
        return f"projects/{self.project_id}/locations/{self.location}/entryGroups/@bigquery/entries/{entry_id}"

    def _ensure_aspect_type(self) -> str:
        """Ensures the glossary-mapping aspect type exists at the entry level."""
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        parent = f"projects/{self.project_id}/locations/{self.location}"
        aspect_id = "glossary-mapping"
        aspect_name = f"{parent}/aspectTypes/{aspect_id}"
        
        try:
            client.get_aspect_type(name=aspect_name)
            return aspect_name
        except Exception:
            aspect_type = dataplex_v1.AspectType()
            aspect_type.description = "Entry-level Business Glossary Mappings"
            template = dataplex_v1.AspectType.MetadataTemplate()
            template.name = "mappings"
            template.type = "RECORD"
            
            field = dataplex_v1.AspectType.MetadataTemplate()
            field.name = "data_json"
            field.type = "STRING"
            field.index = 1
            
            template.record_fields.extend([field])
            aspect_type.metadata_template = template
            
            op = client.create_aspect_type(parent=parent, aspect_type_id=aspect_id, aspect_type=aspect_type)
            op.result()
            return aspect_name

    def apply_terms(self, dataset_id: str, table_id: str, updates: List[Dict[str, str]]):
        """
        Applies glossary terms to columns.
        updates: List of {'column': str, 'term_id': str, 'term_display': str}
        """
        self._ensure_initialized()
        
        # 1. Update BigQuery Schema
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self._bq_client.get_table(table_ref)
        schema = list(table.schema)
        
        updated_cols = []
        for up in updates:
            col_name = up['column']
            term_display = up['term_display']
            
            for i, field in enumerate(schema):
                if field.name == col_name:
                    clean_desc = field.description or ""
                    if "Business Glossary:" in clean_desc:
                        clean_desc = clean_desc.split("Business Glossary:")[0].strip()
                    
                    new_desc = f"{clean_desc}\n\nBusiness Glossary: {term_display}".strip()
                    schema[i] = bigquery.SchemaField(
                        name=field.name, field_type=field.field_type,
                        mode=field.mode, description=new_desc, fields=field.fields
                    )
                    updated_cols.append(col_name)
                    break
        
        table.schema = schema
        try:
            self._bq_client.update_table(table, ["schema"])
            logger.info(f"Updated BQ descriptions for {len(updated_cols)} columns.")
        except Exception as e:
            logger.error(f"Failed to update BQ table: {e}")
            raise

        # 2. Update Dataplex Entry Aspect (Entry-level rollup)
        import json
        from google.protobuf import struct_pb2
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        entry_name = self._get_entry_name(dataset_id, table_id)
        aspect_type_name = self._ensure_aspect_type()
        
        try:
            # Map of column -> term_display for storage
            mapping_data = {up['column']: up['term_display'] for up in updates}
            
            # Fetch existing entry to preserve other aspects
            entry = client.get_entry(name=entry_name)
            aspects = dict(entry.aspects)
            
            aspect_key = f"{self.project_id}.{self.location}.glossary-mapping"
            
            data = struct_pb2.Struct()
            data.update({"data_json": json.dumps(mapping_data)})
            
            aspects[aspect_key] = dataplex_v1.Aspect(
                aspect_type=aspect_type_name,
                data=data
            )
            
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects = aspects
            client.update_entry(entry=new_entry, update_mask={"paths": ["aspects"]})
            logger.info(f"Updated Dataplex glossary-mapping aspect for {table_id}.")
        except Exception as e:
            logger.error(f"Failed to update Dataplex entry aspect: {e}")
            raise

    def scan_for_missing_glossary_terms(self, dataset_id: str) -> pd.DataFrame:
        """
        Scans all tables in a dataset for columns missing glossary terms.
        """
        self._ensure_initialized()
        dataset_ref = self._bq_client.dataset(dataset_id)
        tables = self._bq_client.list_tables(dataset_ref)
        
        gaps = []
        for table_item in tables:
            full_table = self._bq_client.get_table(table_item.reference)
            for field in full_table.schema:
                desc = field.description or ""
                if "Business Glossary:" not in desc:
                    gaps.append({
                        "Table": table_item.table_id,
                        "Column": field.name,
                        "Type": field.field_type
                    })
        
        return pd.DataFrame(gaps)

    def apply_term_to_column(self, dataset_id: str, table_id: str, column_name: str, term_id: str):
        # Deprecated
        pass
