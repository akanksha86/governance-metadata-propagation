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
from lineage_propagation import LineageGraphTraverser

logger = logging.getLogger(__name__)

class GlossaryPlugin(BasePlugin):
    def __init__(self, project_id: str, location: str = "europe-west1"):
        super().__init__(name="glossary_plugin")
        self.project_id = project_id
        self.location = location
        self._glossary_client = None
        self._similarity_engine = None
        self._bq_client = None
        self._lineage_traverser = None

    def _ensure_initialized(self):
        creds = get_credentials(self.project_id)
        if not self._glossary_client:
            self._glossary_client = GlossaryClient(self.project_id, self.location, credentials=creds)
        if not self._similarity_engine:
            # Vertex AI models are best supported in us-central1 for now
            self._similarity_engine = SimilarityEngine(self.project_id, location="us-central1", credentials=creds)
        if not self._bq_client:
            self._bq_client = bigquery.Client(project=self.project_id, credentials=creds)
        if not self._lineage_traverser:
            self._lineage_traverser = LineageGraphTraverser(self.project_id, self.location)

    def _cache_term_embeddings(self, all_terms: List[Dict[str, Any]]):
        """Pre-calculates and caches embeddings for all glossary terms."""
        if not self._similarity_engine.embedder:
            return

        texts_to_embed = []
        term_ids = []
        for term in all_terms:
            if term['name'] not in self._similarity_engine.term_embeddings:
                # Combine name and description for a richer semantic representation
                text = f"{term['display_name']}: {term.get('description', '')}"
                texts_to_embed.append(text)
                term_ids.append(term['name'])
        
        if texts_to_embed:
            logger.info(f"Generating embeddings for {len(texts_to_embed)} glossary terms...")
            embs = self._similarity_engine.embedder.get_embeddings(texts_to_embed)
            new_cache = {term_ids[i]: embs[i] for i in range(len(embs))}
            self._similarity_engine.term_embeddings.update(new_cache)

    # _get_existing_links was removed because ListEntryLinks is currently restricted in this environment.
    # Targeted deduplication via _check_link_exists is used instead.

    def _check_link_exists(self, dataset_id: str, table_id: str, col_name: str, term_id: str) -> bool:
        """Checks if a specific term is already linked to a column using deterministic IDs."""
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        
        # Consistent with apply_terms ID construction
        clean_column = col_name.replace("_", "-").lower()
        clean_table = table_id.replace("_", "-").lower()
        entry_link_id = f"link-{clean_table}-{clean_column}"
        
        parent = f"projects/{self.project_id}/locations/{self.location}/entryGroups/@bigquery"
        link_name = f"{parent}/entryLinks/{entry_link_id}"
        
        try:
            link = client.get_entry_link(name=link_name)
            # Verify it's the SAME term (optional but safer)
            target_ref = next((r for r in link.entry_references if r.type_ == dataplex_v1.EntryLink.EntryReference.Type.TARGET), None)
            if target_ref:
                # Comparison: Term resource names might differ by project ID vs Number
                # We check if the unique term identifier (last segment) matches
                target_term_id = target_ref.name.split('/')[-1]
                source_term_id = term_id.split('/')[-1]
                if target_term_id == source_term_id:
                    return True
        except Exception:
            pass
        return False

    def recommend_terms_for_table(self, dataset_id: str, table_id: str) -> pd.DataFrame:
        """
        Fetches recommendations for all columns in a table using Vertex AI Embeddings.
        """
        self._ensure_initialized()
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self._bq_client.get_table(table_ref)
        
        all_terms = self._glossary_client.get_all_terms()
        if not all_terms:
            logger.warning("No glossary terms found to recommend.")
            return pd.DataFrame()

        # 1. Warm up Term Cache
        self._cache_term_embeddings(all_terms)

        # 2. Batch Generate Column Embeddings
        col_metas = []
        col_texts = []
        for field in table.schema:
            meta = {
                "name": field.name,
                "description": field.description or "",
                "type": field.field_type
            }
            col_metas.append(meta)
            # Use name and description for column semantic context
            col_texts.append(f"{field.name}: {field.description or ''}")

        col_embeddings = []
        if self._similarity_engine.embedder:
            logger.info(f"Generating batch embeddings for {len(col_texts)} columns in {table_id}...")
            col_embeddings = self._similarity_engine.embedder.get_embeddings(col_texts, task_type="RETRIEVAL_QUERY")

        # 3. Get Column Lineage (Upstream)
        
        # Get Column Lineage (Upstream)
        # Entry name for lineage is the BigQuery FQN: bigquery:project.dataset.table
        lineage_fqn = f"bigquery:{self.project_id}.{dataset_id}.{table_id}"
        col_list = [f.name for f in table.schema]
        upstream_lineage = self._lineage_traverser.get_column_lineage(lineage_fqn, col_list)

        # 4. Get Recommendations
        recommendations = []
        for i, col_meta in enumerate(col_metas):
            col_name = col_meta['name']
            col_path = f"Schema.{col_name}"
            col_emb = col_embeddings[i] if i < len(col_embeddings) else None
            
            # Recommendations will check for existing links using _check_link_exists
            
            # A. Lineage-Based Recommendations
            upstream = upstream_lineage.get(col_name)
            if upstream:
                src_entity = upstream['source_entity'] # project.dataset.table
                src_col = upstream['source_column']
                src_dataset = src_entity.split('.')[1]
                src_table = src_entity.split('.')[2]

                # HEURISTIC: Check if any of our known terms are linked upstream to this column
                # Since we can't list, we try a targeted check for all terms we found so far 
                # or just look for the most likely matches.
                # For now, we'll try to find any link matching our deterministic pattern
                for term in all_terms:
                    term_id = term['name']
                    if self._check_link_exists(src_dataset, src_table, src_col, term_id):
                        recommendations.append({
                            "Column": col_name,
                            "Suggested Term": term['display_name'],
                            "Confidence": 1.0, # 1.0 confidence for lineage propagation
                            "Rationale": f"Propagated via Lineage from {src_entity}",
                            "Term ID": term_id 
                        })
                        break # Only propagate one term per column via lineage

            if recommendations and recommendations[-1]['Column'] == col_name:
                # Found a lineage-based recommendation for this column!
                # For demo clarity, we prioritize lineage and skip similarity-based suggestions for this column.
                continue

            # B. Similarity-Based Recommendations
            suggestions = self._similarity_engine.get_ranked_suggestions(col_meta, all_terms, col_embedding=col_emb)
            
            for sug in suggestions:
                term_id = sug['term_name']
                
                # Targeted check for existing link
                if self._check_link_exists(dataset_id, table_id, col_name, term_id):
                    continue

                # Also check legacy check (description based)
                if f"Business Glossary: {sug['display_name']}" in col_meta.get('description', ''):
                     continue

                recommendations.append({
                    "Column": col_name,
                    "Suggested Term": sug['display_name'],
                    "Confidence": sug['confidence'],
                    "Rationale": f"Lexical: {sug['signals']['lexical']}, Semantic: {sug['signals']['semantic']}",
                    "Term ID": term_id
                })
        
        logger.info(f"Generated {len(recommendations)} recommendations for {table_id} after deduplication.")
        return pd.DataFrame(recommendations)

    def _get_entry_name(self, dataset_id: str, table_id: str):
        entry_id = f"bigquery.googleapis.com/projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}"
        # Harvested entries are in the @bigquery group at the same location as the BQ dataset
        return f"projects/{self.project_id}/locations/{self.location}/entryGroups/@bigquery/entries/{entry_id}"

    def _resolve_term_entry_name(self, term_resource_name: str) -> Optional[str]:
        """Maps a Business Glossary term resource name to its Dataplex Catalog Entry name."""
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        
        # We try deterministic patterns FIRST as they are faster and don't rely on eventual consistency of Search
        # and avoid 501/404 errors in certain regions/environments.

        # Pattern 1: Direct Construction with Project ID
        # Format: projects/{id}/locations/{loc}/entryGroups/@dataplex/entries/{resource}
        group_prefix = f"projects/{self.project_id}/locations/{self.location}/entryGroups/@dataplex/entries"
        candidate_id = f"{group_prefix}/{term_resource_name}"
        try:
            client.get_entry(name=candidate_id)
            return candidate_id
        except Exception:
            pass

        # Pattern 2: Direct Construction with Project Number (Harvested format)
        project_number = "1095607222622" # Hint for this specific demo environment
        term_res_num = term_resource_name.replace(self.project_id, project_number)
        candidate_num = f"{group_prefix}/{term_res_num}"
        try:
            client.get_entry(name=candidate_num)
            return candidate_num
        except Exception:
            pass

        # Pattern 3: Search fallback
        parent = f"projects/{self.project_id}/locations/{self.location}"
        query = f'resource:"{term_resource_name}"'
        try:
            request = dataplex_v1.SearchEntriesRequest(query=query)
            results = client.search_entries(request=request)
            for res in results:
                if "glossaries" in res.entry_source.resource:
                    return res.entry_name
        except Exception as e:
            # Downgrade to debug/info if construction works anyway, 
            # or if search is known to be flaky in this environment.
            logger.debug(f"Search fallback failed for glossary term resolution: {e}")

        logger.error(f"Could not resolve glossary term to Catalog Entry: {term_resource_name}")
        return None

    def apply_terms(self, dataset_id: str, table_id: str, updates: List[Dict[str, str]]):
        """
        Applies glossary terms to columns using native Dataplex EntryLinks.
        updates: List of {'column': str, 'term_id': str, 'term_display': str}
        """
        self._ensure_initialized()
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        
        # 1. BigQuery update (Optional/Skipped as per previous preference)
        logger.info(f"Applying {len(updates)} glossary terms to {table_id} via native EntryLinks.")

        # EntryLinks for BigQuery entries MUST reside in the @bigquery EntryGroup
        parent_group = f"projects/{self.project_id}/locations/{self.location}/entryGroups/@bigquery"
        entry_name = self._get_entry_name(dataset_id, table_id)
        
        # Link Type for Glossary Definition
        link_type = "projects/dataplex-types/locations/global/entryLinkTypes/definition"

        for up in updates:
            column = up['column']
            term_resource_name = up['term_id']  # This is the Business Glossary resource name
            
            # Resolve to Catalog Entry Name
            term_entry_name = self._resolve_term_entry_name(term_resource_name)
            if not term_entry_name:
                logger.error(f"Skipping {column}: Could not resolve glossary term to Catalog Entry.")
                continue

            # Deterministic ID for idempotency: link_{table}_{column}
            # EntryLink IDs must be lowercase, alphanumeric/hyphens
            clean_column = column.replace("_", "-").lower()
            clean_table = table_id.replace("_", "-").lower()
            entry_link_id = f"link-{clean_table}-{clean_column}"
            
            try:
                # Create the EntryLink
                link = dataplex_v1.EntryLink()
                link.entry_link_type = link_type
                
                # Source: The Table Column
                source_ref = dataplex_v1.EntryLink.EntryReference()
                source_ref.name = entry_name
                source_ref.path = f"Schema.{column}"
                source_ref.type_ = dataplex_v1.EntryLink.EntryReference.Type.SOURCE
                
                # Target: The Glossary Term
                target_ref = dataplex_v1.EntryLink.EntryReference()
                target_ref.name = term_entry_name
                target_ref.type_ = dataplex_v1.EntryLink.EntryReference.Type.TARGET
                
                link.entry_references = [source_ref, target_ref]
                
                try:
                    # Create in @bigquery group
                    client.create_entry_link(parent=parent_group, entry_link_id=entry_link_id, entry_link=link)
                    logger.info(f"Created native link for {column} -> {up['term_display']} in @bigquery group")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Link for {column} already exists, skipping.")
                    else:
                        raise e

            except Exception as e:
                logger.error(f"Failed to create EntryLink for {column}: {e}")
                # We continue with other updates even if one fails
                continue

    def scan_for_missing_glossary_terms(self, dataset_id: str) -> pd.DataFrame:
        """
        Scans all tables in a dataset for columns missing glossary terms using native EntryLinks.
        """
        self._ensure_initialized()
        client = dataplex_v1.CatalogServiceClient(credentials=get_credentials(self.project_id))
        
        parent = f"projects/{self.project_id}/locations/{self.location}"
        # Links for BigQuery entries are in the @bigquery group
        link_group_name = f"{parent}/entryGroups/@bigquery"
        
        # NOTE: list_entry_links is currently restricted, so this scan may be incomplete.
        # It relies on legacy description-based tags for discovery in this environment.
        existing_links = {}
        # NOTE: list_entry_links is currently restricted in this environment, 
        # so this scan relies on legacy description-based tags for discovery.

        dataset_ref = self._bq_client.dataset(dataset_id)
        tables = self._bq_client.list_tables(dataset_ref)
        
        gaps = []
        for table_item in tables:
            table_id = table_item.table_id
            full_table = self._bq_client.get_table(table_item.reference)
            entry_name = self._get_entry_name(dataset_id, table_id)
            
            for field in full_table.schema:
                path = f"Schema.{field.name}"
                
                # Check if link exists
                if (entry_name, path) not in existing_links:
                    # Also check legacy BQ description for backward compatibility
                    desc = field.description or ""
                    if "Business Glossary:" not in desc:
                        gaps.append({
                            "Table": table_id,
                            "Column": field.name,
                            "Type": field.field_type
                        })
        
        return pd.DataFrame(gaps)


