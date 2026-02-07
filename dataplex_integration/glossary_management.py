import os
import logging
from typing import List, Dict, Any, Optional
from google.cloud import dataplex_v1
from google.api_core import exceptions

logger = logging.getLogger(__name__)

class GlossaryClient:
    """Interface for Dataplex Business Glossary."""
    
    def __init__(self, project_id: str, location: str, credentials: Optional[Any] = None):
        self.project_id = project_id
        self.location = location
        self.client = dataplex_v1.CatalogServiceClient(credentials=credentials)
        self.parent = f"projects/{project_id}/locations/{location}"

    def list_glossaries(self) -> List[Dict[str, Any]]:
        """Lists all glossaries in the given project/location."""
        try:
            # CatalogService doesn't have list_glossaries? 
            # Re-checking available methods in CatalogServiceClient...
            # Actually, BusinessGlossaryService might be a separate client or part of CatalogService.
            # Based on dataplex_dir.txt: BusinessGlossaryServiceClient exists.
            
            # Let's try BusinessGlossaryServiceClient
            from google.cloud import dataplex_v1
            bg_client = dataplex_v1.BusinessGlossaryServiceClient(credentials=self.client._transport._credentials)
            
            request = dataplex_v1.ListGlossariesRequest(parent=self.parent)
            page_result = bg_client.list_glossaries(request=request)
            
            glossaries = []
            for response in page_result:
                glossaries.append({
                    "name": response.name,
                    "display_name": response.display_name,
                    "description": response.description
                })
            return glossaries
        except Exception as e:
            logger.error(f"Failed to list glossaries: {e}")
            return []

    def get_terms(self, glossary_name: str) -> List[Dict[str, Any]]:
        """Fetches all terms for a specific glossary."""
        try:
            from google.cloud import dataplex_v1
            bg_client = dataplex_v1.BusinessGlossaryServiceClient(credentials=self.client._transport._credentials)
            
            request = dataplex_v1.ListGlossaryTermsRequest(parent=glossary_name)
            page_result = bg_client.list_glossary_terms(request=request)
            
            terms = []
            for response in page_result:
                terms.append({
                    "name": response.name,
                    "display_name": response.display_name,
                    "description": response.description
                })
            return terms
        except Exception as e:
            logger.error(f"Failed to fetch terms for {glossary_name}: {e}")
            return []

    def get_all_terms(self) -> List[Dict[str, Any]]:
        """Fetches terms from all glossaries in the location."""
        glossaries = self.list_glossaries()
        all_terms = []
        for g in glossaries:
            all_terms.extend(self.get_terms(g['name']))
        return all_terms
