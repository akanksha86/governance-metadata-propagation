import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

# Add paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/adk_integration')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dataplex_integration')))

# Mock context and ADK before importing
with patch("context.get_credentials", return_value=MagicMock()):
    from glossary_plugin import GlossaryPlugin

class TestNativeGlossaryLinks(unittest.TestCase):
    def setUp(self):
        self.project_id = "governance-agent"
        self.location = "europe-west1"
        self.plugin = GlossaryPlugin(self.project_id, self.location)
        self.plugin._ensure_initialized = MagicMock()
        
    @patch("glossary_plugin.dataplex_v1.CatalogServiceClient")
    @patch("glossary_plugin.get_credentials")
    def test_apply_terms_creates_links(self, mock_creds, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        
        term_resource = f"projects/{self.project_id}/locations/{self.location}/glossaries/g/terms/term1"
        term_entry = f"projects/{self.project_id}/locations/{self.location}/entryGroups/@dataplex/entries/some-id"
        
        # Mock resolve_term_entry_name behavior
        mock_client.search_entries.side_effect = Exception("Search Disabled")
        mock_client.get_entry.return_value = MagicMock(name=term_entry)
        
        updates = [{
            "column": "customer_id",
            "term_id": term_resource,
            "term_display": "Customer Identifier"
        }]
        
        self.plugin.apply_terms("ds", "customers", updates)
        
        # Verify create_entry_link was called
        self.assertTrue(mock_client.create_entry_link.called)
        args, kwargs = mock_client.create_entry_link.call_args
        
        # Verify parent is @bigquery
        self.assertIn("@bigquery", kwargs['parent'])
        
        link = kwargs['entry_link']
        self.assertEqual(link.entry_link_type, "projects/dataplex-types/locations/global/entryLinkTypes/definition")

    @patch("glossary_plugin.dataplex_v1.CatalogServiceClient")
    @patch("glossary_plugin.get_credentials")
    def test_resolve_term_entry_name_fallback(self, mock_creds, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        self.plugin.project_id = "governance-agent"
        
        term_resource = "projects/governance-agent/locations/l/glossaries/g/terms/term1"
        term_entry_num = "projects/governance-agent/locations/europe-west1/entryGroups/@dataplex/entries/projects/1095607222622/locations/l/glossaries/g/terms/term1"
        
        # Mock Search Failure
        mock_client.search_entries.side_effect = Exception("501")
        
        # Mock Pattern 2 Failure (Project ID)
        def get_entry_effect(name):
            if "1095607222622" in name:
                return MagicMock(name=name)
            raise Exception("404")
        
        mock_client.get_entry.side_effect = get_entry_effect
        
        resolved = self.plugin._resolve_term_entry_name(term_resource)
        self.assertIn("1095607222622", resolved)

if __name__ == "__main__":
    unittest.main()
