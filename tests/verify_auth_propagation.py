import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add necessary paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dataplex_integration')))

from lineage_plugin import LineagePlugin
import google.oauth2.credentials

class TestAuthPropagation(unittest.TestCase):
    def setUp(self):
        self.project_id = "test-project"
        self.location = "europe-west1"

    @patch('lineage_plugin.get_oauth_token')
    @patch('lineage_plugin.get_credentials')
    @patch('lineage_propagation.bigquery.Client')
    @patch('lineage_propagation.datacatalog_lineage_v1.LineageClient')
    def test_propagation_with_oauth(self, mock_lineage_client, mock_bq_client, mock_get_creds, mock_get_token):
        # Mocking OAuth token and credentials
        mock_token = "dummy-oauth-token"
        mock_creds = MagicMock(spec=google.oauth2.credentials.Credentials)
        
        mock_get_token.return_value = mock_token
        mock_get_creds.return_value = mock_creds
        
        # Initialize Plugin
        plugin = LineagePlugin(self.project_id, self.location)
        plugin._ensure_initialized()
        
        # Verify SQLFetcher got the credentials
        from lineage_propagation import SQLFetcher
        self.assertIsInstance(plugin._sql_fetcher, SQLFetcher)
        self.assertEqual(plugin._sql_fetcher.client, mock_bq_client.return_value)
        mock_bq_client.assert_called_with(project=self.project_id, credentials=mock_creds)
        
        # Verify LineageGraphTraverser got the token
        from lineage_propagation import LineageGraphTraverser
        self.assertIsInstance(plugin._lineage_traverser, LineageGraphTraverser)
        self.assertEqual(plugin._lineage_traverser.token, mock_token)

    @patch('lineage_plugin.get_oauth_token')
    @patch('lineage_plugin.get_credentials')
    @patch('lineage_propagation.bigquery.Client')
    @patch('lineage_propagation.google.auth.default')
    @patch('lineage_propagation.requests.post')
    def test_adc_fallback(self, mock_post, mock_auth_default, mock_bq_client, mock_get_creds, mock_get_token):
        # Mocking NO OAuth token
        mock_get_token.return_value = None
        mock_get_creds.return_value = None # This triggers ADC in _get_bq_client
        
        # Mock ADC
        mock_adc_creds = MagicMock()
        mock_adc_creds.token = "adc-token"
        mock_auth_default.return_value = (mock_adc_creds, self.project_id)
        
        # Mock API Response for searchLinks
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"links": []}
        mock_post.return_value = mock_response

        # Initialize Plugin
        plugin = LineagePlugin(self.project_id, self.location)
        plugin._ensure_initialized()
        
        # Verify SQLFetcher called with None for credentials (causing bigquery.Client to fallback to ADC internally)
        mock_bq_client.assert_called_with(project=self.project_id, credentials=None)
        
        # Trigger an API call that requires token
        plugin._lineage_traverser._search_links("some-fqn")
        
        # Verify fallback to ADC in search_links
        mock_auth_default.assert_called()
        headers = mock_post.call_args[1]['headers']
        self.assertEqual(headers['Authorization'], "Bearer adc-token")

if __name__ == '__main__':
    unittest.main()
