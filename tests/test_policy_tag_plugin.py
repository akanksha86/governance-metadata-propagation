import sys
import os
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch

# Add necessary paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dataplex_integration')))

from policy_tag_plugin import PolicyTagPlugin

class TestPolicyTagPlugin(unittest.TestCase):
    @patch('policy_tag_plugin.get_credentials')
    @patch('policy_tag_plugin.get_oauth_token')
    def setUp(self, mock_token, mock_creds):
        self.plugin = PolicyTagPlugin(project_id="test-project", location="test-location")
        self.plugin._get_bq_client = MagicMock()
        self.plugin._lineage_traverser = MagicMock()
        self.plugin._sql_fetcher = MagicMock()

    def test_scan_for_policy_tags(self):
        # Mock BigQuery list_tables and get_table
        mock_table_item = MagicMock()
        mock_table_item.table_id = "test_table"
        self.plugin._get_bq_client.return_value.list_tables.return_value = [mock_table_item]
        
        mock_field = MagicMock()
        mock_field.name = "sensitive_col"
        mock_field.policy_tags.names = ["projects/test/locations/us/taxonomies/1/policyTags/2"]
        
        mock_table = MagicMock()
        mock_table.schema = [mock_field]
        self.plugin._get_bq_client.return_value.get_table.return_value = mock_table
        
        df = self.plugin.scan_for_policy_tags("test_dataset")
        
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]["Table"], "test_table")
        self.assertEqual(df.iloc[0]["Column"], "sensitive_col")

    def test_preview_policy_tag_propagation_straight_pull(self):
        # Mock target table schema
        target_field = MagicMock()
        target_field.name = "col1"
        target_field.policy_tags = None
        
        mock_target_table = MagicMock()
        mock_target_table.schema = [target_field]
        
        # Mock lineage
        self.plugin._lineage_traverser.get_column_lineage.return_value = {
            "col1": [{
                "source_entity": "project.dataset.source_table",
                "source_column": "col1"
            }]
        }
        
        # Mock source table schema with policy tag
        src_field = MagicMock()
        src_field.name = "col1"
        src_field.policy_tags.names = ["tag1"]
        
        mock_src_table = MagicMock()
        mock_src_table.schema = [src_field]
        
        # Mock BQ client side effect for target then source
        self.plugin._get_bq_client.return_value.get_table.side_effect = [mock_target_table, mock_src_table]
        
        # Mock SQL logic (straight pull)
        self.plugin._sql_fetcher.get_transformation_sql.return_value = "SELECT col1 FROM source"
        with patch('policy_tag_plugin.TransformationEnricher.extract_column_logic', return_value="col1"):
            df = self.plugin.preview_policy_tag_propagation("test_dataset", "test_table")
        
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]["Recommendation"], "Propagate")
        self.assertEqual(df.iloc[0]["Target Column"], "col1")

    def test_preview_policy_tag_propagation_transformed(self):
        # Mock target table schema
        target_field = MagicMock()
        target_field.name = "col1"
        target_field.policy_tags = None
        
        mock_target_table = MagicMock()
        mock_target_table.schema = [target_field]
        
        # Mock lineage
        self.plugin._lineage_traverser.get_column_lineage.return_value = {
            "col1": [{
                "source_entity": "project.dataset.source_table",
                "source_column": "src_col"
            }]
        }
        
        # Mock source table schema with policy tag
        src_field = MagicMock()
        src_field.name = "src_col"
        src_field.policy_tags.names = ["tag1"]
        
        mock_src_table = MagicMock()
        mock_src_table.schema = [src_field]
        
        # Mock BQ client side effect
        self.plugin._get_bq_client.return_value.get_table.side_effect = [mock_target_table, mock_src_table]
        
        # Mock SQL logic (transformation)
        self.plugin._sql_fetcher.get_transformation_sql.return_value = "SELECT UPPER(src_col) as col1 FROM source"
        with patch('policy_tag_plugin.TransformationEnricher.extract_column_logic', return_value="UPPER(src_col)"):
            df = self.plugin.preview_policy_tag_propagation("test_dataset", "test_table")
        
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]["Recommendation"], "Review Required (Transformed)")

    def test_apply_policy_tags(self):
        # Mock table with schema
        mock_field = MagicMock()
        mock_field.name = "col1"
        mock_field.to_api_repr.return_value = {"name": "col1", "type": "STRING"}
        
        mock_table = MagicMock()
        mock_table.schema = [mock_field]
        self.plugin._get_bq_client.return_value.get_table.return_value = mock_table
        
        updates = [{
            "table": "test_table",
            "column": "col1",
            "policy_tag": "projects/p/locations/l/taxonomies/t/policyTags/pt"
        }]
        
        self.plugin.apply_policy_tags("test_dataset", updates)
        
        # Verify update_table was called
        self.assertTrue(self.plugin._get_bq_client.return_value.update_table.called)

    def test_preview_policy_tag_propagation_skips_existing(self):
        # Mock target table where col1 ALREADY has the tag
        mock_tag = MagicMock()
        mock_tag.names = ["projects/p/locations/l/taxonomies/t/policyTags/pt"]
        
        mock_field = MagicMock()
        mock_field.name = "col1"
        mock_field.policy_tags = mock_tag
        
        mock_table = MagicMock()
        mock_table.schema = [mock_field]
        self.plugin._get_bq_client.return_value.get_table.return_value = mock_table
        
        # Mock lineage
        self.plugin._lineage_traverser.get_column_lineage.return_value = {
            "col1": [{"source_entity": "src_table", "source_column": "col1"}]
        }
        
        # Mock source table with the same tag
        mock_src_tag = MagicMock()
        mock_src_tag.names = ["projects/p/locations/l/taxonomies/t/policyTags/pt"]
        mock_src_field = MagicMock()
        mock_src_field.name = "col1"
        mock_src_field.policy_tags = mock_src_tag
        
        mock_src_table = MagicMock()
        mock_src_table.schema = [mock_src_field]
        self.plugin._get_bq_client.return_value.get_table.side_effect = [mock_table, mock_src_table]
        
        df = self.plugin.preview_policy_tag_propagation("test_dataset", "test_table")
        
        # Should be empty because it matched
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()
