import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

# Add necessary paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dataplex_integration')))

from lineage_plugin import LineagePlugin
from lineage_propagation import TransformationEnricher

class TestSQLEnrichment(unittest.TestCase):
    def setUp(self):
        self.plugin = LineagePlugin("test-project", "europe-west1")
        self.plugin._get_bq_client = MagicMock()
        self.plugin._lineage_traverser = MagicMock()
        self.plugin._sql_fetcher = MagicMock()

    def test_sql_logic_extraction(self):
        sql = """
        CREATE OR REPLACE TABLE `governance-agent.retail_syn_data.transactions` AS
        SELECT 
            t.amount * 1.1 as amount_taxed,
            CASE WHEN t.amount > 100 THEN 'HIGH' ELSE 'LOW' END as val_cat
        FROM `raw_transactions` t
        """
        
        # Test amount_taxed
        expr = TransformationEnricher.extract_column_logic(sql, "amount_taxed")
        self.assertEqual(expr, "t.amount * 1.1")
        
        # Test val_cat
        expr = TransformationEnricher.extract_column_logic(sql, "val_cat")
        self.assertEqual(expr, "CASE WHEN t.amount > 100 THEN 'HIGH' ELSE 'LOW' END")

    def test_description_enrichment_with_sql(self):
        original_desc = "Total order amount"
        target_col = "amount_taxed"
        source_col = "amount"
        sql_expr = "t.amount * 1.1"
        
        enriched = TransformationEnricher.enrich_description(
            target_col, source_col, original_desc, sql_expr=sql_expr
        )
        
        self.assertIn("Total order amount", enriched)
        self.assertIn("amount", enriched)
        self.assertIn("+10% tax/markup", enriched)

    def test_plugin_preview_with_sql(self):
        # Mock table schema
        mock_table = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "amount_taxed"
        mock_field.description = ""
        mock_table.schema = [mock_field]
        self.plugin._get_bq_client().get_table.return_value = mock_table
        
        # Mock SQL Fetcher
        self.plugin._sql_fetcher.get_transformation_sql.return_value = "SELECT t.amount * 1.1 as amount_taxed FROM t"
        
        # Mock Recursive Lineage
        self.plugin._find_description_recursive = MagicMock(return_value={
            "source_entity": "raw_transactions",
            "source_column": "amount",
            "description": "Total order amount",
            "confidence": 1.0,
            "hop_depth": 0
        })
        
        # Run Preview
        results_df = self.plugin.preview_propagation("ds", "tab")
        
        # Verify
        self.assertFalse(results_df.empty)
        row = results_df.iloc[0]
        self.assertEqual(row["Target Column"], "amount_taxed")
        self.assertIn("+10% tax/markup", row["Proposed Description"])

if __name__ == '__main__':
    unittest.main()
