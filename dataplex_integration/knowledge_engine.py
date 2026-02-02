import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DescriptionPropagator:
    def __init__(self, knowledge_engine_json_path=None):
        self.knowledge_json = {}
        if knowledge_engine_json_path:
            try:
                with open(knowledge_engine_json_path, 'r') as f:
                    self.knowledge_json = json.load(f)
                logger.info(f"Loaded Knowledge Engine insights from {knowledge_engine_json_path}")
            except Exception as e:
                logger.error(f"Failed to load Knowledge Engine JSON: {e}")

    def get_semantic_match(self, source_col, target_col, upstream_table, downstream_table):
        """
        Determines semantic match confidence using Knowledge Engine insights.
        """
        # 1. Check if we have insights for this specific relationship
        # This is scanning the mock JSON structure
        
        # Hypothetical JSON structure:
        # {
        #   "relationships": [
        #     {
        #       "source_table": "...", "target_table": "...",
        #       "column_mappings": [
        #           {"source_col": "cust_id", "target_col": "customer_id", "confidence": 0.98, "type": "RENAME"}
        #       ]
        #     }
        #   ]
        # }
        
        for rel in self.knowledge_json.get("relationships", []):
            if rel.get("source_table") == upstream_table and rel.get("target_table") == downstream_table:
                for mapping in rel.get("column_mappings", []):
                    if mapping.get("source_col") == source_col and mapping.get("target_col") == target_col:
                        return {
                            "confidence": mapping.get("confidence", 0.0),
                            "type": mapping.get("type", "UNKNOWN"),
                            "explanation": mapping.get("explanation", "Match found in Knowledge Engine")
                        }
        
        # Fallback if no JSON match but names are identical
        if source_col == target_col:
            return {
                "confidence": 1.0,
                "type": "DIRECT_COPY",
                "explanation": "Identical column names"
            }
            
        return {"confidence": 0.0, "type": "NONE", "explanation": "No match found"}
