import re
import math
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SimilarityEngine:
    """Calculates similarity between columns and business glossary terms."""
    
    def __init__(self):
        # weights for different signals
        self.weights = {
            "lexical": 0.4,
            "semantic": 0.4,
            "lineage": 0.2
        }

    def _normalize(self, text: str) -> str:
        if not text: return ""
        # Lowercase, remove special chars, split by underscore/camelCase
        text = text.lower()
        text = re.sub(r'[^a-z0-9]', ' ', text)
        return text.strip()

    def calculate_lexical_similarity(self, col_name: str, term_display: str, term_id: str = "") -> float:
        """Jaccard similarity on normalized tokens, including term ID."""
        s1 = set(self._normalize(col_name).split())
        s2 = set(self._normalize(term_display).split())
        if term_id:
            s2 = s2.union(set(self._normalize(term_id).split()))
        
        if not s1 or not s2:
            return 0.0
            
        intersection = len(s1.intersection(s2))
        union = len(s1.union(s2))
        
        return intersection / union

    def calculate_semantic_similarity(self, col_metadata: Dict[str, Any], term: Dict[str, Any]) -> float:
        """
        Calculates similarity using embeddings (placeholder for now).
        Fallback to description substring matching if embeddings are unavailable.
        """
        # TODO: Integrate with Vertex AI Embeddings for real semantic match
        
        col_desc = col_metadata.get("description", "").lower()
        term_desc = term.get("description", "").lower()
        
        if not col_desc or not term_desc:
            return 0.0
            
        # Placeholder: keyword overlap in descriptions + term name
        s1 = set(self._normalize(col_desc).split())
        s2 = set(self._normalize(term_desc).split())
        s2_name = set(self._normalize(term.get("display_name", "")).split())
        s2 = s2.union(s2_name)
        
        if not s1 or not s2:
            return 0.0
            
        intersection = len(s1.intersection(s2))
        # Use min length for overlap to be more forgiving on descriptions
        score = intersection / min(len(s1), len(s2))
        
        return min(score, 1.0)

    def calculate_lineage_proximity(self, col_lineage: Dict[str, Any], term_mappings: List[Dict[str, Any]]) -> float:
        """
        Boost score if upstream columns are already mapped to this term.
        """
        # col_lineage contains source_fqn and source_column
        # If any upstream column (via lineage) is already mapped to this term, return 1.0
        # Placeholder logic:
        return 0.0

    def get_ranked_suggestions(self, column: Dict[str, Any], all_terms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Produces ranked suggestions for a single column."""
        suggestions = []
        
        for term in all_terms:
            # We use term['name'] as term_id (it contains the full resource name, but let's extract basename)
            term_id_base = term['name'].split('/')[-1]
            lexical = self.calculate_lexical_similarity(column['name'], term['display_name'], term_id=term_id_base)
            semantic = self.calculate_semantic_similarity(column, term)
            
            # Combine scores - boost lexical if semantic is empty
            if semantic == 0:
                score = lexical * 0.8 # Higher weight on name if no description
            else:
                score = (lexical * self.weights['lexical']) + (semantic * self.weights['semantic'])
            
            if score > 0.15: # Lowered threshold further for demo
                suggestions.append({
                    "term_name": term['name'],
                    "display_name": term['display_name'],
                    "confidence": round(score, 2),
                    "signals": {
                        "lexical": round(lexical, 2),
                        "semantic": round(semantic, 2)
                    }
                })
                
        # Sort by confidence
        suggestions.sort(key=lambda x: x['confidence'], reverse=True)
        return suggestions[:5] # Top 5
