import re
import math
from typing import List, Dict, Any, Optional
import logging

from vertex_embedder import VertexAIEmbedder

logger = logging.getLogger(__name__)

class SimilarityEngine:
    """Calculates similarity between columns and business glossary terms."""
    
    def __init__(self, project_id: Optional[str] = None, location: str = "us-central1", credentials: Optional[Any] = None):
        # weights for different signals
        self.weights = {
            "lexical": 0.3,    # Reduced as semantic becomes more powerful
            "semantic": 0.5,   # Increased and powered by embeddings
            "lineage": 0.2
        }
        self.project_id = project_id
        self.embedder = VertexAIEmbedder(project_id, location, credentials=credentials) if project_id else None
        # Cache for term embeddings: TermID -> Embedding
        self.term_embeddings = {}

    def set_term_embeddings(self, embeddings: Dict[str, List[float]]):
        """Sets pre-calculated embeddings for glossary terms."""
        self.term_embeddings = embeddings

    def _normalize(self, text: str) -> str:
        if not text: return ""
        # Lowercase, remove special chars, split by underscore/camelCase
        text = text.lower()
        text = re.sub(r'[^a-z0-9]', ' ', text)
        return text.strip()

    def _get_primary_entity(self, text: str) -> Optional[str]:
        """Extracts the primary business entity from text."""
        # Main entities (conflict-prone)
        entities = ["customer", "order", "transaction", "product", "item", "user", "account", "membership", "loyalty"]
        # Concept tags (compatibility-prone)
        concepts = ["amount", "price", "date", "timestamp", "time", "status", "type", "category"]
        
        text_norm = self._normalize(text)
        tokens = text_norm.split()
        
        # Check entities first
        for entity in entities:
            if entity in tokens:
                return entity
        
        # Check concepts
        for concept in concepts:
            if concept in tokens:
                return concept
                
        return None

    def _detect_entity_conflict(self, col_name: str, term_display: str, term_id: str) -> bool:
        """Detects if a column and term belong to fundamentally different entities."""
        col_entity = self._get_primary_entity(col_name)
        term_entity = self._get_primary_entity(term_display) or self._get_primary_entity(term_id)
        
        if not col_entity or not term_entity:
            return False
            
        if col_entity == term_entity:
            return False
            
        # Specific allowed overlaps (aliases)
        compatibles = [{"order", "transaction"}, {"item", "product"}, {"amount", "price"}, {"date", "timestamp"}]
        for pair in compatibles:
            if {col_entity, term_entity} == pair:
                return False
                
        # If both are entities (not concepts), it's a conflict
        entities = {"customer", "user", "account", "product", "item", "order", "transaction"}
        if col_entity in entities and term_entity in entities:
            return True
            
        return False

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

    def calculate_semantic_similarity(self, col_metadata: Dict[str, Any], term: Dict[str, Any], col_embedding: Optional[List[float]] = None) -> float:
        """
        Calculates similarity using Vertex AI embeddings.
        Fallback to description keyword matching if embeddings are unavailable.
        """
        term_id = term['name']
        term_emb = self.term_embeddings.get(term_id)
        
        # Priority 1: Vector Similarity
        if col_embedding and term_emb:
            return self.embedder.cosine_similarity(col_embedding, term_emb)
            
        # Priority 2: Fallback to keyword overlap
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
        # Placeholder logic:
        return 0.0

    def get_ranked_suggestions(self, column: Dict[str, Any], all_terms: List[Dict[str, Any]], col_embedding: Optional[List[float]] = None) -> List[Dict[str, Any]]:
        """Produces ranked suggestions for a single column with adaptive filtering and entity awareness."""
        suggestions = []
        col_name = column['name']
        col_entity = self._get_primary_entity(col_name)
        
        for term in all_terms:
            term_id_full = term['name']
            term_id_base = term_id_full.split('/')[-1]
            term_display = term['display_name']
            
            lexical = self.calculate_lexical_similarity(col_name, term_display, term_id=term_id_base)
            semantic = self.calculate_semantic_similarity(column, term, col_embedding=col_embedding)
            
            # Combine scores
            score = (lexical * self.weights['lexical']) + (semantic * self.weights['semantic'])
            
            orig_score = score
            # 1. Entity Conflict Penalty
            conflict = self._detect_entity_conflict(col_name, term_display, term_id_base)
            if conflict:
                score -= 0.30 # Increased penalty to be more decisive
            
            # 2. Entity Match Boost
            term_entity = self._get_primary_entity(term_display) or self._get_primary_entity(term_id_base)
            boost = 0
            if col_entity and term_entity and col_entity == term_entity:
                boost = 0.15 # Increased boost
                score += boost
                
            # 3. Concept Alignment: If both are IDs or both are Amounts, they should be closer
            if not conflict:
                # If they share a concept tag (like 'amount' or 'id')
                col_tags = set(self._normalize(col_name).split())
                term_tags = set(self._normalize(term_display).split()).union(set(self._normalize(term_id_base).split()))
                for tag in ["id", "amount", "price", "date", "timestamp"]:
                    if tag in col_tags and tag in term_tags:
                        score += 0.1 # Concept match boost
                        break

            # 4. Base Thresholding
            if score >= 0.30:
                suggestions.append({
                    "term_name": term_id_full,
                    "display_name": term_display,
                    "confidence": round(max(0, score), 2),
                    "signals": {
                        "lexical": round(lexical, 2),
                        "semantic": round(semantic, 2),
                        "conflict": conflict,
                        "boost": boost
                    }
                })
                
        # Sort by confidence
        suggestions.sort(key=lambda x: x['confidence'], reverse=True)
        
        # 5. Competitive Filtering: 
        if suggestions:
            top_score = suggestions[0]['confidence']
            if top_score > 0.45:
                # Keep suggestions within 70% of top score if top score is strong
                suggestions = [s for s in suggestions if s['confidence'] >= (top_score * 0.7)]
        
        return suggestions[:5] # Top 5 relevant matches
