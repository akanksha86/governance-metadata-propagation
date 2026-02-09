import logging
import numpy as np
from typing import List, Optional, Any
from google.cloud import aiplatform
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

logger = logging.getLogger(__name__)

class VertexAIEmbedder:
    """
    Utility to generate and compare embeddings using Vertex AI.
    """
    def __init__(self, project_id: str, location: str = "us-central1", model_name: str = "text-embedding-004", credentials: Optional[Any] = None):
        """
        Initializes the Vertex AI SDK and the embedding model.
        NOTE: Model availability may vary by region. Defaults to us-central1 for higher availability of latest models.
        """
        self.project_id = project_id
        self.location = location
        self.model_name = model_name
        self._model = None
        
        try:
            aiplatform.init(project=project_id, location=location, credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI: {e}")

    def _get_model(self):
        if self._model is None:
            try:
                self._model = TextEmbeddingModel.from_pretrained(self.model_name)
            except Exception as e:
                logger.error(f"Failed to load Vertex AI model {self.model_name}: {e}")
        return self._model

    def get_embeddings(self, texts: List[str], task_type: str = "RETRIEVAL_DOCUMENT") -> List[List[float]]:
        """
        Generates embeddings for a list of texts in batch.
        """
        model = self._get_model()
        if not model or not texts:
            return []
            
        try:
            inputs = [TextEmbeddingInput(text=t, task_type=task_type) for t in texts]
            # Vertex AI SDK handles batching internally if the list is large, 
            # but we can also manage it if needed (limit is usually 250 per request).
            embeddings = model.get_embeddings(inputs)
            return [e.values for e in embeddings]
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            return []

    def get_embedding(self, text: str, task_type: str = "RETRIEVAL_DOCUMENT") -> Optional[List[float]]:
        """
        Generates embedding for a single text.
        """
        res = self.get_embeddings([text], task_type=task_type)
        return res[0] if res else None

    @staticmethod
    def cosine_similarity(v1: List[float], v2: List[float]) -> float:
        """
        Calculates cosine similarity between two vectors.
        """
        if not v1 or not v2:
            return 0.0
            
        v1 = np.array(v1)
        v2 = np.array(v2)
        
        dot_product = np.dot(v1, v2)
        norm_v1 = np.linalg.norm(v1)
        norm_v2 = np.linalg.norm(v2)
        
        if norm_v1 == 0 or norm_v2 == 0:
            return 0.0
            
        return float(dot_product / (norm_v1 * norm_v2))
