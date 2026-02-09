import sys
import os
import logging

# Add paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))

from vertex_embedder import VertexAIEmbedder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_embeddings():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent")
    print(f"Testing Vertex AI Embeddings in project: {project_id}")
    
    embedder = VertexAIEmbedder(project_id, location="us-central1")
    
    # Test 1: Single Embedding
    text = "Transaction Timestamp: Record of when the order was placed"
    emb = embedder.get_embedding(text)
    if emb:
        print(f"SUCCESS: Generated embedding of length {len(emb)}")
    else:
        print("FAILED: No embedding generated.")
        return

    # Test 2: Similarity
    text1 = "Order Date: Date the customer placed the order"
    text2 = "Transaction Timestamp: When the event occurred"
    text3 = "Customer Name: Full name of the user"
    
    embs = embedder.get_embeddings([text1, text2, text3])
    if len(embs) == 3:
        sim12 = embedder.cosine_similarity(embs[0], embs[1])
        sim13 = embedder.cosine_similarity(embs[0], embs[2])
        
        print(f"\nSimilarity between '{text1[:20]}...' and '{text2[:20]}...': {sim12:.4f}")
        print(f"Similarity between '{text1[:20]}...' and '{text3[:20]}...': {sim13:.4f}")
        
        if sim12 > sim13:
            print("SUCCESS: Semantic similarity logic holds (Order Date closer to Transaction Timestamp than to Customer Name)")
        else:
            print("WARNING: Semantic similarity did not follow expected pattern.")
    else:
        print("FAILED: Could not generate batch embeddings.")

if __name__ == "__main__":
    test_embeddings()
