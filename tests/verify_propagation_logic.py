import sys
import os

# Add parent directory to path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../dataplex_integration'))

from knowledge_engine import DescriptionPropagator

def test_propagation_logic():
    print("Testing Propagation Logic...")
    
    # 1. Load Knowledge Engine
    json_path = os.path.join(os.path.dirname(__file__), '../dataplex_integration/knowledge_engine_sample.json')
    propagator = DescriptionPropagator(json_path)
    
    # 2. Test Direct Match (Implicit in Propagator fallback)
    print("\nTest 1: Direct Match (Same Name)")
    result = propagator.get_semantic_match("active", "active", "customers", "customers_curated")
    print(f"Result: {result}")
    assert result['type'] == 'DIRECT_COPY'
    assert result['confidence'] == 1.0
    
    # 3. Test Knowledge Engine Match (Rename)
    print("\nTest 2: Knowledge Engine Match (cust_id -> customer_id)")
    result = propagator.get_semantic_match("cust_id", "customer_id", "customers", "customers_curated")
    print(f"Result: {result}")
    assert result['type'] == 'RENAME'
    assert result['confidence'] >= 0.99
    
    # 4. Test No Match
    print("\nTest 3: No Match")
    result = propagator.get_semantic_match("random_col", "other_col", "customers", "customers_curated")
    print(f"Result: {result}")
    assert result['type'] == 'NONE'
    
    print("\nSUCCESS: All logic tests passed!")

if __name__ == "__main__":
    test_propagation_logic()
