import gradio as gr
import sys
import os
import pandas as pd
import logging

# Setup paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/plugins')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../agent/adk_integration')))

# Import Agent Components
from lineage_plugin import LineagePlugin
from context import set_oauth_token

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch Project ID from environment or default
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent")
DEFAULT_LOCATION = "europe-west1"
DEFAULT_DATASET_ID = "retail_syn_data"

KNOWLEDGE_JSON_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dataplex_integration/knowledge_engine_sample.json"))

def get_plugin(project_id, location):
    # For demo efficiency, we can cache the plugin instance per project/location
    # but for simplicity here we just re-instantiate or assume one at a time.
    return LineagePlugin(project_id, location, knowledge_json_path=KNOWLEDGE_JSON_PATH)

def set_context_token(token):
    if token and token.strip():
        set_oauth_token(token)
    else:
        set_oauth_token(None)

def scan_dataset(token, project_id, location, dataset_id):
    set_context_token(token)
    try:
        plugin = get_plugin(project_id, location)
        df = plugin.scan_for_missing_descriptions(dataset_id)
        if df.empty:
            gr.Info("No missing descriptions found!")
            return pd.DataFrame(columns=["Table", "Column", "Type"])
        return df
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        raise gr.Error(f"Scan failed: {str(e)}")

def analyze_and_preview(token, project_id, location, dataset_id, target_table):
    set_context_token(token)
    try:
        plugin = get_plugin(project_id, location)
        
        # 1. Get Summary
        summary = plugin.get_lineage_summary(dataset_id, target_table)
        
        # 2. Get Preview DF
        df = plugin.preview_propagation(dataset_id, target_table)
        if df.empty:
            gr.Warning(f"No upstream candidates found for {target_table}.")
            df = pd.DataFrame(columns=["Target Column", "Source", "Source Column", "Confidence", "Proposed Description", "Type"])
            
        return summary, df
    except Exception as e:
        logger.error(f"Analyze & Preview failed: {e}")
        raise gr.Error(f"Operation failed: {str(e)}")

def apply_propagation_improved(token, project_id, location, dataset_id, target_table, candidates_df):
    set_context_token(token)
    try:
        if candidates_df is None or candidates_df.empty:
            raise gr.Error("No candidates to apply.")
        
        plugin = get_plugin(project_id, location)
        updates = []
        for _, row in candidates_df.iterrows():
            if 'Target Column' in row and 'Proposed Description' in row:
                updates.append({
                    "table": target_table,
                    "column": row['Target Column'],
                    "description": row['Proposed Description']
                })
            
        if not updates:
            gr.Warning("No valid updates selected.")
            return "No valid updates selection."

        plugin.apply_propagation(dataset_id, updates)
        return f"Successfully applied {len(updates)} updates to {target_table}!"
    except Exception as e:
        logger.error(f"Apply failed: {e}")
        raise gr.Error(f"Apply failed: {str(e)}")

with gr.Blocks(title="Agentic Data Steward") as demo:
    gr.Markdown("# 🛡️ Agentic Data Steward")
    
    with gr.Accordion("Global Environment Settings", open=True):
        with gr.Row():
            config_project = gr.Textbox(label="Project ID", value=DEFAULT_PROJECT_ID)
            config_location = gr.Textbox(label="Location", value=DEFAULT_LOCATION)
            auth_token = gr.Textbox(label="OAuth Token (Optional)", type="password", placeholder="ya29.a0...")
    
    with gr.Tabs():
        with gr.TabItem("Dashboard"):
            gr.Markdown("## 📋 Data Estate Overview")
            with gr.Group():
                with gr.Row():
                    global_dataset = gr.Textbox(
                        label="Active Dataset ID", 
                        value=DEFAULT_DATASET_ID,
                        placeholder="e.g. retail_synthetic_data",
                        info="This dataset will be used across all tabs."
                    )
            
            gr.Markdown("---")
            gr.Markdown("### 🔍 Metadata Completeness Scan")
            with gr.Row():
                scan_btn = gr.Button("Scan Dataset", variant="secondary")
            
            dash_output = gr.Dataframe(
                label="Columns Missing Descriptions",
                interactive=False,
                wrap=True
            )
            
            scan_btn.click(
                scan_dataset, 
                inputs=[auth_token, config_project, config_location, global_dataset], 
                outputs=dash_output
            )

        with gr.TabItem("Lineage Propagation"):
            gr.Markdown("## 🧬 Analyze & Propagate Metadata")
            with gr.Row():
                prop_table = gr.Textbox(label="Target Table", value="transactions")
            
            preview_btn = gr.Button("Analyze Lineage & Preview Propagation", variant="primary")
            
            summary_output = gr.Markdown("Enter a table and click the button above to start analysis.")
            preview_output = gr.Dataframe(label="Propagation Candidates", interactive=True, wrap=True)
            
            with gr.Row():
                apply_btn = gr.Button("Apply Selection to BigQuery", variant="primary")
            
            apply_result = gr.Textbox(label="Apply Status", interactive=False)
            
            preview_btn.click(
                analyze_and_preview, 
                inputs=[auth_token, config_project, config_location, global_dataset, prop_table], 
                outputs=[summary_output, preview_output]
            )
            apply_btn.click(
                apply_propagation_improved, 
                inputs=[auth_token, config_project, config_location, global_dataset, prop_table, preview_output], 
                outputs=apply_result
            )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
