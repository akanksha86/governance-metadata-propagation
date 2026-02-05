import gradio as gr
import fastapi
from fastapi.responses import RedirectResponse
import sys
import os
import pandas as pd
import logging
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Custom OAuth Setup ---
oauth_config = OAuth()
oauth_config.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={
        'scope': 'openid email profile https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform'
    }
)
# ---------------------------

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

def get_token_from_session(request: gr.Request):
    if request:
        return request.session.get("google_token", {}).get("access_token")
    return None

def scan_dataset(project_id, location, dataset_id, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
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

def analyze_and_preview(project_id, location, dataset_id, target_table, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
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

def apply_propagation_improved(project_id, location, dataset_id, target_table, candidates_df, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
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

def check_auth_status(request: gr.Request):
    if request and "google_token" in request.session:
        # Hide login, show app
        return gr.update(visible=False), gr.update(visible=True)
    return gr.update(visible=True), gr.update(visible=False)


with gr.Blocks(title="Agentic Data Steward") as demo:
    # 1. Login View
    with gr.Column(visible=True) as login_view:
        gr.Markdown("# 🛡️ Agentic Data Steward")
        gr.Markdown("Please log in with your Google account to access the governance tools.")
        login_html = """
        <a href="/google_login" style="
            display: inline-block;
            background-color: #4285F4;
            color: white;
            padding: 10px 24px;
            text-decoration: none;
            border-radius: 4px;
            font-family: 'Roboto', sans-serif;
            font-weight: 500;
        ">Login with Google</a>
        """
        gr.HTML(login_html)

    # 2. Main App View
    with gr.Column(visible=False) as app_view:
        with gr.Row():
            with gr.Column(scale=8):
                gr.Markdown("# 🛡️ Agentic Data Steward")
            with gr.Column(scale=2):
                logout_html = '<a href="/logout" style="color: #666; text-decoration: underline;">Logout</a>'
                gr.HTML(logout_html)
        
        with gr.Accordion("Global Environment Settings", open=True):
            with gr.Row():
                config_project = gr.Textbox(label="Project ID", value=DEFAULT_PROJECT_ID)
                config_location = gr.Textbox(label="Location", value=DEFAULT_LOCATION)
    
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
                inputs=[config_project, config_location, global_dataset], 
                outputs=dash_output
            )

        with gr.TabItem("Lineage Propagation"):
            gr.Markdown("## 🧬 Analyze & Propagate Metadata")
            with gr.Row():
                prop_table = gr.Textbox(label="Target Table", value="transactions")
            
            preview_btn = gr.Button("Analyze Lineage & Preview Propagation", variant="primary")
            
            summary_output = gr.Markdown("Enter a table and click the button above to start analysis.")
            gr.Markdown("*(Optional: Click any cell in the **Proposed Description** column to refine it before applying)*")
            preview_output = gr.Dataframe(
                label="Propagation Candidates (Edit 'Proposed Description' only)", 
                interactive=True, 
                wrap=True
            )
            
            with gr.Row():
                apply_btn = gr.Button("Apply Selection to BigQuery", variant="primary")
            
            apply_result = gr.Textbox(label="Apply Status", interactive=False)
            
            preview_btn.click(
                analyze_and_preview, 
                inputs=[config_project, config_location, global_dataset, prop_table], 
                outputs=[summary_output, preview_output]
            )
            apply_btn.click(
                apply_propagation_improved, 
                inputs=[config_project, config_location, global_dataset, prop_table, preview_output], 
                outputs=apply_result
            )

    # Auth logic: Show info on load if already logged in
    demo.load(check_auth_status, outputs=[login_view, app_view])

if __name__ == "__main__":
    from fastapi import FastAPI
    main_app = FastAPI()
    
    # Add Session Middleware
    main_app.add_middleware(SessionMiddleware, secret_key="some-secret-key-for-auth-propagation")

    @main_app.get("/google_login")
    async def login(request: fastapi.Request):
        # Allow override from .env if needed, but default to /google_callback
        redirect_uri = os.environ.get("GOOGLE_REDIRECT_URI", "http://localhost:7860/google_callback")
        logger.info(f"Initiating login with redirect_uri: {redirect_uri}")
        return await oauth_config.google.authorize_redirect(request, redirect_uri)

    @main_app.get("/google_callback")
    async def auth_callback(request: fastapi.Request):
        try:
            token = await oauth_config.google.authorize_access_token(request)
            request.session["google_token"] = token
            logger.info("Successfully received token and stored in session.")
            return RedirectResponse(url="/")
        except Exception as e:
            logger.error(f"Auth callback failed: {e}")
            return RedirectResponse(url="/?error=auth_failed")

    @main_app.get("/logout")
    async def logout(request: fastapi.Request):
        request.session.pop("google_token", None)
        return RedirectResponse(url="/")

    # Mount Gradio AFTER defining custom routes
    app = gr.mount_gradio_app(main_app, demo, path="/")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)
