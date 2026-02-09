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
from glossary_plugin import GlossaryPlugin
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
        lineage_plugin = get_plugin(project_id, location)
        glossary_plugin = GlossaryPlugin(project_id, location)
        
        # 1. Scan for missing technical descriptions
        desc_df = lineage_plugin.scan_for_missing_descriptions(dataset_id)
        
        # 2. Scan for missing glossary terms
        glossary_df = glossary_plugin.scan_for_missing_glossary_terms(dataset_id)
        
        # 3. Aggregate by Table for a cleaner UI
        desc_agg = desc_df.groupby('Table').size().reset_index(name='Missing Descriptions') if not desc_df.empty else pd.DataFrame(columns=['Table', 'Missing Descriptions'])
        gloss_agg = glossary_df.groupby('Table').size().reset_index(name='Missing Glossary Mappings') if not glossary_df.empty else pd.DataFrame(columns=['Table', 'Missing Glossary Mappings'])
        
        # 4. Generate Natural Language Summary
        desc_count = len(desc_df)
        gloss_count = len(glossary_df)
        
        if desc_count == 0 and gloss_count == 0:
            summary = "✅ **Metadata Estate is Complete!** All objects have both technical descriptions and business glossary mappings."
        else:
            summary = f"### 📊 Governance Gap Analysis\n"
            summary += f"We found **{desc_count}** column gaps in technical descriptions and **{gloss_count}** column gaps in business glossary mappings.\n\n"
            
            if not desc_agg.empty:
                summary += f"🔍 **Technical Gaps**: {len(desc_agg)} objects affected.\n"
            if not gloss_agg.empty:
                summary += f"📖 **Business Gaps**: {len(gloss_agg)} objects affected.\n"
            
            summary += "\n*Detailed column recommendations are available in the 'Lineage Propagation' and 'Glossary Recommendations' tabs.*"

        return summary, desc_agg, gloss_agg
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
            return summary, pd.DataFrame(columns=["Select", "Target Column", "Source", "Source Column", "Confidence", "Proposed Description", "Type"])
            
        # Add selection column
        df.insert(0, "Select", True)
        return summary, df
    except Exception as e:
        logger.error(f"Analyze & Preview failed: {e}")
        raise gr.Error(f"Operation failed: {str(e)}")

def get_glossary_recommendations(project_id, location, dataset_id, table_id, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
    try:
        plugin = GlossaryPlugin(project_id, location)
        df = plugin.recommend_terms_for_table(dataset_id, table_id)
        if df.empty:
            gr.Info(f"No glossary recommendations found for {table_id}.")
            return pd.DataFrame(columns=["Select", "Column", "Suggested Term", "Confidence", "Rationale", "Term ID"])
        
        # Add selection column
        df.insert(0, "Select", True)
        return df
    except Exception as e:
        logger.error(f"Glossary recommendations failed: {e}")
        raise gr.Error(f"Operation failed: {str(e)}")

def apply_propagation_improved(project_id, location, dataset_id, target_table, candidates_df, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
    try:
        if candidates_df is None or candidates_df.empty:
            raise gr.Error("No candidates to apply.")
        
        # Filter selected rows
        selected = candidates_df[candidates_df["Select"] == True]
        if selected.empty:
            gr.Warning("No columns selected for application.")
            return "No columns selected."
            
        plugin = get_plugin(project_id, location)
        updates = []
        for _, row in selected.iterrows():
            if 'Target Column' in row and 'Proposed Description' in row:
                updates.append({
                    "table": target_table,
                    "column": row['Target Column'],
                    "description": row['Proposed Description']
                })
            
        if not updates:
            return "No valid updates found in selection."

        plugin.apply_propagation(dataset_id, updates)
        return f"Successfully applied {len(updates)} updates to {target_table}!"
    except Exception as e:
        logger.error(f"Apply failed: {e}")
        raise gr.Error(f"Apply failed: {str(e)}")

def apply_glossary_selections(project_id, location, dataset_id, table_id, reco_df, request: gr.Request = None):
    token = get_token_from_session(request)
    set_oauth_token(token)
    try:
        if reco_df is None or reco_df.empty:
            raise gr.Error("No recommendations to apply.")
        
        # Filter selected rows
        # Ensure 'Select' column is treated as boolean
        reco_df["Select"] = reco_df["Select"].astype(bool)
        selected = reco_df[reco_df["Select"] == True]
        if selected.empty:
            gr.Warning("No terms selected for application.")
            return "No terms selected."

        plugin = GlossaryPlugin(project_id, location)
        updates = []
        for _, row in selected.iterrows():
            updates.append({
                "column": row['Column'],
                "term_id": row['Term ID'],
                "term_display": row['Suggested Term']
            })
        
        plugin.apply_terms(dataset_id, table_id, updates)
        return f"Successfully applied {len(updates)} glossary terms to {table_id} in Dataplex!"
    except Exception as e:
        logger.error(f"Glossary apply failed: {e}")
        raise gr.Error(f"Apply failed: {str(e)}")

def toggle_all_selection(df, value):
    """Universal helper to toggle a 'Select' column in a dataframe."""
    if df is not None and not df.empty:
        df["Select"] = value
    return df

def select_all_lineage(df):
    return toggle_all_selection(df, True)

def deselect_all_lineage(df):
    return toggle_all_selection(df, False)

def select_all_glossary(df):
    return toggle_all_selection(df, True)

def deselect_all_glossary(df):
    return toggle_all_selection(df, False)

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
            
            dash_summary = gr.Markdown("Click 'Scan Dataset' to analyze metadata completeness.")
            
            with gr.Row():
                with gr.Column():
                    desc_output = gr.Dataframe(
                        label="❌ Missing Technical Descriptions",
                        interactive=False,
                        wrap=True
                    )
                with gr.Column():
                    glossary_gap_output = gr.Dataframe(
                        label="❌ Missing Glossary Mappings",
                        interactive=False,
                        wrap=True
                    )
            
            scan_btn.click(
                scan_dataset, 
                inputs=[config_project, config_location, global_dataset], 
                outputs=[dash_summary, desc_output, glossary_gap_output]
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
                wrap=True,
                datatype=["bool", "str", "str", "str", "number", "str", "str"]
            )
            
            with gr.Row():
                select_all_lineage_btn = gr.Button("Select All", size="sm")
                deselect_all_lineage_btn = gr.Button("Deselect All", size="sm")
            
            with gr.Row():
                apply_btn = gr.Button("Apply Selection to BigQuery", variant="primary")
            
            apply_result = gr.Textbox(label="Apply Status", interactive=False)

            select_all_lineage_btn.click(select_all_lineage, inputs=[preview_output], outputs=[preview_output])
            deselect_all_lineage_btn.click(deselect_all_lineage, inputs=[preview_output], outputs=[preview_output])
            
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

        with gr.TabItem("Glossary Recommendations"):
            gr.Markdown("## 📖 Business Glossary Mapping")
            gr.Markdown("Recommends mappings of columns to business glossary terms across tables.")
            
            with gr.Row():
                glossary_table = gr.Textbox(label="Target Table", value="customers")
            
            recommend_btn = gr.Button("Get Glossary Recommendations", variant="primary")
            
            recommendations_view = gr.Dataframe(
                label="Glossary Recommendations (Select to apply)",
                interactive=True,
                wrap=True,
                datatype=["bool", "str", "str", "number", "str", "str"]
            )
            
            with gr.Row():
                select_all_glossary_btn = gr.Button("Select All", size="sm")
                deselect_all_glossary_btn = gr.Button("Deselect All", size="sm")
            
            with gr.Row():
                apply_glossary_btn = gr.Button("Apply Selected Terms to Dataplex", variant="primary")
            
            glossary_apply_result = gr.Textbox(label="Apply Status", interactive=False)

            select_all_glossary_btn.click(select_all_glossary, inputs=[recommendations_view], outputs=[recommendations_view])
            deselect_all_glossary_btn.click(deselect_all_glossary, inputs=[recommendations_view], outputs=[recommendations_view])
            
            recommend_btn.click(
                get_glossary_recommendations,
                inputs=[config_project, config_location, global_dataset, glossary_table],
                outputs=recommendations_view
            )
            
            apply_glossary_btn.click(
                apply_glossary_selections,
                inputs=[config_project, config_location, global_dataset, glossary_table, recommendations_view],
                outputs=glossary_apply_result
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
