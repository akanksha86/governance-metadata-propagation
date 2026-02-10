# Governance Metadata Propagation Demo

This project demonstrates an agentic data governance solution using Google Cloud Dataplex. It showcases how to automate metadata management, propagate insights via lineage, and leverage Dataplex **Dataset Insights** capabilities. This is only a demonstration and is not part of official product, please review everything before using it for your environments and use-cases.

---

## 🌟 Key Features

*   **Estate Dashboard**: Scan BigQuery datasets to identify metadata gaps (missing descriptions).
*   **Recursive Description Propagation**: Automatically fetch descriptions from upstream sources, bridging multi-hop gaps.
*   **SQL-Based Logic Enrichment**: Extracts BigQuery SQL transformations to generate human-readable descriptions for computed columns.
*   **AI Business Glossary**: Maps technical columns to business terms using Vertex AI Semantic Similarity.
*   **Native Dataplex Integration**: Persists glossary mappings as native `EntryLinks` visible in the Dataplex Schema tab.
*   **Unified UI & CLI**: Manage governance tasks via a Gradio-based web app or a headless CLI.

---

## 🛠 Setup & Installation

### Prerequisites
- Python 3.12+
- Google Cloud Project with billing enabled.
- APIs Enabled: `dataplex`, `bigquery`, `datacatalog`, `datalineage`, `aiplatform`.

### Installation
1.  **Clone & Navigate**:
    ```bash
    git clone <repo-url>
    cd governance-metadata-propagation
    ```
2.  **Environment Setup**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
3.  **Authentication**:
    - **CLI/Dev**: Run `gcloud auth application-default login`.
    - **Gradio App**: Follow [OAUTH_SETUP_GUIDE.md](OAUTH_SETUP_GUIDE.md) to enable "Login with Google".

---

## 🚀 Usage Guide

### 1. Agentic Data Steward (UI)
The Gradio app provides a visual way to scan and apply metadata changes.
```bash
python3 ui/gradio_app.py
```
- **Dashboard**: Run "Scan Dataset" to see health metrics.
- **Description Propagation**: Enter a table name to preview and apply lineage-based descriptions.
- **Settings**: Toggle OAuth/ADC modes for specific user actions.

### 2. Steward CLI (Headless)
The CLI is designed for automation and quick scans.
```bash
# Scan a dataset for missing descriptions
python3 steward_cli.py scan --dataset retail_syn_data

# Preview and apply description propagation to a table
python3 steward_cli.py apply --dataset retail_syn_data --table transactions

# Recommend glossary terms using Vertex AI Semantic Similarity
python3 steward_cli.py glossary-recommend --dataset retail_syn_data --table transactions
```

### 3. Data Integration Scripts
- **Generate Data**: `python3 data_generation/generate_data.py` (Creates tables + lineage).
- **Dataset Insights**: `python3 dataplex_integration/dataset_insights.py` (Applies dataset-level labels).
  > [!NOTE]
  > The **Dataset Insights** script is not yet fully integrated into the automated propagation flow; currently, the system relies on the pre-generated `dataset_insights_sample.json` for lineage enrichment.
- **Table Insights**: `python3 dataplex_integration/manage_insights.py` (Automates documentation scans).

---

## 🧩 Project Modules

| Module | Location | Description |
| :--- | :--- | :--- |
| **Glossary Plugin** | `agent/plugins/glossary_plugin.py` | Handles Business Glossary mapping using Vertex AI. |
| **Lineage Plugin** | `agent/plugins/lineage_plugin.py` | Orchestrates description propagation via Lineage API. |
| **Similarity Engine** | `agent/plugins/similarity_engine.py` | AI logic for scoring lexical and semantic matches. |
| **Traverser** | `dataplex_integration/lineage_propagation.py` | Low-level Graph API logic for traversing dependencies. |
| **Enricher** | `dataplex_integration/lineage_propagation.py` | Context-aware SQL transformation analyzer. |

---

## 💡 Workflow Example

1.  **Initialize**: Generate synthetic data and lineage relationships.
2.  **Enrich**: Run **Dataset Insights** and Table Insight scans to populate initial metadata.
3.  **Propagate**: Use the **Steward App** or **CLI** to bridge description gaps across the lineage chain.
4.  **Tag**: Use the **Glossary Plugin** to map technical columns to the Business Glossary for Dataplex UI visibility.
5.  **Verify**: Check the **BigQuery Console** (Descriptions) and **Dataplex Schema** (Business Terms).
