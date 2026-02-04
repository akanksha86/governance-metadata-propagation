# Governance Metadata Propagation Demo

This project demonstrates an agentic data governance solution using Google Cloud Dataplex. It showcases how to automate metadata management, propagate insights via lineage, and leverage Dataplex "Knowledge Engine" capabilities.

## Overview

The solution focuses on:
1.  **Synthetic Data Generation**: Creating complex retail data with built-in lineage relationships (e.g., `raw_transactions` -> `transactions`).
2.  **Dataset & Table Insights**: Automating Data Documentation scans to generate column-level descriptions and dataset-level entity relationships.
3.  **Agentic Data Steward**: A Gradio-based application for data stewards to scan, analyze, and propagate metadata globally.
4.  **Multi-Hop Lineage Propagation**: Propagates metadata across multiple transformation steps, even through undocumented intermediate tables or views.
- **SQL-Based Logic Enrichment**: Extracts actual BigQuery SQL transformations to generate human-readable descriptions for computed columns.
- **Gradio User Interface**: A streamlined UI for data stewards to scan, preview, and apply metadata changes.
- **Steward CLI**: Command-line interface for headless scanning and propagation.

## Getting Started

### Prerequisites
- Python 3.8+
- Google Cloud Project with BigQuery and Data Lineage API enabled
- Application Default Credentials (ADC) or OAuth Token

### Setup
1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage
#### Gradio UI
```bash
python app.py
```

#### Steward CLI
```bash
# Scan for missing descriptions
python steward_cli.py scan --dataset retail_syn_data

# Preview and apply propagation
python steward_cli.py apply --dataset retail_syn_data --table transactions
```

## Prerequisites

*   Google Cloud Project with billing enabled.
*   APIs Enabled:
    *   Dataplex API (`dataplex.googleapis.com`)
    *   BigQuery API (`bigquery.googleapis.com`)
    *   Data Catalog API (`datacatalog.googleapis.com`)
    *   Data Lineage API (`datalineage.googleapis.com`)
*   Python 3.8+

## Setup

1.  Clone the repository and navigate to the project root.
2.  Set your Google Cloud Project ID:
    ```bash
    export GOOGLE_CLOUD_PROJECT=your-project-id
    ```
3.  Create and activate a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
4.  Install dependencies:
    ```bash
    pip install -r dataplex_integration/requirements.txt
    pip install gradio pandas requests google-cloud-datacatalog-lineage
    ```

## 🚀 The Agentic Data Steward App

The core of this project is now a Gradio-based application that provides a unified interface for data governance.

### How to Run
```bash
python3 ui/gradio_app.py
```

### Key Features
*   **Estate Dashboard**: Scan your BigQuery datasets to identify columns missing descriptions.
*   **Lineage Insights**: Analyze any table to see a holistic summary of its **Upstream sources** and **Downstream targets**.
*   **Metadata Propagation**: Automatically fetch descriptions from upstream, enrich them with transformation logic, and apply them back to BigQuery with one click.
*   **Multi-Hop Discovery**: Recursive lineage traversal that "hops" over missing descriptions in intermediate tables to find the root source.
*   **OAuth Support**: Optional "Global Environment Settings" allow you to provide an OAuth token for user-specific actions. See [OAUTH_SETUP_GUIDE.md](OAUTH_SETUP_GUIDE.md) for more info.

## Key Components

### 1. Data Generation
**Script**: `data_generation/generate_data.py`

Generates synthetic retail data (Customers, Products, Orders, Transactions) and loads it into BigQuery. It performs SQL transformations (`CTAS`) and creates views (e.g., `products_v`) to establish multi-hop lineage chains.

**Usage**:
```bash
python3 data_generation/generate_data.py
```

### 2. Dataset Insights (Knowledge Engine)
**Script**: `dataplex_integration/knowledge_engine.py`

Implements **Dataset-level** Data Documentation scans (often referred to as "Knowledge Engine").
*   Creates a single scan for the entire `retail_syn_data` dataset.
*   **Crucial Feature**: Automatically applies the required `dataplex-data-documentation-published-*` labels to the **BigQuery Dataset**.
*   This ensures that dataset-level insights (e.g., entity relationship diagrams, common join patterns) are published back to BigQuery.

**Usage**:
```bash
python3 dataplex_integration/knowledge_engine.py
```

### 3. Table Insights
**Script**: `dataplex_integration/manage_insights.py`

Implements **Table-level** Data Documentation scans.
*   Iterates through key tables (`raw_customers`, `raw_transactions`, etc.).
*   Creates Data Documentation scans for each table.
*   **Crucial Feature**: Automatically applies the required `dataplex-data-documentation-published-*` labels to the **BigQuery Table**.
*   This ensures column-level descriptions and other table-specific insights are published to BigQuery metadata.

**Usage**:
```bash
python3 dataplex_integration/manage_insights.py
```

### 4. Lineage Propagation & Enrichment
**Script**: `dataplex_integration/propagate_metadata.py`

Propagates metadata (descriptions) from upstream sources to downstream targets.
*   **Core Feature**: Uses Dataplex **Data Lineage API** to find dependencies.
*   **Enrichment**: Can consume **Knowledge Engine** insights (Schema Relationships) to bridge lineage gaps (e.g., inferred joins not yet captured by lineage).
*   **Modes**:
    *   `report`: Logs what would be propagated (dry-run).
    *   `apply`: Actually updates BigQuery column descriptions.

**Usage (Enriched Propagation)**:
```bash
python3 dataplex_integration/propagate_metadata.py \
  --project_id $GOOGLE_CLOUD_PROJECT \
  --dataset_id retail_syn_data \
  --target_table transactions \
  --knowledge_json dataplex_integration/knowledge_engine_sample.json \
  --mode report
```
*Note: We currently use `knowledge_engine_sample.json` to simulate insights while live extraction is being configured.*

## Workflow Example

1.  **Generate Data**: `python3 data_generation/generate_data.py` (Creates tables + lineage).
2.  **Run Dataset Insights**: `python3 dataplex_integration/knowledge_engine.py` (Scans dataset).
3.  **Run Table Insights**: `python3 dataplex_integration/manage_insights.py` (Scans tables).
4.  **Run Propagation**: Use `propagate_metadata.py` to enrich and apply metadata.
5.  **Verify**: Check BigQuery Console. You should see descriptions populated on your tables and columns, and "Insights" tabs available in Dataplex/BigQuery.
