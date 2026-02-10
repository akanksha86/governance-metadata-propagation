import argparse
import sys
import os
import pandas as pd
from typing import List, Dict, Any

# Add necessary paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'agent/plugins')))

try:
    from lineage_plugin import LineagePlugin
    from glossary_plugin import GlossaryPlugin
except ImportError:
    print("Error: Could not import Plugins. Ensure you are running from the project root.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Agentic Data Steward CLI")
    parser.add_argument("--project", "--project_id", dest="project", default=os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent"), help="GCP Project ID")
    parser.add_argument("--location", default="europe-west1", help="GCP Location")
    parser.add_argument("--yes", "-y", action="store_true", help="Automatically approve all prompts")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan dataset for missing descriptions")
    scan_parser.add_argument("--dataset", "--dataset_id", dest="dataset", required=True, help="BigQuery Dataset ID")
    
    # Apply command
    apply_parser = subparsers.add_parser("apply", help="Preview and apply description propagation for a table")
    apply_parser.add_argument("--dataset", "--dataset_id", dest="dataset", required=True, help="BigQuery Dataset ID")
    apply_parser.add_argument("--table", "--table_id", dest="table", required=True, help="BigQuery Table ID")
    # Glossary recommend command
    glossary_parser = subparsers.add_parser("glossary-recommend", help="Recommend glossary terms for a table")
    glossary_parser.add_argument("--dataset", "--dataset_id", dest="dataset", required=True, help="BigQuery Dataset ID")
    glossary_parser.add_argument("--table", "--table_id", dest="table", required=True, help="BigQuery Table ID")
    
    args = parser.parse_args()
    
    plugin = LineagePlugin(args.project, args.location)
    glossary_plugin = GlossaryPlugin(args.project, args.location)
    
    if args.command == "scan":
        print(f"Scanning dataset '{args.dataset}' in project '{args.project}'...")
        df = plugin.scan_for_missing_descriptions(args.dataset)
        if df.empty:
            print("No missing descriptions found!")
        else:
            print("\nMissing Descriptions:")
            print(df.to_string(index=False))
            
    elif args.command == "apply":
        print(f"Analyzing lineage for '{args.dataset}.{args.table}'...")
        df = plugin.preview_propagation(args.dataset, args.table)
        
        if df.empty:
            print("No propagation candidates found.")
            return
            
        print("\nProposed Description Updates:")
        # Display relevant columns
        display_df = df[["Target Column", "Source", "Proposed Description", "Confidence"]]
        print(display_df.to_string(index=False))
        
        if args.yes:
            do_apply = True
        else:
            confirm = input("\nApply these updates to BigQuery? (y/N): ")
            do_apply = confirm.lower() == 'y'
            
        if do_apply:
            print("Applying updates...")
            updates = []
            for _, row in df.iterrows():
                updates.append({
                    "table": args.table,
                    "column": row["Target Column"],
                    "description": row["Proposed Description"]
                })
            plugin.apply_propagation(args.dataset, updates)
            print("Successfully updated metadata in BigQuery.")
        else:
            print("Operation cancelled.")
            
    elif args.command == "glossary-recommend":
        print(f"Fetching glossary recommendations for '{args.dataset}.{args.table}'...")
        df = glossary_plugin.recommend_terms_for_table(args.dataset, args.table)
        
        if df.empty:
            print("No recommendations found.")
        else:
            print("\nGlossary Term Recommendations:")
            print(df[["Column", "Suggested Term", "Confidence", "Rationale"]].to_string(index=False))
            print("\nNote: Use the UI or a separate apply command to persist these mappings.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
