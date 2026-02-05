import argparse
import sys
import os
import pandas as pd
from typing import List, Dict, Any

# Add necessary paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'agent/plugins')))

try:
    from lineage_plugin import LineagePlugin
except ImportError:
    print("Error: Could not import LineagePlugin. Ensure you are running from the project root.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Agentic Data Steward CLI")
    parser.add_argument("--project", "--project_id", dest="project", default=os.environ.get("GOOGLE_CLOUD_PROJECT", "governance-agent"), help="GCP Project ID")
    parser.add_argument("--location", default="europe-west1", help="GCP Location")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan dataset for missing descriptions")
    scan_parser.add_argument("--dataset", "--dataset_id", dest="dataset", required=True, help="BigQuery Dataset ID")
    
    # Apply command
    apply_parser = subparsers.add_parser("apply", help="Preview and apply propagation for a table")
    apply_parser.add_argument("--dataset", "--dataset_id", dest="dataset", required=True, help="BigQuery Dataset ID")
    apply_parser.add_argument("--table", "--table_id", dest="table", required=True, help="BigQuery Table ID")
    apply_parser.add_argument("--yes", action="store_true", help="Apply updates without confirmation")
    
    args = parser.parse_args()
    
    plugin = LineagePlugin(args.project, args.location)
    
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
            
        print("\nProposed Updates:")
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
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
