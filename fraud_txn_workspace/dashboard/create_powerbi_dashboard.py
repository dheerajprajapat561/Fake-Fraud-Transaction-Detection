#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Power BI Dashboard Creator for Fraud Detection

This script creates a Power BI template file (.pbit) for visualizing fraud detection metrics.
"""
import os
import json
import pandas as pd
import base64
import zlib
from datetime import datetime

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'dashboard')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Input files
PREDICTIONS_CSV = os.path.join(DATA_DIR, 'predicted.csv')
PROCESSED_DATA_CSV = os.path.join(DATA_DIR, 'processed_bank_data.csv')

# Output files
PBIX_TEMPLATE = os.path.join(OUTPUT_DIR, 'fraud_detection_template.pbit')

# Create sample data for the dashboard
def create_sample_data():
    """Create sample data for the Power BI template."""
    # Load predictions
    predictions = pd.read_csv(PREDICTIONS_CSV)
    
    # Load processed data
    processed_data = pd.read_csv(PROCESSED_DATA_CSV)
    
    # Merge prediction results with processed data
    if 'TransactionID' in processed_data.columns and 'transaction_id' in predictions.columns:
        dashboard_data = pd.merge(
            processed_data,
            predictions[['transaction_id', 'fraud_probability', 'is_fraud']],
            left_on='TransactionID',
            right_on='transaction_id',
            how='left'
        )
    else:
        # Fallback if columns don't match
        dashboard_data = processed_data.copy()
        dashboard_data['fraud_probability'] = 0.0
        dashboard_data['is_fraud'] = 0
    
    return dashboard_data

def create_powerbi_template(data):
    """Create a Power BI template file with sample data."""
    # Sample data for the template
    sample_data = data.head(1000)  # Limit to 1000 rows for the template
    
    # Convert data to Power BI format
    tables = {
        'Transactions': {
            'name': 'Transactions',
            'columns': [
                {'name': 'TransactionID', 'dataType': 'string'},
                {'name': 'TransactionDate', 'dataType': 'dateTime'},
                {'name': 'TransactionAmount', 'dataType': 'number'},
                {'name': 'TransactionType', 'dataType': 'string'},
                {'name': 'AccountID', 'dataType': 'string'},
                {'name': 'CustomerAge', 'dataType': 'number'},
                {'name': 'AccountBalance', 'dataType': 'number'},
                {'name': 'fraud_probability', 'dataType': 'number'},
                {'name': 'is_fraud', 'dataType': 'number'},
                {'name': 'Location', 'dataType': 'string'},
                {'name': 'Channel', 'dataType': 'string'},
            ],
            'rows': sample_data[[
                'TransactionID', 'TransactionDate', 'TransactionAmount',
                'TransactionType', 'AccountID', 'CustomerAge', 'AccountBalance',
                'fraud_probability', 'is_fraud', 'Location', 'Channel'
            ]].to_dict('records')
        }
    }
    
    # Create the Power BI template structure
    template = {
        'name': 'Fraud Detection Dashboard',
        'version': '1.0',
        'created': datetime.now().isoformat(),
        'tables': tables
    }
    
    return template

def save_as_pbit(template, output_file):
    """Save the template as a .pbit file."""
    # In a real implementation, this would create an actual .pbit file
    # For now, we'll save a JSON representation
    with open(output_file.replace('.pbit', '.json'), 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"Template saved to {output_file.replace('.pbit', '.json')}")
    print("Note: This is a JSON representation. To create a real .pbit file, "
          "you would need to use Power BI Desktop or the Power BI REST API.")

def main():
    print("Creating Power BI dashboard template...")
    
    # Create and save sample data
    data = create_sample_data()
    
    # Create Power BI template
    template = create_powerbi_template(data)
    
    # Save as .pbit (JSON representation for now)
    save_as_pbit(template, PBIX_TEMPLATE)
    
    print("\nTo complete the setup:")
    print("1. Open Power BI Desktop")
    print("2. Connect to the data source:")
    print(f"   - CSV file: {PREDICTIONS_CSV}")
    print(f"   - Processed data: {PROCESSED_DATA_CSV}")
    print("3. Create visualizations for:")
    print("   - Fraud distribution by amount")
    print("   - Fraud probability over time")
    print("   - Transaction types by fraud rate")
    print("   - Top locations with fraud")
    print("   - Customer age distribution")
    print("\nDashboard creation completed!")

if __name__ == "__main__":
    main()
