#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate Power BI Report JSON

This script generates a JSON file that can be used as a starting point
for a Power BI report with fraud detection visualizations.
"""
import json
import os
from datetime import datetime

# Output file
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'fraud_detection_report.json')

# Report template
report = {
    "name": "Fraud Detection Dashboard",
    "version": "1.0",
    "created": datetime.now().isoformat(),
    "pages": [
        {
            "name": "Overview",
            "visuals": [
                {
                    "type": "card",
                    "title": "Total Transactions",
                    "measure": "COUNT(Transactions[TransactionID])",
                    "format": "#,0"
                },
                {
                    "type": "card",
                    "title": "Fraudulent Transactions",
                    "measure": "SUM(Transactions[Is_Fraud])",
                    "format": "#,0"
                },
                {
                    "type": "card",
                    "title": "Fraud Rate",
                    "measure": "DIVIDE(SUM(Transactions[Is_Fraud]), COUNT(Transactions[TransactionID]))",
                    "format": "0.00%"
                },
                {
                    "type": "line_chart",
                    "title": "Fraud Trend",
                    "x_axis": "Transactions[Transaction_Date]",
                    "y_axis": "COUNT(Transactions[TransactionID])",
                    "legend": "Transactions[Is_Fraud]"
                }
            ]
        },
        {
            "name": "Transaction Analysis",
            "visuals": [
                {
                    "type": "scatter_chart",
                    "title": "Amount vs. Fraud Probability",
                    "x_axis": "Transactions[TransactionAmount]",
                    "y_axis": "Transactions[Fraud_Probability]",
                    "size": "Transactions[TransactionAmount]"
                },
                {
                    "type": "bar_chart",
                    "title": "Fraud by Transaction Type",
                    "x_axis": "Transactions[TransactionType]",
                    "y_axis": "COUNT(Transactions[TransactionID])",
                    "legend": "Transactions[Is_Fraud]"
                }
            ]
        },
        {
            "name": "Customer Insights",
            "visuals": [
                {
                    "type": "bar_chart",
                    "title": "Fraud by Age Group",
                    "x_axis": "Transactions[CustomerAge]",
                    "y_axis": "COUNT(Transactions[TransactionID])",
                    "legend": "Transactions[Is_Fraud]"
                },
                {
                    "type": "table",
                    "title": "High-Risk Transactions",
                    "columns": [
                        "TransactionID",
                        "TransactionDate",
                        "TransactionAmount",
                        "Fraud_Probability",
                        "TransactionType"
                    ],
                    "sort": {"column": "Fraud_Probability", "descending": True},
                    "top_n": 10
                }
            ]
        }
    ],
    "slicers": [
        {
            "field": "Transactions[TransactionDate]",
            "type": "date_range"
        },
        {
            "field": "Transactions[TransactionType]",
            "type": "multi_select"
        },
        {
            "field": "Transactions[Fraud_Risk]",
            "type": "multi_select"
        }
    ]
}

def main():
    """Generate the Power BI report JSON file."""
    try:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"Successfully generated Power BI report template: {OUTPUT_FILE}")
        print("\nTo use this template:")
        print("1. Open Power BI Desktop")
        print("2. Click on 'Transform data' and set up your data source")
        print("3. Create the visualizations as described in the README")
    except Exception as e:
        print(f"Error generating report: {str(e)}")

if __name__ == "__main__":
    main()
