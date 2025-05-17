# Fraud Detection Power BI Dashboard

This directory contains resources for creating a Power BI dashboard to visualize fraud detection metrics and insights.

## Prerequisites

1. Install [Power BI Desktop](https://powerbi.microsoft.com/en-us/desktop/)
2. Python 3.7+ with required packages (pandas, numpy)
3. Access to the processed data files:
   - `fraud_txn_workspace/data/processed_bank_data.csv`
   - `fraud_txn_workspace/data/predicted.csv`

## Setup Instructions

1. **Run the dashboard creation script**:
   ```bash
   python create_powerbi_dashboard.py
   ```

2. **Open Power BI Desktop** and connect to the data sources:
   - Click "Get Data" > "CSV"
   - Select both CSV files from the data directory
   - Click "Load"

3. **Create the following visualizations**:

### 1. Fraud Overview
- **Card**: Total Transactions
- **Card**: Fraudulent Transactions
- **Card**: Fraud Rate
- **Gauge**: Overall Fraud Probability

### 2. Fraud by Transaction Type
- **Bar Chart**: Transaction Type vs. Fraud Count
- **Line Chart**: Fraud Rate by Transaction Type Over Time

### 3. Transaction Analysis
- **Scatter Plot**: Transaction Amount vs. Fraud Probability
- **Histogram**: Transaction Amount Distribution (Fraud vs. Non-Fraud)

### 4. Customer Insights
- **Bar Chart**: Fraud Rate by Age Group
- **Table**: Top 10 High-Risk Customers

### 5. Time-Based Analysis
- **Line Chart**: Fraud Incidents by Hour of Day
- **Line Chart**: Fraud Incidents by Day of Week

## Dashboard Layout

1. **Top Section**
   - Title: "Fraud Detection Dashboard"
   - Date/Time filter
   - Key metrics (Total Transactions, Fraud Count, Fraud Rate)

2. **Middle Section**
   - Left: Fraud by Transaction Type
   - Right: Transaction Analysis

3. **Bottom Section**
   - Left: Customer Insights
   - Right: Time-Based Analysis

## Data Model Relationships

- Link `TransactionID` between processed data and predictions
- Create date hierarchy (Year > Month > Day > Hour)

## Scheduled Refresh

To keep the dashboard up-to-date:
1. Publish to Power BI Service
2. Set up scheduled refresh with your data source credentials
3. Configure refresh frequency (daily/hourly as needed)

## Notes

- The dashboard uses a sample of the data for the template
- All visualizations are interactive and cross-filterable
- Use the built-in filtering capabilities to drill down into specific segments
