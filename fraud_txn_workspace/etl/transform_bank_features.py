#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Transform and engineer features for bank transaction fraud detection.
This script reads bank transaction data from MySQL, creates additional features,
and saves the processed data for model training.
"""

import os
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '920400')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'bank_transactions')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, 'processed_bank_data.csv')

# Create directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)

def create_mysql_connection():
    """Create a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT
        )
        if connection.is_connected():
            logger.info("Connected to MySQL database")
            return connection
    except Error as e:
        logger.error("Error connecting to MySQL: %s", str(e))
        return None

def load_bank_data_from_mysql():
    """Load bank transaction data from MySQL database."""
    connection = create_mysql_connection()
    if not connection:
        return None
    
    try:
        logger.info("Loading bank transactions from MySQL")
        query = "SELECT * FROM Transactions"
        df = pd.read_sql(query, connection)
        logger.info("Loaded %d bank transactions", len(df))
        return df
    except Error as e:
        logger.error("Error loading data from MySQL: %s", str(e))
        return None
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("MySQL connection closed")

def normalize_amount(df):
    """Normalize transaction amounts using StandardScaler."""
    logger.info("Normalizing transaction amounts")
    scaler = StandardScaler()
    df['TransactionAmount_Normalized'] = scaler.fit_transform(df[['TransactionAmount']])
    return df

def process_transaction_dates(df):
    """Process transaction dates and create time-based features."""
    logger.info("Processing transaction dates")
    
    # Convert to datetime if not already
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
    df['PreviousTransactionDate'] = pd.to_datetime(df['PreviousTransactionDate'])
    
    # Extract useful components
    df['TransactionHour'] = df['TransactionDate'].dt.hour
    df['TransactionDayOfWeek'] = df['TransactionDate'].dt.dayofweek
    df['TransactionMonth'] = df['TransactionDate'].dt.month
    
    # Is weekend flag
    df['IsWeekend'] = df['TransactionDayOfWeek'].apply(lambda x: 1 if x >= 5 else 0)
    
    # Is evening flag (after 6 PM)
    df['IsEvening'] = df['TransactionHour'].apply(lambda x: 1 if x >= 18 else 0)
    
    # Time since previous transaction (in hours)
    df['HoursSincePrevious'] = (df['TransactionDate'] - df['PreviousTransactionDate']).dt.total_seconds() / 3600
    
    # Cap extremely large values (might be first transaction)
    df['HoursSincePrevious'] = df['HoursSincePrevious'].clip(upper=720)  # 30 days max
    
    return df

def process_ip_and_location(df):
    """Process IP addresses and location information."""
    logger.info("Processing IP addresses and locations")
    
    # Extract IP address first octet (gives a rough network categorization)
    df['IP_FirstOctet'] = df['IP_Address'].str.split('.').str[0].astype(int)
    
    # Location frequency (how common is this location for this account)
    location_counts = df.groupby(['AccountID', 'Location']).size().reset_index(name='LocationFrequency')
    df = pd.merge(df, location_counts, on=['AccountID', 'Location'], how='left')
    
    # Unusual location flag (locations used less than 3 times by this account)
    df['UnusualLocation'] = (df['LocationFrequency'] < 3).astype(int)
    
    return df

def process_device_and_channel(df):
    """Process device and channel information."""
    logger.info("Processing device and channel data")
    
    # Device frequency (how often this device is used by this account)
    device_counts = df.groupby(['AccountID', 'DeviceID']).size().reset_index(name='DeviceFrequency')
    df = pd.merge(df, device_counts, on=['AccountID', 'DeviceID'], how='left')
    
    # Unusual device flag (devices used less than 3 times by this account)
    df['UnusualDevice'] = (df['DeviceFrequency'] < 3).astype(int)
    
    # Channel risk scoring (typically Online > ATM > Branch for fraud risk)
    channel_risk = {'Online': 3, 'ATM': 2, 'Branch': 1}
    df['ChannelRiskScore'] = df['Channel'].map(channel_risk)
    
    return df

def create_account_aggregates(df):
    """Create account level aggregations."""
    logger.info("Creating account level aggregations")
    
    # Calculate total transactions per account
    account_txn_counts = df.groupby('AccountID').size().reset_index(name='AccountTxnCount')
    df = pd.merge(df, account_txn_counts, on='AccountID', how='left')
    
    # Calculate average transaction amount per account
    account_avg_amount = df.groupby('AccountID')['TransactionAmount'].mean().reset_index(name='AccountAvgAmount')
    df = pd.merge(df, account_avg_amount, on='AccountID', how='left')
    
    # Calculate transaction amount standard deviation per account
    account_std_amount = df.groupby('AccountID')['TransactionAmount'].std().reset_index(name='AccountStdAmount')
    account_std_amount['AccountStdAmount'] = account_std_amount['AccountStdAmount'].fillna(0)  # Fill NaN for accounts with single transaction
    df = pd.merge(df, account_std_amount, on='AccountID', how='left')
    
    # Calculate transaction amount as a percentage of the average for this account
    df['AmountToAccountAvgRatio'] = df['TransactionAmount'] / df['AccountAvgAmount']
    df['AmountToAccountAvgRatio'] = df['AmountToAccountAvgRatio'].fillna(1.0)  # Fill NaN values
    
    # Amount to account standard deviation ratio (z-score) - how unusual is this amount
    df['AmountAccountZScore'] = (df['TransactionAmount'] - df['AccountAvgAmount']) / df['AccountStdAmount'].replace(0, 1)
    df['AmountAccountZScore'] = df['AmountAccountZScore'].fillna(0)  # Fill NaN values
    
    # Maximum transaction amount for this account
    account_max_amount = df.groupby('AccountID')['TransactionAmount'].max().reset_index(name='AccountMaxAmount')
    df = pd.merge(df, account_max_amount, on='AccountID', how='left')
    
    # Is this the maximum transaction for this account?
    df['IsMaxAmount'] = (df['TransactionAmount'] == df['AccountMaxAmount']).astype(int)
    
    # Is this transaction > 150% of the average for this account?
    df['IsLargeTransaction'] = (df['AmountToAccountAvgRatio'] > 1.5).astype(int)
    
    return df

def create_timeseries_features(df):
    """Create time-series features for each account."""
    logger.info("Creating time-series features")
    
    # Sort by account and transaction date
    df = df.sort_values(['AccountID', 'TransactionDate'])
    
    # Initialize window features
    df['TxnCountLast24h'] = 0
    df['TxnAmountLast24h'] = 0.0
    df['TxnCountLast7d'] = 0
    df['TxnAmountLast7d'] = 0.0
    
    # Group by account
    for account_id, group in df.groupby('AccountID'):
        # Sort by date within each group
        group = group.sort_values('TransactionDate')
        
        # For each transaction, count and sum amounts in the lookback windows
        for i, row in group.iterrows():
            txn_date = row['TransactionDate']
            
            # Look back 24 hours
            last_24h = group[(group['TransactionDate'] < txn_date) & 
                             (group['TransactionDate'] >= txn_date - timedelta(hours=24))]
            df.at[i, 'TxnCountLast24h'] = len(last_24h)
            df.at[i, 'TxnAmountLast24h'] = last_24h['TransactionAmount'].sum()
            
            # Look back 7 days
            last_7d = group[(group['TransactionDate'] < txn_date) & 
                            (group['TransactionDate'] >= txn_date - timedelta(days=7))]
            df.at[i, 'TxnCountLast7d'] = len(last_7d)
            df.at[i, 'TxnAmountLast7d'] = last_7d['TransactionAmount'].sum()
    
    # Transaction velocity features
    df['TxnAmountPerCountLast24h'] = np.where(df['TxnCountLast24h'] > 0, 
                                             df['TxnAmountLast24h'] / df['TxnCountLast24h'], 0)
    df['TxnAmountPerCountLast7d'] = np.where(df['TxnCountLast7d'] > 0, 
                                            df['TxnAmountLast7d'] / df['TxnCountLast7d'], 0)
    
    # Unusual activity flags
    df['HighVelocity24h'] = ((df['TxnCountLast24h'] > 3) & 
                              (df['TxnAmountLast24h'] > 2 * df['AccountAvgAmount'])).astype(int)
    
    return df

def create_behavior_features(df):
    """Create behavioral features from transaction data."""
    logger.info("Creating behavioral features")
    
    # Login attempts ratio
    df['LoginAttemptsRatio'] = df['LoginAttempts'] / df['AccountTxnCount'].clip(lower=1)
    
    # Amount to balance ratio
    df['AmountToBalanceRatio'] = df['TransactionAmount'] / df['AccountBalance'].clip(lower=1)
    
    # Duration features
    # Transaction duration standard deviation by account
    txn_duration_std = df.groupby('AccountID')['TransactionDuration'].std().reset_index(name='AccountDurationStd')
    txn_duration_std['AccountDurationStd'] = txn_duration_std['AccountDurationStd'].fillna(0)
    df = pd.merge(df, txn_duration_std, on='AccountID', how='left')
    
    # Transaction duration average by account
    txn_duration_avg = df.groupby('AccountID')['TransactionDuration'].mean().reset_index(name='AccountDurationAvg')
    df = pd.merge(df, txn_duration_avg, on='AccountID', how='left')
    
    # Duration z-score (how unusual is this duration)
    df['DurationZScore'] = ((df['TransactionDuration'] - df['AccountDurationAvg']) / 
                           df['AccountDurationStd'].replace(0, 1))
    df['DurationZScore'] = df['DurationZScore'].fillna(0)
    
    # Unusual duration flag
    df['UnusualDuration'] = (abs(df['DurationZScore']) > 2).astype(int)
    
    return df

def create_demographic_features(df):
    """Create demographic-based features."""
    logger.info("Creating demographic features")
    
    # Age groups
    df['AgeGroup'] = pd.cut(df['CustomerAge'], 
                            bins=[0, 18, 25, 35, 50, 65, 100], 
                            labels=['<18', '18-25', '26-35', '36-50', '51-65', '65+'])
    
    # Occupation risk (simplified version - you might want to refine this)
    occupation_risk = {
        'Student': 2,
        'Engineer': 1,
        'Doctor': 1,
        'Retired': 1
    }
    df['OccupationRiskScore'] = df['CustomerOccupation'].map(occupation_risk).fillna(1)
    
    # Young account with high balance flag (potential risk)
    df['YoungHighBalance'] = ((df['CustomerAge'] < 30) & 
                             (df['AccountBalance'] > 10000)).astype(int)
    
    return df

def create_merchant_features(df):
    """Create merchant-related features."""
    logger.info("Creating merchant features")
    
    # Merchant frequency per account
    merchant_counts = df.groupby(['AccountID', 'MerchantID']).size().reset_index(name='MerchantFrequency')
    df = pd.merge(df, merchant_counts, on=['AccountID', 'MerchantID'], how='left')
    
    # Unusual merchant flag
    df['UnusualMerchant'] = (df['MerchantFrequency'] < 3).astype(int)
    
    # Average amount per merchant
    merchant_avg = df.groupby('MerchantID')['TransactionAmount'].mean().reset_index(name='MerchantAvgAmount')
    df = pd.merge(df, merchant_avg, on='MerchantID', how='left')
    
    # Amount ratio compared to merchant average
    df['AmountToMerchantAvgRatio'] = df['TransactionAmount'] / df['MerchantAvgAmount']
    df['AmountToMerchantAvgRatio'] = df['AmountToMerchantAvgRatio'].fillna(1.0)
    
    # High merchant amount flag
    df['HighMerchantAmount'] = (df['AmountToMerchantAvgRatio'] > 2).astype(int)
    
    return df

def create_combined_risk_features(df):
    """Create combined risk features."""
    logger.info("Creating combined risk features")
    
    # Risk score calculation using multiple factors
    df['RiskScore'] = (
        df['UnusualLocation'] * 3 +
        df['UnusualDevice'] * 3 +
        df['ChannelRiskScore'] +
        df['IsLargeTransaction'] * 2 +
        df['HighVelocity24h'] * 3 +
        df['UnusualDuration'] * 2 +
        df['LoginAttemptsRatio'] * 5 +
        df['UnusualMerchant'] * 2 +
        df['HighMerchantAmount'] * 2
    )
    
    # Flag high risk transactions (arbitrary threshold - should be tuned)
    df['HighRiskFlag'] = (df['RiskScore'] > 10).astype(int)
    
    return df

def save_processed_data(df):
    """Save processed data to CSV file."""
    try:
        logger.info("Saving processed data to: %s", PROCESSED_DATA_PATH)
        df.to_csv(PROCESSED_DATA_PATH, index=False)
        logger.info("Processed data saved successfully")
        return True
    except Exception as e:
        logger.error("Error saving processed data: %s", str(e))
        return False

def main():
    """Main function to orchestrate the feature engineering process."""
    logger.info("Starting feature engineering process")
    
    # Load bank data from MySQL
    df = load_bank_data_from_mysql()
    if df is None or df.empty:
        logger.error("No transaction data available for processing")
        # Try to load from CSV as fallback
        try:
            logger.info("Attempting to load data from CSV file as fallback")
            csv_path = os.path.join(DATA_DIR, 'bank_transactions_data_2.csv')
            df = pd.read_csv(csv_path)
            logger.info("Loaded %d records from CSV", len(df))
        except Exception as e:
            logger.error("Error loading data from CSV: %s", str(e))
            return
    
    # Process the data - apply all feature engineering steps
    df = normalize_amount(df)
    df = process_transaction_dates(df)
    df = process_ip_and_location(df)
    df = process_device_and_channel(df)
    df = create_account_aggregates(df)
    df = create_timeseries_features(df)
    df = create_behavior_features(df)
    df = create_demographic_features(df)
    df = create_merchant_features(df)
    df = create_combined_risk_features(df)
    
    # Add these new features
    df['TransactionTimeOfDay'] = pd.to_datetime(df['TransactionDate']).dt.hour.apply(
    lambda x: 'Morning' if 5 <= x < 12 else 'Afternoon' if 12 <= x < 17 else 'Evening' if 17 <= x < 21 else 'Night')
    
    df['TransactionTimeRiskScore'] = df['TransactionTimeOfDay'].map({
    'Morning': 1, 'Afternoon': 2, 'Evening': 3, 'Night': 4
    })
    
    # Velocity features with exponential weighting
    df['ExponentialVelocityScore'] = df['TxnCountLast24h'] * np.exp(df['TxnAmountLast24h'] / 1000)
    
    # Amount deviation from typical behavior
    df['AmountDeviationScore'] = np.abs(df['TransactionAmount'] - df['AccountAvgAmount']) / df['AccountStdAmount']
    
    # Combine multiple risk factors
    df['CombinedRiskScore'] = (
    df['RiskScore'] + 
    df['TransactionTimeRiskScore'] * 0.5 + 
    df['AmountDeviationScore'] * 0.3 + 
    df['ExponentialVelocityScore'] * 0.2
    )
    
    # Save the processed data
    success = save_processed_data(df)
    
    if success:
        logger.info("Feature engineering process completed successfully")
    else:
        logger.error("Feature engineering process completed with errors")

if __name__ == "__main__":
    main()
