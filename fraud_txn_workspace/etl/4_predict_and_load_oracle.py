#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import joblib
import cx_Oracle
import mysql.connector
from dotenv import load_dotenv
import logging
import uuid
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_environment_variables():
    """Load database credentials from .env file."""
    load_dotenv()
    return {
        'mysql': {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', '920400'),
            'database': os.getenv('MYSQL_DATABASE', 'bank_transactions'),
            'port': int(os.getenv('MYSQL_PORT', 3306))
        },
        'oracle': {
            'user': os.getenv('ORACLE_USER', 'bank_admin'),
            'password': os.getenv('ORACLE_PASSWORD', '920400'),
            'dsn': os.getenv('ORACLE_DSN', 'localhost:1521/XEPDB1')
        }
    }

def get_transformed_data():
    """Fetch transformed transaction data from MySQL."""
    try:
        db_config = load_environment_variables()['mysql']
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        
        query = "SELECT * FROM transformed_transactions"
        cursor.execute(query)
        data = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching transformed data: {str(e)}")
        raise

def load_model():
    """Load the trained fraud detection model."""
    try:
        models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        model_path = os.path.join(models_dir, 'fraud_detection_model.joblib')
        
        model = joblib.load(model_path)
        logger.info("Model loaded successfully")
        
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise

def make_predictions(model, df):
    """Make fraud predictions using the loaded model."""
    try:
        # Define features
        features = ['amount', 'amount_log', 'hour', 'day_of_week', 'is_weekend',
                   'location_encoded', 'device_encoded', 'merchant_encoded']
        
        # Make predictions
        predictions = model.predict(df[features])
        probabilities = model.predict_proba(df[features])[:, 1]
        
        # Add predictions to DataFrame
        df['predicted_fraud'] = predictions
        df['fraud_probability'] = probabilities
        
        logger.info(f"Made predictions for {len(df)} transactions")
        return df
    except Exception as e:
        logger.error(f"Error making predictions: {str(e)}")
        raise

def create_oracle_table(cursor):
    """Create predictions table in Oracle if it doesn't exist."""
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fraud_predictions (
            transaction_id VARCHAR2(50) PRIMARY KEY,
            amount NUMBER(10,2),
            merchant_id VARCHAR2(50),
            customer_id VARCHAR2(50),
            predicted_fraud NUMBER(1),
            fraud_probability NUMBER(5,4),
            prediction_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        logger.info("Oracle predictions table created or already exists")
    except Exception as e:
        logger.error(f"Error creating Oracle table: {str(e)}")
        raise

def load_predictions_to_oracle(df):
    """Load predictions to Oracle database."""
    try:
        # Connect to Oracle
        db_config = load_environment_variables()['oracle']
        connection = cx_Oracle.connect(**db_config)
        cursor = connection.cursor()
        
        # Create table if it doesn't exist
        create_oracle_table(cursor)
        
        # Prepare data for insertion
        insert_query = """
        INSERT INTO fraud_predictions 
        (transaction_id, amount, merchant_id, customer_id, predicted_fraud, fraud_probability)
        VALUES (:1, :2, :3, :4, :5, :6)
        """
        
        # Convert DataFrame to list of tuples
        records = df[['transaction_id', 'amount', 'merchant_id', 'customer_id', 
                     'predicted_fraud', 'fraud_probability']].values.tolist()
        
        # Insert data
        cursor.executemany(insert_query, records)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully loaded {len(records)} predictions to Oracle")
    except Exception as e:
        logger.error(f"Error loading predictions to Oracle: {str(e)}")
        raise

def main():
    """Main function to make predictions and load to Oracle."""
    try:
        # Get transformed data
        df = get_transformed_data()
        logger.info(f"Retrieved {len(df)} transformed transactions")
        
        # Load model
        model = load_model()
        
        # Make predictions
        df_with_predictions = make_predictions(model, df)
        
        # Load predictions to Oracle
        load_predictions_to_oracle(df_with_predictions)
        
        logger.info("Prediction and loading process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
