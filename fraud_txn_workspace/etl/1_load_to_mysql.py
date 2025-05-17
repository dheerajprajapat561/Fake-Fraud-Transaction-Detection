#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_environment_variables():
    """Load database credentials from .env file."""
    load_dotenv()
    return {
        'host': os.getenv('MYSQL_HOST'),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE')
    }

def create_mysql_connection():
    """Create MySQL database connection."""
    try:
        db_config = load_environment_variables()
        connection = mysql.connector.connect(**db_config)
        logger.info("Successfully connected to MySQL database")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        raise

def create_transactions_table(cursor):
    """Create transactions table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        timestamp DATETIME,
        amount DECIMAL(10,2),
        merchant_id VARCHAR(50),
        customer_id VARCHAR(50),
        transaction_type VARCHAR(20),
        location VARCHAR(100),
        device_id VARCHAR(50),
        ip_address VARCHAR(15),
        is_fraud BOOLEAN
    )
    """
    try:
        cursor.execute(create_table_query)
        logger.info("Transactions table created or already exists")
    except Exception as e:
        logger.error(f"Error creating transactions table: {str(e)}")
        raise

def load_sample_data(cursor):
    """Load sample transaction data."""
    # Sample data - in production, this would come from a real data source
    sample_data = [
        ('TXN001', '2024-03-15 10:30:00', 150.00, 'MERCH001', 'CUST001', 'PURCHASE', 'New York', 'DEV001', '192.168.1.1', False),
        ('TXN002', '2024-03-15 11:45:00', 2500.00, 'MERCH002', 'CUST002', 'PURCHASE', 'Los Angeles', 'DEV002', '192.168.1.2', True),
        ('TXN003', '2024-03-15 12:15:00', 75.50, 'MERCH001', 'CUST003', 'PURCHASE', 'Chicago', 'DEV003', '192.168.1.3', False)
    ]
    
    insert_query = """
    INSERT INTO transactions 
    (transaction_id, timestamp, amount, merchant_id, customer_id, transaction_type, location, device_id, ip_address, is_fraud)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor.executemany(insert_query, sample_data)
        logger.info(f"Successfully loaded {len(sample_data)} sample transactions")
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
        raise

def main():
    """Main function to load data into MySQL."""
    try:
        # Create database connection
        connection = create_mysql_connection()
        cursor = connection.cursor()
        
        # Create transactions table
        create_transactions_table(cursor)
        
        # Load sample data
        load_sample_data(cursor)
        
        # Commit changes and close connection
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
