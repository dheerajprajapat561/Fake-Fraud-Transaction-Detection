import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging
from tqdm import tqdm

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

# Set paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
BANK_DATA_PATH = os.path.join(DATA_DIR, 'bank_transactions_data_2.csv')

def create_mysql_connection():
    """Create a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT
        )
        if connection.is_connected():
            logger.info("Connected to MySQL database")
            return connection
    except Error as e:
        logger.error("Error connecting to MySQL: %s", str(e))
        return None

def create_database_and_table(connection):
    """Create the database and transactions table if they don't exist."""
    cursor = connection.cursor()
    try:
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
        logger.info(f"Database '{MYSQL_DATABASE}' is ready")
        
        # Switch to the database
        cursor.execute(f"USE {MYSQL_DATABASE}")
        
        # Create transactions table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Transactions (
            TransactionID VARCHAR(50) PRIMARY KEY,
            AccountID VARCHAR(50),
            TransactionAmount DECIMAL(12, 2),
            TransactionDate DATETIME,
            TransactionType VARCHAR(50),
            Location VARCHAR(100),
            DeviceID VARCHAR(100),
            IP_Address VARCHAR(50),
            MerchantID VARCHAR(50),
            Channel VARCHAR(50),
            CustomerAge INT,
            CustomerOccupation VARCHAR(100),
            TransactionDuration DECIMAL(6, 2),
            LoginAttempts INT,
            AccountBalance DECIMAL(12, 2),
            PreviousTransactionDate DATETIME
        )
        """)
        logger.info("Transactions table is ready")
        
        connection.commit()
    except Error as e:
        logger.error("Error creating database or table: %s", str(e))
    finally:
        cursor.close()

def load_data_to_mysql(connection, df):
    """Load data from DataFrame to MySQL database."""
    # Switch to the database
    cursor = connection.cursor()
    cursor.execute(f"USE {MYSQL_DATABASE}")
    
    # Prepare the REPLACE statement (will update existing records with same primary key)
    insert_query = """
    REPLACE INTO Transactions 
    (TransactionID, AccountID, TransactionAmount, TransactionDate, TransactionType, 
     Location, DeviceID, IP_Address, MerchantID, Channel, CustomerAge, 
     CustomerOccupation, TransactionDuration, LoginAttempts, AccountBalance, 
     PreviousTransactionDate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convert DataFrame rows to list of tuples for batch insert
    records = []
    for _, row in df.iterrows():
        record = (
            row['TransactionID'],
            row['AccountID'],
            float(row['TransactionAmount']),
            row['TransactionDate'],
            row['TransactionType'],
            row['Location'],
            row['DeviceID'],
            row['IP Address'],  # Note the space in column name
            row['MerchantID'],
            row['Channel'],
            int(row['CustomerAge']),
            row['CustomerOccupation'],
            float(row['TransactionDuration']),
            int(row['LoginAttempts']),
            float(row['AccountBalance']),
            row['PreviousTransactionDate']
        )
        records.append(record)
    
    # Insert data in batches
    batch_size = 100
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    try:
        for i in tqdm(range(0, len(records), batch_size), total=total_batches, desc="Loading data to MySQL"):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
        
        logger.info("Successfully loaded %d records to MySQL", len(records))
    except Error as e:
        logger.error("Error inserting data to MySQL: %s", str(e))
        connection.rollback()
    finally:
        cursor.close()

def main():
    """Main function to orchestrate the data loading process."""
    logger.info("Starting bank transaction data loading process")
    
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Check if dataset exists
    if not os.path.exists(BANK_DATA_PATH):
        logger.error("Bank transaction dataset not found at: %s", BANK_DATA_PATH)
        return
    
    # Load the dataset
    logger.info("Loading dataset from: %s", BANK_DATA_PATH)
    try:
        df = pd.read_csv(BANK_DATA_PATH)
        logger.info("Dataset loaded successfully with %d rows", len(df))
    except Exception as e:
        logger.error("Error loading dataset: %s", str(e))
        return
    
    # Connect to MySQL
    connection = create_mysql_connection()
    if not connection:
        return
    
    # Create database and table if they don't exist
    create_database_and_table(connection)
    
    # Load data to MySQL
    load_data_to_mysql(connection, df)
    
    # Close connection
    connection.close()
    logger.info("Data loading process completed")

if __name__ == "__main__":
    main()
