-- MySQL schema for fraud transaction data
-- Created: 2025-05-11

CREATE DATABASE IF NOT EXISTS fraud_detection;
USE fraud_detection;

-- Raw transactions table from Kaggle PaySim
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    step INT COMMENT 'Hour of the simulation (1-744)',
    type VARCHAR(20) COMMENT 'Transaction type (CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER)',
    amount DECIMAL(15,2) COMMENT 'Transaction amount',
    nameOrig VARCHAR(50) COMMENT 'Customer who started the transaction',
    oldbalanceOrg DECIMAL(15,2) COMMENT 'Initial balance of origin account before transaction',
    newbalanceOrig DECIMAL(15,2) COMMENT 'New balance of origin account after transaction',
    nameDest VARCHAR(50) COMMENT 'Customer who is the recipient of the transaction',
    oldbalanceDest DECIMAL(15,2) COMMENT 'Initial balance of destination account before transaction',
    newbalanceDest DECIMAL(15,2) COMMENT 'New balance of destination account after transaction',
    isFraud TINYINT(1) COMMENT 'Fraud label (1 for fraud, 0 for legitimate)',
    isFlaggedFraud TINYINT(1) COMMENT 'Flag for suspicious activity by the system',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (type),
    INDEX idx_nameOrig (nameOrig),
    INDEX idx_nameDest (nameDest),
    INDEX idx_isFraud (isFraud)
);

-- Processed features table
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    step INT,
    type VARCHAR(20),
    amount DECIMAL(15,2),
    nameOrig VARCHAR(50),
    oldbalanceOrg DECIMAL(15,2),
    newbalanceOrig DECIMAL(15,2),
    nameDest VARCHAR(50),
    oldbalanceDest DECIMAL(15,2),
    newbalanceDest DECIMAL(15,2),
    isFraud TINYINT(1),
    isFlaggedFraud TINYINT(1),
    amount_normalized DECIMAL(15,6) COMMENT 'Normalized transaction amount',
    balance_diff_orig DECIMAL(15,2) COMMENT 'Balance difference for origin account',
    balance_diff_dest DECIMAL(15,2) COMMENT 'Balance difference for destination account',
    is_zero_balance_orig TINYINT(1) COMMENT 'Flag for zero balance origin account',
    is_zero_balance_dest TINYINT(1) COMMENT 'Flag for zero balance destination account',
    orig_txn_count_1h INT COMMENT 'Number of transactions by origin in last hour',
    dest_txn_count_1h INT COMMENT 'Number of transactions by destination in last hour',
    orig_txn_amt_1h DECIMAL(15,2) COMMENT 'Total amount by origin in last hour',
    dest_txn_amt_1h DECIMAL(15,2) COMMENT 'Total amount by destination in last hour',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (type),
    INDEX idx_nameOrig (nameOrig),
    INDEX idx_nameDest (nameDest),
    INDEX idx_isFraud (isFraud)
);
