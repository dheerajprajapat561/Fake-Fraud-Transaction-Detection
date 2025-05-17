-- Oracle schema for fraud predictions
-- Created: 2025-05-11

-- Create tablespace if it doesn't exist (uncomment and modify as needed)
-- CREATE TABLESPACE fraud_ts DATAFILE 'fraud_ts.dbf' SIZE 100M AUTOEXTEND ON;

-- Create user if it doesn't exist (uncomment and modify as needed)
-- CREATE USER fraud_admin IDENTIFIED BY password DEFAULT TABLESPACE fraud_ts;
-- GRANT CONNECT, RESOURCE, CREATE SESSION TO fraud_admin;

-- Predicted transactions table
CREATE TABLE fraud_predictions (
    prediction_id VARCHAR2(36) PRIMARY KEY,
    transaction_id VARCHAR2(36) NOT NULL,
    step NUMBER(5),
    type VARCHAR2(20),
    amount NUMBER(15,2),
    name_orig VARCHAR2(50),
    name_dest VARCHAR2(50),
    fraud_probability NUMBER(10,6),
    is_fraud NUMBER(1),
    prediction_threshold NUMBER(5,2),
    model_version VARCHAR2(50),
    prediction_time TIMESTAMP,
    CONSTRAINT fraud_prob_range CHECK (fraud_probability BETWEEN 0 AND 1)
);

-- Create indices for performance
CREATE INDEX idx_pred_txn_id ON fraud_predictions(transaction_id);
CREATE INDEX idx_pred_is_fraud ON fraud_predictions(is_fraud);
CREATE INDEX idx_pred_time ON fraud_predictions(prediction_time);

-- Create view for high risk transactions
CREATE OR REPLACE VIEW high_risk_transactions AS
SELECT 
    p.prediction_id,
    p.transaction_id,
    p.type,
    p.amount,
    p.name_orig,
    p.name_dest,
    p.fraud_probability,
    p.prediction_time
FROM 
    fraud_predictions p
WHERE 
    p.fraud_probability >= 0.7
ORDER BY 
    p.fraud_probability DESC,
    p.prediction_time DESC;

-- Create view for fraud statistics
CREATE OR REPLACE VIEW fraud_stats_daily AS
SELECT 
    TRUNC(prediction_time) AS prediction_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS fraud_percentage,
    SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount_total
FROM 
    fraud_predictions
GROUP BY 
    TRUNC(prediction_time)
ORDER BY 
    prediction_date DESC;
