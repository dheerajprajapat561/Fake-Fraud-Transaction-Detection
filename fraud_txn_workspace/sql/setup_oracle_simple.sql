-- Connect to the PDB (Pluggable Database)
ALTER SESSION SET CONTAINER = XEPDB1;

-- Create the user in the PDB
CREATE USER bank_admin IDENTIFIED BY 920400
DEFAULT TABLESPACE USERS
TEMPORARY TABLESPACE TEMP
QUOTA UNLIMITED ON USERS;

-- Grant necessary privileges
GRANT CONNECT, RESOURCE, CREATE SESSION TO bank_admin;
GRANT CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO bank_admin;

-- Connect as the new user
CONNECT bank_admin/920400@localhost:1521/XEPDB1

-- Create the fraud_predictions table
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

-- Verify the table was created
SELECT table_name FROM user_tables;

-- Exit SQL*Plus
EXIT;
