-- Drop user if exists
BEGIN
   EXECUTE IMMEDIATE 'DROP USER bank_admin CASCADE';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -1918 THEN
         RAISE;
      END IF;
END;
/

-- Create user with default tablespace
CREATE USER bank_admin IDENTIFIED BY 920400
DEFAULT TABLESPACE USERS
TEMPORARY TABLESPACE TEMP
QUOTA UNLIMITED ON USERS;

-- Grant necessary privileges
GRANTE CONNECT, RESOURCE, CREATE SESSION TO bank_admin;
GRANTE CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO bank_admin;

-- Create the fraud_predictions table
CREATE TABLE bank_admin.fraud_predictions (
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
CREATE INDEX bank_admin.idx_pred_txn_id ON bank_admin.fraud_predictions(transaction_id);
CREATE INDEX bank_admin.idx_pred_is_fraud ON bank_admin.fraud_predictions(is_fraud);
CREATE INDEX bank_admin.idx_pred_time ON bank_admin.fraud_predictions(prediction_time);

-- Create view for high risk transactions
CREATE OR REPLACE VIEW bank_admin.high_risk_transactions AS
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
    bank_admin.fraud_predictions p
WHERE 
    p.fraud_probability >= 0.7
ORDER BY 
    p.fraud_probability DESC,
    p.prediction_time DESC;

-- Commit changes
COMMIT;

-- Verify the user and table were created
SELECT username, account_status, created FROM dba_users WHERE username = 'BANK_ADMIN';
SELECT table_name FROM all_tables WHERE owner = 'BANK_ADMIN';

-- Exit SQL*Plus
EXIT;
