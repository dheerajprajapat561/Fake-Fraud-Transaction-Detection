# Fraud Transaction Detection System

A complete end-to-end pipeline for detecting fraudulent financial transactions using the Kaggle PaySim dataset. This system includes data ingestion, feature engineering, model training, prediction, and visualization components.

## Project Structure

```
fraud_txn_workspace/
├── data/
│   ├── kaggle_paysim.csv              # Kaggle dataset (you download)
│   ├── processed.csv                  # Feature engineered data
│   └── predicted.csv                  # Output with predictions
│
├── etl/
│   ├── 1_load_to_mysql.py             # Load raw Kaggle data to MySQL
│   ├── 2_transform_features.py        # Feature engineering
│   ├── 3_train_model.py               # Train fraud model
│   ├── 4_predict_and_load_oracle.py   # Predict and push to Oracle
│
├── model/
│   └── fraud_model.pkl                # Trained ML model
│
├── sql/
│   ├── mysql_schema.sql               # MySQL table DDL
│   └── oracle_schema.sql              # Oracle table DDL
│
├── dashboard/
│   └── Fraud_Dashboard.pbix           # Power BI dashboard template
```

## Prerequisites

- Python 3.8+
- MySQL server
- Oracle database (optional)
- Power BI Desktop (optional)

## Setup Instructions

1. **Set up the environment**:
   ```
   pip install -r requirements.txt
   ```

2. **Configure database connections**:
   Create a `.env` file in the project root with the following variables:
   ```
   # MySQL Configuration
   MYSQL_HOST=localhost
   MYSQL_USER=root
   MYSQL_PASSWORD=your_password
   MYSQL_DATABASE=fraud_detection
   MYSQL_PORT=3306

   # Oracle Configuration (Optional)
   ORACLE_HOST=localhost
   ORACLE_PORT=1521
   ORACLE_SERVICE=XE
   ORACLE_USER=fraud_admin
   ORACLE_PASSWORD=your_password
   ```

3. **Initialize the database schemas**:
   - Run the MySQL schema script in your MySQL server
   - Run the Oracle schema script in your Oracle server (optional)

4. **Obtain the dataset**:
   - Download the PaySim dataset from Kaggle and place it in the `data/` directory as `kaggle_paysim.csv`
   - Alternatively, configure the Kaggle API credentials to automatically download the dataset

## Usage

Run the ETL scripts in sequence:

1. **Load data to MySQL**:
   ```
   python etl/1_load_to_mysql.py
   ```

2. **Transform and engineer features**:
   ```
   python etl/2_transform_features.py
   ```

3. **Train the fraud detection model**:
   ```
   python etl/3_train_model.py
   ```

4. **Make predictions and load to Oracle**:
   ```
   python etl/4_predict_and_load_oracle.py
   ```

5. **Visualize results** (optional):
   Open the Power BI dashboard (`dashboard/Fraud_Dashboard.pbix`) and connect it to your databases.

## Model Information

The system uses XGBoost for fraud detection with the following features:
- Transaction amount and balance features
- Transaction velocity and frequency
- Account balance disparities
- Zero-balance indicators
- Transaction type features

## Extending the System

- Add more advanced features to improve model performance
- Implement real-time predictions using a web service
- Create automated alerting for detected fraud cases
- Implement model monitoring and retraining pipelines
