#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import mysql.connector
from dotenv import load_dotenv
import logging
import os

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

def get_transformed_data():
    """Fetch transformed transaction data from MySQL."""
    try:
        db_config = load_environment_variables()
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

def prepare_training_data(df):
    """Prepare data for model training."""
    try:
        # Define features and target
        features = ['amount', 'amount_log', 'hour', 'day_of_week', 'is_weekend',
                   'location_encoded', 'device_encoded', 'merchant_encoded']
        X = df[features]
        y = df['is_fraud']
        
        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        return X_train, X_test, y_train, y_test
    except Exception as e:
        logger.error(f"Error preparing training data: {str(e)}")
        raise

def train_model(X_train, y_train):
    """Train the fraud detection model."""
    try:
        # Initialize and train the model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        logger.info("Model training completed")
        
        return model
    except Exception as e:
        logger.error(f"Error training model: {str(e)}")
        raise

def evaluate_model(model, X_test, y_test):
    """Evaluate model performance."""
    try:
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Generate classification report
        report = classification_report(y_test, y_pred)
        logger.info("\nClassification Report:\n%s", report)
        
        # Generate confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        logger.info("\nConfusion Matrix:\n%s", cm)
        
        return report, cm
    except Exception as e:
        logger.error(f"Error evaluating model: {str(e)}")
        raise

def save_model(model, report, cm):
    """Save the trained model and evaluation metrics."""
    try:
        # Create models directory if it doesn't exist
        models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        os.makedirs(models_dir, exist_ok=True)
        
        # Save model
        model_path = os.path.join(models_dir, 'fraud_detection_model.joblib')
        joblib.dump(model, model_path)
        logger.info(f"Model saved to {model_path}")
        
        # Save evaluation metrics
        metrics_path = os.path.join(models_dir, 'model_metrics.txt')
        with open(metrics_path, 'w') as f:
            f.write("Classification Report:\n")
            f.write(report)
            f.write("\n\nConfusion Matrix:\n")
            f.write(str(cm))
        logger.info(f"Evaluation metrics saved to {metrics_path}")
        
    except Exception as e:
        logger.error(f"Error saving model and metrics: {str(e)}")
        raise

def main():
    """Main function to train the fraud detection model."""
    try:
        # Get transformed data
        df = get_transformed_data()
        logger.info(f"Retrieved {len(df)} transformed transactions")
        
        # Prepare training data
        X_train, X_test, y_train, y_test = prepare_training_data(df)
        logger.info("Data preparation completed")
        
        # Train model
        model = train_model(X_train, y_train)
        
        # Evaluate model
        report, cm = evaluate_model(model, X_test, y_test)
        
        # Save model and metrics
        save_model(model, report, cm)
        
        logger.info("Model training and evaluation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 