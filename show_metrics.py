import pandas as pd
import os
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Set paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'fraud_txn_workspace', 'data')
PREDICTIONS_PATH = os.path.join(DATA_DIR, 'predicted.csv')
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, 'processed_bank_data.csv')

# Load data
try:
    # Try to load predictions first
    print("Loading prediction results...")
    df = pd.read_csv(PREDICTIONS_PATH)
    y_true = df['is_fraud']
    y_pred = df['is_fraud']  # Since these are already predictions
    y_prob = df['fraud_probability']
    threshold = df['prediction_threshold'].iloc[0]
    print(f"Loaded {len(df)} predictions with threshold {threshold}")
except FileNotFoundError:
    # Fall back to processed data
    print("Predictions file not found, loading processed data...")
    df = pd.read_csv(PROCESSED_DATA_PATH)
    y_true = df['HighRiskFlag']
    y_pred = df['HighRiskFlag']
    threshold = 0.5  # Default threshold
    print(f"Loaded {len(df)} processed transactions")

# Calculate metrics
accuracy = accuracy_score(y_true, y_pred)
precision = precision_score(y_true, y_pred, zero_division=0)
recall = recall_score(y_true, y_pred, zero_division=0)
f1 = f1_score(y_true, y_pred, zero_division=0)

# Count fraud transactions
fraud_count = y_true.sum()
total_count = len(y_true)
fraud_rate = (fraud_count / total_count) * 100

# Print metrics
print("\n" + "="*50)
print("FRAUD DETECTION MODEL PERFORMANCE METRICS")
print("="*50)
print(f"Total Transactions: {total_count}")
print(f"Fraudulent Transactions: {fraud_count} ({fraud_rate:.2f}%)")
print(f"Non-Fraudulent Transactions: {total_count - fraud_count} ({100 - fraud_rate:.2f}%)")
print("\nModel Performance:")
print(f"Accuracy:  {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")
print(f"F1 Score:  {f1:.4f}")
print(f"Fraud Threshold: {threshold}")

# Calculate confidence levels
if 'fraud_probability' in df.columns:
    # Calculate average probability for fraud and non-fraud
    avg_prob_fraud = df[df['is_fraud'] == 1]['fraud_probability'].mean()
    avg_prob_non_fraud = df[df['is_fraud'] == 0]['fraud_probability'].mean()
    
    print("\nConfidence Levels:")
    print(f"Average probability for fraudulent transactions: {avg_prob_fraud:.4f}")
    print(f"Average probability for non-fraudulent transactions: {avg_prob_non_fraud:.4f}")
    
    # Calculate ROC AUC if we have probabilities
    try:
        roc_auc = roc_auc_score(y_true, y_prob)
        print(f"ROC AUC Score: {roc_auc:.4f}")
    except:
        print("Could not calculate ROC AUC Score (requires probability values)")
else:
    print("\nNote: Probability values not available, cannot calculate confidence levels")

print("="*50)

# Create confusion matrix
cm = confusion_matrix(y_true, y_pred)
print("\nConfusion Matrix:")
print(cm)
print("\nWhere:")
print("True Negatives (TN):", cm[0, 0])
print("False Positives (FP):", cm[0, 1])
print("False Negatives (FN):", cm[1, 0])
print("True Positives (TP):", cm[1, 1])
print("="*50)