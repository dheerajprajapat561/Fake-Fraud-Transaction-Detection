#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fraud Detection ETL Pipeline Orchestrator

This script orchestrates the execution of the entire fraud detection ETL pipeline:
1. Load transaction data to MySQL
2. Transform and engineer features
3. Train the fraud detection model
4. Make predictions and load results to Oracle
5. Display model performance metrics

Usage:
    python app.py [--step STEP] [--show-metrics]

Options:
    --step STEP           Run a specific step (1-5) or 'all' for the entire pipeline (default: 'all')
    --show-metrics        Display model performance metrics after pipeline execution
"""

import os
import sys
import argparse
import logging
import importlib.util
import time
import subprocess
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, 'fraud_txn_workspace', 'etl')  # Updated path to correct ETL directory

def import_module_from_file(file_path):
    """Import a module from file path."""
    module_name = os.path.basename(file_path).replace('.py', '')
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_step(step_num, step_name, script_path):
    """Run a specific pipeline step."""
    logger.info(f"Starting Step {step_num}: {step_name}")
    start_time = time.time()
    
    try:
        # Import and run the module
        module = import_module_from_file(script_path)
        module.main()
        
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed Step {step_num}: {step_name} in {duration:.2f} seconds")
        return True
    except Exception as e:
        logger.error(f"Error in Step {step_num}: {step_name} - {str(e)}")
        return False

def show_metrics():
    """Run the show_metrics.py script to display model performance metrics."""
    logger.info("Displaying model performance metrics")
    metrics_script = os.path.join(BASE_DIR, 'show_metrics.py')
    
    try:
        subprocess.run([sys.executable, metrics_script], check=True)
        logger.info("Metrics display completed")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error displaying metrics: {str(e)}")
        return False

def main():
    """Main function to orchestrate the ETL pipeline."""
    parser = argparse.ArgumentParser(description='Fraud Detection ETL Pipeline')
    parser.add_argument('--step', type=str, default='all', 
                        help="Run a specific step (1-5) or 'all' for the entire pipeline")
    parser.add_argument('--show-metrics', action='store_true',
                        help="Display model performance metrics after pipeline execution")
    args = parser.parse_args()
    
    # Define pipeline steps
    pipeline_steps = [
        (1, "Load Bank Transactions to MySQL", os.path.join(ETL_DIR, "1_load_to_mysql.py")),
        (2, "Transform Bank Features", os.path.join(ETL_DIR, "transform_bank_features.py")),
        (3, "Train Fraud Detection Model", os.path.join(ETL_DIR, "3_train_model.py")),
        (4, "Make Predictions and Load to Oracle", os.path.join(ETL_DIR, "4_predict_and_load_oracle.py"))
    ]
    
    # Determine which steps to run
    steps_to_run = []
    if args.step == 'all':
        steps_to_run = pipeline_steps
    else:
        try:
            step_num = int(args.step)
            if 1 <= step_num <= len(pipeline_steps):
                steps_to_run = [pipeline_steps[step_num - 1]]
            else:
                logger.error(f"Invalid step number: {step_num}. Must be between 1 and {len(pipeline_steps)}")
                return
        except ValueError:
            logger.error(f"Invalid step argument: {args.step}. Must be a number or 'all'")
            return
    
    # Run the selected steps
    logger.info(f"Starting Fraud Detection ETL Pipeline with {len(steps_to_run)} step(s)")
    pipeline_start_time = time.time()
    
    success = True
    for step_num, step_name, script_path in steps_to_run:
        step_success = run_step(step_num, step_name, script_path)
        if not step_success:
            logger.error(f"Pipeline failed at Step {step_num}: {step_name}")
            success = False
            break
    
    pipeline_end_time = time.time()
    pipeline_duration = pipeline_end_time - pipeline_start_time
    
    if success:
        logger.info(f"Fraud Detection ETL Pipeline completed successfully in {pipeline_duration:.2f} seconds")
        
        # Show metrics if requested or if we ran the full pipeline
        if args.show_metrics or (args.step == 'all' or args.step == '5'):
            show_metrics()
    else:
        logger.error(f"Fraud Detection ETL Pipeline failed after {pipeline_duration:.2f} seconds")

if __name__ == "__main__":
    main()