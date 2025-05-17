# Fake-Fraud-Transaction-Detection

A comprehensive fraud detection system that processes transaction data through an ETL pipeline to identify potentially fraudulent activities. The system includes:

- Transaction data loading to MySQL
- Feature engineering and transformation
- Machine learning model training
- Prediction generation and Oracle database integration
- Performance metrics visualization

## Features

- Complete ETL pipeline orchestration
- Modular design for easy maintenance
- Detailed logging and performance tracking
- Configurable pipeline steps
- Model performance metrics visualization

## Requirements

- Python 3.x
- MySQL Database
- Oracle Database
- Required Python packages (to be listed in requirements.txt)

## Usage

Run the pipeline using:
```bash
python app.py [--step STEP] [--show-metrics]
```

Options:
- `--step STEP`: Run a specific step (1-5) or 'all' for the entire pipeline
- `--show-metrics`: Display model performance metrics after pipeline execution 