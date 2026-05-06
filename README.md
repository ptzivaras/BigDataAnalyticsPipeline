# BigDataAnalyticsPipeline

End-to-end data pipeline project. Takes a Kaggle dataset, generates 500k synthetic rows, processes with PySpark, stores in PostgreSQL, and visualizes with Jupyter.

Built big data workflows: ingestion, transformation, storage, and analytics.

## Quick Start

Requirements: Python 3.10+, PostgreSQL running locally

```bash
git clone https://github.com/ptzivaras/BigDataAnalyticsPipeline.git
cd BigDataAnalyticsPipeline
pip install pandas numpy pyspark psycopg2-binary python-dotenv jupyter matplotlib seaborn
python main.py
```

Then open the notebook:
```bash
jupyter notebook analytics/pipeline_analytics.ipynb
```

## What It Does

The pipeline has 4 layers:

1. Ingestion: Load Kaggle CSV, generate 500k matching rows, validate data quality, output clean dataset

2. Processing: Read the dataset with PySpark, compute revenue per day, top products by revenue, customer segmentation

3. Storage: Load the 3 processed tables into PostgreSQL

4. Analytics: Query the database from Jupyter, create visualizations (revenue trends, product rankings, customer breakdown)

Output: 4 PNG charts saved to analytics/

## Tech Stack

- Python: data generation and orchestration
- pandas: data manipulation and CSV I/O
- numpy: numeric operations
- PySpark: distributed processing and aggregations
- PostgreSQL: relational storage
- psycopg2: Python-PostgreSQL connector
- Jupyter: interactive analysis
- matplotlib / seaborn: visualization


## How Each Layer Works

### Layer 1 - Ingestion

- Loads the Kaggle CSV (original schema and patterns)
- Generates synthetic rows with same structure to reach 500k total
- Validates data: checks for missing critical columns, null values, duplicates, out-of-range values
- Type casts all columns to stable types (int64, float64, string, bool)
- Saves to data/final_dataset.csv
- Outputs data/ingestion_report.json with quality metrics

### Layer 2 - PySpark Processing

- Reads final_dataset.csv into a Spark DataFrame
- Creates purchase_date by calculating backwards from today using "Days Since Last Purchase"
- Derives product_category based on "Items Purchased" (Premium Bundle, Smart Bundle, Essentials, Basics)
- Runs 3 aggregations:
  - Revenue per day (daily sum, transaction count, average order value)
  - Top products by total revenue
  - Customer segmentation (lifetime spend, average rating, total items, membership tier)
- Saves to data/processing/ as CSV files
- Note: Uses pandas write due to Windows environment limitations with Spark

### Layer 3 - PostgreSQL Storage

- Reads the 3 processing CSVs
- Creates tables: revenue_per_day, top_products, customer_segmentation
- Truncates and reloads on each run (full refresh strategy)
- No manual SQL needed; schema created automatically
- Credentials loaded from .env

### Layer 4 - Analytics

- Connects to PostgreSQL from Jupyter
- Loads the 3 tables into pandas DataFrames
- Creates 4 visualizations:
  - Daily revenue with 7-day rolling average (line chart)
  - Top products by revenue (bar chart)
  - Customer count and average spend by membership tier (pie + bar)
  - Rating distribution by membership (histogram)
- Exports PNG charts to analytics/

## Setup

1. Clone the repo

2. Create a Python virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install pandas numpy pyspark psycopg2-binary python-dotenv jupyter matplotlib seaborn
```

4. Create a PostgreSQL database (local):
```sql
CREATE DATABASE bigdata_pipeline;
```

5. Create .env file in project root:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bigdata_pipeline
DB_USER=postgres
DB_PASSWORD=your_password
```

6. Run the pipeline:
```bash
python main.py
```

7. View results:
```bash
jupyter notebook analytics/pipeline_analytics.ipynb
```

## Notes

- Ingestion generates realistic synthetic data by following patterns from the Kaggle dataset (membership distributions, spend ranges, satisfaction scores)
- PySpark runs in local mode on a single machine; easily adaptable to cluster mode
- PostgreSQL is used for portability; can be replaced with any SQL database by updating db_loader.py
- All temporary files (.venv, data/, .env) are excluded from git via .gitignore
