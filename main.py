"""
BigDataAnalyticsPipeline — Full Pipeline Orchestrator
Runs all 4 layers in sequence:
  Layer 1: Ingestion
  Layer 2: PySpark Processing
  Layer 3: PostgreSQL Storage
  Layer 4: Analytics (notebook must be run separately via Jupyter)
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_ingestion():
    logger.info("=== LAYER 1: Ingestion ===")
    from run_ingestion import run as ingestion_run
    ingestion_run()


def run_processing():
    logger.info("=== LAYER 2: PySpark Processing ===")
    from config import OUTPUT_PATH, PROCESSING_OUTPUT_DIR
    from processing.spark_processing import run_processing as spark_run
    spark_run(OUTPUT_PATH, PROCESSING_OUTPUT_DIR)


def run_storage():
    logger.info("=== LAYER 3: PostgreSQL Storage ===")
    from config import PROCESSING_OUTPUT_DIR
    from storage.db_loader import run_storage as db_run
    db_run(PROCESSING_OUTPUT_DIR)


if __name__ == "__main__":
    try:
        run_ingestion()
        run_processing()
        run_storage()
        logger.info("=== Pipeline complete. Run analytics/pipeline_analytics.ipynb for Layer 4. ===")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
