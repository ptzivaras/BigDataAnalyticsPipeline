import os

from config import OUTPUT_PATH, PROCESSING_OUTPUT_DIR
from processing.spark_processing import run_processing


def run() -> None:
    os.makedirs(PROCESSING_OUTPUT_DIR, exist_ok=True)
    run_processing(OUTPUT_PATH, PROCESSING_OUTPUT_DIR)


if __name__ == "__main__":
    run()
