from config import PROCESSING_OUTPUT_DIR
from storage.db_loader import run_storage


def run() -> None:
    run_storage(PROCESSING_OUTPUT_DIR)


if __name__ == "__main__":
    run()
