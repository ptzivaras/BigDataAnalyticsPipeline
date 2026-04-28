import os
import pandas as pd

from config import DATA_SOURCE_PATH, OUTPUT_PATH
from ingestion.loader import load_csv
from ingestion.validator import validate
from ingestion.preprocessor import preprocess
from ingestion.generator import generate


def run():
    print("=" * 50)
    print("INGESTION PIPELINE START")
    print("=" * 50)

    # 1. Load the real Kaggle data
    kaggle_df = load_csv(DATA_SOURCE_PATH)

    # 2. Validate it
    validate(kaggle_df)

    # 3. Light cleaning
    kaggle_df = preprocess(kaggle_df, source="kaggle")

    # 4. Generate synthetic rows with the same schema
    generated_df = generate()

    # 5. Combine both into one big dataset
    final_df = pd.concat([kaggle_df, generated_df], ignore_index=True)

    # 6. Save — this is what Spark will read next
    os.makedirs("data", exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)

    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print("=" * 50)
    print(f"OUTPUT: {OUTPUT_PATH}")
    print(f"ROWS:   {len(final_df):,}  ({size_mb:.1f} MB)")
    print(f"kaggle: {len(kaggle_df):,}  |  generated: {len(generated_df):,}")
    print("=" * 50)


if __name__ == "__main__":
    run()