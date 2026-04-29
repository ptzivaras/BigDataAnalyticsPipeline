import os
import json
import pandas as pd

from config import DATA_SOURCE_PATH, OUTPUT_PATH
from ingestion.loader import load_csv
from ingestion.validator import validate
from ingestion.preprocessor import preprocess, cast_to_schema
from ingestion.generator import generate


def run():
    print("=" * 50)
    print("INGESTION PIPELINE START")
    print("=" * 50)

    # 1. Load the real Kaggle data
    kaggle_df = load_csv(DATA_SOURCE_PATH)

    # 2. Validate it
    validation_report = validate(kaggle_df)

    # 3. Light cleaning
    kaggle_df, preprocess_report = preprocess(kaggle_df, source="kaggle")

    # 4. Generate synthetic rows with the same schema
    generated_df = generate()

    # 5. Combine both into one big dataset
    final_df = pd.concat([kaggle_df, generated_df], ignore_index=True)

    # 6. Enforce stable schema before save (Spark-ready)
    final_df = cast_to_schema(final_df)

    # 7. Save — this is what Spark will read next
    os.makedirs("data", exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)

    report_path = os.path.join("data", "ingestion_report.json")
    quality_report = {
        "input_file": DATA_SOURCE_PATH,
        "output_file": OUTPUT_PATH,
        "rows": {
            "kaggle_loaded": int(validation_report["rows_input"]),
            "kaggle_after_preprocess": int(preprocess_report["rows_after"]),
            "generated": int(len(generated_df)),
            "final": int(len(final_df)),
        },
        "preprocess": preprocess_report,
        "validation": {
            "missing_columns": validation_report["missing_columns"],
            "null_counts": validation_report["null_counts"],
            "duplicate_rows": validation_report["duplicate_rows"],
            "out_of_range_counts": validation_report["out_of_range_counts"],
            "unexpected_values": validation_report["unexpected_values"],
        },
        "schema_dtypes": {col: str(dtype) for col, dtype in final_df.dtypes.items()},
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, indent=2)

    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print("=" * 50)
    print(f"OUTPUT: {OUTPUT_PATH}")
    print(f"REPORT: {report_path}")
    print(f"ROWS:   {len(final_df):,}  ({size_mb:.1f} MB)")
    print(f"kaggle: {len(kaggle_df):,}  |  generated: {len(generated_df):,}")
    print("=" * 50)


if __name__ == "__main__":
    run()