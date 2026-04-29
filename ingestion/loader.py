import os
import pandas as pd


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"[loader] CSV file not found: {path}")

    df = pd.read_csv(path)
    print(f"[loader] Loaded {len(df)} rows from {path}")
    return df