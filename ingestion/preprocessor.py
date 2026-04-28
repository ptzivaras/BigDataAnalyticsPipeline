import pandas as pd


def preprocess(df: pd.DataFrame, source: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    # Drop rows missing critical fields
    df = df.dropna(subset=["Customer ID", "Total Spend", "Membership Type"])

    # Fill missing Satisfaction Level
    df["Satisfaction Level"] = df["Satisfaction Level"].fillna("Unknown")

    # Ensure Discount Applied is always bool
    df["Discount Applied"] = df["Discount Applied"].astype(str).str.lower().isin(["true", "1", "yes"])

    # Tag where this row came from (useful later in Spark)
    df["data_source"] = source

    print(f"[preprocessor] {source}: {len(df)} rows after cleaning")
    return df