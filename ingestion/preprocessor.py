import pandas as pd


SCHEMA_TYPES = {
    "Customer ID": "Int64",
    "Gender": "string",
    "Age": "Int64",
    "City": "string",
    "Membership Type": "string",
    "Total Spend": "float64",
    "Items Purchased": "Int64",
    "Average Rating": "float64",
    "Discount Applied": "boolean",
    "Days Since Last Purchase": "Int64",
    "Satisfaction Level": "string",
    "data_source": "string",
}


def cast_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    typed = df.copy()

    # Numeric fields
    for col in [
        "Customer ID",
        "Age",
        "Items Purchased",
        "Days Since Last Purchase",
        "Total Spend",
        "Average Rating",
    ]:
        if col in typed.columns:
            typed[col] = pd.to_numeric(typed[col], errors="coerce")

    if "Discount Applied" in typed.columns:
        typed["Discount Applied"] = (
            typed["Discount Applied"].astype(str).str.lower().isin(["true", "1", "yes"])
        )

    for col, dtype in SCHEMA_TYPES.items():
        if col in typed.columns:
            typed[col] = typed[col].astype(dtype)

    return typed


def preprocess(df: pd.DataFrame, source: str) -> pd.DataFrame:
    rows_before = len(df)

    # Remove duplicates
    df = df.drop_duplicates()
    rows_after_dedup = len(df)

    # Drop rows missing critical fields
    df = df.dropna(subset=["Customer ID", "Total Spend", "Membership Type"])
    rows_after_drop_critical_nulls = len(df)

    # Fill missing Satisfaction Level
    df = df.copy()
    if "Satisfaction Level" in df.columns:
        df["Satisfaction Level"] = df["Satisfaction Level"].fillna("Unknown")

    # Ensure Discount Applied is always bool
    if "Discount Applied" in df.columns:
        df["Discount Applied"] = df["Discount Applied"].astype(str).str.lower().isin(["true", "1", "yes"])

    # Tag where this row came from (useful later in Spark)
    df["data_source"] = source

    print(f"[preprocessor] {source}: {len(df)} rows after cleaning")
    stats = {
        "rows_before": int(rows_before),
        "rows_after": int(len(df)),
        "duplicates_removed": int(rows_before - rows_after_dedup),
        "rows_removed_missing_critical": int(rows_after_dedup - rows_after_drop_critical_nulls),
    }
    return df, stats