import pandas as pd


EXPECTED_COLUMNS = [
    "Customer ID", "Gender", "Age", "City", "Membership Type",
    "Total Spend", "Items Purchased", "Average Rating",
    "Discount Applied", "Days Since Last Purchase", "Satisfaction Level",
]

VALUE_RANGES = {
    "Age":                     (18, 100),
    "Total Spend":             (0, 100_000),
    "Items Purchased":         (1, 10_000),
    "Average Rating":          (1.0, 5.0),
    "Days Since Last Purchase": (0, 3650),
}

ALLOWED_VALUES = {
    "Gender":           {"Male", "Female"},
    "City":             {"New York", "Los Angeles", "Chicago", "San Francisco", "Miami", "Houston"},
    "Membership Type":  {"Gold", "Silver", "Bronze"},
    "Satisfaction Level": {"Satisfied", "Neutral", "Unsatisfied"},
}


def validate(df: pd.DataFrame) -> None:
    print("[validator] Running checks...")

    # Missing columns
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        print(f"  [FAIL] Missing columns: {missing}")
    else:
        print(f"  [OK] All {len(EXPECTED_COLUMNS)} columns present")

    # Nulls
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if nulls.empty:
        print("  [OK] No nulls")
    else:
        print(f"  [WARN] Nulls found:\n{nulls.to_string()}")

    # Duplicates
    dups = df.duplicated().sum()
    print(f"  [OK] No duplicates" if dups == 0 else f"  [WARN] {dups} duplicate rows")

    # Value ranges
    for col, (lo, hi) in VALUE_RANGES.items():
        if col not in df.columns:
            continue
        bad = ((df[col] < lo) | (df[col] > hi)).sum()
        if bad:
            print(f"  [WARN] '{col}': {bad} values outside [{lo}, {hi}]")
        else:
            print(f"  [OK] '{col}' range is valid")

    # Categorical values
    for col, allowed in ALLOWED_VALUES.items():
        if col not in df.columns:
            continue
        unexpected = set(df[col].dropna().unique()) - allowed
        if unexpected:
            print(f"  [WARN] '{col}': unexpected values {unexpected}")
        else:
            print(f"  [OK] '{col}' values are valid")

    print("[validator] Done")