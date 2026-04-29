import pandas as pd


EXPECTED_COLUMNS = [
    "Customer ID", "Gender", "Age", "City", "Membership Type",
    "Total Spend", "Items Purchased", "Average Rating",
    "Discount Applied", "Days Since Last Purchase", "Satisfaction Level",
]

CRITICAL_COLUMNS = ["Customer ID", "Total Spend", "Membership Type"]

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


def validate(df: pd.DataFrame) -> dict:
    print("[validator] Running checks...")

    if df.empty:
        raise ValueError("[validator] Input dataset is empty")

    report = {
        "rows_input": int(len(df)),
        "missing_columns": [],
        "null_counts": {},
        "duplicate_rows": 0,
        "out_of_range_counts": {},
        "unexpected_values": {},
    }

    # Missing columns
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    report["missing_columns"] = missing
    if missing:
        print(f"  [FAIL] Missing columns: {missing}")
    else:
        print(f"  [OK] All {len(EXPECTED_COLUMNS)} columns present")

    missing_critical = [c for c in CRITICAL_COLUMNS if c not in df.columns]
    if missing_critical:
        raise ValueError(f"[validator] Missing critical columns: {missing_critical}")

    # Nulls
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    report["null_counts"] = {col: int(val) for col, val in nulls.items()}
    if nulls.empty:
        print("  [OK] No nulls")
    else:
        print(f"  [WARN] Nulls found:\n{nulls.to_string()}")

    critical_nulls = {col: int(df[col].isnull().sum()) for col in CRITICAL_COLUMNS}
    critical_nulls = {col: cnt for col, cnt in critical_nulls.items() if cnt > 0}
    if critical_nulls:
        raise ValueError(f"[validator] Critical columns contain nulls: {critical_nulls}")

    # Duplicates
    dups = df.duplicated().sum()
    report["duplicate_rows"] = int(dups)
    print(f"  [OK] No duplicates" if dups == 0 else f"  [WARN] {dups} duplicate rows")

    # Value ranges
    for col, (lo, hi) in VALUE_RANGES.items():
        if col not in df.columns:
            continue
        bad = ((df[col] < lo) | (df[col] > hi)).sum()
        report["out_of_range_counts"][col] = int(bad)
        if bad:
            print(f"  [WARN] '{col}': {bad} values outside [{lo}, {hi}]")
        else:
            print(f"  [OK] '{col}' range is valid")

    # Categorical values
    for col, allowed in ALLOWED_VALUES.items():
        if col not in df.columns:
            continue
        unexpected = set(df[col].dropna().unique()) - allowed
        report["unexpected_values"][col] = sorted([str(x) for x in unexpected])
        if unexpected:
            print(f"  [WARN] '{col}': unexpected values {unexpected}")
        else:
            print(f"  [OK] '{col}' values are valid")

    print("[validator] Done")
    return report