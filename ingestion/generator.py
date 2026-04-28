import numpy as np
import pandas as pd
from config import GENERATED_ROWS, RANDOM_SEED

np.random.seed(RANDOM_SEED)

# These distributions are derived from the real Kaggle dataset
# so generated rows look realistic, not random garbage.

CITIES           = ["New York", "Los Angeles", "Chicago", "San Francisco", "Miami", "Houston"]
MEMBERSHIP_TYPES = ["Gold", "Silver", "Bronze"]
GENDERS          = ["Male", "Female"]
SATISFACTION     = ["Satisfied", "Neutral", "Unsatisfied"]

# Gold members spend more, buy more, are happier
SPEND_BY_MEMBERSHIP   = {"Gold": (1200, 300), "Silver": (750, 200), "Bronze": (450, 150)}
ITEMS_BY_MEMBERSHIP   = {"Gold": (15, 4),     "Silver": (10, 3),    "Bronze": (7, 2)}
DISCOUNT_BY_MEMBERSHIP = {"Gold": 0.6,        "Silver": 0.45,       "Bronze": 0.30}
SATISFACTION_BY_MEMBERSHIP = {
    "Gold":   [0.70, 0.20, 0.10],
    "Silver": [0.50, 0.30, 0.20],
    "Bronze": [0.35, 0.35, 0.30],
}


def generate(n_rows: int = GENERATED_ROWS) -> pd.DataFrame:
    print(f"[generator] Generating {n_rows:,} rows...")

    membership = np.random.choice(MEMBERSHIP_TYPES, size=n_rows)

    spend = np.array([
        max(0.01, round(np.random.normal(*SPEND_BY_MEMBERSHIP[m]), 2))
        for m in membership
    ])
    items = np.array([
        max(1, int(np.random.normal(*ITEMS_BY_MEMBERSHIP[m])))
        for m in membership
    ])
    discount = np.array([
        np.random.rand() < DISCOUNT_BY_MEMBERSHIP[m]
        for m in membership
    ])
    satisfaction = np.array([
        np.random.choice(SATISFACTION, p=SATISFACTION_BY_MEMBERSHIP[m])
        for m in membership
    ])

    # Customer IDs start at 1000 to avoid collision with Kaggle IDs (101–450)
    n_unique_customers = max(1000, n_rows // 8)
    customer_ids = np.random.randint(1000, 1000 + n_unique_customers, size=n_rows)

    df = pd.DataFrame({
        "Customer ID":              customer_ids,
        "Gender":                   np.random.choice(GENDERS, size=n_rows),
        "Age":                      np.clip(np.random.normal(33, 5, n_rows).astype(int), 18, 80),
        "City":                     np.random.choice(CITIES, size=n_rows),
        "Membership Type":          membership,
        "Total Spend":              spend,
        "Items Purchased":          items,
        "Average Rating":           np.clip(np.random.normal(4.0, 0.6, n_rows).round(1), 1.0, 5.0),
        "Discount Applied":         discount,
        "Days Since Last Purchase": np.clip(np.random.normal(27, 13, n_rows).astype(int), 0, 365),
        "Satisfaction Level":       satisfaction,
        "data_source":              "generated",
    })

    print(f"[generator] Done — {len(df):,} rows created")
    return df