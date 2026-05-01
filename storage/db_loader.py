import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# ── DDL ──────────────────────────────────────────────────────────────────────

CREATE_REVENUE_PER_DAY = """
CREATE TABLE IF NOT EXISTS revenue_per_day (
    purchase_date   DATE        PRIMARY KEY,
    revenue         NUMERIC(14,2),
    transactions    INTEGER,
    avg_order_value NUMERIC(10,2)
);
"""

CREATE_TOP_PRODUCTS = """
CREATE TABLE IF NOT EXISTS top_products (
    product_category VARCHAR(64) PRIMARY KEY,
    revenue          NUMERIC(18,2),
    units            BIGINT,
    orders           INTEGER
);
"""

CREATE_CUSTOMER_SEGMENTATION = """
CREATE TABLE IF NOT EXISTS customer_segmentation (
    customer_id    INTEGER PRIMARY KEY,
    lifetime_spend NUMERIC(14,2),
    avg_rating     NUMERIC(4,2),
    total_items    BIGINT,
    membership_type VARCHAR(32),
    city           VARCHAR(64),
    segment        VARCHAR(32)
);
"""


def _create_tables(cur) -> None:
    cur.execute(CREATE_REVENUE_PER_DAY)
    cur.execute(CREATE_TOP_PRODUCTS)
    cur.execute(CREATE_CUSTOMER_SEGMENTATION)


# ── Loaders ──────────────────────────────────────────────────────────────────

def _load_revenue_per_day(cur, df: pd.DataFrame) -> int:
    cur.execute("TRUNCATE TABLE revenue_per_day")
    rows = [
        (
            row["purchase_date"],
            float(row["revenue"]),
            int(row["transactions"]),
            float(row["avg_order_value"]),
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        "INSERT INTO revenue_per_day (purchase_date, revenue, transactions, avg_order_value) VALUES %s",
        rows,
    )
    return len(rows)


def _load_top_products(cur, df: pd.DataFrame) -> int:
    cur.execute("TRUNCATE TABLE top_products")
    rows = [
        (
            str(row["product_category"]),
            float(row["revenue"]),
            int(row["units"]),
            int(row["orders"]),
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        "INSERT INTO top_products (product_category, revenue, units, orders) VALUES %s",
        rows,
    )
    return len(rows)


def _load_customer_segmentation(cur, df: pd.DataFrame) -> int:
    cur.execute("TRUNCATE TABLE customer_segmentation")
    rows = [
        (
            int(row["Customer ID"]),
            float(row["lifetime_spend"]),
            float(row["avg_rating"]),
            int(row["total_items"]),
            str(row["membership_type"]),
            str(row["city"]),
            str(row["segment"]),
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        """INSERT INTO customer_segmentation
           (customer_id, lifetime_spend, avg_rating, total_items, membership_type, city, segment)
           VALUES %s""",
        rows,
    )
    return len(rows)


# ── Public entry ─────────────────────────────────────────────────────────────

def run_storage(processing_dir: str) -> None:
    revenue_path    = f"{processing_dir}/revenue_per_day.csv"
    products_path   = f"{processing_dir}/top_products.csv"
    segments_path   = f"{processing_dir}/customer_segmentation.csv"

    for path in (revenue_path, products_path, segments_path):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[storage] Missing processing output: {path}\n"
                "Run run_processing.py first."
            )

    revenue_df  = pd.read_csv(revenue_path)
    products_df = pd.read_csv(products_path)
    segments_df = pd.read_csv(segments_path)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _create_tables(cur)

                n_rev  = _load_revenue_per_day(cur, revenue_df)
                n_prod = _load_top_products(cur, products_df)
                n_seg  = _load_customer_segmentation(cur, segments_df)

        print("=" * 50)
        print("STORAGE LAYER COMPLETED")
        print("=" * 50)
        print(f"  revenue_per_day       → {n_rev:,} rows")
        print(f"  top_products          → {n_prod:,} rows")
        print(f"  customer_segmentation → {n_seg:,} rows")
        print("=" * 50)
    finally:
        conn.close()
