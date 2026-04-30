import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BigDataAnalyticsPipeline-Layer2")
        .master("local[*]")
        .getOrCreate()
    )


def load_and_prepare(spark: SparkSession, input_path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(input_path)

    prepared = (
        df.withColumn("Customer ID", F.col("Customer ID").cast(IntegerType()))
        .withColumn("Gender", F.col("Gender").cast(StringType()))
        .withColumn("Age", F.col("Age").cast(IntegerType()))
        .withColumn("City", F.col("City").cast(StringType()))
        .withColumn("Membership Type", F.col("Membership Type").cast(StringType()))
        .withColumn("Total Spend", F.col("Total Spend").cast(DoubleType()))
        .withColumn("Items Purchased", F.col("Items Purchased").cast(IntegerType()))
        .withColumn("Average Rating", F.col("Average Rating").cast(DoubleType()))
        .withColumn("Discount Applied", F.col("Discount Applied").cast(BooleanType()))
        .withColumn(
            "Days Since Last Purchase",
            F.col("Days Since Last Purchase").cast(IntegerType()),
        )
        .withColumn("Satisfaction Level", F.col("Satisfaction Level").cast(StringType()))
        .withColumn("data_source", F.col("data_source").cast(StringType()))
    )

    # Derive a processing date to support daily revenue analytics.
    prepared = prepared.withColumn(
        "purchase_date",
        F.date_sub(F.current_date(), F.col("Days Since Last Purchase")),
    )

    # Derive product-like category because the dataset has no product column.
    prepared = prepared.withColumn(
        "product_category",
        F.when(F.col("Items Purchased") >= 18, F.lit("Premium Bundle"))
        .when(F.col("Items Purchased") >= 12, F.lit("Smart Bundle"))
        .when(F.col("Items Purchased") >= 8, F.lit("Essentials"))
        .otherwise(F.lit("Basics")),
    )

    return prepared


def revenue_per_day(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("purchase_date")
        .agg(
            F.round(F.sum("Total Spend"), 2).alias("revenue"),
            F.count(F.lit(1)).alias("transactions"),
            F.round(F.avg("Total Spend"), 2).alias("avg_order_value"),
        )
        .orderBy(F.col("purchase_date").asc())
    )


def top_products(df: DataFrame, limit: int = 10) -> DataFrame:
    return (
        df.groupBy("product_category")
        .agg(
            F.round(F.sum("Total Spend"), 2).alias("revenue"),
            F.sum("Items Purchased").alias("units"),
            F.count(F.lit(1)).alias("orders"),
        )
        .orderBy(F.col("revenue").desc())
        .limit(limit)
    )


def customer_segmentation(df: DataFrame) -> DataFrame:
    customer_kpis = (
        df.groupBy("Customer ID")
        .agg(
            F.round(F.sum("Total Spend"), 2).alias("lifetime_spend"),
            F.round(F.avg("Average Rating"), 2).alias("avg_rating"),
            F.sum("Items Purchased").alias("total_items"),
            F.max("Membership Type").alias("membership_type"),
            F.max("City").alias("city"),
        )
    )

    segmented = (
        customer_kpis.withColumn(
            "segment",
            F.when(F.col("lifetime_spend") >= 2000, F.lit("High Value"))
            .when(F.col("lifetime_spend") >= 900, F.lit("Mid Value"))
            .otherwise(F.lit("Low Value")),
        )
        .orderBy(F.col("lifetime_spend").desc())
    )

    return segmented


def run_processing(input_path: str, output_dir: str) -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        base_df = load_and_prepare(spark, input_path)

        revenue_df = revenue_per_day(base_df)
        top_products_df = top_products(base_df)
        segments_df = customer_segmentation(base_df)

        os.makedirs(output_dir, exist_ok=True)

        # Use pandas writer for Windows compatibility without winutils.exe.
        revenue_df.toPandas().to_csv(
            f"{output_dir}/revenue_per_day.csv", index=False
        )
        top_products_df.toPandas().to_csv(
            f"{output_dir}/top_products.csv", index=False
        )
        segments_df.toPandas().to_csv(
            f"{output_dir}/customer_segmentation.csv", index=False
        )

        print("=" * 50)
        print("SPARK PROCESSING COMPLETED")
        print("=" * 50)
        print(f"Input:  {input_path}")
        print(f"Output: {output_dir}")
        print("Generated: revenue_per_day.csv, top_products.csv, customer_segmentation.csv")
        print("=" * 50)
    finally:
        spark.stop()
