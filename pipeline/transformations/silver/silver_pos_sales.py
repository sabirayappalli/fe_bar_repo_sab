from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"


@dp.temporary_view(
    name="pos_sales_validated",
    comment="Typed POS rows with quality rules applied BEFORE deduplication (null sku_id and negative units_sold dropped).",
)
@dp.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
@dp.expect_or_drop("non_negative_units_sold", "units_sold >= 0")
@dp.expect("valid_store_id", "store_id IS NOT NULL")
def pos_sales_validated():
    return (
        spark.readStream.table(f"{TARGET}.bronze_pos_sales")
        .select(
            F.col("store_id"),
            F.col("sku_id"),
            F.to_date("business_date").alias("business_date"),
            F.col("units_sold").cast("int").alias("units_sold"),
            F.col("net_sales").cast("double").alias("net_sales"),
            F.col("txn_count").cast("int").alias("txn_count"),
            F.col("pos_system"),
            F.to_timestamp("file_ts").alias("file_ts"),
            F.col("source_file"),
            F.col("ingest_ts"),
        )
    )


@dp.table(
    name=f"{TARGET}.silver_pos_sales",
    comment="Silver: cleaned daily POS sales, one row per store_id + sku_id + business_date. Typed columns; rows with null sku_id or negative units_sold are dropped by expectations, then exact duplicates are removed on (store_id, sku_id, business_date).",
)
def silver_pos_sales():
    return (
        spark.readStream.table("pos_sales_validated")
        .withColumn("_business_ts", F.col("business_date").cast("timestamp"))
        .withWatermark("_business_ts", "3 days")
        .dropDuplicatesWithinWatermark(["store_id", "sku_id", "business_date"])
        .drop("_business_ts")
    )
