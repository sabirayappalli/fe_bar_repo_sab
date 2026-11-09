from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"


@dp.table(
    name=f"{TARGET}.silver_inventory",
    comment="Silver: typed end-of-day inventory snapshots, one row per store_id + sku_id + snapshot_date (on_hand_units, on_order_units).",
)
@dp.expect_or_drop("valid_keys", "store_id IS NOT NULL AND sku_id IS NOT NULL AND snapshot_date IS NOT NULL")
@dp.expect("non_negative_on_hand", "on_hand_units >= 0")
def silver_inventory():
    return (
        spark.readStream.table(f"{TARGET}.bronze_inventory")
        .select(
            F.col("store_id"),
            F.col("sku_id"),
            F.to_date("snapshot_date").alias("snapshot_date"),
            F.col("on_hand_units").cast("int").alias("on_hand_units"),
            F.col("on_order_units").cast("int").alias("on_order_units"),
            F.to_timestamp("snapshot_ts").alias("snapshot_ts"),
            F.col("source"),
            F.col("source_file"),
            F.col("ingest_ts"),
        )
        .withColumn("_snapshot_day_ts", F.col("snapshot_date").cast("timestamp"))
        .withWatermark("_snapshot_day_ts", "3 days")
        .dropDuplicatesWithinWatermark(["store_id", "sku_id", "snapshot_date"])
        .drop("_snapshot_day_ts")
    )
