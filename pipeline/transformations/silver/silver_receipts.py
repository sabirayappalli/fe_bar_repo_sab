from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"


@dp.table(
    name=f"{TARGET}.silver_receipts",
    comment="Silver: typed purchase-order receipts, one row per po_id, with actual_lead_time_days (received_date - order_date), days_late (received_date - expected_date) and fill_ratio (received_units / ordered_units).",
)
@dp.expect_or_drop("valid_po", "po_id IS NOT NULL AND store_id IS NOT NULL AND sku_id IS NOT NULL")
@dp.expect_or_drop("positive_ordered_units", "ordered_units > 0")
@dp.expect("received_after_order", "received_date >= order_date")
def silver_receipts():
    return (
        spark.readStream.table(f"{TARGET}.bronze_receipts")
        .select(
            F.col("po_id"),
            F.col("store_id"),
            F.col("sku_id"),
            F.to_date("order_date").alias("order_date"),
            F.to_date("expected_date").alias("expected_date"),
            F.to_date("received_date").alias("received_date"),
            F.col("ordered_units").cast("int").alias("ordered_units"),
            F.col("received_units").cast("int").alias("received_units"),
            F.col("source_file"),
            F.col("ingest_ts"),
        )
        .withColumn("actual_lead_time_days", F.datediff("received_date", "order_date"))
        .withColumn("days_late", F.datediff("received_date", "expected_date"))
        .withColumn("fill_ratio", F.round(F.col("received_units") / F.col("ordered_units"), 4))
        .withColumn("_order_ts", F.col("order_date").cast("timestamp"))
        .withWatermark("_order_ts", "30 days")
        .dropDuplicatesWithinWatermark(["po_id"])
        .drop("_order_ts")
    )
