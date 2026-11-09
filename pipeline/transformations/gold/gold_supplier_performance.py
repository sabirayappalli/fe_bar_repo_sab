from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"


@dp.materialized_view(
    name=f"{TARGET}.gold_supplier_performance",
    comment="Gold: supplier delivery performance per supplier_id over all received POs: po_count, avg/stddev actual lead time (days), on_time_rate (share of POs with days_late <= 0), avg_fill_ratio and short_ship_rate (share of POs with fill_ratio < 0.9). Supplier is derived from the SKU on each PO.",
)
def gold_supplier_performance():
    receipts = spark.read.table(f"{TARGET}.silver_receipts")
    products = spark.read.table(f"{TARGET}.dim_product").select("sku_id", "supplier_id")
    suppliers = spark.read.table(f"{TARGET}.dim_supplier").select("supplier_id", "supplier_name")
    return (
        receipts.join(products, "sku_id", "inner")
        .join(suppliers, "supplier_id", "left")
        .groupBy("supplier_id", "supplier_name")
        .agg(
            F.count("po_id").alias("po_count"),
            F.round(F.avg("actual_lead_time_days"), 3).alias("avg_actual_lead_time_days"),
            F.round(F.stddev("actual_lead_time_days"), 3).alias("stddev_lead_time_days"),
            F.round(F.avg(F.when(F.col("days_late") <= 0, 1.0).otherwise(0.0)), 4).alias("on_time_rate"),
            F.round(F.avg("fill_ratio"), 4).alias("avg_fill_ratio"),
            F.round(F.avg(F.when(F.col("fill_ratio") < 0.9, 1.0).otherwise(0.0)), 4).alias("short_ship_rate"),
        )
    )
