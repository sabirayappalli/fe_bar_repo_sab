from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
LANDING = "/Volumes/serverless_stable_zlgeaw_catalog/stockout_raw/landing"


@dp.table(
    name=f"{TARGET}.bronze_pos_sales",
    comment="Bronze: raw daily POS sales files (pos_sales_YYYYMMDD.csv) ingested incrementally with Auto Loader. All columns kept as raw strings; includes injected defects (duplicates, units_sold = -1, null sku_id). Adds source_file and ingest_ts.",
)
def bronze_pos_sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{LANDING}/pos_sales/")
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
