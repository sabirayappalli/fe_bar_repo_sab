from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
LANDING = "/Volumes/serverless_stable_zlgeaw_catalog/stockout_raw/landing"


@dp.table(
    name=f"{TARGET}.bronze_receipts",
    comment="Bronze: raw purchase-order receipts (receipts_YYYYMMDD.json, JSON lines) ingested incrementally with Auto Loader. Raw string columns plus source_file and ingest_ts.",
)
def bronze_receipts():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{LANDING}/receipts/")
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
