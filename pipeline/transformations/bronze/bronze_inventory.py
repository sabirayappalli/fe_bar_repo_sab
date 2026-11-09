from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
LANDING = "/Volumes/serverless_stable_zlgeaw_catalog/stockout_raw/landing"


@dp.table(
    name=f"{TARGET}.bronze_inventory",
    comment="Bronze: raw end-of-day inventory snapshots (inventory_YYYYMMDD.json, JSON lines) for every store x SKU, ingested incrementally with Auto Loader. Raw string columns plus source_file and ingest_ts.",
)
def bronze_inventory():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{LANDING}/inventory/")
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
