from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
LANDING = "/Volumes/serverless_stable_zlgeaw_catalog/stockout_raw/landing"

# (table name, folder, file glob, comment)
CSV_SOURCES = [
    ("bronze_stores", "master", "stores*.csv",
     "Bronze: raw store master (stores.csv) ingested incrementally with Auto Loader. Contains synthetic PII (store_manager, manager_email, manager_phone). Raw strings plus source_file and ingest_ts."),
    ("bronze_products", "master", "products*.csv",
     "Bronze: raw product/SKU master (products.csv) ingested incrementally with Auto Loader. Raw strings plus source_file and ingest_ts."),
    ("bronze_suppliers", "master", "suppliers*.csv",
     "Bronze: raw supplier master (suppliers.csv) ingested incrementally with Auto Loader. Raw strings plus source_file and ingest_ts."),
    ("bronze_promotions", "promotions", "promotions*.csv",
     "Bronze: raw promotions calendar (promotions.csv) ingested incrementally with Auto Loader. Raw strings plus source_file and ingest_ts."),
]


def _define_bronze_csv(table_name, folder, glob, comment):
    @dp.table(name=f"{TARGET}.{table_name}", comment=comment)
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("pathGlobFilter", glob)
            .load(f"{LANDING}/{folder}/")
            .withColumn("source_file", F.col("_metadata.file_name"))
            .withColumn("ingest_ts", F.current_timestamp())
        )


for _name, _folder, _glob, _comment in CSV_SOURCES:
    _define_bronze_csv(_name, _folder, _glob, _comment)
