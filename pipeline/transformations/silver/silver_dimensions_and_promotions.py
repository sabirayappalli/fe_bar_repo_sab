from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
PII_MASK = f"{TARGET}.mask_pii"  # UC SQL UDF created by notebook 02_uc_governance


def _latest_per_key(df, key):
    # Keep the most recently ingested version of each master-data record
    w = Window.partitionBy(key).orderBy(F.col("ingest_ts").desc(), F.col("source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name=f"{TARGET}.dim_store",
    comment="Silver dimension: one row per store (40 FreshMart stores) with region, format, size, open date and store manager contact. store_manager, manager_email and manager_phone are synthetic PII.",
    schema=f"""
        store_id STRING COMMENT 'Store business key, e.g. S001',
        store_name STRING COMMENT 'Store display name',
        region STRING COMMENT 'Sales region: Northeast, Southeast, Midwest, Southwest, West',
        format STRING COMMENT 'Store format: Supercenter, Neighborhood, Express',
        sq_ft INT COMMENT 'Selling area in square feet',
        open_date DATE COMMENT 'Store opening date',
        store_manager STRING MASK {PII_MASK} COMMENT 'PII: store manager full name (masked unless member of stockout_pii_readers)',
        manager_email STRING MASK {PII_MASK} COMMENT 'PII: store manager email (masked unless member of stockout_pii_readers)',
        manager_phone STRING MASK {PII_MASK} COMMENT 'PII: store manager phone (masked unless member of stockout_pii_readers)'
    """,
)
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def dim_store():
    df = spark.read.table(f"{TARGET}.bronze_stores").select(
        "store_id", "store_name", "region", "format",
        F.col("sq_ft").cast("int").alias("sq_ft"),
        F.to_date("open_date").alias("open_date"),
        "store_manager", "manager_email", "manager_phone",
        "source_file", "ingest_ts",
    )
    return _latest_per_key(df, "store_id").drop("source_file", "ingest_ts")


@dp.materialized_view(
    name=f"{TARGET}.dim_product",
    comment="Silver dimension: one row per SKU (150 SKUs) with category, supplier, unit price/cost, case pack, perishability and shelf capacity.",
)
@dp.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
@dp.expect("price_above_cost", "unit_price >= unit_cost")
def dim_product():
    df = spark.read.table(f"{TARGET}.bronze_products").select(
        "sku_id", "sku_name", "category", "supplier_id",
        F.col("unit_price").cast("double").alias("unit_price"),
        F.col("unit_cost").cast("double").alias("unit_cost"),
        F.col("case_pack").cast("int").alias("case_pack"),
        (F.lower("is_perishable") == "true").alias("is_perishable"),
        F.col("shelf_capacity").cast("int").alias("shelf_capacity"),
        "source_file", "ingest_ts",
    )
    return _latest_per_key(df, "sku_id").drop("source_file", "ingest_ts")


@dp.materialized_view(
    name=f"{TARGET}.dim_supplier",
    comment="Silver dimension: one row per supplier (10 suppliers) with contracted lead time mean/sd (days) and nominal fill rate.",
)
@dp.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
def dim_supplier():
    df = spark.read.table(f"{TARGET}.bronze_suppliers").select(
        "supplier_id", "supplier_name",
        F.col("lead_time_days_mean").cast("double").alias("lead_time_days_mean"),
        F.col("lead_time_days_sd").cast("double").alias("lead_time_days_sd"),
        F.col("fill_rate").cast("double").alias("fill_rate"),
        "source_file", "ingest_ts",
    )
    return _latest_per_key(df, "supplier_id").drop("source_file", "ingest_ts")


@dp.materialized_view(
    name=f"{TARGET}.silver_promotions",
    comment="Silver: typed promotions calendar, one row per promo_id. region is a store region or 'ALL'; promotion applies from start_date to end_date inclusive with expected_lift demand multiplier.",
)
@dp.expect_or_drop("valid_promo", "promo_id IS NOT NULL AND sku_id IS NOT NULL")
@dp.expect_or_drop("valid_date_range", "start_date <= end_date")
def silver_promotions():
    df = spark.read.table(f"{TARGET}.bronze_promotions").select(
        "promo_id", "sku_id", "region",
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        "promo_type",
        F.col("expected_lift").cast("double").alias("expected_lift"),
        "source_file", "ingest_ts",
    )
    return _latest_per_key(df, "promo_id").drop("source_file", "ingest_ts")
