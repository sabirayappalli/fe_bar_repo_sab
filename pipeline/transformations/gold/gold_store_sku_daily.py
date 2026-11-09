from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"
KEYS = ["store_id", "sku_id", "date"]


@dp.materialized_view(
    name=f"{TARGET}.gold_store_sku_daily",
    comment="Gold: one row per store_id, sku_id, date (40 stores x 150 SKUs x 184 days = 1,104,000 rows). Inventory snapshot left-joined to POS sales (units_sold / net_sales = 0 when no sales) and enriched with store, product and supplier attributes. is_stockout = on_hand_units = 0; on_promo = a promotion for the SKU covers the date in the store's region or 'ALL'; promo_lift = expected_lift else 1.0. Feature base for the stockout ML model, Lakebase, Genie and the replenishment app.",
    cluster_by=["date", "store_id"],
)
@dp.expect("has_store_attributes", "store_name IS NOT NULL")
@dp.expect("has_product_attributes", "sku_name IS NOT NULL")
def gold_store_sku_daily():
    inv = spark.read.table(f"{TARGET}.silver_inventory").select(
        "store_id", "sku_id", F.col("snapshot_date").alias("date"), "on_hand_units", "on_order_units"
    )
    sales = spark.read.table(f"{TARGET}.silver_pos_sales").select(
        "store_id", "sku_id", F.col("business_date").alias("date"), "units_sold", "net_sales"
    )
    stores = spark.read.table(f"{TARGET}.dim_store").select("store_id", "store_name", "region", "format")
    products = spark.read.table(f"{TARGET}.dim_product").select(
        "sku_id", "sku_name", "category", "supplier_id", "unit_price", "unit_cost",
        "case_pack", "shelf_capacity", "is_perishable",
    )
    suppliers = spark.read.table(f"{TARGET}.dim_supplier").select("supplier_id", "supplier_name")
    promos = spark.read.table(f"{TARGET}.silver_promotions").select(
        F.col("sku_id").alias("p_sku_id"), F.col("region").alias("p_region"),
        "start_date", "end_date", "expected_lift",
    )

    base = (
        inv.join(sales, KEYS, "left")
        .join(stores, "store_id", "left")
        .join(products, "sku_id", "left")
        .join(suppliers, "supplier_id", "left")
    )

    # Applicable promotion per store-SKU-day (max lift if several overlap)
    promo_match = (
        base.select(*KEYS, "region")
        .join(
            promos,
            (F.col("sku_id") == F.col("p_sku_id"))
            & (F.col("date").between(F.col("start_date"), F.col("end_date")))
            & ((F.col("p_region") == F.col("region")) | (F.col("p_region") == "ALL")),
            "inner",
        )
        .groupBy(*KEYS)
        .agg(F.max("expected_lift").alias("_lift"))
    )

    return (
        base.join(promo_match, KEYS, "left")
        .select(
            "date", "store_id", "store_name", "region", "format",
            "sku_id", "sku_name", "category", "supplier_id", "supplier_name",
            "unit_price", "unit_cost", "case_pack", "shelf_capacity", "is_perishable",
            F.coalesce("units_sold", F.lit(0)).alias("units_sold"),
            F.coalesce("net_sales", F.lit(0.0)).alias("net_sales"),
            "on_hand_units", "on_order_units",
            (F.col("on_hand_units") == 0).alias("is_stockout"),
            F.col("_lift").isNotNull().alias("on_promo"),
            F.coalesce("_lift", F.lit(1.0)).alias("promo_lift"),
        )
    )
