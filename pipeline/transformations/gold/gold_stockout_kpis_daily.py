from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "serverless_stable_zlgeaw_catalog.stockout_core"


@dp.materialized_view(
    name=f"{TARGET}.gold_stockout_kpis_daily",
    comment="Gold: daily stockout KPIs per date, region, category: store_sku_days, stockout_days, stockout_rate, units_sold, net_sales, est_lost_units and est_lost_sales. Lost demand on a stockout day = the store-SKU trailing 28-day average daily units_sold over non-stockout days (prior 28 days, excluding the current day); est_lost_sales = est_lost_units x unit_price.",
)
def gold_stockout_kpis_daily():
    w = Window.partitionBy("store_id", "sku_id").orderBy("date").rowsBetween(-28, -1)
    daily = (
        spark.read.table(f"{TARGET}.gold_store_sku_daily")
        .withColumn(
            "_baseline_units",
            F.avg(F.when(~F.col("is_stockout"), F.col("units_sold"))).over(w),
        )
        .withColumn(
            "_lost_units",
            F.when(F.col("is_stockout"), F.coalesce(F.col("_baseline_units"), F.lit(0.0))).otherwise(F.lit(0.0)),
        )
        .withColumn("_lost_sales", F.col("_lost_units") * F.col("unit_price"))
    )
    return (
        daily.groupBy("date", "region", "category")
        .agg(
            F.count("*").alias("store_sku_days"),
            F.sum(F.col("is_stockout").cast("int")).alias("stockout_days"),
            F.sum("units_sold").alias("units_sold"),
            F.round(F.sum("net_sales"), 2).alias("net_sales"),
            F.round(F.sum("_lost_units"), 2).alias("est_lost_units"),
            F.round(F.sum("_lost_sales"), 2).alias("est_lost_sales"),
        )
        .withColumn("stockout_rate", F.round(F.col("stockout_days") / F.col("store_sku_days"), 4))
        .select(
            "date", "region", "category", "store_sku_days", "stockout_days", "stockout_rate",
            "units_sold", "net_sales", "est_lost_units", "est_lost_sales",
        )
    )
