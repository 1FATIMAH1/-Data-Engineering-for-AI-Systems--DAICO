"""
Stage 2c — Gold.

A genuine aggregate, not a copy of Silver: revenue rolled up by country and
invoice month, with order, customer and product counts. This is the table an
analytics consumer reads — the same "top countries by revenue" question Day 4
answered on the clean data, materialised as a Delta table.
"""

from pyspark.sql import SparkSession, functions as F

from src.config import GOLD_PATH, SILVER_PATH
from src.lakehouse.spark_session import sep


def build_gold(spark: SparkSession) -> int:
    sep("GOLD — Revenue aggregate by country and invoice month")

    silver = spark.read.format("delta").load(SILVER_PATH)

    gold = (
        silver
        .withColumn(
            "invoice_month",
            F.date_format(
                F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm"),
                "yyyy-MM",
            ),
        )
        .groupBy("Country", "invoice_month")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.countDistinct("InvoiceNo").alias("invoice_count"),
            F.countDistinct("CustomerID").alias("customer_count"),
            F.countDistinct("StockCode").alias("product_count"),
            F.sum("Quantity").alias("units_sold"),
            F.round(F.avg("revenue"), 2).alias("avg_line_revenue"),
        )
        .orderBy(F.col("total_revenue").desc())
    )

    (gold.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(GOLD_PATH))

    rows = spark.read.format("delta").load(GOLD_PATH).count()
    print(f"Gold table written: {rows:,} aggregate rows "
          f"(Silver held {silver.count():,} transaction lines).")
    spark.read.format("delta").load(GOLD_PATH) \
         .orderBy(F.col("total_revenue").desc()).show(10, truncate=False)
    return rows
