"""
Shared Spark session, identical to the builder used in Day 1's mini-lakehouse
and Day 2's Delta sink.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.config import DATA_DIR


def create_spark_session(app_name: str = "Capstone_Lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", f"{DATA_DIR}/warehouse")
        # Enable auto-optimization (Delta Lake 2.x+)
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled",   "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Registered Bronze schema — everything lands as string exactly as it came off
# the topic, plus the two ingestion audit columns.
BRONZE_SCHEMA = StructType([
    StructField("InvoiceNo",    StringType(), nullable=False),
    StructField("StockCode",    StringType(), nullable=False),
    StructField("Description",  StringType(), nullable=True),
    StructField("Quantity",     DoubleType(), nullable=True),
    StructField("UnitPrice",    DoubleType(), nullable=True),
    StructField("CustomerID",   StringType(), nullable=True),
    StructField("Country",      StringType(), nullable=True),
    StructField("InvoiceDate",  StringType(), nullable=True),
    StructField("kafka_offset", StringType(), nullable=True),
    StructField("ingested_at",  StringType(), nullable=True),
])


def sep(label: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {label}")
    print("=" * 65)
