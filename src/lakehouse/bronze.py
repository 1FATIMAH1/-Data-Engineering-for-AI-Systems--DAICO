"""
Stage 2a — Bronze.

Raw landing zone -> Delta Bronze table, append-only. This is the Lakehouse
pattern from Day 1 Part 1: contract-valid records land untransformed, and every
later correction of business logic is replayable from here.
"""

import json
import os

from pyspark.sql import SparkSession

from src.config import BRONZE_PATH, LANDING_JSONL
from src.lakehouse.spark_session import BRONZE_SCHEMA, sep


def load_bronze(spark: SparkSession) -> int:
    sep("BRONZE — Append landing-zone records to the Delta Bronze table")

    if not os.path.exists(LANDING_JSONL):
        raise FileNotFoundError(
            f"No landing file at {LANDING_JSONL} — run the ingestion stage first."
        )

    rows = []
    with open(LANDING_JSONL, encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            rows.append((
                r["InvoiceNo"],
                r["StockCode"],
                r["Description"],
                float(r["Quantity"]),
                float(r["UnitPrice"]),
                r["CustomerID"],
                r["Country"],
                r["InvoiceDate"],
                str(r["kafka_offset"]),
                r["ingested_at"],
            ))

    if not rows:
        raise ValueError("Landing zone is empty — nothing passed the contract gate.")

    df = spark.createDataFrame(rows, BRONZE_SCHEMA)
    (df.write
       .format("delta")
       .mode("append")
       .partitionBy("Country")   # Physical partitioning — Spark skips entire
       .save(BRONZE_PATH))       # partition directories during filtered queries.

    total = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"Appended {len(rows):,} records. Bronze now holds {total:,} rows.")
    spark.read.format("delta").load(BRONZE_PATH).show(5, truncate=False)
    return len(rows)
