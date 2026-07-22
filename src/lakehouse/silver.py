"""
Stage 2b — Silver.

Bronze -> Silver as a real Delta `MERGE` keyed on the business key
(`line_id = InvoiceNo_StockCode`), the same upsert Day 1 Step 4 demonstrated:
matched keys are UPDATEd with the corrected price, unmatched keys are INSERTed,
all inside one atomic transaction.

`demonstrate_schema_enforcement` is Day 1 Step 3 applied to this table: a write
carrying an undeclared column is refused by Delta rather than silently widening
the table.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.config import BRONZE_PATH, SILVER_PATH
from src.lakehouse.spark_session import sep

SILVER_SCHEMA = StructType([
    StructField("line_id",     StringType(), nullable=False),
    StructField("InvoiceNo",   StringType(), nullable=False),
    StructField("StockCode",   StringType(), nullable=False),
    StructField("Description", StringType(), nullable=True),
    StructField("Quantity",    DoubleType(), nullable=True),
    StructField("UnitPrice",   DoubleType(), nullable=True),
    StructField("revenue",     DoubleType(), nullable=True),
    StructField("CustomerID",  StringType(), nullable=True),
    StructField("Country",     StringType(), nullable=True),
    StructField("InvoiceDate", StringType(), nullable=True),
    StructField("ingested_at", StringType(), nullable=True),
])


def build_silver_source(spark: SparkSession, keep: str = "latest"):
    """
    Cleans and types Bronze, then keeps exactly one row per business key.
    MERGE fails if the same key appears twice on the source side, so this
    de-duplication is mandatory, not cosmetic.

    keep="first"  -> the originally ingested line (the base batch)
    keep="latest" -> the most recent version, i.e. the price correction wins
    """
    bronze = spark.read.format("delta").load(BRONZE_PATH)

    typed = (
        bronze
        .withColumn("line_id", F.concat_ws("_", F.col("InvoiceNo"), F.col("StockCode")))
        .withColumn("Quantity",  F.col("Quantity").cast(DoubleType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("revenue",   F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    )

    offset = F.col("kafka_offset").cast("long")
    if keep == "first":
        ordering = [F.col("ingested_at").asc(), offset.asc()]
    else:
        ordering = [F.col("ingested_at").desc(), offset.desc()]

    window = Window.partitionBy("line_id").orderBy(*ordering)
    return (
        typed
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(*[f.name for f in SILVER_SCHEMA.fields])
    )


def _merge_batch(spark: SparkSession, source, label: str) -> dict:
    """One real Delta MERGE keyed on the business key, with its metrics printed."""
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("target")
        .merge(
            source.alias("updates"),
            "target.line_id = updates.line_id",
        )
        .whenMatchedUpdate(set={
            "Quantity":    "updates.Quantity",
            "UnitPrice":   "updates.UnitPrice",
            "revenue":     "updates.revenue",
            "ingested_at": "updates.ingested_at",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = silver_table.history(1).select("operation", "operationMetrics").collect()[0]
    metrics = history["operationMetrics"] or {}
    updated  = int(metrics.get("numTargetRowsUpdated", 0))
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    print(f"\n  MERGE [{label}] — operation logged as {history['operation']}")
    print(f"    rows updated  : {updated:,}")
    print(f"    rows inserted : {inserted:,}")
    return {"updated": updated, "inserted": inserted}


def merge_silver(spark: SparkSession) -> dict:
    """
    Two real MERGEs, the same CDC scenario Day 1 Step 4 used:

      1. the base batch — the invoice lines as they were first ingested
      2. the correction batch — the same keys carrying corrected prices

    Batch 1 exercises the INSERT branch, batch 2 exercises the UPDATE branch, so
    both paths of the upsert are proven on a single run.
    """
    sep("SILVER — MERGE (UPSERT) on business key line_id")

    base   = build_silver_source(spark, keep="first")
    latest = build_silver_source(spark, keep="latest")

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (base.limit(0).write
             .format("delta")
             .mode("overwrite")
             .save(SILVER_PATH))
        print("Silver table registered (empty) with the enforced Silver schema.")

    before = spark.read.format("delta").load(SILVER_PATH).count()

    m_base       = _merge_batch(spark, base,   "base batch")
    m_correction = _merge_batch(spark, latest, "correction batch")

    after = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"\n  Silver rows: {before:,} -> {after:,}")

    DeltaTable.forPath(spark, SILVER_PATH).toDF().orderBy("line_id").show(5, truncate=False)

    return {
        "rows_before":   before,
        "rows_after":    after,
        "rows_inserted": m_base["inserted"] + m_correction["inserted"],
        "rows_updated":  m_base["updated"] + m_correction["updated"],
        "merge_base":        m_base,
        "merge_corrections": m_correction,
    }


def demonstrate_schema_enforcement(spark: SparkSession) -> bool:
    """
    Day 1 Step 3 against the Silver table: a write with an undeclared column
    must be REJECTED. Returns True when Delta refused the write.
    """
    sep("SILVER — Schema Enforcement (reject a malformed write)")
    print("Attempting to append a row with an undeclared 'discount' column...")

    bad_schema = StructType(
        list(SILVER_SCHEMA.fields)
        + [StructField("discount", DoubleType(), True)]   # Not in the registered schema
    )
    df_bad = spark.createDataFrame(
        [(
            "BADKEY_99999", "999999", "9999", "SCHEMA BREAKER",
            1.0, 0.0, 0.0, "99999", "United Kingdom",
            "1/1/2011 00:00", "1970-01-01T00:00:00+00:00", 100.0,
        )],
        bad_schema,
    )
    try:
        df_bad.write.format("delta").mode("append").save(SILVER_PATH)
    except Exception as e:
        print("Schema violation caught by Delta Lake — write REJECTED.")
        print(f"  Error: {str(e).splitlines()[0]}")
        print("\nWhy this matters: without schema enforcement a single misconfigured")
        print("upstream pipeline can silently corrupt a production lakehouse table.")
        return True

    print("WARNING: the malformed write was accepted — schema enforcement is not active.")
    return False
