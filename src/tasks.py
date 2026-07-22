"""
The five stages as callable tasks.

Both entry points use these: the Airflow DAG (`dags/capstone_pipeline_dag.py`)
calls them from PythonOperators, and `src/main.py` calls them in sequence for a
local run. Each one is wrapped in `stage_lineage`, so every stage emits a real
OpenLineage START and then COMPLETE or FAIL.
"""

from src.config import SILVER_PATH
from src.ingestion.consumer import consume_and_validate
from src.ingestion.producer import produce_transactions
from src.lakehouse.bronze import load_bronze
from src.lakehouse.gold import build_gold
from src.lakehouse.silver import demonstrate_schema_enforcement, merge_silver
from src.lakehouse.spark_session import create_spark_session
from src.lineage.emitter import stage_lineage
from src.quality.expectations import run_quality_gate
from src.rag.pipeline import run_rag


def task_produce(**_):
    """Stage 1a — stream the source dataset into Kafka."""
    with stage_lineage("ingestion.produce"):
        return produce_transactions()


def task_consume_validate(**_):
    """Stage 1b — consume, validate against the contract, quarantine rejects."""
    with stage_lineage("ingestion.consume_validate"):
        report = consume_and_validate()
        if report["accepted"] == 0:
            raise ValueError("No records passed the contract gate — nothing to load.")
        return report


def task_bronze(**_):
    """Stage 2a — landing zone -> Delta Bronze."""
    with stage_lineage("lakehouse.bronze"):
        spark = create_spark_session("Capstone_Bronze")
        try:
            return load_bronze(spark)
        finally:
            spark.stop()


def task_silver(**_):
    """Stage 2b — Bronze -> Silver MERGE upsert, plus the schema-enforcement proof."""
    with stage_lineage("lakehouse.silver"):
        spark = create_spark_session("Capstone_Silver")
        try:
            metrics = merge_silver(spark)
            rejected = demonstrate_schema_enforcement(spark)
            metrics["schema_enforcement_rejected_bad_write"] = rejected
            return metrics
        finally:
            spark.stop()


def task_quality_gate(**_):
    """
    Stage 4 — Great Expectations checkpoint on Silver.

    Raises on failure. Gold sits downstream of this task in the DAG, so a failed
    gate halts the pipeline before any aggregate is published.
    """
    with stage_lineage("quality.gate"):
        spark = create_spark_session("Capstone_QualityGate")
        try:
            silver_pdf = spark.read.format("delta").load(SILVER_PATH).toPandas()
            print(f"Validating {len(silver_pdf):,} Silver rows...")
            return run_quality_gate(silver_pdf)
        finally:
            spark.stop()


def task_gold(**_):
    """Stage 2c — Silver -> Gold aggregate."""
    with stage_lineage("lakehouse.gold"):
        spark = create_spark_session("Capstone_Gold")
        try:
            return build_gold(spark)
        finally:
            spark.stop()


def task_rag(**_):
    """Stage 3 — hybrid-search RAG with reranking and cited answers."""
    with stage_lineage("rag.pipeline"):
        results = run_rag()
        return len(results)
