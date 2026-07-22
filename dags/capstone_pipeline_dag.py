"""
Stage 5 — orchestration.

One Airflow DAG wiring every stage of the capstone together:

    produce -> consume_validate -> bronze -> silver -> quality_gate -> gold
                                                            \\
                                                             -> rag

`quality_gate` raises `QualityGateFailed` when the Great Expectations checkpoint
does not pass. Both `gold` and `rag` are downstream of it with the default
`all_success` trigger rule, so a failed gate leaves them skipped — the pipeline
halts before anything is published from unvalidated data.

Deploy: copy this repository into $AIRFLOW_HOME (or symlink `dags/` into
$AIRFLOW_HOME/dags) so that `src` is importable, then start the scheduler.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make `src` importable when Airflow loads this file from the dags/ folder.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.tasks import (  # noqa: E402
    task_bronze,
    task_consume_validate,
    task_gold,
    task_produce,
    task_quality_gate,
    task_rag,
    task_silver,
)

default_args = {
    "owner": "sdaia-capstone",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="capstone_data_pipeline",
    description="Kafka -> Delta Bronze/Silver/Gold -> quality gate -> RAG, with OpenLineage events",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # triggered manually for the capstone run
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "kafka", "delta", "rag"],
) as dag:

    produce = PythonOperator(
        task_id="ingestion_produce",
        python_callable=task_produce,
    )

    consume_validate = PythonOperator(
        task_id="ingestion_consume_validate",
        python_callable=task_consume_validate,
    )

    bronze = PythonOperator(
        task_id="lakehouse_bronze",
        python_callable=task_bronze,
    )

    silver = PythonOperator(
        task_id="lakehouse_silver",
        python_callable=task_silver,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=task_quality_gate,
    )

    gold = PythonOperator(
        task_id="lakehouse_gold",
        python_callable=task_gold,
    )

    rag = PythonOperator(
        task_id="rag_pipeline",
        python_callable=task_rag,
    )

    produce >> consume_validate >> bronze >> silver >> quality_gate >> [gold, rag]
