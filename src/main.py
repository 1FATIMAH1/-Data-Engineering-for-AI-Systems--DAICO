"""
Local end-to-end runner.

Runs the same task functions the Airflow DAG runs, in the same order, in one
process — including the quality-gate dependency: if the gate raises, Gold and
RAG are never reached, exactly as the DAG would skip them.

    python -m src.main            # full run
    python -m src.main 2>&1 | tee run.log   # capture the evidence log
"""

import sys

from src.quality.expectations import QualityGateFailed
from src.tasks import (
    task_bronze,
    task_consume_validate,
    task_gold,
    task_produce,
    task_quality_gate,
    task_rag,
    task_silver,
)


def main() -> int:
    print("=" * 65)
    print("  CAPSTONE — Modern Data Engineering for AI Systems")
    print("=" * 65)

    task_produce()
    task_consume_validate()
    task_bronze()
    task_silver()

    try:
        task_quality_gate()
    except QualityGateFailed as exc:
        print(f"\n❌ PIPELINE HALTED at the quality gate: {exc}")
        print("   Gold and RAG were not run — this is the gate doing its job.")
        return 1

    task_gold()
    task_rag()

    print("\n" + "=" * 65)
    print("  Pipeline complete — all five deliverables executed.")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    sys.exit(main())
