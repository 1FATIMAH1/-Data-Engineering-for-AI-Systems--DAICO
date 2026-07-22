"""
Stage 4 — the quality gate.

Day 4's real Great Expectations 1.x fluent-API checkpoint, pointed at the Silver
table and wired so that a failure actually stops the pipeline: `run_quality_gate`
raises when the checkpoint does not succeed, and the Airflow DAG places Gold
downstream of it, so Gold never runs on data that failed validation.
"""

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd


class QualityGateFailed(Exception):
    """Raised when the Great Expectations checkpoint does not pass."""


def run_great_expectations_checkpoint(df: pd.DataFrame) -> dict:
    """Runs the Silver quality rules as a real GX 1.x checkpoint."""
    context     = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("pandas_capstone")
    data_asset  = data_source.add_dataframe_asset(name="silver_transactions")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_quality_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="line_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="line_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="CustomerID"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Quantity",  min_value=0.0001))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="UnitPrice", min_value=0.0001))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="revenue",   min_value=0.0001))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="InvoiceNo", regex=r"^[A-Z]?\d{5,6}$"))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="silver_quality_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="silver_quality_checkpoint",
            validation_definitions=[validation_definition],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})

    print(f"[GX] Real Great Expectations checkpoint success={result.success}")
    failed = []
    for run_result in result.run_results.values():
        for r in run_result["results"]:
            status = "PASSED" if r["success"] else "FAILED"
            expectation = r["expectation_config"]["type"]
            print(f"  [GX] {status} {expectation}")
            if not r["success"]:
                failed.append(expectation)

    return {"success": bool(result.success), "failed_expectations": failed}


def run_quality_gate(silver_pdf: pd.DataFrame) -> dict:
    """
    The gate itself. Passing returns the report; failing raises, which is what
    halts the DAG before Gold.
    """
    report = run_great_expectations_checkpoint(silver_pdf)
    if not report["success"]:
        raise QualityGateFailed(
            "Silver failed the Great Expectations checkpoint: "
            + ", ".join(report["failed_expectations"])
        )
    print("\nQuality gate PASSED — downstream stages are allowed to run.")
    return report
