"""
Stage 5 — lineage.

Day 4's real `openlineage-python` emitter, generalised so every stage of the
capstone emits its own START and then COMPLETE or FAIL. Events go to the local
file transport, so no Marquez server is needed to produce the evidence; point
`OPENLINEAGE_URL` at a Marquez instance to ship the same events over HTTP.

`stage_lineage` is a context manager so the FAIL event is guaranteed even when
the stage raises — which is exactly what happens when the quality gate trips.
"""

import os
from contextlib import contextmanager
from datetime import UTC, datetime

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid

from src.config import LINEAGE_DIR, LINEAGE_LOG, LINEAGE_NAMESPACE, PRODUCER_URI


def _client() -> OpenLineageClient:
    os.makedirs(LINEAGE_DIR, exist_ok=True)
    transport = FileTransport(FileConfig(log_file_path=LINEAGE_LOG))
    return OpenLineageClient(transport=transport)


def emit_event(job_name: str, run_id: str, state: RunState) -> None:
    client = _client()
    client.emit(RunEvent(
        eventType=state,
        eventTime=datetime.now(UTC).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=LINEAGE_NAMESPACE, name=job_name),
        producer=PRODUCER_URI,
    ))
    print(f"[LINEAGE] {state.name:8s} | {LINEAGE_NAMESPACE}.{job_name} | run {run_id}")


@contextmanager
def stage_lineage(job_name: str, run_id: str | None = None):
    """
    Emits START on entry, COMPLETE on clean exit, FAIL if the stage raises —
    then re-raises so the orchestrator still sees the failure.
    """
    run_id = run_id or str(generate_new_uuid())
    emit_event(job_name, run_id, RunState.START)
    try:
        yield run_id
    except Exception:
        emit_event(job_name, run_id, RunState.FAIL)
        raise
    else:
        emit_event(job_name, run_id, RunState.COMPLETE)
