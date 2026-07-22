"""
Stage 1c — real kafka-python consumer with the schema gate at the boundary.

Day 2's `consume_and_validate` accepted or rejected each record with Pydantic.
This extends the rejected branch to the routing Day 4 used: every malformed
record is written to the quarantine zone with its rejection reason, and is also
republished to a dead-letter topic so the producer team can replay it.

Accepted records land in a JSONL landing zone; the Bronze job picks them up
from there. Nothing invalid ever reaches the lakehouse.
"""

import json
import os
from datetime import UTC, datetime

import pandas as pd
from pydantic import ValidationError

from src.config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    DLQ_TOPIC,
    LANDING_JSONL,
    QUARANTINE_DIR,
    TOPIC,
)
from src.ingestion.contracts import RetailTransactionContract


def _dlq_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def consume_and_validate(max_messages: int = 100_000, timeout_s: int = 30) -> dict:
    """
    Reads the raw topic, validates every message against the data contract and
    routes it: accepted -> landing zone, rejected -> quarantine zone + DLQ topic.
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_s * 1000,
    )
    dlq = _dlq_producer()

    os.makedirs(os.path.dirname(LANDING_JSONL), exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    accepted, rejected_rows = 0, []
    with open(LANDING_JSONL, "w", encoding="utf-8") as landing:
        for record in consumer:
            try:
                reading = RetailTransactionContract.model_validate(record.value)
                row = reading.model_dump()
                row["kafka_offset"] = record.offset
                row["ingested_at"]  = datetime.now(UTC).isoformat()
                landing.write(json.dumps(row) + "\n")
                accepted += 1
            except ValidationError as e:
                reason = e.errors()[0]["msg"]
                bad = dict(record.value)
                bad["rejection_reason"] = reason
                bad["kafka_offset"]     = record.offset
                bad["quarantined_at"]   = datetime.now(UTC).isoformat()
                rejected_rows.append(bad)
                dlq.send(DLQ_TOPIC, bad)
                if len(rejected_rows) <= 5:
                    print(f"  [CONSUMER] REJECTED @offset {record.offset}: {reason}")
            if accepted + len(rejected_rows) >= max_messages:
                break

    dlq.flush()
    dlq.close()
    consumer.close()

    q_path = None
    if rejected_rows:
        q_path = f"{QUARANTINE_DIR}/contract_violations_{int(datetime.now(UTC).timestamp())}.csv"
        pd.DataFrame(rejected_rows).to_csv(q_path, index=False)

    total = accepted + len(rejected_rows)
    print(f"\n{'=' * 55}")
    print("  CONTRACT VALIDATION AT THE INGESTION BOUNDARY")
    print(f"{'=' * 55}")
    print(f"  Total consumed : {total:>10,}")
    print(f"  Accepted       : {accepted:>10,}  ({100 * accepted / max(total, 1):.1f}%)")
    print(f"  Rejected       : {len(rejected_rows):>10,}  ({100 * len(rejected_rows) / max(total, 1):.1f}%)")
    print(f"{'=' * 55}")

    if rejected_rows:
        reasons = pd.DataFrame(rejected_rows)["rejection_reason"]
        print("\nTop rejection reasons:")
        print(reasons.str.split("(").str[0].value_counts().head(5).to_string())
        print(f"\n  Quarantine file : {q_path}")
        print(f"  Dead-letter topic: {DLQ_TOPIC} ({len(rejected_rows):,} messages republished)")

    print(f"  Landing zone     : {LANDING_JSONL} ({accepted:,} records)")

    return {
        "accepted":        accepted,
        "rejected":        len(rejected_rows),
        "quarantine_path": q_path,
        "landing_path":    LANDING_JSONL,
    }


if __name__ == "__main__":
    consume_and_validate()
