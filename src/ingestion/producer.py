"""
Stage 1b — real kafka-python producer.

Streams rows of the UCI Online Retail dataset (the Day 4 source) into the
`retail_transactions_raw` topic exactly the way Day 2's `produce_sample_messages`
did: a real `KafkaProducer`, JSON value serializer, flush + close.

No anomalies are planted. The dataset already contains null CustomerIDs,
cancellations with negative Quantity and zero-price lines — those are the
records the consumer's contract gate rejects, which is what proves the
failure path.

A small correction batch is appended: the same invoice lines re-sent with a
different UnitPrice, so the Silver MERGE in Stage 2 has genuine matched rows to
update instead of only inserting.
"""

import glob
import json

import pandas as pd

from src.config import (
    BOOTSTRAP_SERVERS,
    CORRECTION_ROWS,
    SAMPLE_ROWS,
    TOPIC,
)


def download_source_csv() -> str:
    """Downloads the UCI Online Retail dataset from Kaggle (Day 4 Step 3)."""
    import kagglehub

    path = kagglehub.dataset_download("carrie1/ecommerce-data")
    print(f"Downloaded to: {path}")
    csv_files = glob.glob(f"{path}/**/*.csv", recursive=True)
    print(f"CSV files found: {csv_files}")
    raw_csv = csv_files[0]
    print(f"Using: {raw_csv}")
    return raw_csv


def load_sample(raw_csv: str, n_rows: int = SAMPLE_ROWS) -> pd.DataFrame:
    """Reads the raw CSV as strings — no coercion before the contract runs."""
    df_raw = pd.read_csv(raw_csv, encoding="ISO-8859-1", dtype=str)
    print(f"Shape: {df_raw.shape[0]:,} rows × {df_raw.shape[1]} columns")
    sample = df_raw.head(n_rows)
    print(f"Streaming the first {len(sample):,} rows of the feed.")
    return sample


def to_message(row: pd.Series) -> dict:
    """Maps one CSV row to the topic's message shape."""
    return {
        "InvoiceNo":   str(row.get("InvoiceNo",   "") or ""),
        "StockCode":   str(row.get("StockCode",   "") or ""),
        "Description": str(row.get("Description", "") or ""),
        "Quantity":    str(row.get("Quantity",    "") or ""),
        "UnitPrice":   str(row.get("UnitPrice",   "") or ""),
        "CustomerID":  str(row.get("CustomerID",  "") or ""),
        "Country":     str(row.get("Country",     "") or ""),
        "InvoiceDate": str(row.get("InvoiceDate", "") or ""),
    }


def build_correction_batch(sample: pd.DataFrame, n_rows: int = CORRECTION_ROWS) -> list[dict]:
    """
    A CDC-style price-correction feed, the same scenario Day 1's MERGE step used:
    a nightly payload that carries both corrections to existing keys and new rows.

    Only contract-valid rows are re-sent, so every correction is guaranteed to
    match an existing business key in Bronze.
    """
    valid = sample[
        sample["CustomerID"].notna()
        & pd.to_numeric(sample["Quantity"], errors="coerce").gt(0)
        & pd.to_numeric(sample["UnitPrice"], errors="coerce").gt(0)
    ].head(n_rows)

    corrections = []
    for _, row in valid.iterrows():
        msg = to_message(row)
        msg["UnitPrice"] = f"{float(msg['UnitPrice']) * 1.10:.2f}"  # price correction
        corrections.append(msg)
    print(f"Prepared {len(corrections)} price corrections for existing business keys.")
    return corrections


def produce_transactions(raw_csv: str | None = None) -> int:
    """Publishes the sample plus the correction batch to the raw topic."""
    from kafka import KafkaProducer

    raw_csv = raw_csv or download_source_csv()
    sample = load_sample(raw_csv)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    for _, row in sample.iterrows():
        producer.send(TOPIC, to_message(row))
        sent += 1

    for msg in build_correction_batch(sample):
        producer.send(TOPIC, msg)
        sent += 1

    producer.flush()
    producer.close()
    print(f"  [PRODUCER] sent {sent:,} messages -> topic '{TOPIC}'")
    return sent


if __name__ == "__main__":
    produce_transactions()
