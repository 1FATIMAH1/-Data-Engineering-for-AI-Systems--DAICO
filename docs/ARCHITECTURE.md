# Architecture

How the five stages fit together, what each component guarantees, and why the design choices
were made.

---

## Data flow

| # | Stage | Input | Output | Module |
| --- | --- | --- | --- | --- |
| 1 | Ingestion | Online Retail CSV | Kafka topic → landing JSONL + quarantine + DLQ | `src/ingestion/` |
| 2 | Bronze | landing JSONL | `data/delta/bronze_transactions` | `src/lakehouse/bronze.py` |
| 3 | Silver | Bronze | `data/delta/silver_transactions` | `src/lakehouse/silver.py` |
| 4 | Quality gate | Silver | pass ⇒ continue, fail ⇒ raise | `src/quality/expectations.py` |
| 5 | Gold | Silver | `data/delta/gold_revenue_by_country` | `src/lakehouse/gold.py` |
| 6 | RAG | knowledge base | cited answers | `src/rag/pipeline.py` |

Orchestration: `dags/capstone_pipeline_dag.py`. Lineage: `src/lineage/emitter.py`, called by
every task in `src/tasks.py`.

---

## 1. Ingestion — the contract boundary

**Producer** (`producer.py`) downloads the dataset with `kagglehub`, reads it as raw strings
so nothing is coerced before validation, and publishes each row as JSON to
`retail_transactions_raw` with a real `KafkaProducer`.

It then publishes a **correction batch**: `CORRECTION_ROWS` contract-valid lines re-sent with
a 10% higher `UnitPrice`. This is the CDC scenario — a nightly payload carrying both price
corrections and new rows — and it is what gives the Silver `MERGE` genuine matched keys to
update rather than a table of pure inserts.

**Consumer** (`consumer.py`) reads the topic with a real `KafkaConsumer` and validates every
message against `RetailTransactionContract`:

| Rule | Rejects |
| --- | --- |
| `CustomerID` non-empty | ~25% of the feed — anonymous rows |
| `Quantity > 0` | cancellations (negative quantity) |
| `UnitPrice > 0` | no-charge and zero-priced lines |
| `InvoiceNo` matches `^[A-Z]?\d{5,6}$` | malformed invoice references |
| `InvoiceDate`, `Description` non-empty | rows that cannot be attributed or described |

Routing:

- **Accepted** → one JSON object per line in `data/landing/accepted_records.jsonl`, enriched
  with `kafka_offset` and `ingested_at`.
- **Rejected** → a CSV in `quarantine_zone/` carrying `rejection_reason`, `kafka_offset` and
  `quarantined_at`, **and** republished to the `retail_transactions_dlq` topic so the
  producing team can fix and replay them.

Nothing that fails the contract reaches Bronze. That is the whole point of putting the gate
at the boundary rather than downstream.

### Business key

`line_id = InvoiceNo + "_" + StockCode` (`contracts.business_key`). One invoice line is
uniquely identified by the invoice it belongs to and the product on it, and folding the pair
into one column keeps the Delta `MERGE` condition a single-column match.

---

## 2. Lakehouse

### Bronze — append-only

Records land exactly as they arrived, plus `kafka_offset` and `ingested_at`. Partitioned by
`Country` so filtered reads skip whole directories. Bronze is never edited: when a business
rule changes, Silver is rebuilt from Bronze rather than re-ingested from source.

### Silver — MERGE upsert

`build_silver_source` types `Quantity` and `UnitPrice`, derives `revenue`, builds `line_id`,
then keeps one row per `line_id` — the most recently ingested one — using a window function.
De-duplicating the source is required: `MERGE` fails if the same key appears twice on the
source side, and it is what makes a correction win over the original line.

The merge itself:

```python
silver_table.alias("target").merge(
    source.alias("updates"),
    "target.line_id = updates.line_id",
).whenMatchedUpdate(set={
    "Quantity": "updates.Quantity",
    "UnitPrice": "updates.UnitPrice",
    "revenue": "updates.revenue",
    "ingested_at": "updates.ingested_at",
}).whenNotMatchedInsertAll().execute()
```

Matched keys are updated, unmatched keys inserted, in one atomic transaction. The run prints
`numTargetRowsUpdated` and `numTargetRowsInserted` from the Delta history, which is the
evidence that the update path actually fired.

**Schema enforcement.** `demonstrate_schema_enforcement` appends a row with an undeclared
`discount` column. Delta rejects the write and the first line of the exception is printed.
Without that guarantee, one misconfigured upstream job can silently widen a production table.

### Gold — a real aggregate

Grouped by `Country` × `invoice_month`, producing `total_revenue`, `invoice_count`,
`customer_count`, `product_count`, `units_sold`, `avg_line_revenue`. It is a rollup, not a
filtered copy of Silver: thousands of transaction lines collapse into a few dozen rows.

---

## 3. RAG

| Step | Implementation |
| --- | --- |
| Chunking | Sentence-level, 2 sentences per chunk, 1 sentence overlap |
| Embeddings | `all-MiniLM-L6-v2` bi-encoder |
| Vector store | ChromaDB, HNSW index |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion, `k = 60`, parameter-free |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Generation | Groq when `GROQ_API_KEY` is set, otherwise the cited context itself |
| Evaluation | Cosine-based context precision and average similarity |

Dense retrieval finds paraphrases; BM25 finds exact terms such as `line_id`,
`retail_transactions_dlq` or `MERGE`. RRF merges the two ranked lists without weights to
tune, then the cross-encoder scores each (query, chunk) pair jointly for a precise top-3.

**Citations.** Context blocks are numbered `[Source 1] … [Source N]`; the prompt requires
every factual sentence to carry a citation, and the run prints the map from each source
number back to its `chunk_id` and parent `doc_id`, so any claim can be traced to the exact
chunk it came from.

The corpus (`knowledge_base.py`) covers the platform concepts and this pipeline's own
components, so the RAG stage answers questions about the system it is part of.

---

## 4. Quality gate

A Great Expectations 1.x checkpoint built with the fluent API — ephemeral context, pandas
data source, dataframe asset, suite, validation definition, checkpoint. The suite validates
Silver for: unique and non-null `line_id`, non-null `CustomerID`, positive `Quantity`,
`UnitPrice` and `revenue`, and a well-formed `InvoiceNo`.

`run_quality_gate` raises `QualityGateFailed` when the checkpoint does not succeed. Since
`lakehouse_gold` and `rag_pipeline` sit downstream of `quality_gate` in the DAG with the
default `all_success` trigger rule, a failed gate leaves both skipped. The gate is not
advisory — it is load-bearing.

---

## 5. Orchestration and lineage

```
ingestion_produce
      └─> ingestion_consume_validate
                └─> lakehouse_bronze
                          └─> lakehouse_silver
                                    └─> quality_gate
                                              ├─> lakehouse_gold
                                              └─> rag_pipeline
```

Each Airflow task calls a function in `src/tasks.py`, and each of those wraps its work in the
`stage_lineage` context manager:

- **START** on entry
- **COMPLETE** on clean exit
- **FAIL** if the stage raises — then the exception is re-raised so Airflow still fails the
  task

Events are real `openlineage-python` `RunEvent`s under namespace `capstone`, written to
`lineage_events/openlineage_run.log` via the file transport. Swapping `FileTransport` for the
HTTP transport in `emitter.py` ships the identical events to a Marquez server.

Each task creates and stops its own `SparkSession`, so tasks stay independent and a retry of
one stage does not depend on another's session.

---

## Design notes

**Why a landing JSONL between Kafka and Bronze?** It keeps the consumer free of Spark, so the
ingestion task starts fast and the Bronze load is separately retryable. It is also the raw
landing zone of the lakehouse pattern: the untransformed record of what was accepted.

**Why partition Bronze by `Country`?** It is the dimension every downstream read filters on
and it has healthy cardinality — roughly 38 values across the dataset.

**Why does Gold overwrite instead of merge?** It is a full recomputation from Silver, which is
the source of truth. Aggregates are cheap to rebuild and rebuilding removes any chance of
drift between the layers.
