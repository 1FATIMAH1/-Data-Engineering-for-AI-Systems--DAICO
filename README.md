# Data Engineering for AI Systems — Capstone

**Student:** Fatimah ALzeer

**Program:** SDAIA Academy — Modern Data Engineering for AI Systems (DAICO)

**session dates:** 19 July 2026 – 23 July 2026

**Trainer:** Mohammed Albeladi

---

## Project Overview

An end-to-end data engineering pipeline that takes raw retail transactions from a
streaming source all the way to validated analytics tables and a grounded RAG
question-answering layer.

The source is the UCI Online Retail dataset — 541,909 real invoice lines from a UK
online shop, 2010–2011. The dataset is deliberately dirty: roughly a quarter of the
rows carry no customer ID, cancellations arrive as negative quantities, and some
lines are priced at zero. Loaded naively, those rows silently corrupt every revenue
figure downstream and nobody finds out.

This project solves that by putting a machine-enforceable contract at the ingestion
boundary and a quality gate in front of the analytics layer, so bad data is stopped,
recorded with a reason, and replayable — never silently absorbed.

**Scope:** streaming ingestion with schema validation, a three-layer Delta lakehouse
with incremental upserts, a hybrid-search RAG pipeline, Airflow orchestration, and
per-stage quality gating and lineage.

---

## Pipeline Architecture

```
Kafka producer
      │
      ▼
Kafka consumer + Pydantic contract ──► quarantine zone + DLQ topic
      │ (valid records only)
      ▼
Bronze (Delta, append-only)
      │
      ▼
Silver (Delta MERGE upsert on business key)
      │
      ▼
Quality Gate (Great Expectations) ──► raises on failure, halting the pipeline
      │
      ├──────────────► Gold (Delta aggregate)
      └──────────────► RAG pipeline
```

Every stage emits OpenLineage `START` / `COMPLETE` / `FAIL` events.

### 1. Data Ingestion

A real Kafka ingestion path built on `kafka-python`.

- **Producer** streams invoice lines into the `retail_transactions_raw` topic as JSON,
  reading the source as raw strings so nothing is coerced before validation.
- It also publishes a **correction batch** — contract-valid lines re-sent with a 10%
  higher `UnitPrice`. This is the CDC scenario that gives the Silver `MERGE` genuine
  matched keys to update rather than a table of pure inserts.
- **Consumer** validates every message against the `RetailTransactionContract`
  Pydantic model: non-null `CustomerID`, `Quantity > 0`, `UnitPrice > 0`, a well-formed
  `InvoiceNo`, and non-null `Description` and `InvoiceDate`.
- **Accepted** records go to a JSONL landing zone enriched with `kafka_offset` and
  `ingested_at`.
- **Rejected** records go to `quarantine_zone/` as CSV carrying the exact
  `rejection_reason`, **and** are republished to the `retail_transactions_dlq`
  dead-letter topic so the producing team can fix and replay them.

Nothing that fails the contract ever reaches Bronze.

### 2. Delta Lakehouse

Bronze / Silver / Gold on `pyspark` + `delta-spark`.

**Bronze** — append-only. Records land exactly as they arrived, partitioned by
`Country`. Bronze is never edited: when a business rule changes, Silver is rebuilt
from Bronze rather than re-ingested from source.

**Silver** — a real Delta `MERGE` keyed on the business key
`line_id = InvoiceNo + "_" + StockCode`. Matched keys are updated with the corrected
price, unmatched keys are inserted, in one atomic transaction. The source is
de-duplicated to one row per key first, since `MERGE` fails on duplicate source keys.
Schema enforcement is proven explicitly: a write carrying an undeclared `discount`
column is refused by Delta rather than silently widening the table.

**Gold** — a genuine aggregate, not a filtered copy of Silver. Grouped by
`Country × invoice_month`, producing `total_revenue`, `invoice_count`,
`customer_count`, `product_count`, `units_sold`, and `avg_line_revenue`.

### 3. RAG Pipeline

| Step | Implementation |
| --- | --- |
| Chunking | Sentence-level, 2 sentences per chunk, 1 sentence overlap |
| Embeddings | `all-MiniLM-L6-v2` bi-encoder (Sentence Transformers) |
| Vector store | ChromaDB, HNSW index |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion, `k = 60`, parameter-free |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Generation | Groq when `GROQ_API_KEY` is set, otherwise the cited context itself |
| Evaluation | Cosine-based context precision and average similarity |

Dense retrieval finds paraphrases; BM25 finds exact terms such as `line_id` or
`retail_transactions_dlq`. RRF merges the two ranked lists without weights to tune,
then the cross-encoder scores each (query, chunk) pair jointly for a precise top-3.

**Citations.** Context blocks are numbered `[Source 1] … [Source N]`, the prompt
requires every factual sentence to carry a citation, and each run prints the map from
every source number back to its `chunk_id` and parent `doc_id`, so any claim is
traceable to the exact chunk it came from.

### 4. Pipeline Orchestration

An Apache Airflow DAG (`capstone_data_pipeline`, 7 tasks) wires every stage together:

```
ingestion_produce
      └─> ingestion_consume_validate
                └─> lakehouse_bronze
                          └─> lakehouse_silver
                                    └─> quality_gate
                                              ├─> lakehouse_gold
                                              └─> rag_pipeline
```

`lakehouse_gold` and `rag_pipeline` sit downstream of `quality_gate` with the default
`all_success` trigger rule, so a failed gate leaves both **skipped** — the pipeline
halts before anything is published from unvalidated data.

### 5. Data Quality and Lineage

**Quality gate.** A Great Expectations 1.x checkpoint on Silver validating: unique
and non-null `line_id`, non-null `CustomerID`, positive `Quantity`, `UnitPrice` and
`revenue`, and a well-formed `InvoiceNo`. `run_quality_gate` raises
`QualityGateFailed` when the checkpoint does not succeed. The gate is not advisory —
it is load-bearing.

**Lineage.** Every task wraps its work in the `stage_lineage` context manager, which
emits a real `openlineage-python` `RunEvent`: `START` on entry, `COMPLETE` on clean
exit, `FAIL` if the stage raises — then re-raises so the orchestrator still sees the
failure. Events are written to `lineage_events/` via the file transport; swapping
`FileTransport` for the HTTP transport ships identical events to a Marquez server.

---

## Executed Evidence

`Data_Engineering_for_AI_Systems_2.ipynb` contains a **full end-to-end run with all
output captured** — open it directly on GitHub, no re-run needed. It includes:

- a local Kafka (KRaft) broker started and confirmed listening on `localhost:9092`
- producer and consumer output with real contract rejections
- a preview of the quarantine CSV with rejection reasons
- Bronze append, Silver `MERGE` metrics, and the Delta transaction history
- the schema-enforcement rejection with Delta's own error message
- the Gold aggregate table
- the full RAG run: vector + BM25 candidates, RRF fusion, reranked top-3, cited answers
- the parsed Airflow DAG with its topological task order
- the Great Expectations checkpoint results and the OpenLineage event log
- a **forced quality-gate failure**, showing the pipeline halt and the `FAIL` lineage event

Design rationale and component-level detail: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## Technologies Used

Python · Apache Kafka (`kafka-python`) · PySpark · Delta Lake (`delta-spark`) ·
Apache Airflow · Great Expectations · OpenLineage · ChromaDB · Sentence Transformers ·
BM25 (`rank-bm25`) · Pydantic v2

---

## How to Run

### Prerequisites

- **Python 3.10+**
- **JDK 17** — required by both Spark and the Kafka broker
- **A running Kafka broker** on `localhost:9092`
- **Kaggle credentials** — `KAGGLE_USERNAME` and `KAGGLE_KEY`, used by `kagglehub`
  to download the source dataset
- **`GROQ_API_KEY`** *(optional)* — enables LLM answer generation in the RAG stage.
  Without it the stage still runs and returns the retrieved context with its citations.

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

Start a broker on `localhost:9092` before running the ingestion stage. The notebook
downloads and starts a local KRaft broker automatically; see its "Start a local Kafka
broker" step for the exact commands.

### 3. Configure credentials

```bash
export KAGGLE_USERNAME=<your-username>
export KAGGLE_KEY=<your-key>
export GROQ_API_KEY=<your-key>        # optional
```

In Colab, add the same names under **Secrets** (the key icon in the sidebar).

### 4. Run the pipeline

```bash
python -m src.main
```

To capture the run as evidence:

```bash
python -m src.main 2>&1 | tee docs/sample_run.log
```

Or run the notebook end to end, which executes the same task functions in the same
order.

### Running under Airflow

Symlink `dags/` into `$AIRFLOW_HOME/dags` (or copy the repository into
`$AIRFLOW_HOME`) so that `src` is importable, start the scheduler, and trigger
`capstone_data_pipeline` manually — it is not scheduled.

### Expected output

A successful run produces, in order:

| Stage | What you should see |
| --- | --- |
| Ingestion | ~5,050 messages produced; ~3,775 accepted (74.8%), ~1,275 rejected (25.2%) with a breakdown of rejection reasons; a quarantine CSV path and a DLQ republish count |
| Bronze | 3,775 records appended to the Delta Bronze table |
| Silver | Two `MERGE` operations logged, with `numTargetRowsUpdated` / `numTargetRowsInserted` printed; ~3,581 rows after de-duplication; a schema-enforcement rejection message from Delta |
| Quality gate | 7 Great Expectations checks, all `PASSED`, and `success=True` |
| Gold | An aggregate table of a few rows — United Kingdom leading on `total_revenue` |
| RAG | 12 documents → 44 chunks; per query: vector + BM25 candidates, RRF fusion, cross-encoder reranking, a cited answer, and retrieval metrics |
| Lineage | A `START` and a matching `COMPLETE` event per stage under namespace `capstone` |

If the quality gate fails, `main.py` prints `PIPELINE HALTED at the quality gate`,
emits a `FAIL` lineage event, and exits with status `1` — Gold and RAG never run.

---

## Repository Structure

```
├── src/
│   ├── ingestion/
│   │   ├── contracts.py            # Pydantic data contract + business key
│   │   ├── producer.py             # Kafka producer
│   │   └── consumer.py             # Kafka consumer, quarantine + DLQ routing
│   │
│   ├── lakehouse/
│   │   ├── spark_session.py        # Shared Spark + Delta session, Bronze schema
│   │   ├── bronze.py               # Landing zone -> Delta Bronze
│   │   ├── silver.py               # MERGE upsert + schema-enforcement proof
│   │   └── gold.py                 # Revenue aggregate
│   │
│   ├── rag/
│   │   ├── knowledge_base.py       # Corpus
│   │   └── pipeline.py             # Chunking, Chroma, BM25, RRF, reranking, citations
│   │
│   ├── quality/
│   │   └── expectations.py         # Great Expectations checkpoint + gate
│   │
│   ├── lineage/
│   │   └── emitter.py              # OpenLineage START / COMPLETE / FAIL
│   │
│   ├── config.py                   # Paths, topics, model names
│   ├── tasks.py                    # The five stages as callable tasks
│   ├── main.py                     # Local end-to-end runner
│   └── __init__.py
│
├── dags/
│   └── capstone_pipeline_dag.py    # Airflow DAG
│
├── docs/
│   └── ARCHITECTURE.md             # Design rationale and component detail
│
├── Data_Engineering_for_AI_Systems_2.ipynb   # Full executed run with captured output
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Training Attribution

Completed as part of ** Data Engineering for AI Systems** —  **SDAIA Academy (DAICO)**

**Cohort / session dates:** 19 July 2026 – 23 July 2026
**Trainer:** Mohammed Albeladi

SDAIA Academy on GitHub: https://github.com/SDAIAAcademy
