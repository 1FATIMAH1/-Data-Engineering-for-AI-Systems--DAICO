"""
Central configuration for the capstone pipeline.

Every path and topic name used by the five stages lives here so the Airflow DAG,
the notebook and the local runner all agree on the same layout.
"""

import os

# ─────────────────────────────────────────────────────────────────────────────
# KAFKA (Day 2 — real kafka-python round trip)
# ─────────────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = "retail_transactions_raw"
DLQ_TOPIC         = "retail_transactions_dlq"
CONSUMER_GROUP    = "capstone-ingestion"

# How many rows of the source dataset the producer streams per run.
SAMPLE_ROWS       = int(os.getenv("SAMPLE_ROWS", "5000"))
# Rows re-sent with a corrected UnitPrice, so the Silver MERGE has real matches.
CORRECTION_ROWS   = int(os.getenv("CORRECTION_ROWS", "50"))

# ─────────────────────────────────────────────────────────────────────────────
# STORAGE LAYOUT
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR        = os.getenv("CAPSTONE_DATA_DIR", "./data")

LANDING_JSONL   = f"{DATA_DIR}/landing/accepted_records.jsonl"
BRONZE_PATH     = f"{DATA_DIR}/delta/bronze_transactions"
SILVER_PATH     = f"{DATA_DIR}/delta/silver_transactions"
GOLD_PATH       = f"{DATA_DIR}/delta/gold_revenue_by_country"

QUARANTINE_DIR  = "./quarantine_zone"
LINEAGE_DIR     = "./lineage_events"
LINEAGE_LOG     = f"{LINEAGE_DIR}/openlineage_run.log"

LINEAGE_NAMESPACE = "capstone"
PRODUCER_URI      = "https://github.com/SDAIAAcademy"

# ─────────────────────────────────────────────────────────────────────────────
# RAG (Day 3)
# ─────────────────────────────────────────────────────────────────────────────
EMBED_MODEL     = "all-MiniLM-L6-v2"
RERANK_MODEL    = "cross-encoder/ms-marco-MiniLM-L-6-v2"
COLLECTION_NAME = "capstone_knowledge_base"
GROQ_MODEL      = "Llama-3.1-8B-Instant"
