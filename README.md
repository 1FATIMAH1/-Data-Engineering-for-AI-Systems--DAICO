# Modern Data Engineering for AI Systems - Capstone

**Student:** Fatimah ALzeer  
**Program:** SDAIA Academy - DAICO  
**Trainer:** Mohammed Albeladi  

---

## Project Overview

This project is my implementation of the **Modern Data Engineering for AI Systems Capstone**.

The goal of this project is to design and implement a complete modern data pipeline that processes real-world data and prepares it for AI applications.

The pipeline covers the complete data engineering lifecycle, starting from real-time data ingestion, data validation, lakehouse processing, quality checks, orchestration, lineage tracking, and finally an AI-powered Retrieval Augmented Generation (RAG) pipeline.

The main challenge was handling real-world data problems such as invalid records, schema issues, data quality failures, and ensuring that only reliable data reaches the final AI layer.

---

# Pipeline Architecture

The project consists of five main stages:

## 1. Data Ingestion

Implemented a real-time ingestion pipeline using **Apache Kafka**.

Features include:

- Kafka Producer and Consumer implementation.
- Data validation using **Pydantic** schemas.
- Rejection of invalid records at the ingestion boundary.
- Storing rejected records in a quarantine zone with rejection reasons.
- Sending failed messages to a Dead Letter Topic (DLQ).

---

## 2. Delta Lakehouse

Implemented a Bronze, Silver, and Gold architecture using **Delta Lake**.

### Bronze Layer
- Stores accepted raw transaction/event records.
- Maintains the original ingested data.

### Silver Layer
- Cleans and transforms the data.
- Applies Delta Lake operations.
- Implements a real **MERGE (upsert)** operation using business keys.
- Tests schema enforcement by rejecting invalid schema changes.

### Gold Layer
- Produces business-level aggregations and analytical outputs.
- Generates metrics suitable for downstream AI and analytics tasks.

---

## 3. RAG Pipeline

Built a complete **Retrieval Augmented Generation (RAG)** pipeline.

The pipeline includes:

- Document chunking.
- Dense retrieval using vector embeddings and ChromaDB.
- Keyword retrieval using BM25.
- Hybrid retrieval using Reciprocal Rank Fusion (RRF).
- Cross-encoder reranking.
- Context-grounded answers with retrieved information.

---

## 4. Orchestration

Created an **Apache Airflow DAG**:

`dags/capstone_pipeline_dag.py`

The workflow connects all pipeline stages:
