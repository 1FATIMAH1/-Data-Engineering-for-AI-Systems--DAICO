# Modern Data Engineering for AI Systems - Capstone

**Student:** Fatimah ALzeer  
**Program:** SDAIA Academy - DAICO  
**Trainer:** Mohammed Albeladi  

## Project Overview

This project is my implementation of the **Modern Data Engineering for AI Systems Capstone**.

The goal of this project is to build a complete data engineering pipeline that processes real-world retail transaction data and prepares it for analytics and AI applications.

The project uses the **UCI Online Retail dataset** and implements a complete workflow starting from real-time ingestion, data validation, lakehouse processing, data quality validation, orchestration, and Retrieval Augmented Generation (RAG).

The main focus of the project is handling real-world data engineering challenges such as invalid records, schema validation, data quality checks, incremental updates, and producing reliable data layers for downstream AI applications.

---

# Pipeline Architecture

The pipeline is divided into five main stages:

## 1. Data Ingestion

A real Kafka-based ingestion pipeline was implemented.

Features:

- Kafka Producer streams retail transaction records.
- Kafka Consumer receives and validates incoming messages.
- Pydantic data contracts validate records before entering the data platform.
- Invalid records are rejected and stored in a quarantine zone with rejection reasons.
- Failed messages are sent to a Dead Letter Queue (DLQ).

The ingestion layer ensures that only valid records continue to the lakehouse.

---

## 2. Delta Lakehouse Architecture

The project implements a Bronze, Silver, and Gold architecture using **Delta Lake**.

### Bronze Layer

- Stores validated raw transaction records.
- Acts as the initial storage layer after ingestion.

### Silver Layer

- Cleans and transforms Bronze data.
- Applies business rules and prepares analytics-ready transactions.
- Implements Delta Lake **MERGE (upsert)** using a business key.
- Demonstrates schema enforcement by rejecting invalid schema changes.

### Gold Layer

Creates business-level aggregations including:

- Revenue analysis by country.
- Revenue analysis by invoice month.
- Invoice counts.
- Customer counts.
- Product counts.
- Units sold.

The Gold layer provides data ready for analytics consumption.

---

## 3. RAG Pipeline

A Retrieval Augmented Generation pipeline was implemented to retrieve relevant information from the project knowledge base.

The pipeline includes:

- Document chunking with overlap.
- Dense retrieval using Sentence Transformers embeddings.
- Vector storage and retrieval using ChromaDB.
- Keyword retrieval using BM25.
- Hybrid retrieval using Reciprocal Rank Fusion (RRF).
- Cross-encoder reranking.
- Context-grounded responses with source citations.

---

## 4. Pipeline Orchestration

An Apache Airflow DAG was created to orchestrate the complete workflow.

The pipeline execution order is:

```
Ingestion
    ↓
Validation
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Quality Gate
    ↓
Gold Layer
    ↓
RAG Pipeline
```

The Airflow dependencies ensure that downstream tasks do not execute if the quality validation stage fails.

---

## 5. Data Quality and Lineage

The project implements data quality monitoring and pipeline observability.

Implemented features:

- Great Expectations quality checks before publishing final data.
- Quality Gate that stops the pipeline when validation fails.
- OpenLineage events tracking pipeline stages:
  - START
  - COMPLETE
  - FAIL

This provides visibility into pipeline execution and data reliability.

---

# Results

The implemented pipeline successfully demonstrates:

- Kafka-based transaction ingestion.
- Data validation using Pydantic contracts.
- Invalid record handling through quarantine and DLQ.
- Delta Lake Bronze, Silver, and Gold processing.
- Successful Silver MERGE operation for updates.
- Schema enforcement preventing invalid writes.
- Creation of business aggregation tables in the Gold layer.
- RAG retrieval using hybrid search and reranking.
- Airflow orchestration with dependency control.
- Quality Gate failure handling with lineage tracking.

---

# Technologies Used

- Python
- Apache Kafka
- PySpark
- Delta Lake
- Apache Airflow
- Great Expectations
- OpenLineage
- ChromaDB
- Sentence Transformers
- BM25
- Pydantic

---

# How to Run

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Start Kafka

Make sure Kafka is running locally before executing the ingestion stage.

## 3. Run the Pipeline

Execute:

```bash
python -m src.main
```

The pipeline runs all stages:

- Kafka ingestion
- Validation
- Bronze loading
- Silver processing
- Quality Gate
- Gold aggregation
- RAG pipeline

---

# Repository Structure

```
├── src/
│   ├── ingestion/
│   │   ├── producer.py
│   │   ├── consumer.py
│   │   └── contracts.py
│   │
│   ├── lakehouse/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   ├── gold.py
│   │   └── spark_session.py
│   │
│   ├── rag/
│   │   ├── pipeline.py
│   │   └── knowledge_base.py
│   │
│   ├── quality/
│   │   └── expectations.py
│   │
│   ├── lineage/
│   │   └── emitter.py
│   │
│   ├── tasks.py
│   └── main.py
│
├── dags/
│   └── capstone_pipeline_dag.py
│
├── docs/
│   └── ARCHITECTURE.md
│
├── requirements.txt
└── README.md
```

---

# Training Attribution

This project was completed as part of:

**SDAIA Academy - Modern Data Engineering for AI Systems (DAICO) Capstone**

GitHub: https://github.com/SDAIAAcademy
