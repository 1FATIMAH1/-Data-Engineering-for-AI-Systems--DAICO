"""
Knowledge base for the RAG stage.

doc_001 - doc_008 are the Day 3 lab corpus, unchanged. doc_009 - doc_012
describe this capstone's own pipeline in the same format, so the RAG stage
answers questions about the system it is part of rather than sitting beside it.
"""

DOCUMENTS = [
    {
        "id":   "doc_001",
        "text": (
            "Apache Kafka is a distributed event streaming platform. "
            "It uses partitions and offsets to guarantee message ordering "
            "within a partition. Consumer groups allow multiple consumers "
            "to read the same topic in parallel — each partition goes to "
            "exactly one group member. Adding consumers beyond the partition "
            "count yields no additional throughput."
        ),
    },
    {
        "id":   "doc_002",
        "text": (
            "Delta Lake is an open-source storage layer that brings ACID "
            "transactions to Apache Spark. The transaction log (.delta_log) "
            "records every commit as a JSON file. Readers replay the log from "
            "version 0 to reconstruct the current table state. VACUUM removes "
            "old Parquet files once they fall outside the retention window."
        ),
    },
    {
        "id":   "doc_003",
        "text": (
            "Retrieval-Augmented Generation (RAG) combines information retrieval "
            "with large language model generation. The retriever finds relevant "
            "chunks from a vector database using semantic similarity. The LLM "
            "then generates an answer grounded in those retrieved chunks, "
            "significantly reducing hallucination compared to pure generation."
        ),
    },
    {
        "id":   "doc_004",
        "text": (
            "HNSW (Hierarchical Navigable Small World) is the graph-based "
            "approximate nearest-neighbour index used inside ChromaDB, Pinecone, "
            "and Weaviate. The M parameter controls bi-directional links per node. "
            "ef_construction sets the candidate list size at index build time. "
            "ef (search) controls recall vs latency at query time — higher ef "
            "means better recall but slower queries."
        ),
    },
    {
        "id":   "doc_005",
        "text": (
            "Hybrid search combines vector semantic search with BM25 keyword "
            "search. Reciprocal Rank Fusion (RRF) merges both result lists: "
            "score = Σ 1/(k + rank_i), where k=60 is the standard constant. "
            "RRF is parameter-free and consistently outperforms a weighted "
            "linear combination. Most vector databases expose hybrid search natively."
        ),
    },
    {
        "id":   "doc_006",
        "text": (
            "Cross-encoder reranking is a two-stage retrieval pattern. Stage 1: "
            "the bi-encoder retrieves the top-50 candidates quickly using "
            "independent query and document embeddings. Stage 2: the cross-encoder "
            "scores each (query, document) pair jointly — far more accurate because "
            "it sees the full interaction. The top-3 to top-5 reranked results "
            "are passed to the LLM as context."
        ),
    },
    {
        "id":   "doc_007",
        "text": (
            "RAG evaluation requires four metrics. Context Precision: are the "
            "retrieved chunks actually relevant? Context Recall: were all needed "
            "chunks retrieved? Answer Faithfulness: does the answer stay within "
            "the retrieved context (no hallucination)? Answer Relevance: does "
            "the answer address the user question? RAGAS is the standard "
            "open-source framework that computes all four automatically."
        ),
    },
    {
        "id":   "doc_008",
        "text": (
            "Data contracts define the schema and SLA between a data producer "
            "and its consumers. Pydantic v2 enforces contracts in Python using "
            "strict type checking (ConfigDict(strict=True)). dbt supports "
            "enforced: true in model YAML to fail the build if a column type "
            "or name drifts from the declared contract specification."
        ),
    },
    {
        "id":   "doc_009",
        "text": (
            "The capstone ingestion stage runs a kafka-python producer that "
            "streams Online Retail invoice lines into the retail_transactions_raw "
            "topic. The consumer validates every message against the "
            "RetailTransactionContract Pydantic model at the ingestion boundary. "
            "Records that fail the contract are written to the quarantine zone "
            "with their rejection reason and republished to the "
            "retail_transactions_dlq dead-letter topic."
        ),
    },
    {
        "id":   "doc_010",
        "text": (
            "The capstone lakehouse has three Delta layers. Bronze appends the "
            "contract-valid records exactly as they arrived from Kafka. Silver "
            "runs a MERGE upsert keyed on line_id, the business key formed from "
            "InvoiceNo and StockCode, so price corrections update existing rows "
            "and new invoice lines are inserted in the same atomic transaction. "
            "Gold aggregates revenue by country and invoice month."
        ),
    },
    {
        "id":   "doc_011",
        "text": (
            "The capstone quality gate is a Great Expectations checkpoint that "
            "validates the Silver table for unique line_id, non-null CustomerID, "
            "positive Quantity, UnitPrice and revenue, and a valid InvoiceNo "
            "pattern. The gate raises QualityGateFailed when the checkpoint does "
            "not succeed, and the Airflow DAG places the Gold task downstream of "
            "it, so Gold never runs on data that failed validation."
        ),
    },
    {
        "id":   "doc_012",
        "text": (
            "Every capstone stage emits real OpenLineage events through the "
            "openlineage-python client. The stage_lineage context manager emits "
            "START on entry, COMPLETE when the stage finishes cleanly, and FAIL "
            "when it raises. Events are written to a local file transport at "
            "lineage_events/openlineage_run.log, and the same events can be sent "
            "to a Marquez server over HTTP transport instead."
        ),
    },
]
