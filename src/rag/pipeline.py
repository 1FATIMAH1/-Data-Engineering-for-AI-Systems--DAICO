"""
Stage 3 — RAG.

The Day 3 pipeline end to end: sentence chunking with overlap -> ChromaDB (HNSW)
dense index -> BM25 keyword index -> Reciprocal Rank Fusion -> cross-encoder
reranking -> a grounded answer.

The one thing the capstone rubric asks for beyond Day 3 is citations, so
`build_rag_prompt` instructs the model to cite [Source N] and
`answer_with_citations` prints the source table that maps every [Source N] back
to its chunk id and parent document id.
"""

import os
import re

import chromadb
import numpy as np
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder, SentenceTransformer

from src.config import COLLECTION_NAME, EMBED_MODEL, GROQ_MODEL, RERANK_MODEL
from src.rag.knowledge_base import DOCUMENTS


# =====================================================================
# STAGE 1 — CHUNKING (sentence-level with overlap)
# =====================================================================

def chunk_documents(docs: list[dict], chunk_size: int = 2) -> list[dict]:
    """
    Splits each document into overlapping sentence chunks.
    chunk_size = 2 means each chunk contains 2 consecutive sentences.
    Overlap of 1 sentence ensures context is not lost at chunk boundaries.
    """
    all_chunks = []
    for doc in docs:
        sentences = re.split(r"(?<=[.!?])\s+", doc["text"].strip())
        for i in range(0, len(sentences), max(1, chunk_size - 1)):
            chunk_text = " ".join(sentences[i : i + chunk_size])
            if not chunk_text.strip():
                continue
            all_chunks.append({
                "id":     f"{doc['id']}_chunk_{i:03d}",
                "text":   chunk_text,
                "doc_id": doc["id"],
            })
    return all_chunks


# =====================================================================
# STAGE 2 — VECTOR INDEX (ChromaDB + SentenceTransformer bi-encoder)
# =====================================================================

def build_vector_index(chunks: list[dict]) -> chromadb.Collection:
    """
    Embeds all chunks with all-MiniLM-L6-v2 and stores them in ChromaDB.
    ChromaDB uses HNSW internally — the same index type as Pinecone/Weaviate.
    """
    print("\n📦 Building ChromaDB vector index...")
    ef = SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL)
    client     = chromadb.Client()
    collection = client.get_or_create_collection(
        COLLECTION_NAME, embedding_function=ef
    )
    collection.add(
        ids       = [c["id"]   for c in chunks],
        documents = [c["text"] for c in chunks],
        metadatas = [{"doc_id": c["doc_id"]} for c in chunks],
    )
    print(f"   ✅ {len(chunks)} chunks indexed (HNSW backend, {EMBED_MODEL} embeddings)")
    return collection


# =====================================================================
# STAGE 3 — BM25 KEYWORD INDEX
# =====================================================================

class BM25Index:
    """
    Classic TF-IDF keyword search using the BM25 ranking function.
    Finds exact keyword matches that semantic search often misses
    (e.g., product names, acronyms, version numbers like 'GPT-4').
    """

    def __init__(self, chunks: list[dict]):
        tokenised   = [c["text"].lower().split() for c in chunks]
        self.bm25   = BM25Okapi(tokenised)
        self.chunks = chunks

    def search(self, query: str, top_k: int = 10) -> list[tuple[float, dict]]:
        scores = self.bm25.get_scores(query.lower().split())
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
        return [(score, self.chunks[idx]) for idx, score in ranked[:top_k]]


# =====================================================================
# STAGE 4 — HYBRID SEARCH WITH RECIPROCAL RANK FUSION
# =====================================================================

def reciprocal_rank_fusion(
    vector_hits: list[dict],
    bm25_hits:   list[tuple[float, dict]],
    chunk_index: dict[str, dict],
    k:           int = 60,
    top_k:       int = 6,
) -> list[dict]:
    """
    RRF score = Σ 1/(k + rank_i) across both result lists.

    k=60 is the empirically optimal constant — it prevents the top-rank
    position from dominating and rewards documents that rank consistently
    across both systems. No weights to tune: RRF is parameter-free.

    `chunk_index` maps chunk id -> chunk, so the fused list keeps doc_id and the
    answer can cite the parent document.
    """
    rrf_scores: dict[str, float] = {}

    for rank, hit in enumerate(vector_hits):
        cid = hit["id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)

    for rank, (_, chunk) in enumerate(bm25_hits):
        cid = chunk["id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)

    sorted_ids = sorted(rrf_scores, key=lambda x: rrf_scores[x], reverse=True)
    return [chunk_index[cid] for cid in sorted_ids[:top_k]]


# =====================================================================
# STAGE 5 — CROSS-ENCODER RERANKING (Stage 2 precision gate)
# =====================================================================

def rerank(
    query:      str,
    candidates: list[dict],
    model_name: str = RERANK_MODEL,
    top_k:      int = 3,
) -> list[dict]:
    """
    Cross-encoder jointly encodes the (query, document) pair.
    This is much more accurate than cosine similarity between
    independent embeddings, because it can model the interaction
    between query tokens and document tokens directly.

    Cost: O(n) inference calls vs O(1) for bi-encoder similarity.
    Strategy: retrieve top-50 fast with bi-encoder, rerank top-50
    with cross-encoder, return top-5 to the LLM.
    """
    print(f"  🎯 Cross-encoder reranking {len(candidates)} candidates...")
    model  = CrossEncoder(model_name)
    pairs  = [(query, c["text"]) for c in candidates]
    scores = model.predict(pairs)
    ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    return [doc for _, doc in ranked[:top_k]]


# =====================================================================
# STAGE 6 — RAG PROMPT CONSTRUCTION (with citation instruction)
# =====================================================================

def build_rag_prompt(query: str, context_docs: list[dict]) -> str:
    """
    Constructs the prompt sent to the LLM. Every context block is numbered so
    the model can cite it, and the instruction forces the answer to stay inside
    the retrieved context.
    """
    context = "\n\n".join(
        f"[Source {i+1}]: {d['text']}" for i, d in enumerate(context_docs)
    )
    return (
        "You are a data engineering expert. Answer the question strictly based\n"
        "on the provided context. Do not add information not in the context.\n"
        "Cite the sources you used inline as [Source 1], [Source 2], and so on.\n"
        "Every factual sentence in your answer must carry at least one citation.\n\n"
        f"CONTEXT:\n{context}\n\n"
        f"QUESTION: {query}\n\n"
        "ANSWER:"
    )


def answer_with_citations(query: str, context_docs: list[dict]) -> str:
    """
    Calls Groq when GROQ_API_KEY is set (the Day 3 LLM call), otherwise prints
    the grounded context and the citation map so the pipeline still produces a
    fully sourced answer offline.
    """
    prompt = build_rag_prompt(query, context_docs)

    api_key = os.getenv("GROQ_API_KEY")
    if api_key:
        from groq import Groq

        print("\n  🤖 Calling LLM with Groq...")
        groq_client = Groq(api_key=api_key)
        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=GROQ_MODEL,
            temperature=0.60,   # For factual answers
        )
        answer = chat_completion.choices[0].message.content
    else:
        print("\n  ⚠️  GROQ_API_KEY not set — returning the grounded context itself,")
        print("      already cited, instead of a generated answer.")
        answer = "\n".join(
            f"[Source {i+1}] {d['text']}" for i, d in enumerate(context_docs)
        )

    print("\n  ✅ Answer:")
    print(f"    {answer}")

    print("\n  📚 Citations:")
    for i, d in enumerate(context_docs, 1):
        print(f"    [Source {i}] chunk={d['id']}  document={d['doc_id']}")
    return answer


# =====================================================================
# STAGE 7 — EVALUATION (simplified RAGAS-style metrics)
# =====================================================================

def evaluate(
    query:               str,
    retrieved_docs:      list[dict],
    embed_model:         SentenceTransformer,
    relevance_threshold: float = 0.30,
) -> dict:
    """
    Computes two simplified metrics using cosine similarity:

    Context Precision = fraction of retrieved chunks above the
                        relevance threshold (no noise in context).
    Avg Similarity    = mean cosine score between query and chunks
                        (proxy for Context Recall).
    """
    q_emb  = embed_model.encode(query, normalize_embeddings=True)
    scores = [
        float(np.dot(q_emb, embed_model.encode(d["text"], normalize_embeddings=True)))
        for d in retrieved_docs
    ]
    relevant = sum(s > relevance_threshold for s in scores)
    return {
        "context_precision": round(relevant / len(scores), 3),
        "avg_similarity":    round(sum(scores) / len(scores), 3),
        "chunks_in_context": len(retrieved_docs),
    }


# =====================================================================
# MAIN — Full RAG run
# =====================================================================

QUERIES = [
    "How does the ingestion stage quarantine records that break the data contract?",
    "What business key does the Silver MERGE upsert on?",
    "What is Reciprocal Rank Fusion and why is it better than weighted merging?",
    "How does the quality gate stop the pipeline before the Gold layer runs?",
]


def run_rag(queries: list[str] | None = None) -> list[dict]:
    queries = queries or QUERIES

    print("=" * 65)
    print("  Capstone Stage 3 — RAG over the pipeline knowledge base")
    print("=" * 65)

    # Stage 1: Chunk
    chunks = chunk_documents(DOCUMENTS, chunk_size=2)
    print(f"\n📄 {len(DOCUMENTS)} documents → {len(chunks)} chunks after splitting")
    chunk_index = {c["id"]: c for c in chunks}

    # Stage 2: Build indexes
    collection  = build_vector_index(chunks)
    bm25_index  = BM25Index(chunks)
    embed_model = SentenceTransformer(EMBED_MODEL)

    results = []
    for query in queries:
        print(f"\n{'=' * 65}")
        print(f"QUERY: {query}")
        print("=" * 65)

        # Vector (semantic) search
        vec_results = collection.query(query_texts=[query], n_results=6)
        vec_hits = [
            {"id": vid, "document": vdoc}
            for vid, vdoc in zip(
                vec_results["ids"][0],
                vec_results["documents"][0],
            )
        ]
        print(f"\n  🔍 Vector search:   {len(vec_hits)} candidates")

        # BM25 keyword search
        bm25_hits = bm25_index.search(query, top_k=6)
        print(f"  🔑 BM25 search:     {len(bm25_hits)} candidates")

        # Hybrid RRF fusion
        hybrid = reciprocal_rank_fusion(vec_hits, bm25_hits, chunk_index, top_k=6)
        print(f"  ⚡ RRF fusion:      {len(hybrid)} merged candidates")

        # Cross-encoder rerank
        final_docs = rerank(query, hybrid, top_k=3)

        print("\n  📝 Top-3 chunks after reranking:")
        for i, doc in enumerate(final_docs, 1):
            print(f"    [{i}] {doc['text'][:110]}...")

        # Grounded, cited answer
        answer = answer_with_citations(query, final_docs)

        # Evaluate
        metrics = evaluate(query, final_docs, embed_model)
        print(f"\n  📊 Retrieval metrics: {metrics}")
        if metrics["context_precision"] < 0.5:
            print("  ⚠️  Low precision — consider increasing ef or lowering chunk overlap")
        else:
            print("  ✅  Retrieval quality looks good")

        results.append({
            "query":     query,
            "answer":    answer,
            "citations": [{"chunk_id": d["id"], "doc_id": d["doc_id"]} for d in final_docs],
            "metrics":   metrics,
        })

    print("\n" + "=" * 65)
    print(f"RAG stage complete — {len(results)} queries answered with citations.")
    print("=" * 65)
    return results


if __name__ == "__main__":
    run_rag()
