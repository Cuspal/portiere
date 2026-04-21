"""
PGVector Backend — Vector search using PostgreSQL + pgvector extension.

Uses PostgreSQL with the pgvector extension for vector similarity search.
Requires: ``pip install portiere-health[pgvector]``
"""

from __future__ import annotations

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class PGVectorBackend(KnowledgeLayerBackend):
    """
    PostgreSQL + pgvector backend for vector search.

    Best for: Teams already using PostgreSQL, moderate-scale vocabularies.
    Requires: psycopg[binary], pgvector
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "portiere_concepts",
        *,
        embedding_gateway=None,
    ):
        try:
            import psycopg
            from pgvector.psycopg import register_vector

            self._psycopg = psycopg
            self._register_vector = register_vector
        except ImportError:
            raise ImportError(
                "psycopg and pgvector are required for PGVector backend. "
                "Install with: pip install portiere-health[pgvector]"
            )

        self._connection_string = connection_string
        self._table_name = table_name
        self._embedding_gateway = embedding_gateway
        self._dimension: int | None = None

        # Connect and register vector type
        self._conn = self._psycopg.connect(connection_string)
        self._register_vector(self._conn)

        logger.info("pgvector.initialized", table=table_name)

    def _ensure_table(self, dimension: int) -> None:
        """Create table and index if not exists."""
        with self._conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
                    concept_id INTEGER PRIMARY KEY,
                    concept_name TEXT NOT NULL,
                    vocabulary_id TEXT DEFAULT '',
                    domain_id TEXT DEFAULT '',
                    concept_class_id TEXT DEFAULT '',
                    standard_concept TEXT DEFAULT '',
                    embedding vector({dimension})
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._table_name}_embedding
                ON {self._table_name}
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
            """)
        self._conn.commit()
        self._dimension = dimension

    def _embed(self, texts: list[str]) -> np.ndarray:
        """Embed texts using the gateway."""
        if self._embedding_gateway is None:
            raise RuntimeError(
                "PGVector backend requires an embedding_gateway. "
                "Pass embedding_gateway= to the constructor."
            )
        embeddings = self._embedding_gateway.encode(texts, convert_to_numpy=True)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return embeddings / norms

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search with optional filtering."""
        query_embedding = self._embed([query])[0]

        # Build query with filters
        conditions = []
        params: list = [query_embedding.tolist()]
        if vocabularies:
            conditions.append("vocabulary_id = ANY(%s)")
            params.append(vocabularies)
        if domain:
            conditions.append("domain_id = %s")
            params.append(domain)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        params.append(limit)

        sql = f"""
            SELECT concept_id, concept_name, vocabulary_id, domain_id,
                   concept_class_id, standard_concept,
                   1 - (embedding <=> %s::vector) AS score
            FROM {self._table_name}
            {where_clause}
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """
        # Need to pass embedding twice (for SELECT score and ORDER BY)
        params_final = [params[0]] + params[1:] + [params[0], params[-1]]
        # Rebuild params correctly
        base_params: list = [query_embedding.tolist()]
        filter_params: list = []
        if vocabularies:
            filter_params.append(vocabularies)
        if domain:
            filter_params.append(domain)

        sql = f"""
            SELECT concept_id, concept_name, vocabulary_id, domain_id,
                   concept_class_id, standard_concept,
                   1 - (embedding <=> %s::vector) AS score
            FROM {self._table_name}
            {where_clause}
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """
        all_params = base_params + filter_params + base_params + [limit]

        with self._conn.cursor() as cur:
            cur.execute(sql, all_params)
            rows = cur.fetchall()

        return [
            {
                "concept_id": row[0],
                "concept_name": row[1],
                "vocabulary_id": row[2],
                "domain_id": row[3],
                "concept_class_id": row[4],
                "standard_concept": row[5],
                "score": max(0.0, float(row[6])),
            }
            for row in rows
        ]

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT concept_id, concept_name, vocabulary_id, domain_id, "
                f"concept_class_id, standard_concept "
                f"FROM {self._table_name} WHERE concept_id = %s",
                (concept_id,),
            )
            row = cur.fetchone()

        if row is None:
            raise ValueError(f"Concept {concept_id} not found")

        return {
            "concept_id": row[0],
            "concept_name": row[1],
            "vocabulary_id": row[2],
            "domain_id": row[3],
            "concept_class_id": row[4],
            "standard_concept": row[5],
        }

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into PostgreSQL."""
        if not concepts:
            return

        names = [c["concept_name"] for c in concepts]
        logger.info("pgvector.encoding_concepts", count=len(names))
        embeddings = self._embed(names)

        # Ensure table exists with correct dimension
        self._ensure_table(embeddings.shape[1])

        with self._conn.cursor() as cur:
            # Batch insert with upsert
            for i, (concept, emb) in enumerate(zip(concepts, embeddings)):
                cur.execute(
                    f"""
                    INSERT INTO {self._table_name}
                    (concept_id, concept_name, vocabulary_id, domain_id,
                     concept_class_id, standard_concept, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (concept_id) DO UPDATE SET
                        concept_name = EXCLUDED.concept_name,
                        embedding = EXCLUDED.embedding
                    """,
                    (
                        concept["concept_id"],
                        concept["concept_name"],
                        concept.get("vocabulary_id", ""),
                        concept.get("domain_id", ""),
                        concept.get("concept_class_id", ""),
                        concept.get("standard_concept", ""),
                        emb.tolist(),
                    ),
                )

        self._conn.commit()
        logger.info("pgvector.concepts_indexed", count=len(concepts))
