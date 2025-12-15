"""PostgreSQL utilities for storing and querying sentiment summaries and reviews.

This module is designed to be imported from Streamlit dashboards or
other services that process Kafka messages.

It assumes a PostgreSQL instance is reachable using standard
environment variables and uses a small connection pool.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Iterable, List, Mapping, Optional, Tuple

from dotenv import load_dotenv

# Load env so local development works without extra wiring
load_dotenv(override=True)

try:  # Optional dependency to avoid breaking non‑DB environments
    import psycopg2
    from psycopg2.pool import SimpleConnectionPool

    PSYCOPG2_AVAILABLE = True
except Exception:  # pragma: no cover - only hit when psycopg2 missing
    psycopg2 = None  # type: ignore
    SimpleConnectionPool = None  # type: ignore
    PSYCOPG2_AVAILABLE = False


# ----------------------------
# Configuration
# ----------------------------


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: Optional[str] = None

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        """Load PostgreSQL connection settings from environment.

        Supports both the standard libpq env vars and project‑specific
        fallbacks so it works on most setups without changes.
        """

        host = os.environ.get("PGHOST") or os.environ.get("PG_HOST", "localhost")
        port = int(os.environ.get("PGPORT") or os.environ.get("PG_PORT", "5432"))
        database = os.environ.get("PGDATABASE") or os.environ.get("PG_DB", "sentiment_db")
        user = os.environ.get("PGUSER") or os.environ.get("PG_USER", "sentiment_user")
        password = os.environ.get("PGPASSWORD") or os.environ.get("PG_PASSWORD", "changeme")
        sslmode = os.environ.get("PGSSLMODE") or os.environ.get("PG_SSLMODE")

        return cls(host=host, port=port, database=database, user=user, password=password, sslmode=sslmode)


_pool: Optional["SimpleConnectionPool"] = None
_db_initialized: bool = False


def _ensure_pool() -> Optional["SimpleConnectionPool"]:
    """Initialise the global connection pool lazily.

    Returns None if psycopg2 is not installed so that callers can
    gracefully no‑op in environments without PostgreSQL.
    """

    global _pool
    if not PSYCOPG2_AVAILABLE:
        return None

    if _pool is None:
        cfg = PostgresConfig.from_env()
        conn_kwargs = dict(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.database,
            user=cfg.user,
            password=cfg.password,
        )
        if cfg.sslmode:
            conn_kwargs["sslmode"] = cfg.sslmode

        try:
            _pool = SimpleConnectionPool(
                minconn=1,
                maxconn=int(os.environ.get("PG_POOL_MAX", "5")),
                **conn_kwargs,
            )
        except Exception:
            # If the server is not reachable, leave pool as None so callers
            # can decide how to degrade (e.g. return empty history).
            _pool = None

    return _pool


@contextmanager
def get_cursor():
    """Context manager that yields (conn, cursor) and commits/rolls back.

    Usage:

        with get_cursor() as (conn, cur):
            cur.execute(...)
    """

    pool = _ensure_pool()
    if pool is None:
        # psycopg2 not available or pool not initialised; behave as no‑op
        raise RuntimeError("PostgreSQL connection pool is not available. Install psycopg2-binary and configure env vars.")

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        try:
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        pool.putconn(conn)


# ----------------------------
# Schema management
# ----------------------------


SUMMARY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS topic_sentiment_summary (
    id                  BIGSERIAL PRIMARY KEY,
    topic               TEXT        NOT NULL,
    summary_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_posts         INTEGER     NOT NULL,
    positive_count      INTEGER     NOT NULL,
    neutral_count       INTEGER     NOT NULL,
    negative_count      INTEGER     NOT NULL,
    avg_compound_score  NUMERIC(5,4) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_topic_summary_topic_ts
    ON topic_sentiment_summary (topic, summary_ts DESC);
"""


REVIEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS topic_reviews (
    id              BIGSERIAL PRIMARY KEY,
    summary_id      BIGINT      NOT NULL REFERENCES topic_sentiment_summary(id) ON DELETE CASCADE,
    topic           TEXT        NOT NULL,
    summary_ts      TIMESTAMPTZ NOT NULL,
    sentiment       VARCHAR(16) NOT NULL,
    review_text     TEXT        NOT NULL,
    source          VARCHAR(64),
    original_score  NUMERIC(10,2),
    compound_score  NUMERIC(5,4),
    review_rank     SMALLINT    NOT NULL,
    external_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_topic_reviews_topic_sentiment_ts
    ON topic_reviews (topic, sentiment, summary_ts DESC);
"""


def init_db() -> None:
    """Create tables if they do not exist.

    Safe to call multiple times; uses IF NOT EXISTS.
    """

    global _db_initialized

    if not PSYCOPG2_AVAILABLE:
        return

    if _db_initialized:
        return

    with get_cursor() as (_, cur):
        cur.execute(SUMMARY_TABLE_SQL)
        cur.execute(REVIEWS_TABLE_SQL)

    _db_initialized = True


# ----------------------------
# Aggregation helpers
# ----------------------------


def _compute_summary(topic: str, messages: Iterable[Mapping]) -> Optional[Tuple[int, int, int, int, float]]:
    """Compute total/pos/neu/neg and average compound score for a topic.

    Expects each message to have `sentiment` and `compound_score` keys.
    """

    filtered: List[Mapping] = [m for m in messages if not topic or m.get("topic", topic) == topic]
    if not filtered:
        return None

    total = len(filtered)
    positive = sum(1 for m in filtered if str(m.get("sentiment")) == "Positive")
    negative = sum(1 for m in filtered if str(m.get("sentiment")) == "Negative")
    neutral = sum(1 for m in filtered if str(m.get("sentiment")) == "Neutral")

    compounds: List[float] = []
    for m in filtered:
        try:
            compounds.append(float(m.get("compound_score", 0.0) or 0.0))
        except (TypeError, ValueError):
            compounds.append(0.0)

    avg_compound = float(mean(compounds)) if compounds else 0.0
    return total, positive, neutral, negative, avg_compound


def _get_top_reviews(messages: Iterable[Mapping], sentiment: str, top_n: int = 5) -> List[Mapping]:
    """Return top N reviews for a sentiment label, ordered by compound score.

    For positive reviews we sort descending by compound score; for
    negative reviews ascending (most negative first).
    """

    filtered: List[Mapping] = [m for m in messages if str(m.get("sentiment")) == sentiment]
    if not filtered:
        return []

    reverse = sentiment == "Positive"
    sorted_msgs = sorted(
        filtered,
        key=lambda m: float(m.get("compound_score", 0.0) or 0.0),
        reverse=reverse,
    )
    return sorted_msgs[:top_n]


# ----------------------------
# Public API: store snapshot
# ----------------------------


def store_topic_snapshot(topic: str, messages: Iterable[Mapping], top_n: int = 5) -> Optional[int]:
    """Persist a snapshot summary and top reviews for a topic.

    This is intended to be called from the Kafka consumer loop or
    dashboard after new messages have been processed.

    Returns the created summary_id, or None if nothing was stored.
    """

    if not PSYCOPG2_AVAILABLE:
        # Allow dashboards to run without PostgreSQL installed
        return None

    # Ensure tables exist before we attempt to write. If the DB is not
    # reachable, swallow the error so the live dashboard keeps working.
    try:
        init_db()
    except Exception:
        return None

    computed = _compute_summary(topic, messages)
    if computed is None:
        return None

    total, positive, neutral, negative, avg_compound = computed

    snapshot_ts = datetime.utcnow()
    top_pos = _get_top_reviews(messages, "Positive", top_n)
    top_neg = _get_top_reviews(messages, "Negative", top_n)

    try:
        with get_cursor() as (_, cur):
            # Insert summary row
            cur.execute(
                """
                INSERT INTO topic_sentiment_summary (
                    topic, summary_ts, total_posts,
                    positive_count, neutral_count, negative_count,
                    avg_compound_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, summary_ts
                """,
                (topic, snapshot_ts, total, positive, neutral, negative, avg_compound),
            )
            summary_id, db_ts = cur.fetchone()

            # Helper to insert a batch of reviews for one sentiment
            def insert_reviews(batch: List[Mapping], label: str) -> None:
                if not batch:
                    return
                values = []
                for rank, m in enumerate(batch, start=1):
                    text = str(m.get("text") or m.get("title") or "").strip()
                    source = m.get("source") or None
                    ext_id = m.get("id") or None
                    try:
                        original_score = float(m.get("score")) if m.get("score") is not None else None
                    except (TypeError, ValueError):
                        original_score = None
                    try:
                        compound = float(m.get("compound_score", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        compound = 0.0

                    if not text:
                        continue

                    values.append(
                        (
                            summary_id,
                            topic,
                            db_ts,
                            label,
                            text,
                            source,
                            original_score,
                            compound,
                            rank,
                            ext_id,
                        )
                    )

                if not values:
                    return

                cur.executemany(
                    """
                    INSERT INTO topic_reviews (
                        summary_id, topic, summary_ts,
                        sentiment, review_text, source,
                        original_score, compound_score, review_rank, external_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    values,
                )

            insert_reviews(top_pos, "Positive")
            insert_reviews(top_neg, "Negative")

            return int(summary_id)
    except Exception:
        # Any connection/DB error should not break the streaming UI
        return None


# ----------------------------
# Public API: history queries
# ----------------------------


def fetch_topic_summaries(topic: str, limit: int = 100) -> List[Tuple]:
    """Fetch recent summary rows for a topic ordered by time descending.

    Returns a list of tuples matching the topic_sentiment_summary columns
    in natural order.
    """

    if not PSYCOPG2_AVAILABLE:
        return []

    try:
        with get_cursor() as (_, cur):
            cur.execute(
                """
                SELECT
                    id,
                    topic,
                    summary_ts,
                    total_posts,
                    positive_count,
                    neutral_count,
                    negative_count,
                    avg_compound_score
                FROM topic_sentiment_summary
                WHERE topic = %s
                ORDER BY summary_ts DESC
                LIMIT %s
                """,
                (topic, limit),
            )
            return list(cur.fetchall())
    except Exception:
        # If DB is down, just return no history instead of crashing.
        return []


def fetch_topic_top_reviews(topic: str, sentiment: str, limit: int = 50) -> List[Tuple]:
    """Fetch historical top reviews for a topic and sentiment label."""

    if not PSYCOPG2_AVAILABLE:
        return []

    try:
        with get_cursor() as (_, cur):
            cur.execute(
                """
                SELECT
                    summary_id,
                    topic,
                    summary_ts,
                    sentiment,
                    review_text,
                    source,
                    original_score,
                    compound_score,
                    review_rank,
                    external_id
                FROM topic_reviews
                WHERE topic = %s AND sentiment = %s
                ORDER BY summary_ts DESC, review_rank ASC
                LIMIT %s
                """,
                (topic, sentiment, limit),
            )
            return list(cur.fetchall())
    except Exception:
        return []
