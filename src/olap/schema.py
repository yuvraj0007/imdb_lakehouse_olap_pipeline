import logging

from src.config import CLICKHOUSE_DB

logger = logging.getLogger(__name__)


def ensure_schema(client) -> None:
    logger.info("Ensuring ClickHouse schema exists...")

    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.imdb_titles_enriched (
            tconst          String,
            title_type      LowCardinality(String),
            primary_title   String,
            original_title  String,
            is_adult        Nullable(UInt8),
            start_year      Nullable(UInt16),
            end_year        Nullable(UInt16),
            runtime_minutes Nullable(UInt16),
            genres          String DEFAULT 'Unknown',
            average_rating  Nullable(Float32),
            num_votes       Nullable(UInt32),
            parent_tconst   Nullable(String),
            season_number   Nullable(UInt16),
            episode_number  Nullable(UInt16)
        )
        ENGINE = MergeTree()
        PARTITION BY toUInt16(coalesce(start_year, 0) / 10) * 10
        ORDER BY (title_type, coalesce(start_year, 0), tconst)
        SETTINGS index_granularity = 8192
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.imdb_ratings_by_type_year (
            title_type      LowCardinality(String),
            start_year      UInt16,
            avg_rating      Float64,
            total_votes     UInt64,
            title_count     UInt64
        )
        ENGINE = SummingMergeTree()
        ORDER BY (title_type, start_year)
    """)

    logger.info("  Schema verified/created successfully")
