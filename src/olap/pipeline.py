import logging
import sys
import time
from datetime import datetime

from src.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT, LAKE_PATH
from src.olap.connection import get_client, wait_for_clickhouse
from src.olap.loader import (
    discover_files,
    load_parquet_to_clickhouse,
    populate_aggregation_table,
)
from src.olap.schema import ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _log_row_counts(client) -> None:
    result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.imdb_titles_enriched")
    total = result.result_rows[0][0]
    logger.info(f"Total rows loaded: {total:,}")

    result = client.query(f"""
        SELECT title_type, count() as cnt
        FROM {CLICKHOUSE_DB}.imdb_titles_enriched
        GROUP BY title_type
        ORDER BY cnt DESC
        LIMIT 10
    """)
    logger.info("Rows by title_type:")
    for row in result.result_rows:
        logger.info(f"  {row[0]:20s}: {row[1]:>10,}")


def _log_partition_info(client) -> None:
    result = client.query(f"""
        SELECT
            partition,
            count() as parts,
            sum(rows) as total_rows,
            formatReadableSize(sum(bytes_on_disk)) as size
        FROM system.parts
        WHERE database = '{CLICKHOUSE_DB}'
          AND table = 'imdb_titles_enriched'
          AND active
        GROUP BY partition
        ORDER BY partition
        LIMIT 15
    """)
    logger.info("Partition summary (top 15):")
    for row in result.result_rows:
        logger.info(f"  {row[0]:<12} {row[1]:<8} {row[2]:<12,} {row[3]:<10}")


def _log_query_timing(client) -> None:
    start = time.time()
    client.query(f"""
        SELECT title_type, count(), avg(average_rating)
        FROM {CLICKHOUSE_DB}.imdb_titles_enriched
        WHERE start_year >= 2020
        GROUP BY title_type
        ORDER BY count() DESC
    """)
    elapsed_ms = (time.time() - start) * 1000
    logger.info(f"  Aggregate query (titles since 2020): {elapsed_ms:.1f}ms")

    start = time.time()
    client.query(f"""
        SELECT primary_title, average_rating, num_votes
        FROM {CLICKHOUSE_DB}.imdb_titles_enriched
        WHERE title_type = 'movie'
          AND num_votes > 100000
        ORDER BY average_rating DESC
        LIMIT 10
    """)
    elapsed_ms = (time.time() - start) * 1000
    logger.info(f"  Top rated movies (>100K votes): {elapsed_ms:.1f}ms")


def verify_load(client) -> None:
    _log_row_counts(client)
    _log_partition_info(client)
    _log_query_timing(client)


def main() -> None:
    pipeline_start = time.time()
    logger.info(f"OLAP Loading Pipeline - {datetime.now().isoformat()}")
    logger.info(f"Target: ClickHouse @ {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
    logger.info(f"Lake path: {LAKE_PATH}")

    if not wait_for_clickhouse():
        logger.error("Cannot connect to ClickHouse. Exiting.")
        sys.exit(1)

    client = get_client()

    try:
        ensure_schema(client)

        parquet_files = discover_files(LAKE_PATH)
        if not parquet_files:
            logger.error("No Parquet files found! Run the ETL job first.")
            sys.exit(1)

        total_rows = load_parquet_to_clickhouse(client, parquet_files)
        populate_aggregation_table(client)
        verify_load(client)

        duration = time.time() - pipeline_start
        logger.info(f"OLAP loading complete: {total_rows:,} rows in {duration:.1f}s")

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise
    finally:
        client.close()
        logger.info("ClickHouse connection closed.")


if __name__ == "__main__":
    main()
