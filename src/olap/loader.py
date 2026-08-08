import re
import sys
import time
import logging
from pathlib import Path

import pyarrow.parquet as pq

from src.config import CLICKHOUSE_DB, COLUMN_ORDER, BATCH_SIZE

logger = logging.getLogger(__name__)


def discover_files(lake_path: str) -> list[str]:
    lake_dir = Path(lake_path)

    if not lake_dir.exists():
        logger.error(f"Lake path does not exist: {lake_path}")
        logger.error("Have you run the ETL job first? (make etl)")
        sys.exit(1)

    parquet_files = sorted(str(f) for f in lake_dir.rglob("*.parquet"))
    logger.info(f"  Discovered {len(parquet_files)} Parquet files")
    return parquet_files


def load_parquet_to_clickhouse(client, parquet_files: list[str]) -> int:
    logger.info(f"Loading {len(parquet_files)} Parquet files into ClickHouse...")
    logger.info(f"  Batch size: {BATCH_SIZE:,} rows")

    client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.imdb_titles_enriched")
    logger.info("  Truncated existing data")

    total_rows = 0
    total_files = len(parquet_files)
    start_time = time.time()

    for idx, filepath in enumerate(parquet_files, 1):
        try:
            rows_loaded = _load_single_file(client, filepath)
            total_rows += rows_loaded

            if idx % 100 == 0 or idx == total_files:
                elapsed = time.time() - start_time
                rate = total_rows / elapsed if elapsed > 0 else 0
                logger.info(
                    f"  Progress: {idx}/{total_files} files | "
                    f"{total_rows:,} rows | {rate:,.0f} rows/sec"
                )
        except Exception as e:
            logger.warning(f"  Error loading {filepath}: {e}")
            continue

    elapsed = time.time() - start_time
    rate = total_rows / elapsed if elapsed > 0 else 0
    logger.info(f"  Load complete: {total_rows:,} rows in {elapsed:.1f}s ({rate:,.0f} rows/sec)")
    return total_rows


def _prepare_dataframe(filepath: str):
    """Read parquet file, extract partition columns, normalize for ClickHouse."""
    table = pq.read_table(filepath)
    df = table.to_pandas()

    if df.empty:
        return None

    title_type_match = re.search(r'title_type=([^/]+)', filepath)
    start_year_match = re.search(r'start_year=([^/]+)', filepath)

    df['title_type'] = title_type_match.group(1) if title_type_match else None
    df['start_year'] = int(start_year_match.group(1)) if start_year_match else None

    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None

    df = df[COLUMN_ORDER]

    for col in ['title_type', 'primary_title', 'original_title', 'genres']:
        if col in df.columns:
            df[col] = df[col].fillna('')

    df['genres'] = df['genres'].replace('', 'Unknown')
    return df.where(df.notnull(), None)


def _load_single_file(client, filepath: str) -> int:
    df = _prepare_dataframe(filepath)
    if df is None:
        return 0

    rows = df.values.tolist()
    row_count = len(rows)

    for batch_start in range(0, row_count, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, row_count)
        client.insert(
            f"{CLICKHOUSE_DB}.imdb_titles_enriched",
            rows[batch_start:batch_end],
            column_names=COLUMN_ORDER,
        )

    return row_count


def populate_aggregation_table(client) -> None:
    logger.info("Populating ratings aggregation table...")

    client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.imdb_ratings_by_type_year")

    client.command(f"""
        INSERT INTO {CLICKHOUSE_DB}.imdb_ratings_by_type_year
        SELECT
            title_type,
            coalesce(start_year, 0) AS start_year,
            avg(average_rating) AS avg_rating,
            sum(num_votes) AS total_votes,
            count() AS title_count
        FROM {CLICKHOUSE_DB}.imdb_titles_enriched
        WHERE average_rating IS NOT NULL
        GROUP BY title_type, start_year
    """)

    count = client.query(
        f"SELECT count() FROM {CLICKHOUSE_DB}.imdb_ratings_by_type_year"
    ).result_rows[0][0]
    logger.info(f"  Aggregation table populated: {count:,} rows")
