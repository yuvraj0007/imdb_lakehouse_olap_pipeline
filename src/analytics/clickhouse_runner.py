import logging
import time
from typing import Any

import clickhouse_connect

from src.analytics.queries import BENCHMARK_QUERIES
from src.config import (
    CLICKHOUSE_DB,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)

logger = logging.getLogger(__name__)


def run_clickhouse_benchmarks() -> list[dict[str, Any]]:
    logger.info("Connecting to ClickHouse for benchmark...")

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    # Warm up
    logger.info("Warming up ClickHouse...")
    client.query("SELECT count() FROM imdb.imdb_titles_enriched")

    results = []
    for query in BENCHMARK_QUERIES:
        logger.info(f"  Running ClickHouse: {query['name']}...")

        # Run 3 times and take the median
        times = []
        for _ in range(3):
            start = time.time()
            client.query(query["clickhouse_sql"])
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)

        median_time = sorted(times)[1]
        results.append({"name": query["name"], "clickhouse_ms": round(median_time, 1)})
        logger.info(f"    {median_time:.1f}ms (median of 3 runs)")

    client.close()
    return results
