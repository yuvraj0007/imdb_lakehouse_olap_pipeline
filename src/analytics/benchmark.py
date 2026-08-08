import logging
import os
from datetime import datetime
from typing import Any

from src.analytics.clickhouse_runner import run_clickhouse_benchmarks
from src.analytics.spark_runner import run_spark_benchmarks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def print_comparison(spark_results: list[dict[str, Any]], ch_results: list[dict[str, Any]]) -> None:
    logger.info("PERFORMANCE COMPARISON: PySpark (Parquet) vs ClickHouse (OLAP)")

    total_spark = 0.0
    total_ch = 0.0

    for spark_r, ch_r in zip(spark_results, ch_results):
        name = spark_r["name"]
        spark_ms = spark_r["spark_ms"]
        ch_ms = ch_r["clickhouse_ms"]
        speedup = spark_ms / ch_ms if ch_ms > 0 else float("inf")

        total_spark += spark_ms
        total_ch += ch_ms

        logger.info(f"  {name:<43} {spark_ms:>8.1f}ms   {ch_ms:>10.1f}ms    {speedup:>6.1f}x")

    overall_speedup = total_spark / total_ch if total_ch > 0 else float("inf")
    logger.info(f"  {'TOTAL':<43} {total_spark:>8.1f}ms   {total_ch:>10.1f}ms    {overall_speedup:>6.1f}x")
    logger.info(f"CONCLUSION: ClickHouse is {overall_speedup:.0f}x faster overall for analytical queries.")


def main() -> None:
    logger.info(f"IMDb Analytics Performance Benchmark - {datetime.now().isoformat()}")

    skip_spark = os.environ.get("SKIP_SPARK", "false").lower() == "true"

    if not skip_spark:
        try:
            logger.info("[Phase 1] Running PySpark Benchmarks...")
            spark_results = run_spark_benchmarks()
        except Exception as e:
            logger.warning(f"Spark benchmark failed: {e}")
            logger.warning("Running ClickHouse-only benchmark...")
            skip_spark = True

    logger.info("[Phase 2] Running ClickHouse Benchmarks...")
    ch_results = run_clickhouse_benchmarks()

    if not skip_spark:
        print_comparison(spark_results, ch_results)
    else:
        logger.info("CLICKHOUSE QUERY PERFORMANCE")
        for r in ch_results:
            logger.info(f"  {r['name']:<45}: {r['clickhouse_ms']:>8.1f}ms")


if __name__ == "__main__":
    main()
