import time
import logging
from typing import Any

from src.config import LAKE_PATH
from src.analytics.queries import BENCHMARK_QUERIES

logger = logging.getLogger(__name__)


def run_spark_benchmarks() -> list[dict[str, Any]]:
    from pyspark.sql import SparkSession

    logger.info("Initializing Spark for benchmark...")
    spark = (
        SparkSession.builder.appName("IMDb_Analytics_Benchmark")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    logger.info(f"Loading Parquet from: {LAKE_PATH}")
    df = spark.read.parquet(LAKE_PATH)
    df.createOrReplaceTempView("imdb_enriched")

    # Warm up
    logger.info("Warming up Spark...")
    spark.sql("SELECT count(*) FROM imdb_enriched").collect()

    results = []
    for query in BENCHMARK_QUERIES:
        logger.info(f"  Running Spark: {query['name']}...")

        # Run 3 times and take the median
        times = []
        for _ in range(3):
            start = time.time()
            spark.sql(query["spark_sql"]).collect()
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)

        median_time = sorted(times)[1]
        results.append({"name": query["name"], "spark_ms": round(median_time, 1)})
        logger.info(f"    {median_time:.1f}ms (median of 3 runs)")

    spark.stop()
    return results
