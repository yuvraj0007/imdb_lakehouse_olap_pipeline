"""
test_analytics.py - Tests for the analytics/benchmark queries.

Tests that the benchmark queries (from run_analytics.py) execute correctly
against Spark SQL on sample Parquet data.
"""

import sys
from unittest.mock import MagicMock

import pytest

from src.etl.cleaners import clean_titles, clean_ratings, clean_episodes
from src.etl.transforms import join_datasets

sys.modules.setdefault("clickhouse_connect", MagicMock())

from src.analytics.queries import BENCHMARK_QUERIES  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def enriched_view(spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
    """
    Create the enriched dataset and register it as a Spark SQL temp view.
    Module-scoped so it's shared across all tests in this file.
    """
    titles_clean = clean_titles(titles_raw)
    ratings_clean = clean_ratings(ratings_raw)
    episodes_clean = clean_episodes(episodes_raw)
    enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

    # Register as temp view for SQL queries
    enriched.createOrReplaceTempView("imdb_enriched")

    return enriched


# Re-declare session-scoped fixtures as module-scoped for the module fixture
@pytest.fixture(scope="module")
def spark():
    """Module-scoped SparkSession for analytics tests."""
    import tempfile
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("IMDb_Analytics_Tests")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="module")
def titles_raw(spark):
    """Module-scoped titles fixture for analytics tests."""
    from pyspark.sql.types import StructType, StructField, StringType

    data = [
        ("tt0000001", "short", "Carmencita", "Carmencita", "0", "1894", None, "1", "Documentary,Short"),
        ("tt0000002", "short", "Le clown", "Le clown", "0", "1892", None, "5", "Animation,Short"),
        ("tt0000003", "movie", "The Shawshank Redemption", "The Shawshank Redemption",
         "0", "1994", None, "142", "Drama"),
        ("tt0000004", "tvSeries", "Breaking Bad", "Breaking Bad",
         "0", "2008", "2013", "49", "Crime,Drama,Thriller"),
        ("tt0000005", "tvEpisode", "Pilot", "Pilot", "0", "2008", None, "58", "Crime,Drama"),
        ("tt0000006", "movie", "Inception", "Inception", "0", "2010", None, "148", "Action,Sci-Fi,Thriller"),
        ("tt0000007", "movie", "Interstellar", "Interstellar", "0", "2014", None, "169", "Adventure,Drama,Sci-Fi"),
        ("tt0000008", "movie", "The Dark Knight", "The Dark Knight", "0", "2008", None, "152", "Action,Crime,Drama"),
        ("tt0000009", "tvSeries", "Game of Thrones", "Game of Thrones",
         "0", "2011", "2019", "57", "Action,Adventure,Drama"),
        ("tt0000010", "movie", "Pulp Fiction", "Pulp Fiction", "0", "1994", None, "154", "Crime,Drama"),
        ("tt0000011", "movie", "Fight Club", "Fight Club", "0", "1999", None, "139", "Drama"),
        ("tt0000012", "movie", "Forrest Gump", "Forrest Gump", "0", "1994", None, "142", "Drama,Romance"),
        ("tt0000013", "tvEpisode", "Ozymandias", "Ozymandias", "0", "2013", None, "47", "Crime,Drama,Thriller"),
        ("tt0000014", "movie", "A Recent Movie", "A Recent Movie", "0", "2022", None, "120", "Drama"),
        ("tt0000015", "movie", "Another Recent", "Another Recent", "0", "2023", None, "95", "Comedy"),
    ]

    schema = StructType([
        StructField("tconst", StringType(), nullable=True),
        StructField("titleType", StringType(), nullable=True),
        StructField("primaryTitle", StringType(), nullable=True),
        StructField("originalTitle", StringType(), nullable=True),
        StructField("isAdult", StringType(), nullable=True),
        StructField("startYear", StringType(), nullable=True),
        StructField("endYear", StringType(), nullable=True),
        StructField("runtimeMinutes", StringType(), nullable=True),
        StructField("genres", StringType(), nullable=True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def ratings_raw(spark):
    """Module-scoped ratings fixture for analytics tests."""
    from pyspark.sql.types import StructType, StructField, StringType

    data = [
        ("tt0000001", "5.7", "1973"),
        ("tt0000002", "5.8", "263"),
        ("tt0000003", "9.3", "2800000"),
        ("tt0000004", "9.5", "2000000"),
        ("tt0000005", "9.0", "150000"),
        ("tt0000006", "8.8", "2400000"),
        ("tt0000007", "8.7", "1900000"),
        ("tt0000008", "9.0", "2700000"),
        ("tt0000009", "9.2", "2100000"),
        ("tt0000010", "8.9", "2100000"),
        ("tt0000011", "8.8", "2100000"),
        ("tt0000012", "8.8", "2100000"),
        ("tt0000013", "10.0", "180000"),
        ("tt0000014", "7.5", "50000"),
        ("tt0000015", "6.8", "30000"),
    ]

    schema = StructType([
        StructField("tconst", StringType(), nullable=True),
        StructField("averageRating", StringType(), nullable=True),
        StructField("numVotes", StringType(), nullable=True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def episodes_raw(spark):
    """Module-scoped episodes fixture for analytics tests."""
    from pyspark.sql.types import StructType, StructField, StringType

    data = [
        ("tt0000005", "tt0000004", "1", "1"),
        ("tt0000013", "tt0000004", "5", "14"),
    ]

    schema = StructType([
        StructField("tconst", StringType(), nullable=True),
        StructField("parentTconst", StringType(), nullable=True),
        StructField("seasonNumber", StringType(), nullable=True),
        StructField("episodeNumber", StringType(), nullable=True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def tmp_output_dir():
    """Module-scoped temp directory."""
    import tempfile
    import shutil
    tmp_dir = tempfile.mkdtemp(prefix="imdb_analytics_test_")
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark Query Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestBenchmarkQueries:
    """Tests that all benchmark queries execute without error and return results."""

    def test_benchmark_queries_return_results(self, spark, enriched_view):
        """Each benchmark Spark SQL query should execute and return results."""
        for query_def in BENCHMARK_QUERIES:
            query_name = query_def["name"]
            spark_sql = query_def["spark_sql"]

            # Execute the query
            result = spark.sql(spark_sql)
            result.collect()

            # Query should execute without error (if we get here, it did)
            assert result is not None, f"Query '{query_name}' returned None"

    def test_query_results_are_non_empty(self, spark, enriched_view):
        """Each benchmark query should return at least one row with test data."""
        for query_def in BENCHMARK_QUERIES:
            query_name = query_def["name"]
            spark_sql = query_def["spark_sql"]

            result = spark.sql(spark_sql)
            count = result.count()

            assert count > 0, f"Query '{query_name}' returned 0 rows"

    def test_q1_count_by_title_type(self, spark, enriched_view):
        """Q1 should return counts grouped by title type."""
        result = spark.sql(BENCHMARK_QUERIES[0]["spark_sql"])
        rows = result.collect()

        # Should have multiple title types
        assert len(rows) >= 2

        # Each row should have cnt > 0
        for row in rows:
            assert row["cnt"] > 0

    def test_q2_top_rated_movies(self, spark, enriched_view):
        """Q2 should return top-rated movies with >100K votes."""
        result = spark.sql(BENCHMARK_QUERIES[1]["spark_sql"])
        rows = result.collect()

        # Should return movies with high votes
        for row in rows:
            assert row["num_votes"] > 100000
            assert row["average_rating"] is not None

        # Results should be ordered by rating DESC
        if len(rows) >= 2:
            assert rows[0]["average_rating"] >= rows[1]["average_rating"]

    def test_q3_avg_rating_by_genre(self, spark, enriched_view):
        """Q3 should return genre-level aggregations."""
        result = spark.sql(BENCHMARK_QUERIES[2]["spark_sql"])
        rows = result.collect()

        # Should have multiple genres
        assert len(rows) >= 2

        # Each genre should have valid stats
        for row in rows:
            assert row["genre"] is not None
            assert row["cnt"] > 0

    def test_q4_year_over_year(self, spark, enriched_view):
        """Q4 should return year-over-year production data for 2000-2024."""
        result = spark.sql(BENCHMARK_QUERIES[3]["spark_sql"])
        rows = result.collect()

        # All years should be in range
        for row in rows:
            assert 2000 <= row["start_year"] <= 2024

    def test_q5_episode_count_per_series(self, spark, enriched_view):
        """Q5 should return episode counts per series."""
        result = spark.sql(BENCHMARK_QUERIES[4]["spark_sql"])
        rows = result.collect()

        # Should return at least one series
        assert len(rows) >= 1

        # Episode counts should be positive
        for row in rows:
            assert row["episode_count"] > 0

    def test_q6_runtime_distribution(self, spark, enriched_view):
        """Q6 should return runtime distribution buckets."""
        result = spark.sql(BENCHMARK_QUERIES[5]["spark_sql"])
        rows = result.collect()

        # Should have at least one bucket
        assert len(rows) >= 1

        # Valid bucket names
        valid_buckets = {"Short", "Medium", "Standard", "Long"}
        for row in rows:
            assert row["runtime_bucket"] in valid_buckets
            assert row["cnt"] > 0
