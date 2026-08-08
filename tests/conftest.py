"""
conftest.py - Shared pytest fixtures for IMDb Lakehouse OLAP Pipeline tests.
"""

import sys
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Mock clickhouse_connect before any src imports (olap modules import it)
sys.modules.setdefault("clickhouse_connect", MagicMock())

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    StructType,
    StructField,
    StringType,
)


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession Fixture
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for testing.
    Uses local[2] to test parallelism while keeping tests fast.
    Session-scoped to avoid repeated startup/teardown overhead.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("IMDb_ETL_Tests")
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


# ─────────────────────────────────────────────────────────────────────────────
# Temp Directory Fixture
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_output_dir():
    """
    Create a temporary directory for Parquet output.
    Cleaned up after each test.
    """
    tmp_dir = tempfile.mkdtemp(prefix="imdb_test_")
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
def tmp_data_dir():
    """
    Create a temporary directory for input TSV data.
    Cleaned up after each test.
    """
    tmp_dir = tempfile.mkdtemp(prefix="imdb_test_data_")
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# Sample DataFrames Fixtures (raw IMDb-like data)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def titles_raw(spark):
    """
    Sample titles DataFrame mimicking title.basics.tsv structure.
    Includes:
    - Normal valid rows
    - Row with null tconst (should be filtered)
    - Row with invalid year (should be filtered)
    - Row with null genres (should become 'Unknown')
    - Duplicate tconst (should be deduplicated)
    - Row with non-'tt' prefix (should be filtered)
    """
    data = [
        ("tt0000001", "short", "Carmencita", "Carmencita",
         "0", "1894", None, "1", "Documentary,Short"),
        ("tt0000002", "short", "Le clown et ses chiens", "Le clown et ses chiens",
         "0", "1892", None, "5", "Animation,Short"),
        ("tt0000003", "movie", "The Great Train Robbery", "The Great Train Robbery",
         "0", "1903", None, "11", "Action,Crime,Western"),
        ("tt0000004", "tvSeries", "Breaking Bad", "Breaking Bad",
         "0", "2008", "2013", "49", "Crime,Drama,Thriller"),
        ("tt0000005", "tvEpisode", "Pilot", "Pilot",
         "0", "2008", None, "58", None),
        ("tt0000001", "short", "Carmencita", "Carmencita",
         "0", "1894", None, "1", "Documentary,Short"),
        (None, "movie", "No ID Movie", "No ID Movie",
         "0", "2020", None, "90", "Drama"),
        ("tt0000006", "movie", "Future Movie", "Future Movie",
         "0", "9999", None, "120", "Sci-Fi"),
        ("tt0000007", "movie", "Ancient Movie", "Ancient Movie",
         "0", "1800", None, "60", "History"),
        ("nm0000001", "movie", "Wrong Prefix", "Wrong Prefix",
         "0", "2020", None, "90", "Drama"),
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


@pytest.fixture
def ratings_raw(spark):
    """
    Sample ratings DataFrame mimicking title.ratings.tsv structure.
    Includes:
    - Normal valid rows
    - Row with rating > 10 (should be filtered)
    - Row with negative rating (should be filtered)
    - Row with 0 votes (should be filtered)
    - Row with null tconst (should be filtered)
    """
    data = [
        ("tt0000001", "7.4", "2145"),
        ("tt0000002", "6.0", "305"),
        ("tt0000003", "8.2", "75000"),
        ("tt0000004", "9.5", "1800000"),
        ("tt0000005", "8.9", "120000"),
        ("tt0000010", "11.5", "100"),  # invalid rating > 10
        ("tt0000011", "-1.0", "50"),   # invalid negative rating
        ("tt0000012", "7.0", "0"),     # zero votes
        (None, "6.5", "100"),          # null tconst
    ]

    schema = StructType([
        StructField("tconst", StringType(), nullable=True),
        StructField("averageRating", StringType(), nullable=True),
        StructField("numVotes", StringType(), nullable=True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def episodes_raw(spark):
    """
    Sample episodes DataFrame mimicking title.episode.tsv structure.
    Includes:
    - Normal valid rows
    - Row with null tconst (should be filtered)
    """
    data = [
        ("tt0000005", "tt0000004", "1", "1"),
        ("tt0000020", "tt0000004", "1", "2"),
        ("tt0000021", "tt0000004", "2", "1"),
        ("tt0000022", "tt0000010", "3", "5"),
        (None, "tt0000004", "1", "3"),  # null tconst
    ]

    schema = StructType([
        StructField("tconst", StringType(), nullable=True),
        StructField("parentTconst", StringType(), nullable=True),
        StructField("seasonNumber", StringType(), nullable=True),
        StructField("episodeNumber", StringType(), nullable=True),
    ])

    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────────────────────────────────────
# ClickHouse Mock Fixture
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_clickhouse_client():
    """
    Mock ClickHouse client for testing load_to_olap.py without a real connection.
    """
    mock_client = MagicMock()

    # Mock version query for connection verification
    mock_version_result = MagicMock()
    mock_version_result.result_rows = [("23.8.1.2992",)]
    mock_client.query.return_value = mock_version_result

    # Mock command (DDL, DML) - returns None
    mock_client.command.return_value = None

    # Mock insert
    mock_client.insert.return_value = None

    # Mock close
    mock_client.close.return_value = None

    return mock_client


@pytest.fixture
def mock_clickhouse_connect(mock_clickhouse_client):
    """
    Patch clickhouse_connect.get_client to return the mock client.
    """
    with patch("clickhouse_connect.get_client", return_value=mock_clickhouse_client) as mock_get:
        yield mock_get, mock_clickhouse_client
