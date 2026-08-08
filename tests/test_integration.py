"""
test_integration.py - Integration tests for the IMDb Lakehouse OLAP Pipeline.

Tests the full pipeline flow:
- TSV ingestion -> cleaning -> joining -> Parquet output
- Schema verification
- Data quality checks
- ClickHouse load (mocked)
"""

import os
import sys
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F

from src.etl.cleaners import clean_titles, clean_ratings, clean_episodes
from src.etl.transforms import join_datasets, write_parquet
from src.etl.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA, TITLE_EPISODE_SCHEMA

# Mock clickhouse_connect before importing olap modules
sys.modules.setdefault("clickhouse_connect", MagicMock())
from src.olap import loader as load_to_olap  # noqa: E402
from src.olap import schema as load_to_olap_schema  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Full Pipeline End-to-End Test
# ─────────────────────────────────────────────────────────────────────────────


class TestFullPipeline:
    """Integration tests running the full ETL pipeline."""

    @pytest.fixture
    def sample_tsv_dir(self, tmp_data_dir):
        """Create sample TSV files mimicking IMDb dataset format."""
        # title.basics.tsv
        basics_content = (
            "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
            "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short\n"
            "tt0000002\tshort\tLe clown et ses chiens\tLe clown et ses chiens\t0\t1892\t\\N\t5\tAnimation,Short\n"
            "tt0000003\tmovie\tThe Shawshank Redemption\tThe Shawshank Redemption\t0\t1994\t\\N\t142\tDrama\n"
            "tt0000004\ttvSeries\tBreaking Bad\tBreaking Bad\t0\t2008\t2013\t49\tCrime,Drama,Thriller\n"
            "tt0000005\ttvEpisode\tPilot\tPilot\t0\t2008\t\\N\t58\t\\N\n"
            "tt0000006\tmovie\tInception\tInception\t0\t2010\t\\N\t148\tAction,Sci-Fi,Thriller\n"
        )

        # title.ratings.tsv
        ratings_content = (
            "tconst\taverageRating\tnumVotes\n"
            "tt0000001\t5.7\t1973\n"
            "tt0000002\t5.8\t263\n"
            "tt0000003\t9.3\t2800000\n"
            "tt0000004\t9.5\t2000000\n"
            "tt0000005\t9.0\t150000\n"
            "tt0000006\t8.8\t2400000\n"
        )

        # title.episode.tsv
        episode_content = (
            "tconst\tparentTconst\tseasonNumber\tepisodeNumber\n"
            "tt0000005\ttt0000004\t1\t1\n"
        )

        with open(os.path.join(tmp_data_dir, "title.basics.tsv"), "w") as f:
            f.write(basics_content)
        with open(os.path.join(tmp_data_dir, "title.ratings.tsv"), "w") as f:
            f.write(ratings_content)
        with open(os.path.join(tmp_data_dir, "title.episode.tsv"), "w") as f:
            f.write(episode_content)

        return tmp_data_dir

    def test_full_etl_pipeline_end_to_end(self, spark, sample_tsv_dir, tmp_output_dir):
        """
        Full ETL pipeline: Read TSV -> Clean -> Join -> Write Parquet -> Verify.
        """
        # Step 1: Read raw TSV files
        titles_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_BASICS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.basics.tsv"))
        )

        ratings_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_RATINGS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.ratings.tsv"))
        )

        episodes_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_EPISODE_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.episode.tsv"))
        )

        # Step 2: Clean
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        # Step 3: Join
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        # Step 4: Write Parquet
        output_path = os.path.join(tmp_output_dir, "enriched")
        write_parquet(enriched, output_path)

        # Step 5: Verify
        read_back = spark.read.parquet(output_path)

        # Should have all 6 valid titles
        assert read_back.count() == 6

        # All titles should have ratings (since all 6 have matching ratings)
        rated = read_back.filter(F.col("average_rating").isNotNull())
        assert rated.count() == 6

        # Only tt0000005 is an episode
        episodes = read_back.filter(F.col("parent_tconst").isNotNull())
        assert episodes.count() == 1

    def test_parquet_schema_matches_expected(self, spark, sample_tsv_dir, tmp_output_dir):
        """Output Parquet should have the expected schema."""
        # Run pipeline
        titles_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_BASICS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.basics.tsv"))
        )
        ratings_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_RATINGS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.ratings.tsv"))
        )
        episodes_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_EPISODE_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.episode.tsv"))
        )

        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "schema_check")
        write_parquet(enriched, output_path)

        read_back = spark.read.parquet(output_path)

        # Expected columns (partition columns are at the end when reading back)
        expected_columns = {
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "is_adult",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "average_rating",
            "num_votes",
            "parent_tconst",
            "season_number",
            "episode_number",
        }
        actual_columns = set(read_back.columns)
        assert actual_columns == expected_columns

    def test_parquet_partitioning_structure(self, spark, sample_tsv_dir, tmp_output_dir):
        """Parquet output should be partitioned by title_type and start_year."""
        # Run pipeline
        titles_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_BASICS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.basics.tsv"))
        )
        ratings_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_RATINGS_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.ratings.tsv"))
        )
        episodes_raw = (
            spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "\\N")
            .option("quote", "")
            .schema(TITLE_EPISODE_SCHEMA)
            .csv(os.path.join(sample_tsv_dir, "title.episode.tsv"))
        )

        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "partition_check")
        write_parquet(enriched, output_path)

        # Verify partition directory structure exists
        title_type_dirs = [
            d for d in os.listdir(output_path)
            if d.startswith("title_type=") and os.path.isdir(os.path.join(output_path, d))
        ]
        assert len(title_type_dirs) > 0, "No title_type partition dirs found"

        # Check for start_year subdirectories
        first_type_dir = os.path.join(output_path, title_type_dirs[0])
        start_year_dirs = [
            d for d in os.listdir(first_type_dir)
            if d.startswith("start_year=") and os.path.isdir(os.path.join(first_type_dir, d))
        ]
        assert len(start_year_dirs) > 0, "No start_year partition dirs found"


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestDataQuality:
    """Data quality validation tests."""

    def test_data_quality_no_null_primary_keys(self, spark, titles_raw, ratings_raw, episodes_raw):
        """After ETL, no null tconst (primary key) values should exist."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        null_pk_count = enriched.filter(F.col("tconst").isNull()).count()
        assert null_pk_count == 0, f"Found {null_pk_count} rows with null primary key"

    def test_data_quality_ratings_in_valid_range(self, spark, titles_raw, ratings_raw, episodes_raw):
        """All non-null ratings should be between 0 and 10."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        # Check all non-null ratings are in [0, 10]
        invalid_ratings = enriched.filter(
            (F.col("average_rating").isNotNull())
            & (
                (F.col("average_rating") < 0.0) | (F.col("average_rating") > 10.0)
            )
        ).count()
        assert invalid_ratings == 0, f"Found {invalid_ratings} ratings outside [0, 10]"

    def test_data_quality_no_duplicate_tconst(self, spark, titles_raw, ratings_raw, episodes_raw):
        """Enriched dataset should have no duplicate tconst values."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        total = enriched.count()
        distinct = enriched.select("tconst").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate tconst rows"

    def test_data_quality_valid_years(self, spark, titles_raw, ratings_raw, episodes_raw):
        """All non-null start_years should be in a valid range."""
        from src.config import CURRENT_YEAR

        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        invalid_years = enriched.filter(
            (F.col("start_year").isNotNull())
            & (
                (F.col("start_year") < 1874) | (F.col("start_year") > CURRENT_YEAR + 5)
            )
        ).count()
        assert invalid_years == 0, f"Found {invalid_years} rows with invalid years"

    def test_data_quality_genres_not_null(self, spark, titles_raw, ratings_raw, episodes_raw):
        """All genres should be non-null (replaced with 'Unknown')."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        null_genres = enriched.filter(F.col("genres").isNull()).count()
        assert null_genres == 0, f"Found {null_genres} rows with null genres"


# ─────────────────────────────────────────────────────────────────────────────
# ClickHouse Load Tests (Mocked)
# ─────────────────────────────────────────────────────────────────────────────


class TestClickHouseLoad:
    """Tests for the ClickHouse loading pipeline using mocks."""

    @pytest.fixture
    def sample_parquet_dir(self, spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
        """Create sample Parquet files for ClickHouse load testing."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "lake_data")
        write_parquet(enriched, output_path)
        return output_path

    def test_clickhouse_load_inserts_correct_count(self, mock_clickhouse_client, sample_parquet_dir):
        """ClickHouse load should insert all rows from Parquet files."""
        # Discover parquet files
        parquet_files = load_to_olap.discover_files(sample_parquet_dir)
        assert len(parquet_files) > 0

        # Load into mock ClickHouse
        total_rows = load_to_olap.load_parquet_to_clickhouse(mock_clickhouse_client, parquet_files)

        # Should have inserted rows
        assert total_rows > 0

        # Verify insert was called
        assert mock_clickhouse_client.insert.called

    def test_clickhouse_schema_creation(self, mock_clickhouse_client):
        """ensure_schema should create the database and tables."""
        load_to_olap_schema.ensure_schema(mock_clickhouse_client)

        # Should have called command multiple times for DDL
        assert mock_clickhouse_client.command.call_count >= 3

        # Check that CREATE DATABASE was called
        calls = [str(c) for c in mock_clickhouse_client.command.call_args_list]
        ddl_calls = " ".join(calls)
        assert "CREATE DATABASE" in ddl_calls
        assert "CREATE TABLE" in ddl_calls

    def test_clickhouse_truncate_before_load(self, mock_clickhouse_client, sample_parquet_dir):
        """Load should truncate existing data before inserting."""
        parquet_files = load_to_olap.discover_files(sample_parquet_dir)
        load_to_olap.load_parquet_to_clickhouse(mock_clickhouse_client, parquet_files)

        # Verify truncate was called
        calls = [str(c) for c in mock_clickhouse_client.command.call_args_list]
        truncate_calls = [c for c in calls if "TRUNCATE" in c]
        assert len(truncate_calls) > 0

    def test_clickhouse_discover_parquet_files(self, sample_parquet_dir):
        """discover_parquet_files should find all .parquet files recursively."""
        files = load_to_olap.discover_files(sample_parquet_dir)

        # All files should end with .parquet
        for f in files:
            assert f.endswith(".parquet")

        # Should find at least one file
        assert len(files) > 0
