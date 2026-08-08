"""
test_etl_job.py - Unit tests for the ETL pipeline functions.

Tests each transformation function in isolation:
- clean_titles
- clean_ratings
- clean_episodes
- join_datasets
- write_parquet
"""

import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

from src.etl.cleaners import clean_titles, clean_ratings, clean_episodes
from src.etl.transforms import join_datasets, write_parquet


# ─────────────────────────────────────────────────────────────────────────────
# clean_titles tests
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanTitles:
    """Tests for the clean_titles function."""

    def test_clean_titles_removes_null_tconst(self, spark, titles_raw):
        """Rows with null tconst should be removed."""
        result = clean_titles(titles_raw)
        null_tconst_count = result.filter(F.col("tconst").isNull()).count()
        assert null_tconst_count == 0

    def test_clean_titles_casts_year_correctly(self, spark, titles_raw):
        """startYear string should be cast to IntegerType start_year column."""
        result = clean_titles(titles_raw)

        # Verify column exists and has correct type
        start_year_field = result.schema["start_year"]
        assert start_year_field.dataType == IntegerType()

        # Verify a known value
        row = result.filter(F.col("tconst") == "tt0000001").first()
        assert row["start_year"] == 1894

    def test_clean_titles_filters_invalid_years(self, spark, titles_raw):
        """Years outside valid range (1874 to current_year+5) should be filtered."""
        result = clean_titles(titles_raw)

        # tt0000006 has year 9999 (too far future) - should be filtered
        assert result.filter(F.col("tconst") == "tt0000006").count() == 0

        # tt0000007 has year 1800 (before 1874) - should be filtered
        assert result.filter(F.col("tconst") == "tt0000007").count() == 0

        # tt0000001 has year 1894 (valid) - should remain
        assert result.filter(F.col("tconst") == "tt0000001").count() == 1

    def test_clean_titles_replaces_null_genres(self, spark, titles_raw):
        """Null genres should be replaced with 'Unknown'."""
        result = clean_titles(titles_raw)

        # tt0000005 had null genres
        row = result.filter(F.col("tconst") == "tt0000005").first()
        assert row["genres"] == "Unknown"

        # No null genres should exist
        null_genres_count = result.filter(F.col("genres").isNull()).count()
        assert null_genres_count == 0

    def test_clean_titles_deduplicates(self, spark, titles_raw):
        """Duplicate tconst rows should be deduplicated to keep one."""
        result = clean_titles(titles_raw)

        # tt0000001 appears twice in raw data
        count = result.filter(F.col("tconst") == "tt0000001").count()
        assert count == 1

    def test_clean_titles_filters_non_tt_prefix(self, spark, titles_raw):
        """Rows with tconst not starting with 'tt' should be filtered."""
        result = clean_titles(titles_raw)

        # nm0000001 starts with 'nm', not 'tt'
        assert result.filter(F.col("tconst") == "nm0000001").count() == 0

    def test_clean_titles_renames_columns(self, spark, titles_raw):
        """Columns should be renamed to snake_case."""
        result = clean_titles(titles_raw)

        column_names = result.columns
        assert "title_type" in column_names
        assert "primary_title" in column_names
        assert "original_title" in column_names
        assert "start_year" in column_names
        assert "end_year" in column_names
        assert "runtime_minutes" in column_names
        assert "is_adult" in column_names

        # Original camelCase names should not exist
        assert "titleType" not in column_names
        assert "primaryTitle" not in column_names
        assert "startYear" not in column_names


# ─────────────────────────────────────────────────────────────────────────────
# clean_ratings tests
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanRatings:
    """Tests for the clean_ratings function."""

    def test_clean_ratings_casts_types(self, spark, ratings_raw):
        """averageRating -> float, numVotes -> int with correct types."""
        result = clean_ratings(ratings_raw)

        # Check column types
        avg_rating_field = result.schema["average_rating"]
        num_votes_field = result.schema["num_votes"]
        assert avg_rating_field.dataType == FloatType()
        assert num_votes_field.dataType == IntegerType()

        # Verify values
        row = result.filter(F.col("tconst") == "tt0000001").first()
        assert abs(row["average_rating"] - 7.4) < 0.01
        assert row["num_votes"] == 2145

    def test_clean_ratings_filters_invalid_ratings(self, spark, ratings_raw):
        """Ratings > 10 or < 0 should be filtered out."""
        result = clean_ratings(ratings_raw)

        # tt0000010 has rating 11.5 - should be filtered
        assert result.filter(F.col("tconst") == "tt0000010").count() == 0

        # tt0000011 has rating -1.0 - should be filtered
        assert result.filter(F.col("tconst") == "tt0000011").count() == 0

    def test_clean_ratings_filters_zero_votes(self, spark, ratings_raw):
        """Rows with 0 votes should be filtered out."""
        result = clean_ratings(ratings_raw)

        # tt0000012 has 0 votes - should be filtered
        assert result.filter(F.col("tconst") == "tt0000012").count() == 0

    def test_clean_ratings_removes_null_tconst(self, spark, ratings_raw):
        """Rows with null tconst should be removed."""
        result = clean_ratings(ratings_raw)
        null_count = result.filter(F.col("tconst").isNull()).count()
        assert null_count == 0

    def test_clean_ratings_keeps_valid_rows(self, spark, ratings_raw):
        """Valid ratings rows should be preserved."""
        result = clean_ratings(ratings_raw)

        # Should keep tt0000001 through tt0000005 (5 valid rows)
        assert result.count() == 5

        # Verify specific valid rows exist
        assert result.filter(F.col("tconst") == "tt0000004").count() == 1
        row = result.filter(F.col("tconst") == "tt0000004").first()
        assert abs(row["average_rating"] - 9.5) < 0.01
        assert row["num_votes"] == 1800000


# ─────────────────────────────────────────────────────────────────────────────
# clean_episodes tests
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanEpisodes:
    """Tests for the clean_episodes function."""

    def test_clean_episodes_renames_columns(self, spark, episodes_raw):
        """Columns should be renamed to snake_case."""
        result = clean_episodes(episodes_raw)

        column_names = result.columns
        assert "parent_tconst" in column_names
        assert "season_number" in column_names
        assert "episode_number" in column_names

        # Original names should not exist
        assert "parentTconst" not in column_names
        assert "seasonNumber" not in column_names
        assert "episodeNumber" not in column_names

    def test_clean_episodes_casts_numeric_types(self, spark, episodes_raw):
        """seasonNumber and episodeNumber should be cast to IntegerType."""
        result = clean_episodes(episodes_raw)

        season_field = result.schema["season_number"]
        episode_field = result.schema["episode_number"]
        assert season_field.dataType == IntegerType()
        assert episode_field.dataType == IntegerType()

        # Verify values
        row = result.filter(F.col("tconst") == "tt0000005").first()
        assert row["season_number"] == 1
        assert row["episode_number"] == 1

    def test_clean_episodes_removes_null_tconst(self, spark, episodes_raw):
        """Rows with null tconst should be removed."""
        result = clean_episodes(episodes_raw)
        null_count = result.filter(F.col("tconst").isNull()).count()
        assert null_count == 0

    def test_clean_episodes_keeps_valid_rows(self, spark, episodes_raw):
        """Valid episode rows should be preserved."""
        result = clean_episodes(episodes_raw)

        # 5 total rows - 1 null tconst = 4 valid rows
        assert result.count() == 4


# ─────────────────────────────────────────────────────────────────────────────
# join_datasets tests
# ─────────────────────────────────────────────────────────────────────────────


class TestJoinDatasets:
    """Tests for the join_datasets function."""

    def test_join_datasets_left_join_preserves_all_titles(self, spark, titles_raw, ratings_raw, episodes_raw):
        """LEFT JOIN should preserve all title rows."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        result = join_datasets(titles_clean, ratings_clean, episodes_clean)

        # All title rows should be preserved (LEFT JOIN)
        assert result.count() == titles_clean.count()

    def test_join_datasets_adds_rating_columns(self, spark, titles_raw, ratings_raw, episodes_raw):
        """Joined result should include average_rating and num_votes columns."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        result = join_datasets(titles_clean, ratings_clean, episodes_clean)

        assert "average_rating" in result.columns
        assert "num_votes" in result.columns

        # Verify a title with a known rating
        row = result.filter(F.col("tconst") == "tt0000001").first()
        assert row["average_rating"] is not None
        assert abs(row["average_rating"] - 7.4) < 0.01

    def test_join_datasets_adds_episode_columns(self, spark, titles_raw, ratings_raw, episodes_raw):
        """Joined result should include parent_tconst, season_number, episode_number."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        result = join_datasets(titles_clean, ratings_clean, episodes_clean)

        assert "parent_tconst" in result.columns
        assert "season_number" in result.columns
        assert "episode_number" in result.columns

        # Verify a known episode
        row = result.filter(F.col("tconst") == "tt0000005").first()
        assert row["parent_tconst"] == "tt0000004"
        assert row["season_number"] == 1
        assert row["episode_number"] == 1

    def test_join_datasets_null_for_unmatched(self, spark, titles_raw, ratings_raw, episodes_raw):
        """Titles without ratings/episodes should have null in those columns."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        result = join_datasets(titles_clean, ratings_clean, episodes_clean)

        # tt0000003 is a movie, not an episode -> episode fields should be null
        row = result.filter(F.col("tconst") == "tt0000003").first()
        assert row["parent_tconst"] is None
        assert row["season_number"] is None

    def test_join_datasets_output_columns(self, spark, titles_raw, ratings_raw, episodes_raw):
        """Joined result should have exactly the expected columns in order."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        result = join_datasets(titles_clean, ratings_clean, episodes_clean)

        expected_columns = [
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
        ]
        assert result.columns == expected_columns


# ─────────────────────────────────────────────────────────────────────────────
# write_parquet tests
# ─────────────────────────────────────────────────────────────────────────────


class TestWritePartitionedParquet:
    """Tests for the write_parquet function."""

    def test_write_parquet_creates_files(self, spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
        """Parquet files should be created in the output directory."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "output")
        write_parquet(enriched, output_path)

        # Verify parquet files exist
        parquet_files = []
        for root, dirs, files in os.walk(output_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

        assert len(parquet_files) > 0

    def test_write_parquet_uses_snappy(self, spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
        """Parquet files should use snappy compression."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "output")
        write_parquet(enriched, output_path)

        # Read back and check metadata
        import pyarrow.parquet as pq

        parquet_files = []
        for root, dirs, files in os.walk(output_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

        assert len(parquet_files) > 0

        # Check compression of first file
        pf = pq.ParquetFile(parquet_files[0])
        metadata = pf.metadata
        # Check row group column compression
        row_group = metadata.row_group(0)
        col_meta = row_group.column(0)
        assert col_meta.compression.upper() == "SNAPPY"

    def test_write_parquet_partitions_correctly(self, spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
        """Output should be partitioned by title_type and start_year."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "output")
        write_parquet(enriched, output_path)

        # Check for partition directory structure
        # Should have title_type=X/start_year=Y/ subdirectories
        found_title_type_partition = False
        found_start_year_partition = False

        for root, dirs, files in os.walk(output_path):
            for d in dirs:
                if d.startswith("title_type="):
                    found_title_type_partition = True
                if d.startswith("start_year="):
                    found_start_year_partition = True

        assert found_title_type_partition, "Expected title_type partition directories"
        assert found_start_year_partition, "Expected start_year partition directories"

    def test_write_parquet_readable(self, spark, titles_raw, ratings_raw, episodes_raw, tmp_output_dir):
        """Written Parquet files should be readable by Spark."""
        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)
        enriched = join_datasets(titles_clean, ratings_clean, episodes_clean)

        output_path = os.path.join(tmp_output_dir, "output")
        write_parquet(enriched, output_path)

        # Read back with Spark
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == enriched.count()
