-- ═══════════════════════════════════════════════════════════════════════════
-- IMDb Lakehouse OLAP Pipeline - ClickHouse DDL
-- ═══════════════════════════════════════════════════════════════════════════
-- This file defines the ClickHouse schema for the IMDb analytics pipeline.
-- It is automatically executed on container startup via docker-entrypoint-initdb.d.
--
-- Engine: MergeTree (columnar, sorted, partitioned)
-- Compression: LZ4 (default, optimal for analytics)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- Database
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS imdb;

-- ─────────────────────────────────────────────────────────────────────────────
-- Main Fact Table: imdb_titles_enriched
-- ─────────────────────────────────────────────────────────────────────────────
-- Contains all IMDb titles enriched with ratings and episode metadata.
-- This is the primary table for analytical queries.
--
-- Design Decisions:
-- • LowCardinality(String) for title_type: ~15 distinct values → dictionary encoding
-- • Nullable types for optional fields (ratings, episodes)
-- • PARTITION BY decade: reduces partition count while enabling time-range pruning
--   (ClickHouse recommends <1000 active partitions)
-- • ORDER BY (title_type, start_year, tconst): optimizes common filter patterns
-- • index_granularity = 8192: default, one index entry per 8192 rows
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS imdb.imdb_titles_enriched
(
    -- Primary identifier
    tconst          String              COMMENT 'IMDb title identifier (e.g., tt0000001)',

    -- Title metadata
    title_type      LowCardinality(String) COMMENT 'Type: movie, tvSeries, tvEpisode, short, etc.',
    primary_title   String              COMMENT 'Primary display title',
    original_title  String              COMMENT 'Original language title',
    is_adult        Nullable(UInt8)     COMMENT 'Adult content flag (0/1)',
    start_year      Nullable(UInt16)    COMMENT 'Release/start year',
    end_year        Nullable(UInt16)    COMMENT 'End year (for TV series)',
    runtime_minutes Nullable(UInt16)    COMMENT 'Runtime in minutes',
    genres          String DEFAULT 'Unknown' COMMENT 'Comma-separated genre list',

    -- Rating data (from title.ratings)
    average_rating  Nullable(Float32)   COMMENT 'Weighted average rating (0.0-10.0)',
    num_votes       Nullable(UInt32)    COMMENT 'Number of votes',

    -- Episode data (from title.episode)
    parent_tconst   Nullable(String)    COMMENT 'Parent series tconst (for episodes)',
    season_number   Nullable(UInt16)    COMMENT 'Season number',
    episode_number  Nullable(UInt16)    COMMENT 'Episode number within season'
)
ENGINE = MergeTree()
PARTITION BY toUInt16(coalesce(start_year, 0) / 10) * 10
ORDER BY (title_type, coalesce(start_year, 0), tconst)
SETTINGS index_granularity = 8192
COMMENT 'IMDb titles enriched with ratings and episode data. Partitioned by decade.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Aggregation Table: imdb_ratings_by_type_year
-- ─────────────────────────────────────────────────────────────────────────────
-- Pre-aggregated ratings statistics by title type and year.
-- Uses SummingMergeTree for efficient incremental aggregation.
--
-- Use case: Dashboard-level queries that need instant response times
-- for "average rating by genre over time" type visualizations.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS imdb.imdb_ratings_by_type_year
(
    title_type      LowCardinality(String) COMMENT 'Title type category',
    start_year      UInt16              COMMENT 'Release year',
    avg_rating      Float64             COMMENT 'Average rating for this type/year',
    total_votes     UInt64              COMMENT 'Sum of all votes',
    title_count     UInt64              COMMENT 'Number of titles'
)
ENGINE = SummingMergeTree()
ORDER BY (title_type, start_year)
COMMENT 'Pre-aggregated ratings by title type and year for fast dashboard queries.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Top Rated Table: imdb_top_rated
-- ─────────────────────────────────────────────────────────────────────────────
-- Materialized view storing top-rated titles with sufficient votes.
-- Useful for "best of" queries without scanning the full table.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS imdb.imdb_top_rated
(
    tconst          String,
    title_type      LowCardinality(String),
    primary_title   String,
    start_year      Nullable(UInt16),
    genres          String,
    average_rating  Float32,
    num_votes       UInt32
)
ENGINE = MergeTree()
ORDER BY (title_type, average_rating DESC, num_votes DESC)
COMMENT 'Top-rated titles with significant vote counts for fast leaderboard queries.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Genre Exploded Table: imdb_titles_by_genre
-- ─────────────────────────────────────────────────────────────────────────────
-- Since IMDb stores genres as comma-separated strings, this table explodes
-- them into individual rows for efficient genre-based analytics.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS imdb.imdb_titles_by_genre
(
    tconst          String,
    title_type      LowCardinality(String),
    primary_title   String,
    start_year      Nullable(UInt16),
    genre           LowCardinality(String) COMMENT 'Individual genre (exploded from comma-separated)',
    average_rating  Nullable(Float32),
    num_votes       Nullable(UInt32),
    runtime_minutes Nullable(UInt16)
)
ENGINE = MergeTree()
PARTITION BY genre
ORDER BY (genre, title_type, coalesce(start_year, 0))
COMMENT 'Titles exploded by genre for efficient genre-based analytics.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Indexes (Skipping Indexes for Secondary Filtering)
-- ─────────────────────────────────────────────────────────────────────────────
-- ClickHouse MergeTree uses sparse primary indexes by default.
-- These additional skipping indexes improve performance for specific query patterns.
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE imdb.imdb_titles_enriched
    ADD INDEX IF NOT EXISTS idx_rating average_rating TYPE minmax GRANULARITY 4;

ALTER TABLE imdb.imdb_titles_enriched
    ADD INDEX IF NOT EXISTS idx_votes num_votes TYPE minmax GRANULARITY 4;

ALTER TABLE imdb.imdb_titles_enriched
    ADD INDEX IF NOT EXISTS idx_title primary_title TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4;

ALTER TABLE imdb.imdb_titles_enriched
    ADD INDEX IF NOT EXISTS idx_genres genres TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4;


-- ─────────────────────────────────────────────────────────────────────────────
-- Helper Views
-- ─────────────────────────────────────────────────────────────────────────────

-- View: Movies only (convenience)
CREATE VIEW IF NOT EXISTS imdb.v_movies AS
SELECT *
FROM imdb.imdb_titles_enriched
WHERE title_type = 'movie';

-- View: TV Series only
CREATE VIEW IF NOT EXISTS imdb.v_tv_series AS
SELECT *
FROM imdb.imdb_titles_enriched
WHERE title_type = 'tvSeries';

-- View: Recent popular titles (last 5 years, >1000 votes)
CREATE VIEW IF NOT EXISTS imdb.v_recent_popular AS
SELECT *
FROM imdb.imdb_titles_enriched
WHERE start_year >= (toYear(now()) - 5)
  AND num_votes > 1000;
