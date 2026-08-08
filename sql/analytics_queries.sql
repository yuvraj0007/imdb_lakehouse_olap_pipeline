-- ═══════════════════════════════════════════════════════════════════════════
-- IMDb Lakehouse OLAP Pipeline - Analytics Queries
-- ═══════════════════════════════════════════════════════════════════════════
-- These queries demonstrate ClickHouse's OLAP performance for various
-- analytical workloads relevant to Teleparty's viewership analytics.
--
-- Expected performance: Sub-second (<100ms) on the full 10M+ row dataset
-- ═══════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 1: Content Distribution by Type
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Understanding the composition of the content library
-- Teleparty parallel: Distribution of watch parties by content type

SELECT
    title_type,
    count() AS title_count,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct_of_total,
    avg(average_rating) AS avg_rating,
    sum(num_votes) AS total_engagement
FROM imdb.imdb_titles_enriched
GROUP BY title_type
ORDER BY title_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 2: Top 20 Highest-Rated Movies (>50K votes)
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Identifying popular, high-quality content
-- Teleparty parallel: Most popular titles for watch parties

SELECT
    primary_title,
    start_year,
    genres,
    average_rating,
    num_votes,
    runtime_minutes
FROM imdb.imdb_titles_enriched
WHERE title_type = 'movie'
  AND num_votes > 50000
  AND average_rating IS NOT NULL
ORDER BY average_rating DESC, num_votes DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 3: Rating Trends Over Time (by decade)
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Understanding how content quality/perception changes over time
-- Teleparty parallel: Viewership trends over time

SELECT
    toUInt16(start_year / 10) * 10 AS decade,
    title_type,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating,
    sum(num_votes) AS total_votes,
    round(avg(runtime_minutes), 0) AS avg_runtime
FROM imdb.imdb_titles_enriched
WHERE start_year IS NOT NULL
  AND average_rating IS NOT NULL
  AND start_year >= 1920
GROUP BY decade, title_type
HAVING title_count > 100
ORDER BY decade DESC, title_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 4: Genre Popularity Analysis
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Which genres attract the most engagement?
-- Teleparty parallel: Which genres drive the most watch parties?

SELECT
    arrayJoin(splitByString(',', genres)) AS genre,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating,
    sum(num_votes) AS total_engagement,
    round(avg(runtime_minutes), 0) AS avg_runtime
FROM imdb.imdb_titles_enriched
WHERE title_type IN ('movie', 'tvSeries')
  AND average_rating IS NOT NULL
  AND genres != 'Unknown'
GROUP BY genre
ORDER BY total_engagement DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 5: TV Series with Most Episodes
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Identifying long-running series (high engagement potential)
-- Teleparty parallel: Series with most repeat watch parties

SELECT
    parent.primary_title AS series_title,
    parent.start_year,
    parent.genres,
    parent.average_rating AS series_rating,
    count() AS episode_count,
    max(ep.season_number) AS total_seasons,
    avg(ep.average_rating) AS avg_episode_rating
FROM imdb.imdb_titles_enriched AS ep
INNER JOIN imdb.imdb_titles_enriched AS parent
    ON ep.parent_tconst = parent.tconst
WHERE ep.title_type = 'tvEpisode'
  AND parent.title_type = 'tvSeries'
GROUP BY parent.primary_title, parent.start_year, parent.genres, parent.average_rating
HAVING episode_count > 100
ORDER BY episode_count DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 6: Year-over-Year Growth in Content Production
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Content supply growth analysis
-- Teleparty parallel: Growth in available content for watch parties

SELECT
    start_year,
    count() AS titles_released,
    count() - lagInFrame(count()) OVER (ORDER BY start_year) AS yoy_change,
    round(
        (count() - lagInFrame(count()) OVER (ORDER BY start_year)) * 100.0
        / lagInFrame(count()) OVER (ORDER BY start_year), 1
    ) AS yoy_pct_change
FROM imdb.imdb_titles_enriched
WHERE start_year IS NOT NULL
  AND start_year BETWEEN 2000 AND 2024
  AND title_type IN ('movie', 'tvSeries', 'tvMovie')
GROUP BY start_year
ORDER BY start_year;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 7: Runtime Distribution Analysis
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Understanding content length patterns
-- Teleparty parallel: Watch party session length analysis

SELECT
    title_type,
    CASE
        WHEN runtime_minutes < 30 THEN 'Short (<30min)'
        WHEN runtime_minutes BETWEEN 30 AND 60 THEN 'Medium (30-60min)'
        WHEN runtime_minutes BETWEEN 61 AND 120 THEN 'Standard (1-2hr)'
        WHEN runtime_minutes BETWEEN 121 AND 180 THEN 'Long (2-3hr)'
        ELSE 'Very Long (>3hr)'
    END AS runtime_bucket,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating,
    round(avg(num_votes), 0) AS avg_votes
FROM imdb.imdb_titles_enriched
WHERE runtime_minutes IS NOT NULL
  AND runtime_minutes > 0
  AND runtime_minutes < 1000
  AND average_rating IS NOT NULL
GROUP BY title_type, runtime_bucket
ORDER BY title_type, title_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 8: Underrated Gems (High Rating, Low Vote Count)
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Content discovery for recommendations
-- Teleparty parallel: Suggest lesser-known titles for watch parties

SELECT
    primary_title,
    start_year,
    genres,
    average_rating,
    num_votes,
    title_type
FROM imdb.imdb_titles_enriched
WHERE average_rating >= 8.0
  AND num_votes BETWEEN 1000 AND 10000
  AND title_type IN ('movie', 'tvSeries')
  AND start_year >= 2015
ORDER BY average_rating DESC, num_votes DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 9: Seasonal Episode Rating Patterns
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Do TV series ratings decline in later seasons?
-- Teleparty parallel: Engagement drop-off analysis across seasons

SELECT
    season_number,
    count() AS episode_count,
    round(avg(average_rating), 2) AS avg_rating,
    round(median(average_rating), 2) AS median_rating,
    min(average_rating) AS min_rating,
    max(average_rating) AS max_rating
FROM imdb.imdb_titles_enriched
WHERE title_type = 'tvEpisode'
  AND season_number IS NOT NULL
  AND season_number BETWEEN 1 AND 20
  AND average_rating IS NOT NULL
GROUP BY season_number
ORDER BY season_number;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 10: Content Production Heatmap (Type × Decade)
-- ─────────────────────────────────────────────────────────────────────────────
-- Use case: Multi-dimensional content supply analysis
-- Teleparty parallel: Platform content availability matrix

SELECT
    title_type,
    toUInt16(start_year / 10) * 10 AS decade,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating
FROM imdb.imdb_titles_enriched
WHERE start_year IS NOT NULL
  AND start_year >= 1950
GROUP BY title_type, decade
ORDER BY title_type, decade;
