BENCHMARK_QUERIES = [
    {
        "name": "Q1: Count by Title Type",
        "spark_sql": """
            SELECT title_type, COUNT(*) as cnt, AVG(average_rating) as avg_rating
            FROM imdb_enriched
            GROUP BY title_type
            ORDER BY cnt DESC
        """,
        "clickhouse_sql": """
            SELECT title_type, count() as cnt, avg(average_rating) as avg_rating
            FROM imdb.imdb_titles_enriched
            GROUP BY title_type
            ORDER BY cnt DESC
        """,
    },
    {
        "name": "Q2: Top 10 Rated Movies (>100K votes)",
        "spark_sql": """
            SELECT primary_title, average_rating, num_votes
            FROM imdb_enriched
            WHERE title_type = 'movie'
              AND num_votes > 100000
            ORDER BY average_rating DESC
            LIMIT 10
        """,
        "clickhouse_sql": """
            SELECT primary_title, average_rating, num_votes
            FROM imdb.imdb_titles_enriched
            WHERE title_type = 'movie'
              AND num_votes > 100000
            ORDER BY average_rating DESC
            LIMIT 10
        """,
    },
    {
        "name": "Q3: Average Rating by Genre (Exploded)",
        "spark_sql": """
            SELECT genre, COUNT(*) as cnt, AVG(average_rating) as avg_rating
            FROM (
                SELECT explode(split(genres, ',')) as genre, average_rating
                FROM imdb_enriched
                WHERE average_rating IS NOT NULL
            )
            GROUP BY genre
            ORDER BY cnt DESC
            LIMIT 15
        """,
        "clickhouse_sql": """
            SELECT
                arrayJoin(splitByString(',', genres)) AS genre,
                count() as cnt,
                avg(average_rating) as avg_rating
            FROM imdb.imdb_titles_enriched
            WHERE average_rating IS NOT NULL
            GROUP BY genre
            ORDER BY cnt DESC
            LIMIT 15
        """,
    },
    {
        "name": "Q4: Year-over-Year Title Production (2000-2024)",
        "spark_sql": """
            SELECT start_year, COUNT(*) as titles_produced,
                   AVG(average_rating) as avg_rating
            FROM imdb_enriched
            WHERE start_year BETWEEN 2000 AND 2024
              AND title_type IN ('movie', 'tvSeries')
            GROUP BY start_year
            ORDER BY start_year
        """,
        "clickhouse_sql": """
            SELECT start_year, count() as titles_produced,
                   avg(average_rating) as avg_rating
            FROM imdb.imdb_titles_enriched
            WHERE start_year BETWEEN 2000 AND 2024
              AND title_type IN ('movie', 'tvSeries')
            GROUP BY start_year
            ORDER BY start_year
        """,
    },
    {
        "name": "Q5: Episode Count per Series (Top 20)",
        "spark_sql": """
            SELECT p.primary_title, COUNT(*) as episode_count
            FROM imdb_enriched e
            JOIN imdb_enriched p ON e.parent_tconst = p.tconst
            WHERE e.title_type = 'tvEpisode'
              AND p.title_type = 'tvSeries'
            GROUP BY p.primary_title
            ORDER BY episode_count DESC
            LIMIT 20
        """,
        "clickhouse_sql": """
            SELECT p.primary_title, count() as episode_count
            FROM imdb.imdb_titles_enriched AS e
            INNER JOIN imdb.imdb_titles_enriched AS p
                ON e.parent_tconst = p.tconst
            WHERE e.title_type = 'tvEpisode'
              AND p.title_type = 'tvSeries'
            GROUP BY p.primary_title
            ORDER BY episode_count DESC
            LIMIT 20
        """,
    },
    {
        "name": "Q6: Runtime Distribution Histogram",
        "spark_sql": """
            SELECT
                CASE
                    WHEN runtime_minutes < 30 THEN 'Short'
                    WHEN runtime_minutes BETWEEN 30 AND 90 THEN 'Medium'
                    WHEN runtime_minutes BETWEEN 91 AND 150 THEN 'Standard'
                    ELSE 'Long'
                END as runtime_bucket,
                COUNT(*) as cnt,
                AVG(average_rating) as avg_rating
            FROM imdb_enriched
            WHERE runtime_minutes IS NOT NULL AND runtime_minutes > 0
              AND title_type = 'movie'
            GROUP BY runtime_bucket
            ORDER BY cnt DESC
        """,
        "clickhouse_sql": """
            SELECT
                multiIf(
                    runtime_minutes < 30, 'Short',
                    runtime_minutes BETWEEN 30 AND 90, 'Medium',
                    runtime_minutes BETWEEN 91 AND 150, 'Standard',
                    'Long'
                ) as runtime_bucket,
                count() as cnt,
                avg(average_rating) as avg_rating
            FROM imdb.imdb_titles_enriched
            WHERE runtime_minutes IS NOT NULL AND runtime_minutes > 0
              AND title_type = 'movie'
            GROUP BY runtime_bucket
            ORDER BY cnt DESC
        """,
    },
]
