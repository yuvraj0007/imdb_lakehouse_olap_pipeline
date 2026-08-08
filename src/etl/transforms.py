import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def join_datasets(titles_df: DataFrame, ratings_df: DataFrame, episodes_df: DataFrame) -> DataFrame:
    logger.info("Joining datasets...")

    enriched = titles_df.join(ratings_df, on="tconst", how="left").join(episodes_df, on="tconst", how="left")

    enriched = enriched.select(
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
    )

    row_count = enriched.count()
    logger.info(f"  {row_count:,} rows in enriched dataset")
    return enriched


def write_parquet(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing partitioned Parquet to: {output_path}")

    (
        df.coalesce(4)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("title_type", "start_year")
        .parquet(output_path)
    )

    logger.info("  Parquet write complete")


def print_summary_stats(df: DataFrame) -> None:
    total = df.count()
    logger.info(f"Total rows: {total:,}")

    type_counts = df.groupBy("title_type").count().orderBy(F.desc("count")).collect()
    logger.info("Title Type Distribution:")
    for row in type_counts:
        pct = (row["count"] / total) * 100
        logger.info(f"  {row['title_type']:20s}: {row['count']:>10,} ({pct:.1f}%)")

    year_stats = (
        df.filter(F.col("start_year").isNotNull())
        .agg(
            F.min("start_year").alias("min_year"),
            F.max("start_year").alias("max_year"),
        )
        .collect()[0]
    )
    logger.info(f"Year range: {year_stats['min_year']} - {year_stats['max_year']}")

    rated_count = df.filter(F.col("average_rating").isNotNull()).count()
    logger.info(f"Titles with ratings: {rated_count:,} ({rated_count/total*100:.1f}%)")

    episode_count = df.filter(F.col("parent_tconst").isNotNull()).count()
    logger.info(f"Episodes with parent: {episode_count:,} ({episode_count/total*100:.1f}%)")
