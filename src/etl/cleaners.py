import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, FloatType

from src.config import CURRENT_YEAR

logger = logging.getLogger(__name__)


def clean_titles(df: DataFrame) -> DataFrame:
    logger.info("Cleaning titles data...")

    cleaned = (
        df.withColumnRenamed("titleType", "title_type")
        .withColumnRenamed("primaryTitle", "primary_title")
        .withColumnRenamed("originalTitle", "original_title")
        .withColumnRenamed("isAdult", "is_adult_str")
        .withColumnRenamed("startYear", "start_year_str")
        .withColumnRenamed("endYear", "end_year_str")
        .withColumnRenamed("runtimeMinutes", "runtime_minutes_str")
        .withColumn("is_adult", F.col("is_adult_str").cast(IntegerType()))
        .withColumn("start_year", F.col("start_year_str").cast(IntegerType()))
        .withColumn("end_year", F.col("end_year_str").cast(IntegerType()))
        .withColumn("runtime_minutes", F.col("runtime_minutes_str").cast(IntegerType()))
        .drop("is_adult_str", "start_year_str", "end_year_str", "runtime_minutes_str")
        .filter(F.col("tconst").isNotNull())
        .filter(F.col("tconst").startswith("tt"))
        .filter(
            (F.col("start_year").isNull())
            | ((F.col("start_year") >= 1874) & (F.col("start_year") <= CURRENT_YEAR + 5))
        )
        .dropDuplicates(["tconst"])
    )

    cleaned = cleaned.withColumn(
        "genres", F.when(F.col("genres").isNull(), "Unknown").otherwise(F.col("genres"))
    )

    row_count = cleaned.count()
    logger.info(f"  {row_count:,} rows after cleaning")
    return cleaned


def clean_ratings(df: DataFrame) -> DataFrame:
    logger.info("Cleaning ratings data...")

    cleaned = (
        df.withColumn("average_rating", F.col("averageRating").cast(FloatType()))
        .withColumn("num_votes", F.col("numVotes").cast(IntegerType()))
        .drop("averageRating", "numVotes")
        .filter(
            (F.col("average_rating").isNull())
            | ((F.col("average_rating") >= 0.0) & (F.col("average_rating") <= 10.0))
        )
        .filter((F.col("num_votes").isNull()) | (F.col("num_votes") > 0))
        .filter(F.col("tconst").isNotNull())
        .dropDuplicates(["tconst"])
    )

    row_count = cleaned.count()
    logger.info(f"  {row_count:,} rows after cleaning")
    return cleaned


def clean_episodes(df: DataFrame) -> DataFrame:
    logger.info("Cleaning episodes data...")

    cleaned = (
        df.withColumnRenamed("parentTconst", "parent_tconst")
        .withColumn("season_number", F.col("seasonNumber").cast(IntegerType()))
        .withColumn("episode_number", F.col("episodeNumber").cast(IntegerType()))
        .drop("seasonNumber", "episodeNumber")
        .filter(F.col("tconst").isNotNull())
        .dropDuplicates(["tconst"])
    )

    row_count = cleaned.count()
    logger.info(f"  {row_count:,} rows after cleaning")
    return cleaned
