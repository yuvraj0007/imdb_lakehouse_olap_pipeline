import time
import logging
from datetime import datetime

from pyspark.sql import SparkSession

from src.config import RAW_PATH, LAKE_PATH
from src.etl.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA, TITLE_EPISODE_SCHEMA
from src.etl.readers import read_tsv
from src.etl.cleaners import clean_titles, clean_ratings, clean_episodes
from src.etl.transforms import join_datasets, write_parquet, print_summary_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("IMDb_ETL_Pipeline")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    pipeline_start = time.time()
    logger.info(f"IMDb ETL Pipeline - {datetime.now().isoformat()}")
    logger.info(f"Raw: {RAW_PATH} | Output: {LAKE_PATH}")

    spark = create_spark_session()
    logger.info(f"Spark {spark.version} initialized")

    try:
        titles_raw = read_tsv(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA)
        ratings_raw = read_tsv(spark, "title.ratings.tsv", TITLE_RATINGS_SCHEMA)
        episodes_raw = read_tsv(spark, "title.episode.tsv", TITLE_EPISODE_SCHEMA)

        titles_clean = clean_titles(titles_raw)
        ratings_clean = clean_ratings(ratings_raw)
        episodes_clean = clean_episodes(episodes_raw)

        titles_clean.cache()
        ratings_clean.cache()
        episodes_clean.cache()

        enriched_df = join_datasets(titles_clean, ratings_clean, episodes_clean)
        print_summary_stats(enriched_df)
        write_parquet(enriched_df, LAKE_PATH)

        duration = time.time() - pipeline_start
        logger.info(f"ETL complete in {duration:.1f}s. Output: {LAKE_PATH}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.catalog.clearCache()
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
