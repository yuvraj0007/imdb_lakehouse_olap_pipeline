import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from src.config import RAW_PATH

logger = logging.getLogger(__name__)


def read_tsv(spark: SparkSession, filename: str, schema: StructType) -> DataFrame:
    """Read an IMDb TSV file. Uses \\N as null marker, tab delimiter, no quoting."""
    filepath = os.path.join(RAW_PATH, filename)
    logger.info(f"Reading: {filepath}")

    df = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .option("quote", "")
        .schema(schema)
        .csv(filepath)
    )

    row_count = df.count()
    logger.info(f"  Loaded {row_count:,} rows from {filename}")
    return df
