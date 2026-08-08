import os
from datetime import datetime

# Spark / data paths
BASE_PATH = os.environ.get("DATA_BASE_PATH", "/opt/spark/data")
RAW_PATH = os.path.join(BASE_PATH, "raw")
LAKE_PATH = os.environ.get("LAKE_PATH", os.path.join(BASE_PATH, "lake", "imdb_enriched"))
STAGING_PATH = os.path.join(BASE_PATH, "staging")

# ClickHouse connection
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "imdb")

# ETL settings
CURRENT_YEAR = datetime.now().year
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100000"))

# Kaggle dataset
DATASET_SLUG = "ashirwadsangwan/imdb-dataset"
DATA_RAW_PATH = os.environ.get("DATA_RAW_PATH", "data/raw")

REQUIRED_FILES = [
    "title.basics.tsv",
    "title.ratings.tsv",
    "title.episode.tsv",
]

# Column order for ClickHouse inserts
COLUMN_ORDER = [
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
