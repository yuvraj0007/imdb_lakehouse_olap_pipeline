from pyspark.sql.types import StructType, StructField, StringType

TITLE_BASICS_SCHEMA = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("titleType", StringType(), nullable=True),
    StructField("primaryTitle", StringType(), nullable=True),
    StructField("originalTitle", StringType(), nullable=True),
    StructField("isAdult", StringType(), nullable=True),
    StructField("startYear", StringType(), nullable=True),
    StructField("endYear", StringType(), nullable=True),
    StructField("runtimeMinutes", StringType(), nullable=True),
    StructField("genres", StringType(), nullable=True),
])

TITLE_RATINGS_SCHEMA = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("averageRating", StringType(), nullable=True),
    StructField("numVotes", StringType(), nullable=True),
])

TITLE_EPISODE_SCHEMA = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("parentTconst", StringType(), nullable=True),
    StructField("seasonNumber", StringType(), nullable=True),
    StructField("episodeNumber", StringType(), nullable=True),
])
