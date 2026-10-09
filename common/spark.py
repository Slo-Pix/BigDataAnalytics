"""Shared SparkSession factory and data paths."""
import os
import time

from pyspark.sql import SparkSession

from common.paths import CURATED, DATA, RAW, ROOT, STAGING, STREAM  # noqa: F401  (re-exported)

# PySpark converts collected timestamps to the Python process's local zone; keep everything UTC.
os.environ["TZ"] = "UTC"
time.tzset()


def get_spark(app: str = "torrent-bda", mem: str = "8g", packages: str | None = None) -> SparkSession:
    builder = SparkSession.builder
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return (
        builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", mem)
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
