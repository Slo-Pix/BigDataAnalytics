"""Shared SparkSession factory and data paths."""
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW, STAGING, CURATED, STREAM = (DATA / d for d in ("raw", "staging", "curated", "stream"))


def get_spark(app: str = "torrent-bda", mem: str = "8g") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", mem)
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
