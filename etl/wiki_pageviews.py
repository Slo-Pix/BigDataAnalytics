"""Wikimedia hourly pageview dumps -> staging/wiki_pageviews (Parquet, partitioned by date).

Input lines look like `en.m Inception 1234 0` = <domain_code> <page_title> <views> <unused>.
The hour comes from the file name (pageviews-YYYYMMDD-HH0000.gz). Each .gz is read by one task
(gzip is not splittable), so the 168 hourly files give 168 parallel tasks.

Output columns: project, page_title, hour (timestamp UTC), views, date (partition).

Usage: uv run python -m etl.wiki_pageviews
"""
import argparse
import time

from pyspark.sql import functions as F

from common.spark import RAW, STAGING, get_spark

SRC = RAW / "wikipedia" / "pageviews_hourly" / "pageviews-*.gz"
DST = STAGING / "wiki_pageviews"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=str(SRC))
    ap.add_argument("--dst", default=str(DST))
    args = ap.parse_args()

    spark = get_spark("etl-wiki-pageviews", mem="8g")
    t0 = time.time()

    parts = F.split("value", " ")
    df = (
        spark.read.text(args.src)
        .withColumn("file", F.input_file_name())
        .select(
            parts.getItem(0).alias("project"),
            parts.getItem(1).alias("page_title"),
            parts.getItem(2).try_cast("int").alias("views"),
            F.to_timestamp(F.regexp_extract("file", r"pageviews-(\d{8}-\d{2})", 1), "yyyyMMdd-HH").alias("hour"),
            F.size(parts).alias("n"),
        )
        .where((F.col("n") == 4) & F.col("views").isNotNull() & (F.col("page_title") != "-"))
        .drop("n")
        .withColumn("date", F.to_date("hour"))
    )

    df.write.mode("overwrite").partitionBy("date").parquet(args.dst)
    elapsed = time.time() - t0

    out = spark.read.parquet(args.dst)
    stats = out.agg(F.count("*").alias("rows"), F.sum("views").alias("views"),
                    F.countDistinct("hour").alias("hours")).first()
    print(f"wrote {args.dst}: {stats['rows']:,} rows, {stats['views']:,} views, {stats['hours']} hours in {elapsed/60:.1f} min")
    out.groupBy("project").agg(F.count("*").alias("rows")).orderBy(F.desc("rows")).show(8)
    spark.stop()


if __name__ == "__main__":
    main()
