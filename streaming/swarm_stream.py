"""Speed layer: Kafka `tracker.scrapes` -> Spark Structured Streaming -> Parquet + Postgres.

Two queries share one Kafka source:
  1. lake    : raw rows appended to dataset/stream/swarm (Parquet, partitioned by date)
  2. serving : per-batch aggregates upserted into Postgres for Grafana
               (swarm_minute time series + swarm_latest snapshot). Each batch_id is recorded
               in stream_batches inside the same transaction, so replays after a crash are skipped.

Usage:
    uv run python -m streaming.swarm_stream                 # run forever, 1-minute micro-batches
    uv run python -m streaming.swarm_stream --once          # process what is in Kafka, then stop
"""
import argparse
import io

import psycopg
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common.paths import STREAM
from common.spark import get_spark

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"

SCHEMA = StructType([
    StructField("infohash", StringType()),
    StructField("tracker", StringType()),
    StructField("seeders", IntegerType()),
    StructField("completed", IntegerType()),
    StructField("leechers", IntegerType()),
    StructField("scraped_at", StringType()),
])


def parse(raw: DataFrame) -> DataFrame:
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("r"))
        .select("r.*")
        .withColumn("scraped_at", F.to_timestamp("scraped_at"))
        .where(F.col("infohash").isNotNull() & F.col("scraped_at").isNotNull())
        .withColumn("date", F.to_date("scraped_at"))
    )


def make_serving_writer(pg_dsn: str, query_name: str):
    def write(batch: DataFrame, batch_id: int):
        minute = (
            batch.groupBy(F.date_trunc("minute", "scraped_at").alias("minute"), "tracker")
            .agg(F.count("*").alias("rows"), F.sum("seeders").alias("s"), F.sum("leechers").alias("l"))
            .collect()
        )
        if not minute:
            return
        # newest observation per (infohash, tracker) within this batch
        latest = (
            batch.groupBy("infohash", "tracker")
            .agg(F.max_by(F.struct("seeders", "completed", "leechers", "scraped_at"), "scraped_at").alias("x"))
            .select("infohash", "tracker", "x.*")
            .toPandas()
        )
        n_rows = sum(r["rows"] for r in minute)
        with psycopg.connect(pg_dsn) as con, con.cursor() as cur:
            cur.execute(
                "INSERT INTO stream_batches (query, batch_id, rows) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (query_name, batch_id, n_rows),
            )
            if cur.rowcount == 0:
                return  # batch already applied before a restart
            cur.executemany(
                """INSERT INTO swarm_minute VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (minute, tracker) DO UPDATE SET
                     rows = swarm_minute.rows + EXCLUDED.rows,
                     total_seeders = swarm_minute.total_seeders + EXCLUDED.total_seeders,
                     total_leechers = swarm_minute.total_leechers + EXCLUDED.total_leechers""",
                [(r["minute"], r["tracker"], r["rows"], r["s"], r["l"]) for r in minute],
            )
            cur.execute("CREATE TEMP TABLE tmp_latest (LIKE swarm_latest) ON COMMIT DROP")
            buf = io.StringIO()
            latest.to_csv(buf, index=False, header=False)
            with cur.copy("COPY tmp_latest FROM STDIN WITH (FORMAT csv)") as cp:
                cp.write(buf.getvalue())
            cur.execute(
                """INSERT INTO swarm_latest SELECT * FROM tmp_latest
                   ON CONFLICT (infohash, tracker) DO UPDATE SET
                     seeders = EXCLUDED.seeders, completed = EXCLUDED.completed,
                     leechers = EXCLUDED.leechers, scraped_at = EXCLUDED.scraped_at
                   WHERE EXCLUDED.scraped_at >= swarm_latest.scraped_at"""
            )
        print(f"[serving] batch {batch_id}: {n_rows} rows, {len(latest)} torrents")

    return write


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default="localhost:9092")
    ap.add_argument("--topic", default="tracker.scrapes")
    ap.add_argument("--out", default=str(STREAM / "swarm"))
    ap.add_argument("--checkpoint", default=str(STREAM / "_checkpoints"))
    ap.add_argument("--pg", default="postgresql://bda:bda@localhost:5432/torrent")
    ap.add_argument("--trigger", default="1 minute")
    ap.add_argument("--once", action="store_true", help="availableNow: drain Kafka then exit")
    args = ap.parse_args()

    spark = get_spark("swarm-stream", mem="4g", packages=KAFKA_PKG)
    spark.sparkContext.setLogLevel("WARN")

    rows = parse(
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 2_000_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    trigger = {"availableNow": True} if args.once else {"processingTime": args.trigger}

    lake = (
        rows.writeStream.queryName("lake")
        .format("parquet")
        .option("path", args.out)
        .option("checkpointLocation", f"{args.checkpoint}/lake")
        .partitionBy("date")
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )
    serving = (
        rows.writeStream.queryName("serving")
        .foreachBatch(make_serving_writer(args.pg, f"serving:{args.topic}"))
        .option("checkpointLocation", f"{args.checkpoint}/serving")
        .trigger(**trigger)
        .start()
    )
    print(f"streaming {args.topic} -> {args.out} + postgres  (trigger={trigger})")
    if args.once:
        lake.awaitTermination()
        serving.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
