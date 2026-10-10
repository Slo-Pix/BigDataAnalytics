"""Phase 4: summarise tracker swarm data and join it to curated/torrents_enriched.

Live stream (dataset/stream/swarm, written by Spark Structured Streaming):
  Trackers each see part of a swarm and scrape on their own schedule, so measurements are put into
  10-minute slots and, per slot, the max over trackers is taken as the swarm size estimate.
    curated/swarm_slots           infohash x slot: seeders, leechers, completed, n_trackers
    curated/swarm_torrent_day     one row per torrent: median/max seeders & leechers, variability,
                                  download growth (completed/hour on opentrackr only, since each
                                  tracker counts `completed` differently) + torrent metadata
    curated/swarm_category_slots  category x slot totals (intra-day curve)

Full catalog scan (dataset/stream/fullscan_jsonl, one pass over all 1.39M torrents on opentrackr):
    staging/swarm_fullscan        one row per torrent
    curated/decay_by_year         category x upload year: % alive, median / mean / p90 seeders

Re-run after live collection ends to get the final numbers; every output is overwritten.

Usage: uv run python -m etl.curate_swarm [--skip-fullscan]
"""
import argparse

from pyspark.sql import functions as F

from common.spark import CURATED, STAGING, STREAM, get_spark

SLOT = "10 minutes"
GROWTH_TRACKER = "tracker.opentrackr.org:1337"
META = ["infohash", "category", "display_title", "added_at", "added_year", "imdb_id", "imdb_rating", "imdb_votes",
        "steam_app_id", "price_usd", "review_score", "original_language", "theatrical_date", "home_release_date"]


def live(spark, torrents):
    c = F.col
    raw = spark.read.parquet(str(STREAM / "swarm")).where(c("infohash").rlike("^[0-9a-f]{40}$"))
    slots = (
        raw.withColumn("slot", F.window("scraped_at", SLOT).start)
        .groupBy("infohash", "slot")
        .agg(F.max("seeders").alias("seeders"), F.max("leechers").alias("leechers"),
             F.max("completed").alias("completed"), F.countDistinct("tracker").alias("n_trackers"))
    )
    slots.write.mode("overwrite").parquet(str(CURATED / "swarm_slots"))
    slots = spark.read.parquet(str(CURATED / "swarm_slots"))

    growth = (
        raw.where(c("tracker") == GROWTH_TRACKER)
        .groupBy("infohash")
        .agg(F.min_by("completed", "scraped_at").alias("completed_first"),
             F.max_by("completed", "scraped_at").alias("completed_last"),
             ((F.max("scraped_at").cast("long") - F.min("scraped_at").cast("long")) / 3600).alias("growth_hours"))
        .withColumn("completed_per_hour",
                    F.when(c("growth_hours") >= 1, (c("completed_last") - c("completed_first")) / c("growth_hours")))
    )
    per_torrent = (
        slots.groupBy("infohash")
        .agg(F.count("*").alias("n_slots"), F.min("slot").alias("first_slot"), F.max("slot").alias("last_slot"),
             F.percentile_approx("seeders", 0.5).alias("seeders_median"), F.max("seeders").alias("seeders_max"),
             F.min("seeders").alias("seeders_min"), F.avg("seeders").alias("seeders_mean"),
             F.stddev("seeders").alias("seeders_std"),
             F.percentile_approx("leechers", 0.5).alias("leechers_median"), F.max("leechers").alias("leechers_max"))
        .withColumn("seeders_cv", F.when(c("seeders_mean") > 0, c("seeders_std") / c("seeders_mean")))
        .withColumn("leech_seed_ratio", F.when(c("seeders_median") > 0, c("leechers_median") / c("seeders_median")))
        .withColumn("alive", c("seeders_max") > 0)
        .join(growth, "infohash", "left")
        .join(torrents.select(*META), "infohash", "left")
    )
    per_torrent.write.mode("overwrite").parquet(str(CURATED / "swarm_torrent_day"))

    (slots.join(torrents.select("infohash", "category"), "infohash")
     .groupBy("category", "slot")
     .agg(F.count("*").alias("torrents"), F.sum("seeders").alias("seeders"), F.sum("leechers").alias("leechers"))
     .write.mode("overwrite").parquet(str(CURATED / "swarm_category_slots")))

    d = spark.read.parquet(str(CURATED / "swarm_torrent_day"))
    print(f"live: {raw.count():,} raw rows -> {slots.count():,} torrent-slots -> {d.count():,} torrents "
          f"({d.where('alive').count():,} alive)")
    d.where("alive").groupBy("category").agg(
        F.count("*").alias("alive"), F.percentile_approx("seeders_median", 0.5).alias("median_seeders"),
        F.round(F.avg("seeders_cv"), 2).alias("mean_cv"),
        F.round(F.percentile_approx("completed_per_hour", 0.5), 2).alias("median_dl_per_hour"),
    ).orderBy("category").show()


def fullscan(spark, torrents):
    c = F.col
    scan = (
        spark.read.json(str(STREAM / "fullscan_jsonl" / "part*" / "*" / "*.jsonl"))
        .withColumn("scraped_at", F.to_timestamp("scraped_at"))
        .groupBy("infohash").agg(F.max_by(F.struct("seeders", "leechers", "completed", "scraped_at"), "scraped_at").alias("s"))
        .select("infohash", "s.*")
    )
    scan.write.mode("overwrite").parquet(str(STAGING / "swarm_fullscan"))
    scan = spark.read.parquet(str(STAGING / "swarm_fullscan"))
    joined = scan.join(torrents.select("infohash", "category", "added_at", "added_year"), "infohash")
    decay = (
        joined.groupBy("category", "added_year")
        .agg(F.count("*").alias("torrents"), F.sum((c("seeders") > 0).cast("int")).alias("alive"),
             F.sum("seeders").alias("total_seeders"), F.avg("seeders").alias("mean_seeders"),
             F.percentile_approx("seeders", 0.5).alias("median_seeders"),
             F.percentile_approx("seeders", 0.9).alias("p90_seeders"),
             F.percentile_approx("seeders", 0.99).alias("p99_seeders"))
        .withColumn("pct_alive", F.round(c("alive") / c("torrents") * 100, 2))
        .orderBy("category", "added_year")
    )
    decay.write.mode("overwrite").parquet(str(CURATED / "decay_by_year"))
    print(f"fullscan: {scan.count():,} torrents scanned, {scan.where('seeders > 0').count():,} alive")
    spark.read.parquet(str(CURATED / "decay_by_year")).where("category = 'movie'").show(20)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-fullscan", action="store_true")
    ap.add_argument("--skip-live", action="store_true")
    args = ap.parse_args()

    spark = get_spark("curate-swarm", mem="4g")
    torrents = spark.read.parquet(str(CURATED / "torrents_enriched"))
    if not args.skip_live:
        live(spark, torrents)
    if not args.skip_fullscan:
        fullscan(spark, torrents)
    spark.stop()


if __name__ == "__main__":
    main()
