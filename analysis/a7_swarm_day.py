"""A7 — Swarm dynamics over the day (live speed-layer data, 10 Oct 2026, 12:40–21:00 IST).

Measurement design (avoids artefacts seen in a first version that summed max-over-tracker seeders):
  * one tracker only (opentrackr — the only one with no outage all day), so tracker outages and the
    hotspot slow-down cannot move the curve;
  * window 12:40–20:50 IST: both scraper tiers running, last partial round excluded;
  * 20-minute slots (tier-2 rounds take 11–20 min, so each torrent is seen in almost every slot);
  * fixed panel: torrents observed in >= 90 % of slots AND alive in most of them (median seeders > 0).
    Torrents that were merely alive when picked (tier 2 came from the 12:00 full scan) would otherwise
    show regression to the mean: high at first, then falling to their usual (often zero) level;
  * each torrent is indexed to its own mean over the day, then averaged per slot (mean relative
    swarm size x 100), so which torrents happen to be measured in a slot does not matter;
  * slots where < 80 % of the panel was observed are dropped.

Also: download rate (`completed` growth per hour on opentrackr) by category and upload year.

Usage: uv run python -m analysis.a7_swarm_day
"""
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pyspark.sql import Window, functions as F

from analysis.style import CATEGORY_COLOR, INK_2, save, save_table, setup
from common.spark import CURATED, STREAM, get_spark

IST = "Asia/Kolkata"
CATS = ["movie", "tv", "game"]
TRACKER = "tracker.opentrackr.org:1337"
START, END = "2026-10-10 12:40", "2026-10-10 20:50"
SLOT = "20 minutes"


def main():
    setup()
    spark = get_spark("a7-swarm-day", mem="4g")
    c = F.col
    utc = lambda s: F.to_utc_timestamp(F.lit(s).cast("timestamp"), IST)  # noqa: E731

    obs = (
        spark.read.parquet(str(STREAM / "swarm"))
        .where((c("tracker") == TRACKER) & (c("scraped_at") >= utc(START)) & (c("scraped_at") < utc(END)))
        .withColumn("slot", F.window("scraped_at", SLOT).start)
        .groupBy("infohash", "slot").agg(F.avg("seeders").alias("seeders"), F.avg("leechers").alias("leechers"))
    )
    n_slots = obs.select("slot").distinct().count()
    per_torrent = obs.groupBy("infohash").agg(
        F.count("*").alias("n"), F.avg("seeders").alias("mean_s"), F.percentile_approx("seeders", 0.5).alias("med_s"))
    panel = per_torrent.where((c("n") >= 0.9 * n_slots) & (c("med_s") > 0)).select("infohash", "mean_s")
    meta = spark.read.parquet(str(CURATED / "torrents_enriched")).select("infohash", "category")

    rel = obs.join(panel, "infohash").join(meta, "infohash").withColumn("rel", c("seeders") / c("mean_s")).cache()
    panel_size = rel.groupBy("category").agg(F.countDistinct("infohash").alias("panel"))
    series = (
        rel.groupBy("category", "slot")
        .agg(F.count("*").alias("observed"), F.avg("rel").alias("mean_rel"),
             F.sum("leechers").alias("leechers"), F.sum("seeders").alias("seeders"))
        .join(panel_size, "category")
        .where(c("observed") >= 0.8 * c("panel"))
        .withColumn("index", c("mean_rel") * 100)
        .withColumn("leech_per_seed", c("leechers") / c("seeders"))
        .withColumn("slot_ist", F.from_utc_timestamp("slot", IST))
        .orderBy("category", "slot").toPandas()
    )
    save_table(series, "a7_panel_by_slot")
    sizes = dict(zip(panel_size.toPandas().category, panel_size.toPandas().panel))
    summary = series.groupby("category").agg(min_index=("index", "min"), max_index=("index", "max"),
                                             mean_leech_per_seed=("leech_per_seed", "mean"), slots=("index", "size"))
    summary["swing_pct"] = summary.max_index - summary.min_index
    save_table(summary.reset_index(), "a7_summary")
    print(f"{n_slots} slots of {SLOT}, panel sizes {sizes}")
    print(summary.round(2).to_string())

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharex=True)
    for cat in CATS:
        d = series[series.category == cat]
        if d.empty:
            continue
        axes[0].plot(d.slot_ist, d["index"], color=CATEGORY_COLOR[cat], label=f"{cat} ({sizes.get(cat, 0):,})")
        axes[1].plot(d.slot_ist, d.leech_per_seed, color=CATEGORY_COLOR[cat], label=cat)
    axes[0].axhline(100, color=INK_2, linewidth=0.8, linestyle="--")
    axes[0].set_title("Seeders vs each torrent's daily average (= 100)")
    axes[1].set_title("Leechers per seeder")
    axes[1].set_ylim(0, None)
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_xlabel("time (IST), 10 Oct 2026")
        ax.legend(loc="best")
    save(fig, "a7_swarm_day",
         f"opentrackr scrapes, {START[-5:]}–{END[-5:]} IST, {SLOT} slots. Panel: torrents seen in ≥90% of slots with median seeders > 0.")

    growth = (spark.read.parquet(str(CURATED / "swarm_torrent_day"))
              .where(c("completed_per_hour").isNotNull() & c("alive") & c("category").isin(*CATS))
              .groupBy("category", "added_year")
              .agg(F.count("*").alias("torrents"), F.avg("completed_per_hour").alias("mean_dl_per_hour"),
                   F.sum("completed_per_hour").alias("total_dl_per_hour"))
              .orderBy("category", "added_year").toPandas())
    save_table(growth, "a7_download_rate_by_year")
    print(growth.groupby("category")[["torrents", "total_dl_per_hour"]].sum().round(0).to_string())
    spark.stop()


if __name__ == "__main__":
    main()
