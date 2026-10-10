"""A7 — Swarm dynamics over the day (live speed-layer data, 10 Oct 2026, IST).

The tracked set changed during the day (alive-only list from 11:50 IST, plus torrents found alive by the
full catalog scan), so totals over all torrents would show list changes, not swarm changes. Instead a fixed
panel is used: torrents present in >= 80 % of the 10-minute slots from START onwards.

  * panel seeders and leechers per category over the day, indexed to 100 at the first slot
  * leecher / seeder ratio over the day
  * download rate (`completed` growth per hour on opentrackr) by category and upload year

Usage: uv run python -m analysis.a7_swarm_day [--start "2026-10-10 12:00"]
"""
import argparse

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from analysis.style import CATEGORY_COLOR, INK_2, save, save_table, setup
from common.spark import CURATED, get_spark

IST = "Asia/Kolkata"
CATS = ["movie", "tv", "game"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-10 12:00", help="panel start, IST")
    args = ap.parse_args()
    setup()
    spark = get_spark("a7-swarm-day", mem="3g")
    c = F.col
    start_utc = F.to_utc_timestamp(F.lit(args.start).cast("timestamp"), IST)

    slots = spark.read.parquet(str(CURATED / "swarm_slots")).where(c("slot") >= start_utc)
    n_slots = slots.select("slot").distinct().count()
    meta = spark.read.parquet(str(CURATED / "torrents_enriched")).select("infohash", "category", "added_year")
    panel = (slots.groupBy("infohash").agg(F.count("*").alias("n"), F.max("seeders").alias("max_seeders"))
             .where((c("n") >= 0.8 * n_slots) & (c("max_seeders") > 0)).select("infohash"))
    series = (
        slots.join(panel, "infohash").join(meta, "infohash")
        .groupBy("category", "slot").agg(F.count("*").alias("torrents"), F.sum("seeders").alias("seeders"),
                                         F.sum("leechers").alias("leechers"))
        .withColumn("slot_ist", F.from_utc_timestamp("slot", IST))
        .orderBy("category", "slot").toPandas()
    )
    if series.empty:
        print("no panel data yet")
        return
    for col in ("seeders", "leechers"):
        series[f"{col}_index"] = series.groupby("category")[col].transform(lambda s: s / s.iloc[0] * 100)
    series["leech_ratio"] = series.leechers / series.seeders
    save_table(series, "a7_panel_by_slot")
    panel_n = series.groupby("category").torrents.max().to_dict()
    print(f"slots since {args.start} IST: {n_slots}; panel torrents per category: {panel_n}")

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharex=True)
    for cat in CATS:
        d = series[series.category == cat]
        if d.empty:
            continue
        axes[0].plot(d.slot_ist, d.seeders_index, color=CATEGORY_COLOR[cat], label=f"{cat} ({panel_n.get(cat, 0):,})")
        axes[1].plot(d.slot_ist, d.leech_ratio, color=CATEGORY_COLOR[cat], label=cat)
    axes[0].axhline(100, color=INK_2, linewidth=0.8, linestyle="--")
    axes[0].set_title("Seeders, fixed panel (first slot = 100)")
    axes[1].set_title("Leechers per seeder")
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=None))
        ax.set_xlabel("time (IST), 10 Oct 2026")
        ax.legend(loc="best")
    save(fig, "a7_swarm_day", f"Live tracker scrapes (max over trackers per 10-min slot). Panel: torrents in ≥80% of slots since {args.start} IST.")

    growth = (spark.read.parquet(str(CURATED / "swarm_torrent_day"))
              .where(c("completed_per_hour").isNotNull() & c("alive") & c("category").isin(*CATS))
              .groupBy("category", "added_year")
              .agg(F.count("*").alias("torrents"), F.avg("completed_per_hour").alias("mean_dl_per_hour"),
                   F.percentile_approx("completed_per_hour", 0.5).alias("median_dl_per_hour"),
                   F.sum("completed_per_hour").alias("total_dl_per_hour"))
              .orderBy("category", "added_year").toPandas())
    save_table(growth, "a7_download_rate_by_year")
    print(growth.groupby("category")[["torrents", "total_dl_per_hour"]].sum().to_string())
    spark.stop()


if __name__ == "__main__":
    main()
