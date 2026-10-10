"""A4 — Public interest vs piracy: English Wikipedia pageviews vs RARBG torrent activity per title.

Interest  = en + en.m pageviews of the title's page, 1-7 Sep 2026 (curated/wiki_interest, confidence != low)
Piracy    = (a) number of RARBG torrent versions of the title
            (b) current seeders summed over the title's torrents (full catalog scan, Oct 2026)
Both (a) interest and (b) seeders are "current" (2026); torrent versions reflect 2007-2023 uploads.

Spearman rank correlations per category via pyspark.ml.stat.Correlation. Correlation, not causation.

Usage: uv run python -m analysis.a4_wiki_interest
"""
import math

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as F

from analysis.style import CATEGORY_COLOR, INK_2, save, save_table, setup
from common.spark import CURATED, STAGING, get_spark

CATS = ["movie", "tv", "game"]


def main():
    setup()
    spark = get_spark("a4-wiki", mem="4g")
    c = F.col
    e = spark.read.parquet(str(CURATED / "torrents_enriched")).select(
        "infohash", "category", "imdb_id", c("steam_app_id").cast("string").alias("steam_id"))
    scan = spark.read.parquet(str(STAGING / "swarm_fullscan")).select("infohash", "seeders", "leechers")
    per_torrent = e.join(scan, "infohash", "left").fillna({"seeders": 0, "leechers": 0}) \
        .withColumn("entity_type", F.when(c("category") == "game", "steam").otherwise("imdb")) \
        .withColumn("entity_id", F.when(c("category") == "game", c("steam_id")).otherwise(c("imdb_id"))) \
        .where(c("entity_id").isNotNull())
    piracy = per_torrent.groupBy("entity_type", "entity_id", "category").agg(
        F.count("*").alias("torrents"), F.sum("seeders").alias("seeders"), F.sum("leechers").alias("leechers"))

    wiki = spark.read.parquet(str(CURATED / "wiki_interest")).where(c("confidence") != "low") \
        .select("entity_type", "entity_id", "name", "page_title", "views_week", "confidence")
    df = (wiki.join(piracy, ["entity_type", "entity_id"])
          .withColumn("log_views", F.log10("views_week")).withColumn("log_seeders", F.log10(c("seeders") + 1))
          .withColumn("log_torrents", F.log10("torrents"))
          .cache())

    rows = []
    for cat in CATS:
        d = df.where(c("category") == cat)
        vec = VectorAssembler(inputCols=["views_week", "torrents", "seeders"], outputCol="v").transform(d).select("v")
        m = Correlation.corr(vec, "v", "spearman").head()[0].toArray()
        rows.append({"category": cat, "titles": d.count(), "rho_views_torrents": m[0, 1], "rho_views_seeders": m[0, 2],
                     "rho_torrents_seeders": m[1, 2], "pct_with_seeders": d.where("seeders > 0").count() / max(d.count(), 1)})
    corr = pd.DataFrame(rows)
    save_table(corr, "a4_spearman")
    print(corr.to_string(index=False))

    pdf = df.select("category", "name", "page_title", "views_week", "torrents", "seeders", "log_views", "log_seeders").toPandas()
    save_table(pdf.sort_values("views_week", ascending=False).head(500), "a4_top_titles")
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.2), sharex=True, sharey=True)
    for ax, cat in zip(axes, CATS):
        d = pdf[pdf.category == cat]
        r = corr[corr.category == cat].iloc[0]
        ax.hexbin(d.log_views, d.log_seeders, gridsize=35, mincnt=1, bins="log",
                  cmap=LinearSegmentedColormap.from_list(cat, ["#eeedea", CATEGORY_COLOR[cat]]))
        ax.set_title(f"{cat}  (n={len(d):,})")
        ax.annotate(f"Spearman ρ = {r.rho_views_seeders:.2f}", (0.03, 0.95), xycoords="axes fraction", va="top", fontsize=9, color=INK_2)
        ax.set_xlabel("Wikipedia views, 1–7 Sep 2026")
        ax.grid(False)
    axes[0].set_ylabel("current seeders (all versions)")
    yt = [0, 1, 10, 100, 1000]
    axes[0].set_yticks([math.log10(v + 1) for v in yt], [f"{v:,}" for v in yt])
    xt = [1, 100, 10_000]
    for ax in axes:
        ax.set_xticks([math.log10(v) for v in xt], ["1", "100", "10k"])
    save(fig, "a4_wiki_vs_seeders", "English Wikipedia (desktop + mobile) vs seeders on opentrackr, Oct 2026. Darker = more titles. Log scales.")
    spark.stop()


if __name__ == "__main__":
    main()
