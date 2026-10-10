"""A3 — Popularity decay: how swarm size falls with torrent age (cross-sectional, full RARBG catalog).

Data: one tracker scrape (opentrackr) of every movie / TV / game torrent in the dump, Oct 2026
(staging/swarm_fullscan), joined to the upload date. Age = scrape time - upload time.

  * % of torrents still alive (>= 1 seeder) and mean seeders by upload year and category
  * MLlib GeneralizedLinearRegression (Poisson, log link) on all torrents, per category:
        exponential decay   E[seeders] = exp(a - b * age_years)          -> half-life = ln2 / b
        power-law decay     E[seeders] = exp(a) * age_years ^ (-k)
    compared by AIC (lower = better fit).

This is cross-sectional: old torrents that survive were more popular to begin with, so the curve mixes
decay with survivorship. RARBG's catalog before ~2017 is also incomplete (see A2).

Usage: uv run python -m analysis.a3_decay
"""
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.sql import functions as F

from analysis.style import CATEGORY_COLOR, INK_2, save, save_table, setup
from common.spark import CURATED, STAGING, get_spark

CATS = ["movie", "tv", "game"]


def main():
    setup()
    spark = get_spark("a3-decay", mem="4g")
    c = F.col
    df = (
        spark.read.parquet(str(STAGING / "swarm_fullscan"))
        .join(spark.read.parquet(str(CURATED / "torrents_enriched")).select("infohash", "category", "added_at", "added_year"), "infohash")
        .withColumn("age_years", (c("scraped_at").cast("long") - c("added_at").cast("long")) / (365.25 * 86400))
        .where(c("age_years") > 0)
        .withColumn("log_age", F.log("age_years"))
        .withColumn("alive", (c("seeders") > 0).cast("int"))
        .cache()
    )
    total, alive = df.count(), df.where("alive = 1").count()
    print(f"full scan joined: {total:,} torrents, {alive:,} alive ({alive / total:.1%})")

    by_year = df.groupBy("category", "added_year").agg(
        F.count("*").alias("torrents"), F.avg("alive").alias("pct_alive"), F.avg("seeders").alias("mean_seeders"),
        F.percentile_approx("seeders", 0.99).alias("p99_seeders"), F.sum("seeders").alias("total_seeders"),
    ).orderBy("category", "added_year").toPandas()
    save_table(by_year, "a3_decay_by_year")

    fits = []
    for cat in CATS:
        d = df.where(c("category") == cat)
        for model, col in [("exponential", "age_years"), ("power_law", "log_age")]:
            data = VectorAssembler(inputCols=[col], outputCol="features").transform(d)
            m = GeneralizedLinearRegression(family="poisson", link="log", labelCol="seeders", maxIter=50).fit(data)
            coef = m.coefficients[0]
            fits.append({"category": cat, "model": model, "intercept": m.intercept, "coef": coef, "aic": m.summary.aic,
                         "half_life_years": (0.6931 / -coef) if model == "exponential" and coef < 0 else None,
                         "power_k": -coef if model == "power_law" else None, "n": d.count()})
    fits = pd.DataFrame(fits)
    fits["best"] = fits.groupby("category").aic.transform("min") == fits.aic
    save_table(fits, "a3_decay_model_fits")
    print(fits[["category", "model", "coef", "aic", "half_life_years", "power_k", "best"]].to_string(index=False))

    plot = by_year[by_year.added_year.between(2008, 2023)]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for cat in CATS:
        d = plot[plot.category == cat]
        axes[0].plot(d.added_year, d.pct_alive, color=CATEGORY_COLOR[cat], marker="o", markersize=3.5, label=cat)
        axes[1].plot(d.added_year, d.mean_seeders, color=CATEGORY_COLOR[cat], marker="o", markersize=3.5, label=cat)
    axes[0].set_title("Torrents still alive (≥1 seeder)")
    axes[0].yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    axes[1].set_title("Mean seeders per torrent")
    axes[1].set_yscale("log")
    for ax in axes:
        ax.set_xlabel("upload year")
        ax.legend(loc="upper left")
    save(fig, "a3_decay_by_year",
         f"Full catalog scan of {total:,} RARBG movie/TV/game torrents on opentrackr, 10 Oct 2026. Cross-sectional: includes survivorship.")

    best = fits[fits.best]
    lines = [f"{r.category}: {r.model.replace('_', '-')}" + (f", k = {r.power_k:.2f}" if r.power_k else f", half-life {r.half_life_years:.1f} y")
             for r in best.itertuples()]
    print("best fits ->", "; ".join(lines))
    spark.stop()


if __name__ == "__main__":
    main()
