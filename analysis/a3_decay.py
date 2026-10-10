"""A3 — Popularity decay: how a title's swarm shrinks as the title gets older (full RARBG catalog scan).

Data: one tracker scrape (opentrackr) of every movie / TV / game torrent in the dump, 10 Oct 2026
(staging/swarm_fullscan), joined to curated/torrents_enriched.

Two units, because they tell different stories:
  * per torrent, by upload year (descriptive): % of torrents still alive. Old torrents look *healthier* than
    expected because RARBG deleted unpopular old releases (survivorship), and new titles split their seeders
    across many versions (WEB, Blu-ray, 4K, repacks, game updates).
  * per title, by the title's release year (modelled): seeders summed over every version of the film / series
    / game (IMDb start year; Steam release year for games). Spark MLlib GeneralizedLinearRegression
    (Poisson, log link) on titles released 2012-2023, per category:
        exponential   E[seeders] = exp(a - b * age)        half-life = ln2 / b
        power law     E[seeders] = exp(a) * age ^ (-k)
    compared by AIC (lower = better). Still cross-sectional, so survivorship remains a caveat.

Usage: uv run python -m analysis.a3_decay
"""
import math

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MultipleLocator, PercentFormatter
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.sql import functions as F

from analysis.style import CATEGORY_COLOR, save, save_table, setup
from common.spark import CURATED, STAGING, get_spark

CATS = ["movie", "tv", "game"]
SCAN_YEAR = 2026.78  # 10 Oct 2026
FIT_YEARS = (2012, 2023)


def main():
    setup()
    spark = get_spark("a3-decay", mem="4g")
    c = F.col
    torrents = (
        spark.read.parquet(str(STAGING / "swarm_fullscan")).select("infohash", "seeders", "leechers")
        .join(spark.read.parquet(str(CURATED / "torrents_enriched")).select(
            "infohash", "category", "added_year", "imdb_id", "imdb_start_year", "steam_app_id", "steam_release_date"), "infohash")
        .withColumn("alive", (c("seeders") > 0).cast("int"))
        .cache()
    )
    total, alive = torrents.count(), torrents.where("alive = 1").count()
    print(f"full scan joined: {total:,} torrents, {alive:,} alive ({alive / total:.1%})")

    by_upload = torrents.groupBy("category", "added_year").agg(
        F.count("*").alias("torrents"), F.avg("alive").alias("pct_alive"), F.avg("seeders").alias("mean_seeders"),
    ).orderBy("category", "added_year").toPandas()
    save_table(by_upload, "a3_alive_by_upload_year")

    titles = (
        torrents.withColumn("title_id", F.when(c("category") == "game", c("steam_app_id").cast("string")).otherwise(c("imdb_id")))
        .withColumn("title_year", F.when(c("category") == "game", F.year("steam_release_date")).otherwise(c("imdb_start_year")))
        .where(c("title_id").isNotNull() & c("title_year").isNotNull())
        .groupBy("category", "title_id", "title_year")
        .agg(F.count("*").alias("versions"), F.sum("seeders").alias("seeders"), F.max("alive").alias("any_alive"))
        .withColumn("age", F.lit(SCAN_YEAR) - c("title_year") + 0.5)
        .withColumn("log_age", F.log("age"))
        .cache()
    )
    by_title_year = titles.groupBy("category", "title_year").agg(
        F.count("*").alias("titles"), F.avg("seeders").alias("mean_seeders_per_title"),
        F.avg("any_alive").alias("pct_titles_alive"), F.avg("versions").alias("mean_versions"),
    ).orderBy("category", "title_year").toPandas()
    save_table(by_title_year, "a3_title_decay_by_release_year")

    fit_rows = []
    fit_data = titles.where(c("title_year").between(*FIT_YEARS))
    for cat in CATS:
        d = fit_data.where(c("category") == cat)
        for model, col in [("exponential", "age"), ("power_law", "log_age")]:
            m = GeneralizedLinearRegression(family="poisson", link="log", labelCol="seeders", maxIter=50).fit(
                VectorAssembler(inputCols=[col], outputCol="features").transform(d))
            b = m.coefficients[0]
            fit_rows.append({"category": cat, "model": model, "intercept": m.intercept, "coef": b, "aic": m.summary.aic,
                             "half_life_years": math.log(2) / -b if model == "exponential" and b < 0 else None,
                             "power_k": -b if model == "power_law" else None, "titles": d.count()})
    fits = pd.DataFrame(fit_rows)
    fits["best"] = fits.groupby("category").aic.transform("min") == fits.aic
    save_table(fits, "a3_title_decay_fits")
    print(fits[["category", "model", "titles", "coef", "aic", "half_life_years", "power_k", "best"]].to_string(index=False))

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    up = by_upload[by_upload.added_year.between(2008, 2023)]
    for cat in CATS:
        d = up[up.category == cat]
        axes[0].plot(d.added_year, d.pct_alive, color=CATEGORY_COLOR[cat], marker="o", markersize=3.5, label=cat)
        t = by_title_year[(by_title_year.category == cat) & by_title_year.title_year.between(2008, 2023)]
        axes[1].plot(t.title_year, t.mean_seeders_per_title, color=CATEGORY_COLOR[cat], marker="o", markersize=3.5, label=cat)
    axes[0].set_title("Torrents still alive, by upload year")
    axes[0].yaxis.set_major_locator(MultipleLocator(0.05))
    axes[0].yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    axes[0].set_xlabel("upload year")
    axes[1].set_title("Seeders per title (all versions), by release year")
    axes[1].set_yscale("log")
    axes[1].set_xlabel("title release year")
    axes[0].legend(loc="upper left")
    axes[1].legend(loc="lower left")
    save(fig, "a3_decay",
         f"Full catalog scan of {total:,} RARBG movie/TV/game torrents on opentrackr, 10 Oct 2026. Cross-sectional: includes survivorship.")

    for r in fits[fits.best].itertuples():
        if r.model == "power_law":
            shape = f"power law, seeders ~ age^{-r.power_k:+.2f}" + ("" if r.power_k > 0 else " (grows with age: no decay)")
        else:
            shape = (f"exponential, half-life {r.half_life_years:.1f} years" if pd.notna(r.half_life_years)
                     else f"exponential, +{r.coef:.0%}/year (grows with age: no decay)")
        print(f"best fit {r.category}: {shape}")
    spark.stop()


if __name__ == "__main__":
    main()
