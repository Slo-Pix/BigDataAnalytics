"""A6 — Which genres are pirated more or less than their popularity predicts? (Spark MLlib)

Population: every IMDb movie released 2018-2022 with >= 1,000 votes, including films with no RARBG
torrent (so we do not only look at pirated films). Piracy = number of RARBG torrent versions of the film.

Model (MLlib LinearRegression):  log1p(torrents) ~ log(votes) + release-year dummies
A genre's excess piracy = mean residual of its films, shown as exp(mean residual) - 1
("+40%" = 40% more torrent versions than an equally popular film of the same year).

Usage: uv run python -m analysis.a6_genres
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from analysis.style import INK_2, save, save_table, setup
from common.spark import CURATED, STAGING, get_spark

DIVERGING_POS, DIVERGING_NEG = "#e34948", "#2a78d6"  # red = more piracy than expected, blue = less


def main():
    setup()
    spark = get_spark("a6-genres", mem="4g")
    c = F.col
    films = (
        spark.read.parquet(str(STAGING / "imdb_titles"))
        .where((c("title_type") == "movie") & c("start_year").between(2018, 2022) & (c("num_votes") >= 1000) & ~c("is_adult"))
        .select(c("tconst").alias("imdb_id"), "primary_title", "start_year", "num_votes", "genres")
    )
    counts = (spark.read.parquet(str(CURATED / "torrents_enriched")).where(c("category") == "movie")
              .groupBy("imdb_id").agg(F.count("*").alias("torrents")))
    df = (films.join(counts, "imdb_id", "left").fillna({"torrents": 0})
          .withColumn("y", F.log1p("torrents")).withColumn("log_votes", F.log("num_votes"))
          .withColumn("year", c("start_year").cast("string")))

    pipe = Pipeline(stages=[
        StringIndexer(inputCol="year", outputCol="year_idx"),
        OneHotEncoder(inputCols=["year_idx"], outputCols=["year_vec"]),
        VectorAssembler(inputCols=["log_votes", "year_vec"], outputCol="features"),
        LinearRegression(featuresCol="features", labelCol="y"),
    ])
    model = pipe.fit(df)
    lr = model.stages[-1]
    summary = lr.summary
    print(f"films: {df.count():,} ({df.where('torrents > 0').count():,} with torrents); "
          f"R² = {summary.r2:.3f}; log(votes) coefficient = {lr.coefficients[0]:.3f}")

    scored = model.transform(df).withColumn("resid", c("y") - c("prediction"))
    by_genre = (
        scored.select("imdb_id", "resid", "torrents", "num_votes", F.explode("genres").alias("genre"))
        .groupBy("genre").agg(F.count("*").alias("films"), F.avg("resid").alias("mean_resid"),
                              F.stddev("resid").alias("sd_resid"), F.avg((c("torrents") > 0).cast("int")).alias("pct_pirated"),
                              F.percentile_approx("num_votes", 0.5).alias("median_votes"))
        .where(c("films") >= 50)
        .withColumn("excess_piracy", F.exp("mean_resid") - 1)
        .withColumn("ci95", 1.96 * c("sd_resid") / F.sqrt("films"))
        .orderBy("excess_piracy")
    ).toPandas()
    save_table(by_genre, "a6_genre_excess_piracy")
    save_table(
        pd.DataFrame({"term": ["intercept", "log_votes"], "coef": [lr.intercept, lr.coefficients[0]],
                                        "r2": [summary.r2, summary.r2]}), "a6_model")

    fig, ax = plt.subplots(figsize=(8, 6))
    colors = [DIVERGING_POS if v > 0 else DIVERGING_NEG for v in by_genre.excess_piracy]
    y = np.arange(len(by_genre))
    lo = np.exp(by_genre.mean_resid - by_genre.ci95) - 1
    hi = np.exp(by_genre.mean_resid + by_genre.ci95) - 1
    ax.barh(y, by_genre.excess_piracy, color=colors, height=0.7, edgecolor="#fcfcfb", linewidth=1)
    ax.errorbar(by_genre.excess_piracy, y, xerr=[by_genre.excess_piracy - lo, hi - by_genre.excess_piracy],
                fmt="none", ecolor=INK_2, elinewidth=0.8, capsize=0)
    ax.set_yticks(y, [f"{g}  ({n:,})" for g, n in zip(by_genre.genre, by_genre.films)])
    ax.axvline(0, color=INK_2, linewidth=0.8)
    ax.xaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.grid(axis="y", visible=False)
    ax.set_title("Torrent versions vs. an equally popular film, by genre")
    ax.set_xlabel("more (red) or fewer (blue) RARBG torrents than IMDb popularity predicts")
    save(fig, "a6_genre_excess_piracy",
         f"IMDb movies 2018–2022, ≥1,000 votes (n={df.count():,}). MLlib OLS: log1p(torrents) ~ log(votes) + year. "
         f"R²={summary.r2:.2f}. Bars: 95% CI.")
    print(by_genre[["genre", "films", "excess_piracy", "pct_pirated"]].to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
