"""A5 — What predicts whether a Steam game is pirated on RARBG? (Spark MLlib)

Population: Steam games released 2018-2022 with >= 100 user reviews (excludes shovelware).
Pirated = at least one RARBG game torrent matched to the game (curated/steam_matches).

  * LogisticRegression  pirated ~ standardized features   -> odds ratios, test AUC (80/20 split)
  * RandomForestRegressor  log1p(torrents) ~ features      -> feature importance, test RMSE / R²
  * descriptive: % pirated by price band and by popularity band

Matching only covers game torrents whose name could be tied to a Steam app (~79%), so some pirated games
count as not pirated; estimates of "pirated" are therefore lower bounds.

Usage: uv run python -m analysis.a5_games
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F

from analysis.style import CATEGORY_COLOR, INK_2, save, save_table, setup
from common.spark import CURATED, STAGING, get_spark

GENRES = ["Action", "Adventure", "RPG", "Strategy", "Simulation", "Casual", "Indie", "Sports", "Racing",
          "Massively Multiplayer", "Early Access"]


def main():
    setup()
    spark = get_spark("a5-games", mem="4g")
    c = F.col
    torrents = (spark.read.parquet(str(CURATED / "steam_matches")).where(c("app_id").isNotNull())
                .groupBy("app_id").agg(F.count("*").alias("torrents")))
    games = (
        spark.read.parquet(str(STAGING / "steam_games"))
        .where(c("release_year").between(2018, 2022) & (c("reviews_total") >= 100))
        .join(torrents, "app_id", "left").fillna({"torrents": 0})
        .withColumn("pirated", (c("torrents") > 0).cast("double"))
        .withColumn("y_log_torrents", F.log1p("torrents"))
        .withColumn("log_price", F.log1p("price_usd"))
        .withColumn("free", c("is_free").cast("double"))
        .withColumn("log_reviews", F.log("reviews_total"))
        .withColumn("score", c("review_score"))
        .withColumn("years_since_release", F.lit(2023) - c("release_year"))
        .withColumn("single_player", F.array_contains("categories", "Single-player").cast("double"))
        .withColumn("online_pvp", F.array_contains("categories", "Online PvP").cast("double"))
    )
    for g in GENRES:
        games = games.withColumn("g_" + g.lower().replace(" ", "_"), F.array_contains("genres", g).cast("double"))
    feats = ["log_price", "free", "log_reviews", "score", "years_since_release", "single_player", "online_pvp"] + \
            ["g_" + g.lower().replace(" ", "_") for g in GENRES]
    # a game with no category / genre list has none of those flags
    games = games.select("app_id", "name", "price_usd", "reviews_total", "torrents", "pirated", "y_log_torrents", *feats) \
        .na.fill(0.0, subset=[f for f in feats if f not in ("log_price", "log_reviews", "score")]) \
        .na.drop(subset=feats).cache()
    n, n_pirated = games.count(), games.where("pirated = 1").count()
    print(f"games: {n:,}, pirated on RARBG: {n_pirated:,} ({n_pirated / n:.1%})")

    train, test = games.randomSplit([0.8, 0.2], seed=42)
    assemble = VectorAssembler(inputCols=feats, outputCol="raw")
    scale = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)

    logit = Pipeline(stages=[assemble, scale, LogisticRegression(labelCol="pirated", regParam=0.01)]).fit(train)
    auc = BinaryClassificationEvaluator(labelCol="pirated").evaluate(logit.transform(test))
    coefs = pd.DataFrame({"feature": feats, "coef_std": logit.stages[-1].coefficients.toArray()})
    coefs["odds_ratio_per_sd"] = np.exp(coefs.coef_std)
    coefs = coefs.sort_values("coef_std")
    save_table(coefs.assign(test_auc=auc), "a5_logit_pirated")
    print(f"logistic regression: test AUC = {auc:.3f}")

    rf = Pipeline(stages=[assemble, RandomForestRegressor(featuresCol="raw", labelCol="y_log_torrents", numTrees=200,
                                                          maxDepth=8, seed=42)]).fit(train)
    pred = rf.transform(test)
    rmse = RegressionEvaluator(labelCol="y_log_torrents", metricName="rmse").evaluate(pred)
    r2 = RegressionEvaluator(labelCol="y_log_torrents", metricName="r2").evaluate(pred)
    imp = pd.DataFrame({"feature": feats, "importance": rf.stages[-1].featureImportances.toArray()}).sort_values("importance", ascending=False)
    save_table(imp.assign(test_rmse=rmse, test_r2=r2), "a5_rf_importance")
    print(f"random forest log1p(torrents): test RMSE = {rmse:.3f}, R² = {r2:.3f}")
    print(imp.head(8).to_string(index=False))

    labels = {"log_price": "price (log)", "free": "free-to-play", "log_reviews": "popularity (log reviews)",
              "score": "review score", "years_since_release": "years since release", "single_player": "single-player",
              "online_pvp": "online PvP"} | {"g_" + g.lower().replace(" ", "_"): f"genre: {g}" for g in GENRES}
    fig, ax = plt.subplots(figsize=(8, 6))
    y = np.arange(len(coefs))
    ax.barh(y, coefs.odds_ratio_per_sd - 1, left=1, color=[("#e34948" if v > 1 else "#2a78d6") for v in coefs.odds_ratio_per_sd],
            height=0.7, edgecolor="#fcfcfb")
    ax.axvline(1, color=INK_2, linewidth=0.8)
    ax.set_xscale("log")
    ax.set_yticks(y, [labels[f] for f in coefs.feature])
    ax.grid(axis="y", visible=False)
    ax.set_title("What makes a Steam game more likely to be pirated?")
    ax.set_xlabel("odds ratio per 1 standard deviation (log scale; >1 = more likely on RARBG)")
    ticks = [t for t in (0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4) if coefs.odds_ratio_per_sd.min() * 0.9 <= t <= coefs.odds_ratio_per_sd.max() * 1.1]
    ax.set_xticks(ticks, [f"{t:g}×" for t in ticks])
    ax.xaxis.set_minor_formatter(plt.NullFormatter())
    save(fig, "a5_game_piracy_odds", f"Steam games 2018–2022, ≥100 reviews (n={n:,}). MLlib logistic regression, test AUC = {auc:.2f}.")

    pdf = games.select("price_usd", "reviews_total", "pirated").toPandas()
    pdf["price_band"] = pd.cut(pdf.price_usd, [-0.01, 0, 5, 10, 20, 40, 1000], labels=["free", "<$5", "$5–10", "$10–20", "$20–40", ">$40"])
    pdf["pop_band"] = pd.cut(pdf.reviews_total, [99, 300, 1000, 5000, 20000, 1e9], labels=["100–300", "300–1k", "1k–5k", "5k–20k", ">20k"])
    bands = [pdf.groupby("price_band", observed=True).pirated.agg(["mean", "size"]), pdf.groupby("pop_band", observed=True).pirated.agg(["mean", "size"])]
    save_table(pd.concat([b.reset_index().rename(columns={b.index.name: "band"}).assign(by=k) for b, k in zip(bands, ["price", "popularity"])]),
               "a5_pirated_by_band")
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
    for ax, b, title in zip(axes, bands, ["by price", "by popularity (Steam reviews)"]):
        ax.bar(b.index.astype(str), b["mean"], color=CATEGORY_COLOR["game"], width=0.7)
        for i, (m, s) in enumerate(zip(b["mean"], b["size"])):
            ax.annotate(f"{m:.0%}", (i, m), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=9, color=INK_2)
        ax.set_title(f"Share pirated {title}")
        ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
        ax.grid(axis="x", visible=False)
    save(fig, "a5_pirated_by_band", "Steam games 2018–2022 with ≥100 reviews; pirated = matched to ≥1 RARBG torrent.")
    spark.stop()


if __name__ == "__main__":
    main()
