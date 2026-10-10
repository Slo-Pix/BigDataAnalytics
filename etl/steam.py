"""Kaggle Steam games CSV -> staging/steam_games (Parquet).

The CSV header merges `Discount` and `DLC count` into one name (39 names, 40 values per row), so the
header row is ignored and the 40 column names below are used instead. Text fields contain quoted
newlines, hence multiLine + escape='"'. All parsing uses try_* so a bad value becomes null instead of
failing the job (Spark 4 runs in ANSI mode).

Usage: uv run python -m etl.steam
"""
from pyspark.sql import Column, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.spark import RAW, STAGING, get_spark

SRC = RAW / "steam" / "games.csv"
DST = STAGING / "steam_games"

COLUMNS = [
    "AppID", "Name", "Release date", "Estimated owners", "Peak CCU", "Required age", "Price", "Discount",
    "DLC count", "About the game", "Supported languages", "Full audio languages", "Reviews", "Header image",
    "Website", "Support url", "Support email", "Windows", "Mac", "Linux", "Metacritic score", "Metacritic url",
    "User score", "Positive", "Negative", "Score rank", "Achievements", "Recommendations", "Notes",
    "Average playtime forever", "Average playtime two weeks", "Median playtime forever",
    "Median playtime two weeks", "Developers", "Publishers", "Categories", "Genres", "Tags", "Screenshots", "Movies",
]


def norm_title(c: Column) -> Column:
    """Lowercase alphanumerics only; used to match game names against torrent titles."""
    return F.regexp_replace(F.lower(c), r"[^a-z0-9]+", "")


def split_list(c: Column) -> Column:
    return F.when(F.trim(c) == "", F.array().cast("array<string>")).otherwise(F.split(c, ","))


def main():
    spark = get_spark("etl-steam", mem="4g")
    schema = StructType([StructField(c, StringType()) for c in COLUMNS])
    raw = (
        spark.read.option("header", True).option("enforceSchema", True)
        .option("multiLine", True).option("escape", '"')
        .schema(schema).csv(str(SRC))
    )
    c = F.col
    num = lambda name, t="int": c(f"`{name}`").try_cast(t)  # noqa: E731
    owners = F.split(c("`Estimated owners`"), r"\s*-\s*")
    release = F.coalesce(
        F.try_to_timestamp(c("`Release date`"), F.lit("MMM d, yyyy")),
        F.try_to_timestamp(c("`Release date`"), F.lit("MMM yyyy")),
        F.try_to_timestamp(c("`Release date`"), F.lit("d MMM, yyyy")),
    )

    games = (
        raw.select(
            num("AppID", "long").alias("app_id"),
            c("Name").alias("name"),
            norm_title(c("Name")).alias("name_norm"),
            F.to_date(release).alias("release_date"),
            num("Price", "double").alias("price_usd"),
            num("Discount").alias("discount_pct"),
            num("DLC count").alias("dlc_count"),
            num("Required age").alias("required_age"),
            owners.getItem(0).try_cast("long").alias("owners_low"),
            owners.getItem(1).try_cast("long").alias("owners_high"),
            num("Peak CCU").alias("peak_ccu"),
            num("Positive").alias("positive"),
            num("Negative").alias("negative"),
            num("Metacritic score").alias("metacritic_score"),
            num("Recommendations").alias("recommendations"),
            num("Average playtime forever").alias("avg_playtime_min"),
            num("Median playtime forever").alias("median_playtime_min"),
            (F.lower(c("Windows")) == "true").alias("windows"),
            (F.lower(c("Mac")) == "true").alias("mac"),
            (F.lower(c("Linux")) == "true").alias("linux"),
            split_list(c("Developers")).alias("developers"),
            split_list(c("Publishers")).alias("publishers"),
            split_list(c("Categories")).alias("categories"),
            split_list(c("Genres")).alias("genres"),
            split_list(c("Tags")).alias("tags"),
            c("Notes").alias("content_notes"),
        )
        .where(c("app_id").isNotNull() & c("name").isNotNull())
        .dropDuplicates(["app_id"])
        .withColumn("owners_mid", (c("owners_low") + c("owners_high")) / 2)
        .withColumn("reviews_total", c("positive") + c("negative"))
        .withColumn("review_score", F.when(c("reviews_total") > 0, c("positive") / c("reviews_total")))
        .withColumn("is_free", c("price_usd") == 0)
        .withColumn("release_year", F.year("release_date"))
    )

    games.write.mode("overwrite").parquet(str(DST))
    out = spark.read.parquet(str(DST))
    n = out.count()
    print(f"wrote {DST}: {n:,} games")
    out.select(
        *[F.round(F.avg(F.col(x).isNull().cast("int")) * 100, 2).alias(f"%null_{x}")
          for x in ["release_date", "price_usd", "owners_low", "positive"]]
    ).show()
    out.where("reviews_total > 100000").orderBy(F.desc("reviews_total")).select(
        "name", "release_date", "price_usd", "owners_low", "owners_high", "positive", "negative", "genres"
    ).show(5, truncate=40)
    spark.stop()


if __name__ == "__main__":
    main()
