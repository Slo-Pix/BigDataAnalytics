"""TMDB fetch output (JSONL) -> staging Parquet tables.

  staging/tmdb_release_dates  long table: imdb_id, country, type, date, note
  staging/tmdb_titles         one row per title with derived dates:
      theatrical_date    earliest theatrical (type 2/3) in any country, else TMDB primary release_date
      digital_date       earliest digital  (type 4) on/after theatrical_date, any country
      physical_date      earliest physical (type 5) on/after theatrical_date, any country
      home_release_date  least(digital_date, physical_date)  <- used for leak lag
  TV rows carry first_air_date / last_air_date instead.

US-only digital dates are unreliable (missing before ~2013, or late re-releases), hence the any-country rule.

Usage: uv run python -m etl.tmdb
"""
from pyspark.sql import functions as F

from common.spark import RAW, STAGING, get_spark

SRC = RAW / "tmdb" / "tmdb_raw.jsonl"


def main():
    spark = get_spark("etl-tmdb", mem="2g")
    c = F.col
    raw = spark.read.json(str(SRC)).where(c("status") == "ok")

    rel = (
        raw.where(c("kind") == "movie")
        .select("imdb_id", F.explode("release_dates").alias("r"))
        .select("imdb_id", c("r.country").alias("country"), c("r.type").cast("int").alias("type"),
                F.try_to_timestamp(c("r.date"), F.lit("yyyy-MM-dd")).cast("date").alias("date"),
                c("r.note").alias("note"))
        .where(c("date").isNotNull())
    )
    rel.write.mode("overwrite").parquet(str(STAGING / "tmdb_release_dates"))

    primary = raw.select("imdb_id", F.try_to_timestamp(c("release_date"), F.lit("yyyy-MM-dd")).cast("date").alias("primary_date"))
    theatrical = (
        rel.where(c("type").isin(2, 3)).groupBy("imdb_id").agg(F.min("date").alias("theatrical_any"))
        .join(primary, "imdb_id", "right")
        .select("imdb_id", F.coalesce("theatrical_any", "primary_date").alias("theatrical_date"))
    )
    home = (
        rel.join(theatrical, "imdb_id")
        .where(c("date") >= c("theatrical_date"))
        .groupBy("imdb_id")
        .agg(F.min(F.when(c("type") == 4, c("date"))).alias("digital_date"),
             F.min(F.when(c("type") == 5, c("date"))).alias("physical_date"),
             F.min(F.when((c("type") == 4) & (c("country") == "US"), c("date"))).alias("us_digital_date"))
        .withColumn("home_release_date", F.least("digital_date", "physical_date"))
    )

    to_date = lambda col: F.try_to_timestamp(c(col), F.lit("yyyy-MM-dd")).cast("date")  # noqa: E731
    titles = (
        raw.select(
            "imdb_id", "kind", "tmdb_id", "title", "imdb_title", "imdb_year", "imdb_votes", "imdb_rating",
            "popularity", "original_language", "genres", "runtime", "budget", "revenue",
            to_date("first_air_date").alias("first_air_date"), to_date("last_air_date").alias("last_air_date"),
            "number_of_seasons", "number_of_episodes", "networks", "in_production",
        )
        .join(theatrical, "imdb_id", "left")
        .join(home, "imdb_id", "left")
        .withColumn("theatrical_to_home_days", F.datediff("home_release_date", "theatrical_date"))
    )
    titles.write.mode("overwrite").parquet(str(STAGING / "tmdb_titles"))

    t = spark.read.parquet(str(STAGING / "tmdb_titles"))
    print(f"tmdb_titles: {t.count():,} rows, tmdb_release_dates: {spark.read.parquet(str(STAGING / 'tmdb_release_dates')).count():,} rows")
    t.where("kind = 'movie'").agg(
        F.count("*").alias("movies"),
        F.round(F.avg(c("home_release_date").isNotNull().cast("int")) * 100, 1).alias("%has_home_release"),
        F.round(F.avg(c("us_digital_date").isNotNull().cast("int")) * 100, 1).alias("%has_us_digital"),
        F.percentile_approx("theatrical_to_home_days", 0.5).alias("median_days_to_home"),
    ).show()
    t.where("kind = 'movie'").groupBy((F.floor(c("imdb_year") / 5) * 5).alias("period")).agg(
        F.count("*").alias("n"), F.percentile_approx("theatrical_to_home_days", 0.5).alias("median_days_to_home"),
    ).orderBy("period").show()
    spark.stop()


if __name__ == "__main__":
    main()
