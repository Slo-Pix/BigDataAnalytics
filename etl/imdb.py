"""IMDb TSV dumps -> staging Parquet tables.

  staging/imdb_titles      basics ⨝ ratings: one row per title (~12M)
  staging/imdb_episodes    episode -> parent series, season, episode number
  staging/imdb_akas        alternative / localised titles (~55M), used for title matching
  staging/imdb_principals  cast & crew per title (~95M)

IMDb files are tab-separated, unquoted, with `\\N` for null.

Usage: uv run python -m etl.imdb [--skip-principals]
"""
import argparse
import time

from pyspark.sql import Column, functions as F

from common.spark import RAW, STAGING, get_spark

SRC = RAW / "imdb"


def norm_title(c: Column) -> Column:
    """Lowercase alphanumerics only; the join key for matching torrent / Wikipedia / Steam titles."""
    return F.regexp_replace(F.lower(c), r"[^a-z0-9]+", "")


def read_tsv(spark, name: str):
    return (
        spark.read.option("header", True).option("sep", "\t").option("quote", "")
        .option("nullValue", "\\N").csv(str(SRC / f"{name}.tsv.gz"))
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-principals", action="store_true")
    args = ap.parse_args()

    spark = get_spark("etl-imdb", mem="6g")
    c = F.col
    t0 = time.time()

    ratings = read_tsv(spark, "title.ratings").select(
        "tconst", c("averageRating").try_cast("double").alias("avg_rating"),
        c("numVotes").try_cast("int").alias("num_votes"),
    )
    titles = (
        read_tsv(spark, "title.basics")
        .select(
            "tconst",
            c("titleType").alias("title_type"),
            c("primaryTitle").alias("primary_title"),
            c("originalTitle").alias("original_title"),
            norm_title(c("primaryTitle")).alias("title_norm"),
            (c("isAdult") == "1").alias("is_adult"),
            c("startYear").try_cast("int").alias("start_year"),
            c("endYear").try_cast("int").alias("end_year"),
            c("runtimeMinutes").try_cast("int").alias("runtime_min"),
            F.coalesce(F.split("genres", ","), F.array().cast("array<string>")).alias("genres"),
        )
        .join(ratings, "tconst", "left")
    )
    titles.write.mode("overwrite").partitionBy("title_type").parquet(str(STAGING / "imdb_titles"))

    read_tsv(spark, "title.episode").select(
        "tconst", c("parentTconst").alias("parent_tconst"),
        c("seasonNumber").try_cast("int").alias("season"), c("episodeNumber").try_cast("int").alias("episode"),
    ).write.mode("overwrite").parquet(str(STAGING / "imdb_episodes"))

    read_tsv(spark, "title.akas").select(
        c("titleId").alias("tconst"), c("ordering").try_cast("int").alias("ordering"),
        "title", norm_title(c("title")).alias("title_norm"), "region", "language",
        c("types").alias("aka_types"), (c("isOriginalTitle") == "1").alias("is_original"),
    ).write.mode("overwrite").parquet(str(STAGING / "imdb_akas"))

    if not args.skip_principals:
        read_tsv(spark, "title.principals").select(
            "tconst", c("ordering").try_cast("int").alias("ordering"), "nconst", "category", "job", "characters",
        ).write.mode("overwrite").parquet(str(STAGING / "imdb_principals"))

    print(f"IMDb ETL done in {(time.time() - t0) / 60:.1f} min")
    for name in ["imdb_titles", "imdb_episodes", "imdb_akas"] + ([] if args.skip_principals else ["imdb_principals"]):
        print(f"  {name:16} {spark.read.parquet(str(STAGING / name)).count():>12,} rows")
    t = spark.read.parquet(str(STAGING / "imdb_titles"))
    t.groupBy("title_type").count().orderBy(F.desc("count")).show(10)
    spark.stop()


if __name__ == "__main__":
    main()
