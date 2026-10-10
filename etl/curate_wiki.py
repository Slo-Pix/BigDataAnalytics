"""Phase 4: Wikipedia interest per title (curated/wiki_interest, curated/wiki_daily).

Candidate page names are generated for every movie / TV title (IMDb) and matched game (Steam) that has
torrents, e.g. `Inception`, `Inception_(film)`, `Inception_(2010_film)`. English desktop + mobile
pageviews (project `en` / `en.m`, 2026-09-01..07) are filtered against the candidates with a broadcast
join, so the 1.15B-row table is scanned once without a shuffle. Per title the most specific page that
has views wins: (year + type) > (type) > bare title, which avoids disambiguation pages like `Up`.

Usage: uv run python -m etl.curate_wiki
"""
import time

from pyspark.sql import Window, functions as F

from common.spark import CURATED, STAGING, get_spark

SUFFIX = {"movie": "film", "tv": "TV_series", "game": "video_game"}


def page_name(c):
    """Wikipedia page-title form: spaces -> underscores, trademark signs dropped, first letter upper-case."""
    t = F.regexp_replace(F.trim(F.regexp_replace(c, "[®™©]", "")), r"\s+", "_")
    return F.concat(F.upper(F.substring(t, 1, 1)), F.substring(t, 2, 10_000))


def main():
    spark = get_spark("curate-wiki", mem="5g")
    c = F.col
    t0 = time.time()
    e = spark.read.parquet(str(CURATED / "torrents_enriched"))

    titles = (
        e.where(c("imdb_primary_title").isNotNull())
        .select(F.lit("imdb").alias("entity_type"), c("imdb_id").alias("entity_id"), "category",
                c("imdb_primary_title").alias("name"), c("imdb_start_year").alias("year"))
        .unionByName(
            e.where(c("steam_app_id").isNotNull())
            .select(F.lit("steam").alias("entity_type"), c("steam_app_id").cast("string").alias("entity_id"),
                    "category", c("steam_name").alias("name"), F.year("steam_release_date").alias("year")))
        .dropDuplicates(["entity_type", "entity_id"])
    )
    suffix = F.when(c("category") == "movie", SUFFIX["movie"]).when(c("category") == "tv", SUFFIX["tv"]).otherwise(SUFFIX["game"])
    base = page_name(c("name"))
    candidates = titles.select(
        "entity_type", "entity_id", "category", "name",
        F.explode(F.array(
            F.struct(F.concat(base, F.lit("_("), c("year").cast("string"), F.lit("_"), suffix, F.lit(")")).alias("page_title"), F.lit(1).alias("rank")),
            F.struct(F.concat(base, F.lit("_("), suffix, F.lit(")")).alias("page_title"), F.lit(2).alias("rank")),
            F.struct(base.alias("page_title"), F.lit(3).alias("rank")),
        )).alias("cand"),
    ).select("entity_type", "entity_id", "category", "name", "cand.page_title", "cand.rank").where(c("page_title").isNotNull())
    candidates = candidates.cache()
    n_titles, n_cand = titles.count(), candidates.count()

    views = (
        spark.read.parquet(str(STAGING / "wiki_pageviews"))
        .where(c("project").isin("en", "en.m"))
        .join(F.broadcast(candidates.select("page_title").distinct()), "page_title")
        .groupBy("page_title", "date").agg(F.sum("views").alias("views"))
    )
    views.write.mode("overwrite").parquet(str(CURATED / "_wiki_page_daily"))
    views = spark.read.parquet(str(CURATED / "_wiki_page_daily"))

    page_total = views.groupBy("page_title").agg(F.sum("views").alias("views_week"))
    best = (
        candidates.join(page_total, "page_title")
        .withColumn("pick", F.row_number().over(Window.partitionBy("entity_type", "entity_id").orderBy("rank")))
        .where(c("pick") == 1).drop("pick")
    )
    best.write.mode("overwrite").parquet(str(CURATED / "wiki_interest"))
    best = spark.read.parquet(str(CURATED / "wiki_interest"))
    (best.join(views, "page_title").select("entity_type", "entity_id", "page_title", "date", "views")
     .write.mode("overwrite").parquet(str(CURATED / "wiki_daily")))

    print(f"wiki: {n_titles:,} titles, {n_cand:,} candidate pages -> {best.count():,} titles with a page "
          f"in {(time.time() - t0) / 60:.1f} min")
    best.groupBy("entity_type", "category").agg(
        F.count("*").alias("matched"), F.percentile_approx("views_week", 0.5).alias("median_views_week"),
        F.sum((c("rank") == 1).cast("int")).alias("year_specific"), F.sum((c("rank") == 3).cast("int")).alias("bare"),
    ).orderBy("entity_type", "category").show()
    best.orderBy(F.desc("views_week")).select("category", "name", "page_title", "views_week").show(12, truncate=40)
    spark.stop()


if __name__ == "__main__":
    main()
