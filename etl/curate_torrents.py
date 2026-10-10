"""Phase 4: curated/torrents_enriched — one row per movie / TV / game torrent with everything joined in.

  movie, tv  ⨝ imdb_titles on imdb_id (RARBG stores the IMDb id)  ⨝ tmdb_titles on imdb_id
  game       ⨝ steam_games on a cleaned, normalised title (game torrents have no IMDb id)

Derived columns for the leak-lag analysis:
  days_from_theatrical  added_at - theatrical_date      (negative = torrent before cinema release)
  days_from_home        added_at - home_release_date    (negative = torrent before legal home release)
  days_from_steam       added_at - steam release_date

Also writes curated/steam_matches (how each game torrent was matched) and, with --pg, loads
torrent titles into Postgres `torrents` so the live Grafana dashboard shows names, not hashes.

Usage: uv run python -m etl.curate_torrents [--pg]
"""
import argparse
import io

from pyspark.sql import Column, Window, functions as F

from common.spark import CURATED, STAGING, get_spark

GROUP_MIN_TORRENTS = 15  # a "-SUFFIX" that ends at least this many game torrents is a scene group
CUT_MARKERS = (
    r"(?i)\s+(v\s?\d|build\s?\d|update|hotfix|patch\b|multi\d*|read\s?nfo|incl\b|repack|proper|crack|"
    r"usa\b|eur\b|pal\b|ntsc|jb\b|ps3\b|ps4\b|x360|xbox\s?360|region|dlc\b|iso\b|gog\b|\(|\[).*$"
)
EDITIONS = (
    r"(?i)\s+(enhanced|deluxe|complete|definitive|ultimate|gold|anniversary|remastered|goty|"
    r"game of the year|directors cut|special|collectors|premium)(\s+edition)?.*$"
)


def norm(c: Column) -> Column:
    return F.regexp_replace(F.lower(c), r"[^a-z0-9]+", "")


def clean_game_title(title: Column, groups: list[str]) -> Column:
    t = title
    if groups:
        t = F.regexp_replace(t, r"-(" + "|".join(groups) + r")$", "")
    t = F.regexp_replace(t, r"[._]+", " ")
    t = F.regexp_replace(t, CUT_MARKERS, "")
    return F.trim(t)


def match_games(spark, games):
    """games: DataFrame(infohash, title). Returns infohash -> steam app_id with how it matched."""
    c = F.col
    groups = [
        r["g"] for r in games.select(F.regexp_extract("title", r"-([A-Za-z0-9]+)$", 1).alias("g"))
        .where(c("g") != "").groupBy("g").count().where(c("count") >= GROUP_MIN_TORRENTS).collect()
    ]
    cleaned = games.select(
        "infohash", clean_game_title(c("title"), groups).alias("game_name")
    ).withColumn("k1", norm(c("game_name"))).withColumn("k2", norm(F.regexp_replace("game_name", EDITIONS, "")))

    steam = spark.read.parquet(str(STAGING / "steam_games"))
    # several Steam apps can share a normalised name (demos, soundtracks): keep the most-reviewed one
    def keyed(key_expr):
        return (
            steam.withColumn("k", key_expr).where(c("k") != "")
            .withColumn("rank", F.row_number().over(Window.partitionBy("k").orderBy(F.desc("reviews_total"))))
            .where(c("rank") == 1).select("k", "app_id")
        )
    exact = keyed(c("name_norm"))
    no_edition = keyed(norm(F.regexp_replace("name", EDITIONS, "")))

    m1 = cleaned.join(exact.withColumnRenamed("k", "k1"), "k1").select("infohash", "app_id", F.lit("exact").alias("match_type"))
    rest = cleaned.join(m1.select("infohash"), "infohash", "left_anti")
    m2 = rest.join(no_edition.withColumnRenamed("k", "k2"), "k2").select("infohash", "app_id", F.lit("no_edition").alias("match_type"))
    # DLC / expansion / update releases ("Outer Wilds Echoes of the Eye") -> base game: longest word prefix
    # (>= 2 words, >= 8 chars) that is an exact Steam name
    rest = rest.join(m2.select("infohash"), "infohash", "left_anti")
    words = rest.select("infohash", F.split(F.trim("game_name"), r"\s+").alias("w"))
    prefixes = (
        words.select("infohash", "w", F.explode(F.sequence(F.lit(2), F.size("w") - 1)).alias("n"))
        .where(F.size("w") > 2)
        .select("infohash", "n", norm(F.array_join(F.slice("w", 1, c("n")), " ")).alias("k"))
        .where(F.length("k") >= 8)
    )
    m3 = (
        prefixes.join(exact, "k")
        .withColumn("rank", F.row_number().over(Window.partitionBy("infohash").orderBy(F.desc("n"))))
        .where(c("rank") == 1)
        .select("infohash", "app_id", F.lit("base_game_prefix").alias("match_type"))
    )
    matches = m1.unionByName(m2).unionByName(m3).dropDuplicates(["infohash"])
    return cleaned.join(matches, "infohash", "left"), groups


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg", action="store_true", help="also load titles into Postgres for Grafana")
    args = ap.parse_args()

    spark = get_spark("curate-torrents", mem="5g")
    c = F.col
    torrents = spark.read.parquet(str(STAGING / "torrents")).where(c("category").isin("movie", "tv", "game"))

    imdb = spark.read.parquet(str(STAGING / "imdb_titles")).select(
        c("tconst").alias("imdb_id"), c("title_type").alias("imdb_title_type"),
        c("primary_title").alias("imdb_primary_title"), c("start_year").alias("imdb_start_year"),
        c("genres").alias("imdb_genres"), c("avg_rating").alias("imdb_rating"),
        c("num_votes").alias("imdb_votes"), c("runtime_min").alias("imdb_runtime_min"),
    )
    tmdb = spark.read.parquet(str(STAGING / "tmdb_titles")).select(
        "imdb_id", "tmdb_id", c("source").alias("tmdb_source"), "original_language",
        c("genres").alias("tmdb_genres"), "budget", "revenue", "popularity",
        "theatrical_date", "home_release_date", "digital_date", "physical_date", "us_digital_date",
        "in_theatrical_date", "in_home_release_date", "first_air_date",
    )

    game_matches, groups = match_games(spark, torrents.where(c("category") == "game").select("infohash", "title"))
    game_matches.write.mode("overwrite").parquet(str(CURATED / "steam_matches"))
    game_matches = spark.read.parquet(str(CURATED / "steam_matches"))
    steam = spark.read.parquet(str(STAGING / "steam_games")).select(
        c("app_id").alias("steam_app_id"), c("name").alias("steam_name"), c("release_date").alias("steam_release_date"),
        "price_usd", "is_free", "review_score", "reviews_total", "owners_low", "owners_high", "owners_mid",
        c("genres").alias("steam_genres"), "metacritic_score",
    )

    enriched = (
        torrents.join(imdb, "imdb_id", "left")
        .join(tmdb, "imdb_id", "left")
        .join(game_matches.select("infohash", "game_name", c("app_id").alias("steam_app_id"), c("match_type").alias("steam_match")),
              "infohash", "left")
        .join(steam, "steam_app_id", "left")
        .withColumn("days_from_theatrical", F.datediff(F.to_date("added_at"), "theatrical_date"))
        .withColumn("days_from_home", F.datediff(F.to_date("added_at"), "home_release_date"))
        .withColumn("days_from_in_home", F.datediff(F.to_date("added_at"), "in_home_release_date"))
        .withColumn("days_from_steam", F.datediff(F.to_date("added_at"), "steam_release_date"))
        .withColumn("display_title", F.coalesce("imdb_primary_title", "steam_name", "parsed_title", "title"))
    )
    enriched.write.mode("overwrite").partitionBy("category").parquet(str(CURATED / "torrents_enriched"))

    out = spark.read.parquet(str(CURATED / "torrents_enriched"))
    print(f"torrents_enriched: {out.count():,} rows; {len(groups)} scene groups detected for game titles")
    out.groupBy("category").agg(
        F.count("*").alias("torrents"),
        F.round(F.avg(c("imdb_primary_title").isNotNull().cast("int")) * 100, 1).alias("%imdb"),
        F.round(F.avg(c("tmdb_id").isNotNull().cast("int")) * 100, 1).alias("%tmdb"),
        F.round(F.avg(c("home_release_date").isNotNull().cast("int")) * 100, 1).alias("%home_date"),
        F.round(F.avg(c("steam_app_id").isNotNull().cast("int")) * 100, 1).alias("%steam"),
    ).orderBy("category").show()
    spark.read.parquet(str(CURATED / "steam_matches")).groupBy("match_type").count().show()

    if args.pg:
        load_pg_titles(out)
    spark.stop()


def load_pg_titles(enriched):
    import psycopg

    pdf = enriched.select("infohash", F.substring("display_title", 1, 300).alias("title"), "category").toPandas()
    buf = io.StringIO()
    pdf.to_csv(buf, index=False, header=False)
    with psycopg.connect("postgresql://bda:bda@localhost:5432/torrent") as con, con.cursor() as cur:
        cur.execute("TRUNCATE torrents")
        with cur.copy("COPY torrents (infohash, title, category) FROM STDIN WITH (FORMAT csv)") as cp:
            cp.write(buf.getvalue())
    print(f"postgres torrents: {len(pdf):,} titles loaded")


if __name__ == "__main__":
    main()
