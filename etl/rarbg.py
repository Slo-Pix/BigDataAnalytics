"""RARBG SQLite dump -> staging/torrents (Parquet, partitioned by category).

Step 1 (bronze): stream the `items` table out of SQLite in chunks into staging/rarbg_bronze (Parquet).
Step 2 (Spark):  dedup by infohash, normalise RARBG categories, parse release names with guessit
                 (parallel via mapInPandas), write staging/torrents.

Usage: uv run python -m etl.rarbg
"""
import sqlite3
import time
from collections.abc import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from common.spark import RAW, STAGING, get_spark

DB = RAW / "rarbg" / "rarbg_db.sqlite"
BRONZE = STAGING / "rarbg_bronze"
DST = STAGING / "torrents"
EXPECTED = {"hash", "title", "dt", "cat", "size", "imdb"}

PARSED_SCHEMA = (
    "infohash string, parsed_title string, year int, season int, episode int, resolution string, "
    "source string, video_codec string, release_group string, guess_type string"
)


def export_bronze(chunk: int = 250_000) -> int:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(items)")}
    missing = EXPECTED - cols
    if missing:
        raise SystemExit(f"items table is missing {missing}; actual columns: {sorted(cols)}")
    BRONZE.mkdir(parents=True, exist_ok=True)
    for f in BRONZE.glob("*.parquet"):
        f.unlink()
    n = 0
    query = "SELECT hash, title, dt, cat, size, imdb FROM items"
    for i, df in enumerate(pd.read_sql_query(query, con, chunksize=chunk)):
        df = df.astype({"hash": "string", "title": "string", "dt": "string", "cat": "string", "imdb": "string"})
        df["size"] = pd.to_numeric(df["size"], errors="coerce").astype("Int64")
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), BRONZE / f"part-{i:04d}.parquet")
        n += len(df)
    con.close()
    return n


def _first(v):
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _int(v):
    v = _first(v)
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def parse_titles(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    from guessit import guessit  # imported on the executor

    for pdf in batches:
        rows = []
        for h, title in zip(pdf["infohash"], pdf["title"]):
            try:
                g = guessit(title)
            except Exception:
                g = {}
            rows.append((h, _first(g.get("title")), _int(g.get("year")), _int(g.get("season")), _int(g.get("episode")),
                         _first(g.get("screen_size")), _first(g.get("source")), _first(g.get("video_codec")),
                         _first(g.get("release_group")), g.get("type")))
        yield pd.DataFrame(rows, columns=[c.split()[0] for c in PARSED_SCHEMA.split(", ")])


def main():
    t0 = time.time()
    n = export_bronze()
    print(f"bronze: {n:,} rows from SQLite in {time.time() - t0:.0f}s")

    spark = get_spark("etl-rarbg", mem="5g")
    c = F.col
    cat = F.lower(c("cat"))
    base = (
        spark.read.parquet(str(BRONZE))
        .select(
            F.lower(F.trim(c("hash"))).alias("infohash"),
            c("title"),
            c("cat").alias("rarbg_cat"),
            F.when(cat.startswith("movies"), "movie").when(cat.startswith("tv"), "tv")
             .when(cat.startswith("games"), "game").when(cat.startswith("music"), "music")
             .when(cat.startswith("xxx"), "adult").otherwise("other").alias("category"),
            F.try_to_timestamp(c("dt")).alias("added_at"),
            c("size").alias("size_bytes"),
            F.when(c("imdb").rlike(r"^tt\d+$"), c("imdb")).alias("imdb_id"),
        )
        .where(c("infohash").rlike(r"^[0-9a-f]{40}$") & c("title").isNotNull())
        .dropDuplicates(["infohash"])
        .repartition(48)
        .cache()
    )
    parsed = base.select("infohash", "title").mapInPandas(parse_titles, PARSED_SCHEMA)
    torrents = base.join(parsed, "infohash").withColumn("added_year", F.year("added_at"))
    torrents.write.mode("overwrite").partitionBy("category").parquet(str(DST))

    out = spark.read.parquet(str(DST))
    print(f"wrote {DST}: {out.count():,} torrents in {(time.time() - t0) / 60:.1f} min")
    out.groupBy("category").agg(
        F.count("*").alias("torrents"), F.round(F.avg(c("imdb_id").isNotNull().cast("int")) * 100, 1).alias("%imdb"),
        F.min("added_at").alias("first"), F.max("added_at").alias("last"),
    ).orderBy(F.desc("torrents")).show()
    out.select("title", "category", "parsed_title", "year", "season", "episode", "resolution").show(8, truncate=45)
    spark.stop()


if __name__ == "__main__":
    main()
