"""Validation: evidence that the pipeline is complete and the measurements are right.

  1. Reconciliation  raw source rows  ->  staging rows (nothing silently lost; every drop explained)
  2. Stream integrity Kafka offsets = Postgres batch ledger = Parquet rows = JSONL backup lines; time window
  3. Measurement      agreement between independent trackers on the same torrent at the same time
  4. Spot checks      known release dates and parsed titles for well-known films

Writes analysis/results/validation.csv (check, expected, observed, status) and prints it.

Usage: uv run python -m analysis.validate
"""
import gzip
import json
import sqlite3
import subprocess

import pandas as pd
import psycopg
from pyspark.sql import functions as F

from analysis.style import save_table
from common.spark import RAW, STAGING, STREAM, get_spark

PG = "postgresql://bda:bda@localhost:5432/torrent"
checks = []


def check(name, expected, observed, ok=None, note=""):
    ok = (expected == observed) if ok is None else ok
    checks.append({"check": name, "expected": expected, "observed": observed, "status": "PASS" if ok else "CHECK", "note": note})
    print(f"[{'PASS' if ok else 'CHECK'}] {name}: expected {expected}, observed {observed} {note}")


def gz_lines(path) -> int:
    with gzip.open(path, "rb") as f:
        return sum(1 for _ in f)


def main():
    spark = get_spark("validate", mem="5g")
    c = F.col

    # ---- 1. reconciliation -------------------------------------------------------------------
    con = sqlite3.connect(f"file:{RAW / 'rarbg' / 'rarbg_db.sqlite'}?mode=ro", uri=True)
    n_sqlite = con.execute("SELECT count(*) FROM items").fetchone()[0]
    n_distinct = con.execute("SELECT count(DISTINCT lower(hash)) FROM items").fetchone()[0]
    torrents = spark.read.parquet(str(STAGING / "torrents"))
    check("RARBG: SQLite rows = staging rows", n_sqlite, torrents.count())
    check("RARBG: staging infohashes unique", n_distinct, torrents.select("infohash").distinct().count())

    raw_wiki = spark.read.text(str(RAW / "wikipedia" / "pageviews_hourly" / "*.gz")).count()
    wiki = spark.read.parquet(str(STAGING / "wiki_pageviews")).count()
    check("Wikipedia: raw lines vs staging rows", raw_wiki, wiki, ok=0 <= raw_wiki - wiki <= raw_wiki * 0.001,
          note=f"dropped {raw_wiki - wiki:,} malformed lines ({(raw_wiki - wiki) / raw_wiki:.4%})")
    check("Wikipedia: 168 hourly files present", 168,
          spark.read.parquet(str(STAGING / "wiki_pageviews")).select("hour").distinct().count())

    for name, table in [("title.basics", "imdb_titles"), ("title.akas", "imdb_akas"),
                        ("title.principals", "imdb_principals"), ("title.episode", "imdb_episodes")]:
        raw = gz_lines(RAW / "imdb" / f"{name}.tsv.gz") - 1
        check(f"IMDb {name}: raw rows = staging rows", raw, spark.read.parquet(str(STAGING / table)).count())

    steam_raw = int(subprocess.run(
        ["uv", "run", "python", "-c", "import csv,sys; csv.field_size_limit(sys.maxsize); "
         "r=csv.reader(open('dataset/raw/steam/games.csv', newline='', encoding='utf-8')); next(r); print(sum(1 for _ in r))"],
        capture_output=True, text=True).stdout.strip())
    steam = spark.read.parquet(str(STAGING / "steam_games")).count()
    check("Steam: raw rows vs staging rows (dedup by app_id)", steam_raw, steam, ok=steam_raw - steam <= 5,
          note=f"{steam_raw - steam} duplicate/invalid app_id dropped")

    tmdb_lines = [json.loads(line) for line in open(RAW / "tmdb" / "tmdb_raw.jsonl")]
    ok_lines = sum(1 for r in tmdb_lines if r["status"] == "ok")
    check("TMDB: fetched 'ok' records = staging titles", ok_lines, spark.read.parquet(str(STAGING / "tmdb_titles")).count())

    # ---- 2. stream integrity --------------------------------------------------------------------
    kafka = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                            "localhost:9092", "--topic", "tracker.scrapes"], capture_output=True, text=True).stdout
    kafka_total = sum(int(line.rsplit(":", 1)[1]) for line in kafka.split() if line.count(":") == 2)
    with psycopg.connect(PG) as pg:
        ledger_rows, ledger_batches = pg.execute("SELECT sum(rows), count(*) FROM stream_batches").fetchone()
        dup_batches = pg.execute("SELECT count(*) - count(DISTINCT (query, batch_id)) FROM stream_batches").fetchone()[0]
    swarm = spark.read.parquet(str(STREAM / "swarm"))
    parquet_rows = swarm.count()
    jsonl_rows = sum(sum(1 for _ in open(f)) for d in ("jsonl", "jsonl_wide") for f in (STREAM / d).rglob("*.jsonl"))
    check("Stream: Kafka messages = Postgres ledger rows", kafka_total, int(ledger_rows))
    check("Stream: Kafka messages = Parquet rows", kafka_total, parquet_rows)
    check("Stream: no micro-batch applied twice", 0, dup_batches, note=f"{ledger_batches} micro-batches")
    check("Stream: JSONL backup lines = Parquet rows", parquet_rows, jsonl_rows, ok=abs(jsonl_rows - parquet_rows) <= parquet_rows * 0.002,
          note="JSONL write buffer not flushed when the scraper containers were force-stopped at 21:00; Kafka/Parquet complete")
    span = swarm.agg(F.min("scraped_at"), F.max("scraped_at")).first()
    check("Stream: all observations inside 10 Oct 10:36–21:01 IST", "05:06–15:31 UTC",
          f"{span[0]:%H:%M}–{span[1]:%H:%M} UTC", ok=span[0].hour >= 5 and span[1].hour <= 15)
    neg = swarm.where((c("seeders") < 0) | (c("leechers") < 0) | (c("completed") < 0)).count()
    check("Stream: no negative counts", 0, neg)

    # ---- 3. measurement agreement between trackers -------------------------------------------------
    slots = (swarm.where(c("tracker").isin("tracker.opentrackr.org:1337", "open.stealth.si:80"))
             .withColumn("slot", F.window("scraped_at", "20 minutes").start)
             .groupBy("infohash", "slot").pivot("tracker").agg(F.avg("seeders"))
             .withColumnRenamed("tracker.opentrackr.org:1337", "opentrackr").withColumnRenamed("open.stealth.si:80", "stealth")
             .dropna())
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.stat import Correlation
    vec = VectorAssembler(inputCols=["opentrackr", "stealth"], outputCol="v").transform(slots).select("v")
    rho = Correlation.corr(vec, "v", "spearman").head()[0].toArray()[0, 1]
    pairs = slots.count()
    check("Trackers agree: Spearman ρ(opentrackr, stealth.si) seeders", "> 0.7", round(float(rho), 3), ok=rho > 0.7,
          note=f"{pairs:,} torrent-slot pairs; independent trackers measuring the same swarms")
    fs = spark.read.parquet(str(STAGING / "swarm_fullscan")).select("infohash", c("seeders").alias("scan"))
    live_open = (swarm.where((c("tracker") == "tracker.opentrackr.org:1337")
                             & (c("scraped_at") < F.lit("2026-10-10 07:30:00").cast("timestamp")))
                 .groupBy("infohash").agg(F.avg("seeders").alias("live")))
    j = fs.join(live_open, "infohash")
    vec2 = VectorAssembler(inputCols=["scan", "live"], outputCol="v").transform(j).select("v")
    rho2 = Correlation.corr(vec2, "v", "spearman").head()[0].toArray()[0, 1]
    check("Full scan agrees with live scrapes (same tracker, ~same time)", "> 0.8", round(float(rho2), 3), ok=rho2 > 0.8,
          note=f"{j.count():,} torrents")

    # ---- 4. spot checks against known facts -------------------------------------------------------
    tmdb = spark.read.parquet(str(STAGING / "tmdb_titles")).toPandas().set_index("imdb_id")
    known = {"tt1375666": ("Inception", "2010-07-15"), "tt4154796": ("Avengers: Endgame", "2019-04-24"),
             "tt0468569": ("The Dark Knight", "2008-07-16"), "tt0816692": ("Interstellar", "2014-11-05")}
    for imdb_id, (title, date) in known.items():
        got = str(tmdb.loc[imdb_id, "theatrical_date"]) if imdb_id in tmdb.index else "missing"
        check(f"Spot check: {title} first theatrical date", date, got, ok=abs((pd.Timestamp(got) - pd.Timestamp(date)).days) <= 3
              if got != "missing" else False, note="TMDB vs public record (±3 days for premiere vs release)")
    # guessit on real release names of two well-known titles (Inception tt1375666, The Mandalorian tt8111088)
    sample = (torrents.where(c("imdb_id").isin("tt1375666", "tt8111088")).orderBy("added_at").limit(400)
              .toPandas().groupby("imdb_id").head(2))
    expected = {"tt1375666": ("Inception", 2010), "tt8111088": ("The Mandalorian", None)}
    for r in sample.itertuples():
        title, year = expected[r.imdb_id]
        ok = (r.parsed_title or "").lower() == title.lower() and (year is None or r.year == year) \
            and (year is not None or (r.season is not None and r.season == r.season))
        check(f"Spot check guessit: {r.title[:60]}", f"{title}" + (f" ({year})" if year else " + season/episode"),
              f"{r.parsed_title} | year {r.year} | S{r.season} E{r.episode} | {r.resolution}", ok=ok)

    spark.stop()
    df = pd.DataFrame(checks)
    save_table(df, "validation")
    print(f"\n{(df.status == 'PASS').sum()} / {len(df)} checks passed")


if __name__ == "__main__":
    main()
