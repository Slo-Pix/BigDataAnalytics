# Distributed Analytics of Global Torrent Activity — CS404 Big Data Analytics

A Lambda-architecture (batch + streaming) pipeline over **1.38 billion rows** of public BitTorrent, film, TV,
game and Wikipedia metadata. Metadata only: no copyrighted content is downloaded and no peer IP addresses are stored.

**Report:** [report/CS404_Torrent_Analytics_Report.pdf](report/CS404_Torrent_Analytics_Report.pdf) (19 pages) ·
**Schema:** [docs/schema.md](docs/schema.md) · **Build log:** [PHASES.md](PHASES.md)

## Results at a glance

| # | Question | Answer |
|---|---|---|
| A1 | How has torrent activity evolved? | TV uploads 1k (2010) → ~100k/year (2018+); 1080p 2% → 62% of movie uploads (2008→2022); x265 1% → 30% (2019→2022) |
| A2 | How fast do films leak? | Median 9 days after cinema release (2020–21); **66–70% on RARBG by the day after legal digital release**, 18–25% before it; new TV series: 1 day |
| A3 | How does popularity decay? | Films: seeders ∝ age^−0.73 (Poisson GLM, power law beats exponential by AIC); TV series and games do not decay |
| A4 | Does public interest track piracy? | Wikipedia views vs current seeders: Spearman ρ = 0.47 movies, 0.41 TV, 0.24 games |
| A5 | What predicts game piracy? | Free-to-play 5% pirated, $5–40 ~53%, >$40 38%; popularity and single-player raise it (MLlib logistic, test AUC 0.83) |
| A6 | Which genres are over-pirated? | vs equally popular films: Sci-Fi +59%, Horror +56%, Documentary +42%; Romance −18%, Comedy −12% |
| A7 | How do swarms change in a day? | Movie/TV within ±1.5%; games +11% into the European/US day; leechers per seeder TV 0.42, movie 0.35, game 0.24 |

All 26 validation checks pass (`uv run python -m analysis.validate`): every source reconciles with its Parquet table,
Kafka messages = PostgreSQL ledger = Parquet rows (25,422,936, zero loss), independent trackers agree (ρ = 0.74).

## Data

| Source | Rows | Period |
|---|---|---|
| Wikipedia hourly pageviews | 1,153,632,847 | 1–7 Sep 2026 |
| IMDb (titles, akas, principals, episodes) | 184,661,759 | current |
| Live tracker scrapes (Kafka → Spark Structured Streaming) | 25,422,936 | 10 Oct 2026, 10:36–21:00 IST |
| RARBG catalog dump | 2,843,691 | 2007 – May 2023 |
| Full-catalog tracker scan | 1,385,068 | 10 Oct 2026 |
| Steam games (Kaggle) | 125,854 | Oct 2026 snapshot |
| TMDB release dates | 13,462 titles · 212,933 dates | films/TV 2005–2023 |

Everything lives under `dataset/` (≈ 34 GB, git-ignored): `raw/` → `staging/` (cleaned Parquet) → `curated/` (joined),
plus `stream/` (live data). See [dataset/README.md](dataset/README.md).

## Architecture

```
BATCH   raw dumps / APIs ──► Spark ETL (PySpark) ──► Parquet data lake ──► Spark SQL + MLlib ──► results
SPEED   2 tracker scrapers (BEP 15 UDP) ──► Kafka ──► Spark Structured Streaming ──► Parquet + PostgreSQL
SERVING PostgreSQL ──► Grafana: "Live Torrent Swarms" + "Batch Analytics" (http://localhost:3000)
```

Tools: Apache Spark 4.0 (PySpark, Spark SQL, Structured Streaming, MLlib), Apache Kafka 3.9 (KRaft), Apache Parquet,
PostgreSQL 16, Grafana 13, Kafka UI, FastAPI, Docker Compose, guessit, uv. One machine: 12 cores, 15 GB RAM, Spark `local[*]`.

## Running it

```bash
uv sync                                               # Python 3.12 env
docker compose -f infra/docker-compose.yml up -d      # Kafka :9092, PostgreSQL :5432, Grafana :3000 (admin/admin)

# 1. acquire (TMDB needs TMDB_TOKEN in .env)
ingest/download_imdb.sh; ingest/download_wiki_pageviews.sh 2026-09-01 7; ingest/download_rarbg.sh
uv run python -m ingest.fetch_tmdb && uv run python -m ingest.fetch_tmdb_india

# 2. batch ETL -> staging, then joins -> curated
etl/run_phase2.sh                                     # Wikipedia, TMDB, IMDb, RARBG (+ scrape targets)
uv run python -m etl.steam
uv run python -m etl.curate_torrents --pg && uv run python -m etl.curate_wiki

# 3. speed layer
ingest/run_fullscan.sh                                # one pass over all catalog torrents
docker compose -f infra/docker-compose.yml --profile scraper up -d
streaming/run_stream.sh start                         # status | stop
streaming/stop_at.sh 21:00 &                          # graceful stop: scrapers, drain Kafka, stop stream

# 4. analyses, figures, dashboards, validation
analysis/run_final.sh                                 # swarm summaries, A1–A7, Grafana tables
uv run python -m analysis.validate
```

## Project portal and live demo (for the presentation)

```bash
uv run python -m portal.server   # http://localhost:8000 — overview, live stream, Kafka messages, dataset browser,
                                 # Spark SQL in the browser, findings, links to Grafana / Kafka UI (:8080) / Spark UI
```

```bash
demo/live_demo.sh start      # demo scraper -> Kafka -> Spark Structured Streaming (10 s batches) -> PostgreSQL
                             # Grafana "Live Demo" (refresh 5 s): http://localhost:3000/d/live-demo
uv run python -m demo.sql    # interactive Spark SQL over every table; \run 1 counts 1.15 B rows in ~10 s
demo/live_demo.sh stop
```
Isolated from the 10 Oct dataset (own topic, Parquet folder and Postgres schema). Step-by-step: [demo/DEMO.md](demo/DEMO.md).

## Repository

```
common/      Spark session (UTC, zstd), data paths, .env loader
ingest/      downloaders, TMDB fetchers, BEP 15 tracker scraper, scrape-target builders
etl/         Spark jobs raw → staging → curated
streaming/   Structured Streaming job and its start / stop / scheduled-stop / reboot-recovery scripts
analysis/    A1–A7 (Spark SQL + MLlib), validation, figure style; results/*.csv
dashboards/  results → PostgreSQL loader; Grafana live, demo and batch dashboard generators
demo/        live classroom demo (start/stop), interactive Spark SQL console, prepared queries
portal/      project portal: FastAPI backend (live data from Postgres, Kafka, Parquet, Docker, Spark) + web UI
infra/       Docker Compose, PostgreSQL schema, Grafana provisioning + dashboard JSON, scraper image
report/      final report (PDF + HTML source) and figures
docs/        dataset schema
```
