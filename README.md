# Distributed Analytics of Global Torrent Activity (CS404)

Lambda-style pipeline over public BitTorrent metadata. Phase plan and status: [PHASES.md](PHASES.md).
Table contract: [docs/schema.md](docs/schema.md). Metadata only: no content is downloaded and no peer IPs are stored.

## Setup
```bash
uv sync                                            # Python env (PySpark 4.0, guessit, kafka, psycopg)
docker compose -f infra/docker-compose.yml up -d   # Kafka :9092, Postgres :5432, Grafana :3000 (admin/admin)
```

## Data acquisition (Phase 1)
```bash
ingest/download_imdb.sh
ingest/download_wiki_pageviews.sh 2026-09-01 7
uv run python -m ingest.export_infohashes --n 100000   # needs dataset/raw/rarbg/rarbg_db.sqlite
```

## Speed layer (Phase 3)
```
tracker_scraper (container, hourly rounds, BEP 15 UDP)
   ├─> dataset/stream/jsonl/<date>/<hour>.jsonl          raw backup
   └─> Kafka topic tracker.scrapes (6 partitions, 7-day retention)
          └─> Spark Structured Streaming (streaming/swarm_stream.py, 1-min micro-batches)
                 ├─> dataset/stream/swarm/date=…/*.parquet   data lake (append)
                 └─> Postgres swarm_minute / swarm_latest  serving layer -> Grafana "Live Torrent Swarms"
```
```bash
docker compose -f infra/docker-compose.yml --profile scraper up -d   # 24/7 scraper (restarts on reboot)
streaming/run_stream.sh start | status | stop                        # Spark streaming job
```
The Postgres writer records each micro-batch id in `stream_batches` in the same transaction, so a
restart that replays a batch never double-counts (exactly-once into the serving layer).

Optional HDFS: `docker compose -f infra/docker-compose.yml --profile hdfs up -d` (NameNode UI :9870).
