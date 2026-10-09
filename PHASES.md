# Build Phases — Distributed Analytics of Global Torrent Activity

Derived from `BDA_Project_Plan.pdf` (1-week scope). Owner: A = Shubham (data eng), B = Ajay (analytics).
Status: `[ ]` todo · `[~]` in progress · `[x]` done

## Phase 0 — Foundation (Day 1, A)
- [x] Repo layout: `ingest/ etl/ streaming/ analysis/ dashboards/ report/ infra/ dataset/ logs/`
- [x] Python env (`uv`) with PySpark, guessit, kafka client
- [x] Schema contract between A and B → `docs/schema.md`
- [x] Spark local smoke test (local[*], 12 cores)

## Phase 1 — Data acquisition (Day 1–2, A + B)  ← START FIRST
- [~] IMDb TSVs (basics, ratings, akas, principals) → `dataset/raw/imdb/`
- [ ] RARBG catalog dump (SQLite) → `dataset/raw/rarbg/`
- [~] Wikipedia hourly pageviews, 7–14 days → `dataset/raw/wikipedia/pageviews_hourly/`
- [x] Kaggle Steam games dataset → `dataset/raw/steam/` (games.csv 401 MB, 125,855 games; games.json 965 MB)
- [ ] TMDB release dates for top 5–10k titles → `dataset/raw/tmdb/`  (B, needs TMDB API key)
- [~] **Tracker scraper (BEP 15 UDP) running 24/7 → JSONL** `dataset/stream/jsonl/` (built + tested; waiting on RARBG hash list)

## Phase 2 — Batch ETL to Parquet (Day 3, A)
- [ ] RARBG → `staging/torrents` (dedup by infohash, category normalisation)
- [ ] Title parsing with guessit (title, year, season/episode, quality)
- [ ] IMDb, Wikipedia, Steam, TMDB → `staging/*` Parquet, partitioned
- [ ] Delete raw `.gz` once converted

## Phase 3 — Streaming / speed layer (Day 2–3, A)
- [x] Docker Compose: Kafka 3.9 (KRaft) + Postgres 16 + Grafana 13 — running, healthy
- [x] Scraper publishes to topic `tracker.scrapes` (also containerised, `--profile scraper`, restarts on reboot)
- [x] Spark Structured Streaming: Kafka → Parquet `stream/swarm` (by date) + Postgres serving tables, 1-min trigger
- [x] Exactly-once into Postgres via `stream_batches` (replay test passed)
- [x] Grafana "Live Torrent Swarms" dashboard provisioned, all 8 panel queries verified
- [ ] Go live: start scraper container + `streaming/run_stream.sh start` once `dataset/raw/scrape_targets/infohashes.txt` exists
- [ ] HDFS profile (`--profile hdfs`) — written, not yet tested (big image pull; optional per plan)

## Phase 4 — Integration / curated tables (Day 4, A → handoff to B)
- [ ] torrents ⨝ IMDb ⨝ TMDB ⨝ Wikipedia ⨝ Steam → `curated/*`

## Phase 5 — Analytics + MLlib (Day 2–5, B; on samples first)
- [ ] Volume by category × year (activity index)
- [ ] Popularity decay: seeders vs age (exp / power-law fit)
- [ ] Leak lag: TMDB digital release vs first upload
- [ ] Wikipedia interest vs swarm size
- [ ] Games: price/rating vs activity (MLlib regression)
- [ ] Genres pirated relative to popularity

## Phase 6 — Dashboards (Day 5, B)
- [ ] Grafana dashboards (batch results + live swarm)

## Phase 7 — Report & demo (Day 6–7, both)
- [ ] Runtime / volume stats, report, slides, rehearsal. Feature freeze end of Day 5.
