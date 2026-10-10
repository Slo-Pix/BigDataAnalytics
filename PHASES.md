# Build Phases — Distributed Analytics of Global Torrent Activity

Derived from `BDA_Project_Plan.pdf` (1-week scope). **Ajay is not contributing (2026-10-10): Shubham owns every phase**, so the A/B split below is historical.
Status: `[ ]` todo · `[~]` in progress · `[x]` done — all phases complete

## Phase 0 — Foundation (Day 1, A)
- [x] Repo layout: `ingest/ etl/ streaming/ analysis/ dashboards/ report/ infra/ dataset/ logs/`
- [x] Python env (`uv`) with PySpark, guessit, kafka client
- [x] Schema contract between A and B → `docs/schema.md`
- [x] Spark local smoke test (local[*], 12 cores)

## Phase 1 — Data acquisition (Day 1–2, A + B)  ← START FIRST
- [x] IMDb TSVs (basics, ratings, akas, principals) → `dataset/raw/imdb/`
- [~] RARBG catalog dump (SQLite, 775 MB, archive.org/details/rarbg_db, md5-verified) → `dataset/raw/rarbg/` (downloading)
- [x] Wikipedia hourly pageviews, 7 days (2026-09-01..07, 168 files, 9.9 GB) → `dataset/raw/wikipedia/pageviews_hourly/`
- [x] Kaggle Steam games dataset → `dataset/raw/steam/` (games.csv 401 MB, 125,855 games; games.json 965 MB)
- [x] TMDB release dates, top 8k movies + 2k TV (IMDb votes, 2005–2023) → `dataset/raw/tmdb/` (9,956 found, 19 not found)
- [~] **Tracker scraper (BEP 15 UDP) running 24/7 → JSONL** `dataset/stream/jsonl/` (built + tested; waiting on RARBG hash list)

## Phase 2 — Batch ETL to Parquet (Day 3, A)
- [ ] RARBG → `staging/torrents` (dedup by infohash, category normalisation)
- [ ] Title parsing with guessit (title, year, season/episode, quality)
- [~] IMDb, Wikipedia, Steam, TMDB → `staging/*` Parquet (Steam ✅ 125,854 games; Wikipedia running; IMDb + TMDB scripts ready)
- [ ] Delete raw `.gz` once converted

## Phase 3 — Streaming / speed layer — DONE: live 10:36–21:00 IST, 25,422,936 rows, 0 lost (Kafka offsets = rows applied), no gap > 5 min
- [x] Docker Compose: Kafka 3.9 (KRaft) + Postgres 16 + Grafana 13 — running, healthy
- [x] Scraper publishes to topic `tracker.scrapes` (also containerised, `--profile scraper`, restarts on reboot)
- [x] Spark Structured Streaming: Kafka → Parquet `stream/swarm` (by date) + Postgres serving tables, 1-min trigger
- [x] Exactly-once into Postgres via `stream_batches` (replay test passed)
- [x] Grafana "Live Torrent Swarms" dashboard provisioned, all 8 panel queries verified
- [ ] Go live: start scraper container + `streaming/run_stream.sh start` once `dataset/raw/scrape_targets/infohashes.txt` exists
- [ ] HDFS profile (`--profile hdfs`) — written, not yet tested (big image pull; optional per plan)

## Phase 4 — Integration / curated tables
- [x] `curated/torrents_enriched` 1,390,470 movie/TV/game torrents ⨝ IMDb ⨝ TMDB (+ leak-lag day columns) ⨝ Steam (79% of game torrents, 81.6% of PC; exact / no-edition / base-game-prefix)
- [x] Postgres `torrents` titles loaded → Grafana shows names
- [x] `curated/swarm_slots`, `swarm_torrent_day`, `swarm_category_slots` (live; re-run after 21:00: `uv run python -m etl.curate_swarm`)
- [x] `curated/wiki_interest` + `wiki_daily`: 57,284 titles matched to en Wikipedia pages (confidence high/medium/low) in 1 min over 1.15B rows
- [x] Full catalog scan: 1,385,068 / 1,390,470 torrents (99.6%), 124,476 alive → `staging/swarm_fullscan`; two-tier live scraping (16.5k panel + 110k wide)

## Phase 5 — Analytics + MLlib (figures: report/figures, tables: analysis/results)
- [x] A1 activity index: uploads by category/year, resolution mix, x265 share
- [x] A2 leak lag (films 2018+; catalog incomplete earlier): median 9 d in 2020–21, ~70% on RARBG within a day of legal release; TV median 1 d
- [x] A3 decay per title (MLlib Poisson GLM): movies power law k=0.73; TV and games grow with age (no decay)
- [x] A4 Wikipedia interest vs seeders (MLlib Spearman): movie 0.47, TV 0.41, game 0.24
- [x] A5 games (MLlib logistic AUC 0.83 + random forest): free 5% pirated, $5–40 ~53%, >$40 38%
- [x] A6 genres (MLlib OLS residuals): Sci-Fi +59%, Horror +56%, Romance −18%
- [x] A7 intra-day swarms (opentrackr, fixed panel 110k torrents, 12:40–20:50 IST): movie/TV within ±1.5%, games +11% into the European/US day; leechers per seeder TV 0.42, movie 0.35, game 0.24

## Phase 6 — Dashboards — DONE
- [x] Grafana "Live Torrent Swarms" (speed layer, Postgres serving tables, 8 panels)
- [x] Grafana "Batch Analytics" (A1–A7 + dataset KPIs + data-lake inventory, 25 panels) from Postgres schema `analytics`
      (`dashboards/load_results.py` → `dashboards/build_batch_dashboard.py`); all queries verified, rendered with headless Chrome

## Phase 7 — Report & validation — DONE
- [x] `analysis/validate.py`: 26 / 26 checks pass (source → Parquet reconciliation, Kafka = ledger = Parquet, tracker agreement, spot checks)
- [x] Final report: `report/CS404_Torrent_Analytics_Report.pdf` (19 pages, HTML source `report/report.html`)
- [ ] Slides (in progress)
- [x] Repo cleanup: lint clean, dashboard builders in `dashboards/`, README rewritten, Kafka log on its volume for future runs

**Status: project complete (10 Oct 2026).**
