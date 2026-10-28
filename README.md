# ExposureIQ — Content Exposure Intelligence

A company-facing local pilot for monitoring movie, television and game torrent exposure, built on
**Distributed Analytics of Global Torrent Activity**, the original CS404 Big Data Analytics research project.

A Lambda-architecture (batch + streaming) pipeline over **1.38 billion rows** of public BitTorrent, film, TV,
game and Wikipedia metadata. Metadata only: no copyrighted content is downloaded and no peer IP addresses are stored.

**Report:** [report/CS404_Torrent_Analytics_Report.pdf](report/CS404_Torrent_Analytics_Report.pdf) (19 pages) ·
**Presentation:** [showcase/Big Data Analytics.pdf](showcase/Big%20Data%20Analytics.pdf) ·
**Schema:** [docs/schema.md](docs/schema.md) · **Build log:** [PHASES.md](PHASES.md)

## Start the Existing Product

With dependencies and research datasets already prepared:

```bash
uv run python -m portal.server
# Open http://localhost:8800
```

Create a company workspace or sign in to an existing account. For the shared streaming feature, Docker,
Java, Kafka/PostgreSQL and the historical serving tables must also be available. On-demand title checks
do not require the Kafka/Spark collector. Do not rerun acquisition, ETL or reset scripts just to start
the site. First-time data preparation is described under [Running it](#running-it).

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

These are historical research results, not current product measurements or proof of infringement.
The six presentation questions use a different order from the analysis modules:

| Slide Question | Analysis Module | Prepared Spark SQL Example |
|---|---|---|
| Q1: Activity and release quality | `analysis.a1_activity` | `\run 3`: category/year uploads; not the complete quality analysis |
| Q2: Decay and persistence | `analysis.a3_decay` | `\run 6`: alive share by upload year; not the fitted decay models |
| Q3: Release-to-listing lag | `analysis.a2_leak_lag` | `\run 4` / `\run 5`: one film / yearly film summary |
| Q4: Game characteristics | `analysis.a5_games` | `\run 7`: price bands; not the full regression |
| Q5: Genre effects | `analysis.a6_genres` | No equivalent preset; use the analysis module and saved results |
| Q6: Public interest | `analysis.a4_wiki_interest` | No equivalent preset; use the analysis module and saved results |

Run these examples with `uv run python -m demo.sql`; `\list` lists prepared queries. Outputs are under
[analysis/results/](analysis/results/). The product's live query presets are separate PostgreSQL queries,
not the six research analyses. Running a historical query now does not make its inputs fresh.

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
PRODUCT FastAPI ──► ExposureIQ website + company workflows (http://localhost:8800)
        SQLite: accounts/catalogues/cases/checks/alerts/reports; PostgreSQL: shared live serving data
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

## ExposureIQ company pilot

```bash
uv run python -m portal.server   # http://localhost:8800
```

The default site is **ExposureIQ**, a product-oriented interface for movie, TV, and game torrent-exposure
intelligence. Its dark, rounded **single scrollable landing page** contains Platform, Pricing, About and
Contact sections. The contact form downloads an enquiry file; it does not pretend to send a message.

Select **Create workspace** to register your company and administrator account; there are no default
passwords. The authenticated workspace includes private catalogue CSV onboarding and match approval,
on-demand title-level seeder checks, saved observation history, assigned investigations and notes,
threshold alerts, CSV/JSON/print-to-PDF reports, daily/weekly summary snapshots, invitations and roles,
read-only API keys, audit history and operator health. Email/webhook alert delivery is optional and
must be configured by the operator. [Operator and user-test guide](docs/CWATCH_PILOT.md).

### Company Catalogue and Demo Workflow

The **Research Catalogue** is the shared historical source. The **Company Catalogue** is a private
selection of titles for a company's saved checks, rules, cases and reports. Uploading is optional:
search in **Check Live Seeders** and choose **Add to Catalogue** for individual titles.

For bulk onboarding, import a UTF-8 comma-separated CSV with exactly these unique headers:

```csv
title,category,external_id
Interstellar,movie,tt0816692
The Sopranos,tv,
Portal 2,game,
```

`external_id` is optional; the alternative header is `title,category`. Keep only the selected columns
when saving from a spreadsheet: extra unnamed columns can produce duplicate blank headers. Accepted
categories are `movie`, `tv` and `game`; imports allow up to 200 rows and 1 MB. Imports are atomic:
validation failure imports nothing. Imported titles remain Pending until a user approves a candidate.
An approved match is not independent verification of ownership, rights or infringement. Titles absent
from the source catalogue cannot be made monitorable simply by importing their names.

Demonstrate **Import → Review → Approve → Check → Investigate → Report**:

1. Import an illustrative catalogue and approve the correct title associations.
2. Before checking, create an enabled title-specific alert rule in **Alerts & Rules**.
   A low demonstration threshold is for testing the workflow, not a commercial risk recommendation.
3. Use **Check Seeders** on an approved company title. Portfolio checks are saved and evaluated against
   rules; ad-hoc title searches/checks are not automatically saved as company check history.
4. Inspect the timestamp, responding trackers, sampled hashes and failures. A rule triggers only on a
   successful qualifying check; an existing unresolved alert for that rule/title prevents duplicates.
5. Create an assigned investigation, update its status and add an actual review note.
6. Reopen saved records and export the company summary, catalogue CSV or complete check JSON.

Catalogue CSV exports describe title identities and approval decisions, not tracker measurements.
`title_key` is a catalogue identity (for example `movie|imdb|tt0816692`), not an infohash. The export's
`exported_at` records file generation, not observation time. Use **Export Observations** in check results
or **Download JSON** in saved checks for measurements, coverage and observation timestamps.

Signing out preserves company records and does not stop live collection. A newly registered company
has empty private workflows, while another member of the same company sees its shared records.
The research catalogue and streaming history are shared across companies and do not reset on login.

The research catalogue also provides historical search, category/upload-year/activity filters, pagination,
record details, a browser-local personal watchlist scoped by company/user, and CSV exports.
Its read-only `/api/contentwatch/catalogue` endpoint filters the complete `curated/swarm_torrent_day`
Parquet table; the UI displays 25 records per page and permits browsing the first 10,025 matching records.
Page exports contain only the displayed records. CSVs include source/mode fields and observation timestamps.
Missing datasets and failed requests are shown as errors, not replaced with sample data.

### Live monitoring and SQL

Open **Live pipeline & SQL** in the authenticated workspace (`/#/live`). Administrators choose 100, 500, or 2,000
historically busy known torrent hashes, then select **Start live monitoring**. The server checks/starts
the existing Kafka and PostgreSQL services and launches the existing tracker scraper and Spark stream
processor. Docker access, Java, and the historical `public.swarm_latest`/`public.torrents` serving data
are required. Initial startup can take 1–2 minutes; unreachable public UDP trackers can delay observations.

The page shows committed observations, distinct monitored hashes, freshness, recent throughput, and
the retained serving snapshot. It also provides example queries, an editable read-only PostgreSQL
console, and query-result CSV exports. Available views:

- `live_latest`: latest observation per `(infohash, tracker)`, joined to catalogue titles/categories.
- `live_minutes`: per-minute/per-tracker event counts and accumulated peer-count observations.
- `live_batches`: committed Spark serving batches.

The query field is labelled **Query**; the former SQL Workspace badge has been removed. The execution
engine is still PostgreSQL, not Spark SQL. The five presets are **Latest Observations**, **Activity by
Category**, **Minute Throughput**, **All Retained Observations** and **Stream Batch Ledger**. Throughput
measures collected records, not audience size. Freshness depends on actual observation timestamps.

Queries use a dedicated non-superuser database login with SELECT access to these views, read-only
transactions, a five-second statement timeout, a one-second lock timeout, and a 200-row result cap.
Use `WHERE scraped_at >= now() - interval '10 minutes'` to distinguish fresh records from retained runs.
This monitors **known hashes**, not newly discovered releases; tracker counts are not unique people.
Full raw event history is retained in Parquet; these SQL views expose the serving snapshots/aggregates.

The ExposureIQ pipeline is isolated: topic `tracker.scrapes.cwatch`, PostgreSQL schema `cwatch`,
`dataset/stream/cwatch`, `_checkpoints_cwatch`, and `jsonl_cwatch`. Start/stop never deletes data,
topics, or checkpoints. Stop halts only the processes owned by the ExposureIQ monitor, not shared Docker
services. In-flight Kafka records can be processed after the next start. Logs are written to
`logs/cwatch-stream.log` and `logs/cwatch-scraper.log`.

Collection continues across page navigation and stops on **Stop collection** or a normal portal
shutdown. Run a single portal server process: its database reader credentials and process ownership
are managed in memory. Keep this administrative interface bound to localhost.

**Pilot boundaries:** company records are authenticated and organisation-isolated; the public research
catalogue and live pipeline are intentionally shared. Title checks are requested on demand, not an
automated new-release discovery or continuous company-catalogue scanning service. Personal watchlists
remain browser-local. Scheduled summaries and optional delivery require the portal to remain running.
There is no billing, server-submitted contact request, email verification, MFA or password-recovery service.
Keep the single-process server bound to localhost. Internet/commercial deployment requires licensed sources
(including review/replacement of IMDb non-commercial inputs), HTTPS and additional operational safeguards.
Historical observations date from 10 Oct 2026 over listings ending May 2023; title checks have their own
actual timestamps. Counts are not unique people, proof of infringement, downloads or revenue loss.

The dark visual theme pairs rounded navy surfaces with blue accents across the public site and
workspace. It uses local assets and system fonts and does not require external web services.

Headings and interface labels use consistent capitalization while user-entered titles, emails, hashes
and SQL retain their original casing. Check results use responsive cards and horizontally scrollable
tables. **System Health** separates database size, background maintenance and notification configuration;
configured email/webhooks are not proof of delivery. The sidebar retains **Sign Out** but no longer
shows Settings & Data, Back to Website or a top-right IQ shortcut.

```bash
uv run python -m unittest discover -s portal -p 'test_*.py'
```

## Research portal and live demo (for the presentation)

```bash
CWATCH_ENABLE_RESEARCH=1 uv run python -m portal.server
                                 # http://localhost:8800/research.html — administrator login required; overview, live stream, Kafka messages, dataset browser,
                                 # Spark SQL in the browser, findings, links to Grafana / Kafka UI (:8080) / Spark UI
```

```bash
demo/live_demo.sh start      # WARNING: resets the separate classroom-demo topic, tables, files and checkpoints
                             # Grafana "Live Demo" (refresh 5 s): http://localhost:3000/d/live-demo
uv run python -m demo.sql    # interactive Spark SQL over registered research tables; \list shows examples
demo/live_demo.sh stop
```
The classroom stream is isolated from both the 10 Oct research collection and the product's `cwatch`
pipeline. Do not use its reset script to restart ExposureIQ. Queries 10/11 in the research console read
the separate `demo_swarm` output, not the product's stream. Spark SQL execution and large scans may take
time; rehearse with the available machine resources. Step-by-step: [demo/DEMO.md](demo/DEMO.md).

## Repository

```
common/      Spark session (UTC, zstd), data paths, .env loader
ingest/      downloaders, TMDB fetchers, BEP 15 tracker scraper, scrape-target builders
etl/         Spark jobs raw → staging → curated
streaming/   Structured Streaming job and its start / stop / scheduled-stop / reboot-recovery scripts
analysis/    A1–A7 (Spark SQL + MLlib), validation, figure style; results/*.csv
dashboards/  results → PostgreSQL loader; Grafana live, demo and batch dashboard generators
demo/        live classroom demo (start/stop), interactive Spark SQL console, prepared queries
portal/      ExposureIQ: FastAPI, company SQLite store, public site, authenticated workspace, legacy research UI
infra/       Docker Compose, PostgreSQL schema, Grafana provisioning + dashboard JSON, scraper image
report/      final report (PDF + HTML source) and figures
docs/        dataset schema and company-pilot operator guide
showcase/    presentation slides (PDF)
```

Research datasets, local company databases, logs and credentials are not published with the source.
The local `torrent_data.zip` archive is not included in the repository; obtain/prepare the required data
separately. Do not commit company databases, passwords, API keys or `.env` files.
