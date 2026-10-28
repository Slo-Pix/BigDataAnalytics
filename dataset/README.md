# ExposureIQ Data Storage

Research files and the default company database live here (git-ignored except this file).
Metadata only: the project does not download copyrighted media or store peer IP addresses.
Kafka and PostgreSQL are separate Docker-backed services; their records are not all stored in this folder.

```
dataset/
├── raw/                          source data exactly as downloaded / fetched
│   ├── imdb/                     IMDb non-commercial TSVs (basics, ratings, akas, principals, episode)
│   ├── wikipedia/
│   │   └── pageviews_hourly/     Wikimedia hourly pageview dumps, 2026-09-01 .. 09-07 (168 files)
│   ├── rarbg/                    rarbg_db.sqlite (torrent catalog, 2007–2023)
│   ├── steam/                    Kaggle "Steam Games Dataset" (fronkongames) + Steam/SteamSpy API pulls
│   ├── tmdb/                     TMDB release dates (JSONL from the API)
│   └── scrape_targets/           research infohashes.txt; cwatch.txt for product stream; demo.txt for classroom stream
├── stream/                       speed layer output
│   ├── jsonl/<date>/<hour>.jsonl raw tracker scrapes (backup of the Kafka topic)
│   ├── swarm/date=…/             Parquet written by Spark Structured Streaming
│   ├── _checkpoints/             research Spark streaming checkpoints (do not edit)
│   ├── cwatch/date=…/            ExposureIQ pipeline observation history in Parquet
│   ├── jsonl_cwatch/             ExposureIQ scraper JSONL output
│   ├── _checkpoints_cwatch/      product lake/serving progress (do not edit)
│   ├── demo/                    separate classroom-demo Parquet output, when used
│   ├── jsonl_demo/              classroom-demo scraper output, when used
│   └── _checkpoints_demo/       classroom-demo progress, when used
├── app/
│   └── cwatch.sqlite3           private company accounts and workflows (never commit)
├── staging/                      cleaned Parquet, one folder per source (Phase 2)
└── curated/                      joined analysis tables handed to analytics (Phase 4)
```
Column definitions: [../docs/schema.md](../docs/schema.md).

## Which Store Serves Which Feature?

| Store | Contents and Use |
|---|---|
| `curated/swarm_torrent_day` | Historical Research Catalogue search, filters and record exports |
| Other staging/curated Parquet | Research joins, Spark SQL, statistical analyses and models |
| Kafka topic `tracker.scrapes.cwatch` | Buffered product tracker events, subject to Kafka retention |
| PostgreSQL `cwatch.swarm_latest` | Latest retained observation per infohash/tracker pair |
| PostgreSQL `cwatch.swarm_minute` | Minute-level collection aggregates |
| PostgreSQL `cwatch.stream_batches` | Committed serving-batch ledger |
| `stream/cwatch/` | Retained product observations in Parquet |
| `app/cwatch.sqlite3` | Accounts, company catalogue, saved portfolio checks, investigations, rules, alerts, reports, audit and credentials represented by hashes |
| Browser local storage | Personal watchlists, scoped by company/user; not synchronised across devices |

The default SQLite location can be overridden using `CWATCH_APP_DB`. SQLite may create `-wal` and `-shm`
sidecar files during operation. Use the documented backup/restore tool, not an arbitrary copy of the
active database. Never share the database or sidecars as demo files.

The product's query editor runs **read-only PostgreSQL**, exposing `live_latest`, `live_minutes` and
`live_batches` in the `cwatch` schema. Spark Structured Streaming writes the underlying serving data;
the editor does not run Spark SQL. For Spark SQL over the research lake, run
`uv run python -m demo.sql` from the repository root.

The dashboard's committed-observation total comes from the serving batch ledger across retained runs.
It is not the row count of `swarm_latest`, which retains a snapshot. Kafka, Parquet and PostgreSQL are
separate processing/storage surfaces; a timestamped measurement is not a unique viewer or download.

## Catalogue Imports and Exports

Company CSV imports accept unique headers `title,category` with optional `external_id`. Supported
categories are `movie`, `tv` and `game`. A title is a catalogue identity; an infohash identifies a
particular torrent. Several hashes can correspond to one title.

Imported entries and their approved `title_key` are stored in the company database, not written into the
shared research catalogue. Adding an unknown title does not discover its torrents. Catalogue exports
contain matching/workflow fields; check JSON and observation CSV exports contain tracker measurements.
Downloads are saved through the browser to the user's chosen destination, not into this data lake.

## Persistence, Isolation and Safe Operation

- Research data, the classroom `demo` stream and the product `cwatch` stream use separate paths/topics/schemas.
- Company records are organisation-isolated. Public research metadata and the live pipeline are shared.
- Signing out or creating a new company does not erase research or streaming observations.
- **Stop Collection** preserves committed product data and checkpoints. Restarting resumes retained progress.
- Checkpoints store processing progress, not a complete backup of observations.
- `demo/live_demo.sh start` deliberately resets the separate classroom-demo area. Do not use it to restart
  the product or preserve a previous classroom session.
- Do not delete `dataset/`, truncate shared tables, or rerun acquisition/ETL just to prepare a presentation.
- The repository excludes local datasets and `torrent_data.zip`; a source checkout alone does not include
  the populated data lake or company accounts.

For startup, role permissions, backups and demo instructions see the
[project README](../README.md) and [company-pilot guide](../docs/CWATCH_PILOT.md).

## Known data quirks
- **steam/games.csv** (Kaggle fronkongames, v53, 125,855 games): the header has 39 names but every row has 40
  values, because the header merges `Discount` and `DLC count` into `DiscountDLC count`. Replace that header
  name with the two columns when reading, or every column after `Price` is shifted by one.
- **steam/games.json** is the same data as one large JSON object keyed by AppID (not line-delimited),
  so Spark can't split it; convert it to JSONL first if it's needed.
