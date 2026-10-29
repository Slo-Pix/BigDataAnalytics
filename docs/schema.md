# ExposureIQ data and identity reference

Research Parquet uses `raw -> staging -> curated`; stream events are retained separately.
Saved counts below describe the historical October 2026 research run, not a fresh validation.
Authoritative field definitions live in ETL/streaming code and the SQLite initialisers.

## Identity and joins

| Identity | Meaning | Use |
|---|---|---|
| `infohash` | 40-character hexadecimal torrent identity | Join tracker events to a particular release |
| `imdb_id` / `tconst` | IMDb title identity | Movie/TV metadata, release dates and research joins |
| `app_id` / `steam_app_id` | Steam game identity | Game matching and analysis |
| `entity_type, entity_id` | IMDb/Steam research entity | Wikipedia interest tables |
| `title_key` | Product title identity, e.g. `movie\|imdb\|tt0816692` | Company approval and title checks; not an infohash |
| `org_id` | Private company boundary | Scope company records, users and access |

Several torrents can represent one title. A title match is not a rights assertion. Aggregating
tracker observations by sum can double-count overlapping swarms.

## Research lake

| Path below `dataset/` | Grain / important fields | Saved count or purpose |
|---|---|---|
| `staging/torrents` | `infohash`; category, title, `added_at`, IMDb ID, parsed resolution/source/codec | 2,843,691 RARBG records; 2007-May 2023 |
| `staging/wiki_pageviews` | project, page title, UTC hour, date, views | 1,153,632,847 valid rows; 168 hourly files |
| `staging/imdb_titles` | `tconst`; title type, names, year, genres, rating/votes | 12,849,219 rows |
| `staging/imdb_akas` | `tconst`, ordering; localised title/region/language | 59,678,827 rows |
| `staging/imdb_principals` | title/person/category/job | 102,198,330 rows |
| `staging/imdb_episodes` | episode title, parent series, season, episode | 9,935,383 rows |
| `staging/tmdb_titles` | IMDb ID; theatrical, digital, physical, home-release dates; TV premiere | 13,462 titles |
| `staging/tmdb_release_dates` | IMDb ID, country, release type, date | 212,933 release records |
| `staging/steam_games` | app ID; price/free status, reviews, genres/categories, release date | 125,854 games |
| `staging/swarm_fullscan` | infohash; seeders, leechers, completed, scrape timestamp | 1,385,068 observations, 10 Oct 2026 |
| `curated/torrents_enriched` | infohash; source fields plus IMDb/TMDB/Steam matches and release lags | Movie/TV/game analysis |
| `curated/steam_matches` | infohash, app ID, match type | Name-based game associations |
| `curated/wiki_interest` | entity type/ID; page, weekly views, confidence | English desktop/mobile interest |
| `curated/wiki_daily` | entity type/ID, date, views | Daily interest |
| `curated/swarm_slots` | infohash, slot; max-over-tracker counts and coverage | Cross-tracker research summaries |
| `curated/swarm_torrent_day` | infohash; count summaries, activity, metadata and release dates | Historical product catalogue |
| `curated/swarm_category_slots` | category, slot; swarm summaries | Category research |
| `curated/decay_by_year` | category, upload year; alive/count distributions | Cross-sectional research |

`home_release_date` is the earliest digital/physical date on or after theatrical release.
Release lags are upload date minus the corresponding release date; negative values mean
an earlier listing. Catalogue incompleteness can hide older first uploads.

## Tracker events and serving stores

Kafka JSON fields are `infohash`, `tracker`, integer `seeders`, `leechers`, `completed`, and
`scraped_at`. Spark parses the timestamp and adds the Parquet partition `date`.
The parser drops events lacking a hash or valid timestamp. `completed` is a tracker-reported
cumulative counter, not independently verified unique downloads.

| Surface | Research | Product | Classroom |
|---|---|---|---|
| Kafka topic | `tracker.scrapes` | `tracker.scrapes.cwatch` | Separate demo topic; see demo guide |
| Parquet | `stream/swarm` | `stream/cwatch` | `stream/demo` |
| Checkpoints | `stream/_checkpoints` | `stream/_checkpoints_cwatch` | `stream/_checkpoints_demo` |
| PostgreSQL schema | `public` | `cwatch` | `demo` |

PostgreSQL stores `swarm_latest` (latest per infohash/tracker), `swarm_minute` (minute/tracker
event counts and accumulated seeder/leecher observations), and `stream_batches` (query/batch ID,
applied row count and commit time). The serving writer records the batch ledger and updates
aggregates in one transaction so replayed batch IDs do not apply twice.

The product's read-only views are `cwatch.live_latest` (joined title metadata),
`cwatch.live_minutes`, and `cwatch.live_batches`. The latest snapshot and batch-ledger total have
different grains; do not compare them as if both counted raw events. PostgreSQL SQL is distinct
from research Spark SQL. Retained rows need timestamp filtering to represent recent collection.

## Company SQLite database

Default: `dataset/app/cwatch.sqlite3`, overridable by `CWATCH_APP_DB`.
All private workflow access must be scoped by the current organisation.

| Tables | Contents |
|---|---|
| `organisations`, `users` | Companies, members, roles and password hashes |
| `sessions`, `invitations`, `api_keys` | Hashed access tokens and expiry/revocation state |
| `audit_log` | Company/user action, entity, detail, timestamp |
| `company_portfolio` | Imported/selected title, candidates, explicit approval and `title_key` |
| `company_checks` | Saved portfolio observations, maximum seeders, freshness and complete result JSON |
| `company_rules`, `company_alerts` | Thresholds, title association, qualifying checks, alert state |
| `company_cases`, `company_notes` | Assigned investigations, status and actual analyst notes |
| `company_deliveries` | Email/webhook channel status and explicit errors |
| `company_reports`, `company_report_runs` | Daily/weekly UTC schedules and persisted summary snapshots |

Browser watchlists are not SQLite records. Catalogue CSV exports describe workflow identity;
observation CSV and check JSON describe measurements with their observation times.
Never commit the database, sidecars, secrets or private exports. Use the backup API documented
in [the operator guide](EXPOSUREIQ_PILOT.md#backup-and-restore).

## Saved results and validation

`analysis/results/` contains the tracked research CSVs and the archived Kafka offset snapshot.
`validation.csv` records 26 historical PASS statuses, but its notes matter: the JSONL backup is
1,111 records short because a buffer was not flushed, and its timestamp-envelope check records
a minute-level boundary discrepancy. Kafka/ledger/Parquet row reconciliation is separate from
those caveats. The original research topic was later removed; these are archived results,
not proof that the current live deployment passes the same checks.
