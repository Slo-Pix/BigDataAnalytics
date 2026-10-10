# Dataset schema

All tables are Parquet (zstd) under `dataset/`. Layers: **raw** (as downloaded) → **staging** (cleaned, one
table per source) → **curated** (joined, analysis-ready). The live speed layer writes `stream/swarm`.
Metadata only: no content is downloaded and no peer IP addresses are stored.

```
                          imdb_id (tt…)                         app_id
 staging/imdb_titles ◄──────────────────┐            ┌────────────────────► staging/steam_games
 staging/tmdb_titles ◄──────────────────┤            │   (title match,
                                        │            │    curated/steam_matches)
                       staging/torrents (infohash) ──┤
                                        │            │
 stream/swarm ──── infohash ────────────┤            └── curated/torrents_enriched (infohash)
 staging/swarm_fullscan ── infohash ────┘                        │ imdb_id / app_id
                                                                 ▼
 staging/wiki_pageviews ── page_title ──► curated/wiki_interest (entity_type, entity_id)
```

Row counts are as of 10 Oct 2026; `stream/swarm` and the swarm tables grow until live collection stops (21:00 IST).

## Staging

### staging/torrents — RARBG catalog · 2,843,691 rows · key `infohash` · partitioned by `category`
| column | type | notes |
|---|---|---|
| infohash | string | 40-char lowercase hex (SHA-1 of the torrent info dict) |
| title | string | raw release name |
| rarbg_cat | string | original RARBG category, e.g. `movies_x265_4k`, `tv_uhd`, `games_pc_iso` |
| category | string | `movie` · `tv` · `game` · `music` · `adult` · `other` |
| added_at, added_year | timestamp, int | upload time on RARBG (2007-06 … 2023-05) |
| size_bytes | long | |
| imdb_id | string | `tt…`, from RARBG; null for games / music / adult |
| parsed_title, year, season, episode | string, int, int, int | from guessit |
| resolution, source, video_codec, release_group, guess_type | string | from guessit (`1080p`, `Web`, `H.265`, …) |

### staging/wiki_pageviews — 1,153,632,847 rows · partitioned by `date`
`project` (e.g. `en`, `en.m` = English mobile) · `page_title` · `views` int · `hour` timestamp (UTC) · `date`. Wikimedia hourly dumps, 1–7 Sep 2026.

### staging/imdb_titles — 12,849,219 rows · key `tconst` · partitioned by `title_type`
`tconst, title_type, primary_title, original_title, title_norm, is_adult, start_year, end_year, runtime_min, genres array<string>, avg_rating, num_votes`

### staging/imdb_akas — 59,678,827 rows
`tconst, ordering, title, title_norm, region, language, aka_types, is_original` — localised / alternative titles.

### staging/imdb_principals — 102,198,330 rows
`tconst, ordering, nconst, category, job, characters` — cast and crew.

### staging/imdb_episodes — 9,935,383 rows
`tconst, parent_tconst, season, episode`.

### staging/tmdb_titles — 13,462 rows · key `imdb_id`
| column group | columns |
|---|---|
| identity | imdb_id, kind (`movie`/`tv`), source (`imdb_top` / `tmdb_discover_in`), discover_language, tmdb_id, title, imdb_title, imdb_year, imdb_votes, imdb_rating |
| metadata | popularity, original_language, genres array, runtime, budget, revenue |
| TV | first_air_date, last_air_date, number_of_seasons, number_of_episodes, networks array, in_production |
| release windows (movies) | theatrical_date (earliest type 2/3, any country), digital_date (type 4), physical_date (type 5), **home_release_date** = least(digital, physical) on/after theatrical, us_digital_date, theatrical_to_home_days |
| India | in_theatrical_date, in_home_release_date, in_theatrical_to_home_days |

### staging/tmdb_release_dates — 212,933 rows
`imdb_id, country, type (1 premiere · 2 limited · 3 theatrical · 4 digital · 5 physical · 6 TV), date, note`.

### staging/steam_games — 125,854 rows · key `app_id`
`app_id, name, name_norm, release_date, release_year, price_usd, is_free, discount_pct, dlc_count, required_age, owners_low, owners_high, owners_mid, peak_ccu, positive, negative, reviews_total, review_score, metacritic_score, recommendations, avg_playtime_min, median_playtime_min, windows, mac, linux, developers / publishers / categories / genres / tags (array<string>), content_notes`.

### staging/swarm_fullscan — 1,385,068 rows · key `infohash`
`infohash, seeders, leechers, completed, scraped_at` — one opentrackr scrape of every movie/TV/game torrent, 10 Oct 2026 11:41–12:29 IST.

## Stream (speed layer)

### stream/swarm — live tracker scrapes · ~27M rows by 21:00 · partitioned by `date`
| column | type | notes |
|---|---|---|
| infohash | string | |
| tracker | string | `tracker.opentrackr.org:1337`, `open.stealth.si:80`, `tracker.torrent.eu.org:451`, `exodus.desync.com:6969` |
| seeders, leechers | int | peers with the complete / incomplete file, as counted by that tracker |
| completed | int | cumulative completed downloads reported by that tracker |
| scraped_at | timestamp | UTC |

Written by Spark Structured Streaming from Kafka topic `tracker.scrapes`. Postgres holds per-minute
summaries (`swarm_minute`, `swarm_latest`, `stream_batches`) and torrent titles (`torrents`) for Grafana.

## Curated

### curated/torrents_enriched — 1,390,470 rows (movie / tv / game) · key `infohash` · partitioned by `category`
All `staging/torrents` columns, plus:
| from | columns |
|---|---|
| IMDb | imdb_title_type, imdb_primary_title, imdb_start_year, imdb_genres, imdb_rating, imdb_votes, imdb_runtime_min |
| TMDB | tmdb_id, tmdb_source, original_language, tmdb_genres, budget, revenue, popularity, theatrical_date, home_release_date, digital_date, physical_date, us_digital_date, in_theatrical_date, in_home_release_date, first_air_date |
| Steam (games) | steam_app_id, steam_match (`exact` / `no_edition` / `base_game_prefix`), game_name, steam_name, steam_release_date, price_usd, is_free, review_score, reviews_total, owners_low/high/mid, steam_genres, metacritic_score |
| derived | days_from_theatrical, days_from_home, days_from_in_home, days_from_steam (upload date − release date; negative = before release), display_title |

### curated/steam_matches — 37,758 rows
`infohash, game_name (cleaned release name), k1, k2 (match keys), app_id, match_type`.

### curated/wiki_interest — 57,284 rows · key (`entity_type`, `entity_id`)
`entity_type (imdb / steam), entity_id, category, name, page_title, rank (1 = "(YEAR film)" … 3 = bare name), views_week, confidence (high / medium / low)`.

### curated/wiki_daily — 362,477 rows
`entity_type, entity_id, page_title, date, views` (en + en.m).

### curated/swarm_slots · key (`infohash`, `slot`)
`infohash, slot (10-minute window start, UTC), seeders, leechers, completed (max over trackers), n_trackers`.

### curated/swarm_torrent_day · key `infohash`
`n_slots, first_slot, last_slot, seeders_median/max/min/mean/std, seeders_cv, leechers_median/max, leech_seed_ratio, alive, completed_first, completed_last, growth_hours, completed_per_hour (opentrackr only)` + torrent metadata (category, display_title, added_at, added_year, imdb_id, imdb_rating, imdb_votes, steam_app_id, price_usd, review_score, original_language, theatrical_date, home_release_date).

### curated/swarm_category_slots
`category, slot, torrents, seeders, leechers`.

### curated/decay_by_year
`category, added_year, torrents, alive, pct_alive, total_seeders, mean_seeders, median_seeders, p90_seeders, p99_seeders`.

## Notes for analysis
- **TMDB release dates (leak lag):** US digital dates are often missing or late re-releases, so `home_release_date` uses the earliest digital/physical date in any country on or after the first theatrical date.
- **RARBG catalog completeness:** the dump only holds torrents still listed in May 2023; before ~2017 many original releases had been replaced, so leak-lag analysis uses films released 2018+.
- **Wikipedia matches:** drop `confidence = 'low'` (single-word bare page names such as "Krakatoa" often point to a different topic).
- **Swarm counts:** each tracker sees part of a swarm; use the max over trackers per slot, and only compare `completed` within one tracker.
