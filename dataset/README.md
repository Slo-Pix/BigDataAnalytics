# dataset/

All project data lives here (git-ignored except this file). Metadata only: no copyrighted content, no peer IPs.

```
dataset/
├── raw/                          source data exactly as downloaded / fetched
│   ├── imdb/                     IMDb non-commercial TSVs (basics, ratings, akas, principals, episode)
│   ├── wikipedia/
│   │   └── pageviews_hourly/     Wikimedia hourly pageview dumps, 2026-09-01 .. 09-07 (168 files)
│   ├── rarbg/                    rarbg_db.sqlite (torrent catalog, 2007–2023)
│   ├── steam/                    Kaggle "Steam Games Dataset" (fronkongames) + Steam/SteamSpy API pulls
│   ├── tmdb/                     TMDB release dates (JSONL from the API)
│   └── scrape_targets/           infohashes.txt fed to the tracker scraper
├── stream/                       speed layer output
│   ├── jsonl/<date>/<hour>.jsonl raw tracker scrapes (backup of the Kafka topic)
│   ├── swarm/date=…/             Parquet written by Spark Structured Streaming
│   └── _checkpoints/             Spark streaming checkpoints (do not edit)
├── staging/                      cleaned Parquet, one folder per source (Phase 2)
└── curated/                      joined analysis tables handed to analytics (Phase 4)
```
Column definitions: [../docs/schema.md](../docs/schema.md).

## Known data quirks
- **steam/games.csv** (Kaggle fronkongames, v53, 125,855 games): the header has 39 names but every row has 40
  values, because the header merges `Discount` and `DLC count` into `DiscountDLC count`. Replace that header
  name with the two columns when reading, or every column after `Price` is shifted by one.
- **steam/games.json** is the same data as one large JSON object keyed by AppID (not line-delimited),
  so Spark can't split it; convert it to JSONL first if it's needed.
