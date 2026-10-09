# Parquet schema contract (A ↔ B)

B writes analyses against these tables; A guarantees the columns. Add columns freely, never rename/remove without telling the other person.

Paths are relative to `dataset/`.

## staging/torrents  (partitioned by `category`)
| column      | type      | notes |
|-------------|-----------|-------|
| infohash    | string    | 40-char lowercase hex, primary key |
| title       | string    | raw release name |
| category    | string    | `movie` · `tv` · `game` · `music` · `other` |
| added_at    | timestamp | upload time on RARBG |
| size_bytes  | long      | |
| imdb_id     | string    | `tt1234567`, nullable |
| parsed_title| string    | from guessit |
| year        | int       | from guessit, nullable |
| season      | int       | nullable |
| episode     | int       | nullable |
| resolution  | string    | e.g. `1080p`, nullable |

## stream/swarm  (partitioned by `date`)
| column     | type      | notes |
|------------|-----------|-------|
| infohash   | string    | |
| tracker    | string    | tracker host:port |
| seeders    | int       | |
| completed  | int       | total downloads reported by tracker |
| leechers   | int       | |
| scraped_at | timestamp | UTC |
| date       | date      | partition |

## staging/imdb_titles
`tconst, title_type, primary_title, start_year, runtime_min, genres (array<string>), avg_rating, num_votes`

## staging/wiki_pageviews  (partitioned by `date`)
`project, page_title, hour (timestamp), views`

## staging/steam_games
`app_id, name, release_date, price_usd, positive, negative, owners_est, genres (array<string>)`

## staging/tmdb_releases
`imdb_id, tmdb_id, theatrical_date, digital_date, physical_date`

No peer IPs are ever stored. No content is downloaded — metadata only.

### Note on TMDB release dates (for leak-lag)
`dataset/raw/tmdb/tmdb_raw.jsonl` keeps *every* release date for every country. US digital (type 4) is
often missing for pre-2013 films or is a much later re-release (e.g. The Avengers 2012 → US digital 2024).
Define **home_release = earliest type 4 or 5 date in any country that falls on/after the first theatrical date**,
and treat titles without one as missing rather than dropping the filter.
