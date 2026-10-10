-- Prepared Spark SQL for the live demo. In the console: \list shows them, \run N runs one.
-- Every table in dataset/staging, dataset/curated and dataset/stream is registered under its folder name.

-- 1. Scale: count 1.15 billion Wikipedia pageview rows
SELECT count(*) AS rows, sum(views) AS total_views, count(DISTINCT hour) AS hours FROM wiki_pageviews;

-- 2. Most-read English Wikipedia pages, 1-7 Sep 2026 (desktop + mobile)
SELECT page_title, sum(views) AS views FROM wiki_pageviews
WHERE project IN ('en', 'en.m') AND page_title NOT LIKE '%:%' AND page_title <> 'Main_Page'
GROUP BY page_title ORDER BY views DESC LIMIT 15;

-- 3. A1 activity: uploads per category per year
SELECT added_year, count_if(category = 'movie') AS movie, count_if(category = 'tv') AS tv,
       count_if(category = 'game') AS game, count_if(category = 'music') AS music, count_if(category = 'adult') AS adult
FROM torrents WHERE added_year BETWEEN 2008 AND 2023 GROUP BY added_year ORDER BY added_year;

-- 4. A2 one film end to end: cinema date, legal home release, first torrent and its source (change the title!)
SELECT display_title, theatrical_date, home_release_date, min(added_at) AS first_torrent,
       min_by(source, added_at) AS first_source, count(*) AS versions,
       datediff(to_date(min(added_at)), home_release_date) AS days_after_home_release
FROM torrents_enriched WHERE category = 'movie' AND lower(display_title) = 'dune' AND imdb_start_year = 2021
GROUP BY display_title, theatrical_date, home_release_date;

-- 5. A2 leak lag by release year (median days cinema -> first torrent, films 2018+)
WITH first AS (SELECT imdb_id, min(theatrical_date) AS th, min(home_release_date) AS home, min(added_at) AS first_upload
               FROM torrents_enriched WHERE category = 'movie' AND theatrical_date >= '2018-01-01' GROUP BY imdb_id)
SELECT year(th) AS release_year, count(*) AS films,
       percentile_approx(datediff(to_date(first_upload), th), 0.5) AS median_days_to_torrent,
       round(avg(CASE WHEN datediff(to_date(first_upload), home) <= 1 THEN 1.0 ELSE 0 END), 3) AS share_by_day_after_home_release
FROM first WHERE th <= '2023-03-31' GROUP BY 1 ORDER BY 1;

-- 6. A3 decay: share of catalog torrents still alive by upload year (full scan of 1.39 M torrents)
SELECT t.category, t.added_year, count(*) AS torrents, round(avg(CASE WHEN s.seeders > 0 THEN 1.0 ELSE 0 END), 3) AS share_alive
FROM swarm_fullscan s JOIN torrents t USING (infohash)
WHERE t.category IN ('movie', 'tv', 'game') AND t.added_year >= 2015 GROUP BY 1, 2 ORDER BY 1, 2;

-- 7. A5 games: share pirated by price band (Steam 2018-2022, >= 100 reviews)
WITH g AS (SELECT s.app_id, s.price_usd, count(m.infohash) AS torrents FROM steam_games s
           LEFT JOIN steam_matches m ON m.app_id = s.app_id
           WHERE s.release_year BETWEEN 2018 AND 2022 AND s.reviews_total >= 100 GROUP BY s.app_id, s.price_usd)
SELECT CASE WHEN price_usd = 0 THEN '1 free' WHEN price_usd < 5 THEN '2 <$5' WHEN price_usd < 10 THEN '3 $5-10'
            WHEN price_usd < 20 THEN '4 $10-20' WHEN price_usd < 40 THEN '5 $20-40' ELSE '6 >$40' END AS price_band,
       count(*) AS games, round(avg(CASE WHEN torrents > 0 THEN 1.0 ELSE 0 END), 3) AS share_pirated
FROM g GROUP BY 1 ORDER BY 1;

-- 8. Biggest swarms during the 10 Oct live collection (25.4 M streamed rows)
SELECT e.display_title, e.category, max(s.seeders) AS max_seeders, max(s.leechers) AS max_leechers, count(*) AS observations
FROM swarm s JOIN torrents_enriched e USING (infohash) GROUP BY 1, 2 ORDER BY max_seeders DESC LIMIT 15;

-- 9. Stream integrity: rows per tracker in the 10 Oct stream (total must equal Kafka's 25,422,936)
SELECT tracker, count(*) AS rows, min(scraped_at) AS first, max(scraped_at) AS last FROM swarm GROUP BY tracker ORDER BY rows DESC;

-- 10. LIVE: what the demo stream has written to Parquet so far (re-run it: the numbers grow)
SELECT count(*) AS rows, count(DISTINCT infohash) AS torrents, max(scraped_at) AS latest_observation FROM demo_swarm;

-- 11. LIVE: busiest swarms right now, from the demo stream
SELECT e.display_title, e.category, max(d.seeders) AS seeders, max(d.leechers) AS leechers, max(d.scraped_at) AS seen
FROM demo_swarm d JOIN torrents_enriched e USING (infohash) GROUP BY 1, 2 ORDER BY seeders DESC LIMIT 10;
