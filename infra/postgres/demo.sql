-- Same tables as init.sql, in schema `demo`, for the live classroom demo (demo/live_demo.sh).
CREATE SCHEMA IF NOT EXISTS demo;
SET search_path = demo;
-- Serving layer for Grafana. Written by streaming/swarm_stream.py (foreachBatch).

-- Swarm totals per minute per tracker (live time series).
CREATE TABLE IF NOT EXISTS swarm_minute (
    minute          timestamptz NOT NULL,
    tracker         text        NOT NULL,
    rows            bigint      NOT NULL,
    total_seeders   bigint      NOT NULL,
    total_leechers  bigint      NOT NULL,
    PRIMARY KEY (minute, tracker)
);

-- Latest observation per torrent per tracker.
CREATE TABLE IF NOT EXISTS swarm_latest (
    infohash    char(40)    NOT NULL,
    tracker     text        NOT NULL,
    seeders     int         NOT NULL,
    completed   int         NOT NULL,
    leechers    int         NOT NULL,
    scraped_at  timestamptz NOT NULL,
    PRIMARY KEY (infohash, tracker)
);
CREATE INDEX IF NOT EXISTS swarm_latest_seeders ON swarm_latest (seeders DESC);

-- Micro-batches already applied, so a restarted stream never double-counts.
CREATE TABLE IF NOT EXISTS stream_batches (
    query       text        NOT NULL,
    batch_id    bigint      NOT NULL,
    rows        bigint      NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query, batch_id)
);

