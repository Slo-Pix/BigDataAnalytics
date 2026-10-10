"""Generates dashboards/live_swarm.json (provisioned into Grafana). Re-run after editing."""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "torrentdb"}


def target(sql, fmt="time_series"):
    return [{"refId": "A", "datasource": DS, "rawQuery": True, "editorMode": "code", "format": fmt, "rawSql": sql}]


CAT_COLORS = {"movie": "#2a78d6", "tv": "#eb6834", "game": "#1baf7a", "unmatched": "#9a9893"}  # same as report figures


def panel(pid, title, kind, sql, x, y, w, h, fmt="time_series", **extra):
    p = {"id": pid, "title": title, "type": kind, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": target(sql, fmt)}
    if kind == "stat":
        p["fieldConfig"] = {"defaults": {"unit": "locale", "color": {"mode": "fixed", "fixedColor": "#2a78d6"}}, "overrides": []}
        p["options"] = {"colorMode": "none", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"], "values": False}}
    if kind == "piechart":
        p["fieldConfig"] = {"defaults": {"unit": "locale"}, "overrides": [
            {"matcher": {"id": "byName", "options": k}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": v}}]}
            for k, v in CAT_COLORS.items()]}
    p.update(extra)
    return p


panels = [
    panel(1, "Torrents tracked", "stat", "SELECT count(DISTINCT infohash) FROM swarm_latest", 0, 0, 6, 4, "table"),
    panel(2, "Seeders right now (max over trackers)", "stat",
          "SELECT sum(s) FROM (SELECT max(seeders) s FROM swarm_latest GROUP BY infohash) t", 6, 0, 6, 4, "table"),
    panel(3, "Rows ingested (selected range)", "stat",
          "SELECT sum(rows) FROM swarm_minute WHERE $__timeFilter(minute)", 12, 0, 6, 4, "table"),
    panel(4, "Micro-batches applied", "stat", "SELECT count(*) FROM stream_batches", 18, 0, 6, 4, "table"),
    panel(5, "Stream throughput: scrape rows per minute", "timeseries",
          """SELECT minute AS time, sum(rows) AS rows FROM swarm_minute
             WHERE $__timeFilter(minute) GROUP BY 1 ORDER BY 1""", 0, 4, 12, 8,
          fieldConfig={"defaults": {"custom": {"drawStyle": "bars", "fillOpacity": 60}}, "overrides": []}),
    panel(6, "Average seeders per torrent, by tracker (hourly)", "timeseries",
          """SELECT date_trunc('hour', minute) AS time, tracker,
                    sum(total_seeders)::float / nullif(sum(rows), 0) AS avg_seeders
             FROM swarm_minute WHERE $__timeFilter(minute) GROUP BY 1, 2 ORDER BY 1""", 12, 4, 12, 8),
    panel(7, "Largest swarms right now", "table",
          """SELECT coalesce(t.title, l.infohash) AS torrent, t.category,
                    max(l.seeders) AS seeders, max(l.leechers) AS leechers, max(l.completed) AS completed,
                    max(l.scraped_at) AS last_seen
             FROM swarm_latest l LEFT JOIN torrents t USING (infohash)
             GROUP BY 1, 2 ORDER BY seeders DESC LIMIT 25""", 0, 12, 16, 12, "table"),
    panel(8, "Seeders by category right now", "piechart",
          """SELECT sum(s) FILTER (WHERE category = 'movie') AS movie, sum(s) FILTER (WHERE category = 'tv') AS tv,
                    sum(s) FILTER (WHERE category = 'game') AS game, sum(s) FILTER (WHERE category IS NULL) AS unmatched
             FROM (SELECT infohash, max(seeders) s FROM swarm_latest GROUP BY 1) l
             LEFT JOIN torrents t USING (infohash)""", 16, 12, 8, 12, "table",
          options={"reduceOptions": {"values": False, "calcs": ["lastNotNull"]}, "pieType": "donut",
                   "legend": {"displayMode": "table", "placement": "right", "values": ["value", "percent"]}}),
]

dash = {"uid": "live-swarm", "title": "Live Torrent Swarms (speed layer)", "schemaVersion": 39,
        "time": {"from": "now-24h", "to": "now"}, "refresh": "1m", "tags": ["torrent-bda", "streaming"],
        "panels": panels}
out = Path(__file__).parent / "dashboards" / "live_swarm.json"
out.write_text(json.dumps(dash, indent=2))
print("wrote", out)
