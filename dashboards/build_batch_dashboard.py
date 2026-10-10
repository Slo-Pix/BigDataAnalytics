"""Generates infra/grafana/dashboards/batch_analytics.json — the "Batch Analytics" Grafana dashboard.

Reads the `analytics` schema in Postgres (filled by dashboards.load_results). Category colors match the
report figures (movie blue, tv orange, game aqua); red = more piracy than expected, blue = less.

Usage: uv run python -m dashboards.build_batch_dashboard   (Grafana picks the file up within ~10 s)
"""
import json

from analysis.style import CATEGORY_COLOR, SERIES
from common.paths import ROOT

DS = {"type": "grafana-postgresql-datasource", "uid": "torrentdb"}
OUT = ROOT / "infra" / "grafana" / "dashboards" / "batch_analytics.json"
BLUE, RED = "#2a78d6", "#e34948"
_ids = iter(range(1, 1000))


def q(sql: str, fmt: str = "table") -> list:
    return [{"refId": "A", "datasource": DS, "rawQuery": True, "editorMode": "code", "format": fmt, "rawSql": sql.strip()}]


def color_overrides(names_colors: dict) -> list:
    return [{"matcher": {"id": "byName", "options": n},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": col}}]} for n, col in names_colors.items()]


def row(title: str, y: int) -> dict:
    return {"id": next(_ids), "type": "row", "title": title, "collapsed": False, "panels": [],
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}


def stat(title, sql, x, y, w=4, unit="short", decimals=None):
    d = {"unit": unit, "color": {"mode": "fixed", "fixedColor": SERIES[0]}}
    if decimals is not None:
        d["decimals"] = decimals
    return {"id": next(_ids), "type": "stat", "title": title, "datasource": DS, "targets": q(sql),
            "gridPos": {"x": x, "y": y, "w": w, "h": 4},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "values": False}, "colorMode": "none",
                        "graphMode": "none", "textMode": "value"},
            "fieldConfig": {"defaults": d, "overrides": []}}


def bars(title, sql, x, y, w, h, x_field, unit="short", stacking="none", horizontal=False, log=False,
         colors=None, diverging=False, description=""):
    custom = {"fillOpacity": 90, "lineWidth": 0, "gradientMode": "none", "axisSoftMin": 0 if not diverging else None}
    if log:
        custom["scaleDistribution"] = {"type": "log", "log": 10}
    defaults = {"unit": unit, "custom": custom}
    if diverging:
        defaults["color"] = {"mode": "thresholds"}
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"color": BLUE, "value": None}, {"color": RED, "value": 0}]}
    return {"id": next(_ids), "type": "barchart", "title": title, "description": description, "datasource": DS,
            "targets": q(sql), "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"xField": x_field, "orientation": "horizontal" if horizontal else "vertical",
                        "stacking": stacking, "showValue": "never", "barWidth": 0.85, "groupWidth": 0.75,
                        "xTickLabelRotation": 0, "xTickLabelSpacing": 0,
                        "legend": {"displayMode": "list", "placement": "bottom", "showLegend": not diverging},
                        "tooltip": {"mode": "multi", "sort": "none"}},
            "fieldConfig": {"defaults": defaults, "overrides": color_overrides(colors or {}) + [
                {"matcher": {"id": "byName", "options": x_field}, "properties": [{"id": "unit", "value": "string"}]}]}}


def table(title, sql, x, y, w, h, units=None, description=""):
    overrides = [{"matcher": {"id": "byName", "options": n}, "properties": [{"id": "unit", "value": u}]}
                 for n, u in (units or {}).items()]
    return {"id": next(_ids), "type": "table", "title": title, "description": description, "datasource": DS,
            "targets": q(sql), "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"showHeader": True, "cellHeight": "sm"},
            "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides}}


def lines(title, sql, x, y, w, h, unit="short", colors=None, description=""):
    return {"id": next(_ids), "type": "timeseries", "title": title, "description": description, "datasource": DS,
            "targets": q(sql, "time_series"), "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}},
                            "overrides": color_overrides(colors or {})}}


CAT = {k: CATEGORY_COLOR[k] for k in ("movie", "tv", "game", "music", "adult")}
pivot = lambda val, cats=("movie", "tv", "game"), col="category": ", ".join(  # noqa: E731
    f"max({val}) FILTER (WHERE {col} = '{c}') AS \"{c}\"" for c in cats)


def build() -> dict:
    p, y = [], 0
    p.append(row("Dataset", y)); y += 1
    inv = "SELECT rows FROM analytics.dataset_inventory WHERE table_name = '{}'"
    p += [
        stat("RARBG torrents", inv.format("torrents"), 0, y),
        stat("Wikipedia pageview rows", inv.format("wiki_pageviews"), 4, y),
        stat("IMDb rows (4 tables)", "SELECT sum(rows) FROM analytics.dataset_inventory WHERE table_name LIKE 'imdb_%'", 8, y),
        stat("Live swarm rows (10 Oct)", inv.format("swarm"), 12, y),
        stat("Torrents alive (full scan)", "SELECT round(sum(pct_alive * torrents)) FROM analytics.a3_alive_by_upload_year", 16, y),
        stat("Films in leak-lag analysis", "SELECT count(*) FROM analytics.a2_movie_first_upload", 20, y),
    ]; y += 4

    p.append(row("A1 · Activity index — RARBG uploads 2008–2022", y)); y += 1
    p += [
        bars("Uploads per year by category (log scale)",
             f"SELECT year::text AS year, {pivot('uploads', ('adult', 'tv', 'movie', 'music', 'game'))} "
             "FROM analytics.a1_uploads_by_category_year WHERE year BETWEEN 2008 AND 2022 GROUP BY year ORDER BY year",
             0, y, 12, 9, "year", log=True, colors=CAT),
        bars("Movie uploads by resolution",
             "SELECT year::text AS year, " + ", ".join(
                 f"max(share) FILTER (WHERE resolution = '{r}') AS \"{r}\"" for r in ("SD / unknown", "720p", "1080p", "2160p (4K)")) +
             " FROM analytics.a1_movie_resolution_share_long GROUP BY year ORDER BY year",
             12, y, 12, 9, "year", unit="percentunit", stacking="normal",
             colors={"SD / unknown": "#9ec5f4", "720p": "#3987e5", "1080p": "#184f95", "2160p (4K)": "#0d366b"}),
    ]; y += 9

    p.append(row("A2 · Release-to-leak lag — films released 2018–Mar 2023", y)); y += 1
    p += [
        bars("Median days after cinema release",
             "SELECT release_year::text AS release_year, median_days_to_leak AS \"first torrent on RARBG\", "
             "median_window_days AS \"legal home release\" FROM analytics.a2_movie_leak_by_year ORDER BY 1",
             0, y, 8, 9, "release_year", unit="none", colors={"first torrent on RARBG": SERIES[0], "legal home release": SERIES[3]}),
        bars("Source of each film's first torrent",
             "SELECT release_year::text AS release_year, " + ", ".join(
                 f"max(share) FILTER (WHERE source = '{s}') AS \"{s}\"" for s in ("CAM / TS", "WEB", "TV", "Disc", "Other / unknown")) +
             " FROM analytics.a2_first_upload_source_mix_long GROUP BY release_year ORDER BY 1",
             8, y, 8, 9, "release_year", unit="percentunit", stacking="normal",
             colors={"CAM / TS": SERIES[7], "WEB": SERIES[0], "TV": SERIES[3], "Disc": SERIES[6], "Other / unknown": "#c3c2b7"}),
        table("Leak lag by year",
              "SELECT release_year::text AS year, films, median_days_to_leak AS \"median days\", "
              "pct_within_7d AS \"≤7 days\", pct_before_home AS \"before home rel.\" "
              "FROM analytics.a2_movie_leak_by_year ORDER BY 1",
              16, y, 8, 5, units={"≤7 days": "percentunit", "before home rel.": "percentunit"}),
        table("By original language",
              "SELECT lang_group AS language, films, median_days_to_leak AS \"median days\", median_versions AS \"median versions\" "
              "FROM analytics.a2_leak_by_language ORDER BY films DESC", 16, y + 5, 8, 4),
    ]; y += 9

    p.append(row("A3 · Popularity decay — full catalog scan, 1.39M torrents", y)); y += 1
    p += [
        bars("Torrents still alive (≥1 seeder), by upload year",
             f"SELECT added_year::text AS year, {pivot('pct_alive')} FROM analytics.a3_alive_by_upload_year "
             "WHERE added_year BETWEEN 2008 AND 2023 GROUP BY added_year ORDER BY added_year",
             0, y, 9, 9, "year", unit="percentunit", colors=CAT),
        bars("Seeders per title (all versions), by release year",
             f"SELECT title_year::text AS year, {pivot('mean_seeders_per_title')} FROM analytics.a3_title_decay_by_release_year "
             "WHERE title_year BETWEEN 2008 AND 2023 GROUP BY title_year ORDER BY title_year",
             9, y, 8, 9, "year", log=True, colors=CAT),
        table("MLlib Poisson GLM fits (titles 2012–2023)",
              "SELECT category, replace(model, '_', ' ') AS model, round(coef::numeric, 3) AS coef, "
              "CASE WHEN best THEN '✓' ELSE '' END AS \"best (AIC)\" "
              "FROM analytics.a3_title_decay_fits ORDER BY category, aic", 17, y, 7, 9),
    ]; y += 9

    p.append(row("A4 · Public interest vs piracy — English Wikipedia (Sep 2026) vs seeders (Oct 2026)", y)); y += 1
    p += [
        bars("Spearman correlation with Wikipedia views",
             "SELECT category, rho_views_seeders AS \"current seeders\", rho_views_torrents AS \"torrent versions\" "
             "FROM analytics.a4_spearman ORDER BY titles DESC", 0, y, 8, 9, "category",
             colors={"current seeders": SERIES[0], "torrent versions": SERIES[3]}),
        table("Most-read titles and their swarms",
              "SELECT category, name, views_week AS \"Wikipedia views (week)\", torrents AS versions, seeders "
              "FROM analytics.a4_top_titles ORDER BY views_week DESC LIMIT 100", 8, y, 16, 9,
              units={"Wikipedia views (week)": "locale", "seeders": "locale"}),
    ]; y += 9

    p.append(row("A5 · Games — Steam 2018–2022, ≥100 reviews (MLlib logistic regression, test AUC 0.83)", y)); y += 1
    labels = {"log_price": "price (log)", "free": "free-to-play", "log_reviews": "popularity (log reviews)", "score": "review score",
              "years_since_release": "years since release", "single_player": "single-player", "online_pvp": "online PvP"}
    case = "CASE " + " ".join(f"WHEN feature = '{k}' THEN '{v}'" for k, v in labels.items()) + \
           " ELSE 'genre: ' || initcap(replace(substr(feature, 3), '_', ' ')) END"
    p += [
        bars("Change in odds of being pirated, per 1 SD",
             f"SELECT {case} AS feature, odds_ratio_per_sd - 1 AS effect FROM analytics.a5_logit_pirated ORDER BY coef_std DESC",
             0, y, 12, 11, "feature", unit="percentunit", horizontal=True, diverging=True,
             description="Red = more likely to be on RARBG, blue = less. Standardized MLlib logistic regression coefficients."),
        bars("Share pirated by price", "SELECT band, mean AS pirated FROM analytics.a5_pirated_by_band WHERE \"by\" = 'price'",
             12, y, 6, 11, "band", unit="percentunit", colors={"pirated": CATEGORY_COLOR["game"]}),
        bars("Share pirated by popularity (reviews)",
             "SELECT band, mean AS pirated FROM analytics.a5_pirated_by_band WHERE \"by\" = 'popularity'",
             18, y, 6, 11, "band", unit="percentunit", colors={"pirated": CATEGORY_COLOR["game"]}),
    ]; y += 11

    p.append(row("A6 · Genres — torrent versions vs an equally popular film (MLlib OLS residuals)", y)); y += 1
    p += [
        bars("Excess piracy by genre",
             "SELECT genre || ' (' || films || ')' AS genre, excess_piracy FROM analytics.a6_genre_excess_piracy ORDER BY excess_piracy DESC",
             0, y, 14, 12, "genre", unit="percentunit", horizontal=True, diverging=True,
             description="IMDb movies 2018–2022 with ≥1,000 votes. Model: log1p(torrents) ~ log(votes) + year."),
        table("Genre detail",
              "SELECT genre, films, excess_piracy AS \"excess piracy\", pct_pirated AS \"on RARBG\", median_votes AS \"median IMDb votes\" "
              "FROM analytics.a6_genre_excess_piracy ORDER BY excess_piracy DESC",
              14, y, 10, 12, units={"excess piracy": "percentunit", "on RARBG": "percentunit", "median IMDb votes": "locale"}),
    ]; y += 12

    p.append(row("A7 · Swarms through the day — opentrackr, fixed panel, 10 Oct 2026", y)); y += 1
    p += [
        lines("Seeders vs each torrent's daily average (= 100)",
              "SELECT slot AS time, category AS metric, index AS value FROM analytics.a7_panel_by_slot ORDER BY 1",
              0, y, 12, 9, colors=CAT),
        lines("Leechers per seeder",
              "SELECT slot AS time, category AS metric, leech_per_seed AS value FROM analytics.a7_panel_by_slot ORDER BY 1",
              12, y, 12, 9, colors=CAT),
    ]; y += 9

    p.append(row("Data lake inventory", y)); y += 1
    p.append(table("Parquet tables", "SELECT layer, table_name AS \"table\", rows, size_mb AS \"size (MB)\" "
                   "FROM analytics.dataset_inventory ORDER BY rows DESC", 0, y, 24, 10, units={"rows": "locale"}))

    return {"uid": "batch-analytics", "title": "Batch Analytics — Global Torrent Activity", "schemaVersion": 39,
            "tags": ["torrent-bda", "batch"], "editable": True, "graphTooltip": 1,
            "time": {"from": "2026-10-10T07:00:00.000Z", "to": "2026-10-10T15:20:00.000Z"},
            "timepicker": {"hidden": False}, "panels": p}


def main():
    OUT.write_text(json.dumps(build(), indent=2))
    print("wrote", OUT.relative_to(ROOT))


if __name__ == "__main__":
    main()
