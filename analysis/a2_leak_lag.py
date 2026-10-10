"""A2 — Release-to-leak lag: how long after release does a title first appear on RARBG, and from what source?

Movies: first RARBG upload per film vs TMDB first theatrical date (and legal home-release date).
Only films released 2018-01-01 .. 2023-03-31 count. The dump only holds torrents still listed in May 2023
and RARBG replaced old releases over time, so before ~2017 the "first upload" in the dump is often years
after the real first release (median 3,363 days for 2009 films) and leak lag cannot be measured. TV: first upload of any episode vs the series premiere (first_air_date >= 2013).

Usage: uv run python -m analysis.a2_leak_lag
"""
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter

from analysis.style import CATEGORY_COLOR, INK_2, SEQ, SERIES, save, save_table, setup
from common.spark import CURATED, get_spark

INDIAN = ("hi", "ta", "te", "ml", "kn", "bn", "mr", "pa")
SOURCE_GROUP = """
  CASE WHEN source IN ('Camera','Telesync','HD Camera','HD Telesync','Telecine','Workprint') THEN 'CAM / TS'
       WHEN source IN ('Web','Video on Demand') THEN 'WEB'
       WHEN source IN ('Blu-ray','Ultra HD Blu-ray','DVD','HD-DVD') THEN 'Disc'
       WHEN source IN ('HDTV','TV','Satellite','Digital TV') THEN 'TV'
       ELSE 'Other / unknown' END
"""


def main():
    setup()
    spark = get_spark("a2-leak-lag", mem="3g")
    spark.read.parquet(str(CURATED / "torrents_enriched")).createOrReplaceTempView("t")

    films = spark.sql(f"""
        WITH ranked AS (
          SELECT imdb_id, display_title, original_language, theatrical_date, home_release_date, added_at,
                 {SOURCE_GROUP} AS source_group,
                 row_number() OVER (PARTITION BY imdb_id ORDER BY added_at) AS rn,
                 count(*) OVER (PARTITION BY imdb_id) AS versions
          FROM t
          WHERE category = 'movie' AND theatrical_date BETWEEN '2018-01-01' AND '2023-03-31'
        )
        SELECT imdb_id, display_title, original_language, theatrical_date, home_release_date, versions,
               added_at AS first_upload, source_group AS first_source,
               year(theatrical_date) AS release_year,
               datediff(to_date(added_at), theatrical_date) AS days_to_leak,
               datediff(to_date(added_at), home_release_date) AS days_vs_home,
               datediff(home_release_date, theatrical_date) AS window_days,
               CASE WHEN original_language IN {INDIAN} THEN 'Indian languages'
                    WHEN original_language = 'en' THEN 'English' ELSE 'Other languages' END AS lang_group
        FROM ranked WHERE rn = 1
    """).toPandas()
    save_table(films, "a2_movie_first_upload")

    yearly = films.groupby("release_year").agg(
        films=("imdb_id", "size"),
        median_days_to_leak=("days_to_leak", "median"),
        median_window_days=("window_days", "median"),
        pct_before_home=("days_vs_home", lambda s: (s.dropna() < 0).mean()),
        pct_within_7d=("days_to_leak", lambda s: (s <= 7).mean()),
    ).reset_index()
    save_table(yearly, "a2_movie_leak_by_year")

    fig, ax = plt.subplots()
    ax.plot(yearly.release_year, yearly.median_days_to_leak, color=CATEGORY_COLOR["movie"], marker="o", markersize=4,
            label="first torrent on RARBG")
    ax.plot(yearly.release_year, yearly.median_window_days, color=SERIES[3], marker="o", markersize=4,
            label="legal home release (TMDB)")
    for col, lab in [("median_days_to_leak", "first torrent"), ("median_window_days", "home release")]:
        r = yearly.dropna(subset=[col]).iloc[-1]
        ax.annotate(f"{lab}: {r[col]:.0f} d", (r.release_year, r[col]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK_2)
    ax.set_title("Median days after cinema release, by release year")
    ax.set_ylabel("days after first theatrical release")
    ax.set_ylim(0, None)
    ax.set_xlim(yearly.release_year.min() - 0.3, yearly.release_year.max() + 1.3)
    ax.set_xticks(yearly.release_year)
    ax.legend(loc="upper right")
    save(fig, "a2_leak_vs_window", f"{len(films):,} films with TMDB release dates and RARBG torrents, released 2018 – Mar 2023 (earlier years: catalog incomplete).")

    # when does the first torrent appear relative to the legal home release?
    d = films.dropna(subset=["days_vs_home"])
    fig, ax = plt.subplots()
    periods = [(2018, 2019), (2020, 2021), (2022, 2023)]
    for (a, b), color in zip(periods, [SERIES[0], SERIES[1], SERIES[2]]):
        x = np.sort(d[d.release_year.between(a, b)].days_vs_home.clip(-120, 240))
        ax.plot(x, np.arange(1, len(x) + 1) / len(x), color=color, label=f"released {a}–{b} (n={len(x):,})")
    ax.axvline(0, color=INK_2, linewidth=1, linestyle="--")
    ax.annotate("legal home release", (0, 0.03), xytext=(5, 0), textcoords="offset points", fontsize=9, color=INK_2)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_title("Share of films already on RARBG, relative to legal home release")
    ax.set_xlabel("days from legal home release to first torrent (clipped to −120 … 240)")
    ax.set_ylabel("cumulative share of films")
    ax.legend(loc="lower right")
    save(fig, "a2_leak_vs_home_ecdf")

    mix = films.pivot_table(index="release_year", columns="first_source", values="imdb_id", aggfunc="count", fill_value=0)
    order = [c for c in ["CAM / TS", "WEB", "TV", "Disc", "Other / unknown"] if c in mix.columns]
    mix = mix[order].div(mix.sum(axis=1), axis=0)
    save_table(mix.reset_index(), "a2_first_upload_source_mix")
    colors = {"CAM / TS": SERIES[7], "WEB": SERIES[0], "TV": SERIES[3], "Disc": SERIES[6], "Other / unknown": "#c3c2b7"}
    fig, ax = plt.subplots()
    bottom = np.zeros(len(mix))
    for col in order:
        ax.bar(mix.index, mix[col], bottom=bottom, color=colors[col], label=col, width=0.8, edgecolor="#fcfcfb", linewidth=1)
        bottom += mix[col].values
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_title("Source of the first torrent of each film")
    ax.set_ylabel("share of films")
    ax.legend(loc="upper left", ncols=5, bbox_to_anchor=(0, -0.08))
    ax.grid(axis="x", visible=False)
    save(fig, "a2_first_source_mix", "Source parsed from release names (guessit). CAM/TS = filmed in a cinema.")

    lang = films.groupby("lang_group").agg(
        films=("imdb_id", "size"), median_days_to_leak=("days_to_leak", "median"),
        pct_cam_first=("first_source", lambda s: (s == "CAM / TS").mean()),
        median_versions=("versions", "median")).reset_index()
    save_table(lang, "a2_leak_by_language")
    print(lang.to_string(index=False))

    tv = spark.sql("""
        SELECT imdb_id, first(display_title) AS title, first(first_air_date) AS premiere,
               min(added_at) AS first_upload, datediff(to_date(min(added_at)), first(first_air_date)) AS days_to_leak
        FROM t WHERE category = 'tv' AND first_air_date >= '2013-01-01' AND first_air_date <= '2023-03-31'
        GROUP BY imdb_id
    """).toPandas()
    save_table(tv, "a2_tv_first_upload")
    print(f"\nTV series: {len(tv):,}; median days premiere -> first torrent: {tv.days_to_leak.median():.0f}; "
          f"within 2 days: {(tv.days_to_leak <= 2).mean():.0%}")
    print(yearly.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
