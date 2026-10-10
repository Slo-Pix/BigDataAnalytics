"""A1 — Cross-media torrent activity index (RARBG uploads 2007 – May 2023).

  * uploads per category per year, and an activity index (each category's year / its 2010-2022 mean x 100)
  * movie release quality over time: resolution mix and x265/HEVC adoption

Usage: uv run python -m analysis.a1_activity
"""
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter

from analysis.style import CATEGORY_COLOR, INK_2, SEQ, save, save_table, setup
from common.spark import STAGING, get_spark

FULL_YEARS = "added_year BETWEEN 2008 AND 2022"  # 2007 starts in June, 2023 ends in May


def main():
    setup()
    spark = get_spark("a1-activity", mem="3g")
    spark.read.parquet(str(STAGING / "torrents")).createOrReplaceTempView("torrents")

    by_year = spark.sql("""
        SELECT category, added_year AS year, count(*) AS uploads, sum(size_bytes) / 1e12 AS terabytes
        FROM torrents WHERE added_year IS NOT NULL
        GROUP BY category, added_year ORDER BY category, year
    """).toPandas()
    base = by_year[by_year.year.between(2010, 2022)].groupby("category").uploads.mean().rename("base")
    by_year = by_year.join(base, on="category")
    by_year["activity_index"] = (by_year.uploads / by_year.base * 100).round(1)
    save_table(by_year.drop(columns="base"), "a1_uploads_by_category_year")

    fig, ax = plt.subplots()
    for cat in ["adult", "tv", "movie", "music", "game"]:
        d = by_year[(by_year.category == cat) & by_year.year.between(2008, 2022)]
        ax.plot(d.year, d.uploads, color=CATEGORY_COLOR[cat], label=cat, marker="o", markersize=4)
        ax.annotate(cat, (d.year.iloc[-1], d.uploads.iloc[-1]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK_2)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_title("RARBG uploads per year by category")
    ax.set_ylabel("torrents uploaded (log scale)")
    ax.set_xlim(2007.5, 2023.6)
    ax.legend(loc="lower left", ncols=5)
    save(fig, "a1_uploads_by_year", "Source: RARBG catalog dump, 2,843,691 torrents. 2007 and 2023 are partial years and omitted.")

    quality = spark.sql(f"""
        SELECT added_year AS year,
               CASE WHEN resolution = '2160p' THEN '2160p (4K)' WHEN resolution = '1080p' THEN '1080p'
                    WHEN resolution = '720p' THEN '720p' ELSE 'SD / unknown' END AS res,
               count(*) AS n,
               avg(CASE WHEN lower(video_codec) IN ('h.265', 'hevc') OR lower(title) RLIKE 'x265|hevc|h\\\\.?265' THEN 1 ELSE 0 END) AS x265
        FROM torrents WHERE category = 'movie' AND {FULL_YEARS}
        GROUP BY 1, 2
    """).toPandas()
    share = quality.pivot_table(index="year", columns="res", values="n", fill_value=0)
    share = share.div(share.sum(axis=1), axis=0)
    order = ["SD / unknown", "720p", "1080p", "2160p (4K)"]
    share = share[order]
    save_table(share.reset_index(), "a1_movie_resolution_share")

    fig, ax = plt.subplots()
    ax.stackplot(share.index, *[share[c] for c in order], labels=order, colors=[SEQ[1], SEQ[3], SEQ[5], SEQ[6]],
                 edgecolor="#fcfcfb", linewidth=1.5)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_ylim(0, 1)
    ax.set_xlim(share.index.min(), share.index.max())
    ax.set_title("Movie torrents by resolution")
    ax.set_ylabel("share of movie uploads")
    ax.legend(loc="upper left", ncols=4, bbox_to_anchor=(0, -0.08))
    ax.grid(axis="x", visible=False)
    save(fig, "a1_movie_resolution", "Resolution parsed from release names with guessit.")

    x265 = quality.groupby("year").apply(lambda g: (g.x265 * g.n).sum() / g.n.sum(), include_groups=False).rename("x265_share").reset_index()
    save_table(x265, "a1_movie_x265_share")
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.plot(x265.year, x265.x265_share, color=CATEGORY_COLOR["movie"], marker="o", markersize=4)
    last = x265.iloc[-1]
    ax.annotate(f"{last.x265_share:.0%}", (last.year, last.x265_share), xytext=(6, 0), textcoords="offset points",
                va="center", color=INK_2)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_ylim(0, None)
    ax.set_title("Share of movie torrents encoded in x265 / HEVC")
    save(fig, "a1_movie_x265")
    spark.stop()


if __name__ == "__main__":
    main()
