"""Rebuild report/research-console figures from saved CSVs without Spark or live services."""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter

from analysis.style import CATEGORY_COLOR, RESULTS_DIR, SERIES, save, setup

CATEGORIES = ("movie", "tv", "game")


def read(name: str) -> pd.DataFrame:
    frame = pd.read_csv(RESULTS_DIR / f"{name}.csv")
    if frame.empty:
        raise ValueError(f"Saved research result is empty: {name}.csv")
    return frame


def activity():
    uploads = read("a1_uploads_by_category_year")
    fig, ax = plt.subplots()
    for category in CATEGORIES:
        data = uploads[uploads.category.eq(category)].sort_values("year")
        ax.plot(data.year, data.uploads / 1000, label=category, color=CATEGORY_COLOR[category])
    ax.set(title="Recorded RARBG uploads by year", xlabel="upload year", ylabel="uploads (thousands)")
    ax.legend()
    save(fig, "a1_uploads_by_year", "Saved research catalogue; 2023 ends in May and is not a full year.")

    resolution = read("a1_movie_resolution_share")
    fig, ax = plt.subplots()
    for name, color in zip(("SD / unknown", "720p", "1080p", "2160p (4K)"), SERIES):
        ax.plot(resolution.year, resolution[name], label=name, color=color)
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set(title="Movie release resolution", xlabel="upload year", ylabel="share of movie uploads")
    ax.legend()
    save(fig, "a1_movie_resolution", "Release-name parsing; unknown resolution is grouped with SD.")

    codec = read("a1_movie_x265_share")
    fig, ax = plt.subplots()
    ax.plot(codec.year, codec.x265_share, color=CATEGORY_COLOR["movie"])
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set(title="Movie releases using x265 / HEVC", xlabel="upload year", ylabel="share of movie uploads")
    save(fig, "a1_movie_x265", "Saved release-name parsing results; catalogue coverage ends May 2023.")


def release_lag():
    films = read("a2_movie_first_upload").dropna(subset=["days_vs_home"])
    fig, ax = plt.subplots()
    for (start, end), color in zip(((2018, 2019), (2020, 2021), (2022, 2023)), SERIES):
        values = films.loc[films.release_year.between(start, end), "days_vs_home"].sort_values()
        if values.empty:
            raise ValueError(f"No saved release-lag observations for {start}-{end}.")
        cumulative = pd.Series(range(1, len(values) + 1), dtype=float) / len(values)
        ax.step(values.clip(-120, 240), cumulative, where="post", color=color,
                label=f"{start}-{end} (n={len(values):,})")
    ax.axvline(0, color="#52514e", linestyle="--", linewidth=1)
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set(title="First recorded torrent relative to legal home release",
           xlabel="days relative to home release (clipped to -120 / +240)", ylabel="cumulative share of films")
    ax.legend(loc="lower right")
    save(fig, "a2_leak_vs_home_ecdf", "First surviving RARBG listing, not independently verified first online availability.")


def decay():
    uploads = read("a3_alive_by_upload_year")
    titles = read("a3_title_decay_by_release_year")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for category in CATEGORIES:
        data = uploads[uploads.category.eq(category) & uploads.added_year.between(2008, 2023)]
        axes[0].plot(data.added_year, data.pct_alive, label=category, color=CATEGORY_COLOR[category])
        data = titles[titles.category.eq(category) & titles.title_year.between(2008, 2023)]
        axes[1].plot(data.title_year, data.mean_seeders_per_title, label=category, color=CATEGORY_COLOR[category])
    axes[0].yaxis.set_major_formatter(PercentFormatter(1))
    axes[0].set(title="Torrents still alive", xlabel="upload year", ylabel="share with seeders")
    axes[1].set(title="Mean seeders per title, all versions", xlabel="title release year", yscale="log")
    for ax in axes:
        ax.legend()
    save(fig, "a3_decay", "Cross-sectional full scan, 10 Oct 2026; survivorship and catalogue selection remain.")


def interest():
    data = read("a4_spearman")
    fig, ax = plt.subplots()
    ax.bar(data.category, data.rho_views_seeders, color=[CATEGORY_COLOR[name] for name in data.category])
    ax.set(title="Wikipedia interest versus observed seeders", ylabel="Spearman correlation", ylim=(0, 1))
    for row in data.itertuples():
        ax.annotate(f"{row.rho_views_seeders:.2f}\nn={row.titles:,}",
                    (row.category, row.rho_views_seeders), xytext=(0, 5), textcoords="offset points", ha="center")
    save(fig, "a4_wiki_vs_seeders", "Weekly English Wikipedia interest and historical seeders; association, not causation.")


def games():
    data = read("a5_pirated_by_band")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, group in zip(axes, ("price", "popularity")):
        values = data[data.by.eq(group)]
        ax.bar(values.band, values["mean"], color=CATEGORY_COLOR["game"])
        for index, row in enumerate(values.itertuples()):
            ax.annotate(f"{row.mean:.0%}", (index, row.mean), xytext=(0, 4),
                        textcoords="offset points", ha="center")
        ax.yaxis.set_major_formatter(PercentFormatter(1))
        ax.set(title=f"Matched RARBG listing by {group}", ylim=(0, 0.8))
    save(fig, "a5_pirated_by_band", "Steam releases 2018-2022 with >=100 reviews; matched listing is the study outcome.")


def genres():
    data = read("a6_genre_excess_piracy").sort_values("excess_piracy")
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(data.genre, data.excess_piracy, color=[SERIES[1] if value > 0 else SERIES[0]
                                                for value in data.excess_piracy])
    ax.axvline(0, color="#52514e", linewidth=1)
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.set(title="Genre association after popularity/year adjustment",
           xlabel="exp(mean log-count residual) - 1")
    save(fig, "a6_genre_excess_piracy", "Saved OLS residual summaries; overlapping genres, no causal interpretation.")


def swarms():
    data = read("a7_panel_by_slot")
    data["slot_ist"] = pd.to_datetime(data.slot_ist)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharex=True)
    for category in CATEGORIES:
        values = data[data.category.eq(category)].sort_values("slot_ist")
        axes[0].plot(values.slot_ist, values["index"], color=CATEGORY_COLOR[category], label=category)
        axes[1].plot(values.slot_ist, values.leech_per_seed, color=CATEGORY_COLOR[category], label=category)
    axes[0].axhline(100, color="#52514e", linestyle="--", linewidth=1)
    axes[0].set(title="Seeders relative to each torrent's mean", ylabel="daily mean = 100")
    axes[1].set(title="Leechers per seeder", ylim=(0, None))
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_xlabel("20-minute slot label (IST), 10 Oct 2026")
        ax.legend()
    save(fig, "a7_swarm_day", "Fixed panel, opentrackr only; stored window labels can precede the observation cutoff.")


def main():
    setup()
    activity()
    release_lag()
    decay()
    interest()
    games()
    genres()
    swarms()


if __name__ == "__main__":
    main()
