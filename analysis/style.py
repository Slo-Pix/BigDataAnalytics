"""Shared figure style for report charts (static PNG, light surface).

Colors are the validated reference palette (dataviz skill, references/palette.md). Each category keeps
the same color in every figure: movie = slot 1, tv = slot 2, game = slot 3 (the first three slots pass
the all-pairs colorblind check, so they are safe even in scatter plots).
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from common.paths import ROOT  # noqa: E402

FIG_DIR = ROOT / "report" / "figures"
RESULTS_DIR = ROOT / "analysis" / "results"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
CATEGORY_COLOR = {"movie": SERIES[0], "tv": SERIES[1], "game": SERIES[2], "music": SERIES[3], "adult": SERIES[4],
                  "other": "#9a9893"}
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]  # blue, light -> dark


def setup():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "figure.dpi": 110, "savefig.dpi": 200, "figure.figsize": (8, 4.5),
        "font.size": 10, "font.family": "DejaVu Sans",
        "text.color": INK, "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2,
        "axes.edgecolor": GRID, "axes.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
        "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 12,
        "lines.linewidth": 2, "lines.markersize": 5,
        "legend.frameon": False, "legend.fontsize": 9,
    })


def save(fig, name: str, note: str | None = None):
    """Save to report/figures/<name>.png; `note` becomes a small source/caveat line under the plot."""
    if note:
        fig.text(0.01, 0.01, note, fontsize=8, color=INK_2, ha="left", va="bottom")
    fig.tight_layout(rect=(0, 0.04 if note else 0, 1, 1))
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"figure: {path.relative_to(ROOT)}")


def save_table(df, name: str):
    path = RESULTS_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"table:  {path.relative_to(ROOT)} ({len(df)} rows)")
