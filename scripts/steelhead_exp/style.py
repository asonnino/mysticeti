# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Figure style: the historical Mysticeti plotter's look (dotted lines, marker
cycle, legend above the axes) sized for the LNCS 12.2cm text width."""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402  (backend must be set first)

from simconfig import PLOTS_DIR  # noqa: E402

FIG_WIDTH_IN = 4.8
FIG_HEIGHT_IN = 2.2
TIMELINE_HEIGHT_IN = 2.4

MARKERS = ["o", "v", "s", "p", "D", "P", "X", "^"]

# One look per protocol slug across every figure. Adaptive Steelhead is black.
PROTO_STYLE = {
    "myst": ("Mysticeti", "C0", "o"),
    "bbps": ("BlueBottle-PS", "C0", "o"),
    "mahi4": ("Mahi-Mahi-4", "C1", "v"),
    "mahi5": ("Mahi-Mahi-5", "C2", "s"),
    "bbasync": ("BlueBottle-Async", "C2", "s"),
    "sh-p2": ("Steelhead p=2", "C3", "p"),
    "sh-p4": ("Steelhead p=4", "C4", "D"),
    "sh-p8": ("Steelhead p=8", "C5", "P"),
    "sh-p16": ("Steelhead p=16", "C6", "X"),
    "sh-p1": ("Steelhead p=1", "C7", "^"),
    "sh-ada": ("Steelhead adaptive", "black", "*"),
    "sh-sync": ("sync-optimized", "black", "*"),
    "sh-bal": ("balanced", "black", "*"),
    "sh-tumult": ("tumult-optimized", "black", "*"),
    "sh-p1-canary": ("Steelhead p=1 (canary)", "C4", "D"),
    "sh-p1-nocanary": ("Steelhead p=1 (no canary)", "C7", "^"),
}


def apply_style():
    plt.rcParams.update({
        "figure.figsize": (FIG_WIDTH_IN, FIG_HEIGHT_IN),
        "font.size": 8,
        "axes.labelsize": 8,
        "axes.labelweight": "bold",
        "axes.grid": True,
        "grid.alpha": 0.4,
        "xtick.labelsize": 7,
        "ytick.labelsize": 7,
        "legend.fontsize": 7,
        "savefig.bbox": "tight",
        "pdf.fonttype": 42,
    })


def throughput_formatter(value, _position):
    return f"{value / 1000:.0f}k" if value >= 10_000 else f"{value:,.0f}"


def seconds_formatter(value, _position):
    return f"{value:,.0f}" if value >= 10 else f"{value:,.1f}"


def legend_above(axes, ncol=3, handles=None, labels=None):
    arguments = dict(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=ncol,
                        frameon=False, borderaxespad=0.2)
    if handles is not None:
        axes.legend(handles, labels, **arguments)
    else:
        axes.legend(**arguments)


# Phase shading colors (the Barnacle style): green healthy, red degraded.
PHASE_COLORS = {"healthy": "#d9f0d3", "attack": "#fde0dd"}


def shade_phases(axes, phases):
    for phase in phases:
        color = PHASE_COLORS.get(phase.label)
        if color:
            axes.axvspan(phase.start_s, phase.end_s, color=color, alpha=0.5, lw=0, zorder=0)


def trim_spines(axes):
    for side in ("top", "right"):
        axes.spines[side].set_visible(False)


def save(figure, name):
    """plots/<name>.pdf (deterministic metadata) + a .png preview."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    pdf = PLOTS_DIR / f"{name}.pdf"
    figure.savefig(pdf, metadata={"CreationDate": None})
    figure.savefig(PLOTS_DIR / f"{name}.png", dpi=200)
    plt.close(figure)
    print(f"wrote {pdf}")
