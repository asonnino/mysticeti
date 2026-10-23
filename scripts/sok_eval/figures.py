# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Aggregate the per-run summaries across seeds, and plot them.

    python3 figures.py aggregate --out results/sok      # summary/*.csv -> summary/medians.csv
    python3 figures.py plot --medians medians.csv --figures results/figures   # needs matplotlib

Aggregation needs only the standard library, so it runs where the runs are; plotting runs
wherever matplotlib is installed, from the small medians file.
"""

import argparse
import csv
import re
import statistics
from pathlib import Path

EXPERIMENTS = ("happy", "crashes", "byzantine", "jitter", "buffer-rounds")
KEYS = ("experiment", "protocol", "jitter", "crashes", "equivocators")
MEDIAN_COLUMNS = (
    "latency_mean_ms",
    "latency_stdev_ms",
    "latency_p50_ms",
    "latency_p90_ms",
    "goodput",
    "committed_leaders_s",
    "commit_window_fraction",
    "direct_commit_rate",
)

# Fault model and synchrony of each protocol label (hybrid labels carry a point suffix).
FAMILIES = [
    ("hybrid-mahi-mahi-w", "3f+2c+1", "async"),
    ("hybrid-mysticeti", "3f+2c+1", "ps"),
    ("orcaella-async", "5f+3c+1", "async"),
    ("orcaella", "5f+3c+1", "ps"),
    ("nemo-nemo-async", "2c+1", "async"),
    ("nemo-nemo", "2c+1", "ps"),
    ("bb-async", "5f+1", "async"),
    ("bb-ps", "5f+1", "ps"),
    ("mahi-mahi", "3f+1", "async"),
    ("mysticeti", "3f+1", "ps"),
    ("cm-async", "3f+1", "async"),
    ("cm-ps", "3f+1", "ps"),
]
FAULT_MODELS = ("3f+1", "5f+1", "5f+3c+1", "3f+2c+1", "2c+1")


def family(protocol):
    for prefix, fault_model, synchrony in FAMILIES:
        if protocol.startswith(prefix):
            return fault_model, synchrony
    raise ValueError(f"unknown protocol {protocol}")


def aggregate(out):
    """Median (and min/max of the mean latency) across seeds, per experiment and point."""
    groups = {}
    for experiment in EXPERIMENTS:
        path = out / "summary" / f"{experiment}.csv"
        if not path.exists():
            continue
        with path.open() as file:
            for row in csv.DictReader(file):
                key = (experiment, *(row[column] for column in KEYS[1:]))
                groups.setdefault(key, []).append(row)
    binaries = {run["binary"] for runs in groups.values() for run in runs if run["status"] == "ok"}
    if len(binaries) > 1:
        raise SystemExit(f"runs from different builds: {sorted(binaries)}")
    rows = []
    for key, runs in sorted(groups.items()):
        done = [run for run in runs if run["status"] == "ok"]
        row = dict(zip(KEYS, key))
        row["runs"] = len(runs)
        row["done"] = len(done)
        for outcome in ("diverged", "no-progress"):
            row[outcome] = sum(run.get("outcome") == outcome for run in done)
        for flag in ("stalled", "unstable"):
            row[flag] = sum(run.get(flag) == "True" for run in done)
        for column in MEDIAN_COLUMNS:
            values = [float(run[column]) for run in done if run.get(column)]
            row[column] = round(statistics.median(values), 3) if values else ""
            if column == "latency_mean_ms":
                row["latency_min_ms"] = round(min(values), 3) if values else ""
                row["latency_max_ms"] = round(max(values), 3) if values else ""
        rows.append(row)
    if not rows:
        print("no summaries to aggregate")
        return
    path = out / "summary" / "medians.csv"
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} points -> {path}")


def load(medians):
    with medians.open() as file:
        rows = list(csv.DictReader(file))
    for row in rows:
        row["fault_model"], row["synchrony"] = family(row["protocol"])
        for column in ("crashes", "equivocators", "done", "runs", "no-progress", "stalled"):
            row[column] = int(row[column])
        for column in (*MEDIAN_COLUMNS, "latency_min_ms", "latency_max_ms"):
            row[column] = float(row[column]) if row.get(column) else None
    return rows


class Statistic:
    """How a latency point is drawn: a centre column and the error bar around it."""

    def __init__(self, name, centre, low, high, suffix):
        self.name, self.centre, self.low, self.high, self.suffix = name, centre, low, high, suffix

    def errors(self, row):
        """(below, above) lengths of the error bar, clipped so it stays above zero."""
        centre = row[self.centre]
        return min(centre - self.low(row), 0.95 * centre), self.high(row) - centre


STATISTICS = (
    Statistic(
        "p50, bar to p90", "latency_p50_ms",
        lambda row: row["latency_p50_ms"], lambda row: row["latency_p90_ms"], "",
    ),
    Statistic(
        "mean \u00b1 stdev", "latency_mean_ms",
        lambda row: row["latency_mean_ms"] - row["latency_stdev_ms"],
        lambda row: row["latency_mean_ms"] + row["latency_stdev_ms"], "-mean",
    ),
)


def bars(plt, rows, statistic, title, path, log=False):
    """Latency per protocol (medians across seeds), by fault model."""
    figure, axes = plt.subplots(figsize=(12, 4.5))
    if log:
        axes.set_yscale("log")
    labels, positions = [], []
    position = 0
    for fault_model in FAULT_MODELS:
        group = sorted(
            (row for row in rows if row["fault_model"] == fault_model),
            key=lambda row: (row["synchrony"], row["protocol"]),
        )
        for row in group:
            latency = row[statistic.centre]
            if latency is None:
                continue
            below, above = statistic.errors(row)
            colour = "tab:blue" if row["synchrony"] == "ps" else "tab:orange"
            axes.bar(position, latency, yerr=[[below], [above]], color=colour, capsize=2)
            if row["stalled"] or row["no-progress"]:
                axes.text(position, latency, "stall", ha="center", va="bottom", fontsize=7)
            labels.append(f"{row['protocol']} ({row['done']}/{row['runs']})")
            positions.append(position)
            position += 1
        position += 1
    axes.set_xticks(positions, labels, rotation=60, ha="right", fontsize=7)
    axes.set_ylabel(f"latency (ms): {statistic.name}")
    axes.grid(axis="y", which="both", alpha=0.4)
    axes.set_axisbelow(True)
    axes.set_title(f"{title} (blue: partially synchronous, orange: asynchronous)")
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


X_LABELS = {"faulty": "faulty nodes (e equivocators + k crashes)"}


def sweep(plt, rows, statistic, x, title, path, fault_models=FAULT_MODELS):
    """Latency against `x`, one panel per fault model, one line per protocol."""
    from matplotlib.ticker import MaxNLocator

    figure, panels = plt.subplots(
        1, len(fault_models), figsize=(4 * len(fault_models), 4.5), sharey=True, squeeze=False
    )
    panels = panels[0]
    markers = []
    for axes, fault_model in zip(panels, fault_models):
        group = [row for row in rows if row["fault_model"] == fault_model]
        stalled = []
        for protocol in sorted({row["protocol"] for row in group}):
            points = [row for row in group if row["protocol"] == protocol]
            points.sort(key=lambda row: row[x])
            live = [row for row in points if row[statistic.centre] and not row["no-progress"]]
            style = "-" if live and live[0]["synchrony"] == "ps" else "--"
            errors = list(zip(*(statistic.errors(row) for row in live))) or [[], []]
            container = axes.errorbar(
                [row[x] for row in live], [row[statistic.centre] for row in live],
                yerr=errors, fmt=style, marker="o", markersize=3, capsize=2, elinewidth=0.8,
                label=protocol,
            )
            stalls = [row[x] for row in points if row["no-progress"] or row["stalled"]]
            stalled.append((stalls, container.lines[0].get_color()))
        markers.append((axes, stalled))
        axes.set_title(fault_model)
        axes.set_xlabel(X_LABELS.get(x, x))
        axes.xaxis.set_major_locator(MaxNLocator(integer=True))
        axes.grid(True, which="both", alpha=0.4)
        if group:
            axes.legend(fontsize=6)
    # Stalls have no latency: mark them along the bottom, once the shared limits are settled.
    panels[0].set_ylim(bottom=0)
    bottom, top = panels[0].get_ylim()
    for axes, stalled in markers:
        for stalls, colour in stalled:
            if stalls:
                axes.scatter(stalls, [bottom] * len(stalls), marker="x", color=colour, zorder=3)
    panels[0].set_ylim(bottom, top)
    panels[0].set_ylabel(f"latency (ms): {statistic.name}, x = stalled")
    figure.suptitle(f"{title} (solid: partially synchronous, dashed: asynchronous)")
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


WAVE_COLOURS = {2: "tab:blue", 3: "tab:orange", 4: "tab:green", 5: "tab:red"}


def wave_length(protocol):
    """The wave length in an asynchronous protocol's label (`…-w<length>…`)."""
    return int(re.search(r"-w(\d)", protocol).group(1))


def buffer_rounds(plt, rows, path):
    """Direct-commit rate and latency of each asynchronous protocol, coloured by wave length."""
    from matplotlib.patches import Patch

    conditions = [
        ("happy", lambda row: not row["jitter"] == "True" and row["crashes"] == 0),
        ("jitter", lambda row: row["jitter"] == "True" and row["crashes"] == 0),
        ("jitter + crashes", lambda row: row["jitter"] == "True" and row["crashes"] > 0),
    ]
    statistic = STATISTICS[0]
    figure, panels = plt.subplots(2, len(conditions), figsize=(18, 8))
    for column, (name, select) in enumerate(conditions):
        group = sorted((row for row in rows if select(row)), key=lambda row: row["protocol"])
        labels = [f"{row['protocol']} ({row['done']}/{row['runs']})" for row in group]
        colours = [WAVE_COLOURS[wave_length(row["protocol"])] for row in group]
        for line, metric in enumerate(("direct_commit_rate", statistic.centre)):
            axes = panels[line][column]
            values = [row[metric] for row in group]
            errors = None
            if metric == statistic.centre:
                errors = list(zip(*(
                    statistic.errors(row) if row[metric] else (0, 0) for row in group
                )))
            axes.bar(
                range(len(group)), [value or 0 for value in values], yerr=errors,
                color=colours, capsize=2,
            )
            for position, value in enumerate(values):
                if value is None:
                    axes.text(position, 0, "n/a", ha="center", va="bottom", fontsize=7)
            axes.set_xticks(range(len(group)), labels, rotation=60, ha="right", fontsize=6)
            title = metric if metric != statistic.centre else f"latency (ms): {statistic.name}"
            axes.set_title(f"{name}: {title}")
            axes.grid(axis="y", alpha=0.4)
            axes.set_axisbelow(True)
    handles = [
        Patch(color=colour, label=f"w = {length}") for length, colour in WAVE_COLOURS.items()
    ]
    panels[0][0].legend(handles=handles, fontsize=7, loc="lower left")
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def plot(medians, figures):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures.mkdir(parents=True, exist_ok=True)
    rows = load(medians)
    by_experiment = {experiment: [] for experiment in EXPERIMENTS}
    for row in rows:
        by_experiment[row["experiment"]].append(row)
    byzantine = by_experiment["byzantine"]
    for row in byzantine:
        row["faulty"] = row["equivocators"] + row["crashes"]
    for statistic in STATISTICS:
        def name(stem):
            return figures / f"{stem}{statistic.suffix}.png"

        bars(plt, by_experiment["happy"], statistic, "Happy case", name("1-happy"))
        sweep(plt, by_experiment["crashes"], statistic, "crashes", "Crash sweep", name("2-crashes"))
        # Crash-only protocols (2c+1) tolerate no equivocator.
        sweep(
            plt, [row for row in byzantine if row["crashes"] == 0], statistic, "equivocators",
            "Byzantine equivocators", name("3-byzantine"), fault_models=FAULT_MODELS[:-1],
        )
        # Hybrid configurations add crashes once all f equivocate: one line per configuration.
        sweep(
            plt, byzantine, statistic, "faulty", "Equivocators, then crashes",
            name("3b-byzantine-crashes"), fault_models=("5f+3c+1", "3f+2c+1"),
        )
        bars(plt, by_experiment["jitter"], statistic, "Jitter", name("4-jitter"), log=True)
    buffer_rounds(plt, by_experiment["buffer-rounds"], figures / "5-buffer-rounds.png")
    print(f"figures -> {figures}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    aggregate_parser = commands.add_parser("aggregate")
    aggregate_parser.add_argument("--out", type=Path, required=True)
    plot_parser = commands.add_parser("plot")
    plot_parser.add_argument("--medians", type=Path, required=True)
    plot_parser.add_argument("--figures", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.command == "aggregate":
        aggregate(arguments.out)
    else:
        plot(arguments.medians, arguments.figures)


if __name__ == "__main__":
    main()
