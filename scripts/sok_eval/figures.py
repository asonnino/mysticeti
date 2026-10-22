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
import statistics
from pathlib import Path

EXPERIMENTS = ("happy", "crashes", "byzantine", "jitter", "buffer-rounds")
KEYS = ("experiment", "protocol", "jitter", "crashes", "equivocators")
MEDIAN_COLUMNS = (
    "latency_mean_ms",
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
            row[column] = float(row[column]) if row[column] else None
    return rows


def bars(plt, rows, title, path):
    """Mean latency per protocol (median across seeds, min-max as error bars), by fault model."""
    figure, axes = plt.subplots(figsize=(12, 4.5))
    labels, positions, colours = [], [], []
    position = 0
    for fault_model in FAULT_MODELS:
        group = sorted(
            (row for row in rows if row["fault_model"] == fault_model),
            key=lambda row: (row["synchrony"], row["protocol"]),
        )
        for row in group:
            latency = row["latency_mean_ms"]
            if latency is None:
                continue
            error = [[latency - row["latency_min_ms"]], [row["latency_max_ms"] - latency]]
            colour = "tab:blue" if row["synchrony"] == "ps" else "tab:orange"
            axes.bar(position, latency, yerr=error, color=colour, capsize=2)
            if row["stalled"] or row["no-progress"]:
                axes.text(position, latency, "stall", ha="center", va="bottom", fontsize=7)
            labels.append(f"{row['protocol']} ({row['done']}/{row['runs']})")
            positions.append(position)
            position += 1
        position += 1
    axes.set_xticks(positions, labels, rotation=60, ha="right", fontsize=7)
    axes.set_ylabel("mean latency (ms)")
    axes.set_title(f"{title} (blue: partially synchronous, orange: asynchronous)")
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def sweep(plt, rows, x, title, path):
    """Mean latency against `x`, one panel per fault model, one line per protocol."""
    figure, panels = plt.subplots(1, len(FAULT_MODELS), figsize=(20, 4.5), sharey=True)
    for axes, fault_model in zip(panels, FAULT_MODELS):
        group = [row for row in rows if row["fault_model"] == fault_model]
        for protocol in sorted({row["protocol"] for row in group}):
            points = [row for row in group if row["protocol"] == protocol]
            points.sort(key=lambda row: row[x])
            live = [row for row in points if row["latency_mean_ms"] and not row["no-progress"]]
            style = "-" if live and live[0]["synchrony"] == "ps" else "--"
            line, = axes.plot(
                [row[x] for row in live], [row["latency_mean_ms"] for row in live],
                style, marker="o", markersize=3, label=protocol,
            )
            stalls = [row[x] for row in points if row["no-progress"] or row["stalled"]]
            if stalls:
                bottom = [axes.get_ylim()[0]] * len(stalls)
                axes.scatter(stalls, bottom, marker="x", color=line.get_color())
        axes.set_title(fault_model)
        axes.set_xlabel(x)
        axes.set_yscale("log")
        if group:
            axes.legend(fontsize=6)
    panels[0].set_ylabel("mean latency (ms), x = stalled")
    figure.suptitle(f"{title} (solid: partially synchronous, dashed: asynchronous)")
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def buffer_rounds(plt, rows, path):
    """Direct-commit rate and latency of each asynchronous protocol at both wave lengths."""
    conditions = [
        ("happy", lambda row: not row["jitter"] == "True" and row["crashes"] == 0),
        ("jitter", lambda row: row["jitter"] == "True" and row["crashes"] == 0),
        ("jitter + crashes", lambda row: row["jitter"] == "True" and row["crashes"] > 0),
    ]
    figure, panels = plt.subplots(2, len(conditions), figsize=(18, 8))
    for column, (name, select) in enumerate(conditions):
        group = sorted((row for row in rows if select(row)), key=lambda row: row["protocol"])
        labels = [f"{row['protocol']} ({row['done']}/{row['runs']})" for row in group]
        for line, metric in enumerate(("direct_commit_rate", "latency_mean_ms")):
            axes = panels[line][column]
            values = [row[metric] or 0 for row in group]
            axes.bar(range(len(group)), values, color="tab:orange")
            axes.set_xticks(range(len(group)), labels, rotation=60, ha="right", fontsize=6)
            axes.set_title(f"{name}: {metric}")
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
    bars(plt, by_experiment["happy"], "Happy case", figures / "1-happy.png")
    sweep(plt, by_experiment["crashes"], "crashes", "Crash sweep", figures / "2-crashes.png")
    byzantine = [row for row in by_experiment["byzantine"] if row["crashes"] == 0]
    sweep(plt, byzantine, "equivocators", "Byzantine equivocators", figures / "3-byzantine.png")
    bars(plt, by_experiment["jitter"], "Jitter", figures / "4-jitter.png")
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
