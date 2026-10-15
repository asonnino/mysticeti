#!/usr/bin/env python3
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Hydrozoan evaluation: parsers for the new metrics and a per-file summary.

Extends `plot.py` (whose loaders read the Orcaella campaign) with the series that only the
new campaign (`results/results-0669999/`) carries: the commit-path breakdown
`committed_leaders_total{authority, commit_type}` and the proposal-to-commit block latency
`block_latency_s{kind}`. The same steady-state aggregation as `plot.parse_yaml` is used: the
throughput plateau (scrapes with tps >= 0.9 * max), floor of the latencies over it, median
throughput, median path shares.

    scripts/.venv-eval/bin/python scripts/eval/hydrozoan.py summary [results-dir]
"""

import glob
import math
import os
import re
import sys
from collections import defaultdict

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plot  # noqa: E402  (plot.py helpers: _YamlLoader, _series, _finite, _median, REPO)

RESULTS_NEW = os.path.join(plot.REPO, "results", "results-0669999")
OLD_EU_US = os.path.join(plot.RESULTS, "eu-us")

COMMITS = ("fast-commit", "slow-commit", "indirect-commit-certificate", "indirect-commit-weak")
SKIPS = ("direct-skip", "indirect-skip")

_FILE_RE = re.compile(
    r"measurements-(?P<name>dag-hydrangea|orcaella|mysticeti|blue-bottle-ps)-l(?P<leaders>\d+)"
    r"(?:-f(?P<f>\d+)-c(?P<c>\d+))?(?:-k(?P<k>\d+))?"
    r"-512-(?P<faults>\d+)(?P<order>-region-order)?-(?P<nodes>\d+)-(?P<load>\d+)\.yaml"
)


class Run:
    """One measurements file, decoded from its name."""

    def __init__(self, path):
        self.path = path
        match = _FILE_RE.match(os.path.basename(path))
        if not match:
            raise ValueError(f"unrecognised measurements file: {path}")
        self.name = match["name"]
        self.leaders = int(match["leaders"])
        self.f = int(match["f"]) if match["f"] else None
        self.c = int(match["c"]) if match["c"] else None
        self.k = int(match["k"]) if match["k"] else None
        self.faults = int(match["faults"])
        self.crash_order = "region-order" if match["order"] else "round-robin"
        self.nodes = int(match["nodes"])
        self.load = int(match["load"])

    @property
    def label(self):
        if self.name == "dag-hydrangea":
            return f"Hydrozoan ({self.f}, {self.c}, {self.k})"
        if self.name == "orcaella":
            return f"Orcaella ({self.f}, {self.c})"
        return {"mysticeti": "Mysticeti", "blue-bottle-ps": "Blue Bottle"}[self.name]

    def __repr__(self):
        return f"{self.label} n={self.nodes} x={self.faults} load={self.load}"


def runs(root=RESULTS_NEW):
    found = []
    for path in sorted(glob.glob(os.path.join(root, "measurements-*.yaml"))):
        try:
            found.append(Run(path))
        except ValueError:
            continue
    return found


class _FastLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    """`plot._YamlLoader` on top of libyaml: the n = 50 files are 55 MB each."""


_FastLoader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        loader.construct_mapping(node)
        if isinstance(node, yaml.MappingNode)
        else loader.construct_scalar(node)
        if isinstance(node, yaml.ScalarNode)
        else loader.construct_sequence(node)
    ),
)

_CACHE = {}


def _document(path):
    if path not in _CACHE:
        with open(path) as file:
            _CACHE[path] = yaml.load(file, Loader=_FastLoader)
    return _CACHE[path]


def _samples(path):
    return _document(path)["samples"]


_HUMANTIME = re.compile(r"(\d+)\s*(h|m|s|ms)")


def _humantime_seconds(delay):
    """Seconds of a humantime string such as `2m`, `30s` or `1m 30s` (or a `{secs, nanos}` map)."""
    if isinstance(delay, dict):
        return delay["secs"]
    units = {"h": 3600, "m": 60, "s": 1, "ms": 0.001}
    return sum(int(n) * units[u] for n, u in _HUMANTIME.findall(str(delay)))


def _warmup_end(path):
    """Timestamp before which scrapes are ignored: the load generator's initial delay after
    the first scrape, plus one scrape interval. The first scrapes of a run still carry the
    rates of the previous run on the same nodes."""
    document = _document(path)
    seconds = _humantime_seconds(document["parameters"]["client_parameters"]["initial_delay"])
    first = min(
        (s["timestamp"] for s in document["samples"].get("latency_s_count", [])),
        default=0,
    )
    return first + seconds + 15


def latency(path):
    """(tps, p50_s, p90_s): `plot.parse_yaml` on the cached document."""
    samples = _samples(path)
    plateau = _plateau(path)
    count = plot._series(samples.get("latency_s_count", []))
    tps = plot._median([count[t] for t in plateau])
    p50 = _floor(plot._series(samples.get("latency_s.p50", [])), plateau)
    p90 = _floor(plot._series(samples.get("latency_s.p90", [])), plateau)
    return tps, p50, p90


def _plateau(path):
    """Scrape timestamps of the throughput plateau (tps >= 0.9 * max), after warm-up."""
    samples = _samples(path)
    warmup_end = _warmup_end(path)
    count = plot._series(samples.get("latency_s_count", []))
    finite = {t: c for t, c in count.items() if not math.isnan(c) and t >= warmup_end}
    if not finite:
        return []
    top = max(finite.values())
    return [t for t, c in finite.items() if c >= 0.9 * top]


def _floor(series, plateau):
    values = [
        series[t] for t in plateau if t in series and not math.isnan(series[t]) and series[t] > 0
    ]
    return min(values) if values else float("nan")


def block_latency(path, kind="leader"):
    """(p50_s, p90_s, mean_s) of proposal-to-commit latency for blocks of `kind`."""
    samples = _samples(path)
    plateau = _plateau(path)

    def of(metric):
        return plot._series(
            [s for s in samples.get(metric, []) if s["labels"].get("kind") == kind]
        )

    p50 = _floor(of("block_latency_s.p50"), plateau)
    p90 = _floor(of("block_latency_s.p90"), plateau)
    count, total = of("block_latency_s_count"), of("block_latency_s_sum")
    means = [total[t] / count[t] for t in plateau if t in count and t in total and count[t]]
    mean = plot._median(means) if means else float("nan")
    return p50, p90, mean


def path_shares(path, authorities=None):
    """Share of decided leader slots per commit type over the plateau.

    Rates are summed over nodes and authorities per scrape (restricted to `authorities`
    when given, e.g. the crashed ones), then the per-type share is the median over the
    plateau. Returns {commit_type: share}; commit shares are relative to committed slots,
    skip shares relative to all decided slots.
    """
    samples = _samples(path)
    plateau = set(_plateau(path))
    per_scrape = defaultdict(lambda: defaultdict(float))
    for sample in samples.get("committed_leaders_total", []):
        labels = sample["labels"]
        if authorities is not None and labels.get("authority") not in authorities:
            continue
        value = sample["value"]
        if isinstance(value, (int, float)) and not math.isnan(value):
            per_scrape[round(sample["timestamp"])][labels["commit_type"]] += value
    shares = defaultdict(list)
    for t, by_type in per_scrape.items():
        if t not in plateau:
            continue
        committed = sum(by_type[c] for c in COMMITS)
        decided = committed + sum(by_type[s] for s in SKIPS)
        for commit_type in COMMITS:
            if committed:
                shares[commit_type].append(by_type[commit_type] / committed)
        for commit_type in SKIPS:
            if decided:
                shares[commit_type].append(by_type[commit_type] / decided)
    return {t: plot._median(v) for t, v in shares.items()}


def rows(root=RESULTS_NEW, extra_roots=()):
    """One dict per measurements file: configuration, load, crashes and the steady-state
    metrics (throughput, e2e, leader- and non-leader-block p50/p90, mean queuing, path
    shares)."""
    result = []
    for base in (root, *extra_roots):
        for run in runs(base):
            tps, p50, p90 = latency(run.path)
            b50, b90, bmean = block_latency(run.path)
            n50, n90, nmean = block_latency(run.path, kind="non-leader")
            shares = path_shares(run.path)
            row = {
                "protocol": run.name,
                "label": run.label,
                "f": run.f if run.f is not None else "",
                "c": run.c if run.c is not None else "",
                "k": run.k if run.k is not None else "",
                "leaders": run.leaders,
                "nodes": run.nodes,
                "crashes": run.faults,
                "crash_order": run.crash_order,
                "load_tx_s": run.load,
                "source": os.path.basename(base),
                "tps": round(tps, 1),
                "e2e_p50_ms": round(1000 * p50, 1),
                "e2e_p90_ms": round(1000 * p90, 1),
                "block_p50_ms": round(1000 * b50, 1),
                "block_p90_ms": round(1000 * b90, 1),
                "block_mean_ms": round(1000 * bmean, 1),
                "queuing_p50_ms": round(1000 * (p50 - b50), 1),
                "nonleader_block_p50_ms": round(1000 * n50, 1),
                "nonleader_block_p90_ms": round(1000 * n90, 1),
                "nonleader_block_mean_ms": round(1000 * nmean, 1),
            }
            for commit_type in COMMITS + SKIPS:
                row[f"share_{commit_type}"] = round(shares.get(commit_type, 0.0), 4)
            result.append(row)
    return result


def write_csv(path, root=RESULTS_NEW, extra_roots=()):
    import csv

    table = rows(root, extra_roots)
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(table[0]))
        writer.writeheader()
        writer.writerows(table)
    print(f"{len(table)} runs -> {path}")


def summary(root=RESULTS_NEW):
    print(
        "| run | tps | e2e p50/p90 ms | leader block p50/p90 ms | queuing ms |"
        " fast | slow | ind-cert | ind-weak | d-skip | i-skip |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for run in runs(root):
        tps, p50, p90 = latency(run.path)
        b50, b90, _ = block_latency(run.path)
        shares = path_shares(run.path)
        cell = " | ".join(f"{shares.get(t, float('nan')):.2f}" for t in COMMITS + SKIPS)
        print(
            f"| {run!r} | {tps:,.0f} | {1000 * p50:.0f}/{1000 * p90:.0f} |"
            f" {1000 * b50:.0f}/{1000 * b90:.0f} | {1000 * (p50 - b50):.0f} | {cell} |"
        )


def new_curve(predicate, root=RESULTS_NEW):
    """(tps, p50, p90) lists over the new-campaign files selected by `predicate(run)`."""
    points = sorted(latency(run.path) for run in runs(root) if predicate(run))
    return [p[0] for p in points], [p[1] for p in points], [p[2] for p in points]


def old_curve(predicate):
    """(tps, p50, p90) lists over the Orcaella-campaign eu-us files (`plot.load_curve`)."""
    return plot.load_curve("eu-us", predicate)


HYDROZOAN_STYLE = {  # (f, c, k) -> (marker, linestyle)
    (6, 6, 19): ("o", "solid"),
    (11, 0, 16): ("s", "dashed"),
    (8, 3, 19): ("^", "dashdot"),
    (2, 13, 17): ("v", "dotted"),
    (6, 8, 15): ("P", "solid"),
}
COLOR_HYDROZOAN = "tab:purple"


def figure_e1():
    """E1: latency vs throughput, healthy n = 50, every curve from this build (the earlier
    Orcaella campaign runs ~30 ms slower at 10k; `old_curve` keeps it available for tables)."""
    plt = plot.plt
    plt.figure(figsize=plot.FIGSIZE)
    # Draw order matters: Orcaella (6, 6) coincides with Hydrozoan (6, 6, 19) to the
    # millisecond, so it is drawn last and dashed to stay visible on top of it.
    this_build = {  # label -> (color, marker, linestyle, predicate on Run)
        "Mysticeti": (plot.COLOR["mysticeti"], "D", "solid",
            lambda r: r.name == "mysticeti"),
        "Blue Bottle": (plot.COLOR["bluebottle"], "s", "solid",
            lambda r: r.name == "blue-bottle-ps"),
    }
    for (f_, c_, k_), (marker, ls) in HYDROZOAN_STYLE.items():
        this_build[f"Hydrozoan ({f_}, {c_}, {k_})"] = (COLOR_HYDROZOAN, marker, ls,
            lambda r, f_=f_, c_=c_, k_=k_: r.name == "dag-hydrangea"
            and (r.f, r.c, r.k) == (f_, c_, k_))
    this_build["Orcaella (8, 3)"] = (plot.COLOR["orcaella"], "^", "dashdot",
        lambda r: r.name == "orcaella" and (r.f, r.c) == (8, 3))
    this_build["Orcaella (6, 6)"] = (plot.COLOR["orcaella"], "o", "dashed",
        lambda r: r.name == "orcaella" and (r.f, r.c) == (6, 6))
    for label, (color, marker, ls, predicate) in this_build.items():
        xs, p50, p90 = new_curve(lambda r: predicate(r) and r.faults == 0 and r.nodes == 50)
        plot.plot_line(xs, p50, p90, label, color, marker, ls)

    plt.xlim(left=0, right=105000)
    plt.gca().xaxis.set_major_formatter(plot.x_formatter)
    _axes("Throughput (tx/s)", "Latency (s)", top=0.65)
    plt.legend(
        loc="upper center", ncol=2, frameon=True, framealpha=0.9,
        prop={"weight": "bold", "size": 7}, handlelength=1.6, columnspacing=1.0,
    )
    plot._save("hydrozoan_e1_healthy_n50")
    plt.close()


def _axes(xlabel, ylabel, top=None):
    plt = plot.plt
    plt.xlabel(xlabel, fontweight="bold", fontsize=14)
    plt.ylabel(ylabel, fontweight="bold", fontsize=14)
    plt.xticks(weight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    if top is not None:
        plt.ylim(bottom=0, top=top)
    plt.grid()
    plt.gca().yaxis.set_major_formatter(plot.y_formatter)
    plt.legend(
        loc="best", frameon=True, framealpha=0.9, prop={"weight": "bold", "size": 8},
        handlelength=1.6,
    )


def figure_e2(load=10_000):
    """E2: p50/p90 latency vs the slack k at (f, c) = (6, 6), n = 50, with Mysticeti (this
    build) and the Orcaella (6, 6) endpoint as horizontal references."""
    plt = plot.plt
    plt.figure(figsize=plot.FIGSIZE)
    points = sorted(
        (run.k, *latency(run.path))
        for run in runs()
        if run.name == "dag-hydrangea" and (run.f, run.c) == (6, 6)
        and run.faults == 0 and run.nodes == 50 and run.load == load
    )
    ks = [p[0] for p in points]
    plot.plot_line(ks, [p[2] for p in points], [p[3] for p in points], "Hydrozoan (6, 6, k)",
        COLOR_HYDROZOAN, "o")
    for label, color, ls, predicate in [
        ("Mysticeti, this build", plot.COLOR["mysticeti"], "dashed",
            lambda r: r.name == "mysticeti" and r.faults == 0 and r.nodes == 50 and r.load == load),
        ("Orcaella (6, 6), this build", plot.COLOR["orcaella"], "dotted",
            lambda r: r.name == "orcaella" and (r.f, r.c) == (6, 6) and r.faults == 0
            and r.load == load),
    ]:
        reference = [latency(r.path) for r in runs() if predicate(r)]
        if reference:
            plt.axhline(reference[0][1], color=color, linestyle=ls, linewidth=3, label=label)
    if ks:
        plt.axvline(10, color="gray", linestyle=":", linewidth=1.5,
            label="fast quorum fits the 42 nearby (k >= 10)")
    _axes("Slack k", "Latency (s)", top=0.65)
    plt.gca().xaxis.set_major_locator(plot.plt.matplotlib.ticker.MaxNLocator(integer=True))
    plot._save(f"hydrozoan_e2_k_sweep_{load // 1000}k")
    plt.close()


def figure_e3(load=10_000):
    """E3: p50 latency (whisker to p90) and fast-commit share vs crashed validators at n = 50,
    nearby-first crash order, for Hydrozoan Graded (6, 8, 15), Orcaella (8, 3) and Mysticeti."""
    plt = plot.plt
    curves = [
        ("Hydrozoan (6, 8, 15)", COLOR_HYDROZOAN, "P",
            lambda r: r.name == "dag-hydrangea" and (r.f, r.c, r.k) == (6, 8, 15)),
        ("Orcaella (8, 3)", plot.COLOR["orcaella"], "^",
            lambda r: r.name == "orcaella" and (r.f, r.c) == (8, 3)),
        ("Mysticeti", plot.COLOR["mysticeti"], "D", lambda r: r.name == "mysticeti"),
    ]

    def sweep(predicate):
        return sorted(
            (run.faults, run.path) for run in runs()
            if predicate(run) and run.nodes == 50 and run.load == load
            and (run.faults == 0 or run.crash_order == "region-order")
        )

    # First crash count past each protocol's liveness bound (f + c for Hydrozoan and Orcaella,
    # f for Mysticeti at n = 50): drawn as a zero-latency "stall" point, the visual anchor the
    # Hydrangea curves of `plot.py` also use. The stall itself is arithmetic, confirmed in the
    # simulator campaign, not an AWS run.
    stall_at = {"Hydrozoan (6, 8, 15)": 15, "Orcaella (8, 3)": 12, "Mysticeti": 17}

    plt.figure(figsize=plot.FIGSIZE)
    for label, color, marker, predicate in curves:
        points = [(x, *latency(path)) for x, path in sweep(predicate)]
        plot.plot_line([p[0] for p in points], [p[2] for p in points], [p[3] for p in points],
            label, color, marker)
        if points:
            last_x, _, last_p50, _ = points[-1]
            stall = stall_at[label]
            plt.plot([last_x, stall], [last_p50, 0.0], color=color, linestyle=":", linewidth=3)
            plt.plot([stall], [0.0], color=color, marker="X", markersize=12, linestyle="none")
            plt.annotate("stall", (stall, 0.0), textcoords="offset points", xytext=(0, 8),
                ha="center", fontsize=9, fontweight="bold", color=color)
    _axes("Crashed validators (nearby first)", "Latency (s)", top=0.8)
    plt.gca().xaxis.set_major_locator(plot.plt.matplotlib.ticker.MaxNLocator(integer=True))
    plot._save(f"hydrozoan_e3_crash_latency_{load // 1000}k")
    plt.close()

    plt.figure(figsize=plot.FIGSIZE)
    for label, color, marker, predicate in curves:
        points = [(x, path_shares(path).get("fast-commit", 0.0)) for x, path in sweep(predicate)]
        if points:
            plt.plot([p[0] for p in points], [p[1] for p in points], label=label, color=color,
                marker=marker, linewidth=4, markersize=10)
    _axes("Crashed validators (nearby first)", "Fast-commit share", top=1.05)
    plt.gca().xaxis.set_major_locator(plot.plt.matplotlib.ticker.MaxNLocator(integer=True))
    plot._save(f"hydrozoan_e3_crash_fast_share_{load // 1000}k")
    plt.close()


def figure_e3_bars(xs=(0, 4, 8, 10, 12), load=10_000):
    """E3 zoom: grouped bars of p50 (whisker to p90) at selected crash counts, with the leader
    block latency as a hatched inner bar so the queuing share is visible. A protocol past its
    bound is drawn as a "stall" marker."""
    plt = plot.plt
    import numpy as np

    protocols = [
        ("Hydrozoan (6, 8, 15)", COLOR_HYDROZOAN, 15,
            lambda r: r.name == "dag-hydrangea" and (r.f, r.c, r.k) == (6, 8, 15)),
        ("Orcaella (8, 3)", plot.COLOR["orcaella"], 12,
            lambda r: r.name == "orcaella" and (r.f, r.c) == (8, 3)),
        ("Mysticeti", plot.COLOR["mysticeti"], 17, lambda r: r.name == "mysticeti"),
    ]
    by_x = {
        (label, run.faults): run.path
        for label, _, _, predicate in protocols
        for run in runs()
        if predicate(run) and run.nodes == 50 and run.load == load
        and (run.faults == 0 or run.crash_order == "region-order")
    }
    width = 0.26
    plt.figure(figsize=plot.FIGSIZE)
    for index, (label, color, stall, _) in enumerate(protocols):
        offsets = np.arange(len(xs)) + (index - 1) * width
        for offset, x in zip(offsets, xs):
            path = by_x.get((label, x))
            if path is None:
                if x >= stall:
                    plt.annotate("stall", (offset, 0.0), textcoords="offset points",
                        xytext=(0, 4), ha="center", fontsize=8, fontweight="bold",
                        color=color, rotation=90)
                continue
            _, p50, p90 = latency(path)
            block50, _, _ = block_latency(path)
            plt.bar(offset, p50, width, color=color, edgecolor="black", linewidth=0.8,
                label=label if x == xs[0] else None)
            plt.bar(offset, block50, width * 0.6, color="white", alpha=0.55, hatch="///",
                edgecolor="black", linewidth=0.5)
            plt.errorbar(offset, p50, yerr=[[0.0], [max(0.0, p90 - p50)]], color="black",
                capsize=3, elinewidth=plot.ERRORBAR_LW, capthick=plot.ERRORBAR_LW)
    plt.bar(np.nan, np.nan, width, color="white", hatch="///", edgecolor="black",
        label="leader block latency")
    plt.xticks(np.arange(len(xs)), [str(x) for x in xs])
    _axes("Crashed validators (nearby first)", "Latency (s)", top=0.8)
    plt.legend(loc="upper left", frameon=True, framealpha=0.9,
        prop={"weight": "bold", "size": 8}, handlelength=1.6)
    plot._save(f"hydrozoan_e3_crash_bars_{load // 1000}k")
    plt.close()


def figure_e3_paths(xs=(0, 4, 8, 10, 12), load=10_000):
    """E3 zoom: how the leader slots were decided at selected crash counts. One stacked bar
    per protocol and x: fast commit, slow commit, direct skip, indirect skip (shares of all
    decided slots)."""
    plt = plot.plt
    import numpy as np

    protocols = [
        ("Hydrozoan (6, 8, 15)", 15,
            lambda r: r.name == "dag-hydrangea" and (r.f, r.c, r.k) == (6, 8, 15)),
        ("Orcaella (8, 3)", 12, lambda r: r.name == "orcaella" and (r.f, r.c) == (8, 3)),
        ("Mysticeti", 17, lambda r: r.name == "mysticeti"),
    ]
    layers = [  # (commit_type, label, color, hatch)
        ("fast-commit", "fast commit (votes)", "#6a3d9a", None),
        ("slow-commit", "direct commit (certificates)", "#b39ddb", None),
        ("direct-skip", "direct skip", "#bdbdbd", None),
        ("indirect-skip", "indirect skip", "#616161", "xx"),
    ]
    by_x = {
        (label, run.faults): run.path
        for label, _, predicate in protocols
        for run in runs()
        if predicate(run) and run.nodes == 50 and run.load == load
        and (run.faults == 0 or run.crash_order == "region-order")
    }
    width = 0.26
    plt.figure(figsize=plot.FIGSIZE)
    for index, (label, stall, _) in enumerate(protocols):
        offsets = np.arange(len(xs)) + (index - 1) * width
        for offset, x in zip(offsets, xs):
            path = by_x.get((label, x))
            if path is None:
                if x >= stall:
                    plt.annotate("stall", (offset, 0.0), textcoords="offset points",
                        xytext=(0, 4), ha="center", fontsize=8, fontweight="bold",
                        rotation=90)
                continue
            shares = path_shares(path)
            # Shares of ALL decided slots: rescale the commit shares by the committed fraction.
            skipped = shares.get("direct-skip", 0.0) + shares.get("indirect-skip", 0.0)
            committed = 1.0 - skipped
            bottom = 0.0
            for commit_type, layer_label, color, hatch in layers:
                value = shares.get(commit_type, 0.0)
                if commit_type in ("fast-commit", "slow-commit"):
                    value *= committed
                if value <= 0:
                    continue
                plt.bar(offset, value, width, bottom=bottom, color=color, hatch=hatch,
                    edgecolor="black", linewidth=0.6,
                    label=layer_label if (index == 0 and x == xs[0]) or (
                        commit_type in ("direct-skip", "indirect-skip") and x == 12
                        and label == "Hydrozoan (6, 8, 15)") else None)
                bottom += value
            plt.annotate(label.split(" ")[0][0], (offset, 1.01), ha="center", fontsize=7,
                fontweight="bold")
    plt.xticks(np.arange(len(xs)), [str(x) for x in xs])
    _axes("Crashed validators (nearby first)", "Share of leader slots", top=1.12)
    plt.legend(loc="lower left", ncol=2, frameon=True, framealpha=0.95,
        prop={"weight": "bold", "size": 7}, handlelength=1.4)
    plot._save(f"hydrozoan_e3_crash_paths_{load // 1000}k")
    plt.close()


def figure_e5(load=10_000):
    """E5: small committees, 1 and 2 nearby crashes (Tokyo holds one validator). Grouped bars
    of p50 (whisker to p90) with the leader block latency as a hatched inner bar."""
    plt = plot.plt
    import numpy as np

    configs = [  # (label, color, hatch, predicate)
        ("Mysticeti n=10", plot.COLOR["mysticeti"], None,
            lambda r: r.name == "mysticeti" and r.nodes == 10),
        ("Mysticeti n=7", plot.COLOR["mysticeti"], "..",
            lambda r: r.name == "mysticeti" and r.nodes == 7),
        ("Orcaella (1,1) n=9", plot.COLOR["orcaella"], None,
            lambda r: r.name == "orcaella" and (r.f, r.c) == (1, 1) and r.nodes == 9),
        ("Hydrozoan (1,1,4) n=10", COLOR_HYDROZOAN, None,
            lambda r: r.name == "dag-hydrangea" and (r.f, r.c, r.k) == (1, 1, 4)
            and r.nodes == 10),
    ]
    crashes = (1, 2)
    by_key = {
        (label, run.faults): run.path
        for label, _, _, predicate in configs
        for run in runs()
        if predicate(run) and run.load == load and run.faults in crashes
        and run.crash_order == "region-order"
    }
    width = 0.2
    plt.figure(figsize=plot.FIGSIZE)
    for index, (label, color, hatch, _) in enumerate(configs):
        for group, x in enumerate(crashes):
            path = by_key.get((label, x))
            if path is None:
                continue
            offset = group + (index - 1.5) * width
            _, p50, p90 = latency(path)
            block50, _, _ = block_latency(path)
            plt.bar(offset, p50, width, color=color, hatch=hatch, edgecolor="black",
                linewidth=0.8, label=label if group == 0 or (label, 1) not in by_key else None)
            plt.bar(offset, block50, width * 0.6, color="white", alpha=0.55, hatch="///",
                edgecolor="black", linewidth=0.5)
            plt.errorbar(offset, p50, yerr=[[0.0], [max(0.0, p90 - p50)]], color="black",
                capsize=3, elinewidth=plot.ERRORBAR_LW, capthick=plot.ERRORBAR_LW)
    plt.bar(np.nan, np.nan, width, color="white", hatch="///", edgecolor="black",
        label="leader block latency")
    plt.xticks(range(len(crashes)), [f"{x} crash{'es' if x > 1 else ''}" for x in crashes])
    _axes("Crashed validators (nearby first)", "Latency (s)", top=0.7)
    plt.legend(loc="upper left", ncol=2, frameon=True, framealpha=0.9,
        prop={"weight": "bold", "size": 7}, handlelength=1.6)
    plot._save(f"hydrozoan_e5_small_committees_{load // 1000}k")
    plt.close()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "summary"
    if command == "summary":
        summary(sys.argv[2] if len(sys.argv) > 2 else RESULTS_NEW)
    elif command == "e1":
        figure_e1()
    elif command == "e2":
        figure_e2()
    elif command == "e3":
        figure_e3()
    elif command == "e3bars":
        figure_e3_bars()
    elif command == "e3paths":
        figure_e3_paths()
    elif command == "e5":
        figure_e5()
    elif command == "csv":
        # `csv <path>` writes the plotted deployment only (the paper's table); `csv <path> all`
        # adds the probe and later-day runs, kept in this repo as the comparison base.
        target = sys.argv[2] if len(sys.argv) > 2 else os.path.join(RESULTS_NEW, "aws-summary.csv")
        extra = (RESULTS_NEW + "-probe", RESULTS_NEW + "-day2") if "all" in sys.argv[3:] else ()
        write_csv(target, extra_roots=extra)
    elif command == "all":
        summary()
        figure_e1()
        figure_e2()
        figure_e3()
    else:
        sys.exit(__doc__)
