# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The paper figures, rendered from cached run directories only."""

import sys

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter, MaxNLocator

import matrix
import summary
import timeseries
from style import (
    PROTO_STYLE,
    TIMELINE_HEIGHT_IN,
    apply_style,
    legend_above,
    save,
    seconds_formatter,
    shade_phases,
    throughput_formatter,
)


def by_params(jobs, **match):
    return [job for job in jobs
            if all(job.params.get(key) == value for key, value in match.items())]


def seed_summaries(jobs):
    """Pass-run summaries for a seed group; warns on incomplete groups."""
    summaries = []
    for job in jobs:
        result = summary.run_summary(job.out_dir)
        if result is None:
            print(f"note: skipping {job.name} (missing or non-pass)", file=sys.stderr)
        else:
            summaries.append(result)
    return summaries


def seed_timelines(jobs):
    tick_sets = []
    for job in jobs:
        columns = timeseries.load(job.out_dir)
        if columns is None:
            print(f"note: skipping {job.name} (no timeseries)", file=sys.stderr)
        else:
            tick_sets.append(timeseries.per_tick(columns))
    return timeseries.seed_mean(tick_sets) if tick_sets else None


def fig_good(jobs):
    """Latency vs load L-graphs, one per (committee, pair)."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            figure, axes = plt.subplots()
            plotted = False
            for slug in matrix.protocols(pair):
                label, color, marker = PROTO_STYLE[slug]
                points = []
                for load in matrix.LOADS:
                    group = by_params(jobs, committee=committee, pair=pair,
                                        proto=slug, load=load)
                    summaries = seed_summaries(group)
                    if not summaries:
                        continue
                    tps_mean, tps_std = summary.seed_stats([s.tps for s in summaries])
                    p50_mean, p50_std = summary.seed_stats(
                        [s.latency_percentile_s[0.5] for s in summaries])
                    if tps_mean is None or p50_mean is None:
                        continue
                    points.append((tps_mean, tps_std, p50_mean, p50_std))
                if not points:
                    continue
                x, xerr, y, yerr = (np.array(v) for v in zip(*points))
                axes.errorbar(x, y, xerr=xerr, yerr=yerr, label=label, color=color,
                                marker=marker, linestyle="dotted", capsize=3,
                                markersize=4, linewidth=1)
                plotted = True
            if not plotted:
                plt.close(figure)
                continue
            axes.set_xlabel("Throughput (tx/s)")
            axes.set_ylabel("Latency (s)")
            axes.set_xlim(left=0)
            axes.set_ylim(bottom=0)
            axes.xaxis.set_major_formatter(FuncFormatter(throughput_formatter))
            axes.yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
            legend_above(axes, ncol=3)
            save(figure, f"good-n{committee}-{pair}")


def timeline_axes(figure_height=TIMELINE_HEIGHT_IN):
    figure, axes = plt.subplots(figsize=(plt.rcParams["figure.figsize"][0], figure_height))
    axes.set_xlabel("Time (s)")
    axes.set_ylabel("Latency (s)")
    axes.set_ylim(bottom=0)
    axes.yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
    return figure, axes


def fig_attack(jobs):
    """Windowed-p50 latency timelines under targeted delay."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            figure, axes = timeline_axes()
            phases = None
            plotted = False
            for slug in matrix.timeline_protocols(pair):
                label, color, _marker = PROTO_STYLE[slug]
                group = by_params(jobs, committee=committee, pair=pair, proto=slug)
                merged = seed_timelines(group)
                if merged is None:
                    continue
                phases = phases or (group[0].phases if group else None)
                axes.plot(merged["time_s"], merged["latency_p50_ms"] / 1000.0,
                            label=label, color=color, linewidth=1)
                plotted = True
            if not plotted:
                plt.close(figure)
                continue
            if phases:
                shade_phases(axes, phases)
            axes.set_xlim(left=0)
            legend_above(axes, ncol=2)
            save(figure, f"attack-n{committee}-{pair}")


def fig_adaptive(jobs):
    """Adaptive Steelhead: latency (left axis) + period in force (right)."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            adaptive = by_params(jobs, committee=committee, pair=pair, proto="sh-ada")
            merged = seed_timelines(adaptive)
            if merged is None:
                continue
            figure, axes = timeline_axes()
            for slug, line_style in [("sh-p16", (0, (4, 2))), ("sh-p1", (0, (1, 2)))]:
                reference = seed_timelines(
                    by_params(jobs, committee=committee, pair=pair, proto=slug))
                if reference is None:
                    continue
                label, color, _marker = PROTO_STYLE[slug]
                axes.plot(reference["time_s"], reference["latency_p50_ms"] / 1000.0,
                            label=label, color=color, linewidth=0.8, linestyle=line_style,
                            alpha=0.7)
            label, color, _marker = PROTO_STYLE["sh-ada"]
            axes.plot(merged["time_s"], merged["latency_p50_ms"] / 1000.0,
                        label=label, color=color, linewidth=1.2)
            period_axes = axes.twinx()
            period_axes.step(merged["time_s"], merged["steelhead_period"], where="post",
                                color="C3", linewidth=1.2, label="period in force")
            period_axes.set_ylabel("Period")
            period_axes.set_ylim(bottom=0)
            period_axes.yaxis.set_major_locator(MaxNLocator(integer=True))
            period_axes.grid(False)
            if adaptive:
                shade_phases(axes, adaptive[0].phases)
            axes.set_xlim(left=0)
            latency_handles, latency_labels = axes.get_legend_handles_labels()
            period_handles, period_labels = period_axes.get_legend_handles_labels()
            legend_above(axes, ncol=2, handles=latency_handles + period_handles,
                            labels=latency_labels + period_labels)
            save(figure, f"adaptive-n{committee}-{pair}")


def fig_async(jobs):
    """Appendix: random asynchrony with f crashed — canary on/off vs Mahi."""
    figure, axes = timeline_axes()
    plotted = False
    for slug in ["sh-p1-canary", "sh-p1-nocanary", "mahi5"]:
        label, color, _marker = PROTO_STYLE[slug]
        merged = seed_timelines(by_params(jobs, proto=slug))
        if merged is None:
            continue
        axes.plot(merged["time_s"], merged["latency_p50_ms"] / 1000.0,
                    label=label, color=color, linewidth=1)
        plotted = True
    if not plotted:
        plt.close(figure)
        return
    axes.set_xlim(left=0)
    legend_above(axes, ncol=3)
    save(figure, "async-n10-mm")


FIGURES = {
    "good": (fig_good, matrix.good_jobs),
    "attack": (fig_attack, matrix.attack_jobs),
    "adaptive": (fig_adaptive, matrix.adaptive_jobs),
    "async": (fig_async, matrix.async_jobs),
}


def plot(only=None):
    apply_style()
    for name, (render, jobs) in FIGURES.items():
        if only and name not in only:
            continue
        render(jobs())
