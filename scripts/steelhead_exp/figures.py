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
    trim_spines,
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
                for load in matrix.LOADS[committee]:
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
    axes.yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
    return figure, axes


def fig_phase(jobs):
    """The Barnacle-style figure: latency over time across the good -> bad ->
    good phases; adaptive Steelhead (solid) must track the best of the pure
    sync (dashed) and pure async (dotted) baselines in every phase."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            sync_slug = "myst" if pair == "mm" else "bbps"
            async_slug = "mahi5" if pair == "mm" else "bbasync"
            figure, axes = timeline_axes()
            phases = None
            plotted = False
            for slug, line_style in [(sync_slug, "--"), (async_slug, ":"), ("sh-ada", "-")]:
                label, color, _marker = PROTO_STYLE[slug]
                group = by_params(jobs, committee=committee, pair=pair, proto=slug)
                merged = seed_timelines(group)
                if merged is None:
                    continue
                phases = phases or (group[0].phases if group else None)
                width = 1.4
                axes.plot(merged["time_s"], merged["latency_avg_ms"] / 1000.0,
                    line_style, label=label, color=color, linewidth=width)
                plotted = True
            if not plotted:
                plt.close(figure)
                continue
            if phases:
                shade_phases(axes, phases)
                axes.set_xlim(0, phases[-1].end_s)
            # Clip the transition backlog off-frame; the steady states and the
            # settled async level are the story, the transient is text.
            axes.set_ylim(0, 4)
            trim_spines(axes)
            legend_above(axes, ncol=3)
            save(figure, f"phase-n{committee}-{pair}")


def fig_period(jobs):
    """The adaptive knob trace (Barnacle's leaders figure): the period in
    force over time, one step line per seed, with the static bounds as
    horizontal guides."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            group = by_params(jobs, committee=committee, pair=pair, proto="sh-ada")
            tick_sets = []
            for job in group:
                columns = timeseries.load(job.out_dir)
                if columns is not None:
                    tick_sets.append(timeseries.per_tick(columns))
            if not tick_sets:
                continue
            figure, axes = timeline_axes()
            axes.set_ylabel("Period in force")
            label, color, _marker = PROTO_STYLE["sh-ada"]
            for index, ticks in enumerate(tick_sets):
                axes.step(ticks["time_s"], ticks["steelhead_period"], where="post",
                    color=color, linewidth=1.4 if index == 0 else 0.6,
                    alpha=1.0 if index == 0 else 0.35)
            max_period = matrix.ADAPTIVE["max_period"]
            axes.axhline(max_period, linestyle="--", color="C3", linewidth=0.9,
                label=f"max period ({max_period})")
            axes.axhline(1, linestyle=":", color="C2", linewidth=0.9, label="period 1")
            axes.plot([], [], "-", color=color, label=label)
            axes.set_ylim(0, max_period + 1)
            axes.yaxis.set_major_locator(MaxNLocator(integer=True))
            axes.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0f}"))
            if group:
                shade_phases(axes, group[0].phases)
                axes.set_xlim(0, group[0].phases[-1].end_s)
            trim_spines(axes)
            legend_above(axes, ncol=3)
            save(figure, f"period-n{committee}-{pair}")


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
    axes.set_ylim(bottom=0)
    trim_spines(axes)
    legend_above(axes, ncol=3)
    save(figure, "async-n10-mm")


def fig_profiles(jobs, campaign, filename):
    """Three-panel profile comparison: one panel per parameter profile, the
    baselines faint behind the bold adaptive line."""
    profiles = ["sh-sync", "sh-bal", "sh-tumult"]
    figure, panels = plt.subplots(
        1, 3, figsize=(plt.rcParams["figure.figsize"][0], 1.9), sharey=True)
    plotted = False
    for panel, slug in zip(panels, profiles):
        phases = None
        for base_slug, line_style in [("myst", "--"), ("mahi5", ":")]:
            label, color, _marker = PROTO_STYLE[base_slug]
            merged = seed_timelines(by_params(jobs, proto=base_slug))
            if merged is None:
                continue
            panel.plot(merged["time_s"], merged["latency_avg_ms"] / 1000.0,
                line_style, label=label, color=color, linewidth=0.9, alpha=0.8)
        group = by_params(jobs, proto=slug)
        merged = seed_timelines(group)
        if merged is None:
            continue
        phases = group[0].phases if group else None
        panel.plot(merged["time_s"], merged["latency_avg_ms"] / 1000.0,
            label="Steelhead", color="black", linewidth=1.3)
        plotted = True
        if phases:
            shade_phases(panel, phases)
            panel.set_xlim(0, phases[-1].end_s)
        panel.set_title(PROTO_STYLE[slug][0], fontsize=8, pad=3)
        panel.set_xlabel("Time (s)")
        trim_spines(panel)
    if not plotted:
        plt.close(figure)
        return
    panels[0].set_ylabel("Latency (s)")
    panels[0].set_ylim(0, 1.5)
    panels[0].yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
    handles, labels = panels[0].get_legend_handles_labels()
    figure.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.98),
        ncol=3, frameon=False)
    figure.tight_layout(pad=0.4)
    save(figure, filename)


def fig_profiles_attack(jobs):
    fig_profiles(jobs, "profiles", "profiles-attack")


def fig_profiles_storm(jobs):
    fig_profiles(jobs, "storm", "profiles-storm")


# Two rows with distinct y-scales: sync-ish cases (sub-second) on top, the
# async-plateau cases (multi-second) below.
WEATHER_PANELS = [
    ("subthresh", "Small leader delay"),
    ("targeted", "Large leader delay"),
    ("crash", "Permanent crash faults"),
    ("partial", "Partial random network"),
    ("full", "Full random network"),
    ("jitter", "High jitter"),
]
WEATHER_ROW_YLIM = [0.5, 1.2]  # top row, bottom row (baseline ~0.2s)
WEATHER_SETTLE_MODELS = {"partial", "full", "jitter"}  # switching panels get a settle guide


def _settle_time(merged, phases):
    """The tick where Steelhead first reaches (and holds) its async plateau
    after the sync->async transition; None if it never settles."""
    attack = next((p for p in phases if p.label == "attack"), None)
    if attack is None:
        return None
    t = merged["time_s"]
    lat = merged["latency_avg_ms"] / 1000.0
    window = (t > attack.start_s + 20) & (t <= attack.end_s)
    plateau = np.nanmedian(lat[window])
    idx = np.where((t > attack.start_s) & (t <= attack.end_s))[0]
    for k, i in enumerate(idx):
        if np.all(np.abs(lat[idx[k:]] - plateau) <= 0.2 * plateau):
            return float(t[i])
    return None


def _denoise(y, window=3):
    """Nan-aware rolling median over a plotted latency timeline: drops isolated
    single-window spikes/dips (loose commits from a backlogged protocol) while
    leaving plateaus and the phase transitions in place. Only non-NaN samples
    are rewritten, so no-commit gaps stay gaps."""
    y = np.asarray(y, dtype=float)
    half = window // 2
    out = y.copy()
    for i in range(len(y)):
        if np.isnan(y[i]):
            continue
        out[i] = np.nanmedian(y[max(0, i - half):i + half + 1])
    return out


def fig_weather(jobs):
    """One 6-panel figure per protocol pair and committee size; each panel a
    network model, with the two pure baselines and adaptive Steelhead bold."""
    for committee in matrix.COMMITTEES:
        for pair in matrix.PAIRS:
            sub = [j for j in jobs
                if j.params["pair"] == pair and j.params["committee"] == committee]
            if sub:
                _fig_weather_pair(sub, pair, committee)


# The bottom row breaks its y-axis: a short upper band catches the sync
# protocol's degraded plateau (~3s) while the lower band keeps the async
# plateaus legible. The sync protocol's full stall (Full random network, ~19s)
# still runs off the top of the upper band, which reads as "shoots off".
WEATHER_BREAK_LO = 1.2
WEATHER_BREAK_HI = (2.0, 4.5)


def _draw_weather_lines(axes, sub, sync_slug, async_slug, phases):
    """Plot the two baselines and Steelhead (denoised) on one axis; shade the
    phases. Returns Steelhead's merged timeline (or None)."""
    for base_slug, ls in [(sync_slug, "--"), (async_slug, ":")]:
        merged = seed_timelines(by_params(sub, proto=base_slug))
        if merged is None:
            continue
        label, color, _m = PROTO_STYLE[base_slug]
        axes.plot(merged["time_s"], _denoise(merged["latency_avg_ms"]) / 1000.0, ls,
            label=label, color=color, linewidth=1.2)
    merged = seed_timelines(by_params(sub, proto="sh-ada"))
    if merged is not None:
        axes.plot(merged["time_s"], _denoise(merged["latency_avg_ms"]) / 1000.0,
            label="Steelhead", color="black", linewidth=1.2)
    if phases:
        shade_phases(axes, phases)
        axes.set_xlim(0, phases[-1].end_s)
    return merged


def _weather_transitions(phases):
    return sorted({p.start_s for p in phases if p.label == "attack"}
                | {p.end_s for p in phases if p.label == "attack"
                    and p.end_s < phases[-1].end_s})


def _weather_settle(axeslist, model, merged, phases):
    if phases and model in WEATHER_SETTLE_MODELS and merged is not None:
        settle = _settle_time(merged, phases)
        if settle is not None:
            for axes in axeslist:
                axes.axvline(settle, color="0.4", linewidth=0.7, linestyle=(0, (3, 2)))


def _break_marks(ax_hi, ax_lo):
    """Draw the // break marks between an upper and lower band (point-sized, so
    they stay square despite the two bands' different heights)."""
    kwargs = dict(marker=[(-1, -0.6), (1, 0.6)], markersize=6, linestyle="none",
        color="k", mec="k", mew=0.8, clip_on=False)
    ax_hi.plot([0, 1], [0, 0], transform=ax_hi.transAxes, **kwargs)
    ax_lo.plot([0, 1], [1, 1], transform=ax_lo.transAxes, **kwargs)


def _draw_full_panel(axes, sub, sync_slug, async_slug, phases, ylim, title, model):
    """A single (unbroken) weather panel: the three lines, y-limit, title,
    settle guide and transition ticks."""
    merged = _draw_weather_lines(axes, sub, sync_slug, async_slug, phases)
    axes.set_ylim(0, ylim)
    axes.set_title(title, fontsize=8, pad=3)
    axes.yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
    if phases:
        _weather_settle([axes], model, merged, phases)
        axes.set_xticks(_weather_transitions(phases))
    trim_spines(axes)
    return merged


def _needs_break(jobs, sync_slug):
    """Whether any bottom-row panel's sync baseline runs off the lower band --
    only then is the broken axis worth its cost. A robust sync rule (which
    never leaves the band) keeps a plain axis."""
    for model, _title in WEATHER_PANELS[3:]:
        sub = [j for j in jobs if j.params["model"] == model]
        merged = seed_timelines(by_params(sub, proto=sync_slug))
        if merged is not None:
            values = _denoise(merged["latency_avg_ms"]) / 1000.0
            if np.nanmax(values) > WEATHER_BREAK_LO:
                return True
    return False


def _fig_weather_pair(jobs, pair, committee=10):
    sync_slug, async_slug = ("myst", "mahi5") if pair == "mm" else ("bbps", "bbasync")
    broken = _needs_break(jobs, sync_slug)
    width = plt.rcParams["figure.figsize"][0]
    top, hi, lo = [], [], []
    if broken:
        # Bottom row is split into an upper break band over the main lower band;
        # row 1 is an empty spacer so the bottom titles clear the top row.
        figure = plt.figure(figsize=(width, 3.5))
        grid = figure.add_gridspec(4, 3, height_ratios=[1.0, 0.16, 0.32, 1.0])
        for col in range(3):
            t = figure.add_subplot(grid[0, col], sharey=top[0] if top else None)
            h = figure.add_subplot(grid[2, col], sharex=t, sharey=hi[0] if hi else None)
            low = figure.add_subplot(grid[3, col], sharex=t, sharey=lo[0] if lo else None)
            top.append(t)
            hi.append(h)
            lo.append(low)
    else:
        figure = plt.figure(figsize=(width, 3.2))
        grid = figure.add_gridspec(2, 3)
        for col in range(3):
            t = figure.add_subplot(grid[0, col], sharey=top[0] if top else None)
            low = figure.add_subplot(grid[1, col], sharex=t, sharey=lo[0] if lo else None)
            top.append(t)
            lo.append(low)
    for axes in top:
        axes.tick_params(labelbottom=False)  # only the bottom band carries the x-axis

    for index, (model, title) in enumerate(WEATHER_PANELS):
        sub = [j for j in jobs if j.params["model"] == model]
        phases = sub[0].phases if sub else None
        col = index % 3
        if index < 3:
            _draw_full_panel(top[col], sub, sync_slug, async_slug, phases,
                WEATHER_ROW_YLIM[0], title, model)
        elif not broken:
            _draw_full_panel(lo[col], sub, sync_slug, async_slug, phases,
                WEATHER_ROW_YLIM[1], title, model)
        else:
            ax_hi, ax_lo = hi[col], lo[col]
            merged = _draw_weather_lines(ax_hi, sub, sync_slug, async_slug, phases)
            _draw_weather_lines(ax_lo, sub, sync_slug, async_slug, phases)
            ax_lo.set_ylim(0, WEATHER_BREAK_LO)
            ax_hi.set_ylim(*WEATHER_BREAK_HI)
            ax_hi.set_yticks([3, 4])
            ax_hi.set_title(title, fontsize=8, pad=3)
            for axes in (ax_hi, ax_lo):
                axes.yaxis.set_major_formatter(FuncFormatter(seconds_formatter))
            ax_hi.spines["bottom"].set_visible(False)
            ax_hi.spines["right"].set_visible(False)
            ax_hi.spines["top"].set_visible(False)
            ax_lo.spines["top"].set_visible(False)
            ax_hi.tick_params(bottom=False, labelbottom=False)
            _break_marks(ax_hi, ax_lo)
            if phases:
                _weather_settle([ax_hi, ax_lo], model, merged, phases)
                ax_lo.set_xticks(_weather_transitions(phases))
            trim_spines(ax_lo)

    rows = [top, lo] + ([hi] if broken else [])
    for col in (1, 2):
        for row in rows:
            row[col].tick_params(labelleft=False)
    top[0].set_ylabel("Latency (s)")
    lo[0].set_ylabel("Latency (s)")
    figure.supxlabel("Time (s)", fontsize=8, fontweight="bold", y=0.015)
    handles, labels = top[0].get_legend_handles_labels()
    figure.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.925),
        ncol=3, frameon=False)
    # Explicit margins: tight_layout ignores the figure legend, over-reserves the
    # top, and warns on the broken-axis sub-axes.
    figure.subplots_adjust(left=0.1, right=0.99, top=0.90, bottom=0.11,
        hspace=0.06 if broken else 0.18, wspace=0.13)
    name = f"weather-{pair}" if committee == 10 else f"weather-{pair}-{committee}"
    save(figure, name)


FIGURES = {
    "phase": (fig_phase, matrix.adaptive_jobs),
    "weather": (fig_weather, matrix.weather_jobs),
    "period": (fig_period, matrix.adaptive_jobs),
    "async": (fig_async, matrix.async_jobs),
    "profiles": (fig_profiles_attack, matrix.profile_jobs),
    "storm": (fig_profiles_storm, matrix.storm_jobs),
    # Latency-vs-load L-graphs: kept for reference, not a headline figure
    # (the simulator's saturation is not hardware-real).
    "good": (fig_good, matrix.good_jobs),
}


def plot(only=None):
    apply_style()
    for name, (render, jobs) in FIGURES.items():
        if only and name not in only:
            continue
        render(jobs())
