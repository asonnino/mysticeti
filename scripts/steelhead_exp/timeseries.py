# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""timeseries.csv loading and per-tick aggregation for the timeline figures."""

import csv
import warnings

import numpy as np

# All-NaN windows (nothing committed) are expected; nanmean's warning is noise.
warnings.filterwarnings("ignore", message="Mean of empty slice")

COUNTER_COLUMNS = ["direct_commits", "indirect_commits", "direct_skips",
                    "indirect_skips", "leader_timeouts"]
MEAN_COLUMNS = ["steelhead_period", "latency_p50_ms", "latency_avg_ms"]


def load(run_dir):
    """Column arrays for one run's timeseries.csv, or None when absent.
    Blank latency cells become NaN; missing columns are tolerated."""
    path = run_dir / "timeseries.csv"
    if not path.exists():
        return None
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return None
    columns = {}
    for key in rows[0]:
        values = [row.get(key, "") for row in rows]
        columns[key] = np.array([float(v) if v else np.nan for v in values])
    return columns


def per_tick(columns):
    """Aggregate across replicas per tick: counters are summed, the latency
    and period columns averaged (NaN-aware). Returns {column: array} keyed on
    a sorted `time_s` grid."""
    times = np.unique(columns["time_s"])
    ticks = {"time_s": times}
    for name in COUNTER_COLUMNS:
        if name in columns:
            ticks[name] = np.array([
                np.nansum(columns[name][columns["time_s"] == t]) for t in times
            ])
    for name in MEAN_COLUMNS:
        if name in columns:
            with np.errstate(invalid="ignore"):
                ticks[name] = np.array([
                    np.nanmean(columns[name][columns["time_s"] == t]) for t in times
                ])
    return ticks


def window_rate(ticks, name):
    """Per-window rate of a cumulative counter, aligned on the tick grid
    (first window measured from zero)."""
    values = ticks[name]
    times = ticks["time_s"]
    deltas = np.diff(values, prepend=0.0)
    spans = np.diff(times, prepend=0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(spans > 0, deltas / spans, np.nan)


def seed_mean(tick_sets):
    """Mean across seeds on the common tick grid (deterministic runs share
    it); NaN-aware so empty windows don't drag the mean."""
    times = tick_sets[0]["time_s"]
    merged = {"time_s": times}
    for name in tick_sets[0]:
        if name == "time_s":
            continue
        stack = np.vstack([ticks[name] for ticks in tick_sets])
        with np.errstate(invalid="ignore"):
            merged[name] = np.nanmean(stack, axis=0)
    return merged
