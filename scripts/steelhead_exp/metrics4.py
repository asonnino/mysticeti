# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The four headline numbers per config: green / red overhead (ms) and the
s->a / a->s transitions (s). All latencies are seconds internally."""
import warnings

import numpy as np

import timeseries
from figures import by_params

warnings.filterwarnings("ignore")


def timeline(jobs, proto):
    """Seed-mean latency in SECONDS on the common tick grid."""
    stack, times = [], None
    for job in by_params(jobs, proto=proto):
        columns = timeseries.load(job.out_dir)
        if columns is None:
            continue
        ticks = timeseries.per_tick(columns)
        if times is None:
            times = ticks["time_s"]
        if len(ticks["time_s"]) == len(times):
            stack.append(ticks["latency_avg_ms"] / 1000.0)
    if not stack:
        return None, None
    return times, np.nanmean(np.vstack(stack), axis=0)


def measure(jobs, proto, start=30, end=150):
    times, latency = timeline(jobs, proto)
    if latency is None:
        return None
    _, sync_ref = timeline(jobs, "myst")
    _, async_ref = timeline(jobs, "mahi5")
    green_mask = times <= start
    red_mask = (times > start + 40) & (times <= end)
    green_oh = (np.nanmean(latency[green_mask]) - np.nanmean(sync_ref[green_mask])) * 1000
    red_oh = (np.nanmedian(latency[red_mask]) - np.nanmedian(async_ref[red_mask])) * 1000
    baseline = np.nanmean(latency[green_mask])
    plateau = np.nanmedian(latency[red_mask])
    inside = np.where((times > start) & (times <= end))[0]
    into = next((times[i] - start for k, i in enumerate(inside)
                 if np.all(np.abs(latency[inside[k:]] - plateau) <= 0.15 * plateau)), None)
    after = np.where(times > end)[0]
    back = next((times[i] - end for k, i in enumerate(after)
                 if np.all(latency[after[k:]] <= baseline * 1.3)), None)
    return dict(green_oh=green_oh, red_oh=red_oh, into=into, back=back)


def print_row(label, jobs, proto):
    m = measure(jobs, proto)
    if m is None:
        print(f"{label:24} (no data)")
        return
    into = f"{m['into']:.0f}s" if m["into"] is not None else "n/a"
    back = f"{m['back']:.0f}s" if m["back"] is not None else "NEVER"
    print(f"{label:24} {m['green_oh']:>6.0f}ms {m['red_oh']:>6.0f}ms {into:>7} {back:>8}")
