# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Summarise every run of the matrix into one CSV per experiment.

    python3 summary.py --out results/sok

Measured from MEASURE_FROM_SECS to the end of the run, over live replicas only (crashed
replicas still generate load but commit nothing).
"""

import argparse
import csv
import re
from pathlib import Path

import matrix

MEASURE_FROM_SECS = 30
# A run is stalled when fewer than this fraction of its windows commit a leader.
STALL_FRACTION = 0.1

SERIES = re.compile(r"^(\w+)(?:\{([^}]*)\})? (\S+)$")
LABEL = re.compile(r'(\w+)="([^"]*)"')
DIRECT_COMMITS = ("slow-commit", "fast-commit")


def read_prom(path):
    """Map (name, frozenset of labels) to value."""
    values = {}
    for line in path.read_text().splitlines():
        match = SERIES.match(line)
        if match:
            name, labels, value = match.groups()
            values[(name, frozenset(LABEL.findall(labels or "")))] = float(value)
    return values


def authority_index(name):
    """Authority names count A..Z, AA, AB, … like spreadsheet columns."""
    index = 0
    for char in name:
        index = index * 26 + ord(char) - ord("A") + 1
    return index - 1


def leaders(values, live=None):
    """Committed-leader counts by commit type, restricted to leaders in `live`."""
    counts = {}
    for (name, labels), value in values.items():
        if name != "committed_leaders_total":
            continue
        labels = dict(labels)
        if live is not None and authority_index(labels["authority"]) not in live:
            continue
        counts[labels["commit_type"]] = counts.get(labels["commit_type"], 0) + value
    return counts


def histogram_quantile(start, end, quantile):
    """Linear-interpolated quantile (seconds) of the latency observed between two snapshots."""
    buckets = []
    for (name, labels), value in end.items():
        if name == "latency_s_bucket":
            bound = dict(labels)["le"]
            delta = value - start.get((name, labels), 0)
            buckets.append((float("inf") if bound == "+Inf" else float(bound), delta))
    buckets.sort()
    total = buckets[-1][1] if buckets else 0
    if total == 0:
        return None
    target, lower, below = quantile * total, 0.0, 0.0
    for bound, cumulative in buckets:
        if cumulative >= target:
            if bound == float("inf"):
                return lower
            return lower + (bound - lower) * (target - below) / max(cumulative - below, 1)
        lower, below = bound, cumulative
    return lower


def snapshots(run_dir, replica):
    """The replica's sampled snapshots by time, in seconds."""
    samples = {}
    for sample_dir in (run_dir / "samples").iterdir():
        time = int(sample_dir.name.rstrip("s"))
        samples[time] = read_prom(sample_dir / f"metrics-{replica}.prom")
    return dict(sorted(samples.items()))


def summarise(job, run_dir):
    meta = (run_dir / "meta.yaml").read_text()
    outcome = re.search(r"outcome: (\S+)", meta).group(1)
    n = matrix.COMMITTEE_SIZE
    live = set(range(n - job.crashes))
    replicas = sorted(path.stem.split("-")[1] for path in run_dir.glob("metrics-*.prom"))
    replicas = [name for name in replicas if authority_index(name) in live]

    latencies, p90s, throughputs, rates = [], [], [], []
    window_fractions, direct_rates = [], []
    for replica in replicas:
        samples = snapshots(run_dir, replica)
        times = list(samples)
        start = samples.get(MEASURE_FROM_SECS)
        end = samples[times[-1]]
        if start is None:
            continue
        span = times[-1] - MEASURE_FROM_SECS
        count = end.get(("latency_s_count", frozenset()), 0)
        count -= start.get(("latency_s_count", frozenset()), 0)
        latency = end.get(("latency_s_sum", frozenset()), 0)
        latency -= start.get(("latency_s_sum", frozenset()), 0)
        if count:
            latencies.append(latency / count * 1000)
            p90 = histogram_quantile(start, end, 0.9)
            if p90 is not None:
                p90s.append(p90 * 1000)
        throughputs.append(count / span)

        committed_start, committed_end = leaders(start), leaders(end)
        decided = sum(committed_end.values()) - sum(committed_start.values())
        rates.append(decided / span)

        # Windows after warm-up with at least one decided leader.
        window_times = [time for time in times if time > MEASURE_FROM_SECS]
        previous, committing = start, 0
        for time in window_times:
            current = samples[time]
            if sum(leaders(current).values()) > sum(leaders(previous).values()):
                committing += 1
            previous = current
        window_fractions.append(committing / max(len(window_times), 1))

        # Direct-commit rate of live leaders' slots, for experiment 5.
        live_start, live_end = leaders(start, live), leaders(end, live)
        live_decided = sum(live_end.values()) - sum(live_start.values())
        live_direct = sum(
            live_end.get(kind, 0) - live_start.get(kind, 0) for kind in DIRECT_COMMITS
        )
        if live_decided:
            direct_rates.append(live_direct / live_decided)

    def mean(values):
        return round(sum(values) / len(values), 2) if values else ""

    window_fraction = mean(window_fractions)
    few_windows = window_fraction != "" and window_fraction < STALL_FRACTION
    stalled = outcome == "no-progress" or few_windows
    return {
        "protocol": job.protocol.label,
        "f": job.protocol.byzantine,
        "crashes_tolerated": job.protocol.crashes,
        "jitter": job.jitter,
        "crashes": job.crashes,
        "equivocators": job.equivocators,
        "seed": job.seed,
        "outcome": outcome,
        "latency_mean_ms": mean(latencies),
        "latency_p90_ms": mean(p90s),
        "throughput_tx_s": mean(throughputs),
        "decided_leaders_s": mean(rates),
        "commit_window_fraction": window_fraction,
        "direct_commit_rate": mean(direct_rates),
        "stalled": stalled,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()

    summary_dir = arguments.out / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    for experiment, jobs in matrix.experiments().items():
        rows, missing = [], 0
        for job in jobs:
            run_dir = arguments.out / "runs" / job.name
            if not (run_dir / "meta.yaml").exists():
                missing += 1
                continue
            rows.append(summarise(job, run_dir))
        path = summary_dir / f"{experiment}.csv"
        if rows:
            with path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        diverged = sum(row["outcome"] == "diverged" for row in rows)
        print(f"{experiment}: {len(rows)} runs, {missing} missing, {diverged} diverged -> {path}")


if __name__ == "__main__":
    main()
