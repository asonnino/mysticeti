# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Summarise every run of the matrix into one CSV per experiment, plus the consistency check.

    python3 summary.py --out results/sok

Measured from MEASURE_FROM_SECS to the end of the run, over live replicas only (crashed
replicas still generate load but commit nothing). Every job gets a row; one that did not
finish says why in its status.
"""

import argparse
import csv
import functools
import re
import statistics
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import matrix
import run

MEASURE_FROM_SECS = 30
# A run is stalled when its median live replica commits a leader in fewer than this fraction
# of its windows.
STALL_FRACTION = 0.1
# A run is unstable when it commits less than this fraction of the load its live replicas offer.
UNSTABLE_GOODPUT = 0.9
# The load generator sends ceil(load / 10) transactions every 100 ms.
OFFERED_PER_REPLICA = -(-matrix.LOAD_PER_REPLICA // 10) * 10
DIGITS = 6

SERIES = re.compile(r"^(\w+)(?:\{([^}]*)\})? (\S+)\s*$")
LABEL = re.compile(r'(\w+)="([^"]*)"')
SAMPLE_DIR = re.compile(r"^(\d+)s$")
# The only metric families the summary reads.
FAMILIES = ("latency_s_", "committed_leaders_total")
DIRECT_COMMITS = ("slow-commit", "fast-commit")
SKIPS = ("direct-skip", "indirect-skip")


def read_prom(path):
    """Map (name, frozenset of labels) to value, for the families the summary reads."""
    values = {}
    for line in path.read_text().splitlines():
        if not line.startswith(FAMILIES):
            continue
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
    """Decided-leader counts by commit type, restricted to leaders in `live`."""
    counts = {}
    for (name, labels), value in values.items():
        if name != "committed_leaders_total":
            continue
        labels = dict(labels)
        if live is not None and authority_index(labels["authority"]) not in live:
            continue
        counts[labels["commit_type"]] = counts.get(labels["commit_type"], 0) + value
    return counts


def committed(counts):
    """Committed leaders among `leaders()` counts, leaving out skipped slots."""
    return sum(value for kind, value in counts.items() if kind not in SKIPS)


def bucket_deltas(start, end):
    """Cumulative latency bucket counts observed between two snapshots, by upper bound."""
    deltas = {}
    for (name, labels), value in end.items():
        if name == "latency_s_bucket":
            bound = dict(labels)["le"]
            bound = float("inf") if bound == "+Inf" else float(bound)
            deltas[bound] = value - start.get((name, labels), 0)
    return deltas


def histogram_quantile(buckets, quantile):
    """Linear-interpolated quantile (seconds) of cumulative bucket counts by upper bound."""
    buckets = sorted(buckets.items())
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
        match = SAMPLE_DIR.match(sample_dir.name)
        if match and sample_dir.is_dir():
            samples[int(match.group(1))] = read_prom(sample_dir / f"metrics-{replica}.prom")
    return dict(sorted(samples.items()))


def status_of(run_dir):
    """'ok' for a finished run, otherwise why it did not finish."""
    if (run_dir / run.COMPLETE).exists():
        return "ok"
    if (run_dir / run.FAILED).exists():
        return (run_dir / run.FAILED).read_text().strip()
    return "missing"


def summarise(job, out):
    """One CSV row for the job."""
    row = {
        "protocol": job.protocol.label,
        "f": job.protocol.byzantine,
        "crashes_tolerated": job.protocol.crashes,
        "jitter": job.jitter,
        "crashes": job.crashes,
        "equivocators": job.equivocators,
        "seed": job.seed,
    }
    run_dir = out / "runs" / job.name
    status = status_of(run_dir)
    if status != "ok":
        return {**row, "status": status}
    row["outcome"] = re.search(r"outcome: (\S+)", (run_dir / "meta.yaml").read_text()).group(1)

    live = set(range(matrix.COMMITTEE_SIZE - job.crashes))
    replicas = sorted(path.stem.split("-")[1] for path in run_dir.glob("metrics-*.prom"))
    replicas = [name for name in replicas if authority_index(name) in live]

    latency_sum = latency_count = 0.0
    buckets = {}
    live_direct = live_decided = 0.0
    throughputs, committed_rates, skipped_rates, window_fractions = [], [], [], []
    for replica in replicas:
        samples = snapshots(run_dir, replica)
        times = list(samples)
        if MEASURE_FROM_SECS not in samples:
            return {**row, "status": f"no {MEASURE_FROM_SECS}s sample"}
        start, end = samples[MEASURE_FROM_SECS], samples[times[-1]]
        span = times[-1] - MEASURE_FROM_SECS

        # Every replica observes every committed transaction: pool them for the run's latency.
        count = end.get(("latency_s_count", frozenset()), 0)
        count -= start.get(("latency_s_count", frozenset()), 0)
        latency_count += count
        latency_sum += end.get(("latency_s_sum", frozenset()), 0)
        latency_sum -= start.get(("latency_s_sum", frozenset()), 0)
        for bound, delta in bucket_deltas(start, end).items():
            buckets[bound] = buckets.get(bound, 0) + delta
        throughputs.append(count / span)

        decided_start, decided_end = leaders(start), leaders(end)
        commits = committed(decided_end) - committed(decided_start)
        decided = sum(decided_end.values()) - sum(decided_start.values())
        committed_rates.append(commits / span)
        skipped_rates.append((decided - commits) / span)

        # Windows after warm-up with at least one committed (not skipped) leader.
        window_times = [time for time in times if time > MEASURE_FROM_SECS]
        previous, committing = start, 0
        for time in window_times:
            current = samples[time]
            if committed(leaders(current)) > committed(leaders(previous)):
                committing += 1
            previous = current
        window_fractions.append(committing / max(len(window_times), 1))

        # Direct commits among the decided slots of live leaders, for experiment 5. A skipped
        # live leader counts as not committed directly.
        live_start, live_end = leaders(start, live), leaders(end, live)
        live_decided += sum(live_end.values()) - sum(live_start.values())
        live_direct += sum(
            live_end.get(kind, 0) - live_start.get(kind, 0) for kind in DIRECT_COMMITS
        )

    def rounded(value):
        return "" if value is None else round(value, DIGITS)

    def mean(values):
        return sum(values) / len(values) if values else None

    p90 = histogram_quantile(buckets, 0.9)
    throughput = mean(throughputs)
    offered = OFFERED_PER_REPLICA * len(live)
    goodput = throughput / offered if throughput is not None else None
    window_fraction = statistics.median(window_fractions) if window_fractions else None
    stalled = row["outcome"] == "no-progress" or (
        window_fraction is not None and window_fraction < STALL_FRACTION
    )
    return {
        **row,
        "status": status,
        "latency_mean_ms": rounded(latency_sum / latency_count * 1000 if latency_count else None),
        "latency_p90_ms": rounded(p90 * 1000 if p90 is not None else None),
        "throughput_tx_s": rounded(throughput),
        "offered_tx_s": offered,
        "goodput": rounded(goodput),
        "committed_leaders_s": rounded(mean(committed_rates)),
        "skipped_leaders_s": rounded(mean(skipped_rates)),
        "commit_window_fraction": rounded(window_fraction),
        "direct_commit_rate": rounded(live_direct / live_decided if live_decided else None),
        "stalled": stalled,
        "unstable": goodput is not None and goodput < UNSTABLE_GOODPUT,
    }


def final_metrics(out, job):
    """Every replica's final snapshot, or None when the run did not finish."""
    run_dir = out / "runs" / job.name
    if status_of(run_dir) != "ok":
        return None
    return {path.name: read_prom(path) for path in sorted(run_dir.glob("metrics-*.prom"))}


def consistency(out):
    """Each c = 0 configuration and its partner must end with identical metrics."""
    protocols = {protocol.label: protocol for protocol in matrix.MAIN}
    rows = []
    for protocol, partner in matrix.CONSISTENCY_PAIRS:
        first = final_metrics(out, matrix.Job(protocol, matrix.CONSISTENCY_SEED))
        second = final_metrics(out, matrix.Job(protocols[partner], matrix.CONSISTENCY_SEED))
        if first is None or second is None:
            verdict = "missing"
        else:
            verdict = "identical" if first == second else "different"
        rows.append({"configuration": protocol.label, "partner": partner, "verdict": verdict})
    return rows


def write_csv(path, rows):
    with path.open("w", newline="") as file:
        fields = list(dict.fromkeys(field for row in rows for field in row))
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=None)
    arguments = parser.parse_args()

    summary_dir = arguments.out / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    # Summarise each distinct job once; experiments share many runs.
    jobs = matrix.all_jobs()
    task = functools.partial(summarise, out=arguments.out)
    if arguments.workers == 1:
        summaries = list(map(task, jobs))
    else:
        with ProcessPoolExecutor(arguments.workers) as pool:
            summaries = list(pool.map(task, jobs, chunksize=8))
    rows = {job.name: row for job, row in zip(jobs, summaries)}
    for experiment, experiment_jobs in matrix.experiments().items():
        experiment_rows = [rows[job.name] for job in experiment_jobs]
        path = summary_dir / f"{experiment}.csv"
        write_csv(path, experiment_rows)
        statuses = [row["status"] for row in experiment_rows]
        flagged = {
            flag: sum(row.get(flag) is True for row in experiment_rows)
            for flag in ("stalled", "unstable")
        }
        diverged = sum(row.get("outcome") == "diverged" for row in experiment_rows)
        print(
            f"{experiment}: {statuses.count('ok')} ok, {len(statuses) - statuses.count('ok')} "
            f"not finished, {diverged} diverged, {flagged['stalled']} stalled, "
            f"{flagged['unstable']} unstable -> {path}"
        )

    consistency_rows = consistency(arguments.out)
    write_csv(summary_dir / "consistency.csv", consistency_rows)
    for row in consistency_rows:
        print(f"consistency: {row['configuration']} vs {row['partner']}: {row['verdict']}")


if __name__ == "__main__":
    main()
