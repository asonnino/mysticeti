#!/usr/bin/env python3
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Simulator campaign for the Hydrozoan evaluation.

Every run places the validators on the six-region AWS testbed (round-robin, Tokyo the tail) and
delays each message by half the measured region-to-region RTT (``latency.geographic``, matrix
from ``scripts/eval/ping-matrix.sh``). Crashes isolate validators nearby-first (a ``partition``
that lists the alive group, us-east-1 first as the testbed's ``crash_order: region-order``).
Run groups (``generate --group``):

- ``validation``: the AWS configurations, fault-free, k sweep, E3 crash sweeps, E5/E6 small
    committees; pass = within ~10% of the AWS p50s with the same fast-quorum steps.
- ``equiv``: equivocating leaders, one and two nearby (eu-west-3) plus a Tokyo one, and
    equivocation on top of eight crashes.
- ``heatmap``: every tight split 3f + 2c + k + 1 = 50, fault-free and with f + c crashes.
- ``sweep``: full crash sweeps past p and the liveness bound for the six paper configurations.

Sub-commands: ``generate`` writes one YAML per run, ``parse`` summarises ``metrics-A.prom`` of
every run directory into a CSV plus tables, ``plot`` draws the figures from that CSV.
"""

import argparse
import csv
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMMITTEE_SIZE = 50
DURATION_SECS = 120
LOAD_GENERATOR = {"load": 10, "transaction_size": 512, "initial_delay": "0s"}
# Testbed regions in the orchestrator's settings order: authority i sits in REGIONS[i % 6], so
# at n = 50 Tokyo (the tail) holds indices 5, 11, ..., 47 and us-east-1 holds 0, 6, ..., 48.
REGIONS = ["us-east-1", "us-east-2", "eu-central-1", "eu-west-2", "eu-west-3", "ap-northeast-1"]
RTT_MATRIX = ROOT / "results" / "ping-matrix-2026-09-18T0819.csv"


def hydrozoan(f, c, k):
    return {"protocol": "dag-hydrangea", "leader_count": 2, "f": f, "c": c, "k": k}


def orcaella(f, c):
    return {"protocol": "orcaella", "leader_count": 2, "f": f, "c": c}


MYSTICETI = {"protocol": "mysticeti", "leader_count": 2}
BLUE_BOTTLE = {"protocol": "blue-bottle-partially-synchronous", "leader_count": 2}

# Paper configurations at n = 50 (docs/hydrozoan-evaluation-plan.md, section 2).
PROTOCOLS = {
    "hydrozoan-graded": hydrozoan(6, 8, 15),
    "hydrozoan-balanced": hydrozoan(6, 6, 19),
    "hydrozoan-byz-only": hydrozoan(11, 0, 16),
    "hydrozoan-byz-heavy": hydrozoan(8, 3, 19),
    "hydrozoan-crash-heavy": hydrozoan(2, 13, 17),
    "hydrozoan-mysticeti-end": hydrozoan(16, 0, 0),
    "mysticeti": MYSTICETI,
    "orcaella-8-3": orcaella(8, 3),
    "orcaella-6-6": orcaella(6, 6),
    "blue-bottle": BLUE_BOTTLE,
}

# Thresholds that the sweeps must straddle: p (fast slack) and f + c (liveness bound).
THRESHOLDS = {
    "hydrozoan-graded": {"p": 11, "liveness": 14},
    "hydrozoan-balanced": {"p": 12, "liveness": 12},
    "hydrozoan-byz-only": {"p": 8, "liveness": 11},
    "mysticeti": {"p": None, "liveness": 16},
    "orcaella-8-3": {"p": 11, "liveness": 11},
    "blue-bottle": {"p": 9, "liveness": 9},
}

# Crash counts of the AWS sweeps (E3, E5); the simulator reproduces them for validation.
AWS_CRASHES = {
    "hydrozoan-graded": [0, 2, 4, 6, 8, 10, 11, 12, 14],
    "orcaella-8-3": [0, 2, 4, 8, 10, 11],
    "mysticeti": [4, 8, 10, 12, 16],
}
# Full sweeps past p and past the liveness bound (optional group 4).
SWEEP_CRASHES = {
    "hydrozoan-graded": [0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 15, 16],
    "hydrozoan-balanced": [0, 4, 8, 10, 11, 12, 13, 14],
    "hydrozoan-byz-only": [0, 4, 8, 9, 10, 11, 12],
    "mysticeti": [0, 4, 8, 12, 14, 16, 17],
    "orcaella-8-3": [0, 4, 8, 10, 11, 12, 13],
    "blue-bottle": [0, 4, 8, 9, 10],
}
# Small committees of E5/E6 (Tokyo holds index 5 only): (name, consensus, n).
SMALL = [
    ("mysticeti-n10", MYSTICETI, 10),
    ("mysticeti-n7", MYSTICETI, 7),
    ("orcaella-1-1-n9", orcaella(1, 1), 9),
    ("hydrozoan-1-1-4-n10", hydrozoan(1, 1, 4), 10),
]
# Equivocators: eu-west-3 validators (indices 4 and 10), the region crashed last among the
# nearby ones, so they survive every crash count of the sweeps; index 5 is the Tokyo one.
NEARBY_EQUIVOCATORS = [[4], [4, 10]]
TAIL_EQUIVOCATOR = [5]

COMMIT_TYPES = [
    "fast-commit",
    "slow-commit",
    "indirect-commit-certificate",
    "indirect-commit-weak",
    "direct-skip",
    "indirect-skip",
]
COMMITS = COMMIT_TYPES[:4]
SKIPS = COMMIT_TYPES[4:]


class TaggedLoader(yaml.SafeLoader):
    """serde_yaml writes enums as tags (`topology: !partition [...]`); read them as a mapping."""


def construct_tagged(loader, suffix, node):
    if isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_scalar(node)
    return {suffix: value}


TaggedLoader.add_multi_constructor("!", construct_tagged)


def load_yaml(path):
    return yaml.load(path.read_text(), Loader=TaggedLoader)


def authority_label(index):
    if index < 26:
        return chr(ord("A") + index)
    return chr(ord("A") + index // 26 - 1) + chr(ord("A") + index % 26)


def region_of(index):
    return REGIONS[index % len(REGIONS)]


def crash_order(n):
    """The testbed's `crash_order: region-order`: whole regions in settings order, so us-east-1
    goes first and Tokyo last; inside a region, selection (index) order."""
    return [i for region in REGIONS for i in range(n) if region_of(i) == region]


def geography(rtt_csv, extra_ms=(0.0, 1.0)):
    """The `latency.geographic` block: region medians of the measured per-pair average RTTs."""
    import statistics

    pairs = defaultdict(list)
    with open(rtt_csv, newline="") as file:
        for row in csv.DictReader(file):
            if row["received"] != "0":
                pairs[(row["src_region"], row["dst_region"])].append(float(row["avg_ms"]))
    rtt = {a: {b: round(statistics.median(pairs[(a, b)]), 1) for b in REGIONS} for a in REGIONS}
    return {
        "geographic": {
            "regions": list(REGIONS),
            "rtt_ms": rtt,
            "extra_ms": {"start": extra_ms[0], "end": extra_ms[1]},
        }
    }


def run_config(
    name, consensus, seed, n=COMMITTEE_SIZE, crashes=0, equivocators=(),
    latency=None, load=LOAD_GENERATOR["load"], duration=DURATION_SECS,
):
    crashed = set(crash_order(n)[:crashes])
    alive = [i for i in range(n) if i not in crashed]
    assert not crashed & set(equivocators), f"{name}: an equivocator is crashed"
    config = {
        "name": name,
        "committee_size": n,
        "duration_secs": duration,
        "rng_seed": seed,
        "topology": "fullMesh" if crashes == 0 else {"partition": [alive]},
        "equivocating_leaders": list(equivocators),
        "replica_parameters": {"consensus": dict(consensus)},
        "load_generator": dict(LOAD_GENERATOR, load=load),
    }
    if latency is not None:
        config["latency"] = latency
    return config


def tight_splits(n=COMMITTEE_SIZE):
    """Every (f, c, k) with 3f + 2c + k + 1 = n and k as small as possible... i.e. k = n - 1 -
    3f - 2c >= 0: the tight splits, one cell per (f, c) of the resilience plane."""
    return [(f, c, n - 1 - 3 * f - 2 * c)
            for f in range(0, (n - 1) // 3 + 1) for c in range(0, (n - 1 - 3 * f) // 2 + 1)]


def group_runs(group, seeds, latency, load):
    """The run configs of one campaign group (docs/hydrozoan-evaluation-plan.md, Simulator)."""
    runs = []

    def add(prefix, consensus, seed, **kwargs):
        runs.append(
            run_config(f"{prefix}-s{seed}", consensus, seed, latency=latency, load=load, **kwargs)
        )

    for seed in range(seeds):
        if group == "validation":
            for name, consensus in PROTOCOLS.items():
                add(f"val-{name}-x00", consensus, seed)
            for k in (0, 8, 10, 12):
                add(f"val-hydrozoan-6-6-{k}-x00", hydrozoan(6, 6, k), seed)
            for name, xs in AWS_CRASHES.items():
                for x in xs:
                    if x:
                        add(f"val-{name}-x{x:02d}", PROTOCOLS[name], seed, crashes=x)
            for name, consensus, n in SMALL:
                for x in (0, 1, 2):
                    add(f"val-{name}-x{x:02d}", consensus, seed, n=n, crashes=x)
        elif group == "equiv":
            for name in SWEEP_CRASHES:
                for group_ in NEARBY_EQUIVOCATORS:
                    add(f"equiv-{name}-e{len(group_)}", PROTOCOLS[name], seed,
                        equivocators=group_)
            add("equiv-hydrozoan-graded-tail-e1", PROTOCOLS["hydrozoan-graded"], seed,
                equivocators=TAIL_EQUIVOCATOR)
            for name in ("hydrozoan-graded", "mysticeti"):
                add(f"equiv-crash-{name}-x08-e1", PROTOCOLS[name], seed, crashes=8,
                    equivocators=NEARBY_EQUIVOCATORS[0])
        elif group == "heatmap":
            for f, c, k in tight_splits():
                for x in (0, f + c):
                    add(f"heat-f{f:02d}-c{c:02d}-x{x:02d}", hydrozoan(f, c, k), seed, crashes=x)
        elif group == "sweep":
            for name, xs in SWEEP_CRASHES.items():
                for x in xs:
                    add(f"sweep-{name}-x{x:02d}", PROTOCOLS[name], seed, crashes=x)
        else:
            sys.exit(f"unknown group {group}")
    return runs


def generate(out_dir, group, seeds, rtt_csv, extra_ms, load):
    out_dir.mkdir(parents=True, exist_ok=True)
    latency = geography(rtt_csv, extra_ms) if rtt_csv else None
    runs = group_runs(group, seeds, latency, load)
    for run in runs:
        with open(out_dir / f"{run['name']}.yaml", "w") as file:
            yaml.safe_dump(run, file, sort_keys=False)
    print(f"{len(runs)} run configs of group {group} written to {out_dir}")


SAMPLE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$"
)
LABEL = re.compile(r'(\w+)="([^"]*)"')


def parse_prom(path):
    """Return {(metric, frozenset(labels)): value} for a Prometheus text exposition."""
    samples = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        match = SAMPLE.match(line)
        if not match:
            continue
        labels = frozenset(LABEL.findall(match.group("labels") or ""))
        samples[(match.group("name"), labels)] = float(match.group("value"))
    return samples


def select(samples, metric, **labels):
    """Samples of `metric` whose labels include `labels`, as [(labels, value)]."""
    wanted = set(labels.items())
    return [
        (dict(key_labels), value)
        for (name, key_labels), value in samples.items()
        if name == metric and wanted <= set(key_labels)
    ]


def quantile(buckets, count, q):
    """Linear interpolation inside the Prometheus histogram bucket holding quantile q."""
    if count == 0:
        return math.nan
    target = q * count
    previous_bound, previous_cumulative = 0.0, 0.0
    for bound, cumulative in sorted(buckets):
        if cumulative >= target:
            if cumulative == previous_cumulative:
                return bound
            fraction = (target - previous_cumulative) / (cumulative - previous_cumulative)
            return previous_bound + fraction * (bound - previous_bound)
        previous_bound, previous_cumulative = bound, cumulative
    return previous_bound


def histogram(samples, metric, **labels):
    buckets = [
        (float(key["le"]), value)
        for key, value in select(samples, f"{metric}_bucket", **labels)
        if key["le"] != "+Inf"
    ]
    count = sum(value for _, value in select(samples, f"{metric}_count", **labels))
    total = sum(value for _, value in select(samples, f"{metric}_sum", **labels))
    squared_metric = metric.replace("_s", "_squared_s")
    squared = sum(value for _, value in select(samples, squared_metric, **labels))
    mean = total / count if count else math.nan
    variance = squared / count - mean * mean if count else math.nan
    return {
        "count": count,
        "mean": mean,
        "stdev": math.sqrt(max(variance, 0.0)) if count else math.nan,
        "p50": quantile(buckets, count, 0.5),
        "p90": quantile(buckets, count, 0.9),
    }


TAIL_REGION = "ap-northeast-1"
POOLED_PREFIXES = ("block_latency_s", "block_latency_squared_s", "latency_s", "latency_squared_s")


def observer_samples(run_dir, alive):
    """Metrics of the run as seen by its correct replicas. Latency histograms are pooled over
    every alive replica whose `metrics-<label>.prom` is present (the testbed's Prometheus
    aggregates over all nodes the same way); the commit-type counters come from the first alive
    replica, which is A unless a crash sweep took us-east-1 down. Returns the samples and the
    number of pooled replicas."""
    files = {path.stem[len("metrics-"):]: path for path in run_dir.glob("metrics-*.prom")}
    labels = [authority_label(i) for i in sorted(alive) if authority_label(i) in files] or ["A"]
    per_replica = {label: parse_prom(files[label]) for label in labels}
    samples = dict(per_replica[labels[0]])
    pooled = defaultdict(float)
    for label in labels:
        for key, value in per_replica[label].items():
            if key[0].startswith(POOLED_PREFIXES):
                pooled[key] += value
    samples.update(pooled)
    return samples, len(labels)


def summarise(run_dir):
    config = load_yaml(run_dir / "config.yaml")
    meta = load_yaml(run_dir / "meta.yaml")
    n = config["committee_size"]
    duration = config["duration_secs"]
    consensus = config["replica_parameters"]["consensus"]
    protocol = next(
        (name for name, params in PROTOCOLS.items() if params.items() <= consensus.items()),
        "-".join(str(consensus[key]) for key in ("protocol", "f", "c", "k") if key in consensus),
    )

    topology = config["topology"]
    alive = set(range(n))
    if isinstance(topology, dict) and "partition" in topology:
        alive = {index for group in topology["partition"] for index in group}
    crashed = sorted(set(range(n)) - alive)
    equivocators = config.get("equivocating_leaders", [])
    samples, observers = observer_samples(run_dir, alive)
    regions = config.get("latency", {}).get("geographic", {}).get("regions", [])
    crashed_tail = sum(1 for i in crashed if regions and regions[i % len(regions)] == TAIL_REGION)

    by_authority = defaultdict(lambda: defaultdict(float))
    for key, value in select(samples, "committed_leaders_total"):
        by_authority[key["authority"]][key["commit_type"]] += value

    def totals(indices):
        labels = {authority_label(index) for index in indices}
        result = defaultdict(float)
        for label in labels:
            for commit_type, value in by_authority[label].items():
                result[commit_type] += value
        return result

    everyone = totals(range(n))
    crashed_totals = totals(crashed)
    equivocator_totals = totals(equivocators)
    decided = sum(everyone.values())
    committed = sum(everyone[t] for t in COMMITS)

    leader = histogram(samples, "block_latency_s", kind="leader")
    non_leader = histogram(samples, "block_latency_s", kind="non-leader")
    transactions = histogram(samples, "latency_s")

    row = {
        "run": config.get("name", run_dir.name),
        "experiment": run_dir.name.split("-")[0] if not run_dir.name.startswith("equiv-crash")
        else "equiv-crash",
        "protocol": protocol,
        "consensus": consensus["protocol"],
        "n": n,
        "f": consensus.get("f", ""),
        "c": consensus.get("c", ""),
        "k": consensus.get("k", ""),
        "seed": config.get("rng_seed", 0),
        "crashes": len(crashed),
        "crashed_tail": crashed_tail,
        "equivocators": len(equivocators),
        "observers": observers,
        "outcome": meta["outcome"],
        "decided_slots": int(decided),
        "commits_per_s": decided / duration,
        "fast_share": everyone["fast-commit"] / committed if committed else math.nan,
        "leader_latency_mean_ms": 1000 * leader["mean"],
        "leader_latency_p50_ms": 1000 * leader["p50"],
        "leader_latency_p90_ms": 1000 * leader["p90"],
        "nonleader_latency_mean_ms": 1000 * non_leader["mean"],
        "nonleader_latency_p50_ms": 1000 * non_leader["p50"],
        "nonleader_latency_p90_ms": 1000 * non_leader["p90"],
        "tx_latency_mean_ms": 1000 * transactions["mean"],
        "tx_latency_p50_ms": 1000 * transactions["p50"],
        "tx_latency_p90_ms": 1000 * transactions["p90"],
        # Every replica observes every committed transaction and block, so pooled counts are
        # `observers` copies of the run's totals; the quantiles are the pooled distribution.
        "tps": transactions["count"] / duration / observers,
    }
    for commit_type in COMMIT_TYPES:
        row[commit_type] = int(everyone[commit_type])
    for commit_type in COMMIT_TYPES:
        row[f"crashed_{commit_type}"] = int(crashed_totals[commit_type])
    for commit_type in COMMIT_TYPES:
        row[f"equivocator_{commit_type}"] = int(equivocator_totals[commit_type])
    return row


def parse(results_dir, csv_path):
    rows = []
    for run_dir in sorted(results_dir.iterdir()):
        if (run_dir / "meta.yaml").exists() and (run_dir / "metrics-A.prom").exists():
            rows.append(summarise(run_dir))
        elif run_dir.is_dir():
            print(f"skipping {run_dir.name}: incomplete", file=sys.stderr)
    if not rows:
        sys.exit(f"no complete runs under {results_dir}")
    with open(csv_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} runs -> {csv_path}\n")
    print_tables(rows)


def print_tables(rows):
    def fmt(value, digits=0):
        if isinstance(value, float):
            return "nan" if math.isnan(value) else f"{value:.{digits}f}"
        return str(value)

    crash_rows = [r for r in rows if r["experiment"] == "crash"]
    if crash_rows:
        print("## Crash sweep (n = 50, metrics of replica A, seeds averaged)\n")
        print(
            "| protocol | x | outcome | fast share | leader lat mean/p50/p90 ms | tx p50 ms |"
            " commits/s | crashed slots: direct-skip / indirect-skip / committed |"
        )
        print("|---|---|---|---|---|---|---|---|")
        groups = defaultdict(list)
        for row in crash_rows:
            groups[(row["protocol"], row["crashes"])].append(row)
        for (protocol, x), group in sorted(groups.items()):
            mean = lambda key: sum(r[key] for r in group) / len(group)  # noqa: E731
            outcomes = "/".join(sorted({r["outcome"] for r in group}))
            crashed_committed = sum(
                mean(f"crashed_{t}") for t in COMMITS
            )
            print(
                f"| {protocol} | {x} | {outcomes} | {fmt(mean('fast_share'), 2)} |"
                f" {fmt(mean('leader_latency_mean_ms'))}/{fmt(mean('leader_latency_p50_ms'))}"
                f"/{fmt(mean('leader_latency_p90_ms'))} | {fmt(mean('tx_latency_p50_ms'))} |"
                f" {fmt(mean('commits_per_s'), 1)} | {fmt(mean('crashed_direct-skip'))} /"
                f" {fmt(mean('crashed_indirect-skip'))} / {fmt(crashed_committed)} |"
            )
        print()

    equiv_rows = [r for r in rows if r["experiment"].startswith("equiv")]
    if equiv_rows:
        print("## Equivocating leaders (n = 50, metrics of replica A)\n")
        print(
            "| run | outcome | fast share | leader lat mean/p90 ms | tx p50 ms |"
            " equivocated slots: fast / slow / ind-cert / ind-weak / direct-skip / indirect-skip |"
        )
        print("|---|---|---|---|---|---|")
        for row in sorted(equiv_rows, key=lambda r: r["run"]):
            slots = " / ".join(fmt(row[f"equivocator_{t}"]) for t in COMMIT_TYPES)
            print(
                f"| {row['run']} | {row['outcome']} | {fmt(row['fast_share'], 2)} |"
                f" {fmt(row['leader_latency_mean_ms'])}/{fmt(row['leader_latency_p90_ms'])} |"
                f" {fmt(row['tx_latency_p50_ms'])} | {slots} |"
            )
        print()


AWS_SUMMARY = ROOT.parent / "hydrozoan-paper" / "data" / "evaluation" / "aws-summary.csv"
AWS_PROTOCOL = {"dag-hydrangea": "dag-hydrangea", "mysticeti": "mysticeti", "orcaella": "orcaella",
                "blue-bottle-partially-synchronous": "blue-bottle-ps"}


def compare(sim_csv, aws_csv, load=10_000):
    """Join simulator rows to the AWS rows with the same protocol, (f, c, k), n and crash count
    and print e2e / leader p50 side by side with the fast-commit shares."""
    aws = {}
    with open(aws_csv, newline="") as file:
        for row in csv.DictReader(file):
            if int(float(row["load_tx_s"])) != load:
                continue
            key = (
                row["protocol"], row["f"], row["c"], row["k"], int(row["nodes"]),
                int(row["crashes"]),
            )
            aws.setdefault(key, row)
    with open(sim_csv, newline="") as file:
        rows = list(csv.DictReader(file))
    print(
        "| run | e2e p50 sim / AWS | ratio | leader p50 sim / AWS | fast share sim / AWS |"
        " skip share sim / AWS |"
    )
    print("|---|---|---|---|---|---|")
    for row in sorted(rows, key=lambda r: r["run"]):
        with open(Path(sim_csv).parent / row["run"] / "config.yaml") as file:
            protocol = load_yaml(Path(sim_csv).parent / row["run"] / "config.yaml")
        protocol = protocol["replica_parameters"]["consensus"]["protocol"]
        key = (
            AWS_PROTOCOL.get(protocol, protocol), row["f"], row["c"], row["k"], int(row["n"]),
            int(row["crashes"]),
        )
        a = aws.get(key)
        e2e, leader = float(row["tx_latency_p50_ms"]), float(row["leader_latency_p50_ms"])
        if a is None:
            fast = row["fast_share"][:4]
            print(f"| {row['run']} | {e2e:.0f} / - | | {leader:.0f} / - | {fast} / - | |")
            continue
        ae2e, aleader = float(a["e2e_p50_ms"]), float(a["block_p50_ms"])
        decided = max(int(row["decided_slots"]), 1)
        skips = (int(row["direct-skip"]) + int(row["indirect-skip"])) / decided
        askips = float(a["share_direct-skip"]) + float(a["share_indirect-skip"])
        print(
            f"| {row['run']} | {e2e:.0f} / {ae2e:.0f} | {e2e / ae2e:.2f} | {leader:.0f} /"
            f" {aleader:.0f} | {row['fast_share'][:4]} / {a['share_fast-commit'][:4]} |"
            f" {skips:.2f} / {askips:.2f} |"
        )


def plot(csv_path, plots_dir):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plots_dir.mkdir(parents=True, exist_ok=True)
    with open(csv_path, newline="") as file:
        rows = list(csv.DictReader(file))
    crash_rows = [r for r in rows if r["experiment"] == "crash" and r["outcome"] == "pass"]
    series = defaultdict(lambda: defaultdict(list))
    for row in crash_rows:
        series[row["protocol"]][int(row["crashes"])].append(row)

    def curve(protocol, key):
        points = sorted(series[protocol].items())
        xs = [x for x, _ in points]
        ys = [sum(float(r[key]) for r in group) / len(group) for _, group in points]
        return xs, ys

    figures = {
        "sim-crash-leader-latency": ("leader_latency_mean_ms", "Leader latency (mean, ms)"),
        "sim-crash-tx-latency": ("tx_latency_p50_ms", "Transaction latency (p50, ms)"),
        "sim-crash-fast-share": ("fast_share", "Fast-commit share"),
    }
    for stem, (key, label) in figures.items():
        figure, axis = plt.subplots(figsize=(5, 3.2))
        for protocol in series:
            xs, ys = curve(protocol, key)
            axis.plot(xs, ys, marker="o", label=protocol)
            threshold = THRESHOLDS.get(protocol, {}).get("p")
            if threshold is not None and key != "fast_share":
                axis.axvline(threshold, color=axis.lines[-1].get_color(), ls=":", lw=0.8)
        axis.set_xlabel("Crashed validators")
        axis.set_ylabel(label)
        axis.grid(alpha=0.3)
        axis.legend(fontsize=7)
        figure.tight_layout()
        for extension in ("pdf", "png"):
            figure.savefig(plots_dir / f"{stem}.{extension}", dpi=200)
        plt.close(figure)

    # How crashed-leader slots are decided: direct vs indirect skip.
    figure, axis = plt.subplots(figsize=(5, 3.2))
    for protocol in series:
        points = sorted((x, g) for x, g in series[protocol].items() if x > 0)
        xs = [x for x, _ in points]
        share = []
        for _, group in points:
            direct = sum(float(r["crashed_direct-skip"]) for r in group)
            indirect = sum(float(r["crashed_indirect-skip"]) for r in group)
            share.append(direct / (direct + indirect) if direct + indirect else math.nan)
        axis.plot(xs, share, marker="o", label=protocol)
    axis.set_xlabel("Crashed validators")
    axis.set_ylabel("Direct-skip share of crashed slots")
    axis.grid(alpha=0.3)
    axis.legend(fontsize=7)
    figure.tight_layout()
    for extension in ("pdf", "png"):
        figure.savefig(plots_dir / f"sim-crash-skip-type.{extension}", dpi=200)
    plt.close(figure)
    print(f"figures written to {plots_dir}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate", help="write one YAML per run")
    gen.add_argument("--out", type=Path, default=ROOT / "data" / "sim" / "configs")
    gen.add_argument(
        "--group", choices=["validation", "equiv", "heatmap", "sweep"], default="validation"
    )
    gen.add_argument("--seeds", type=int, default=2)
    gen.add_argument(
        "--rtt", type=Path, default=RTT_MATRIX,
        help="per-pair RTT CSV of ping-matrix.sh; 'none' for uniform 50-100 ms links",
    )
    gen.add_argument(
        "--extra-ms", type=float, nargs=2, default=(0.0, 1.0), metavar=("LO", "HI"),
        help="uniform extra added to every one-way delay (processing, jitter)",
    )
    gen.add_argument(
        "--load", type=int, default=LOAD_GENERATOR["load"],
        help="transactions per second per validator",
    )
    par = commands.add_parser("parse", help="summarise run directories into a CSV")
    par.add_argument("results", type=Path)
    par.add_argument("--csv", type=Path, default=None)
    cmp = commands.add_parser("compare", help="simulator summary vs the AWS summary")
    cmp.add_argument("csv", type=Path)
    cmp.add_argument("--aws", type=Path, default=AWS_SUMMARY)
    cmp.add_argument("--load", type=int, default=10_000)
    plo = commands.add_parser("plot", help="draw figures from the CSV")
    plo.add_argument("csv", type=Path)
    plo.add_argument("--out", type=Path, default=ROOT / "plots")
    args = parser.parse_args()

    if args.command == "generate":
        rtt = None if str(args.rtt) == "none" else args.rtt
        generate(args.out, args.group, args.seeds, rtt, tuple(args.extra_ms), args.load)
    elif args.command == "parse":
        parse(args.results, args.csv or args.results / "summary.csv")
    elif args.command == "compare":
        compare(args.csv, args.aws, args.load)
    elif args.command == "plot":
        plot(args.csv, args.out)


if __name__ == "__main__":
    main()
