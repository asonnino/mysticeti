# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Per-run summaries from metrics-*.prom, aggregated with the repo's
conventions (crates/dag/src/metrics/aggregate.rs): a percentile is the mean
across replicas of each replica's percentile, dropping empty replicas; TPS is
the mean across replicas of count/duration."""

import math
from dataclasses import dataclass

import numpy as np
import yaml

import prom

COMMIT_TYPES = ["direct-commit", "indirect-commit", "direct-skip", "indirect-skip"]


def percentile_from_buckets(buckets, total, p):
    """Linear interpolation between bucket upper bounds; the +Inf terminal
    falls back to the previous finite bound. Mirrors the Rust implementation."""
    if total == 0 or not buckets:
        return None
    target = min(max(p, 0.0), 1.0) * total
    prev_bound, prev_count, last_finite = 0.0, 0, 0.0
    for upper, count in buckets:
        if count >= target:
            high = upper if math.isfinite(upper) else last_finite
            if count == prev_count:
                return prev_bound
            fraction = (target - prev_count) / (count - prev_count)
            return prev_bound + fraction * (high - prev_bound)
        if math.isfinite(upper):
            last_finite = upper
            prev_bound = upper
        prev_count = count
    return None


@dataclass
class ReplicaSummary:
    count: int
    latency_percentile_s: dict
    latency_mean_s: float
    latency_std_s: float
    tps: float
    commit_types: dict
    leader_timeouts: float


def replica_summary(prom_path, duration_s, percentiles=(0.5, 0.9, 0.99)):
    series = prom.parse(prom_path)
    buckets = sorted(
        (float(labels["le"]), int(value))
        for labels, value in series.get("latency_s_bucket", [])
    )
    count = int(prom.scalar(series, "latency_s_count"))
    total = prom.scalar(series, "latency_s_sum")
    squared = prom.scalar(series, "latency_squared_s")
    mean = total / count if count else 0.0
    variance = max(squared / count - mean * mean, 0.0) if count else 0.0
    commit_types = {kind: 0.0 for kind in COMMIT_TYPES}
    for labels, value in series.get("committed_leaders_total", []):
        kind = labels.get("commit_type")
        if kind in commit_types:
            commit_types[kind] += value
    return ReplicaSummary(
        count=count,
        latency_percentile_s={
            p: percentile_from_buckets(buckets, count, p) for p in percentiles
        },
        latency_mean_s=mean,
        latency_std_s=math.sqrt(variance),
        tps=count / duration_s if duration_s else 0.0,
        commit_types=commit_types,
        leader_timeouts=prom.scalar(series, "leader_timeout_total"),
    )


@dataclass
class RunSummary:
    outcome: str
    duration_s: float
    latency_percentile_s: dict
    latency_mean_s: float
    tps: float
    commit_types: dict
    leader_timeouts: float


def run_summary(run_dir):
    """Aggregate one run directory; None when the run is absent or non-pass."""
    meta_path = run_dir / "meta.yaml"
    if not meta_path.exists():
        return None
    meta = yaml.safe_load(meta_path.read_text())
    if meta.get("outcome") != "pass":
        return None
    duration_s = float(meta["duration_secs"])
    replicas = [
        replica_summary(path, duration_s)
        for path in sorted(run_dir.glob("metrics-*.prom"))
    ]
    committing = [replica for replica in replicas if replica.count > 0]
    percentiles = {}
    for p in (0.5, 0.9, 0.99):
        values = [r.latency_percentile_s[p] for r in committing
                    if r.latency_percentile_s[p] is not None]
        percentiles[p] = float(np.mean(values)) if values else None
    return RunSummary(
        outcome=meta["outcome"],
        duration_s=duration_s,
        latency_percentile_s=percentiles,
        latency_mean_s=float(np.mean([r.latency_mean_s for r in committing]))
        if committing else 0.0,
        tps=float(np.mean([r.tps for r in replicas])),
        commit_types={
            kind: float(np.sum([r.commit_types[kind] for r in replicas]))
            for kind in COMMIT_TYPES
        },
        leader_timeouts=float(np.sum([r.leader_timeouts for r in replicas])),
    )


def seed_stats(values):
    """(mean, std) across seeds; std is 0 for fewer than two samples."""
    values = [v for v in values if v is not None]
    if not values:
        return None, None
    mean = float(np.mean(values))
    std = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
    return mean, std
