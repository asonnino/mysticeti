# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Job model and SimulationConfig construction for the experiment matrix."""

from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "steelhead"
PLOTS_DIR = ROOT / "plots"
BINARY = ROOT / "target" / "release" / "replica"

LATENCY_MIN_MS = 50
LATENCY_MAX_MS = 100
TRANSACTION_SIZE = 512


@dataclass(frozen=True)
class Phase:
    """A timed network phase, for timeline shading and per-phase analysis."""

    label: str
    start_s: int
    end_s: int


@dataclass
class Job:
    """One simulation run: its name, config mapping, and analysis metadata."""

    name: str
    campaign: str
    params: dict
    spec: dict
    phases: list = field(default_factory=list)
    debug_log: bool = False

    @property
    def out_dir(self) -> Path:
        return DATA_DIR / self.campaign / self.name

    @property
    def disk_weight(self) -> int:
        """Approximate transient disk (bytes) of the run's ephemeral WALs:
        every replica stores every replica's blocks."""
        generator = self.spec.get("load_generator", {})
        system_load = generator.get("load", 0) * self.spec["committee_size"]
        return (self.spec["committee_size"] * system_load
                * generator.get("transaction_size", 0) * self.spec["duration_secs"])


def mysticeti(leader_count=1):
    return {"protocol": "mysticeti", "leader_count": leader_count}


def mahi(wave_length, leader_count=1):
    return {"protocol": "mahi-mahi", "wave_length": wave_length, "leader_count": leader_count}


def blue_bottle_ps(leader_count=1):
    return {"protocol": "blue-bottle-partially-synchronous", "leader_count": leader_count}


def blue_bottle_async(leader_count=1):
    return {"protocol": "blue-bottle-asynchronous", "leader_count": leader_count}


def steelhead(pair, period=None, adaptive=None, canary=1, leader_count=1):
    """Steelhead config; `canary=1` (the default) is omitted, `canary=None`
    emits an explicit null (pure quorum pacing on async rounds)."""
    consensus = {
        "protocol": "steelhead",
        "pair": pair,
        "async_wave_length": 5 if pair == "mysticeti-mahi-mahi" else 3,
        "leader_count": leader_count,
    }
    if adaptive is not None:
        consensus["adaptive"] = dict(adaptive)
    else:
        consensus["period"] = period
    if canary != 1:
        consensus["canary"] = canary
    return consensus


def duration_ms(milliseconds):
    """serde-friendly std::time::Duration mapping."""
    return {"secs": milliseconds // 1000, "nanos": (milliseconds % 1000) * 1_000_000}


def run_spec(
    committee_size,
    seed,
    duration_secs,
    load,
    consensus,
    conditions=None,
    crashes=None,
    sample_interval_secs=None,
    leader_timeout_ms=None,
):
    """One mapping-form SimulationConfig (never a suite list: one config per
    simulator invocation, so runs parallelize and never share a directory).
    `load` is the SYSTEM tx/s, split evenly across the replicas' generators."""
    spec = {
        "committee_size": committee_size,
        "latency_min_ms": LATENCY_MIN_MS,
        "latency_max_ms": LATENCY_MAX_MS,
        "duration_secs": duration_secs,
        "rng_seed": seed,
        "replica_parameters": {"consensus": consensus},
        "load_generator": {
            "load": max(load // committee_size, 1),
            "transaction_size": TRANSACTION_SIZE,
            "initial_delay": "0s",
        },
    }
    if leader_timeout_ms is not None:
        # A bare round_timeout override applies to both caps; pin the quorum
        # cap at its default explicitly.
        spec["replica_parameters"]["dag"] = {
            "round_timeout": duration_ms(leader_timeout_ms),
            "quorum_round_timeout": duration_ms(75),
        }
    if conditions:
        spec["conditions"] = conditions
    if crashes:
        spec["crashes"] = crashes
    if sample_interval_secs is not None:
        spec["sample_interval_secs"] = sample_interval_secs
    return spec
