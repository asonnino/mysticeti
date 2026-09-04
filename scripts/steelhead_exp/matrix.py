# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The experiment matrix: the single source of truth for every simulated run,
its parameters, and its network phases. Analysis iterates this matrix and reads
each job's cached directory; job names are never parsed back."""

from simconfig import (
    Job,
    Phase,
    blue_bottle_async,
    blue_bottle_ps,
    mahi,
    mysticeti,
    run_spec,
    steelhead,
)

COMMITTEES = [10, 50]
PAIRS = ["mm", "bb"]
SEEDS = [0, 1, 2]
# Loads are SYSTEM-wide tx/s (split evenly across replicas): the figures plot
# measured throughput, and per-replica loads would make committee sizes
# incomparable — and n=50 disk-infeasible (each replica's ephemeral WAL holds
# every replica's blocks, so a run transiently writes about
# committee * system_load * tx_size * duration bytes).
LOADS = {
    10: [1_000, 5_000, 10_000, 20_000, 50_000, 100_000],
    50: [1_000, 5_000, 10_000, 20_000],
}
REFERENCE_LOAD = {10: 20_000, 50: 20_000}
TIMELINE_LOAD = 10_000
STATIC_PERIODS = [2, 4, 8, 16]
ADAPTIVE = {"interval": 32, "max_period": 8, "epsilon_percent": 10}
ATTACK_DELAY_MS = 2_000

PAIR_TAG = {"mm": "mysticeti-mahi-mahi", "bb": "blue-bottle"}


def protocols(pair):
    """The healthy-grid protocol set for one pair: slug -> consensus dict."""
    tag = PAIR_TAG[pair]
    if pair == "mm":
        grid = {"myst": mysticeti(), "mahi4": mahi(4), "mahi5": mahi(5)}
    else:
        grid = {"bbps": blue_bottle_ps(), "bbasync": blue_bottle_async()}
    for period in STATIC_PERIODS:
        grid[f"sh-p{period}"] = steelhead(tag, period=period)
    grid["sh-ada"] = steelhead(tag, adaptive=ADAPTIVE)
    return grid


def timeline_protocols(pair):
    """The timeline comparison set: base sync, base async, static and adaptive
    Steelhead."""
    grid = protocols(pair)
    keep = ["myst", "mahi5"] if pair == "mm" else ["bbps", "bbasync"]
    return {slug: grid[slug] for slug in keep + ["sh-p4", "sh-ada"]}


def attack_conditions(start_s, end_s):
    return [
        {"from_secs": start_s,
            "model": {"kind": "targeted-leader-delay", "delay_ms": ATTACK_DELAY_MS}},
        {"from_secs": end_s},
    ]


def attack_phases(start_s, end_s, duration_s):
    return [
        Phase("healthy", 0, start_s),
        Phase("attack", start_s, end_s),
        Phase("healthy", end_s, duration_s),
    ]


def good_jobs():
    jobs = []
    for committee in COMMITTEES:
        for pair in PAIRS:
            for slug, consensus in protocols(pair).items():
                for load in LOADS[committee]:
                    for seed in SEEDS:
                        jobs.append(Job(
                            name=f"good--n{committee}--{pair}--{slug}--L{load}--s{seed}",
                            campaign="good",
                            params=dict(committee=committee, pair=pair, proto=slug,
                                        load=load, seed=seed),
                            spec=run_spec(committee, seed, 60, load, consensus),
                        ))
    return jobs


def attack_jobs():
    jobs = []
    for committee in COMMITTEES:
        for pair in PAIRS:
            for slug, consensus in timeline_protocols(pair).items():
                for seed in SEEDS:
                    jobs.append(Job(
                        name=f"attack--n{committee}--{pair}--{slug}--L{TIMELINE_LOAD}--s{seed}",
                        campaign="attack",
                        params=dict(committee=committee, pair=pair, proto=slug,
                                    load=TIMELINE_LOAD, seed=seed),
                        spec=run_spec(committee, seed, 90, TIMELINE_LOAD, consensus,
                                        conditions=attack_conditions(30, 60),
                                        sample_interval_secs=2),
                        phases=attack_phases(30, 60, 90),
                    ))
    return jobs


def sched_jobs():
    """Scheduled-asynchrony variant of the attack timeline (claim:async's
    'identical relative ordering'); data collected, plotting optional."""
    conditions = [
        {"from_secs": 30, "model": {"kind": "scheduled-asynchrony", "burst_ms": 1_000}},
        {"from_secs": 60},
    ]
    jobs = []
    for pair in PAIRS:
        for slug, consensus in timeline_protocols(pair).items():
            for seed in SEEDS:
                jobs.append(Job(
                    name=f"sched--n10--{pair}--{slug}--L{TIMELINE_LOAD}--s{seed}",
                    campaign="sched",
                    params=dict(committee=10, pair=pair, proto=slug,
                                load=TIMELINE_LOAD, seed=seed),
                    spec=run_spec(10, seed, 90, TIMELINE_LOAD, consensus,
                                    conditions=conditions, sample_interval_secs=2),
                    phases=attack_phases(30, 60, 90),
                ))
    return jobs


def adaptive_jobs():
    """Adaptive timeline plus the static references used for the overhead
    fillin; adaptive runs log at debug so tracing.log carries replay scores."""
    jobs = []
    for committee in COMMITTEES:
        for pair in PAIRS:
            tag = PAIR_TAG[pair]
            grid = {
                "sh-ada": steelhead(tag, adaptive=ADAPTIVE),
                "sh-p16": steelhead(tag, period=16),
                "sh-p1": steelhead(tag, period=1),
            }
            for slug, consensus in grid.items():
                for seed in SEEDS:
                    jobs.append(Job(
                        name=f"adaptive--n{committee}--{pair}--{slug}--L{TIMELINE_LOAD}--s{seed}",
                        campaign="adaptive",
                        params=dict(committee=committee, pair=pair, proto=slug,
                                    load=TIMELINE_LOAD, seed=seed),
                        spec=run_spec(committee, seed, 150, TIMELINE_LOAD, consensus,
                                        conditions=attack_conditions(30, 100),
                                        sample_interval_secs=2),
                        phases=attack_phases(30, 100, 150),
                        debug_log=(slug == "sh-ada"),
                    ))
    return jobs


def async_jobs():
    """Appendix: random asynchrony with f crashed — canary on/off vs the Mahi
    baseline."""
    tag = PAIR_TAG["mm"]
    grid = {
        "sh-p1-canary": steelhead(tag, period=1),
        "sh-p1-nocanary": steelhead(tag, period=1, canary=None),
        "mahi5": mahi(5),
    }
    conditions = [{
        "from_secs": 0,
        "model": {"kind": "random-link-delay", "percent": 30,
                    "delay_min_ms": 100, "delay_max_ms": 400},
    }]
    crashes = [{"replica": replica, "at_secs": 1} for replica in (7, 8, 9)]
    jobs = []
    for slug, consensus in grid.items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"async--n10--mm--{slug}--L{TIMELINE_LOAD}--s{seed}",
                campaign="async",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 60, TIMELINE_LOAD, consensus,
                                conditions=conditions, crashes=crashes,
                                sample_interval_secs=2),
            ))
    return jobs


def smoke_jobs():
    """Tiny end-to-end checks: one healthy point, one timeline shape."""
    return [
        Job(
            name="smoke--n10--mm--myst--L100--s0",
            campaign="smoke",
            params=dict(committee=10, pair="mm", proto="myst", load=100, seed=0),
            spec=run_spec(10, 0, 20, 100, mysticeti()),
        ),
        Job(
            name="smoke--n10--mm--sh-p4--L100--s0",
            campaign="smoke",
            params=dict(committee=10, pair="mm", proto="sh-p4", load=100, seed=0),
            spec=run_spec(10, 0, 30, 100, steelhead(PAIR_TAG["mm"], period=4),
                            conditions=attack_conditions(10, 20), sample_interval_secs=2),
            phases=attack_phases(10, 20, 30),
        ),
    ]


def all_jobs():
    jobs = (smoke_jobs() + good_jobs() + attack_jobs() + sched_jobs()
            + adaptive_jobs() + async_jobs())
    names = [job.name for job in jobs]
    assert len(names) == len(set(names)), "job names must be unique"
    return jobs
