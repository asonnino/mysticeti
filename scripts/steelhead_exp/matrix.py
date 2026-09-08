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
# Real-deployment leader timeout for every timeline campaign.
LEADER_TIMEOUT_MS = 100
REFERENCE_LOAD = {10: 20_000, 50: 20_000}
TIMELINE_LOAD = 1_000
STATIC_PERIODS = [2, 4, 8, 16]
# The derived default: pick the interval (reaction horizon); max_period and
# canary follow their laws (largest power of two <= interval/2; largest odd
# <= interval/4).
# The headline default: ~1% green overhead, ~0 red, graceful ~2min recovery
# from a fully asynchronous period (needs retention >= interval).
ADAPTIVE = {"interval": 128, "max_period": 64, "epsilon_percent": 10}
# Just past the leader timeout + grace + slack: enough to defeat every
# sync wait without a large constant.
ATTACK_DELAY_MS = 300

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
    grid["sh-ada"] = steelhead(tag, adaptive=ADAPTIVE, canary=5)
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
                                        sample_interval_secs=2,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
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
                                    conditions=conditions, sample_interval_secs=2,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
                    phases=attack_phases(30, 60, 90),
                ))
    return jobs


def adaptive_jobs():
    """The phase timeline: adaptive Steelhead vs the pure sync and async
    baselines across good -> bad -> good, plus the static references for the
    overhead fillin. The attack must outlast the descent (interval rounds at
    the crawling leader cap), hence the long degraded window. Adaptive runs
    log at debug so tracing.log carries replay scores."""
    jobs = []
    for committee in COMMITTEES:
        for pair in PAIRS:
            tag = PAIR_TAG[pair]
            grid = {
                "sh-ada": steelhead(tag, adaptive=ADAPTIVE, canary=31),
                "sh-p16": steelhead(tag, period=16),
                "sh-p1": steelhead(tag, period=1),
            }
            if pair == "mm":
                grid["myst"] = mysticeti()
                grid["mahi5"] = mahi(5)
            else:
                grid["bbps"] = blue_bottle_ps()
                grid["bbasync"] = blue_bottle_async()
            for slug, consensus in grid.items():
                for seed in SEEDS:
                    jobs.append(Job(
                        name=f"adaptive--n{committee}--{pair}--{slug}--L{TIMELINE_LOAD}--s{seed}",
                        campaign="adaptive",
                        params=dict(committee=committee, pair=pair, proto=slug,
                                    load=TIMELINE_LOAD, seed=seed),
                        spec=run_spec(committee, seed, 450, TIMELINE_LOAD, consensus,
                                        conditions=[{"from_secs": 30, "model": {
                                            "kind": "random-link-delay", "percent": 100,
                                            "delay_min_ms": 400, "delay_max_ms": 400}},
                                            {"from_secs": 330}],
                                        sample_interval_secs=5,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
                        phases=attack_phases(30, 330, 450),
                        debug_log=(slug == "sh-ada"),
                    ))
    return jobs


PROFILES = {
    "sh-sync": {"interval": 64, "max_period": 32, "epsilon_percent": 10, "canary": 3},
    "sh-bal": {"interval": 32, "max_period": 16, "epsilon_percent": 10, "canary": 5},
    "sh-tumult": {"interval": 16, "max_period": 4, "epsilon_percent": 10, "canary": 3},
}


def profile_grid():
    grid = {}
    for slug, config in PROFILES.items():
        adaptive = {key: config[key] for key in ("interval", "max_period", "epsilon_percent")}
        grid[slug] = steelhead(PAIR_TAG["mm"], adaptive=adaptive, canary=config["canary"])
    grid["myst"] = mysticeti()
    grid["mahi5"] = mahi(5)
    return grid


def profile_jobs():
    """The parameter-space profiles under the single-attack timeline."""
    jobs = []
    for slug, consensus in profile_grid().items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"profiles--n10--mm--{slug}--L{TIMELINE_LOAD}--s{seed}",
                campaign="profiles",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 210, TIMELINE_LOAD, consensus,
                                conditions=attack_conditions(30, 150),
                                sample_interval_secs=2,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=attack_phases(30, 150, 210),
            ))
    return jobs


STORM_CYCLE_S = 15


def storm_jobs():
    """The same profiles under a tumultuous network: alternating 30s
    good/bad phases."""
    duration = 210
    conditions = []
    phases = [Phase("healthy", 0, STORM_CYCLE_S)]
    start = STORM_CYCLE_S
    while start + STORM_CYCLE_S <= duration - STORM_CYCLE_S:
        conditions.append({
            "from_secs": start,
            "model": {"kind": "targeted-leader-delay", "delay_ms": ATTACK_DELAY_MS},
        })
        conditions.append({"from_secs": start + STORM_CYCLE_S})
        phases.append(Phase("attack", start, start + STORM_CYCLE_S))
        phases.append(Phase("healthy", start + STORM_CYCLE_S, start + 2 * STORM_CYCLE_S))
        start += 2 * STORM_CYCLE_S
    jobs = []
    for slug, consensus in profile_grid().items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"storm--n10--mm--{slug}--L{TIMELINE_LOAD}--s{seed}",
                campaign="storm",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, duration, TIMELINE_LOAD, consensus,
                                conditions=conditions, sample_interval_secs=2,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=phases,
            ))
    return jobs


# Interval 96 gives every canary >= 3 probes/window, so failures isolate
# alignment (canary a multiple of max_period -> its probes all land on the
# top candidate's async slots) from probe scarcity. 15/31 are odd (coprime
# to the power-of-two candidates); 16/32 are aligned to max_period 16.
CANARY_SWEEP = [15, 16, 31, 32]
CANARY_INTERVAL = 96


def canary_jobs():
    """Fixed window (interval 96, max 16); vary only the canary spacing to
    separate red-premium, probe scarcity, and alignment."""
    adaptive = {"interval": CANARY_INTERVAL, "max_period": 16, "epsilon_percent": 10}
    jobs = []
    for canary in CANARY_SWEEP:
        consensus = steelhead(PAIR_TAG["mm"], adaptive=adaptive, canary=canary)
        for seed in SEEDS:
            jobs.append(Job(
                name=f"canary--n10--mm--c{canary}--L{TIMELINE_LOAD}--s{seed}",
                campaign="canary",
                params=dict(committee=10, pair="mm", proto=f"c{canary}",
                            canary=canary, load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 210, TIMELINE_LOAD, consensus,
                                conditions=attack_conditions(30, 150),
                                sample_interval_secs=2,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=attack_phases(30, 150, 210),
            ))
    return jobs


# Fixed green premium (max_period 8); interval is the transition dial, with
# canary = largest odd <= interval/4 (coprime to the power-of-two candidates).
INTERVAL_SWEEP = [(16, 3), (32, 7), (64, 15)]


def interval_jobs():
    grid = {"myst": mysticeti(), "mahi5": mahi(5)}
    for interval, canary in INTERVAL_SWEEP:
        adaptive = {"interval": interval, "max_period": 8, "epsilon_percent": 10}
        grid[f"i{interval}"] = steelhead(PAIR_TAG["mm"], adaptive=adaptive, canary=canary)
    jobs = []
    for slug, consensus in grid.items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"interval--n10--mm--{slug}--L{TIMELINE_LOAD}--s{seed}",
                campaign="interval",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 210, TIMELINE_LOAD, consensus,
                                conditions=attack_conditions(30, 150),
                                sample_interval_secs=2,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=attack_phases(30, 150, 210),
            ))
    return jobs


# One-knob-at-a-time ablations; every config runs against shared baselines.
ABLATION = (
    [(i, 8, 3) for i in (16, 32, 64, 96)]              # interval sweep
    + [(64, m, 3) for m in (4, 16, 32)]                # max_period sweep (64,8,3 shared)
    + [(64, 8, c) for c in (5, 7, 11, 15)]             # canary sweep
    + [(96, 8, 23)]                                    # today's ceiling
)


def ablation_jobs():
    grid = {"myst": mysticeti(), "mahi5": mahi(5)}
    for interval, max_period, canary in ABLATION:
        adaptive = {"interval": interval, "max_period": max_period, "epsilon_percent": 10}
        slug = f"i{interval}-m{max_period}-c{canary}"
        grid[slug] = steelhead(PAIR_TAG["mm"], adaptive=adaptive, canary=canary)
    jobs = []
    for slug, consensus in grid.items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"abl--n10--mm--{slug}--L{TIMELINE_LOAD}--s{seed}",
                campaign="abl",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 210, TIMELINE_LOAD, consensus,
                                conditions=attack_conditions(30, 150),
                                sample_interval_secs=2,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=attack_phases(30, 150, 210),
            ))
    return jobs


# Red-zone network models ("internet weather"), all on the standard 210s
# timeline. Every model applies identically to all protocols.
# The weather panels, each isolating one condition (crash is added separately
# in weather_jobs). The leader timeout is LEADER_TIMEOUT_MS = 100.
WEATHER = {
    # Sub-threshold fluctuation: leader delay below the timeout; sync survives,
    # so Steelhead must not switch (false-alarm robustness).
    "subthresh": {"kind": "targeted-leader-delay", "delay_ms": 30},
    # Targeted leader delay: the round-robin leader delayed past the timeout;
    # sync loses liveness and Steelhead switches to the asynchronous rule.
    "targeted": {"kind": "targeted-leader-delay", "delay_ms": 125},
    # Partial random network: a fraction of links delayed past the timeout
    # (the DISC'25 random model at f/n asynchronous participation).
    "partial": {"kind": "random-link-delay", "percent": 30,
                "delay_min_ms": 100, "delay_max_ms": 150},
    # Full random network: every link delayed past the timeout (pure DISC'25;
    # the timeout is of no help).
    "full": {"kind": "random-link-delay", "percent": 100,
                "delay_min_ms": 100, "delay_max_ms": 150},
    # High jitter: an unstable network -- every message suffers an
    # exponentially distributed delay (heavy tail), most small, a few large.
    "jitter": {"kind": "exponential-jitter", "percent": 100,
                "base_ms": 0, "mean_ms": 75, "cap_ms": 400},
}
# (a structured-partial model that could park at an intermediate period is a
# candidate follow-up; the delay models here are all uniform-severity.)


WEATHER_SEEDS = list(range(7))


def weather_grid(pair):
    """Per-pair protocol set: the two pure baselines plus adaptive Steelhead."""
    tag = PAIR_TAG[pair]
    if pair == "mm":
        grid = {"myst": mysticeti(), "mahi5": mahi(5)}
    else:
        grid = {"bbps": blue_bottle_ps(), "bbasync": blue_bottle_async()}
    grid["sh-ada"] = steelhead(tag, adaptive=ADAPTIVE, canary=31)
    return grid


def weather_jobs():
    jobs = []
    for committee in COMMITTEES:
        for pair in PAIRS:
            # Crash faults are permanent (replicas die at 30s, no recovery), up
            # to each family's fault tolerance: f = (n-1)/3 for the 3f+1 pair,
            # (n-1)/5 for the 5f+1 pair; the last f replicas are crashed.
            f = (committee - 1) // 3 if pair == "mm" else (committee - 1) // 5
            crashed = tuple(range(committee - f, committee))
            # n=50 runs a shorter timeline: its sims are ~25x heavier (the
            # discrete-event cost scales with n^2), so the full n=10 timeline is
            # impractical; the transitions still show clearly at 210s.
            start, cond_end, dur = (30, 330, 450) if committee == 10 else (30, 150, 210)
            for proto_slug, consensus in weather_grid(pair).items():
                for model_slug, model in WEATHER.items():
                    conditions = [{"from_secs": start, "model": model},
                                    {"from_secs": cond_end}]
                    for seed in WEATHER_SEEDS:
                        jobs.append(Job(
                            name=f"weather--n{committee}--{pair}--{model_slug}"
                                f"--{proto_slug}--s{seed}",
                            campaign="weather",
                            params=dict(committee=committee, pair=pair, proto=proto_slug,
                                        model=model_slug, load=TIMELINE_LOAD, seed=seed),
                            spec=run_spec(committee, seed, dur, TIMELINE_LOAD, consensus,
                                            conditions=conditions, sample_interval_secs=5,
                                            leader_timeout_ms=LEADER_TIMEOUT_MS),
                            phases=attack_phases(start, cond_end, dur),
                        ))
                for seed in WEATHER_SEEDS:
                    jobs.append(Job(
                        name=f"weather--n{committee}--{pair}--crash--{proto_slug}--s{seed}",
                        campaign="weather",
                        params=dict(committee=committee, pair=pair, proto=proto_slug,
                                    model="crash", load=TIMELINE_LOAD, seed=seed),
                        spec=run_spec(committee, seed, dur, TIMELINE_LOAD, consensus,
                                        crashes=[{"replica": r, "at_secs": 30} for r in crashed],
                                        sample_interval_secs=5,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
                        phases=[Phase("healthy", 0, start), Phase("attack", start, dur)],
                    ))
    return jobs


# Feasibility diagnostic: the two heavy weather panes (full, partial) at n=50 on
# a short 60s timeline, so the runs actually complete and reveal whether
# Steelhead adapts under sustained delay (few no-commit ticks) rather than merely
# being too slow to simulate at 210s. Finer sampling and debug logs on sh-ada.
DIAG_MODELS = {"full": WEATHER["full"], "partial": WEATHER["partial"]}
DIAG_SEEDS = list(range(3))


def diag_jobs():
    jobs = []
    committee, pair = 50, "mm"
    start, cond_end, dur = 10, 40, 60
    for proto_slug, consensus in weather_grid(pair).items():
        for model_slug, model in DIAG_MODELS.items():
            conditions = [{"from_secs": start, "model": model},
                            {"from_secs": cond_end}]
            for seed in DIAG_SEEDS:
                jobs.append(Job(
                    name=f"diag--n{committee}--{pair}--{model_slug}--{proto_slug}--s{seed}",
                    campaign="diag",
                    params=dict(committee=committee, pair=pair, proto=proto_slug,
                                model=model_slug, load=TIMELINE_LOAD, seed=seed),
                    spec=run_spec(committee, seed, dur, TIMELINE_LOAD, consensus,
                                    conditions=conditions, sample_interval_secs=2,
                                    leader_timeout_ms=LEADER_TIMEOUT_MS),
                    phases=attack_phases(start, cond_end, dur),
                    debug_log=(proto_slug == "sh-ada"),
                ))
    return jobs


# Recovery stress test: a ~1% steady-state overhead config (max 64) under a
# sustained scheduled-asynchrony period, long enough for s->a to complete and
# a->s afterward. Needs retention > interval (bumped to 512).
def recovery_jobs():
    adaptive = {"interval": 128, "max_period": 64, "epsilon_percent": 10}
    consensus = steelhead(PAIR_TAG["mm"], adaptive=adaptive, canary=31)
    grid = {"sh-1pct": consensus, "mahi5": mahi(5), "myst": mysticeti()}
    conditions = [
        {"from_secs": 30, "model": {"kind": "random-link-delay", "percent": 100,
                                    "delay_min_ms": 400, "delay_max_ms": 400}},
        {"from_secs": 330},
    ]
    jobs = []
    for slug, c in grid.items():
        for seed in SEEDS:
            jobs.append(Job(
                name=f"recovery--n10--mm--{slug}--s{seed}",
                campaign="recovery",
                params=dict(committee=10, pair="mm", proto=slug,
                            load=TIMELINE_LOAD, seed=seed),
                spec=run_spec(10, seed, 450, TIMELINE_LOAD, c,
                                conditions=conditions, sample_interval_secs=5,
                                leader_timeout_ms=LEADER_TIMEOUT_MS),
                phases=[Phase("healthy", 0, 30), Phase("attack", 30, 330),
                        Phase("healthy", 330, 450)],
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
                                sample_interval_secs=2,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
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
                            conditions=attack_conditions(10, 20), sample_interval_secs=2,
                                        leader_timeout_ms=LEADER_TIMEOUT_MS),
            phases=attack_phases(10, 20, 30),
        ),
    ]


# Appendix D (commit probability under a mistimed leader timeout): the two
# sync rules alone, condition held from 30s to the end of a 210s run.
APPENDIX_DURATION_S = 210


def _appendix_job(campaign, slug, consensus, model_slug, model, seed, committee=10):
    return Job(
        name=f"{campaign}--n{committee}--{model_slug}--{slug}--s{seed}",
        campaign=campaign,
        params=dict(committee=committee, pair="mm" if slug == "myst" else "bb",
                    proto=slug, model=model_slug, load=TIMELINE_LOAD, seed=seed),
        spec=run_spec(committee, seed, APPENDIX_DURATION_S, TIMELINE_LOAD, consensus,
                        conditions=[{"from_secs": 30, "model": model}],
                        sample_interval_secs=5, leader_timeout_ms=LEADER_TIMEOUT_MS),
        phases=[Phase("healthy", 0, 30), Phase("attack", 30, APPENDIX_DURATION_S)],
    )


SYNC_RULES = {"myst": mysticeti, "bbps": blue_bottle_ps}

# Reference truncation: every link delayed by a fixed 800ms, far past the
# 100ms leader timeout, so every proposer references exactly n-f blocks.
TRUNC_DELAY_MS = 800
TRUNC_COMMITTEES = [10, 50]


def trunc_jobs():
    model = {"kind": "random-link-delay", "percent": 100,
                "delay_min_ms": TRUNC_DELAY_MS, "delay_max_ms": TRUNC_DELAY_MS}
    return [
        _appendix_job("trunc", slug, make(), f"d{TRUNC_DELAY_MS}", model, seed, committee)
        for committee in TRUNC_COMMITTEES
        for slug, make in SYNC_RULES.items()
        for seed in SEEDS
    ]


# Partial asynchrony: a fraction phi of links delayed past the timeout, the
# weather figure's ]100, 150]ms range; the grid is dense around f/n (0.1 for
# the 5f+1 pair, 0.3 for the 3f+1 pair). n=10 only: a stalled Mysticeti run
# at n=50 takes over an hour of wall time.
PHI_PERCENTS = [5, 10, 15, 20, 25, 30, 40, 50, 75, 100]
PHI_COMMITTEES = [10]


def phi_jobs():
    jobs = []
    for committee in PHI_COMMITTEES:
        for percent in PHI_PERCENTS:
            model = {"kind": "random-link-delay", "percent": percent,
                        "delay_min_ms": 100, "delay_max_ms": 150}
            for slug, make in SYNC_RULES.items():
                for seed in SEEDS:
                    jobs.append(_appendix_job("phi", slug, make(), f"p{percent}", model,
                            seed, committee))
    return jobs


def all_jobs():
    jobs = (smoke_jobs() + good_jobs() + attack_jobs() + sched_jobs()
            + adaptive_jobs() + async_jobs() + profile_jobs() + storm_jobs()
            + canary_jobs() + interval_jobs() + ablation_jobs() + weather_jobs() + recovery_jobs()
            + trunc_jobs() + phi_jobs() + diag_jobs())
    names = [job.name for job in jobs]
    assert len(names) == len(set(names)), "job names must be unique"
    return jobs
