# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The lean-dag SoK simulation matrix: one simulator config per job."""

from dataclasses import dataclass

COMMITTEE_SIZE = 46
DURATION_SECS = 120
SAMPLE_INTERVAL_SECS = 5
SEEDS = range(7)
# The load generator sends ceil(load / 10) transactions every 100 ms, so 22 means 30 tx/s per
# replica: about 1,380 tx/s system-wide.
LOAD_PER_REPLICA = 22
TRANSACTION_SIZE = 20
# Link delays and timeouts are 1.6 times steelhead's (25-50 ms links), so latencies land near
# those of geo-distributed deployments (Mysticeti at about 330 ms).
LATENCY_MS = {"start": 40, "end": 80}
JITTER = {"mean_ms": 120, "cap_ms": 640}
PARTIALLY_SYNCHRONOUS_TIMEOUT_MS = 160
ASYNCHRONOUS_TIMEOUT_MS = 64


@dataclass(frozen=True)
class Protocol:
    """One protocol configuration and the faults it tolerates."""

    label: str
    consensus: dict
    asynchronous: bool
    byzantine: int  # f
    crashes: int  # crashes tolerated: f + c, or c for crash-only protocols

    @property
    def timeout_ms(self):
        if self.asynchronous:
            return ASYNCHRONOUS_TIMEOUT_MS
        return PARTIALLY_SYNCHRONOUS_TIMEOUT_MS


def consensus(name, **fields):
    return {"protocol": name, "leader_count": 1, **fields}


ORCAELLA_POINTS = {"byz": (7, 3), "bal": (6, 5), "crash": (2, 11)}
HYBRID_POINTS = {"byz": (13, 3), "bal": (9, 9), "crash": (2, 19)}


def orcaella(label, f, c, wave_length=None):
    if wave_length is None:
        return Protocol(f"orcaella-{label}", consensus("orcaella", f=f, c=c), False, f, f + c)
    fields = {"f": f, "c": c, "wave_length": wave_length}
    return Protocol(
        f"orcaella-async-w{wave_length}-{label}",
        consensus("orcaella-asynchronous", **fields),
        True,
        f,
        f + c,
    )


def hybrid(label, f, c, wave_length=None):
    if wave_length is None:
        protocol = consensus("hybrid-mysticeti", f=f, c=c)
        return Protocol(f"hybrid-mysticeti-{label}", protocol, False, f, f + c)
    protocol = consensus("hybrid-mahi-mahi", f=f, c=c, wave_length=wave_length)
    return Protocol(f"hybrid-mahi-mahi-w{wave_length}-{label}", protocol, True, f, f + c)


def mahi_mahi(wave_length):
    protocol = consensus("mahi-mahi", wave_length=wave_length)
    return Protocol(f"mahi-mahi-w{wave_length}", protocol, True, 15, 15)


def nemo_nemo_asynchronous(wave_length):
    protocol = consensus("nemo-nemo-asynchronous", wave_length=wave_length)
    return Protocol(f"nemo-nemo-async-w{wave_length}", protocol, True, 0, 22)


MAIN = [
    Protocol("cm-ps", consensus("cordial-miners-partially-synchronous"), False, 15, 15),
    Protocol("cm-async", consensus("cordial-miners-asynchronous"), True, 15, 15),
    Protocol("mysticeti", consensus("mysticeti"), False, 15, 15),
    mahi_mahi(5),
    Protocol("bb-ps", consensus("blue-bottle-partially-synchronous"), False, 9, 9),
    Protocol("bb-async", consensus("blue-bottle-asynchronous"), True, 9, 9),
    Protocol("nemo-nemo", consensus("nemo-nemo"), False, 0, 22),
    nemo_nemo_asynchronous(3),
    *(orcaella(label, f, c) for label, (f, c) in ORCAELLA_POINTS.items()),
    *(orcaella(label, f, c, 3) for label, (f, c) in ORCAELLA_POINTS.items()),
    *(hybrid(label, f, c) for label, (f, c) in HYBRID_POINTS.items()),
    *(hybrid(label, f, c, 5) for label, (f, c) in HYBRID_POINTS.items()),
]

# c = 0 configurations with exactly the parameters of a protocol in MAIN, paired with its label:
# at the same seed, both must commit exactly the same.
CONSISTENCY_PAIRS = [
    (orcaella("c0", 9, 0), "bb-ps"),
    (orcaella("c0", 9, 0, 3), "bb-async"),
    (hybrid("c0", 15, 0), "mysticeti"),
    (hybrid("c0", 15, 0, 5), "mahi-mahi-w5"),
]
CONSISTENCY_SEED = 0

# Experiment 5: each asynchronous protocol at its primary and alternative wave length.
BUFFER_ROUND_PAIRS = [
    (mahi_mahi(5), mahi_mahi(4)),
    *((hybrid(label, f, c, 5), hybrid(label, f, c, 4)) for label, (f, c) in HYBRID_POINTS.items()),
    (orcaella("c0", 9, 0, 3), orcaella("c0", 9, 0, 4)),
    *(
        (orcaella(label, f, c, 3), orcaella(label, f, c, 4))
        for label, (f, c) in ORCAELLA_POINTS.items()
    ),
    (nemo_nemo_asynchronous(3), nemo_nemo_asynchronous(2)),
]


@dataclass(frozen=True)
class Job:
    """One simulation run. The name depends only on the run's content, so experiments
    that need the same run share it."""

    protocol: Protocol
    seed: int
    crashes: int = 0
    equivocators: int = 0
    jitter: bool = False

    @property
    def name(self):
        condition = "jitter" if self.jitter else "base"
        return (
            f"{self.protocol.label}--{condition}--k{self.crashes}"
            f"--e{self.equivocators}--s{self.seed}"
        )

    def config(self):
        n = COMMITTEE_SIZE
        config = {
            "name": self.name,
            "committee_size": n,
            "latency": {"uniform": {"range_ms": LATENCY_MS}},
            "duration_secs": DURATION_SECS,
            "rng_seed": self.seed,
            "sample_interval_secs": SAMPLE_INTERVAL_SECS,
            "replica_parameters": {
                "dag": {
                    "round_timeout": {
                        "secs": 0,
                        "nanos": self.protocol.timeout_ms * 1_000_000,
                    }
                },
                "consensus": self.protocol.consensus,
            },
            "load_generator": {
                "load": LOAD_PER_REPLICA,
                "transaction_size": TRANSACTION_SIZE,
                "initial_delay": "0s",
            },
        }
        if self.jitter:
            config["jitter"] = JITTER
        if self.crashes:
            # Crashed nodes are the last indices, each isolated in its own group.
            live = list(range(n - self.crashes))
            crashed = [[index] for index in range(n - self.crashes, n)]
            config["topology"] = {"partition": [live] + crashed}
        if self.equivocators:
            config["equivocating_leaders"] = list(range(self.equivocators))
        return config


def sweep(maximum, step=2, start=0):
    """start, start + step, … up to maximum, always including maximum itself."""
    return sorted(set(range(start, maximum + 1, step)) | {maximum})


def experiments():
    """Map each experiment to its jobs."""
    happy = [Job(protocol, seed) for protocol in MAIN for seed in SEEDS]
    happy += [Job(protocol, CONSISTENCY_SEED) for protocol, _ in CONSISTENCY_PAIRS]

    crashes = [
        Job(protocol, seed, crashes=k)
        for protocol in MAIN
        for k in sweep(protocol.crashes) + [protocol.crashes + 1]
        for seed in SEEDS
    ]

    byzantine = []
    for protocol in MAIN:
        if protocol.byzantine == 0:
            continue
        for e in sweep(protocol.byzantine):
            byzantine += [Job(protocol, seed, equivocators=e) for seed in SEEDS]
        # Hybrid configurations then add crashes up to c, at the full fault budget.
        c = protocol.crashes - protocol.byzantine
        if c > 0:
            for k in sweep(c, start=2):
                byzantine += [
                    Job(protocol, seed, crashes=k, equivocators=protocol.byzantine)
                    for seed in SEEDS
                ]

    jitter = [Job(protocol, seed, jitter=True) for protocol in MAIN for seed in SEEDS]

    buffer_rounds = []
    for pair in BUFFER_ROUND_PAIRS:
        for protocol in pair:
            for seed in SEEDS:
                buffer_rounds += [
                    Job(protocol, seed),
                    Job(protocol, seed, jitter=True),
                    Job(protocol, seed, crashes=protocol.crashes - 1, jitter=True),
                ]

    return {
        "happy": happy,
        "crashes": crashes,
        "byzantine": byzantine,
        "jitter": jitter,
        "buffer-rounds": buffer_rounds,
    }


def all_jobs():
    """Every distinct job across the experiments, in a stable order."""
    jobs = {}
    for experiment_jobs in experiments().values():
        for job in experiment_jobs:
            jobs.setdefault(job.name, job)
    return list(jobs.values())
