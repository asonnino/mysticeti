# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Parallel, cached execution of matrix jobs via the simulator CLI."""

import os
import shutil
import subprocess
import threading

import yaml

from simconfig import BINARY

# One hung simulator run must not wedge the pool forever. Overridable for
# heavy n=50 stalling-protocol runs that legitimately need more wall time.
SIMULATION_TIMEOUT_S = int(os.environ.get("STEELHEAD_SIM_TIMEOUT_H", "2")) * 60 * 60

# Concurrent runs' transient WALs must fit on disk (see Job.disk_weight);
# a single oversized job still runs, alone. Overridable for large-disk hosts
# via STEELHEAD_DISK_BUDGET_GB (e.g. a cloud box provisioned for full n=50
# parallelism).
DISK_BUDGET_BYTES = int(os.environ.get("STEELHEAD_DISK_BUDGET_GB", "60")) * 1024**3


class Scheduler:
    """Weight-aware work queue: each idle worker takes the heaviest pending
    job that fits the remaining disk budget, so light jobs fill in around
    heavy ones instead of queueing behind them."""

    def __init__(self, jobs, budget):
        self.pending = sorted(jobs, key=lambda job: job.disk_weight, reverse=True)
        self.budget = budget
        self.in_flight = 0
        self.condition = threading.Condition()

    def take(self):
        """The next job that fits, or None when the queue is exhausted."""
        with self.condition:
            while True:
                if not self.pending:
                    return None
                for index, job in enumerate(self.pending):
                    fits = self.in_flight + job.disk_weight <= self.budget
                    if fits or self.in_flight == 0:
                        self.in_flight += job.disk_weight
                        return self.pending.pop(index)
                self.condition.wait()

    def release(self, job):
        with self.condition:
            self.in_flight -= job.disk_weight
            self.condition.notify_all()


def outcome_of(job):
    """The cached outcome ('pass', 'no-progress', 'diverged', 'error') or None
    when the job has not run."""
    meta_path = job.out_dir / "meta.yaml"
    if not meta_path.exists():
        return None
    try:
        meta = yaml.safe_load(meta_path.read_text())
    except yaml.YAMLError:
        return "error"
    if not isinstance(meta, dict):
        return "error"
    return meta.get("outcome", "error")


def is_cached(job):
    return outcome_of(job) == "pass"


def run_one(job, binary):
    """Run one job to completion; returns its outcome string. Never raises —
    any environment failure (disk full, killed binary) records as 'error' so
    one bad job cannot take the pool down."""
    try:
        if job.out_dir.exists():
            shutil.rmtree(job.out_dir)
        job.out_dir.mkdir(parents=True)
        input_path = job.out_dir / "input.yaml"
        input_path.write_text(yaml.safe_dump(job.spec, sort_keys=False))
        command = [str(binary), "simulate", "--config-path", str(input_path),
                    "--output-dir", str(job.out_dir)]
        if job.debug_log:
            command += ["--log-level", "debug"]
        completed = subprocess.run(command, capture_output=True, text=True,
                                    timeout=SIMULATION_TIMEOUT_S)
        outcome = outcome_of(job)
        if completed.returncode != 0 or outcome is None:
            error_log = job.out_dir / "runner-error.log"
            error_log.write_text(
                f"exit code {completed.returncode}\n\n"
                f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}\n"
            )
            return outcome or "error"
        return outcome
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"error: {job.name}: {error}")
        return "error"


def run_jobs(jobs, workers=None, binary=BINARY):
    """Run every non-cached job; failures are recorded, never fatal. Returns
    the list of (job, outcome) for jobs that ran."""
    if not binary.exists():
        raise SystemExit(f"{binary} not found; run `cargo build --release -p cli` first")
    cached = [job for job in jobs if is_cached(job)]
    pending = [job for job in jobs if not is_cached(job)]
    print(f"{len(cached)} cached, {len(pending)} to run")
    if not pending:
        return []
    workers = workers or max(1, (os.cpu_count() or 4) - 2)
    scheduler = Scheduler(pending, DISK_BUDGET_BYTES)
    results = []
    results_lock = threading.Lock()

    def worker():
        while True:
            job = scheduler.take()
            if job is None:
                return
            outcome = run_one(job, binary)
            scheduler.release(job)
            with results_lock:
                results.append((job, outcome))
                index = len(results)
            print(f"[{index}/{len(pending)}] {outcome.upper():12} {job.name}", flush=True)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    failed = [(job, outcome) for job, outcome in results if outcome != "pass"]
    if failed:
        print(f"\n{len(failed)} runs did not pass:")
        for job, outcome in failed:
            print(f"  {outcome:12} {job.out_dir}")
    return results


def status(jobs):
    """Per-campaign coverage report."""
    campaigns = {}
    for job in jobs:
        entry = campaigns.setdefault(job.campaign, {"pass": 0, "missing": 0, "failed": []})
        outcome = outcome_of(job)
        if outcome == "pass":
            entry["pass"] += 1
        elif outcome is None:
            entry["missing"] += 1
        else:
            entry["failed"].append((job.name, outcome))
    for campaign, entry in campaigns.items():
        failed = len(entry["failed"])
        print(f"{campaign:10} pass {entry['pass']:4}  missing {entry['missing']:4}  "
                f"failed {failed}")
        for name, outcome in entry["failed"]:
            print(f"           {outcome:12} {name}")
