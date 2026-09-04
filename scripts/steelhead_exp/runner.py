# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Parallel, cached execution of matrix jobs via the simulator CLI."""

import os
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml

from simconfig import BINARY

# One hung simulator run must not wedge the pool forever.
SIMULATION_TIMEOUT_S = 2 * 60 * 60

# Concurrent runs' transient WALs must fit on disk (see Job.disk_weight);
# a single oversized job still runs, alone.
DISK_BUDGET_BYTES = 60 * 1024**3


class DiskGate:
    """Admits jobs while the sum of their disk weights stays under budget."""

    def __init__(self, budget):
        self.budget = budget
        self.in_flight = 0
        self.condition = threading.Condition()

    def acquire(self, weight):
        with self.condition:
            self.condition.wait_for(
                lambda: self.in_flight == 0 or self.in_flight + weight <= self.budget)
            self.in_flight += weight

    def release(self, weight):
        with self.condition:
            self.in_flight -= weight
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


def run_one(job, binary, gate=None):
    """Run one job to completion; returns its outcome string. Never raises —
    any environment failure (disk full, killed binary) records as 'error' so
    one bad job cannot take the pool down."""
    if gate is not None:
        gate.acquire(job.disk_weight)
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
    finally:
        if gate is not None:
            gate.release(job.disk_weight)


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
    # Heaviest first, so big runs hold the disk gate while light ones fill in.
    pending.sort(key=lambda job: job.disk_weight, reverse=True)
    gate = DiskGate(DISK_BUDGET_BYTES)
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_one, job, binary, gate): job for job in pending}
        for index, future in enumerate(as_completed(futures), start=1):
            job = futures[future]
            outcome = future.result()
            results.append((job, outcome))
            print(f"[{index}/{len(pending)}] {outcome.upper():12} {job.name}")
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
