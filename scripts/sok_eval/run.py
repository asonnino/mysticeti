# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run one shard of the simulation matrix in parallel, skipping jobs that already ran.

    python3 run.py --binary target/release/replica --out results/sok --shard 0/4 --workers 60
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matrix

# One hung run must not wedge the pool.
TIMEOUT_SECS = 2 * 60 * 60
# Written once the simulator has exported the whole run; meta.yaml alone may be partial. It
# holds the run's identity (config and binary), so a run from other settings is not reused.
COMPLETE = "complete"
# Written instead when the run failed, with the reason.
FAILED = "failed"
# The simulator exports a diverged run, then exits with this error.
DIVERGED = "simulation(s) diverged"


def in_shard(job, shard, shards):
    return zlib.crc32(job.name.encode()) % shards == shard


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity(job, binary_hash):
    """What a cached run must match to be reused: its config and the binary that ran it."""
    config = json.dumps(job.config(), sort_keys=True).encode()
    return json.dumps({"config": sha256(config), "binary": binary_hash}, sort_keys=True)


def jobs_hash(jobs):
    """Short hash of the job list, to check that every shard ran from the same matrix."""
    return sha256("\n".join(sorted(job.name for job in jobs)).encode())[:12]


def run(job, binary, binary_hash, out):
    run_dir = out / "runs" / job.name
    marker = run_dir / COMPLETE
    expected = identity(job, binary_hash)
    if marker.exists() and marker.read_text() == expected:
        # A cached divergence is still a failure.
        if "outcome: diverged" in (run_dir / "meta.yaml").read_text():
            return "diverged"
        return "cached"
    # Unfinished, failed, or produced by other settings or another build: run it again.
    if run_dir.exists():
        shutil.rmtree(run_dir)
    config_path = out / "configs" / f"{job.name}.yaml"
    # JSON is valid YAML, and needs no third-party module.
    config_path.write_text(json.dumps(job.config(), indent=1))
    log_path = out / "logs" / f"{job.name}.log"
    command = [str(binary), "simulate", "--config-path", str(config_path)]
    command += ["--output-dir", str(run_dir)]
    with log_path.open("w") as log:
        try:
            process = subprocess.run(command, stdout=log, stderr=log, timeout=TIMEOUT_SECS)
            status = None
        except subprocess.TimeoutExpired:
            status = "timeout"
    if status is None:
        # The tracing log is empty at the default level; drop it so archives stay small.
        (run_dir / "tracing.log").unlink(missing_ok=True)
        diverged = DIVERGED in log_path.read_text(errors="replace")
        if process.returncode == 0 or diverged:
            marker.write_text(expected)
            return "diverged" if diverged else "ok"
        status = f"exit {process.returncode}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / FAILED).write_text(status)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--shard", default="0/1", help="index/count, e.g. 0/4")
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    arguments = parser.parse_args()

    shard, shards = map(int, arguments.shard.split("/"))
    all_jobs = matrix.all_jobs()
    jobs = [job for job in all_jobs if in_shard(job, shard, shards)]
    binary = arguments.binary.resolve()
    binary_hash = file_sha256(binary)
    for directory in ("runs", "configs", "logs"):
        (arguments.out / directory).mkdir(parents=True, exist_ok=True)
    print(
        f"shard {shard}/{shards}: {len(jobs)} of {len(all_jobs)} jobs "
        f"(matrix {jobs_hash(all_jobs)}, binary {binary_hash[:12]}), "
        f"{arguments.workers} workers",
        flush=True,
    )

    def task(job):
        try:
            status = run(job, binary, binary_hash, arguments.out)
        except Exception as error:  # noqa: BLE001 - one broken run must not lose the tally
            status = f"error {type(error).__name__}: {error}"
        print(f"{status:8} {job.name}", flush=True)
        return status

    with ThreadPoolExecutor(arguments.workers) as pool:
        statuses = list(pool.map(task, jobs))
    failures = [status for status in statuses if status not in ("ok", "cached")]
    print(f"done: {len(statuses) - len(failures)} ok, {len(failures)} failed", flush=True)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
