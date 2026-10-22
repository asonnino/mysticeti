# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run one shard of the simulation matrix in parallel, skipping jobs that already ran.

    python3 run.py --binary target/release/replica --out results/sok --shard 0/4 --workers 60
"""

import argparse
import json
import os
import subprocess
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matrix

# One hung run must not wedge the pool.
TIMEOUT_SECS = 2 * 60 * 60
# Written once the simulator has exported the whole run; meta.yaml alone may be partial.
COMPLETE = "complete"
# The simulator exports a diverged run, then exits with this error.
DIVERGED = "simulation(s) diverged"


def in_shard(job, shard, shards):
    return zlib.crc32(job.name.encode()) % shards == shard


def run(job, binary, out):
    run_dir = out / "runs" / job.name
    if (run_dir / COMPLETE).exists():
        return "cached"
    config_path = out / "configs" / f"{job.name}.yaml"
    # JSON is valid YAML, and needs no third-party module.
    config_path.write_text(json.dumps(job.config(), indent=1))
    log_path = out / "logs" / f"{job.name}.log"
    command = [str(binary), "simulate", "--config-path", str(config_path)]
    command += ["--output-dir", str(run_dir)]
    with log_path.open("w") as log:
        try:
            process = subprocess.run(command, stdout=log, stderr=log, timeout=TIMEOUT_SECS)
        except subprocess.TimeoutExpired:
            return "timeout"
    # The tracing log is empty at the default level; drop it so archives stay small.
    (run_dir / "tracing.log").unlink(missing_ok=True)
    diverged = DIVERGED in log_path.read_text()
    if process.returncode == 0 or diverged:
        (run_dir / COMPLETE).touch()
    if diverged:
        return "diverged"
    return "ok" if process.returncode == 0 else f"exit {process.returncode}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--shard", default="0/1", help="index/count, e.g. 0/4")
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    arguments = parser.parse_args()

    shard, shards = map(int, arguments.shard.split("/"))
    jobs = [job for job in matrix.all_jobs() if in_shard(job, shard, shards)]
    for directory in ("runs", "configs", "logs"):
        (arguments.out / directory).mkdir(parents=True, exist_ok=True)
    print(f"shard {shard}/{shards}: {len(jobs)} jobs, {arguments.workers} workers", flush=True)

    def task(job):
        status = run(job, arguments.binary.resolve(), arguments.out)
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
