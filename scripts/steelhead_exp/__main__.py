# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI: run | plot | fillins | status. Invoke from the repo root as
`scripts/.venv/bin/python scripts/steelhead_exp <command>`."""

import argparse
from fnmatch import fnmatch
from pathlib import Path

import matrix
import runner
from simconfig import BINARY


def filtered_jobs(pattern):
    jobs = matrix.all_jobs()
    if pattern:
        jobs = [job for job in jobs if fnmatch(job.name, pattern)
                or job.campaign == pattern]
    return jobs


def main():
    parser = argparse.ArgumentParser(prog="steelhead_exp")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="run missing matrix jobs (cached)")
    run_parser.add_argument("--filter", help="fnmatch on job names, or a campaign name")
    run_parser.add_argument("--workers", type=int, help="worker pool size")
    run_parser.add_argument("--binary", type=Path, default=BINARY)
    run_parser.add_argument("--strict", action="store_true",
                            help="exit non-zero when any run does not pass")

    plot_parser = commands.add_parser("plot", help="render figures from cached data")
    plot_parser.add_argument("--only", help="comma-separated figure names "
                                            "(good,attack,adaptive,async)")

    commands.add_parser("fillins", help="write plots/fillins.md")

    status_parser = commands.add_parser("status", help="matrix coverage report")
    status_parser.add_argument("--filter", help="fnmatch on job names, or a campaign name")

    arguments = parser.parse_args()
    if arguments.command == "run":
        results = runner.run_jobs(filtered_jobs(arguments.filter),
                                    workers=arguments.workers, binary=arguments.binary)
        if arguments.strict and any(outcome != "pass" for _, outcome in results):
            raise SystemExit(1)
    elif arguments.command == "plot":
        import figures

        only = arguments.only.split(",") if arguments.only else None
        figures.plot(only)
    elif arguments.command == "fillins":
        import fillins

        fillins.write_report()
    elif arguments.command == "status":
        runner.status(filtered_jobs(arguments.filter))


if __name__ == "__main__":
    main()
