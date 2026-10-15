#!/usr/bin/env python3
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""AWS campaign driver for the Hydrozoan evaluation (docs/hydrozoan-evaluation-plan.md).

Runs on the control box next to a checkout of this repository. Every run gets its own
orchestrator settings, node parameters and client parameters under ``data/aws/<run>/``, and the
orchestrator is invoked once per run (one ``benchmark`` call may sweep several loads). A run is
skipped when its ``done`` marker exists, so the driver can be restarted after any failure.

    python3 scripts/eval/aws_campaign.py list [--full]
    python3 scripts/eval/aws_campaign.py run E3-graded-x00 [E1 ...] [--full] [--dry-run]

``run`` takes experiment ids (``E1``) or single run names (``E3-graded-x00``); the campaign is
driven run by run so that each run is assessed (Grafana, summary table) before the next one.

``--full`` selects the complete run list of the plan; the default is the trimmed list agreed on
2026-09-17 (curves the simulator already shows to coincide are dropped).
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "aws"
BASE_SETTINGS = ROOT / "scripts" / "eval" / "settings-eval.yml"
LOADS = [100_000, 10_000]  # 50k only at the end, budget permitting (author, 2026-09-17)
LOAD = 10_000

# Committee-size dependent run lengths: (benchmark_duration_s, initial_delay). The delay is
# part of the duration, so n = 50 runs measure 120 s of steady state (author, 2026-09-17; the
# first n = 50 runs of the campaign used 300 s).
def timing(committee):
    return (240, "120s") if committee > 10 else (120, "30s")


def hydrozoan(f, c, k, leaders=2):
    return {"protocol": "dag-hydrangea", "leader_count": leaders, "f": f, "c": c, "k": k}


def orcaella(f, c, leaders=2):
    return {"protocol": "orcaella", "leader_count": leaders, "f": f, "c": c}


def mysticeti(leaders=2):
    return {"protocol": "mysticeti", "leader_count": leaders}


BLUE_BOTTLE = {"protocol": "blue-bottle-partially-synchronous", "leader_count": 2}


def run(name, consensus, committee=50, loads=(LOAD,), crashes=0, region_order=False,
        crash_recovery=None):
    duration, initial_delay = timing(committee)
    return {
        "name": name,
        "consensus": consensus,
        "committee": committee,
        "loads": list(loads),
        "crashes": crashes,
        "region_order": region_order,
        "crash_recovery": crash_recovery,
        "duration": duration,
        "initial_delay": initial_delay,
    }


def experiments(full):
    """Experiments in run order: the n = 50 ones first, so problems surface early."""
    e = {}
    # E1: healthy n = 50, latency vs throughput.
    curves = {
        "mysticeti": mysticeti(),
        "byz-only": hydrozoan(11, 0, 16),
        "balanced": hydrozoan(6, 6, 19),
        "orcaella-6-6": orcaella(6, 6),
    }
    if full:
        curves |= {
            "byz-heavy": hydrozoan(8, 3, 19),
            "crash-heavy": hydrozoan(2, 13, 17),
            "blue-bottle": BLUE_BOTTLE,
            # Orcaella's crash-leaning split: 5f + 3c + 1 = 50 exactly, quorum 37.
            "orcaella-5-8": orcaella(5, 8),
            # The Mysticeti end of the spectrum: p = 0, quorums 34 like Mysticeti's, fast quorum
            # 50 (unanimity). n = 3f + 1 = 49 <= 50, so the constructor accepts the committee.
            "mysticeti-end": hydrozoan(16, 0, 0),
        }
    e["E1"] = [run(f"E1-{n}", c, loads=LOADS) for n, c in curves.items()]
    # E2: the k knob at (6, 6); k = 19, Mysticeti and Orcaella (6, 6) come from E1.
    ks = [0, 6, 8, 10, 12] if full else [0, 8, 10, 12]
    e["E2"] = [run(f"E2-k{k:02d}", hydrozoan(6, 6, k)) for k in ks]
    # E3: crash sweep, nearby-first.
    # x = 11 = p: fast quorum 39 with zero spare, the last point with the direct rules alive.
    graded = [0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 16] if full else [0, 2, 4, 6, 8, 10, 11, 12, 14]
    myst = [2, 4, 6, 8, 10, 12, 14, 16] if full else [4, 8, 10, 12, 16]
    orca = [0, 2, 4, 8, 10, 11, 12] if full else [0, 2, 4, 8, 10, 11]
    e["E3"] = (
        [run(f"E3-graded-x{x:02d}", hydrozoan(6, 8, 15), crashes=x, region_order=True)
            for x in graded]
        + [run(f"E3-mysticeti-x{x:02d}", mysticeti(), crashes=x, region_order=True)
            for x in myst]
        + [run(f"E3-orcaella-8-3-x{x:02d}", orcaella(8, 3), crashes=x, region_order=True)
            for x in orca]
    )
    # E4: crash and recovery over time (full only).
    e["E4"] = [] if not full else [
        run(f"E4-{n}", c, crash_recovery={"max_faults": 12, "interval": 60}, region_order=True)
        for n, c in {"graded": hydrozoan(6, 8, 15), "mysticeti": mysticeti()}.items()
    ]
    # E5: quorum location, small committees, 1 and 2 crashes.
    small = {
        "mysticeti-n10": (mysticeti(), 10),
        "mysticeti-n7": (mysticeti(), 7),
        "orcaella-1-1-n9": (orcaella(1, 1), 9),
        "hydrozoan-1-1-4-n10": (hydrozoan(1, 1, 4), 10),
    }
    e["E5"] = [
        run(f"E5-{n}-x{x}", c, committee=size, crashes=x, region_order=True)
        for n, (c, size) in small.items()
        for x in (1, 2)
    ]
    # E6: scalability at n = 10, healthy.
    n10 = {"mysticeti": mysticeti(), "hydrozoan-1-1-4": hydrozoan(1, 1, 4)}
    if full:
        n10["orcaella-1-1"] = orcaella(1, 1)
    e["E6"] = [run(f"E6-{n}", c, committee=10, loads=LOADS) for n, c in n10.items()]
    return e


class TaggedLoader(yaml.SafeLoader):
    """Read serde_yaml's externally tagged enums (`!aws`, `!Permanent`) as `{tag: value}`."""


def construct_tagged(loader, suffix, node):
    return {suffix: loader.construct_mapping(node, deep=True)}


TaggedLoader.add_multi_constructor("!", construct_tagged)


def render_settings(base, spec, run_dir, results_dir):
    aws = base["cloud_provider"]["aws"]
    if spec["crash_recovery"]:
        faults = [
            "faults: !CrashRecovery",
            f"  max_faults: {spec['crash_recovery']['max_faults']}",
            "  interval:",
            f"    secs: {spec['crash_recovery']['interval']}",
            "    nanos: 0",
        ]
    else:
        faults = ["faults: !Permanent", f"  faults: {spec['crashes']}"]
    lines = [
        f"testbed_id: {base['testbed_id']}",
        "cloud_provider: !aws",
        f"  specs: {aws['specs']}",
        f"  token_file: {aws['token_file']}",
        f"ssh_private_key_file: {base['ssh_private_key_file']}",
        "regions:",
        *[f"  - {region}" for region in base["regions"]],
        "repository:",
        f"  url: {base['repository']['url']}",
        f"  commit: {base['repository']['commit']}",
        f"node_parameters_path: {run_dir / 'node-parameters.yml'}",
        f"client_parameters_path: {run_dir / 'client-parameters.yml'}",
        f"results_dir: {results_dir}",
        f"benchmark_duration: {spec['duration']}",
        *faults,
        f"crash_order: {'region-order' if spec['region_order'] else 'round-robin'}",
    ]
    return "\n".join(lines) + "\n"


def write_run_files(spec, results_dir):
    run_dir = DATA / spec["name"]
    run_dir.mkdir(parents=True, exist_ok=True)
    base = yaml.load(BASE_SETTINGS.read_text(), Loader=TaggedLoader)
    (run_dir / "settings.yml").write_text(render_settings(base, spec, run_dir, results_dir))
    (run_dir / "node-parameters.yml").write_text(yaml.safe_dump({
        "dag": {"round_timeout": {"secs": 1, "nanos": 0}},
        "consensus": spec["consensus"],
    }, sort_keys=False))
    (run_dir / "client-parameters.yml").write_text(
        yaml.safe_dump({"initial_delay": spec["initial_delay"]})
    )
    return run_dir


def collect_measurements(results_dir):
    """Flatten the orchestrator's `results-<branch>/` nesting into `results_dir` itself."""
    for nested in results_dir.glob("results-*/"):
        for file in nested.glob("measurements-*.yaml"):
            file.rename(results_dir / file.name)
        if not any(nested.iterdir()):
            nested.rmdir()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["list", "run"])
    parser.add_argument("experiments", nargs="*", help="E1..E6 or run names; default: all")
    parser.add_argument("--full", action="store_true", help="the plan's complete run list")
    parser.add_argument("--dry-run", action="store_true", help="write files, print commands")
    parser.add_argument("--loads", type=lambda text: [int(x) for x in text.split(",")],
        default=None, help="override the loads of the selected runs, e.g. 10000")
    parser.add_argument("--results", type=Path, default=None,
        help="results directory (default: results/results-<git sha>)")
    args = parser.parse_args()

    catalogue = experiments(args.full)
    by_name = {spec["name"]: spec for specs in catalogue.values() for spec in specs}
    runs = []
    for wanted in args.experiments or list(catalogue):
        if wanted in catalogue:
            runs.extend(catalogue[wanted])
        elif wanted in by_name:
            runs.append(by_name[wanted])
        else:
            sys.exit(f"unknown experiment or run: {wanted}")
    if args.loads:
        runs = [{**spec, "loads": args.loads} for spec in runs]
    if args.command == "list":
        minutes = sum((r["duration"] + 90) * len(r["loads"]) / 60 for r in runs)
        for spec in runs:
            print(
                f"{spec['name']:32} n={spec['committee']:<3} loads={spec['loads']}"
                f" crashes={spec['crashes']} {spec['duration']}s"
            )
        print(
            f"{len(runs)} orchestrator calls, {sum(len(r['loads']) for r in runs)} runs,"
            f" about {minutes / 60:.1f} h"
        )
        return

    sha = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True).stdout.strip()
    results_dir = args.results or ROOT / "results" / f"results-{sha}"
    results_dir.mkdir(parents=True, exist_ok=True)
    binary = ROOT / "target" / "release" / "replica"
    for spec in runs:
        run_dir = write_run_files(spec, results_dir)
        if (run_dir / "done").exists():
            print(f"skip {spec['name']} (done)")
            continue
        command = [
            str(binary), "remote-testbed", "--settings-path", str(run_dir / "settings.yml"),
            "benchmark", "--committee", str(spec["committee"]),
            "--loads", ",".join(str(load) for load in spec["loads"]),
        ]
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{stamp}] {spec['name']}: {' '.join(command)}", flush=True)
        if args.dry_run:
            continue
        # Keep a pseudo-terminal so the orchestrator's live progress shows in tmux, while
        # `script` records everything to the run's log file.
        log = run_dir / "orchestrator.log"
        started = datetime.now()
        status = subprocess.run(
            ["script", "-q", "-e", "-a", "-c", " ".join(command), str(log)]
        ).returncode
        elapsed = (datetime.now() - started).total_seconds()
        expected = spec["duration"] * len(spec["loads"])
        if status == 0 and elapsed < expected:
            # An interrupted orchestrator cleans up and exits 0; do not count that as done.
            print(f"[{stamp}] {spec['name']} ended after {elapsed:.0f}s < {expected}s: not done",
                flush=True)
            sys.exit(1)
        if status == 0:
            collect_measurements(results_dir)
            (run_dir / "done").touch()
        else:
            print(
                f"[{stamp}] {spec['name']} FAILED (exit {status}), see {run_dir}/orchestrator.log",
                flush=True,
            )
            sys.exit(status)
    print("ALL-DONE", flush=True)


if __name__ == "__main__":
    main()
