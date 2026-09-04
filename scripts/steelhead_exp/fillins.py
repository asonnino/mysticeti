# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The paper's \\fillin numbers, computed from cached runs -> plots/fillins.md."""

import re
import subprocess

import numpy as np

import matrix
import summary
import timeseries
from figures import by_params, seed_summaries, seed_timelines
from simconfig import PLOTS_DIR, ROOT

REPLAY_SCORE = re.compile(r"replay window top (\d+) candidate (\d+) score (\d+)")


def percent_delta(value, reference):
    return (value - reference) / reference * 100.0


def good_rows():
    """Healthy-latency comparisons at the reference load (MM pair)."""
    jobs = matrix.good_jobs()
    for committee in matrix.COMMITTEES:
        load = matrix.REFERENCE_LOAD[committee]
        p50 = {}
        for slug in matrix.protocols("mm"):
            group = by_params(jobs, committee=committee, pair="mm", proto=slug, load=load)
            summaries = seed_summaries(group)
            mean, std = summary.seed_stats(
                [s.latency_percentile_s[0.5] for s in summaries])
            if mean is not None:
                p50[slug] = (mean, std)
        static = {slug: mean for slug, (mean, _) in p50.items()
                    if slug.startswith("sh-p")}
        if not static or "myst" not in p50:
            continue
        best_slug = min(static, key=static.get)
        best = static[best_slug]
        source = f"good n={committee} mm L={load}"
        yield (f"claim:good vs Mysticeti (n={committee})",
                f"{percent_delta(best, p50['myst'][0]):+.1f}%",
                f"p50 of best static Steelhead ({best_slug}) vs Mysticeti", source)
        for mahi_slug in ("mahi4", "mahi5"):
            if mahi_slug in p50:
                yield (f"claim:good vs {mahi_slug} (n={committee})",
                        f"{percent_delta(best, p50[mahi_slug][0]):+.1f}%",
                        f"p50 of {best_slug} vs {mahi_slug}", source)
        if "mahi4" in p50 and "mahi5" in p50:
            yield (f"claim:good mahi4-vs-mahi5 gap (n={committee})",
                    f"{percent_delta(p50['mahi4'][0], p50['mahi5'][0]):+.1f}%",
                    "p50 gap of Mahi-Mahi-4 vs Mahi-Mahi-5", source)


def attack_rows():
    """Mysticeti's timeout-commit interval during the attack (seconds)."""
    jobs = matrix.attack_jobs()
    for committee in matrix.COMMITTEES:
        group = by_params(jobs, committee=committee, pair="mm", proto="myst")
        if not group:
            continue
        phases = group[0].phases
        attack = next(p for p in phases if p.label == "attack")
        intervals = []
        for job in group:
            columns = timeseries.load(job.out_dir)
            if columns is None:
                continue
            # Max across replicas: the shared leader sequence, not the
            # replica-summed activity; the span comes from the tick times the
            # delta actually covers.
            ticks = timeseries.per_tick(columns, counter_mode="max")
            mask = (ticks["time_s"] > attack.start_s) & (ticks["time_s"] <= attack.end_s)
            commits = ticks["direct_commits"] + ticks["indirect_commits"]
            window, window_times = commits[mask], ticks["time_s"][mask]
            if len(window) < 2 or window[-1] <= window[0]:
                continue
            span = float(window_times[-1] - window_times[0])
            intervals.append(span / (window[-1] - window[0]))
        mean, std = summary.seed_stats(intervals)
        if mean is not None:
            yield (f"claim:async Mysticeti commit interval under attack (n={committee})",
                    f"{mean:.2f}s ± {std:.2f}",
                    "attack-phase seconds per committed leader",
                    f"attack n={committee} mm myst")


def adaptive_rows():
    """Convergence and overhead of the adaptive period."""
    jobs = matrix.adaptive_jobs()
    for committee in matrix.COMMITTEES:
        group = by_params(jobs, committee=committee, pair="mm", proto="sh-ada")
        merged = seed_timelines(group)
        if merged is None or not group:
            continue
        phases = group[0].phases
        attack = next(p for p in phases if p.label == "attack")
        times, period = merged["time_s"], merged["steelhead_period"]

        def settle_delay(from_s, until_s):
            """Start of the final stable run of the period within the phase."""
            mask = (times > from_s) & (times <= until_s)
            if not mask.any():
                return None
            window_times, window_period = times[mask], period[mask]
            unstable = np.where(~np.isclose(window_period, window_period[-1]))[0]
            start = unstable[-1] + 1 if len(unstable) else 0
            return float(window_times[start] - from_s)

        descent = settle_delay(attack.start_s, attack.end_s)
        recovery = settle_delay(attack.end_s, times[-1])
        for label, delay in [("descent", descent), ("recovery", recovery)]:
            if delay is not None:
                yield (f"claim:adaptive {label} settle time (n={committee})",
                        f"{delay:.0f}s",
                        f"seconds from phase change until the period holds its "
                        f"final {label} value (interval = {matrix.ADAPTIVE['interval']} rounds)",
                        f"adaptive n={committee} mm sh-ada")

        for slug, phase_label, from_s, until_s in [
            ("sh-p16", "healthy", attack.end_s + 20, times[-1]),
            ("sh-p1", "attack", attack.start_s, attack.end_s),
        ]:
            reference = seed_timelines(
                by_params(jobs, committee=committee, pair="mm", proto=slug))
            if reference is None:
                continue
            mask = (times > from_s) & (times <= until_s)
            adaptive_p50 = np.nanmean(merged["latency_p50_ms"][mask])
            reference_p50 = np.nanmean(reference["latency_p50_ms"][mask])
            if np.isnan(adaptive_p50) or np.isnan(reference_p50):
                continue
            yield (f"claim:adaptive overhead vs {slug} in {phase_label} (n={committee})",
                    f"{percent_delta(adaptive_p50, reference_p50):+.1f}%",
                    f"{phase_label}-phase mean windowed p50, adaptive vs {slug}",
                    f"adaptive n={committee} mm")


def replay_tracking_rows():
    """Best-effort: replayed expected delay (rounds) vs measured latency, from
    the adaptive runs' debug tracing. Flagged approximate."""
    jobs = by_params(matrix.adaptive_jobs(), committee=10, pair="mm", proto="sh-ada")
    ratios = []
    for job in jobs:
        log = job.out_dir / "tracing.log"
        columns = timeseries.load(job.out_dir)
        if not log.exists() or columns is None:
            continue
        # Max across replicas: the shared decided sequence (commits and
        # skips) approximates rounds once divided by the cohort size.
        ticks = timeseries.per_tick(columns, counter_mode="max")
        scores = {}
        for top, candidate, score in REPLAY_SCORE.findall(log.read_text()):
            scores.setdefault(int(top), {})[int(candidate)] = int(score)
        decided = (ticks["direct_commits"] + ticks["indirect_commits"]
                    + ticks["direct_skips"] + ticks["indirect_skips"])
        total_rounds = decided[-1] / 2  # leader_count = 2 in every matrix spec
        round_ms = ticks["time_s"][-1] * 1000.0 / total_rounds if total_rounds else None
        if not scores or round_ms is None:
            continue
        n = job.params["committee"]
        interval = matrix.ADAPTIVE["interval"]
        for window in scores.values():
            best = min(window.values())
            expected_ms = best / (n * interval) * round_ms
            measured = np.nanmean(ticks["latency_p50_ms"])
            if measured and not np.isnan(measured):
                ratios.append(expected_ms / measured)
    if ratios:
        mean, std = summary.seed_stats(ratios)
        yield ("claim:adaptive replay-vs-measured (approximate)",
                f"ratio {mean:.2f} ± {std:.2f}",
                "replayed expected delay (best candidate, converted via the mean "
                "round time) over measured mean windowed p50; approximate",
                "adaptive n=10 mm sh-ada (debug tracing)")


def loc_rows():
    """Implementation-size fillins (crude line counts)."""
    consensus_src = sorted((ROOT / "crates" / "consensus" / "src").rglob("*.rs"))
    def count(paths):
        result = subprocess.run(["wc", "-l", *[str(p) for p in paths]],
                                capture_output=True, text=True)
        return int(result.stdout.split()[-2]) if len(paths) > 1 else \
            int(result.stdout.split()[0])
    yield ("impl consensus crate LOC", str(count(consensus_src)),
            "wc -l over crates/consensus/src", "-")
    for name in ("replay.rs", "wave.rs"):
        path = ROOT / "crates" / "consensus" / "src" / name
        if path.exists():
            yield (f"impl {name} LOC", str(count([path])), f"wc -l {name}", "-")


def write_report():
    rows = []
    for generator in (good_rows(), attack_rows(), adaptive_rows(),
                        replay_tracking_rows(), loc_rows()):
        rows.extend(generator)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = PLOTS_DIR / "fillins.md"
    with path.open("w") as handle:
        handle.write("# Fillins report\n\n")
        handle.write("| fillin | value | definition | source |\n")
        handle.write("| --- | --- | --- | --- |\n")
        for fillin, value, definition, source in rows:
            handle.write(f"| {fillin} | {value} | {definition} | {source} |\n")
    print(f"wrote {path} ({len(rows)} rows)")
