# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Latency-throughput plots for the Orcaella / Hydrozoan evaluation. Style
# emulates the Blue Bottle paper's `data/plot.py`. Reads the raw measurement
# files under `results/` READ-ONLY and writes figures to `plots/` (gitignored).
#
#   python scripts/eval/plot.py      # from the repo root (any cwd works)
#
# Deps: matplotlib, numpy, pyyaml (see requirements.txt).

import glob
import json
import math
import os
import re

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
import numpy as np
import yaml

matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", ".."))
RESULTS = os.path.join(REPO, "results", "results-96dee8d")
HYDRANGEA = os.path.join(HERE, "hydrangea")
PLOTS = os.path.join(REPO, "plots")
# Checkpoint-instrumented campaign (same orchestrator, extra per-stage metrics:
# end_to_end_latency_s = submission -> certified checkpoint). Same layout as
# results-96dee8d (region subdirs); results-96dee8d holds the earlier
# pure-consensus campaign still used for the fault/geo figures.
RESULTS_CKPT = os.path.join(REPO, "results", "results-2a2bc14")

# Color scheme requested for the Orcaella paper.
COLOR = {
    "bluebottle": "tab:blue",
    "orcaella": "tab:green",
    "mysticeti": "tab:orange",
    "hydrangea": "tab:red",
}

# Blue Bottle is unpublished and the c=0 run is just Orcaella collapsed to 5f+1.
# By default we therefore present that run as an ordinary Orcaella configuration,
# "OrcDAG (f=10,c=0)" (green). Flip this to True to restore it as a separate
# "Blue Bottle" baseline (blue) without touching any call site.
BLUEBOTTLE_AS_BASELINE = False


def bb_name():
    """Display family name for the c=0 run (Blue Bottle vs Orcaella)."""
    return "Blue Bottle" if BLUEBOTTLE_AS_BASELINE else "OrcDAG"


def bb_color():
    """Display color for the c=0 run (blue as a baseline, else Orcaella green)."""
    return COLOR["bluebottle"] if BLUEBOTTLE_AS_BASELINE else COLOR["orcaella"]

# Shared error-bar (p90 whisker) line/cap thickness, used across all figures.
ERRORBAR_LW = 1.2

# Shared figure size for the evaluation plots: short and wide to save space.
FIGSIZE = (6.4, 2.9)


def _save(name):
    """Write the current figure to pdf+png at the uniform FIGSIZE.

    Deliberately no bbox_inches="tight": tight cropping trims each figure to its
    own content, so figures with legends/long labels render at a different height
    than bare ones under width=\\linewidth. Saving the full FIGSIZE canvas
    (tight_layout keeps labels inside it) makes every evaluation figure identical
    in size."""
    plt.gcf().set_layout_engine("constrained")
    out_dir = PLOTS
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        plt.savefig(os.path.join(out_dir, f"{name}.{ext}"), dpi=300)
    print(f"wrote {out_dir}/{name}.pdf")


# -- YAML measurement parser (orcaella / mysticeti) --------------------------
#
# The orchestrator (mysticeti `collector.rs`) exports PromQL-windowed metrics:
# counters as per-second rates `rate(name[60s])` and histograms as quantiles.
# Because every node commits the full total order, each node's `latency_s_count`
# rate is the cluster throughput; the orchestrator's own live view averages the
# latest scrape across node-series. We replicate that: take the latest scrape,
# average across series. Mean latency = mean(sum) / mean(count); stdev from the
# `latency_squared_s` second moment.


class _YamlLoader(yaml.SafeLoader):
    pass


_YamlLoader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        loader.construct_mapping(node)
        if isinstance(node, yaml.MappingNode)
        else (
            loader.construct_scalar(node)
            if isinstance(node, yaml.ScalarNode)
            else loader.construct_sequence(node)
        )
    ),
)


def _finite(values):
    return [v for v in values if isinstance(v, (int, float)) and not math.isnan(v)]


def _latest_group(samples):
    """Values of `samples` at the most recent scrape timestamp."""
    if not samples:
        return []
    tmax = max(s["timestamp"] for s in samples)
    return _finite([s["value"] for s in samples if s["timestamp"] == tmax])


def _mean(values):
    values = _finite(values)
    return sum(values) / len(values) if values else float("nan")


def _series(samples):
    """Per-scrape across-node mean of a metric, keyed by scrape timestamp.

    Timestamps are rounded to the nearest second so that metrics scraped a few
    ms apart within the same scrape align (scrapes are ~15s apart)."""
    from collections import defaultdict
    byts = defaultdict(list)
    for x in samples:
        byts[round(x["timestamp"])].append(x["value"])
    return {t: _mean(v) for t, v in byts.items()}


def _median(values):
    values = sorted(_finite(values))
    if not values:
        return float("nan")
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2


def parse_yaml(path):
    """Return (tps, p50_s, p90_s) for one measurement file, robust to transients.

    The orchestrator exports 60s-windowed metrics (counters as per-second rates,
    histograms as quantiles), averaged across node-series. Rather than the single
    latest scrape (fragile to end-of-run spikes), we restrict to the throughput
    PLATEAU (scrapes with tps >= 0.9*max) and report the steady-state floor:
    min p50 and min p90 over the plateau, with the median sustained throughput.
    For a system keeping up with load, steady latency is flat, so the floor is
    the transient-free value; warm-up overshoot and late blips only push it up.
    """
    with open(path) as f:
        doc = yaml.load(f, Loader=_YamlLoader)
    s = doc["samples"]

    cnt = _series(s.get("latency_s_count", []))
    p50s = _series(s.get("latency_s.p50", []))
    p90s = _series(s.get("latency_s.p90", []))
    finite_cnt = {t: c for t, c in cnt.items() if not math.isnan(c)}
    if not finite_cnt:
        return float("nan"), float("nan"), float("nan")
    cmax = max(finite_cnt.values())

    plateau = [t for t, c in finite_cnt.items() if c >= 0.9 * cmax]
    tps = _median([cnt[t] for t in plateau])
    p50_vals = [p50s[t] for t in plateau
                if t in p50s and not math.isnan(p50s[t]) and p50s[t] > 0]
    p90_vals = [p90s[t] for t in plateau
                if t in p90s and not math.isnan(p90s[t]) and p90s[t] > 0]
    p50 = min(p50_vals) if p50_vals else float("nan")
    p90 = min(p90_vals) if p90_vals else float("nan")
    return tps, p50, p90


# Crash order the measurement loaders accept. The E3 crash-sweep figures set it
# to "region-order", so round-robin and region-order runs never mix in a curve.
CRASH_ORDER = "round-robin"

_MEASUREMENTS_RE = re.compile(
    r"measurements-(orcaella|mysticeti|)-?l2-(?:f(\d+)-c(\d+)-)?"
    r"512-(\d+)(-region-order)?-(\d+)-(\d+)\.yaml")


def _decode(base):
    """(name, f, c, faults, nodes, load) of a measurements file, or None when it
    does not parse or was run under a crash order other than `CRASH_ORDER`."""
    m = _MEASUREMENTS_RE.match(base)
    if not m:
        return None
    order = "region-order" if m.group(5) else "round-robin"
    if order != CRASH_ORDER:
        return None
    return (m.group(1) or "none",
            int(m.group(2)) if m.group(2) is not None else None,
            int(m.group(3)) if m.group(3) is not None else None,
            int(m.group(4)), int(m.group(6)), int(m.group(7)))


def quorum_of(path):
    """Commit quorum of the run at `path`, from its decoded committee.

    Mirrors the implementation (mysticeti `protocol.rs`): Mysticeti commits at
    floor(2n/3)+1; Orcaella's direct-commit quorum is n-f-c at the DEPLOYED n
    (only the skip quorum stays at the minimal-committee 4f+2c+1). The old
    campaign deployed minimal committees, where the two coincide."""
    if not path:
        return None
    dec = _decode(os.path.basename(path))
    if not dec:
        return None
    name, ff, cc, faults, nodes, load = dec
    if name == "mysticeti":
        return 2 * nodes // 3 + 1
    return nodes - (ff or 0) - (cc or 0)


def find_file(region, load, predicate, root=None):
    """Path of the single file in `region` at `load` matching `predicate`."""
    for path in sorted(glob.glob(os.path.join(root or RESULTS, region, "*.yaml"))):
        dec = _decode(os.path.basename(path))
        if not dec:
            continue
        name, ff, cc, faults, nodes, l = dec
        if l == load and predicate(name, ff, cc, faults, nodes):
            return path
    return None


def placement_std(path):
    """Across-node spread (s) of per-node mean latency at the floor scrape.

    Captures committee geographic-placement variance (NOT the within-node tx
    spread, which is dominated by the eu-us↔Tokyo split)."""
    import statistics
    from collections import defaultdict
    with open(path) as f:
        s = yaml.load(f, Loader=_YamlLoader)["samples"]
    cnt = defaultdict(dict); summ = defaultdict(dict); p50m = defaultdict(dict)
    for x in s.get("latency_s_count", []):
        cnt[round(x["timestamp"])][x["labels"].get("instance")] = x["value"]
    for x in s.get("latency_s_sum", []):
        summ[round(x["timestamp"])][x["labels"].get("instance")] = x["value"]
    for x in s.get("latency_s.p50", []):
        p50m[round(x["timestamp"])][x["labels"].get("instance")] = x["value"]
    if not cnt:
        return 0.0
    cmax = max(_mean(list(c.values())) for c in cnt.values())
    cand = [t for t in cnt if _mean(list(cnt[t].values())) >= 0.9 * cmax and t in p50m]
    t = min(cand, key=lambda t: _mean([v for v in p50m[t].values()
                                        if not (isinstance(v, float) and math.isnan(v))]))
    means = [summ[t][i] / cnt[t][i] for i in cnt[t]
            if cnt[t][i] and not math.isnan(cnt[t][i]) and i in summ[t]]
    return statistics.pstdev(means) if len(means) > 1 else 0.0


def load_curve(region, predicate, root=None):
    """Collect (tps, p50_s, p90_s) points for files matching `predicate`.

    `predicate(name, f, c, faults, nodes, load)` decides inclusion. Points are
    sorted by throughput so the line connects monotonically.
    """
    points = []
    pattern = os.path.join(root or RESULTS, region, "*.yaml")
    for path in sorted(glob.glob(pattern)):
        dec = _decode(os.path.basename(path))
        if not dec:
            continue
        name, ff, cc, faults, nodes, load = dec
        if not predicate(name, ff, cc, faults, nodes, load):
            continue
        points.append(parse_yaml(path))
    points.sort(key=lambda p: p[0])
    xs = [p[0] for p in points]
    p50 = [p[1] for p in points]
    p90 = [p[2] for p in points]
    return xs, p50, p90


# -- Hydrangea parser (plain-text bench summaries) ---------------------------


def load_hydrangea(faults, nodes):
    """Return (tps, median_s, median_s) sorted by throughput for Hydrangea runs.

    Hydrangea's text summaries expose mean/median (not p50/p90); we use the
    End-To-End median as the line and carry no band.
    """
    points = []
    pattern = os.path.join(HYDRANGEA, f"bench-{faults}-{nodes}-*-True-*-512.txt")
    import statistics
    for path in sorted(glob.glob(pattern)):
        # A file holds one SUMMARY block per trial; aggregate the trials by median.
        trials = []
        for block in open(path).read().split("SUMMARY:")[1:]:
            seg = block[block.find("End-To-End"):]
            med = re.search(
                r"To First Commit:\s*Mean Latency:\s*[\d,]+ ms\s*"
                r"Median Latency:\s*([\d,]+) ms", seg)
            tps = re.search(r"TPS:\s*([\d,]+) tx/s", seg)
            if med and tps:
                tps_v = int(tps.group(1).replace(",", ""))
                trials.append((tps_v, int(med.group(1).replace(",", "")) / 1000.0))
        if not trials:
            continue
        tps_v = statistics.median(t for t, _ in trials)
        lat_s = statistics.median(l for _, l in trials)
        points.append((tps_v, lat_s, lat_s))
    points.sort(key=lambda p: p[0])
    return [p[0] for p in points], [p[1] for p in points], [p[2] for p in points]


# -- Plotting (Blue Bottle aesthetic) ----------------------------------------


@tick.FuncFormatter
def x_formatter(x, pos):
    return f"{x/1000:.0f}k" if x >= 1000 else f"{x:,.0f}"


@tick.FuncFormatter
def y_formatter(y, pos):
    return f"{y:,.0f}" if y >= 10 else f"{y:,.2f}"


# Fixed legend order for the latency-throughput plots (system families).
_LEGEND_ORDER = ("Hydrangea", "Mysticeti", "Blue Bottle", "OrcDAG")


def _ordered_handles(ax):
    """Return (handles, labels) sorted into _LEGEND_ORDER (stable within group)."""
    handles, labels = ax.get_legend_handles_labels()

    def key(lbl):
        for i, name in enumerate(_LEGEND_ORDER):
            if lbl.startswith(name):
                return i
        return len(_LEGEND_ORDER)

    order = sorted(range(len(labels)), key=lambda i: key(labels[i]))
    return [handles[i] for i in order], [labels[i] for i in order]


def plot_line(xs, p50, p90, label, color, marker, linestyle="solid"):
    """Plot p50 as the line with an upper whisker reaching p90."""
    if not xs:
        return
    upper = [max(0.0, hi - lo) for lo, hi in zip(p50, p90)]
    lower = [0.0] * len(xs)
    plt.errorbar(
        xs, p50, yerr=[lower, upper],
        label=label, color=color, marker=marker, linestyle=linestyle,
        capsize=3, linewidth=4, markersize=10, elinewidth=ERRORBAR_LW, capthick=ERRORBAR_LW,
    )


def happy_case_large(ckpt=True):
    """Latency-throughput, happy case, large committees (~50-51 nodes).

    Canonical source is the checkpoint-instrumented campaign
    (`results-2a2bc14`; Mysticeti n=50, Orcaella n=51). The Hydrangea line is
    overlaid from the original campaign (not re-run; illustrative, off-scale
    at every load) -- disclosed in the setup. Pass `ckpt=False` to rebuild
    the original-campaign variant for comparison."""
    plt.figure(figsize=FIGSIZE)

    region = "eu-us"  # WAN testbed (canonical for the paper)
    root = RESULTS_CKPT if ckpt else None
    y_top = 0.65

    # Mysticeti (orange), n=49 (original campaign) / n=50 (re-run).
    xs, p50, p90 = load_curve(
        region, lambda n, f, c, flt, nodes, load: n == "mysticeti" and flt == 0 and nodes >= 49,
        root=root,
    )
    plot_line(xs, p50, p90, "Mysticeti (f=16)", COLOR["mysticeti"], "D")

    # c=0 run = Orcaella collapsed to 5f+1 (n=51); shown as OrcDAG (f=10,c=0)
    # by default, or as the Blue Bottle baseline when BLUEBOTTLE_AS_BASELINE.
    xs, p50, p90 = load_curve(
        region,
        lambda n, f, c, flt, nodes, load:
            n == "orcaella" and c == 0 and flt == 0 and nodes >= 49,
        root=root,
    )
    plot_line(xs, p50, p90, f"{bb_name()} (f=10,c=0)", bb_color(), "s")

    # Orcaella (green) = the three c>0 configs at ~50 nodes.
    orcaella_cfgs = [
        (6, 6, "o", "solid"),
        (8, 3, "^", "dashed"),
        (2, 13, "v", "dotted"),
    ]
    for f_, c_, marker, ls in orcaella_cfgs:
        xs, p50, p90 = load_curve(
            region,
            lambda n, f, c, flt, nodes, load, f_=f_, c_=c_: (
                n == "orcaella" and c > 0 and f == f_ and c == c_ and flt == 0 and nodes >= 49
            ),
            root=root,
        )
        plot_line(xs, p50, p90, f"OrcDAG (f={f_},c={c_})", COLOR["orcaella"], marker, ls)

    # Hydrangea (red), n=50. Its end-to-end latency is ~67 s at EVERY measured
    # load (it does not scale to large committees), so all real points lie far
    # off the top of the frame. As Blue Bottle does, we prepend a single visual
    # anchor near zero throughput (HY_ANCHOR) and connect it to the real
    # measured points; the line therefore rises from the floor and runs
    # straight off the top. Only the anchor is invented -- every other point is
    # real data. (Not re-run in the checkpoint campaign; overlaid from the
    # original campaign -- illustrative only.)
    HY_ANCHOR = (100, 0.34)
    xs, p50, _ = load_hydrangea(0, 50)
    xs = [HY_ANCHOR[0]] + xs
    p50 = [HY_ANCHOR[1]] + p50
    plot_line(xs, p50, p50, "Hydrangea (f=9,c=10,k=2)", COLOR["hydrangea"], "x")

    plt.ylim(bottom=0, top=y_top)
    plt.xlim(left=0, right=105000)
    plt.xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.xticks(weight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_formatter(x_formatter)
    ax.yaxis.set_major_formatter(y_formatter)
    h, l = _ordered_handles(ax)
    ax.legend(
        h, l, loc="upper center", ncol=2, frameon=True, framealpha=0.9,
        prop={"weight": "bold", "size": 8}, handlelength=1.6,
        columnspacing=1.0, borderaxespad=0.4,
    )

    _save("happy_case_large")
    plt.close()


def latency_bars(load, style="arrow", ckpt=True):
    """Per-protocol p50 bar chart at `load`, large committees, eu-us.

    style:
        'arrow'    -- bars + a single <-> arrow for the overall gain (the 50k fig).
        'variance' -- bars + placement-stdev error bars + quorum-size annotations;
                    shows the gain is ~constant across Orcaella configs and NOT
                    ordered by quorum size (effect A is dormant at low load).
        'queuing'  -- bars stacked as base(@10k) + queuing(p50@load − p50@10k);
                    the hatched queuing segment is larger for the larger quorum.
    Canonical source is the checkpoint-instrumented campaign (`ckpt=True`,
    the default); pass `ckpt=False` to rebuild the original-campaign variant.
    """
    region = "eu-us"
    root = RESULTS_CKPT if ckpt else None

    def stats(predicate, l=load):
        path = find_file(region, l, predicate, root=root)
        return tuple(parse_yaml(path)[1:3]) if path else (float("nan"), float("nan"))

    # (label, color, hatch, predicate); q is computed per run via quorum_of()
    bars = [
        ("Mysticeti\n(f=16)", COLOR["mysticeti"], "",
        lambda n, f, c, flt, nodes: n == "mysticeti" and flt == 0 and nodes >= 49),
        (f"{bb_name()}\n(f=10,c=0)", bb_color(), "",
        lambda n, f, c, flt, nodes: n == "orcaella" and c == 0 and flt == 0 and nodes >= 49),
        ("OrcDAG\n(f=6,c=6)", COLOR["orcaella"], "\\\\",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 6 and c == 6 and flt == 0),
        ("OrcDAG\n(f=8,c=3)", COLOR["orcaella"], "//",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 8 and c == 3 and flt == 0),
        ("OrcDAG\n(f=2,c=13)", COLOR["orcaella"], "..",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 2 and c == 13 and flt == 0),
    ]

    plt.figure(figsize=FIGSIZE)
    ax = plt.gca()
    heights = []
    for i, (label, color, hatch, predicate) in enumerate(bars):
        p50, p90 = stats(predicate)
        heights.append(p50)
        path = find_file(region, load, predicate, root=root)
        err = placement_std(path) if style == "variance" and path else None
        if style == "queuing":
            base = stats(predicate, 10000)[0]
            ax.bar(i, base, width=0.68, color=color, hatch=hatch, edgecolor="white",
                    linewidth=1.0, zorder=2)
            ax.bar(i, max(0.0, p50 - base), width=0.68, bottom=base, color=color, alpha=0.5,
                    hatch="xxx", edgecolor="white", linewidth=1.0, zorder=2)
            # queuing overhead label, boxed inside the xxx segment
            ax.text(i, (base + p50) / 2, f"+{(p50 - base) * 1000:.0f}", ha="center",
                    va="center", fontsize=9, fontweight="bold", color="0.1",
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.82))
        else:
            ax.bar(i, p50, width=0.68, color=color, hatch=hatch, edgecolor="white",
                    linewidth=1.0, yerr=err, capsize=4,
                    error_kw=dict(ecolor="0.2", lw=1.2, zorder=3))

    myst, others = heights[0], heights[1:]
    omin, omax = min(others), max(others)
    ax.axhline(myst, color=COLOR["mysticeti"], ls="--", lw=2.5, zorder=1)
    gain = f"{(myst - omax) * 1000:.0f}–{(myst - omin) * 1000:.0f} ms"

    if style in ("variance", "queuing"):
        # Hatched "improvement" zone from the Orcaella band up to the Mysticeti
        # line, with the gain labelled inside it (clearer than a thin <-> arrow).
        ax.axhspan(omin, myst, facecolor=COLOR["orcaella"], alpha=0.13, hatch="///",
                    edgecolor=COLOR["orcaella"], linewidth=0.0, zorder=0)
        ax.text((len(bars) - 1) / 2.0, (omin + myst) / 2, gain, ha="center",
                va="center", fontsize=11, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.6", alpha=0.9))
    else:  # arrow (50k): band over the Orcaella range + a <-> arrow to Mysticeti
        ax.axhspan(omin, omax, color=COLOR["orcaella"], alpha=0.13, zorder=0)
        xa = 2.5
        ax.annotate("", xy=(xa, myst), xytext=(xa, (omin + omax) / 2),
                    arrowprops=dict(arrowstyle="<->", color="black", lw=2.2))
        ax.text(xa + 0.2, (omin + omax) / 2, gain, ha="left", va="center",
                fontsize=10.5, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="0.7", alpha=0.95))

    # quorum size inside every bar (boxed so it stays readable over the hatches);
    # computed from each run's deployed committee (see quorum_of).
    for i, (label, color, hatch, predicate) in enumerate(bars):
        q = quorum_of(find_file(region, load, predicate, root=root))
        ax.text(i, 0.14, f"q={q}", ha="center", va="center", fontsize=9,
                fontweight="bold", color="0.1",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.72))

    ax.set_xticks(range(len(bars)))
    ax.set_xticklabels([b[0] for b in bars], fontweight="bold", fontsize=10)
    ax.set_ylim(0, 0.42) if style == "variance" else ax.set_ylim(0, 0.45)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    ax.yaxis.set_major_formatter(tick.FuncFormatter(lambda y, pos: f"{y:g}"))
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    cap = f"({load // 1000}k tx/s)"
    ax.text(0.5, 0.96, cap, transform=ax.transAxes, ha="center", va="top",
            fontsize=9.5, fontweight="bold")

    _save(f"latency_bars_{load // 1000}k")
    plt.close()


def fault_tradeoff(load=50000):
    """Fault-tolerance vs latency tradeoff (eu-us), 3f+1 (Mysticeti) -> 5f+3c+1.

    The (f, c) plane shows the fault budget. Both protocols' fault-tolerance
    frontiers at the measured committee (n=49) are drawn: Mysticeti (3f+1)
    tolerates f+c <= 16 faults; Orcaella (5f+3c+1) tolerates 5f+3c <= 48. The
    two frontiers meet at the pure-crash corner (f=0, c=16) -- a crash costs
    Orcaella 3 slots, exactly like a Mysticeti fault -- and diverge toward the
    Byzantine axis: the hatched wedge is the Byzantine fault tolerance Orcaella
    trades away. In return, the measured Orcaella configs (colored markers) are
    11-19% faster than Mysticeti (p50 at 50k tx/s)."""
    N_REF = 49  # committee size of the measured Mysticeti baseline (3*16+1)

    def p50_at(predicate):
        _, p50, _ = load_curve(
            "eu-us",
            lambda n, f, c, flt, nodes, l, p=predicate: l == load and p(n, f, c, flt, nodes),
        )
        return p50[0] if p50 else float("nan")

    myst = p50_at(lambda n, f, c, flt, nodes: n == "mysticeti" and flt == 0 and nodes == 49)
    configs = [(10, 0), (8, 3), (6, 6), (2, 13)]
    improvements = [
        100.0 * (myst - p50_at(
            lambda n, f, c, flt, nodes, ff=ff, cc=cc:
                n == "orcaella" and f == ff and c == cc and flt == 0
        )) / myst
        for ff, cc in configs
    ]

    fig, ax = plt.subplots(figsize=(6.0, 4.3))
    cc = np.linspace(0, 16, 200)
    orca_f = (48 - 3 * cc) / 5.0          # Orcaella frontier: 5f+3c = 48 (n=49)
    myst_f = 16 - cc                       # Mysticeti frontier: f+c = 16 (n=49)

    # Region Orcaella can deploy, and the Byzantine tolerance it gives up.
    ax.fill_betweenx(cc, 0, orca_f, color=COLOR["orcaella"], alpha=0.10)
    ax.fill_betweenx(cc, orca_f, myst_f, color=COLOR["mysticeti"], alpha=0.16, hatch="//",
                    edgecolor=COLOR["mysticeti"], linewidth=0.0)
    ax.plot(orca_f, cc, color=COLOR["orcaella"], lw=3,
            label="Orcaella frontier ($n{=}5f{+}3c{+}1$)")
    ax.plot(myst_f, cc, color=COLOR["mysticeti"], lw=3, ls="--",
            label="Optimal frontier ($n{=}3f{+}1$)")

    # Measured Orcaella configs, colored by % latency improvement over Mysticeti.
    fs = [ff for ff, _ in configs]
    csv = [cc_ for _, cc_ in configs]
    sc = ax.scatter(fs, csv, c=improvements, cmap="Greens", vmin=10, vmax=24,
                    s=240, edgecolor="black", linewidth=1.5, zorder=6)
    cbar = fig.colorbar(sc, ax=ax, pad=0.02)
    cbar.set_label(f"Latency improvement vs Optimal  (%)\n(p50 @ {load // 1000}k tx/s)",
                    fontweight="bold", fontsize=10)
    cbar.ax.tick_params(labelsize=10)
    for (ff, cc_), imp in zip(configs, improvements):
        ax.annotate(f"{imp:.0f}%", (ff, cc_), textcoords="offset points", xytext=(9, 6),
                    fontsize=10, fontweight="bold", zorder=7,
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85))

    # Mysticeti baseline (pure Byzantine, f=16).
    ax.scatter([16], [0], marker="*", s=360, color=COLOR["mysticeti"],
                edgecolor="black", linewidth=1.2, zorder=6)
    ax.annotate("Mysticeti", (16, 0), textcoords="offset points", xytext=(-2, 24),
                ha="right", fontsize=10, fontweight="bold", color=COLOR["mysticeti"])
    ax.text(4.5, 5.0, "Orcaella", color=COLOR["orcaella"], fontweight="bold",
            fontsize=11, ha="center", va="center")

    ax.set_xlabel("Byzantine faults  f", fontweight="bold", fontsize=13)
    ax.set_ylabel("Crash faults  c", fontweight="bold", fontsize=13)
    ax.set_xlim(0, 17)
    ax.set_ylim(0, 17)
    ax.set_xticks(range(0, 18, 2))
    ax.set_yticks(range(0, 18, 2))
    ax.tick_params(labelsize=11)
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontweight("bold")
    ax.grid(alpha=0.25)
    ax.set_axisbelow(True)
    ax.legend(loc="upper right", frameon=True, prop={"weight": "bold", "size": 9})

    out_dir = PLOTS
    os.makedirs(out_dir, exist_ok=True)
    tag = f"{load // 1000}k"
    for ext in ("pdf", "png"):
        plt.savefig(os.path.join(out_dir, f"fault_tradeoff_{tag}.{ext}"),
                    bbox_inches="tight", dpi=300)
    plt.close()
    print(f"wrote {out_dir}/fault_tradeoff_{tag}.pdf")


def crash_only_eu():
    """Latency-throughput for a single-region (EU) crash-only Orcaella run.

    The eu testbed only has f=0 data: a 3-node committee (f=0, c=1). The
    histogram quantiles in this run are bucket artifacts (the real ~22 ms
    latency falls below the first bucket, so p50/p90/p99 pin to 50/90/99 ms).
    We therefore plot mean +/- stdev, both computed from raw accumulators
    (sum, count, squared-sum) and so independent of the bucket resolution."""
    def latest_by_instance(samples):
        if not samples:
            return {}
        tmax = max(x["timestamp"] for x in samples)
        return {x["labels"].get("instance"): x["value"]
                for x in samples if x["timestamp"] == tmax}

    points = []
    for path in sorted(glob.glob(os.path.join(RESULTS, "eu", "*.yaml"))):
        with open(path) as fh:
            doc = yaml.load(fh, Loader=_YamlLoader)
        s = doc["samples"]
        cnt = latest_by_instance(s.get("latency_s_count", []))
        summ = latest_by_instance(s.get("latency_s_sum", []))
        sq = latest_by_instance(s.get("latency_squared_s", []))
        means, stdevs = [], []
        for inst, c in cnt.items():
            if not c or math.isnan(c) or inst not in summ or inst not in sq:
                continue
            mu = summ[inst] / c
            var = sq[inst] / c - mu * mu
            means.append(mu)
            stdevs.append(math.sqrt(var) if var > 0 else 0.0)
        if not means:
            continue
        tps = sum(cnt.values()) / len(cnt)
        points.append((tps, sum(means) / len(means), sum(stdevs) / len(stdevs)))
    points.sort()
    xs = [p[0] for p in points]
    ys_ms = [p[1] * 1000 for p in points]   # mean latency (ms)
    es_ms = [p[2] * 1000 for p in points]   # stdev (ms)

    # With only two loads, a box-per-load reads better than a 2-point line.
    # Boxes are built from mean +/- stdev (NOT quartiles -- the histogram is too
    # coarse for real percentiles here), so we label that explicitly.
    fig, ax = plt.subplots(figsize=FIGSIZE)
    stats = [dict(med=mu, q1=mu - sd, q3=mu + sd,
                    whislo=max(0, mu - 2 * sd), whishi=mu + 2 * sd, fliers=[])
            for mu, sd in zip(ys_ms, es_ms)]
    positions = list(range(len(stats)))
    ax.bxp(stats, positions=positions, widths=0.5, showfliers=False,
            patch_artist=True,
            boxprops=dict(facecolor=COLOR["orcaella"], alpha=0.45,
                        edgecolor=COLOR["orcaella"], lw=2),
            medianprops=dict(color=COLOR["orcaella"], lw=3),
            whiskerprops=dict(color=COLOR["orcaella"], lw=2),
            capprops=dict(color=COLOR["orcaella"], lw=2))

    ax.set_xticks(positions)
    ax.set_xticklabels([f"{round(x / 1000)}k" for x in xs], fontweight="bold", fontsize=13)
    ax.set_ylim(0, 45)
    ax.set_yticks(range(0, 46, 5))   # finer horizontal grid (every 5 ms)
    ax.set_xlim(-0.7, len(stats) - 0.3)
    ax.set_xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    ax.set_ylabel("Latency (ms)", fontweight="bold", fontsize=14)
    ax.tick_params(labelsize=13)
    for lbl in ax.get_yticklabels():
        lbl.set_fontweight("bold")
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)

    _save("crash_only_eu")
    plt.close()


def faults_small_eu_us():
    """Latency-throughput under 2 crash faults, small committees (~10), eu-us.

    Committees are the minimum each protocol needs for the fault budget:
    Mysticeti n=10, Blue Bottle (5f+1) n=11, OrcDAG (f=1,c=1) n=9. The three
    DAG systems sustain the load with no latency inflation (fast leader-skip);
    Hydrangea collapses (~27-79 s) and runs off the top of the frame.
    NOTE: at these tiny committees Orcaella/Blue Bottle are NOT faster than
    Mysticeti -- their ~80% quorum costs more than Mysticeti's extra round when
    n is small; the 2-message-delay win only shows at large committees."""
    plt.figure(figsize=FIGSIZE)
    region = "eu-us"
    y_top = 0.65

    # p50 lines with p90 upper whiskers. We focus on the 2-fault budget, so only
    # the minimal committee for f=2 (n=7) is shown for Mysticeti -- its quorum
    # 2f+1=5 = the whole alive set under 2 crashes, so every round waits for the
    # slowest node.
    xs, p50, p90 = load_curve(
        region, lambda n, f, c, flt, nodes, load: n == "mysticeti" and flt == 2 and nodes == 7)
    plot_line(xs, p50, p90, "Mysticeti (f=2)", COLOR["mysticeti"], "D")

    xs, p50, p90 = load_curve(
        region,
        lambda n, f, c, flt, nodes, load:
            n == "orcaella" and c == 0 and flt == 2 and nodes == 11)
    plot_line(xs, p50, p90, f"{bb_name()} (f=2,c=0)", bb_color(), "s")

    orca_xs, orca_p50, orca_p90 = load_curve(
        region,
        lambda n, f, c, flt, nodes, load:
            n == "orcaella" and c > 0 and flt == 2 and nodes == 9)
    plot_line(orca_xs, orca_p50, orca_p90, "OrcDAG (f=1,c=1)", COLOR["orcaella"], "o")

    # Hydrangea (red), n=10, 2 faults: ~27-79 s, off-scale. One made-up anchor
    # near zero throughput, then the real (off-scale) points -- as Blue Bottle does.
    HY_ANCHOR = (100, 0.31)
    xs, p50, _ = load_hydrangea(2, 10)
    xs = [HY_ANCHOR[0]] + xs
    p50 = [HY_ANCHOR[1]] + p50
    plot_line(xs, p50, p50, "Hydrangea (f=1,c=2,k=2)", COLOR["hydrangea"], "x")

    plt.ylim(bottom=0, top=1.0)
    plt.xlim(left=0, right=45000)
    plt.xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.xticks(weight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_formatter(x_formatter)
    ax.yaxis.set_major_formatter(
        tick.FuncFormatter(lambda y, pos: "0" if abs(y) < 1e-9 else f"{y:.1f}"))
    h, l = _ordered_handles(ax)
    ax.legend(h, l, loc="upper center", ncol=2, frameon=True, framealpha=0.9,
                prop={"weight": "bold", "size": 8.5})

    _save("faults_small_eu_us")
    plt.close()


def improvement_vs_load(ckpt=True):
    """% latency improvement of each Orcaella config over Mysticeti vs load.

    Large committee (~50, no faults), eu-us. Shows the 2-message-delay advantage
    shrinking with load (~23% at 10k -> ~13% at 100k) because Orcaella's larger
    quorum is more load-sensitive -- and that this trend is the same across all
    (f,c) splits (the lines overlap). Canonical source is the
    checkpoint-instrumented campaign (`ckpt=True`, the default)."""
    region = "eu-us"
    root = RESULTS_CKPT if ckpt else None
    if ckpt:
        # only loads for which the re-run campaign has a Mysticeti baseline
        loads = sorted({
            _decode(os.path.basename(p))[5]
            for p in glob.glob(os.path.join(RESULTS_CKPT, "eu-us", "measurements-mysticeti-*.yaml"))
            if _decode(os.path.basename(p))
        })
    else:
        loads = [10000, 50000, 100000]

    def pq(predicate, load):
        _, p, q = load_curve(
            region,
            lambda n, f, c, flt, nodes, l, P=predicate, L=load: l == L and P(n, f, c, flt, nodes),
            root=root,
        )
        return (p[0], q[0]) if p else (float("nan"), float("nan"))

    myst = {l: pq(lambda n, f, c, flt, nodes: n == "mysticeti" and flt == 0 and nodes >= 49, l)
            for l in loads}  # (p50, p90)
    cfgs = [
        (f"{bb_name()} (f=10,c=0)", bb_color(), "s", "solid",
        lambda n, f, c, flt, nodes: n == "orcaella" and c == 0 and flt == 0 and nodes >= 49),
        ("OrcDAG (f=6,c=6)", COLOR["orcaella"], "o", "solid",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 6 and c == 6 and flt == 0),
        ("OrcDAG (f=8,c=3)", COLOR["orcaella"], "^", "dashed",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 8 and c == 3 and flt == 0),
        ("OrcDAG (f=2,c=13)", COLOR["orcaella"], "v", "dotted",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 2 and c == 13 and flt == 0),
    ]

    plt.figure(figsize=FIGSIZE)
    for label, color, marker, ls, pred in cfgs:
        imp50, imp90 = [], []
        for l in loads:
            op, oq = pq(pred, l)
            mp, mq = myst[l]
            imp50.append((mp - op) / mp * 100)   # dot: improvement at p50
            imp90.append((mq - oq) / mq * 100)   # whisker: improvement at the p90 tail
        lower = [max(0.0, a - b) for a, b in zip(imp50, imp90)]
        upper = [max(0.0, b - a) for a, b in zip(imp50, imp90)]
        plt.errorbar(loads, imp50, yerr=[lower, upper], color=color, marker=marker,
                    linestyle=ls, linewidth=3, markersize=10, capsize=3,
                    elinewidth=ERRORBAR_LW, capthick=ERRORBAR_LW, label=label)

    plt.ylim(bottom=0, top=28)
    plt.xlim(left=0, right=105000)
    plt.xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    plt.ylabel("Latency improvement\nvs Mysticeti (%)", fontweight="bold", fontsize=13)
    plt.xticks(weight="bold", fontsize=13)
    plt.yticks(weight="bold", fontsize=13)
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_formatter(x_formatter)
    plt.legend(loc="lower center", ncol=2, frameon=True, framealpha=0.9,
                prop={"weight": "bold", "size": 8.5})

    _save("improvement_vs_load")
    plt.close()


def fault_coverage():
    """Intro figure: fault-tolerance coverage in the (f, c) plane (no latency).

    Frontiers derived straight from the bounds at a fixed committee size n:
        - Orcaella needs n >= 5f+3c+1, so it tolerates any (f,c) with 5f+3c <= n-1
        (green area); its frontier runs from (f_cap, 0) to (0, fc_cap).
        - A latency-optimal 3f+1 protocol treats every fault as Byzantine, so it
        needs n >= 3(f+c)+1, i.e. f+c <= (n-1)/3 -- a single line (orange).
    Both frontiers meet at the f=0 apex (0, (n-1)/3). The orange wedge between
    them is the Byzantine tolerance Orcaella trades for its 2-delay commit."""
    n = 100                       # committee size (matches the intro's n ~ 100)
    budget = n - 1
    f_cap = budget / 5.0          # Orcaella f-intercept at c=0:  5f = n-1
    fc_cap = budget / 3.0         # f=0 apex shared by both:      3c = n-1
    lim = fc_cap + 2              # axis headroom above the apex

    fig, ax = plt.subplots(figsize=(5.6, 3.0))
    cc = np.linspace(0, fc_cap, 200)
    orca_f = (budget - 3 * cc) / 5.0   # Orcaella frontier: 5f + 3c = n-1
    myst_f = fc_cap - cc               # optimal frontier:  f + c = (n-1)/3
    # Solid translucent fills (no hatch): the green region is everything
    # Orcaella can be configured to tolerate; the orange wedge above its frontier
    # is the Byzantine tolerance traded away for the 2-message-delay commit.
    ax.fill_betweenx(cc, 0, orca_f, color=COLOR["orcaella"], alpha=0.18, lw=0)
    ax.fill_betweenx(cc, orca_f, myst_f, color=COLOR["mysticeti"], alpha=0.14, lw=0)
    ax.plot(myst_f, cc, color=COLOR["mysticeti"], lw=3, ls="--",
            label="Optimal (max)")
    ax.plot(orca_f, cc, color=COLOR["orcaella"], lw=3, ls="--",
            label="Orcaella (max)")

    # Example configurations discussed in the intro (n ~ 100). Each provisioned
    # cap (f,c) tolerates, at runtime, any mix along the line from (f,c) to
    # (0,f+c) -- a Byzantine fault trades 1:1 for a crash.
    bench = [(12, 13), (16, 6), (5, 25)]
    for i, (f, c) in enumerate(bench):
        ax.plot([f, 0], [c, f + c], color=COLOR["orcaella"], lw=3, zorder=5,
                label="Orcaella (examples)" if i == 0 else None)

    ax.text(6, 6, "Orcaella", color=COLOR["orcaella"],
            fontweight="bold", fontsize=11, ha="center", va="center")
    ax.text(0.63 * fc_cap, 0.16 * fc_cap, "traded for\nlatency",
            color=COLOR["mysticeti"], fontweight="bold", fontsize=11,
            ha="center", va="center")
    ax.set_xlabel("Byzantine faults  f", fontweight="bold", fontsize=13)
    ax.set_ylabel("Crash faults  c", fontweight="bold", fontsize=13)
    # x starts at f=1: the paper scopes its results to f>0 (at f=0 the CFT
    # bound n=2c+1 applies instead), so showing f=0 would be misleading.
    ax.set_xlim(1, lim)
    ax.set_ylim(0, lim)
    ax.set_xticks([1] + list(range(5, int(lim) + 1, 5)))
    ax.set_yticks(range(0, int(lim) + 1, 5))
    ax.tick_params(labelsize=11)
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontweight("bold")
    ax.grid(alpha=0.25)
    ax.set_axisbelow(True)
    ax.legend(loc="upper right", frameon=True,
                prop={"weight": "bold", "size": 9})
    out_dir = PLOTS
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        plt.savefig(os.path.join(out_dir, f"fault_coverage.{ext}"),
                    bbox_inches="tight", dpi=300)
    plt.close()
    print(f"wrote {out_dir}/fault_coverage.pdf")


def geo_boundary_bars():
    """Step 3: quorum size bites when it forces the slow region into the quorum.

    Small committees under 2 crashes, eu-us, p50 @ 10k. Mysticeti f=3 (n=10)
    keeps slack and excludes Tokyo (fast); Orcaella f1,c1 (n=9) and Mysticeti f=2
    (n=7) have no slack and must include Tokyo. Orcaella beats minimal Mysticeti
    (2 rounds < 3) but loses to the Mysticeti that kept slack to dodge Tokyo."""
    region = "eu-us"

    def pq(pred):
        path = find_file(region, 10000, pred)
        return tuple(parse_yaml(path)[1:3]) if path else (float("nan"), float("nan"))

    bars = [
        ("Mysticeti\n(f=3, n=10)", COLOR["mysticeti"], "", "excludes\nTokyo",
        lambda n, f, c, flt, nodes: n == "mysticeti" and flt == 2 and nodes == 10),
        ("OrcDAG\n(f=1,c=1, n=9)", COLOR["orcaella"], "", "needs\nTokyo",
        lambda n, f, c, flt, nodes: n == "orcaella" and c > 0 and flt == 2 and nodes == 9),
        ("Mysticeti\n(f=2, n=7)", COLOR["mysticeti"], "//", "needs\nTokyo",
        lambda n, f, c, flt, nodes: n == "mysticeti" and flt == 2 and nodes == 7),
    ]
    plt.figure(figsize=FIGSIZE)
    ax = plt.gca()
    vals = []
    for i, (label, color, hatch, note, pred) in enumerate(bars):
        v, _ = pq(pred)
        vals.append(v)
        ax.bar(i, v, width=0.6, color=color, hatch=hatch, edgecolor="white", linewidth=1.0)
        ax.text(i, v + 0.008, f"{v * 1000:.0f} ms", ha="center", va="bottom",
                fontsize=11, fontweight="bold")
        # note in a rounded translucent box so it stays readable over the hatch
        ax.text(i, 0.06, note, ha="center", va="bottom", fontsize=9, fontweight="bold",
                color="0.1", bbox=dict(boxstyle="round,pad=0.3", fc="white",
                                        ec="none", alpha=0.72))

    ax.set_xticks(range(len(bars)))
    ax.set_xticklabels([b[0] for b in bars], fontweight="bold", fontsize=10)
    ax.set_ylim(0, 0.6)
    ax.set_ylabel("Latency (s)", fontweight="bold", fontsize=13)
    plt.yticks(weight="bold", fontsize=13)
    ax.yaxis.set_major_formatter(
        tick.FuncFormatter(lambda y, pos: "0" if abs(y) < 1e-9 else f"{y:.1f}"))
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    ax.text(0.5, 0.97, "(10k tx/s)", transform=ax.transAxes,
            ha="center", va="top", fontsize=9, fontweight="bold")

    _save("geo_boundary_bars")
    plt.close()


def param_coverage():
    """Runtime fault tolerance of the benchmarked Orcaella configs (eval preamble).

    A deployment configured (f,c) at n=5f+3c+1 (fast quorum q=4f+2c+1) tolerates,
    at runtime, any integer (b Byzantine, k crash) with b<=f (safety: <=f
    equivocators) and b+k<=f+c (liveness: n-b-k>=q). So 1 Byzantine trades 1:1
    for a crash, giving the frontier line from (f,c) to (0,f+c). Orange dashed =
    the optimal 3f+1 line (b+k=16), as in fault_coverage. No hatched area."""
    fig, ax = plt.subplots(figsize=FIGSIZE)
    # hatched backdrop: everything Orcaella can cover (frontier (10,0)->(0,16))
    cc = np.linspace(0, 16, 200)
    orca_f = 10.0 * (16 - cc) / 16.0
    ax.fill_betweenx(cc, 0, orca_f, facecolor=COLOR["orcaella"],
                    alpha=0.10, hatch="///", edgecolor=COLOR["orcaella"],
                    linewidth=0.0, zorder=0)
    # orange wedge between the Orcaella frontier and the Mysticeti line (cf. Fig 1)
    ax.fill_betweenx(cc, orca_f, 16 - cc, color=COLOR["mysticeti"], alpha=0.14,
                    lw=0, zorder=0)
    ax.plot([16, 0], [0, 16], color=COLOR["mysticeti"], lw=3, ls="--", zorder=2,
            label="Mysticeti (f=16)")

    # one line per config: same Orcaella green, distinguished by line style +
    # marker (consistent with the other figures), not by shade.
    configs = [
        ((10, 0), "dashdot", "s"),
        ((6, 6), "solid", "o"),
        ((8, 3), "dashed", "^"),
        ((2, 13), "dotted", "v"),
    ]
    for (f, c), ls, mk in configs:
        bs = list(range(f, -1, -1))            # b = f, f-1, ..., 0
        cs = [c + (f - b) for b in bs]          # crash = c + traded Byzantine
        ax.plot(bs, cs, color=COLOR["orcaella"], lw=2.5, linestyle=ls, marker=mk,
                markersize=6, markeredgecolor="white", markeredgewidth=0.8,
                zorder=3, label=f"OrcDAG (f={f},c={c})")

    ax.set_xlabel("Byzantine faults  f", fontweight="bold", fontsize=13)
    ax.set_ylabel("Crash faults  c", fontweight="bold", fontsize=13)
    ax.set_xlim(0, 17)
    ax.set_ylim(0, 17)
    ax.set_xticks(range(0, 18, 2))
    ax.set_yticks(range(0, 18, 2))
    ax.tick_params(labelsize=11)
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontweight("bold")
    ax.grid(alpha=0.25)
    ax.set_axisbelow(True)
    ax.legend(loc="upper right", frameon=True, prop={"weight": "bold", "size": 8.5})

    _save("param_coverage")
    plt.close()


# -- Checkpoint / end-to-end latency figures (results-2a2bc14 campaign) -----
#
# The checkpoint campaign exports, alongside the fast-path `latency_s`
# (submission -> commit), the certified end-to-end latency
# `end_to_end_latency_s` (submission -> certified checkpoint, i.e. the
# Resilient-Path client metric under the certify-by-ordering engine). Both are
# aggregated with the same steady-state floor as `parse_yaml`.


def parse_ckpt_yaml(path):
    """Return (tps, commit_p50, commit_p90, e2e_p50, e2e_p90) in seconds.

    Same robust aggregation as `parse_yaml`: restrict to the throughput plateau
    (tps >= 0.9*max) and take the transient-free floor (min) per metric."""
    with open(path) as f:
        doc = yaml.load(f, Loader=_YamlLoader)
    s = doc["samples"]

    cnt = _series(s.get("latency_s_count", []))
    finite_cnt = {t: c for t, c in cnt.items() if not math.isnan(c)}
    if not finite_cnt:
        return (float("nan"),) * 5
    cmax = max(finite_cnt.values())
    plateau = [t for t, c in finite_cnt.items() if c >= 0.9 * cmax]
    tps = _median([cnt[t] for t in plateau])

    def floor(metric):
        series = _series(s.get(metric, []))
        vals = [series[t] for t in plateau
                if t in series and not math.isnan(series[t]) and series[t] > 0]
        return min(vals) if vals else float("nan")

    return (tps, floor("latency_s.p50"), floor("latency_s.p90"),
            floor("end_to_end_latency_s.p50"), floor("end_to_end_latency_s.p90"))


def ckpt_find(load, predicate, region="eu-us"):
    """Path of the single checkpoint-campaign file at `load` matching `predicate`."""
    for path in sorted(glob.glob(os.path.join(RESULTS_CKPT, region, "*.yaml"))):
        dec = _decode(os.path.basename(path))
        if not dec:
            continue
        name, ff, cc, faults, nodes, l = dec
        if l == load and predicate(name, ff, cc, faults, nodes):
            return path
    return None


# (label, color, hatch, predicate) for the checkpoint-campaign committees:
# Mysticeti n=50 (3f+1 -> f=16), Orcaella n=51. q computed via quorum_of().
_CKPT_BARS = [
    ("Mysticeti\n(f=16)", "mysticeti", "",
    lambda n, f, c, flt, nodes: n == "mysticeti"),
    ("OrcDAG\n(f=10,c=0)", "orcaella", "",
    lambda n, f, c, flt, nodes: n == "orcaella" and f == 10 and c == 0),
    ("OrcDAG\n(f=6,c=6)", "orcaella", "\\\\",
    lambda n, f, c, flt, nodes: n == "orcaella" and f == 6 and c == 6),
    ("OrcDAG\n(f=8,c=3)", "orcaella", "//",
    lambda n, f, c, flt, nodes: n == "orcaella" and f == 8 and c == 3),
    ("OrcDAG\n(f=2,c=13)", "orcaella", "..",
    lambda n, f, c, flt, nodes: n == "orcaella" and f == 2 and c == 13),
]


def checkpoint_happy_case():
    """Latency-throughput: ordering vs end-to-end latency, all committees.

    Mysticeti plus all four Orcaella configs (fig-3 markers), each with two
    curves: solid = ordering latency, dashed = end-to-end latency (certified
    checkpoint). Certification runs through consensus (certify-by-ordering),
    so the dashed curve sits roughly one ordering cycle above the solid one
    for both systems -- and Orcaella's shorter cycle compounds. The Orcaella
    configs overlap almost exactly, visualizing the (f,c)-split neutrality."""
    from matplotlib.lines import Line2D

    plt.figure(figsize=FIGSIZE)

    # (label, color, marker, predicate); markers match happy_case_large.
    systems = [
        ("Mysticeti (f=16)", "mysticeti", "D",
        lambda n, f, c, flt, nodes: n == "mysticeti"),
        (f"{bb_name()} (f=10,c=0)", "orcaella", "s",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 10 and c == 0),
        ("OrcDAG (f=6,c=6)", "orcaella", "o",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 6 and c == 6),
        ("OrcDAG (f=8,c=3)", "orcaella", "^",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 8 and c == 3),
        ("OrcDAG (f=2,c=13)", "orcaella", "v",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 2 and c == 13),
    ]
    for label, color, marker, pred in systems:
        points = []
        for path in sorted(glob.glob(os.path.join(RESULTS_CKPT, "eu-us", "*.yaml"))):
            dec = _decode(os.path.basename(path))
            if not dec:
                continue
            name, ff, cc, faults, nodes, load = dec
            if not pred(name, ff, cc, faults, nodes):
                continue
            points.append(parse_ckpt_yaml(path))
        points.sort(key=lambda p: p[0])
        xs = [p[0] for p in points]
        # solid = ordering latency; dashed = end-to-end latency
        plot_line(xs, [p[1] for p in points], [p[2] for p in points],
                    None, COLOR[color], marker, "solid")
        plot_line(xs, [p[3] for p in points], [p[4] for p in points],
                    None, COLOR[color], marker, "dashed")

    plt.ylim(bottom=0, top=1.1)
    plt.xlim(left=0, right=105000)
    plt.xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.xticks(weight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_formatter(x_formatter)
    ax.yaxis.set_major_formatter(y_formatter)
    # Composite legend: markers identify the committee, linestyle the metric.
    handles = [Line2D([0], [0], color=COLOR[color], marker=marker, lw=4,
                        ms=9, label=label)
                for label, color, marker, pred in systems]
    handles += [Line2D([0], [0], color="0.25", lw=3, ls="solid",
                        label="ordering"),
                Line2D([0], [0], color="0.25", lw=3, ls="dashed",
                        label="end-to-end")]
    ax.legend(handles=handles, loc="upper center", ncol=3, frameon=True,
                framealpha=0.9, prop={"weight": "bold", "size": 8},
                handlelength=1.8, columnspacing=0.8, borderaxespad=0.3)

    _save("checkpoint_happy_case")
    plt.close()


def checkpoint_bars(load):
    """Per-protocol latency at `load`, stacked commit + certification.

    Bottom (solid) = fast-path commit p50 (VoteQC); top (xxx hatch) = the extra
    latency to certified checkpoint finality (includes the 1-8 ms execution).
    Dashed orange line = Mysticeti's certified finality; the shaded span is the
    certified-path gain of Orcaella over Mysticeti."""
    plt.figure(figsize=FIGSIZE)
    ax = plt.gca()

    commits, e2es = [], []
    for i, (label, color, hatch, predicate) in enumerate(_CKPT_BARS):
        path = ckpt_find(load, predicate)
        _, c50, _, e50, _ = parse_ckpt_yaml(path) if path else ((float("nan"),) * 5)
        commits.append(c50)
        e2es.append(e50)
        ax.bar(i, c50, width=0.68, color=COLOR[color], hatch=hatch,
                edgecolor="white", linewidth=1.0, zorder=2)
        ax.bar(i, max(0.0, e50 - c50), width=0.68, bottom=c50, color=COLOR[color],
                alpha=0.5, hatch="xxx", edgecolor="white", linewidth=1.0, zorder=2)
        ax.text(i, (c50 + e50) / 2, f"+{(e50 - c50) * 1000:.0f}", ha="center",
                va="center", fontsize=9, fontweight="bold", color="0.1",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.82))

    myst_e2e, orca_e2es = e2es[0], e2es[1:]
    omin, omax = min(orca_e2es), max(orca_e2es)
    ax.axhline(myst_e2e, color=COLOR["mysticeti"], ls="--", lw=2.5, zorder=1)
    gain = f"{(myst_e2e - omax) * 1000:.0f}–{(myst_e2e - omin) * 1000:.0f} ms"
    ax.axhspan(omin, myst_e2e, facecolor=COLOR["orcaella"], alpha=0.13, hatch="///",
                edgecolor=COLOR["orcaella"], linewidth=0.0, zorder=0)
    ax.text((len(_CKPT_BARS) - 1) / 2.0, (omin + myst_e2e) / 2, gain, ha="center",
            va="center", fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.6", alpha=0.9))

    for i, (label, color, hatch, predicate) in enumerate(_CKPT_BARS):
        q = quorum_of(ckpt_find(load, predicate))
        ax.text(i, 0.05, f"q={q}", ha="center", va="center", fontsize=9,
                fontweight="bold", color="0.1",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.72))

    ax.set_xticks(range(len(_CKPT_BARS)))
    ax.set_xticklabels([b[0] for b in _CKPT_BARS], fontweight="bold", fontsize=10)
    ax.set_ylim(0, 0.92)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    ax.yaxis.set_major_formatter(tick.FuncFormatter(lambda y, pos: f"{y:g}"))
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    ax.text(0.5, 0.96, f"({load // 1000}k tx/s)", transform=ax.transAxes,
            ha="center", va="top", fontsize=9.5, fontweight="bold")

    _save(f"checkpoint_bars_{load // 1000}k")
    plt.close()


# Re-run of Mysticeti on the global testbed (the C benchmark's own exporter).
MYSTICETI_C_NEW = os.path.join(RESULTS, "global", "mysticeti-c-new")


def _bucket_quantile(bucket_deltas, q):
    """histogram_quantile over a cumulative-bucket delta {upper_bound: count}."""
    items = sorted(((float(k), v) for k, v in bucket_deltas.items()),
                    key=lambda x: x[0])
    total = items[-1][1]
    if total <= 0:
        return float("nan")
    target = q * total
    prev_le, prev_c = 0.0, 0
    for le, c in items:
        if c >= target:
            if c == prev_c:
                return le
            return prev_le + (target - prev_c) / (c - prev_c) * (le - prev_le)
        prev_le, prev_c = le, c
    return items[-1][0]


def parse_mysticeti_c(path):
    """Return (tps, p50_s, p90_s) for a `mysticeti-c-new` JSON dump.

    Unlike the PromQL-windowed YAMLs, the C benchmark exports per-node
    CUMULATIVE latency counters (count, sum, coarse histogram buckets) at
    ~15s scrapes. We difference consecutive scrapes per node (aligned by
    sample index), sum the deltas across nodes, and apply the plateau-floor
    aggregation of parse_yaml with two changes. (1) The plateau anchors on
    the MEDIAN interval rate, not the max -- the warm-up backlog drains in
    intervals committing far above the offered load (with tens-of-seconds
    latencies), which would otherwise hijack a max-based plateau. (2) The
    tail of that drain can still land inside the rate band (e.g. 104k tx/s
    at p50 2.2s during the 100k run), so intervals whose p50 exceeds 2x the
    plateau floor are pruned before taking the tps median and latency
    floor. Caveat: the buckets are coarse (0.25s wide around these
    latencies), so the interpolated percentiles carry ~tens-of-ms
    uncertainty."""
    import statistics
    with open(path) as f:
        doc = json.load(f)
    intervals = {}
    for samples in doc["data"]["shared"].values():
        s = sorted(samples, key=lambda x: x["timestamp"]["secs"])
        for i in range(1, len(s)):
            a, b = s[i - 1], s[i]
            dt = b["timestamp"]["secs"] - a["timestamp"]["secs"]
            dc = b["count"] - a["count"]
            if dt <= 0 or dc < 0:
                continue
            slot = intervals.setdefault(i, [0.0, 0, {}])
            slot[0] += dc / dt  # each node commits the full order
            slot[1] += 1
            for k, v in b["buckets"].items():
                slot[2][k] = slot[2].get(k, 0) + v - a["buckets"].get(k, 0)
    rows = [(rate_sum / nn, bd)
            for _, (rate_sum, nn, bd) in sorted(intervals.items()) if nn]
    rates = [r[0] for r in rows if r[0] > 0]
    if not rates:
        return float("nan"), float("nan"), float("nan")
    cmed = statistics.median(rates)
    plateau = [(rate, _bucket_quantile(bd, 0.5), _bucket_quantile(bd, 0.9))
                for rate, bd in rows if 0.9 * cmed <= rate <= 1.1 * cmed]
    plateau = [(r, q50, q90) for r, q50, q90 in plateau if q50 > 0]
    if not plateau:
        return float("nan"), float("nan"), float("nan")
    floor50 = min(q50 for _, q50, _ in plateau)
    steady = [(r, q50, q90) for r, q50, q90 in plateau if q50 <= 2 * floor50]
    tps = statistics.median(r for r, _, _ in steady)
    p50 = min(q50 for _, q50, _ in steady)
    p90 = min((q90 for _, _, q90 in steady if q90 > 0), default=float("nan"))
    return tps, p50, p90


def mysticeti_c_file(load, nodes=50, faults=0):
    """Path of the mysticeti-c-new run at `load` (None if absent)."""
    path = os.path.join(
        MYSTICETI_C_NEW, f"measurements-c-512-{faults}-{nodes}-{load}.json")
    return path if os.path.exists(path) else None


def mysticeti_c_curve(nodes=50, faults=0, max_load=100000):
    """(tps, p50_s, p90_s) lists over the mysticeti-c-new load sweep."""
    points = []
    for path in sorted(glob.glob(os.path.join(
            MYSTICETI_C_NEW, f"measurements-c-512-{faults}-{nodes}-*.json"))):
        m = re.match(rf"measurements-c-512-{faults}-{nodes}-(\d+)\.json",
                    os.path.basename(path))
        if not m or int(m.group(1)) > max_load:
            continue
        points.append(parse_mysticeti_c(path))
    points.sort(key=lambda p: p[0])
    return ([p[0] for p in points], [p[1] for p in points],
            [p[2] for p in points])


def happy_case_global():
    """Latency-throughput, happy case, large committees (~50), global testbed.

    Appendix variant of `happy_case_large`, sourced from the pure-consensus
    campaign's `global` region (ordering latency only). No Hydrangea run
    exists there. The Mysticeti line comes from the mysticeti-c-new re-run
    (n=50, 10k/50k/100k; see parse_mysticeti_c) -- consistent with the lone
    original-campaign YAML point (p50 464ms vs 437ms at 50k), which it
    replaces. Includes the pure-crash f=0,c=16 config, which was only run
    on this testbed."""
    plt.figure(figsize=FIGSIZE)

    region = "global"
    y_top = 0.85

    xs, p50, p90 = mysticeti_c_curve(nodes=50)
    plot_line(xs, p50, p90, "Mysticeti (f=16)", COLOR["mysticeti"], "D")

    xs, p50, p90 = load_curve(
        region,
        lambda n, f, c, flt, nodes, load:
            n == "orcaella" and c == 0 and flt == 0 and nodes >= 49,
    )
    plot_line(xs, p50, p90, f"{bb_name()} (f=10,c=0)", bb_color(), "s")

    orcaella_cfgs = [
        (6, 6, "o", "solid"),
        (8, 3, "^", "dashed"),
        (2, 13, "v", "dotted"),
        (0, 16, "P", "dashdot"),
    ]
    for f_, c_, marker, ls in orcaella_cfgs:
        xs, p50, p90 = load_curve(
            region,
            lambda n, f, c, flt, nodes, load, f_=f_, c_=c_: (
                n == "orcaella" and c > 0 and f == f_ and c == c_ and flt == 0 and nodes >= 49
            ),
        )
        plot_line(xs, p50, p90, f"OrcDAG (f={f_},c={c_})", COLOR["orcaella"], marker, ls)

    plt.ylim(bottom=0, top=y_top)
    plt.xlim(left=0, right=105000)
    plt.xlabel("Throughput (tx/s)", fontweight="bold", fontsize=14)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.xticks(weight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_formatter(x_formatter)
    ax.yaxis.set_major_formatter(y_formatter)
    h, l = _ordered_handles(ax)
    ax.legend(
        h, l, loc="upper center", ncol=2, frameon=True, framealpha=0.9,
        prop={"weight": "bold", "size": 8}, handlelength=1.6,
        columnspacing=1.0, borderaxespad=0.4,
    )

    _save("happy_case_global")
    plt.close()


def latency_bars_global(load, style="plain"):
    """Per-config p50 bar chart at `load`, large committees, global testbed.

    Appendix variant of `latency_bars`, plus the pure-crash f=0,c=16 config.
    The Mysticeti bar (and the dashed line / improvement band it anchors)
    comes from the mysticeti-c-new re-run (see parse_mysticeti_c). style
    'queuing' stacks base(@10k) + the load-induced increment, as in the
    eu-us 100k figure."""
    region = "global"

    def stats(predicate, l=load):
        if predicate is None:  # Mysticeti: from the mysticeti-c-new re-run
            path = mysticeti_c_file(l)
            return tuple(parse_mysticeti_c(path)[1:3]) if path else (float("nan"), float("nan"))
        path = find_file(region, l, predicate)
        return tuple(parse_yaml(path)[1:3]) if path else (float("nan"), float("nan"))

    bars = [
        ("Mysticeti\n(f=16)", COLOR["mysticeti"], "", None),
        (f"{bb_name()}\n(f=10,c=0)", bb_color(), "",
        lambda n, f, c, flt, nodes: n == "orcaella" and c == 0 and flt == 0 and nodes >= 49),
        ("OrcDAG\n(f=6,c=6)", COLOR["orcaella"], "\\\\",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 6 and c == 6 and flt == 0),
        ("OrcDAG\n(f=8,c=3)", COLOR["orcaella"], "//",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 8 and c == 3 and flt == 0),
        ("OrcDAG\n(f=2,c=13)", COLOR["orcaella"], "..",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 2 and c == 13 and flt == 0),
        ("OrcDAG\n(f=0,c=16)", COLOR["orcaella"], "++",
        lambda n, f, c, flt, nodes: n == "orcaella" and f == 0 and c == 16 and flt == 0),
    ]

    plt.figure(figsize=FIGSIZE)
    ax = plt.gca()
    heights = []
    for i, (label, color, hatch, predicate) in enumerate(bars):
        p50, p90 = stats(predicate)
        heights.append(p50)
        if style == "queuing":
            base = stats(predicate, 10000)[0]
            ax.bar(i, base, width=0.68, color=color, hatch=hatch, edgecolor="white",
                    linewidth=1.0, zorder=2)
            ax.bar(i, max(0.0, p50 - base), width=0.68, bottom=base, color=color, alpha=0.5,
                    hatch="xxx", edgecolor="white", linewidth=1.0, zorder=2)
            ax.text(i, (base + p50) / 2, f"+{(p50 - base) * 1000:.0f}", ha="center",
                    va="center", fontsize=9, fontweight="bold", color="0.1",
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.82))
        else:
            ax.bar(i, p50, width=0.68, color=color, hatch=hatch, edgecolor="white",
                    linewidth=1.0)

    myst, others = heights[0], heights[1:]
    omin, omax = min(others), max(others)
    ax.axhline(myst, color=COLOR["mysticeti"], ls="--", lw=2.5, zorder=1)
    gain = f"{(myst - omax) * 1000:.0f}–{(myst - omin) * 1000:.0f} ms"
    ax.axhspan(omin, myst, facecolor=COLOR["orcaella"], alpha=0.13, hatch="///",
                edgecolor=COLOR["orcaella"], linewidth=0.0, zorder=0)
    ax.text((len(bars) - 1) / 2.0, (omin + myst) / 2, gain, ha="center",
            va="center", fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.6", alpha=0.9))

    for i, (label, color, hatch, predicate) in enumerate(bars):
        if predicate is None:
            q = 2 * 50 // 3 + 1  # Mysticeti n=50: floor(2n/3)+1
        else:
            q = quorum_of(find_file(region, load, predicate))
        ax.text(i, 0.14, f"q={q}", ha="center", va="center", fontsize=9,
                fontweight="bold", color="0.1",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.72))

    ax.set_xticks(range(len(bars)))
    ax.set_xticklabels([b[0] for b in bars], fontweight="bold", fontsize=10)
    ax.set_ylim(0, 0.55 if style == "queuing" else 0.5)
    plt.ylabel("Latency (s)", fontweight="bold", fontsize=14)
    plt.yticks(weight="bold", fontsize=14)
    ax.yaxis.set_major_formatter(tick.FuncFormatter(lambda y, pos: f"{y:g}"))
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    cap = f"({load // 1000}k tx/s)"
    ax.text(0.5, 0.96, cap, transform=ax.transAxes, ha="center", va="top",
            fontsize=9.5, fontweight="bold")

    _save(f"latency_bars_global_{load // 1000}k")
    plt.close()


if __name__ == "__main__":
    # The checkpoint-instrumented campaign (results-2a2bc14) and the
    # mysticeti-c-new re-run are not tracked in this repo; fall back to the
    # original campaign (results-96dee8d) and skip the figures that need them.
    ckpt = os.path.isdir(RESULTS_CKPT)
    if not ckpt:
        print(f"note: {RESULTS_CKPT} not found; using results-96dee8d for the "
                "happy-case/bars/improvement figures and skipping checkpoint figures")
    happy_case_large(ckpt=ckpt)
    param_coverage()
    latency_bars(50000, "arrow", ckpt=ckpt)
    latency_bars(10000, "variance", ckpt=ckpt)
    latency_bars(100000, "queuing", ckpt=ckpt)
    fault_tradeoff(10000)
    fault_tradeoff(50000)
    fault_tradeoff(100000)
    fault_coverage()
    crash_only_eu()
    faults_small_eu_us()
    improvement_vs_load(ckpt=ckpt)
    geo_boundary_bars()
    if ckpt:
        checkpoint_happy_case()
        checkpoint_bars(10000)
        checkpoint_bars(100000)
    if os.path.isdir(MYSTICETI_C_NEW):
        happy_case_global()
        latency_bars_global(10000)
        latency_bars_global(100000, "queuing")
    else:
        print(f"note: {MYSTICETI_C_NEW} not found; skipping global-testbed figures")
