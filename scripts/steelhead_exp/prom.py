# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Minimal Prometheus text-exposition parser (the four series shapes the
pipeline needs: histograms, plain counters, labeled counters, gauges)."""

import re

# No escaped-quote handling: every label the simulator emits is plain
# (authority letters, commit types, bucket bounds).
SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)\s*$")
LABEL = re.compile(r'(\w+)="([^"]*)"')


def parse(path):
    """Parse one .prom file into {series_name: [(labels, value), ...]}."""
    series = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        match = SAMPLE.match(line)
        if match is None:
            continue
        name, raw_labels, raw_value = match.groups()
        labels = dict(LABEL.findall(raw_labels)) if raw_labels else {}
        series.setdefault(name, []).append((labels, float(raw_value)))
    return series


def scalar(series, name, default=0.0):
    """The single unlabeled value of `name`, or `default`."""
    for labels, value in series.get(name, []):
        if not labels:
            return value
    return default
