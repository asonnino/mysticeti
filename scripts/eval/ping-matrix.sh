#!/usr/bin/env bash
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

# Measure the round-trip time between every pair of active testbed instances and write a CSV
# (source region/ip, destination region/ip, probes sent/received, min/avg/max/mdev ms). It is
# the input of the simulator's calibrated topology (docs/hydrozoan-evaluation-plan.md, §6):
# avg/2 is the one-way delay of a link, mdev its jitter, sent - received its loss.
#
# Runs on the control machine next to the orchestrator; instances come from `remote-testbed
# status`. Every source pings every other instance (public IP, the address the validators use),
# eight destinations at a time, all sources in parallel: with the default 20 probes per pair the
# whole 54-instance matrix takes about half a minute. ICMP must be allowed by the testbed
# security group (the orchestrator's group opens all inbound traffic; if every pair times out,
# add an ICMP rule to `hydrozoan-eval` in every region).
#
#   scripts/eval/ping-matrix.sh [settings.yml] [out.csv] [probes-per-pair]
#
# Run it once right after `remote-testbed start` (idle instances) and once more around midday
# to check for drift; keep both files.
set -euo pipefail

SETTINGS=${1:-scripts/eval/settings-eval.yml}
OUT=${2:-results/ping-matrix-$(date -u +%Y-%m-%dT%H%M).csv}
COUNT=${3:-20}
KEY=$(grep '^ssh_private_key_file:' "$SETTINGS" | awk '{print $2}')
KEY=${KEY/#\~/$HOME}

# "region ip" pairs of the active instances, in the orchestrator's order: a "[REGION]" heading
# (possibly wrapped in ANSI bold) then one "  ● index  ssh -i key ubuntu@ip" line per instance
# (○ marks a stopped instance).
HOSTS=$(./target/release/replica remote-testbed --settings-path "$SETTINGS" status 2>&1 \
    | awk 'match($0, /\[[A-Za-z0-9-]+\]/) {region = tolower(substr($0, RSTART + 1, RLENGTH - 2))}
        /●/ {for (i = 1; i <= NF; i++) if ($i ~ /@/) {split($i, a, "@"); print region, a[2]}}')
[ -n "$HOSTS" ] || { echo "no active instances"; exit 1; }
printf "%s active instances, %s probes per pair\n" "$(echo "$HOSTS" | wc -l | tr -d ' ')" "$COUNT"

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15
    -o LogLevel=ERROR)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Executed on each source with `bash -s <self_ip> <count>`; the destination list is appended
# to the script as a heredoc. Prints one CSV fragment per destination:
# dst_region,dst_ip,sent,received,min,avg,max,mdev.
read -r -d '' REMOTE <<'EOF' || true
self=$1; count=$2
probe() {
    out=$(ping -n -q -c "$count" -i 0.2 -W 2 "$2" 2>/dev/null || true)
    tx=$(awk '/transmitted/ {print $1}' <<< "$out")
    rx=$(awk '/transmitted/ {print $4}' <<< "$out")
    # "rtt min/avg/max/mdev = a/b/c/d ms" with an optional ", pipe N" suffix when the RTT
    # exceeds the probe interval: take the token after "= " and split it on "/".
    summary='/rtt|round-trip/ {split($2, v, "[/ ]"); OFS = ","; print v[1], v[2], v[3], v[4]}'
    rtt=$(awk -F'= ' "$summary" <<< "$out")
    echo "$1,$2,${tx:-0},${rx:-0},${rtt:-nan,nan,nan,nan}"
}
export -f probe
export count
grep -v " $self\$" <<'HOSTS' | xargs -P 8 -n 2 bash -c 'probe "$0" "$1"'
EOF

while read -r region ip; do
    (
        "${SSH[@]}" "ubuntu@$ip" bash -s "$ip" "$COUNT" <<< "$REMOTE
$HOSTS
HOSTS" \
            | awk -v src="$region,$ip" '{print src "," $0}' > "$TMP/$ip.csv" \
            || echo "source $ip failed" >&2
    ) &
done <<< "$HOSTS"
wait

mkdir -p "$(dirname "$OUT")"
{
    echo "src_region,src_ip,dst_region,dst_ip,sent,received,min_ms,avg_ms,max_ms,mdev_ms"
    cat "$TMP"/*.csv
} > "$OUT"
echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) directed pairs)"

# Region-to-region summary (median of the pair averages, diagonal = intra-region) and the pairs
# that lost probes, as a quick sanity check.
python3 - "$OUT" <<'PY'
import csv, statistics, sys
from collections import defaultdict
pairs, lossy = defaultdict(list), []
for row in csv.DictReader(open(sys.argv[1])):
    if row["avg_ms"] != "nan":
        pairs[(row["src_region"], row["dst_region"])].append(float(row["avg_ms"]))
    if row["sent"] != row["received"]:
        lossy.append(f'{row["src_ip"]} -> {row["dst_ip"]} ({row["received"]}/{row["sent"]})')
regions = sorted({r for k in pairs for r in k})
print("median RTT (ms) between regions:")
print(" " * 16 + "".join(f"{r[:14]:>16}" for r in regions))
for a in regions:
    print(f"{a[:14]:>16}" + "".join(
        f"{statistics.median(pairs[(a, b)]):>16.1f}" if pairs.get((a, b)) else f"{'-':>16}"
        for b in regions))
print(f"{len(lossy)} pairs lost probes" + (": " + "; ".join(lossy[:10]) if lossy else ""))
PY
