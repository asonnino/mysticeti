#!/usr/bin/env bash
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

# Run the simulator campaign (scripts/eval/sim_campaign.py) on a throwaway EC2 instance,
# fetch the per-run summaries, and terminate the instance.
#
# Needs the AWS CLI with credentials, and the orchestrator's SSH key (~/.ssh/aws by default),
# so run it outside any sandbox:
#
#   scripts/eval/sim-remote.sh [results-dir]      # default: results/sim-<git sha>
#   scripts/eval/sim-remote.sh fetch <ip> [results-dir]   # only fetch + parse from a running box
#
# A failed fetch never terminates the instance: the script leaves it running (until its
# dead-man shutdown) and prints the `fetch` command to retry.
#
# Knobs (environment): REGION, INSTANCE_TYPE, PARALLEL (concurrent runs), NAME (tag, key pair
# and security group name), KEY (private key path), DEADMAN_MINUTES (the instance powers off by
# itself after this long, even if this script dies; shutdown terminates it).
set -euo pipefail

REGION=${REGION:-us-east-1}
INSTANCE_TYPE=${INSTANCE_TYPE:-m6i.8xlarge}
PARALLEL=${PARALLEL:-32}
NAME=${NAME:-hydrozoan-sim}
KEY=${KEY:-$HOME/.ssh/aws}
DEADMAN_MINUTES=${DEADMAN_MINUTES:-180}
VOLUME_GB=${VOLUME_GB:-250}

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SHA=$(git -C "$ROOT" rev-parse --short HEAD)
MODE=run
if [ "${1:-}" = fetch ]; then
    MODE=fetch
    IP=${2:?usage: sim-remote.sh fetch <ip> [results-dir]}
    shift 2
fi
RESULTS=${1:-$ROOT/results/sim-$SHA}
CONFIGS=$ROOT/data/sim/configs
PYTHON=$ROOT/scripts/.venv-eval/bin/python
[ -x "$PYTHON" ] || PYTHON=python3

aws() { command aws --region "$REGION" --output text "$@"; }
log() { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }

SSH_OPTIONS=(-i "$KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR)
ssh_run() { ssh "${SSH_OPTIONS[@]}" "ubuntu@$IP" "$@"; }

fetch() {
    log "Fetching results to $RESULTS"
    mkdir -p "$RESULTS"
    local attempt
    for attempt in 1 2 3 4 5 6 7 8 9 10; do
        if rsync -az -e "ssh ${SSH_OPTIONS[*]}" --include '*/' --include 'metrics-A.prom' \
            --include 'config.yaml' --include 'meta.yaml' --include '*.log' --exclude '*' \
            "ubuntu@$IP:mysticeti/results/" "$RESULTS/"; then
            rm -f "$RESULTS/tracing.log"
            return 0
        fi
        log "fetch attempt $attempt failed; retrying in 30 s"
        sleep 30
    done
    return 1
}

if [ "$MODE" = fetch ]; then
    fetch
    "$PYTHON" "$ROOT/scripts/eval/sim_campaign.py" parse "$RESULTS"
    exit 0
fi

rm -rf "$CONFIGS"
"$PYTHON" "$ROOT/scripts/eval/sim_campaign.py" generate --out "$CONFIGS"

log "Ubuntu 24.04 image in $REGION"
IMAGE=$(aws ec2 describe-images --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*" \
    "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId')

log "Key pair $NAME"
if ! aws ec2 describe-key-pairs --key-names "$NAME" >/dev/null 2>&1; then
    PUBLIC_KEY=$(mktemp)
    if [ -f "$KEY.pub" ]; then
        cat "$KEY.pub" >"$PUBLIC_KEY"
    else
        ssh-keygen -y -f "$KEY" >"$PUBLIC_KEY"
    fi
    aws ec2 import-key-pair --key-name "$NAME" \
        --public-key-material "fileb://$PUBLIC_KEY" >/dev/null
    rm -f "$PUBLIC_KEY"
fi

log "Security group $NAME (SSH only)"
SECURITY_GROUP=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$NAME" \
    --query 'SecurityGroups[0].GroupId')
if [ "$SECURITY_GROUP" = "None" ] || [ -z "$SECURITY_GROUP" ]; then
    SECURITY_GROUP=$(aws ec2 create-security-group --group-name "$NAME" \
        --description "throwaway simulator box" --query GroupId)
    MY_IP=$(curl -fs https://checkip.amazonaws.com || true)
    CIDR=${MY_IP:+$MY_IP/32}
    aws ec2 authorize-security-group-ingress --group-id "$SECURITY_GROUP" --protocol tcp \
        --port 22 --cidr "${CIDR:-0.0.0.0/0}" >/dev/null
fi

log "Launching $INSTANCE_TYPE"
INSTANCE=$(aws ec2 run-instances --image-id "$IMAGE" --instance-type "$INSTANCE_TYPE" \
    --key-name "$NAME" --security-group-ids "$SECURITY_GROUP" \
    --block-device-mappings \
    "DeviceName=/dev/sda1,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId')
KEEP_INSTANCE=0
terminate() {
    if [ "$KEEP_INSTANCE" = 1 ]; then
        log "Leaving $INSTANCE running at $IP (dead-man shutdown in $DEADMAN_MINUTES min)."
        log "Retry with: $0 fetch $IP $RESULTS"
        return
    fi
    log "Terminating $INSTANCE"
    aws ec2 terminate-instances --instance-ids "$INSTANCE" >/dev/null || true
}
trap terminate EXIT
aws ec2 wait instance-running --instance-ids "$INSTANCE"
IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE" \
    --query 'Reservations[0].Instances[0].PublicIpAddress')
log "Instance $INSTANCE at $IP"

until ssh_run true 2>/dev/null; do sleep 5; done
ssh_run "sudo shutdown -h +$DEADMAN_MINUTES" >/dev/null 2>&1 || true

log "Uploading the working tree"
rsync -az -e "ssh ${SSH_OPTIONS[*]}" \
    --exclude target --exclude .git --exclude lean/.lake --exclude plots --exclude results \
    --exclude 'scripts/.venv*' --exclude .claude --exclude __pycache__ --exclude data \
    "$ROOT/" "ubuntu@$IP:mysticeti/"
ssh_run "mkdir -p mysticeti/data/sim/configs"
rsync -az -e "ssh ${SSH_OPTIONS[*]}" "$CONFIGS/" "ubuntu@$IP:mysticeti/data/sim/configs/"

log "Building and running $(find "$CONFIGS" -name '*.yaml' | wc -l | tr -d ' ') runs" \
    "($PARALLEL in parallel)"
ssh_run "PARALLEL=$PARALLEL bash -s" <<'REMOTE'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq >/dev/null
sudo apt-get install -y -qq build-essential pkg-config libssl-dev >/dev/null
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y -q >/dev/null
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
cd mysticeti
cargo build --release --bin replica 2>&1 | tail -1
mkdir -p results
find data/sim/configs -name '*.yaml' | sort | xargs -P "$PARALLEL" -I{} bash -c '
    name=$(basename {} .yaml)
    if ./target/release/replica simulate --config-path {} --output-dir "results/$name" \
        >"results/$name.log" 2>&1; then echo "done   $name"; else echo "FAILED $name"; fi'
REMOTE

KEEP_INSTANCE=1
fetch
KEEP_INSTANCE=0

"$PYTHON" "$ROOT/scripts/eval/sim_campaign.py" parse "$RESULTS"
