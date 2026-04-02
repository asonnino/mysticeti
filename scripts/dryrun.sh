#!/usr/bin/env bash
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

# bash run.sh

export RUST_LOG=warn,dag::consensus=trace,dag::sync::net_sync=DEBUG,dag::core=DEBUG

tmux kill-server || true

CMD="cargo run --bin validator -- dry-run --committee-size 4"
tmux new -d -s "v0" "$CMD --authority 0 > v0.log.ansi"
tmux new -d -s "v1" "$CMD --authority 1 > v1.log.ansi"
tmux new -d -s "v2" "$CMD --authority 2 > v2.log.ansi"
tmux new -d -s "v3" "$CMD --authority 3 > v3.log.ansi"

sleep 60
tmux kill-server
