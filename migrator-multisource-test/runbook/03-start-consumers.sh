#!/usr/bin/env bash
# Step 3: start an application consumer per source in group app-group, reading orders and
# payments with auto-commit. Consumed records go to .state/consumed-source-{a,b}.jsonl.
source "$(dirname "$0")/lib.sh"
step 3 "start consumers on the source clusters"

ensure_rbtool
start_bg consumer-a "$RBTOOL" consume -brokers "$SRC_A_ADDR" -group "$GROUP" -topics orders,payments -log "$STATE_DIR/consumed-source-a.jsonl"
start_bg consumer-b "$RBTOOL" consume -brokers "$SRC_B_ADDR" -group "$GROUP" -topics orders,payments -log "$STATE_DIR/consumed-source-b.jsonl"
sleep 5
info "source A group $GROUP:"; describe_group redpanda-a "$GROUP"
info "source B group $GROUP:"; describe_group redpanda-b "$GROUP"
next "./04-start-proxies.sh"
