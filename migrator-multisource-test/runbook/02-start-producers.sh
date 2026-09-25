#!/usr/bin/env bash
# Step 2: create orders (3 partitions) and payments (1 partition) on both sources and start a
# producer per source writing "<source>-<topic>-<n>" at RATE records/s per topic (default 20).
source "$(dirname "$0")/lib.sh"
step 2 "start producers on the source clusters"

ensure_rbtool
for s in A B; do
  rbtool create-topics -source "$s" | sed 's/^/  /'
done
start_bg producer-a "$RBTOOL" produce -source A -rate "$RATE"
start_bg producer-b "$RBTOOL" produce -source B -rate "$RATE"
next "./03-start-consumers.sh"
