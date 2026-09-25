#!/usr/bin/env bash
# Step 1: start the source clusters (redpanda-a, redpanda-b) and the destination (redpanda-dest).
source "$(dirname "$0")/lib.sh"
step 1 "start clusters"

"${COMPOSE[@]}" up -d --wait redpanda-a redpanda-b redpanda-dest
info "source A:    $SRC_A_ADDR   (schema registry localhost:18081)"
info "source B:    $SRC_B_ADDR   (schema registry localhost:28081)"
info "destination: $DEST_ADDR   (schema registry localhost:38081)"
next "./02-start-producers.sh"
