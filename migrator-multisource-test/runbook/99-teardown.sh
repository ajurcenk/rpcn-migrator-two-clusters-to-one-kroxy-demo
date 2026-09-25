#!/usr/bin/env bash
# Stop everything the runbook started and delete the containers, their data and .state/.
source "$(dirname "$0")/lib.sh"
step 99 "teardown"

for name in consumer-a consumer-b producer-a producer-b; do
  stop_bg "$name"
done
"${COMPOSE[@]}" down -v
rm -rf "$STATE_DIR"
info "done"
