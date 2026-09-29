#!/usr/bin/env bash
# Stop everything the TLS/SCRAM runbook started and delete the containers, their data and .state/
# (including the certificates and passwords; 00-setup.sh or step 1 generates new ones).
source "$(dirname "$0")/lib.sh"
step 99 "teardown"

for name in consumer-a consumer-b producer-a producer-b; do
  stop_bg "$name"
done
"${COMPOSE[@]}" down -v
rm -rf "$STATE_DIR"
info "done"
