#!/usr/bin/env bash
# Step 10: wait until the destination holds every source record and every app-group position is
# translated exactly, then stop the migrators. Stopping them before this point would freeze an
# imprecise position; if it doesn't converge, the migrators are left running (see
# docs/findings.md, "Known issues").
source "$(dirname "$0")/lib.sh"
step 10 "wait until all data is replicated, then stop the migrators"

ensure_rbtool
is_running producer-a || is_running producer-b && fail "producers still running; run ./09-stop-producers.sh first"
is_running consumer-a || is_running consumer-b && fail "source consumers still running; run ./08-stop-consumers.sh first"

info "waiting for all records ..."
retry 180 rbtool check-data -caught-up | sed 's/^/  /' || fail "destination did not catch up within 180s; migrators left running"
info "waiting for exact offset translation ..."
retry 120 rbtool check-offsets -group "$GROUP" -exact | sed 's/^/  /' || fail "offsets not exact within 120s; migrators left running"

"${COMPOSE[@]}" stop migrator-a migrator-b
info "migrators stopped"
next "./11-cutover-consumers.sh"
