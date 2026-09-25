#!/usr/bin/env bash
# Step 5: start migrator-a (redpanda-a -> kroxylicious-a -> redpanda-dest) and migrator-b.
source "$(dirname "$0")/lib.sh"
step 5 "start migrators"

is_running producer-a && is_running producer-b || fail "start the producers first (./02-start-producers.sh)"
# DEST_BROKERS must be empty so each migrator writes through its own proxy (the negative control
# sets it to bypass them).
DEST_BROKERS= "${COMPOSE[@]}" up -d migrator-a migrator-b
sleep 15
for m in migrator-a migrator-b; do
  info "$m:"
  "${COMPOSE[@]}" logs --no-color "$m" 2>&1 | grep -E 'created destination topic|schema created|successfully committed' | tail -4 | sed 's/.*msg="\([^"]*\)".*/    \1/'
done
next "./06-check-data.sh"
