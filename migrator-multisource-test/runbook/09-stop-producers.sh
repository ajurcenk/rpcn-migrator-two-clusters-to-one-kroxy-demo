#!/usr/bin/env bash
# Step 9: stop the source producers. No new data after this point.
source "$(dirname "$0")/lib.sh"
step 9 "stop producers on the source clusters"

stop_bg producer-a
stop_bg producer-b
next "./10-wait-replication-and-stop-migrators.sh"
