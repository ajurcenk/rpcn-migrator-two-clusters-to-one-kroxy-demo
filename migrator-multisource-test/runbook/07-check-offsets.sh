#!/usr/bin/env bash
# Step 7: check the migrators translate app-group offsets while the source consumers are active.
# Each translated position is mapped back to a source offset through the x-source-offset header.
# Expected: never AHEAD. "behind" is normal here: the source keeps committing between the
# migrator's 10s sync cycles, and active groups get timestamp-based translation.
source "$(dirname "$0")/lib.sh"
step 7 "check the migrators translate offsets"

ensure_rbtool
retry 60 rbtool check-offsets -group "$GROUP" | sed 's/^/  /' || fail "offsets not translated within 60s"
next "./08-stop-consumers.sh"
