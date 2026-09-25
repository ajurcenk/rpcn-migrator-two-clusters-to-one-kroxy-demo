#!/usr/bin/env bash
# Step 6: check the migrators move data. Destination topics a_* / b_* must grow while the
# producers run, and their newest records must come from the right source (value prefix and
# x-source-cluster header).
source "$(dirname "$0")/lib.sh"
step 6 "check the migrators move data"

ensure_rbtool
rbtool check-data -watch 15s | sed 's/^/  /'
next "./07-check-offsets.sh"
