#!/usr/bin/env bash
# Step 8: stop the source consumers. Each commits its final position and leaves the group, so
# app-group becomes Empty. The migrators translate that final commit on their next sync (exact
# translation, since the group is Empty).
source "$(dirname "$0")/lib.sh"
step 8 "stop consumers on the source clusters"

stop_bg consumer-a
stop_bg consumer-b
info "source A group $GROUP:"; describe_group redpanda-a "$GROUP"
info "source B group $GROUP:"; describe_group redpanda-b "$GROUP"
next "./09-stop-producers.sh"
