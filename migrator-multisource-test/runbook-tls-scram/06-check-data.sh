#!/usr/bin/env bash
# Step 6: check the migrators move data. Destination topics a_* / b_* must grow while the
# producers run, and their newest records must come from the right source (value prefix and
# x-source-cluster header). The migrators must also have copied each source's orders-value schema
# to the destination Schema Registry as a_orders-value / b_orders-value (HTTPS + basic auth on
# both ends).
source "$(dirname "$0")/lib.sh"
step 6 "check the migrators move data"

require_secrets
ensure_rbtool
rbtool check-data -watch 15s | sed 's/^/  /'
subjects="$(sr_curl "$SR_DEST_ADDR" /subjects)" || fail "destination schema registry did not answer"
info "destination schema registry subjects: $subjects"
for subj in a_orders-value b_orders-value; do
  grep -q "\"$subj\"" <<<"$subjects" || fail "schema $subj missing on the destination"
done
info "schemas OK: a_orders-value and b_orders-value copied"
next "./07-check-offsets.sh"
