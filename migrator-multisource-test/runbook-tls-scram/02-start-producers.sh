#!/usr/bin/env bash
# Step 2: create orders (3 partitions) and payments (1 partition) on both sources (as admin),
# register an orders-value schema on each source's Schema Registry (HTTPS + basic auth; the
# migrators copy it as a_orders-value / b_orders-value), and start a producer per source, as the
# app user, writing "<source>-<topic>-<n>" at RATE records/s per topic (default 20).
source "$(dirname "$0")/lib.sh"
step 2 "start producers on the source clusters"

require_secrets
ensure_rbtool
for s in A B; do
  rbtool create-topics -source "$s" | sed 's/^/  /'
done
for sr in "A $SR_A_ADDR" "B $SR_B_ADDR"; do
  read -r s addr <<<"$sr"
  schema='{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}'
  id="$(sr_curl "$addr" /subjects/orders-value/versions -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d "$schema")" \
    || fail "could not register orders-value on source $s's schema registry"
  info "source $s: schema orders-value registered $id"
done
as_app start_bg producer-a "$RBTOOL" produce -source A -rate "$RATE"
as_app start_bg producer-b "$RBTOOL" produce -source B -rate "$RATE"
next "./03-start-consumers.sh"
