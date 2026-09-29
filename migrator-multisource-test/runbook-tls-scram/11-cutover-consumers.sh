#!/usr/bin/env bash
# Step 11: move the consumers to the destination. The source A application now consumes
# a_orders / a_payments as group a_app-group; source B's as b_app-group. They run until they
# have been idle for 15s. They connect straight to redpanda-dest over TLS as the SCRAM user app. Then each partition is checked: the first record read on the destination
# must be the source record at the source group's final committed offset (no gaps, no re-reads),
# and the consumers must have read to the end. Finally, the source and destination consumer logs
# together must contain every source record exactly once.
source "$(dirname "$0")/lib.sh"
step 11 "move consumers to the destination and check where they resume"

require_secrets
ensure_rbtool
running="$("${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -c '^migrator-' || true)"
[[ "$running" -eq 0 ]] || fail "migrators still running; run ./10-wait-replication-and-stop-migrators.sh first (their commits would fight the consumers: UNKNOWN_MEMBER_ID)"

rm -f "$STATE_DIR"/consumed-dest-{a,b}.jsonl
info "consuming on the destination until idle for 15s ..."
as_app "$RBTOOL" consume -brokers "$DEST_ADDR" -group a_$GROUP -topics a_orders,a_payments -log "$STATE_DIR/consumed-dest-a.jsonl" -until-idle 15s >"$STATE_DIR/logs/dest-consumer-a.log" 2>&1 &
pa=$!
as_app "$RBTOOL" consume -brokers "$DEST_ADDR" -group b_$GROUP -topics b_orders,b_payments -log "$STATE_DIR/consumed-dest-b.jsonl" -until-idle 15s >"$STATE_DIR/logs/dest-consumer-b.log" 2>&1 &
pb=$!
wait "$pa" "$pb"
tail -1 "$STATE_DIR/logs/dest-consumer-a.log" | sed 's/^/  /'
tail -1 "$STATE_DIR/logs/dest-consumer-b.log" | sed 's/^/  /'

rbtool verify-cutover -group "$GROUP" -log "$STATE_DIR/consumed-dest-a.jsonl,$STATE_DIR/consumed-dest-b.jsonl" | sed 's/^/  /'

# Every source record consumed exactly once across the whole migration (source consumers from
# step 3 plus destination consumers above). ALLOW_DUPLICATES=1 reports duplicates without failing.
info ""
info "checking every record was consumed exactly once (source + destination) ..."
rbtool check-duplicates \
  -source-log "A=$STATE_DIR/consumed-source-a.jsonl,B=$STATE_DIR/consumed-source-b.jsonl" \
  -dest-log "$STATE_DIR/consumed-dest-a.jsonl,$STATE_DIR/consumed-dest-b.jsonl" \
  ${ALLOW_DUPLICATES:+-allow-duplicates} | sed 's/^/  /'
next "./99-teardown.sh when done"
