#!/usr/bin/env bash
# Step 5: start migrator-a (redpanda-a -> kroxylicious-a -> redpanda-dest) and migrator-b. Every
# connection is TLS + SCRAM: input as "migrator" on its source, output as migrator-a / migrator-b
# through its proxy, Schema Registry over HTTPS with basic auth.
source "$(dirname "$0")/lib.sh"
step 5 "start migrators"

require_secrets
is_running producer-a && is_running producer-b || fail "start the producers first (./02-start-producers.sh)"
"${COMPOSE[@]}" up -d migrator-a migrator-b
sleep 15
for m in migrator-a migrator-b; do
  info "$m:"
  "${COMPOSE[@]}" logs --no-color "$m" 2>&1 | grep -E 'created destination topic|schema created|successfully committed' | tail -4 | sed 's/.*msg="\([^"]*\)".*/    \1/'
  if "${COMPOSE[@]}" logs --no-color "$m" 2>&1 | grep -qiE 'SASL_AUTHENTICATION_FAILED|x509:|tls: |401 Unauthorized'; then
    "${COMPOSE[@]}" logs --no-color "$m" 2>&1 | grep -iE 'SASL_AUTHENTICATION_FAILED|x509:|tls: |401' | tail -3 | sed 's/^/    /'
    fail "$m reports TLS or authentication errors"
  fi
done
# The group traffic reaches the filter, i.e. the proxy decrypted it and the SASL session through
# it is authenticated (redpanda-dest would reject group requests on an unauthenticated connection).
for s in a b; do
  info "kroxylicious-$s, latest group rewrites:"
  "${COMPOSE[@]}" logs --no-color "kroxylicious-$s" 2>&1 | grep 'ConsumerGroupPrefixFilter' | grep "app-group" | tail -2 \
    | sed 's/.*\(api=.*\)/    \1/' || info "    none yet (the first group sync runs within 10s)"
done
next "./06-check-data.sh"
