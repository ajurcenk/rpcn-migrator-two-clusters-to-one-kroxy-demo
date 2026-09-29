#!/usr/bin/env bash
# Step 4: start one Kroxylicious proxy per source in front of redpanda-dest. kroxylicious-a stores
# consumer groups as a_<group>, kroxylicious-b as b_<group>. Each terminates client TLS with its
# own certificate, connects to redpanda-dest over TLS, and passes SASL through, so the migrators
# authenticate against redpanda-dest itself. Builds the proxy image if needed.
source "$(dirname "$0")/lib.sh"
step 4 "start proxies (TLS both sides, SASL passthrough)"

require_secrets
ensure_rbtool
"${COMPOSE[@]}" up -d --build --wait kroxylicious-a kroxylicious-b

# Through each proxy: the migrator's own destination user must get in; plaintext, no SASL, a
# wrong password (refused by redpanda-dest, relayed by the proxy) and an untrusted CA must not.
info "checking each proxy enforces TLS and relays SCRAM to redpanda-dest ..."
retry 30 rbtool check-auth -endpoints "kroxylicious-a=$PROXY_A_ADDR@migrator-a:$MIGRATOR_A_PASSWORD,kroxylicious-b=$PROXY_B_ADDR@migrator-b:$MIGRATOR_B_PASSWORD" | sed 's/^/  /' \
  || fail "a proxy does not enforce TLS + SCRAM as expected"
info ""
info "kroxylicious-a: prefix a_, TLS on kroxylicious-a:9192 (host $PROXY_A_ADDR), metrics http://localhost:19191/metrics"
info "kroxylicious-b: prefix b_, TLS on kroxylicious-b:9192 (host $PROXY_B_ADDR), metrics http://localhost:29191/metrics"
next "./05-start-migrators.sh"
