#!/usr/bin/env bash
# Step 4: start one Kroxylicious proxy per source in front of redpanda-dest. kroxylicious-a stores
# consumer groups as a_<group>, kroxylicious-b as b_<group>. Builds the proxy image if needed.
source "$(dirname "$0")/lib.sh"
step 4 "start proxies"

"${COMPOSE[@]}" up -d --build --wait kroxylicious-a kroxylicious-b
info "kroxylicious-a: prefix a_, metrics http://localhost:19190/metrics"
info "kroxylicious-b: prefix b_, metrics http://localhost:29190/metrics"
next "./05-start-migrators.sh"
