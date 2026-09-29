#!/usr/bin/env bash
# Setup: prepare every artifact the TLS/SCRAM runbook needs, before step 1. Safe to re-run: it
# only builds, pulls or generates what's missing or out of date. FORCE=1 rebuilds everything (and
# regenerates certificates and passwords, unless the stack is running); FILTER_TESTS=1 also runs
# the proxy filter's unit tests before building its image.
#
#   1. prerequisites: docker, docker compose, go >= 1.26
#   2. container images: Redpanda, Redpanda Connect (migrator), and the proxy image's build bases
#   3. proxy image cgprefix-proxy:0.24.0 (Kroxylicious 0.24.0 + the ConsumerGroupPrefix filter JAR)
#   4. rbtool (the runbook's producers, consumers and checks, with TLS + SCRAM)
#   5. TLS certificates (demo CA, brokers, proxies) and SCRAM passwords, in .state/
#   6. validation: migrator configs (lint), proxy configs, the compose file
#   7. warnings: host ports in use, a stack already running
source "$(dirname "$0")/lib.sh"
step 0 "setup"

CONNECT_IMAGE=docker.redpanda.com/redpandadata/connect:4.100.0
PROXY_IMAGE=cgprefix-proxy:0.24.0
BUILD_IMAGES=(maven:3.9-eclipse-temurin-21 quay.io/kroxylicious/proxy:0.24.0)
FILTER_DIR="$PROJECT_DIR/kroxylicious-filter"
HOST_PORTS=(19093 29093 39093 18082 28082 38082 19645 29645 39645 19191 29191 19192 29192)
warnings=0
warn() { printf '  WARNING: %s\n' "$*"; warnings=$((warnings + 1)); }
ok() { printf '  ok    %s\n' "$*"; }
did() { printf '  done  %s\n' "$*"; }

# ---- 1. prerequisites ----
info "1. prerequisites"
command -v docker >/dev/null || fail "docker not found"
docker info >/dev/null 2>&1 || fail "docker daemon not reachable"
docker compose version >/dev/null 2>&1 || fail "docker compose (v2) not found"
ok "docker $(docker version --format '{{.Server.Version}}'), $(docker compose version --short 2>/dev/null | sed 's/^/compose /')"
command -v go >/dev/null || fail "go not found (needed to build rbtool)"
gover="$(go env GOVERSION)"                     # e.g. go1.26.0
IFS=. read -r gomajor gominor _ <<<"${gover#go}"
((gomajor > 1 || (gomajor == 1 && gominor >= 26))) || fail "$gover is too old; rbtool needs go 1.26+"
ok "$gover"

# ---- 2. images ----
info "2. container images"
for image in "$REDPANDA_IMAGE" "$CONNECT_IMAGE" "${BUILD_IMAGES[@]}"; do
  if [[ -z "${FORCE:-}" ]] && docker image inspect "$image" >/dev/null 2>&1; then
    ok "$image"
  else
    docker pull -q "$image" >/dev/null || fail "could not pull $image"
    did "pulled $image"
  fi
done
docker run --rm "$CONNECT_IMAGE" list inputs 2>/dev/null | grep -q redpanda_migrator \
  || fail "$CONNECT_IMAGE has no redpanda_migrator input"
ok "$CONNECT_IMAGE provides redpanda_migrator ($(docker run --rm "$CONNECT_IMAGE" --version 2>/dev/null | head -1))"

# ---- 3. proxy image ----
info "3. proxy image $PROXY_IMAGE"
newest_src="$(find "$FILTER_DIR/src" "$FILTER_DIR/pom.xml" "$FILTER_DIR/Dockerfile" "$FILTER_DIR/log4j2.yaml" -type f -printf '%T@\n' | sort -n | tail -1)"
image_created="$(docker image inspect -f '{{.Created}}' "$PROXY_IMAGE" 2>/dev/null || true)"
if [[ -n "${FORCE:-}" || -z "$image_created" ]] || (( $(date -d "$image_created" +%s) < ${newest_src%.*} )); then
  if [[ -n "${FILTER_TESTS:-}" ]]; then
    info "   running the filter unit tests (make filter-test) ..."
    make -C "$PROJECT_DIR" -s filter-test >"$STATE_DIR/logs/filter-test.log" 2>&1 \
      || { tail -20 "$STATE_DIR/logs/filter-test.log"; fail "filter unit tests failed (.state/logs/filter-test.log)"; }
    did "filter unit tests: $(grep -oE 'Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+' "$STATE_DIR/logs/filter-test.log" | tail -1)"
  fi
  info "   building (Maven compiles the filter inside the image build; first build takes a few minutes) ..."
  docker build -q -t "$PROXY_IMAGE" "$FILTER_DIR" >"$STATE_DIR/logs/proxy-image-build.log" 2>&1 \
    || { tail -20 "$STATE_DIR/logs/proxy-image-build.log"; fail "proxy image build failed (.state/logs/proxy-image-build.log)"; }
  did "built $PROXY_IMAGE"
else
  ok "$PROXY_IMAGE is up to date with kroxylicious-filter/"
fi
docker run --rm --entrypoint ls "$PROXY_IMAGE" /opt/kroxylicious/classpath-plugins/consumer-group-prefix/consumer-group-prefix.jar >/dev/null 2>&1 \
  || fail "$PROXY_IMAGE does not contain the filter JAR"
ok "filter JAR present in $PROXY_IMAGE"

# ---- 4. rbtool ----
info "4. rbtool"
ensure_rbtool
ok "$( ("$RBTOOL" 2>&1 || true) | head -1 | sed 's/^usage: //')"

# ---- 5. certificates and passwords ----
info "5. TLS certificates and SCRAM passwords"
if [[ -n "${FORCE:-}" ]] && stack_running; then
  warn "FORCE=1 but the stack is running: keeping the existing certificates and passwords"
fi
ensure_certs
ensure_secrets
for name in redpanda-a redpanda-b redpanda-dest kroxylicious-a kroxylicious-b; do
  openssl_out="$(docker run --rm --entrypoint openssl -v "$TLS_DIR:/certs:ro,z" "$REDPANDA_IMAGE" \
    verify -CAfile /certs/ca.crt "/certs/$name.crt" 2>&1)" || fail "certificate $name.crt does not verify: $openssl_out"
done
ok "certificates in .state/tls verify against the demo CA (ca.crt)"
ok "passwords in .state/secrets.env: $(cut -d= -f1 "$SECRETS_FILE" | tr '\n' ' ')"

# ---- 6. validation ----
info "6. validation"
for m in migrator-a migrator-b; do
  docker run --rm -e SRC_SASL_PASSWORD="$MIGRATOR_PASSWORD" -e DEST_SASL_PASSWORD="$MIGRATOR_A_PASSWORD" \
    -v "$PROJECT_DIR/migrator/tls-scram:/cfg:ro,z" "$CONNECT_IMAGE" lint "/cfg/$m.yaml" >/dev/null 2>"$STATE_DIR/logs/lint-$m.log" \
    || { cat "$STATE_DIR/logs/lint-$m.log"; fail "migrator/tls-scram/$m.yaml does not lint"; }
  ok "migrator/tls-scram/$m.yaml lints"
done
for s in a b; do
  grep -q "privateKeyFile: /certs/kroxylicious-$s.key" "$PROJECT_DIR/proxy/config-tls-scram-$s.yaml" \
    || fail "proxy/config-tls-scram-$s.yaml has no gateway TLS key for kroxylicious-$s"
done
ok "proxy/config-tls-scram-{a,b}.yaml configure TLS on both sides"
"${COMPOSE[@]}" config --quiet || fail "compose/docker-compose.tls-scram.yaml is invalid"
ok "compose/docker-compose.tls-scram.yaml is valid"

# ---- 7. environment ----
info "7. environment"
running="$("${COMPOSE[@]}" ps -q 2>/dev/null | wc -l)"
if ((running > 0)); then
  warn "$running containers of this stack are already running (a previous TLS/SCRAM run). Run ./99-teardown.sh to start clean."
else
  busy=()
  for port in "${HOST_PORTS[@]}"; do
    ss -ltnH "sport = :$port" 2>/dev/null | grep -q . && busy+=("$port")
  done
  if ((${#busy[@]} > 0)); then
    warn "host ports in use by something else: ${busy[*]} (step 1 or 4 will fail to bind them)"
  else
    ok "host ports free: ${HOST_PORTS[*]}"
  fi
fi
for name in producer-a producer-b consumer-a consumer-b; do
  is_running "$name" && warn "$name from a previous run is still running (./99-teardown.sh stops it)"
done

if ((warnings > 0)); then
  printf '\nSetup finished with %d warning(s).\n' "$warnings"
else
  printf '\nSetup complete.\n'
fi
next "./01-start-clusters.sh"
