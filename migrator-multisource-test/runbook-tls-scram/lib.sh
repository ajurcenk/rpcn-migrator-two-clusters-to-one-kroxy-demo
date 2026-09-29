# Shared by the TLS/SCRAM runbook step scripts. Source it; don't run it.
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$RUNBOOK_DIR/.." && pwd)"
STATE_DIR="$RUNBOOK_DIR/.state"
RBTOOL="$STATE_DIR/bin/rbtool"
REDPANDA_IMAGE=docker.redpanda.com/redpandadata/redpanda:v26.2.2

# Its own stack: the TLS/SCRAM variant of Step 3 (compose project cgprefix-tls-scram, its own host
# ports), so it can run next to the plaintext runbook or `make step3`.
COMPOSE=(docker compose -f "$PROJECT_DIR/compose/docker-compose.tls-scram.yaml" --profile migrators)

export SRC_A_ADDR="${SRC_A_ADDR:-localhost:19093}"
export SRC_B_ADDR="${SRC_B_ADDR:-localhost:29093}"
export DEST_ADDR="${DEST_ADDR:-localhost:39093}"
PROXY_A_ADDR=localhost:19192 # kroxylicious-a's bootstrap port, published for the auth checks
PROXY_B_ADDR=localhost:29192
GROUP=app-group
RATE="${RATE:-20}" # records per second per topic, per producer

# Certificates (demo CA, one server cert per broker and proxy) and SASL passwords live in .state/;
# the compose file mounts TLS_DIR and reads the passwords from the environment.
export TLS_DIR="$STATE_DIR/tls"
SECRETS_FILE="$STATE_DIR/secrets.env"

mkdir -p "$STATE_DIR/bin" "$STATE_DIR/logs" "$STATE_DIR/pids"

step() { printf '\n=== Step %s: %s ===\n\n' "$1" "$2"; }
info() { printf '  %s\n' "$*"; }
fail() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }
next() { printf '\nNext: %s\n' "$*"; }

stack_running() { [[ -n "$("${COMPOSE[@]}" ps -q 2>/dev/null)" ]]; }

# Generates the demo PKI when missing, or again with FORCE=1 (only while the stack is down: the
# running brokers and proxies would keep the old certificates).
ensure_certs() {
  if [[ -f "$TLS_DIR/ca.crt" && ( -z "${FORCE:-}" || stack_running ) ]]; then
    return
  fi
  info "generating certificates into .state/tls ..."
  rm -rf "$TLS_DIR" && mkdir -p "$TLS_DIR"
  docker run --rm -u "$(id -u):$(id -g)" --entrypoint bash \
    -v "$RUNBOOK_DIR/certs:/scripts:ro,z" -v "$TLS_DIR:/out:z" \
    "$REDPANDA_IMAGE" /scripts/gen-certs.sh | sed 's/^/    /'
}

# Generates one random password per SCRAM user when missing (again with FORCE=1 while the stack
# is down; users are created from these in step 1).
#   sources: admin (checks), app (producers/consumers), migrator (migrator inputs)
#   destination: admin, app (cutover consumers), migrator-a / migrator-b (migrator outputs)
ensure_secrets() {
  if [[ -f "$SECRETS_FILE" && ( -z "${FORCE:-}" || stack_running ) ]]; then
    return
  fi
  info "generating SCRAM passwords into .state/secrets.env ..."
  (
    umask 077
    for var in ADMIN_PASSWORD APP_PASSWORD MIGRATOR_PASSWORD MIGRATOR_A_PASSWORD MIGRATOR_B_PASSWORD; do
      printf '%s=%s\n' "$var" "$(head -c 18 /dev/urandom | base64 | tr -d '/+=')"
    done >"$SECRETS_FILE"
  )
  load_secrets
}

load_secrets() {
  if [[ -f "$SECRETS_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$SECRETS_FILE"
    set +a
  fi
  # rbtool's defaults: TLS with the demo CA, SCRAM as admin. as_app switches to the app user.
  export RB_TLS_CA="$TLS_DIR/ca.crt" RB_SASL_USER=admin RB_SASL_PASS="${ADMIN_PASSWORD:-}" RB_SASL_MECHANISM=SCRAM-SHA-256
}
load_secrets

require_secrets() {
  [[ -f "$TLS_DIR/ca.crt" && -f "$SECRETS_FILE" ]] || fail "no certificates or passwords in .state/; run ./00-setup.sh first"
}

# as_app CMD... runs CMD with rbtool authenticating as the application user.
as_app() { RB_SASL_USER=app RB_SASL_PASS="$APP_PASSWORD" "$@"; }

# Builds rbtool (producers, consumers, checks) when missing or older than its source, or always
# with FORCE=1.
ensure_rbtool() {
  if [[ -n "${FORCE:-}" || ! -x "$RBTOOL" || -n "$(find "$RUNBOOK_DIR/rbtool" -newer "$RBTOOL" -name '*.go' -o -newer "$RBTOOL" -name go.mod)" ]]; then
    info "building rbtool ..."
    (cd "$RUNBOOK_DIR/rbtool" && go build -o "$RBTOOL" .)
  fi
}

rbtool() { "$RBTOOL" "$@"; }

# start_bg NAME CMD... runs CMD in the background with its output in .state/logs/NAME.log.
start_bg() {
  local name="$1"; shift
  if is_running "$name"; then
    info "$name is already running (pid $(cat "$STATE_DIR/pids/$name.pid"))"
    return
  fi
  nohup "$@" >>"$STATE_DIR/logs/$name.log" 2>&1 &
  echo $! >"$STATE_DIR/pids/$name.pid"
  sleep 2
  is_running "$name" || { tail -5 "$STATE_DIR/logs/$name.log" >&2; fail "$name exited right after starting"; }
  info "started $name (pid $!, log .state/logs/$name.log)"
}

# stop_bg NAME sends SIGTERM and waits for a clean exit (consumers commit their final position).
stop_bg() {
  local name="$1" pidfile="$STATE_DIR/pids/$1.pid"
  if ! is_running "$name"; then
    info "$name is not running"
    rm -f "$pidfile"
    return
  fi
  local pid; pid="$(cat "$pidfile")"
  kill -TERM "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$pid" 2>/dev/null && fail "$name (pid $pid) did not stop within 30s"
  rm -f "$pidfile"
  info "stopped $name; last log lines:"
  tail -2 "$STATE_DIR/logs/$name.log" | sed 's/^/    /'
}

is_running() {
  local pidfile="$STATE_DIR/pids/$1.pid"
  [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

# rpk_in SERVICE ARGS... runs rpk inside a Redpanda container over its internal TLS + SASL
# listener, as admin (no local rpk needed).
rpk_in() {
  local svc="$1"; shift
  "${COMPOSE[@]}" exec -T "$svc" rpk "$@" \
    -X brokers="$svc:9092" -X tls.enabled=true -X tls.ca=/certs/ca.crt \
    -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass="$ADMIN_PASSWORD"
}

# describe_group SERVICE GROUP shows a group from inside a Redpanda container.
describe_group() {
  rpk_in "$1" group describe "$2" 2>&1 | sed 's/^/    /'
}

# create_user SERVICE USER PASSWORD creates a SCRAM-SHA-256 user through the Admin API. Creating
# an existing user with the same password succeeds (re-running step 1); with a different password
# Redpanda answers "User already exists", and the password is reset instead.
create_user() {
  local svc="$1" user="$2" pass="$3" out
  if out="$("${COMPOSE[@]}" exec -T "$svc" rpk security user create "$user" -p "$pass" --mechanism SCRAM-SHA-256 2>&1)"; then
    info "$svc: created user $user"
  elif grep -qi 'already exists' <<<"$out"; then
    "${COMPOSE[@]}" exec -T "$svc" rpk security user update "$user" --new-password "$pass" --mechanism SCRAM-SHA-256 >/dev/null
    info "$svc: user $user already exists, password reset"
  else
    printf '%s\n' "$out" >&2
    fail "could not create user $user on $svc"
  fi
}

# sr_curl ADDR PATH [CURL ARGS...] calls a Schema Registry over HTTPS as admin.
sr_curl() {
  local addr="$1" path="$2"; shift 2
  curl -sf --cacert "$TLS_DIR/ca.crt" -u "admin:$ADMIN_PASSWORD" "$@" "https://$addr$path"
}
SR_A_ADDR=localhost:18082
SR_B_ADDR=localhost:28082
SR_DEST_ADDR=localhost:38082

# retry TIMEOUT_SECONDS CMD... reruns CMD every 5s until it succeeds.
retry() {
  local timeout="$1"; shift
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    ((SECONDS < deadline)) || return 1
    info "not there yet, retrying in 5s ..."
    sleep 5
  done
}
