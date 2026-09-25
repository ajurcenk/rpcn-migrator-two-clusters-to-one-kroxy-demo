# Shared by the runbook step scripts. Source it; don't run it.
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$RUNBOOK_DIR/.." && pwd)"
STATE_DIR="$RUNBOOK_DIR/.state"
RBTOOL="$STATE_DIR/bin/rbtool"

# The runbook reuses the Step 3 stack: 3 Redpanda clusters, one proxy per source, one migrator per
# source. Don't run `make step3` at the same time; they share containers and ports.
COMPOSE=(docker compose -f "$PROJECT_DIR/compose/docker-compose.step3.yaml" --profile migrators)

export SRC_A_ADDR="${SRC_A_ADDR:-localhost:19092}"
export SRC_B_ADDR="${SRC_B_ADDR:-localhost:29092}"
export DEST_ADDR="${DEST_ADDR:-localhost:39092}"
GROUP=app-group
RATE="${RATE:-20}" # records per second per topic, per producer

mkdir -p "$STATE_DIR/bin" "$STATE_DIR/logs" "$STATE_DIR/pids"

step() { printf '\n=== Step %s: %s ===\n\n' "$1" "$2"; }
info() { printf '  %s\n' "$*"; }
fail() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }
next() { printf '\nNext: %s\n' "$*"; }

# Builds rbtool (producers, consumers, checks) when missing or older than its source.
ensure_rbtool() {
  if [[ ! -x "$RBTOOL" || -n "$(find "$RUNBOOK_DIR/rbtool" -newer "$RBTOOL" -name '*.go' -o -newer "$RBTOOL" -name go.mod)" ]]; then
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

# describe_group SERVICE GROUP shows a group from inside a Redpanda container (no local rpk needed).
describe_group() {
  "${COMPOSE[@]}" exec -T "$1" rpk group describe "$2" 2>&1 | sed 's/^/    /'
}

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
