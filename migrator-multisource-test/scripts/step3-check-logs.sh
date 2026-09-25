#!/usr/bin/env bash
# Step 3 health checks from container logs.
#   migrators: no WARN/ERROR lines at all (this covers every group-sync error path listed in
#              docs/migrator-consumer-group-api-calls.md), and at least one successful group commit.
#   proxies:   each proxy rewrote app-group to its own prefix only, and never saw an
#              unprefixed response group.
# With "negative" as the second argument only the migrator logs are printed, not checked.
set -euo pipefail

compose="docker compose -f ${1:?usage: $0 <compose file> [negative]} --profile migrators"
mode="${2:-proxy}"
fail=0

for m in migrator-a migrator-b; do
  logs="$($compose logs --no-color "$m" 2>&1)"
  bad="$(grep -E 'level=(warn|error)' <<<"$logs" || true)"
  if [[ "$mode" == negative ]]; then
    echo "== $m WARN/ERROR lines (negative control, informational):"
    echo "${bad:-  none}"
    continue
  fi
  ok=1
  if [[ -n "$bad" ]]; then
    echo "FAIL $m logged WARN/ERROR:"; echo "$bad" | cut -c1-400 | head -10; ok=0
  fi
  if ! grep -q 'Consumer group migration: successfully committed' <<<"$logs"; then
    echo "FAIL $m never committed consumer group offsets"; ok=0
  fi
  if [[ $ok -eq 1 ]]; then echo "OK   $m: no WARN/ERROR, group offsets committed"; else fail=1; fi
done

if [[ "$mode" != negative ]]; then
  for s in a b; do
    other=$([[ $s == a ]] && echo b || echo a)
    logs="$($compose logs --no-color "kroxylicious-$s" 2>&1)"
    n=$(grep -cE "request group 'app-group' -> '${s}_app-group'" <<<"$logs" || true)
    wrong=$(grep -cE -- "-> '${other}_" <<<"$logs" || true)
    ok=1
    if [[ "$n" -eq 0 ]]; then echo "FAIL kroxylicious-$s: no app-group -> ${s}_app-group rewrites"; ok=0; fi
    if [[ "$wrong" -ne 0 ]]; then echo "FAIL kroxylicious-$s: $wrong rewrites to prefix ${other}_"; ok=0; fi
    if grep -q "lacks prefix" <<<"$logs"; then echo "FAIL kroxylicious-$s: unprefixed response group"; ok=0; fi
    if [[ $ok -eq 1 ]]; then echo "OK   kroxylicious-$s: $n request rewrites, none to ${other}_"; else fail=1; fi
  done
fi
exit $fail
