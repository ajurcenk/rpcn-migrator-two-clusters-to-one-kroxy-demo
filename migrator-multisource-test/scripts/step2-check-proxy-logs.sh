#!/usr/bin/env bash
# Step 2 acceptance: the proxy's DEBUG log shows a group ID rewrite for every destination-side API
# from docs/migrator-consumer-group-api-calls.md, in both its legacy and batched wire shapes, and
# no response ever came back without the prefix.
set -euo pipefail

compose_file="${1:?usage: $0 <compose file>}"
logs="$(docker compose -f "$compose_file" logs --no-color kroxylicious 2>&1)"

required=(
  'api=FIND_COORDINATOR version=[0-3] request'           # single Key
  'api=FIND_COORDINATOR version=[4-6] request'           # CoordinatorKeys[]
  'api=FIND_COORDINATOR version=[4-6] response'          # Coordinators[].Key stripped
  'api=OFFSET_FETCH version=[0-7] request'               # single GroupId
  'api=OFFSET_FETCH version=([89]|10) request'           # Groups[]
  'api=OFFSET_FETCH version=([89]|10) response'          # Groups[].GroupId stripped
  'api=OFFSET_COMMIT version=[0-9]+ request'
)

fail=0
for pattern in "${required[@]}"; do
  n=$(grep -cE "$pattern" <<<"$logs" || true)
  if [[ "$n" -eq 0 ]]; then
    echo "MISSING rewrite log: $pattern"
    fail=1
  else
    printf '%5d x %s\n' "$n" "$pattern"
  fi
done

if grep -q "lacks prefix" <<<"$logs"; then
  echo "UNEXPECTED: response group without prefix:"
  grep "lacks prefix" <<<"$logs" | head -5
  fail=1
fi

[[ $fail -eq 0 ]] && echo "step2 proxy log check: OK"
exit $fail
