#!/usr/bin/env bash
# Verifies that the file:line references in docs/migrator-consumer-group-api-calls.md
# still point at the expected code. Fails if the migrator or franz-go sources drift.
set -euo pipefail

CONNECT="${MIGRATOR_SRC:?set MIGRATOR_SRC to a Redpanda Connect v4.100.0 checkout}/connect"
MODCACHE="${GOMODCACHE:-$(go env GOMODCACHE)}"
declare -A PREFIX=(
  [CONNECT]="$CONNECT"
  [MIG]="$CONNECT/internal/impl/redpanda/migrator"
  [KAFKA]="$CONNECT/internal/impl/kafka"
  [KGO]="$MODCACHE/github.com/twmb/franz-go@v1.20.7/pkg/kgo"
  [KADM]="$MODCACHE/github.com/twmb/franz-go/pkg/kadm@v1.17.2"
  [KMSG]="$MODCACHE/github.com/twmb/franz-go/pkg/kmsg@v1.12.0"
)

fail=0; n=0
while IFS='|' read -r ref line want; do
  [[ -z "$ref" || "$ref" == \#* ]] && continue
  n=$((n+1))
  file="${PREFIX[${ref%%/*}]}/${ref#*/}"
  if [[ ! -f "$file" ]]; then echo "MISSING $file"; fail=1; continue; fi
  if [[ "$line" == "*" ]]; then
    grep -qF -- "$want" "$file" || { echo "FAIL $ref: '$want' not found"; fail=1; }
  else
    got="$(sed -n "${line}p" "$file")"
    [[ "$got" == *"$want"* ]] || { echo "FAIL $ref:$line: want '$want', got '$got'"; fail=1; }
  fi
done < "$(dirname "$0")/step1-refs.txt"

[[ $fail -eq 0 ]] && echo "step1-verify: all $n references OK"
exit $fail
