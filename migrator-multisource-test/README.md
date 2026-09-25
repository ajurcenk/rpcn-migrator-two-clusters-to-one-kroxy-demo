# Migrator multi-source test

Two Redpanda Migrator pipelines replicate clusters A and B into one destination. A Kroxylicious filter keeps their consumer groups apart by prefixing the group IDs. The task spec is in `../docs/MIGRATOR_MULTI_SOURCE_TEST_PLAN.md`.

## Step 1: consumer-group API inventory

Deliverables:
- `docs/migrator-consumer-group-api-calls.md`
- `docs/findings.md`

```sh
make step1-verify
```

This checks that every key file:line reference in the inventory still matches the migrator source (`MIGRATOR_SRC`, default `$MIGRATOR_SRC`) and the franz-go module cache (`GOMODCACHE`). The reference list is `scripts/step1-refs.txt`.
