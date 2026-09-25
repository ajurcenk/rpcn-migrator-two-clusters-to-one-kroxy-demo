# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

`docs/MIGRATOR_MULTI_SOURCE_TEST_PLAN.md` is the task specification. Steps 1-3 are implemented in `migrator-multisource-test/`. Results, deviations from the spec, and open questions are in `migrator-multisource-test/docs/findings.md`; read it before changing anything.

## Commands (run from `migrator-multisource-test/`)

| Command | What it does |
|---|---|
| `make step1-verify` | Checks the file:line references in the Step 1 inventory against the migrator source, the franz-go module cache, and `tests/go.mod` |
| `make filter-test` | Filter unit tests (Maven in `maven:3.9-eclipse-temurin-21`, `~/.m2` mounted; no local Maven) |
| `make step2` | Unit tests + proxy integration test on a fresh stack, then tear down |
| `make step3` / `make step3-negative` | End-to-end run with / without proxies, then tear down |
| `runbook/01-…sh` … `11-…sh`, `99-teardown.sh` | Manual migration walk-through with live producers and consumers (see `runbook/README.md`); uses the Step 3 stack, so never alongside `make step3` |
| `make step3-known-issues` | Migrator requirements Connect 4.100.0 doesn't meet. **Expected to fail**; keep it out of `make step3` |
| `make step2-up` / `step2-test` / `step2-down` | Iterate on a running Step 2 stack (same pattern for `step3-*`) |

Run a single test:
- Java: `$(MVN) -Dtest=ConsumerGroupPrefixFilterTest#offsetFetchIsPrefixedAndStripped test` (the `MVN` docker command is in the Makefile)
- Go: `cd tests && go test -tags step2 -run 'TestStep2Proxy/4b' -v ./...`. Go tests are split by build tag (`step2`, `step3`) and expect the matching stack to be running.

## What this project proves

Two Redpanda Migrator pipelines replicate source clusters A and B into one destination. Topics are separated by the migrator's `topic: 'a_${! @kafka_topic }'` interpolation. Consumer groups need a different mechanism, because the migrator has no group-rename option and identically named groups (`app-group`) would collide. A custom **Kroxylicious filter (`ConsumerGroupPrefix`)** sits between each migrator's *output* and the destination. It adds the prefix to group IDs on requests and removes it on responses.

- Migrator **input** connects directly to its source cluster (not proxied).
- Migrator **output** Kafka `seed_brokers` point at its own Kroxylicious virtual cluster (`dest-a` / `dest-b`). Schema Registry is HTTP and bypasses the proxy; subjects are separated with `schema_registry.subject` interpolation.
- Only group-related APIs are rewritten. Everything else passes through untouched.

## External inputs (outside this repo)

| Item | Path |
|---|---|
| Redpanda Connect / Migrator source v4.100.0 | `$MIGRATOR_SRC` |
| Kroxylicious source | `$KROXYLICIOUS_SRC` |
| Kroxylicious topic-rename example | `$KROXY_DEMO_SRC` |

Read `kroxy-linking-demo` end to end before writing filter code. It is the source of truth for the Kroxylicious version, the build tool, how the filter JAR reaches the proxy classpath, the proxy YAML schema, and Compose conventions. The Kroxylicious config schema has changed across releases, so do not trust config snippets from memory or from the spec over the demo.

## Layout decisions that span files

- **One Kroxylicious proxy per source** (`kroxylicious-a`/`-b`), each running the Step 2 config shape with its own prefix. The user chose this over one proxy with two virtual clusters.
- **Kroxylicious 0.24.0 everywhere** (`kroxylicious-filter/pom.xml`, `Dockerfile` base image). Read the Kroxylicious source at the `v0.24.0` tag; the local checkout is ahead of it.
- **The migrator is the published `connect:4.100.0` image**, not a source build (the user's decision).
- **The test module (`tests/go.mod`) must stay on the migrator's franz-go versions** (v1.20.7 / kadm v1.17.2 / kmsg v1.12.0). `go mod tidy` with no imports will silently upgrade them; `make step1-verify` catches it.
- **Migrator configs have no `label`**, because `lint` rejects matching input/output labels; unlabelled, both pair up as `"default"`.

## Constraints that are easy to get wrong

- **Use the naming conventions in spec §3 exactly** (service names, `a_`/`b_` prefixes, `dest-a`/`dest-b`, `orders`/`payments`, `app-group`, `migrator`).
- **Step 1 decides the filter's API set.** Get the destination-side wire APIs from the franz-go source in the module cache (`go env GOMODCACHE`), not from assumptions. Include the implicit `FindCoordinator`, and handle every request version the client can negotiate: FindCoordinator v4+ batches `CoordinatorKeys`, and OffsetFetch v8+ batches `Groups[]`.
- **FindCoordinator:** only prefix keys with `KeyType == 0` (GROUP). Leave transaction keys alone.
- **Responses must be un-prefixed.** franz-go matches batched responses to requests by name.
- **Never add "skip if already prefixed" logic.** A source group `a_x` must become `a_a_x` on the wire and come back as `a_x`.
- **ListGroups** (when `filterListGroups`): drop groups that lack the prefix and strip it from the rest.
- **Filter interfaces:** Kroxylicious does not allow mixing a generic `RequestFilter`/`ResponseFilter` with specific per-API interfaces in the same class. Use whichever style the demo uses.
- **Exclude the migrator's own group** with `consumer_groups.exclude: ["^migrator$"]`, and assert that no `migrator`, `a_migrator`, or `b_migrator` group exists on the destination.
- **Test clients:** use Go with franz-go (`kgo`/`kadm`/`kmsg`) so tests exercise the same request versions as the migrator. Run e2e assertions directly against `redpanda-dest`, bypassing the proxy.
- **Offset assertions:** translation is timestamp-based by default. It can be imprecise when records share a millisecond timestamp, and destination offsets never move backward, so assert within tolerance.
- **Findings:** record ambiguities, fallbacks, and observed results in `docs/findings.md`.
- **Redpanda quirks already found** (they also happen without the proxy):
  - OffsetFetch v1 returns no offsets.
  - Deleting an Empty group's last offset deletes the group.
  - Commits to a `Stable` destination group fail with `UNKNOWN_MEMBER_ID`.
- **Migrator behaviors already found**, detailed in `findings.md` under "Known issues":
  - Active source groups get timestamp-only translation, which is always behind with bulk-produced data.
  - A translated source offset is never re-translated until it changes.
  - After a restart, group sync is idle until the migrator writes a record (or 5 minutes pass), and exact translation fails for topics it hasn't written to.
- **Runbook scripts `source lib.sh`**, which sets `set -euo pipefail`. Don't source it into an interactive shell; use `bash -c 'source ./lib.sh; …'`.
- **Makefile recipes:** `GOTEST3` starts with `cd tests &&`, so call test targets through `$(MAKE)`. Inlining it makes the following teardown run in the wrong directory.
- **Proxy DEBUG log lines** look like `api=OFFSET_FETCH version=8 request group 'x' -> 'a_x'`. The logger name prints abbreviated as `de.cg.ConsumerGroupPrefixFilter`, so grep for that, not `demo.cgprefix`.
