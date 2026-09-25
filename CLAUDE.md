# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

This repo is currently spec-only. `docs/MIGRATOR_MULTI_SOURCE_TEST_PLAN.md` is the authoritative task specification, written for Claude Code. Work through its steps in order (Step 1 → 2 → 3). Do not start a step until the previous step's acceptance criteria pass. When the spec and this file disagree, the spec wins. Update this file once real build/test commands exist.

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

## Planned layout and commands

Everything goes under `migrator-multisource-test/`: `kroxylicious-filter/` (Java plugin), `proxy/`, `migrator/`, `compose/`, `tests/` (Go), `docs/`. The planned Makefile targets are `step1-verify`, `step2`, `step3`, and `clean`. `make step3` must bring the stack up, seed data, run the migrators and assertions, tear down, and exit 0. Record the exact migrator image build command in the README.

Validate migrator configs with `rpk connect lint` (or the built binary's `lint` subcommand). If v4.100.0 field names differ from the spec, follow the source code and record the difference in `docs/findings.md`.

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
- **Findings:** record ambiguities, fallbacks (such as using a published image), and the negative-control results (migrators pointed directly at the destination) in `docs/findings.md`.
