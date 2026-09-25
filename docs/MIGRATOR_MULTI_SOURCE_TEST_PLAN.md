# Redpanda Migrator: Multi-Source Replication with Consumer Group Prefixing via Kroxylicious

> **Audience:** Claude Code. This document is the task specification. Work through the steps in order, commit the artifacts listed under each step, and do not start a step until the previous step's acceptance criteria pass.

---

## 0. Goal

Prove that two Redpanda Migrator pipelines can replicate **two source clusters (A and B) into one destination cluster** such that:

1. **Topics are separated** in the destination by a per-source prefix (`a_orders`, `b_orders`).
2. **Consumer groups are separated** in the destination by the same per-source prefix (`a_app-group`, `b_app-group`), even though both sources contain a group with the **same name** (`app-group`).

Without intervention, the migrator writes source group names unchanged to the destination, so identically named groups from A and B collide and produce consumer-group translation errors. The migrator has no group-rename option (it only has `consumer_groups.include/exclude/only_empty/interval/fetch_timeout`). The fix under test: put a **Kroxylicious proxy between each migrator's output and the destination cluster**, with a custom filter that prefixes the group ID on every group-related request and strips it on the way back.

## 1. Inputs

| Item | Path |
|---|---|
| Redpanda Migrator (Redpanda Connect) source, v4.100.0 | `$MIGRATOR_SRC` |
| Kroxylicious source | `$KROXYLICIOUS_SRC` |
| Kroxylicious topic-rename example (reference for filter structure, build, config format, compose wiring) | `$KROXY_DEMO_SRC` |

**Before writing any code**, read `kroxy-linking-demo` end to end. Reuse its build setup (Maven/Gradle, Kroxylicious version, how the filter JAR is placed on the proxy classpath), its proxy YAML format, and its Docker Compose conventions. The Kroxylicious config schema has changed across releases (`filterDefinitions`, `defaultFilters`, per-virtual-cluster `filters`, `gateways`, `bootstrapServers`); the demo is the source of truth for the version in use, not this document.

## 2. Target architecture

```
                 ┌──────────────┐        ┌───────────────────┐
  source-a ──────► migrator-a   ├───────►│ kroxylicious      │
  (redpanda)     │ topic: a_*   │        │  vcluster dest-a  │──┐
                 └──────────────┘        │  prefix "a_"      │  │
                                         │                   │  ├──► destination
                 ┌──────────────┐        │  vcluster dest-b  │  │    (redpanda)
  source-b ──────► migrator-b   ├───────►│  prefix "b_"      │──┘
  (redpanda)     │ topic: b_*   │        └───────────────────┘
                 └──────────────┘
```

- The migrator **input** talks to its source cluster directly (no proxy). Groups on the source are read with their real names.
- The migrator **output** (topic creation, produce, consumer-group offset commits, schema registry) points its Kafka `seed_brokers` at its own Kroxylicious virtual cluster. Schema Registry traffic is HTTP and does not go through Kroxylicious.
- One Kroxylicious container with two virtual clusters (one per source), each with its own filter instance/config. Two proxy containers is an acceptable fallback if per-cluster filter config is not supported in the version used.
- Everything that is not a group-related API passes through unchanged.

## 3. Naming conventions (use exactly these)

| Thing | Source A | Source B |
|---|---|---|
| Compose service | `redpanda-a` | `redpanda-b` |
| Destination service | `redpanda-dest` | |
| Migrator service | `migrator-a` | `migrator-b` |
| Topic/group prefix | `a_` | `b_` |
| Proxy virtual cluster | `dest-a` | `dest-b` |
| Test topics (same names in both sources) | `orders`, `payments` | `orders`, `payments` |
| Test consumer group (same name in both sources) | `app-group` | `app-group` |
| Migrator's own input consumer group | `migrator` | `migrator` |

The migrator's own `migrator` group exists in both sources too. Exclude it from group migration (`consumer_groups.exclude: ["^migrator$"]`), and add a test asserting it does not appear in the destination.

## 4. Project layout to create

```
migrator-multisource-test/
├── README.md                          # how to run each step
├── docs/
│   ├── migrator-consumer-group-api-calls.md   # Step 1 deliverable
│   └── findings.md                            # observed errors, decisions, open questions
├── kroxylicious-filter/               # Step 2: custom filter (mirror kroxy-linking-demo build)
│   ├── pom.xml (or build.gradle)
│   └── src/{main,test}/java/...
├── proxy/
│   ├── config-step2.yaml
│   └── config-step3.yaml
├── migrator/
│   ├── migrator-a.yaml
│   └── migrator-b.yaml
├── compose/
│   ├── docker-compose.step2.yaml      # 1 redpanda + proxy + test client
│   └── docker-compose.step3.yaml      # 3 redpanda + proxy + 2 migrators
├── tests/                             # Go (franz-go) preferred: same client library as the migrator
│   ├── step2_proxy_test.go
│   └── step3_e2e_test.go
└── Makefile                           # step1-verify, step2, step3, clean
```

Go with `franz-go` (`kgo`, `kadm`, `kmsg`) is preferred for test clients because the migrator is built on it, so the tests exercise the same request versions and batching behavior. If Step 1 shows the migrator uses a different client, use that instead.

---

## 5. Step 1: Inventory the migrator's consumer-group API calls

### Task

Review `$MIGRATOR_SRC` and produce `docs/migrator-consumer-group-api-calls.md`, listing **every Kafka protocol request the migrator issues that carries a consumer group ID, or whose response carries one**, and which cluster (source or destination) receives it.

### How to search

1. Locate the migrator implementation (start with `grep -rl "redpanda_migrator" --include=*.go`, then follow into the input/output and consumer-group sync code).
2. In that code, find all admin/client calls. Grep for at least:
   - `kadm` methods: `ListGroups`, `DescribeGroups`, `FetchOffsets`, `FetchOffsetsForTopics`, `FetchManyOffsets`, `CommitOffsets`, `CommitAllOffsets`, `DeleteGroups`, `DeleteOffsets`, `DescribeConsumerGroups`, `ListEndOffsets`, `ListStartOffsets`, `ListOffsetsAfterMilli`
   - Raw `kmsg.` request types (e.g. `kmsg.NewOffsetCommitRequest`, `kmsg.NewPtrOffsetFetchRequest`, `kmsg.NewDescribeGroupsRequest`, `kmsg.NewFindCoordinatorRequest`)
   - `kgo` consumer-group usage (`kgo.ConsumerGroup(`), since the input's own group membership generates JoinGroup/SyncGroup/Heartbeat/OffsetCommit/LeaveGroup against the **source**
3. For each high-level call, record the **wire-level** Kafka APIs it produces. franz-go issues `FindCoordinator` implicitly before any group-coordinator request; include it. Confirm by reading the franz-go code vendored or in the module cache (`go env GOMODCACHE`), not by assumption.
4. Note which client instance issues each call (source client vs. destination client). This decides what passes through the proxy.
5. Note request/response version ranges the client may use (e.g. `FindCoordinator` v4+ batches keys in `CoordinatorKeys`; `OffsetFetch` v8+ batches in `Groups[]`). The filter must handle every version the client can negotiate.

### Deliverable format (`docs/migrator-consumer-group-api-calls.md`)

One table row per wire API, plus a code reference:

| # | Kafka API (key) | Target cluster | Issued by (file:line, function) | High-level call | Group ID fields in request | Group ID fields in response | Versions used | Purpose |
|---|---|---|---|---|---|---|---|---|
| e.g. | OffsetCommit (8) | destination | `.../groups.go:123 syncGroup()` | `kadm.CommitOffsets` | `GroupId` | none | v? | write translated offsets |

Then sections for:
- **Call sequence** per sync cycle (ordered list of wire calls for one group, source side and destination side).
- **Destination-side calls**: the set the proxy filter must handle. This is the contract for Step 2.
- **Source-side calls**: for completeness; these are not proxied.
- **How the migrator matches responses to groups** (e.g. does it key maps by the group name returned in the response?). This determines whether responses must be un-prefixed.
- **Does the migrator ever list/describe destination groups** and filter by name? If yes, document how the prefix affects it.
- **Reproduction of the original error**: exact log line(s)/code path that fail when two sources have the same group name, if identifiable from code.

### Acceptance criteria

- Every destination-bound request containing a group ID is listed, including implicit `FindCoordinator`.
- Every entry has a file:line reference.
- `findings.md` records anything ambiguous.

---

## 6. Step 2: Build and verify the Kroxylicious group-prefix filter

### 6.1 Filter specification

Create a filter plugin `ConsumerGroupPrefix` (factory + filter, service-loader registration, following `kroxy-linking-demo`).

**Config:**
```yaml
prefix: "a_"            # required
stripOnResponse: true   # default true
filterListGroups: true  # default true: only return groups that carry the prefix, with prefix stripped
```

**Request rewriting (client → broker):** add `prefix` to group IDs. The final API set must come from the Step 1 document; at minimum implement:

| API | Field(s) to prefix |
|---|---|
| FindCoordinator (10) | `Key` (v0–3) / each `CoordinatorKeys[]` (v4+), **only when `KeyType == 0` (GROUP)**. Leave transaction keys untouched. |
| OffsetCommit (8) | `GroupId` |
| OffsetFetch (9) | `GroupId` (v0–7) / `Groups[].GroupId` (v8+) |
| DescribeGroups (15) | `Groups[]` |
| DeleteGroups (42) | `GroupsNames[]` |
| OffsetDelete (47) | `GroupId` |
| JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13), TxnOffsetCommit (28) | `GroupId` (not expected from the migrator output, but prefix for safety and log a WARN) |
| ConsumerGroupDescribe (69), ConsumerGroupHeartbeat (68) | group ID fields, if present in the Kroxylicious Kafka version and used per Step 1 |

**Response rewriting (broker → client), when `stripOnResponse`:** remove the prefix wherever the request added it, so the client can match responses to what it asked for:

| API | Field(s) to un-prefix |
|---|---|
| FindCoordinator v4+ | `Coordinators[].Key` (GROUP keys only) |
| OffsetFetch v8+ | `Groups[].GroupId` |
| DescribeGroups | `Groups[].GroupId` |
| DeleteGroups | `Results[].GroupId` |
| ListGroups (16) | if `filterListGroups`: drop groups without the prefix, strip prefix from the rest |

This matters because franz-go correlates batched responses (for example FindCoordinator v4 coordinator keys, OffsetFetch v8 groups) back to the request by name. If the response returns `a_app-group` for a request the client believes asked for `app-group`, the client will treat the result as missing. Step 1 must confirm the exact matching behavior; test both directions regardless.

**Other requirements:**
- Non-group APIs pass through untouched (Produce, Metadata, CreateTopics, ApiVersions, InitProducerId, ListOffsets, Fetch, DescribeConfigs, ACL APIs, etc.).
- Never double-prefix within a single request/response pair. Do not add "skip if already prefixed" logic: a source group legitimately named `a_x` must become `a_a_x`, and the response must return `a_x`.
- Log each rewrite at DEBUG: `api, version, original -> rewritten`. Expose a counter per API if the Kroxylicious metrics API makes it easy.
- Use the specific per-API filter interfaces or one generic `RequestFilter` + `ResponseFilter`; Kroxylicious does not allow mixing a generic interface with specific ones in the same filter class. Pick whichever the demo uses.

### 6.2 Unit tests (`kroxylicious-filter/src/test`)

For every API/version in the table: build the request object, run the filter, assert the rewritten fields; build the response, run the filter, assert stripping. Include:
- FindCoordinator with a mix of GROUP and TRANSACTION keys (only GROUP rewritten).
- OffsetFetch v8+ with multiple groups.
- ListGroups response with prefixed and unprefixed groups.
- Group name already starting with the prefix.
- Empty group ID.

### 6.3 Proxy integration test (`compose/docker-compose.step2.yaml`)

Services:
- `redpanda-dest` (single node, dev-container mode)
- `kroxylicious` with the filter JAR, virtual cluster `dest-a`, prefix `a_`
- test runner (host-side `go test` is fine)

Test `tests/step2_proxy_test.go`, for **each wire call in the Step 1 destination list**, reproducing the migrator's usage with the same franz-go high-level call:

1. Create topic `orders` (via proxy), produce 100 records.
2. Via proxy: `kadm.CommitOffsets("app-group", orders/0 → 42)`.
3. Directly against `redpanda-dest` (bypassing proxy): assert group `a_app-group` exists with offset 42 and `app-group` does **not** exist.
4. Via proxy: `FetchOffsets("app-group")` returns 42 without error.
5. Via proxy: `DescribeGroups("app-group")` returns the group under the name `app-group`.
6. Via proxy: `ListGroups` returns `app-group` and hides groups without the prefix (create a direct, unprefixed group `other` first to check).
7. Via proxy: `DeleteOffsets` / `DeleteGroups` (if used by the migrator) affect only `a_app-group`.
8. Assert non-group operations (topic create, produce, metadata) behave identically through the proxy.

Also add a test that pins franz-go to force older request versions where possible (`kgo.MaxVersions`) to cover pre-batching FindCoordinator/OffsetFetch paths.

### Acceptance criteria

- All unit tests and the Step 2 integration test pass.
- Proxy DEBUG logs show a rewrite for every destination API in the Step 1 doc.

---

## 7. Step 3: Three-cluster end-to-end test

### 7.1 Docker Compose (`compose/docker-compose.step3.yaml`)

- `redpanda-a`, `redpanda-b`, `redpanda-dest`: single-node Redpanda, `redpanda start --mode dev-container --smp 1`, each with an internal listener for the compose network and an external listener on a distinct host port. Each exposes Schema Registry on a distinct host port. Use the same Redpanda image tag for all three, with a healthcheck (`rpk cluster health`).
- `kroxylicious`: two virtual clusters, `dest-a` (prefix `a_`) and `dest-b` (prefix `b_`), both targeting `redpanda-dest:9092`, on non-overlapping bootstrap/broker port ranges. Advertised addresses must be resolvable from the migrator containers.
- `migrator-a`, `migrator-b`: Redpanda Connect built from `$MIGRATOR_SRC` (Dockerfile in that repo or a multi-stage build; record the exact build command in README). Only fall back to a published image if the source build fails, and note the image tag in `findings.md`.
- Start order: brokers healthy → proxy → seed data → migrators.

### 7.2 Migrator configs

`migrator/migrator-a.yaml` (B is identical with `b`/`redpanda-b`/`dest-b`):

```yaml
input:
  label: "migrator_a"
  redpanda_migrator:
    seed_brokers: [ "redpanda-a:9092" ]
    regexp_topics_include: [ '.' ]
    regexp_topics_exclude: [ '^_' ]
    consumer_group: migrator
    schema_registry:
      url: http://redpanda-a:8081

output:
  label: "migrator_a"
  redpanda_migrator:
    seed_brokers: [ "kroxylicious:<dest-a bootstrap port>" ]   # via proxy
    topic: 'a_${! @kafka_topic }'
    provenance_header: "x-source-cluster"
    schema_registry:
      url: http://redpanda-dest:8081
      subject: 'a_${! metadata("schema_registry_subject") }'
      translate_ids: true
    consumer_groups:
      enabled: true
      interval: 10s
      exclude: [ "^migrator$" ]
```

Validate both with `rpk connect lint` (or the built binary's `lint` subcommand) before running. If field names differ in v4.100.0, follow the source code and record the difference in `findings.md`. Consider setting `offset_header` for exact offset translation and compare results with timestamp-based translation.

### 7.3 Test data seeding (before migrators start)

On **each** source:
1. Create `orders` (3 partitions) and `payments` (1 partition).
2. Produce distinguishable records: source A values `A-<topic>-<n>`, source B values `B-<topic>-<n>`. Use different counts (A: 1000 orders / 200 payments; B: 700 orders / 300 payments) so mixing is detectable.
3. Create group `app-group` by consuming and committing **different** positions on each source (A: orders p0=500; B: orders p0=250, payments p0=100). Stop the consumer so the group is `Empty`.
4. Register a schema subject `orders-value` on both Schema Registries (optional, to exercise subject prefixing).

### 7.4 Assertions (`tests/step3_e2e_test.go`, run directly against `redpanda-dest`, bypassing the proxy)

Topics:
- Destination has `a_orders`, `a_payments`, `b_orders`, `b_payments`; no unprefixed `orders`/`payments`.
- Partition counts match the source.
- Record counts match per topic, and every record in `a_*` has value prefix `A-` (and vice versa). Check `x-source-cluster` header.

Consumer groups:
- Destination has `a_app-group` and `b_app-group`; no `app-group`, no `migrator`, `a_migrator`, or `b_migrator`.
- Committed offsets on `a_app-group` / `b_app-group` correspond to the source positions (translated; the record at the committed destination offset has the same value as at the source offset, within the documented timestamp-translation tolerance).

Live sync:
- Advance `app-group` on source A only (commit a later offset), wait > 2 × `interval`, assert `a_app-group` advanced and `b_app-group` did not change.
- Produce new records to both sources and assert they arrive in the correct prefixed topics.

Health:
- Migrator logs contain no consumer-group errors (grep for `error`/`failed` in group-sync code paths identified in Step 1).
- Proxy logs show rewrites from both virtual clusters with the correct prefix.

Schema Registry (if seeded): `a_orders-value` and `b_orders-value` exist.

### 7.5 Negative control

Run the same scenario with both migrators pointed **directly** at `redpanda-dest` (no proxy). Capture and record in `findings.md` the exact errors or wrong outcome (e.g. one source's offsets overwriting the other's in a shared `app-group`). This demonstrates the problem the proxy solves.

### Acceptance criteria

- `make step3` brings up the stack, seeds, runs the migrators, runs the assertions, and tears down, exiting 0.
- The negative control reproduces the collision.
- `findings.md` summarizes results, caveats (timestamp-based offset translation precision, proxy as an extra hop and single point of failure, Kroxylicious version compatibility with the client's request versions), and open questions.

---

## 8. Notes and alternatives to evaluate

- **Built-in Kroxylicious `MultiTenant` filter.** It prefixes resource names (topics, group IDs, transactional IDs) per virtual cluster. It could replace both the migrator `topic:` interpolation and the custom filter, but its separator and naming are fixed, and it also rewrites topics, which would double-prefix if the migrator `topic` field is kept. Evaluate briefly in `findings.md`; the custom filter remains the primary path because it touches only group IDs.
- **ApiVersions.** Kroxylicious may cap supported API versions at the Kafka version it was built against. Confirm the migrator's negotiated versions are within what the filter handles.
- **Schema Registry** does not pass through Kroxylicious; subject separation relies on the migrator's `schema_registry.subject` interpolation.
- **Offset translation** is timestamp-based by default and can be imprecise when records share a millisecond timestamp; offsets never move backward at the destination. Keep this in mind when writing offset assertions.
