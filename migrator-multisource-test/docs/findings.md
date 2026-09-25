# Findings

Observed errors, decisions, and open questions, recorded per step.

## Step 1: consumer-group API inventory

Full detail is in [`migrator-consumer-group-api-calls.md`](migrator-consumer-group-api-calls.md).

### Results

- **Only three destination-bound APIs carry a group ID: FindCoordinator (GROUP), OffsetFetch, OffsetCommit.** The migrator never lists, describes, deletes, or joins groups on the destination.
- **Stripping on responses is required** for FindCoordinator v4+ (`Coordinators[].Key`) and OffsetFetch v8+ (`Groups[].GroupId`), because franz-go matches the results by name. Failure modes if the filter doesn't strip:
  - FindCoordinator: visible. `failed to update offsets for group '…': coordinator was not returned in broker response`.
  - OffsetFetch: **silent**. The migrator ignores per-group fetch errors and misses the map lookup, so the no-rewind check (`migrator_groups.go:532`) is skipped without any log line. The tests must assert on this explicitly.
- **Both legacy and batched wire shapes are reachable.** The destination OffsetFetch gets one `Groups[]` entry per group **partition** (duplicates included), which pins it to v8+ whenever a group has more than one partition with offsets. FindCoordinator with a single key goes unpinned. If the broker or proxy caps versions below v8/v4, kgo falls back to per-group requests at ≤v7/≤v3.
- **The migrator's own group is already excluded automatically.** `SkipSourceGroup` is read from the input's `consumer_group` and skipped at `migrator_groups.go:365`. The spec's `consumer_groups.exclude: ["^migrator$"]` is redundant but harmless. Keep it, and keep the destination assertion.
- **The input group uses the classic protocol.** KIP-848 is opt-in through a context key that Connect never sets. All of this is source-side, so it doesn't matter to the proxy.
- **Config field names in the spec's Step 3 migrator YAML match v4.100.0:** `topic`, `provenance_header`, `offset_header`, `schema_registry.subject`, `schema_registry.translate_ids`, `consumer_groups.{enabled,interval,fetch_timeout,include,exclude,only_empty}`, `regexp_topics_include/exclude`, `consumer_group`. `rpk connect lint` still needs to run in Step 3.
- ACL sync creates **topic-resource ACLs only**, so there are no group names in CreateACLs.

### The negative control's expected outcome differs from the spec's assumption

The spec expects the no-proxy run to produce "consumer-group translation errors" or "one source's offsets overwriting the other's". The code says otherwise. The migrator commits by **destination** topic name, which is already prefixed (`a_orders` vs `b_orders`), so the two migrators write disjoint partitions of one shared destination group `app-group`. The predicted result is **a merged `app-group` with offsets for both sources' topics and probably no migrator errors**, as long as the destination group is `Empty`.

Errors or overwrites are only expected when:
- (a) apps consume from the destination under `app-group`: the group becomes shared and non-empty, and the migrator's generation `-1` commits should then be rejected; or
- (b) topics are *not* prefixed, so both sources race on the same partitions.

Step 3 must observe and record what actually happens. It could also add a variant for (a), with an active consumer in the destination `app-group`, if the plain run shows no errors.

### Latent upstream oddity (not relevant here)

`listGroupsOffsets(ctx, adm, …)` uses its `adm` parameter for ListGroups but calls `m.srcAdm.FetchManyOffsets` (`migrator_groups.go:273`). It is only ever called with `srcAdm`, so there is no effect today.

### Open questions carried forward

1. ~~Kroxylicious ApiVersions cap~~: answered in Step 2 below.
2. ~~Redpanda OffsetFetch v8+ with duplicate `Groups[]` entries~~: the migrator-shaped call with duplicates works through the proxy (Step 2, test `4b`). The filter rewrites each entry on its own and never relies on response order, so whether Redpanda deduplicates doesn't matter. Not inspected further.
3. ~~The exact error code for a generation `-1` commit to a `Stable` group~~: `UNKNOWN_MEMBER_ID`, observed in Step 3's negative control.

## Step 2: ConsumerGroupPrefix filter

`make step2` passes from a clean state: 58 unit tests, `TestStep2Proxy` (the spec's steps 1–8 plus extras), `TestStep2OlderVersions`, and the proxy log check.

### Decisions

- **Kroxylicious 0.24.0**, as pinned by `kroxy-linking-demo` (`pom.xml`, `quay.io/kroxylicious/proxy:0.24.0`). The local Kroxylicious checkout is at `v0.24.0-85` (0.25.0-SNAPSHOT), so config schema and behavior were read from the `v0.24.0` tag, not the working tree.
- **The demo is Kubernetes-only** (Kroxylicious operator CRDs), so it has no standalone proxy YAML to copy. The Compose wiring and proxy config come from the v0.24.0 repo's own `compose/` example: `virtualClusters[].gateways[].portIdentifiesNode`, `filterDefinitions`, and per-virtual-cluster `filters`. Per-virtual-cluster filters exist in 0.24.0, so Step 3 can use **one proxy with two virtual clusters**.
- **The rename direction is the opposite of the demo's.** The demo's `TopicPrefixerFilter` adds the prefix on responses and strips it on requests (cosmetic renaming). Ours adds on requests and strips on responses, like `MultiTenant`. The demo's structure was reused: specific per-API filter interfaces, `FilterFactory` + `@Plugin`, service-loader registration, `MockFilterContext` tests, the Maven/Docker build, and the FindCoordinator correlation-ID set that keeps transactional-ID lookups untouched.
- **Fields are chosen by `apiVersion`** (FindCoordinator `key` for v0–3 vs `coordinatorKeys` for v4+, OffsetFetch `groupId` for v0–7 vs `groups` for v8+). `MultiTenant` rewrites both fields regardless of version. Choosing by version means the logs and counters only report rewrites that actually go on the wire. Unit tests serialize and deserialize every message at every version the proxy decodes, to prove the right field is rewritten.
- **Empty group IDs get the prefix like any other name** (`""` → `a_`, and back again). This keeps the mapping one-to-one per prefix, so A and B can't collide even on `""`. The side effect: a group `""` that the broker would reject for JoinGroup is sent as a valid `a_`.
- **`filterListGroups: false` passes ListGroups through unchanged** (raw backend names), rather than stripping some names and not others.
- **A response group ID without the prefix is left unchanged and logged at WARN** (`lacks prefix`). The Step 2 log check fails if this ever appears.
- `DeleteGroupsResponse.results()` is a collection hashed on `groupId`. It is rebuilt from `duplicate()`s rather than renamed in place, so lookups by name still work (there's a unit test for this).
- Logging: the stock 0.24.0 `log4j2.yaml` plus a `demo.cgprefix` logger (`CG_PREFIX_LOG_LEVEL`, default DEBUG), baked into the image. Metrics: a Micrometer counter on `Metrics.globalRegistry`, the same registry record-encryption uses, which the proxy's Prometheus endpoint exports.

### Negotiated versions (Redpanda v26.2.2 through the 0.24.0 proxy, franz-go v1.20.7)

| API | Proxy advertises | franz-go max | Used |
|---|---|---|---|
| FindCoordinator (10) | v0–4 | 6 | **v4** (batched) |
| OffsetFetch (9) | v1–8 | 9 | **v8** (batched) |
| OffsetCommit (8) | v2–8 | 9 | v8 |
| DescribeGroups (15) | v0–5 | 6 | v5 |
| ListGroups (16) | v0–4 | 5 | v4 |
| DeleteGroups (42) | v0–2 | 2 | v2 |
| OffsetDelete (47) | v0 | 0 | v0 |

- **Redpanda sets the caps, not Kroxylicious.** The 0.24.0 message classes (Kafka 4.3 spec) decode up to FindCoordinator v6, OffsetFetch v10 and OffsetCommit v10.
- **Kroxylicious raises the minimums**: OffsetCommit ≥ v2 and OffsetFetch ≥ v1, because Kafka 4 removed the older versions (KIP-896). A client pinned below that can't use those APIs through the proxy at all.
- So by default the migrator sends the **batched** shapes (FindCoordinator v4, OffsetFetch v8), and stripping on responses is required. The legacy shapes (FindCoordinator v0/v3, OffsetFetch v2/v7) are covered by `TestStep2OlderVersions` using `kgo.MaxVersions`, and the log check confirms they reached the proxy.

### Redpanda behaviors found along the way (all reproduced without the proxy)

- **OffsetFetch v1 returns no offsets** (Redpanda v26.2.2), both for all topics and for explicit topics. The "oldest" test variant therefore uses OffsetFetch v2.
- **An Empty group is removed as soon as its last committed offset is deleted.** A DeleteGroups after that returns `GROUP_ID_NOT_FOUND`. Step 2's DeleteOffsets test keeps a second partition committed.

### Known gaps (not needed by the migrator)

- **Share groups and streams groups** (KIP-932 / KIP-1071 APIs, which 0.24.0 exposes as filter interfaces) and **ACL/config APIs addressing a GROUP resource** pass through **unprefixed**. A client using them through the proxy would see or affect raw backend groups.
- The membership/transactional APIs (Join/Sync/Heartbeat/Leave, TxnOffsetCommit, AddOffsetsToTxn, ConsumerGroupHeartbeat) are prefixed and logged at WARN once per connection. They're unit-tested but not exercised end to end.

### Alternatives noted for the Step 3 write-up

- `MultiTenant` (built in): see spec §8. It also rewrites topics, so it would double-prefix alongside the migrator's `topic:` interpolation.
- The v0.24.0 BOM also ships `kroxylicious-entity-isolation`, which hasn't been examined yet. It may offer per-virtual-cluster group isolation without a custom filter. Worth a look before recommending the custom filter for production.

## Step 3: three clusters, two proxies, two migrators

`make step3` passes from a clean state (exit 0, stack torn down). `make step3-negative` reproduces the collision.

### Results (with proxies)

| Check (spec §7.4) | Result |
|---|---|
| Topics `a_orders`, `a_payments`, `b_orders`, `b_payments`; no `orders`/`payments` | pass |
| Partition counts match the source (3 / 1) | pass |
| Every record in the same partition and order with the same value (destination offset = source offset minus the records trimmed from the source); `a_*` only `A-…`, `b_*` only `B-…`; `x-source-cluster` = that source's cluster ID | pass (all records still on the sources) |
| Groups `a_app-group`, `b_app-group`; no `app-group`, `migrator`, `a_migrator`, `b_migrator` | pass |
| Translated offsets point at the same record, and are **shifted** by the trimmed amount | pass, **exact**: A orders/0 500→300; B orders/0 250→150, payments/0 100→50 |
| Live sync: A's group moved to 610 → `a_app-group` followed to 410 within one 10s cycle; `b_app-group` unchanged after > 2 cycles | pass |
| New records on both sources arrive in the right prefixed topics | pass |
| Migrator logs: no WARN/ERROR at all; group offsets committed | pass |
| Proxy logs: each proxy rewrote `app-group` only to its own prefix, never saw an unprefixed response group | pass |
| Schema subjects `a_orders-value`, `b_orders-value`; no `orders-value` | pass |

Wire traffic per proxy per sync cycle was exactly the Step 1 prediction: FindCoordinator v4 (once, then cached), OffsetFetch v8 and OffsetCommit v8, all rewritten.

**Source and destination offsets differ.** Before the migrators start, the seed deletes the first records of every source partition that has group commits (`DeleteRecords`: A orders/0 before 200, B orders/0 before 100, B payments/0 before 50). The migrator copies from the source's start offset, so destination offsets trail the source by exactly those amounts. The offset check therefore requires `destination = source − trimmed` as well as matching records at the position. If the offsets were identical on both sides, an untranslated offset would pass too.

### Negative control (no proxies): observed

- **Plain run:** both migrators write into **one shared destination group `app-group`**, holding `a_orders/0=300`, `b_orders/0=150` and `b_payments/0=50`, all correctly translated. **There are no errors in either migrator's log.** This matches the Step 1 prediction: the two sources don't overwrite each other's partitions, because the topic names already differ, but the groups can't be told apart.
- **With an active consumer:** an application consumer joined `app-group` on the destination (subscribed to `a_orders`, group `Stable`), then source A moved its group. migrator-a logged, every cycle:
  `level=error msg="Consumer group migration: failed to update offset for group 'app-group' topic 'a_orders' partition 0: UNKNOWN_MEMBER_ID: The coordinator is not aware of this member."`
  The destination offset stayed at 300 (the source was at 605, which should have become 405). migrator-b logged nothing.

### Deviations from the spec

- **The migrator image is the published `docker.redpanda.com/redpandadata/connect:4.100.0`, not a source build** (the user's decision). The spec's reasons for a source build don't apply for an unpatched test, and the source build needs the whole Connect tree compiled with Go 1.26.5, which is slow. The image reports `Version: 4.100.0` and includes `redpanda_migrator`. `migrator/Dockerfile` (a source build) is kept for a possible patched migrator, but it is **untested**: its first build was stopped.
- **The input and output have no `label`.** The migrator pairs input and output by label, and they must match (`migrator.go:98-99`), but `lint` rejects duplicate labels ("label 'migrator_a' collides…"). Without labels, both fall back to `"default"` and still pair up (`migrator.go:323-326`); that's safe with one migrator per config file. Both configs pass `lint`.
- **Orders are spread 60/20/20** across partitions 0/1/2, rather than evenly. That way A's `orders/0=500` is inside the partition (A: 600 records, B: 420). With an even three-way split, partition 0 would hold only 334, and the migrator skips offsets beyond the partition end.
- **The seed trims the start of three source partitions** (see above) so that translated offsets differ from source offsets. The spec doesn't ask for this; without it, the offset check can't tell a translated offset from an untranslated one.
- **The seed's `app-group` is created with a kadm offset commit**, not by running a consumer and stopping it. The result is the same: an `Empty` group with committed offsets.
- **`offset_header: "x-source-offset"` is set**, which gives exact translation for `Empty` groups. That's why the translated offsets are exact. **Timestamp-only translation was not measured** (see open questions).
- **The proxies needed a healthcheck** (`curl -sf localhost:9190/livez`). The 0.24.0 image defines none, so `depends_on: service_healthy` never succeeded.
- **The output's `seed_brokers` is `${DEST_BROKERS:kroxylicious-X:9192}`**, so the negative control reuses the same config files with `DEST_BROKERS=redpanda-dest:9092`.

### Caveats

- **Active destination consumers block group sync, with or without the proxy.** The negative control's `UNKNOWN_MEMBER_ID` is the broker rejecting the migrator's commits (made outside any group generation) to a group that has live members. An application consuming `a_app-group` on the destination while migrator-a is still syncing would cause the same errors through the proxy. Stop consumers on the destination (or stop group sync) at cutover.
- **All of a migrator's destination traffic goes through its proxy**, including Produce, not just group APIs. The migrator uses a single client for its whole output (`migrator.go:547-560`), so group traffic can't be routed separately. Each proxy is an extra hop for data and a single point of failure *for its own source*; having one proxy per source keeps A and B independent.
- **Kroxylicious version compatibility.** The versions negotiated with Redpanda v26.2.2 are FindCoordinator v4, OffsetFetch v8 and OffsetCommit v8. The 0.24.0 proxy can decode up to v6 / v10 / v10, so there's headroom for Redpanda upgrades. The proxy rejects OffsetCommit below v2 and OffsetFetch below v1.
- **Timestamp-based translation** (without `offset_header`) can be imprecise when several records share a millisecond timestamp, and destination offsets never move backward. The exact matches here come from `offset_header`.
- The migrator loaded a built-in `open_source` license by itself (`Successfully loaded Redpanda license … license_type=open_source`); no license key was needed for `redpanda_migrator` 4.100.0.

### Alternatives (spec §8), from reading the v0.24.0 source; neither was run

- **`MultiTenant`** prefixes topics, group IDs and transactional IDs with `<virtual cluster name><separator>` (the separator defaults to `-`; `MultiTenantConfig.prefixResourceNameSeparator`), and hides unprefixed resources. Combined with the migrator's `topic: 'a_…'` setting it would double-prefix (`dest-a-a_orders`). It could replace both mechanisms with virtual clusters named `a`/`b`, `prefixResourceNameSeparator: "_"`, and a migrator `topic: '${! @kafka_topic }'`. But it also renames transactional IDs and covers more APIs, which would need retesting. It is also marked internally as a POC (`// TODO naive - POC implementation uses virtual cluster name as a tenant prefix`).
- **`EntityIsolation`** (`kroxylicious-entity-isolation`) isolates **only group IDs and transactional IDs** (`TOPIC_NAME` is rejected as not supported), which is exactly the scope needed here. It also covers ACL and config requests addressing groups, which the custom filter doesn't. Its only mapper (`PrincipalEntityNameMapper`) derives the prefix from the **authenticated principal** (`<principal><separator><group>`), and it's an error if a connection has no authenticated subject. It would need SASL on the proxy's client side, with each migrator's output authenticating as its own principal. **It's the strongest candidate to replace the custom filter in production**, but it needs its own test.

### Open questions

1. **Timestamp-only offset translation.** Run Step 3 without `offset_header` and measure how far offsets deviate from the source positions.
2. **`EntityIsolation` with SASL.** Worth a spike: principals `migrator-a`/`migrator-b`, then confirm FindCoordinator v4 and OffsetFetch v8 stripping works for franz-go the same way as with the custom filter.
3. **Cutover procedure:** the order for stopping group sync and starting destination consumers, given the `UNKNOWN_MEMBER_ID` behavior.
