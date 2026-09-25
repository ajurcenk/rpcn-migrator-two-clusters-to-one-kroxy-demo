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
3. **The exact error code** Redpanda returns for a generation `-1` OffsetCommit to a `Stable` group (negative control, variant a). Still open for Step 3.

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
