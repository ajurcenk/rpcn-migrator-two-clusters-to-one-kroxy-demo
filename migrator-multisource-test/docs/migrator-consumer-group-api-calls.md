# Redpanda Migrator v4.100.0: consumer-group Kafka API inventory

Step 1 deliverable. It lists every Kafka request the migrator issues that carries a consumer group ID in the request or response, and which cluster receives it.

## Sources and path abbreviations

| Abbrev. | Path | Version |
|---|---|---|
| `mig/` | `$MIGRATOR_SRC/connect/internal/impl/redpanda/migrator/` | Connect v4.100.0 |
| `kafka/` | `$MIGRATOR_SRC/connect/internal/impl/kafka/` | Connect v4.100.0 |
| `kgo/` | `$GOMODCACHE/github.com/twmb/franz-go@v1.20.7/pkg/kgo/` | franz-go v1.20.7 |
| `kadm/` | `$GOMODCACHE/github.com/twmb/franz-go/pkg/kadm@v1.17.2/` | kadm v1.17.2 |
| `kmsg/` | `$GOMODCACHE/github.com/twmb/franz-go/pkg/kmsg@v1.12.0/` | kmsg v1.12.0 |

Here `$GOMODCACHE` is `$(go env GOMODCACHE)`. The versions come from `connect/go.mod`. Every high-level call listed below was traced into the franz-go source to find the wire requests it produces.

The migrator uses franz-go (`kgo`/`kadm`/`kmsg`) for all Kafka traffic. The Sarama-based inputs in `kafka/` are not used.

## Client instances

| Client | Created at | Cluster | Used for |
|---|---|---|---|
| **src** (`FranzReaderOrdered.Client`, wrapped as `srcAdm`) | `mig/migrator.go:526` (`onInputConnected`); the input's kgo client, which joins group `consumer_group`, is built at `kafka/franz_reader_ordered.go:510` | source | record consumption, the input's own group membership, group discovery, reading source records for timestamp lookup |
| **dst** (`lazyFranzSharedClientInfo` client, wrapped as `dstAdm`) | `mig/franz.go:64` (`GetClient`), `mig/migrator.go:547` (`onOutputConnected`) | destination (**through the proxy**) | Produce, topic/ACL sync, **group offset reads and commits** |

A single dst `kgo.Client` is shared by the producer, the topic migrator and the groups migrator (`mig/migrator.go:547-560`), so everything below labeled "destination" goes through the Kroxylicious virtual cluster.

The client pins no request versions: there is no `kgo.MaxVersions`/`MinVersions` in `kafka/franz_client.go:158-189`, `kafka/franz_writer.go` or `mig/franz.go`. kgo negotiates the highest version that both kmsg v1.12.0 and the ApiVersions response support. Kroxylicious can lower that ceiling.

## Wire API table

### Destination-side calls (these go through the proxy)

| # | Kafka API (key) | Target cluster | Issued by (file:line, function) | High-level call | Group ID fields in request | Group ID fields in response | Versions used | Purpose |
|---|---|---|---|---|---|---|---|---|
| D1 | **FindCoordinator (10)**, `KeyType=0` (GROUP) | destination | built at `kgo/client.go:1912` `doLoadCoordinators()`; triggered by the OffsetFetch sharder (`kgo/client.go:3215` `offsetFetchSharder.shard` → `loadCoordinators`) for D2 and by `handleCoordinatorReqSimple` (`kgo/client.go:2161`, `2192`) for D3 | implicit, before D2 and D3 (results cached per `{key,type}`) | v0–3: `Key`; v4+: `CoordinatorKeys[]` | v4+: `Coordinators[].Key`; v0–3: none on the wire (kgo copies the key from its own request, `kgo/client.go:3424`) | 0–6 (kmsg max 6, `kmsg/generated.go:12839`). One key: no version pin. More than one key: pinned **≥4** (`kgo/client.go:3397`). If the broker is too old: split into one request per key, pinned **≤3** (`kgo/client.go:3409`) | find the group coordinator for D2/D3 |
| D2 | **OffsetFetch (9)** | destination | `mig/migrator_groups.go:516` `groupsMigrator.Sync()` → `kadm/groups.go:1100` `FetchManyOffsets` → sharder `kgo/client.go:3199` | `dstAdm.FetchManyOffsets(ctx, extractGroupNames(gcos)...)` | v0–7: `GroupId`; v8+: `Groups[].GroupId` | v8+: `Groups[].GroupId`; v0–7: none on the wire (kgo copies it from its own request, `kgo/client.go:3161`) | 0–9 (kmsg max 9, `kmsg/generated.go:11707`). One group entry: unpinned. **More than one entry: pinned ≥8** (`kgo/client.go:3278`). If the broker is too old: one request per group, pinned **≤7** (`kgo/client.go:3264`) | read current destination offsets so the migrator never moves one backward (`mig/migrator_groups.go:532`) |
| D3 | **OffsetCommit (8)** | destination | `mig/migrator_groups.go:577` `groupsMigrator.Sync()` (one goroutine per group) → `kadm/groups.go:810` `CommitOffsets` → `kgo/client.go:2161` | `dstAdm.CommitOffsets(ctx, g, offsets)` | `GroupId` (`kadm/groups.go:812`). `Generation=-1` (kmsg default, `kmsg/generated.go:11289`), `MemberId=""`: an "admin" commit outside any group generation | none (the response has only topics/partitions) | 0–9 (kmsg max 9, `kmsg/generated.go:10967`), unpinned | write the translated offsets |

No other destination-bound request carries a group ID. In particular, the migrator never sends ListGroups, DescribeGroups, DeleteGroups, OffsetDelete, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, TxnOffsetCommit, ConsumerGroupHeartbeat or ConsumerGroupDescribe to the destination.

### Source-side calls (not proxied; listed for completeness)

| # | Kafka API (key) | Target cluster | Issued by (file:line, function) | High-level call | Group ID fields in request | Group ID fields in response | Versions used | Purpose |
|---|---|---|---|---|---|---|---|---|
| S1 | ListGroups (16) | source, **every broker** (`listGroupsSharder`, `kgo/client.go:2459`) | `mig/migrator_groups.go:245` `listGroupsOffsets()` → `kadm/groups.go:270` | `srcAdm.ListGroups(ctx)` | none (`StatesFilter`/`TypesFilter` are empty) | `Groups[].Group` | 0–5 | discover the groups to migrate |
| S2 | FindCoordinator (10) | source | as D1 | implicit, before S3 and S4–S9 | as D1 | as D1 | as D1 | |
| S3 | OffsetFetch (9) | source | `mig/migrator_groups.go:273` → `kadm/groups.go:1100` | `m.srcAdm.FetchManyOffsets(ctx, groups...)` | as D2 | as D2 | as D2 | read the source group offsets to translate |
| S4 | JoinGroup (11) | source | `kgo/consumer_group.go:1171` `joinAndSync()` | `kgo.ConsumerGroup("migrator")` at `kafka/franz_reader_ordered.go:510` | `GroupId` | none | 0–9 | the input's own group membership (classic protocol) |
| S5 | SyncGroup (14) | source | `kgo/consumer_group.go:1219` `joinAndSync()` | same | `GroupId` | none | 0–5 | |
| S6 | Heartbeat (12) | source | `kgo/consumer_group.go:972` | same | `GroupId` | none | 0–4 | |
| S7 | OffsetFetch (9) | source | `kgo/consumer_group.go:1621` `fetchOffsets()` | same, on partition assignment | `GroupId` / `Groups[]` | as D2 | as D2 | resume position of the `migrator` group |
| S8 | OffsetCommit (8) | source | `kgo/consumer_group.go:2884` `commit()` | `kgo.AutoCommitMarks()` (`kafka/franz_reader_ordered.go:511`) | `GroupId` | none | 0–9 | commit the migrator's input progress |
| S9 | LeaveGroup (13) | source | `kgo/consumer_group.go:569` `leave()` | same, on close | `GroupId` | none | 0–5 | |

KIP-848 (ConsumerGroupHeartbeat, key 68) is **not** used. `kgo/consumer_group_848.go:20-23` `should848()` returns false unless the client context has `opt_in_kafka_next_gen_balancer_beta`, and Connect v4.100.0 never sets it (checked with grep).

### Destination-side calls with no group ID (the proxy must pass these through unchanged)

| API | Where |
|---|---|
| ApiVersions, Metadata | client bootstrap; `dstAdm.Metadata` at `mig/migrator.go:549`; `ListTopics` at `mig/migrator_groups.go:712` (via `fillTopicIDs`, `:418`) |
| Produce, InitProducerID (idempotent, `TransactionalID=nil`) | `kafka/franz_writer.go` (no `TransactionalID` option anywhere) |
| CreateTopics, DescribeConfigs, CreateACLs, etc. | topic sync, `mig/migrator_topic.go`. ACLs are **topic-resource only** (`aclBuilderFromDescribed`, `mig/migrator_topic.go:564`), so no group ACLs are created |
| ListOffsets | `dstAdm.ListEndOffsets` `mig/migrator_groups.go:422`; `dstAdm.ListOffsetsAfterMilli` `mig/migrator_groups.go:755` |
| Fetch | `readRecordAtOffset(m.dst, …)` `mig/migrator_groups.go:824` → `:886` (exact offset translation, only when `offset_header` is set and the group is `Empty`) |

## Call sequence per sync cycle

`groupsMigrator.SyncLoop` (`mig/migrator_groups.go:308`) calls `Sync` every `consumer_groups.interval`. The first call happens after one interval, not at startup. One cycle for group `app-group`:

**Source side (src client, direct):**
1. `ListGroups` → every source broker (S1), `:245`
2. Filter groups by include/exclude regex and state (`:249-270`)
3. `FindCoordinator` (GROUP, keys = the remaining groups) (S2), then `OffsetFetch` for those groups (S3), `:273`
4. Drop the input's own group (`SkipSourceGroup`, `:365`) and partitions already synced (`:378`)
5. `Metadata` (ListTopics, topic IDs), `ListOffsets` start and end (`:397-408`)
6. Per group partition, in parallel: `Fetch` of the source record at `offset-1` (`:747`)

**Destination side (dst client, via proxy):**

7. `Metadata` (ListTopics, dst topic IDs) (`:418`), `ListOffsets` end (`:422`)
8. Per group partition, in parallel: `ListOffsets` by timestamp (`:755`), and optionally `Fetch` for the exact-offset lookup (`:824`)
9. **`FindCoordinator` (GROUP, `app-group`) → `OffsetFetch` (`app-group`, one entry per group partition, see below)** (D1, D2), `:516`
10. **`FindCoordinator` (cached, usually skipped) → `OffsetCommit` (`app-group`)**, one request per group, in parallel (D1, D3), `:577`

The `migrator` input group runs its own classic-protocol loop against the source the whole time (S4–S9).

## Destination-side contract for the Step 2 filter

Must handle, based on what the migrator actually sends:

| API | Request: add prefix | Response: strip prefix |
|---|---|---|
| FindCoordinator (10), all of v0–6 | `Key` (v0–3) / each `CoordinatorKeys[]` (v4+), **only when `KeyType == 0`** | v4+: each `Coordinators[].Key` (GROUP requests only). v0–3 responses have no key field |
| OffsetFetch (9), all of v0–9 | `GroupId` (v0–7) / each `Groups[].GroupId` (v8+) | v8+: each `Groups[].GroupId`. v0–7 responses have no group field |
| OffsetCommit (8), all of v0–9 | `GroupId` | nothing |

The spec lists more APIs for defensive coverage (DescribeGroups, ListGroups, DeleteGroups, OffsetDelete, JoinGroup/SyncGroup/Heartbeat/LeaveGroup/TxnOffsetCommit). The migrator doesn't use them, but Step 2 tests exercise them with kadm, so implement them anyway.

Both code paths are reachable in practice, so the filter must handle both the legacy single-group fields and the batched v4+/v8+ fields:
- With only one group being migrated, D1 carries a single key and goes out unpinned (normally at the highest supported version).
- D2 is **pinned to v8+ as soon as the request has more than one group entry**. That is the normal case, because `extractGroupNames(gcos)` (`mig/migrator_groups.go:693-698`) returns **one entry per group partition, with duplicates** (e.g. B's `app-group` has offsets on `orders/0` and `payments/0` → `Groups = [app-group, app-group]`). The filter must prefix every entry separately and must not deduplicate.
- If the negotiated ApiVersions (after the Kroxylicious cap) don't reach v8/v4, kgo falls back to one request per group/key with pins ≤7/≤3.

## How the migrator matches responses to groups

Stripping the prefix on responses is **required**, not cosmetic. Two places key off the group name **taken from the response**:

1. **FindCoordinator v4+**: `kgo/client.go:1944` looks up `key2load[rc.Key]`. If the broker returns `a_app-group`, the lookup misses, the load stays at its initial error `"coordinator was not returned in broker response"` (`kgo/client.go:1898`), and the key is removed from the cache (`:1960-1966`). That error then surfaces:
   - for D3, as `CommitOffsets` returning an error → log `Consumer group migration: failed to update offsets for group 'app-group': coordinator was not returned in broker response` (`mig/migrator_groups.go:579`)
   - for D2, as a per-group `Err` in `FetchManyOffsets`, which the migrator **ignores** (see 2).
2. **OffsetFetch v8+**: `kadm/groups.go:1148` stores results as `fetched[g.Group]`, using the response's group name. The migrator then reads `dstOffsets[g]` with the **source** name (`mig/migrator_groups.go:532`). If the prefix isn't stripped, the lookup returns an empty value, `Lookup` gives `ok=false`, and the **no-rewind check is silently skipped**. There is no error or log line; the destination offset can simply move backward. kgo also calls `maybeDeleteStaleCoordinator(group.Group, …)` with the response name (`kgo/client.go:3318`), so coordinator-error handling would target the wrong cache key.

OffsetCommit responses have no group field. The migrator matches the commit response by topic/partition against the `offsets` it sent (`kadm/groups.go:834-858`), so nothing needs stripping.

v0–3 FindCoordinator and v0–7 OffsetFetch responses have no name field. kgo fills in the name from its **own unprefixed request** (`kgo/client.go:3424`, `:3161`), so these versions work with request-side prefixing alone.

Because the D2 failure mode is silent, the Step 2 tests must check the `FetchOffsets`/`FetchManyOffsets` result map explicitly (the group key is present, `Err == nil`, the offset is correct), and must cover both the single-entry and multi-entry (v8+) request shapes.

## Does the migrator list or describe destination groups?

**No.** `listGroupsOffsets(ctx, adm, …)` takes an admin client parameter, but it is only called with `m.srcAdm` (`mig/migrator_groups.go:240`), and its `FetchManyOffsets` call hard-codes `m.srcAdm` anyway (`:273`). The destination is never enumerated. The migrator only reads offsets for groups it already knows from the source, by name (D2). `filterListGroups`/ListGroups handling has no effect on the migrator. It only matters for other clients connecting through the proxy (and for the Step 2 test).

Include/exclude regexes (`consumer_groups.include/exclude`) are applied to **source** names (`:249`), before any prefixing.

## Reproduction of the original problem (no proxy)

Tracing the code gives a different expected outcome for the no-proxy case than the spec assumes, so Step 3's negative control has to be observed rather than presumed:

- Both migrators commit to the same destination group `app-group` (D3 with no prefix). Topic names **are** separated by `topic: 'a_${! @kafka_topic }'`, and the commits are keyed by destination topic (`mig/migrator_groups.go:528`, `544`). So A commits `app-group/a_orders/*` and B commits `app-group/b_orders/*` and `app-group/b_payments/*`. These **don't overwrite each other at the partition level**. The rewind check (`:532`) also looks up by the prefixed destination topic, so it doesn't interfere either.
- The expected wrong outcome is therefore **one merged destination group `app-group` holding offsets for both sources' topics**, most likely with **no errors in the migrator logs** while the destination group stays `Empty`.
- Visible errors are plausible once applications actually consume from the destination under `app-group`: A-side and B-side apps would share one group (and rebalance each other). Once the group is `Stable`, the migrator's generation `-1` admin commits (D3) should be rejected by the coordinator. The migrator would log `Consumer group migration: failed to update offset for group 'app-group' topic '…' partition N: <kerr>` (`mig/migrator_groups.go:597`). The exact error code has to be observed in Step 3.
- Partition-level overwrites would only happen if both sources mapped to the **same destination topic name** (no topic prefix). Then A and B would race on `app-group/orders/p`, and the rewind check would keep whichever offset is higher.

Log lines to grep in Step 3 (group sync code paths):
- `Consumer group migration: sync error:` (`:330`)
- `Consumer group migration: failed to update offsets for group` (`:579`)
- `Consumer group migration: failed to update offset for group` (`:597`, `:629`)
- `failed to translate offset` (`:504`)
- `exact offset translation:` (`:451`, WARN)
- `not found in source cluster - skipping` (`:475`, `:483`)
