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

### Open questions for Step 2

1. **Kroxylicious ApiVersions cap.** Which max versions does the Kroxylicious build in use advertise for FindCoordinator, OffsetFetch and OffsetCommit, compared with kmsg v1.12.0 (6 / 9 / 9)? Capped versions change which wire shape the migrator sends, but the filter must handle all shapes anyway.
2. **Redpanda OffsetFetch v8+ with duplicate `Groups[]` entries.** Does the broker reply with one entry per requested entry, or deduplicate? Either works for kadm (it writes into a map), but the filter's response rewrite must not assume a 1:1 index correspondence with the request.
3. **The exact error code** Redpanda returns for a generation `-1` OffsetCommit to a `Stable` group (negative control, variant a).
