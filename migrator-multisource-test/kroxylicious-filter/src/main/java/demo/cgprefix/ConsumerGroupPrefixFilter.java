package demo.cgprefix;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData;
import io.kroxylicious.kafka.common.message.ConsumerGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.ConsumerGroupDescribeResponseData;
import io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.DeleteGroupsRequestData;
import io.kroxylicious.kafka.common.message.DeleteGroupsResponseData;
import io.kroxylicious.kafka.common.message.DescribeGroupsRequestData;
import io.kroxylicious.kafka.common.message.DescribeGroupsResponseData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.FindCoordinatorResponseData;
import io.kroxylicious.kafka.common.message.HeartbeatRequestData;
import io.kroxylicious.kafka.common.message.JoinGroupRequestData;
import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetDeleteRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SyncGroupRequestData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.filter.AddOffsetsToTxnRequestFilter;
import io.kroxylicious.proxy.filter.ConsumerGroupDescribeRequestFilter;
import io.kroxylicious.proxy.filter.ConsumerGroupDescribeResponseFilter;
import io.kroxylicious.proxy.filter.ConsumerGroupHeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.DeleteGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteGroupsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FindCoordinatorRequestFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.HeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.JoinGroupRequestFilter;
import io.kroxylicious.proxy.filter.LeaveGroupRequestFilter;
import io.kroxylicious.proxy.filter.ListGroupsResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SyncGroupRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitRequestFilter;

import demo.cgprefix.RewriteMetrics.Direction;

/**
 * Adds a fixed prefix to every consumer group ID a client sends, and strips it from every group ID
 * the broker sends back, so the client only ever sees its own unprefixed names while the backend
 * stores them as {@code <prefix><name>}.
 * <p>
 * The prefix is always added, even to a name that already starts with it: a client group
 * {@code a_x} becomes {@code a_a_x} on the backend and comes back as {@code a_x}. This keeps the
 * mapping injective, so two clients using different prefixes can never collide. Empty group IDs
 * are prefixed like any other name for the same reason.
 * <p>
 * Where a field only exists in some versions of an API (FindCoordinator {@code key} v0-3 vs
 * {@code coordinatorKeys} v4+, OffsetFetch {@code groupId} v0-7 vs {@code groups} v8+), the field
 * is chosen by {@code apiVersion}, i.e. only the field actually on the wire is rewritten.
 * <p>
 * The Redpanda Migrator only sends FindCoordinator, OffsetFetch and OffsetCommit to the destination
 * (see docs/migrator-consumer-group-api-calls.md). The remaining group APIs are rewritten for safety;
 * the ones only a group member would send are additionally logged at WARN, once per connection.
 * <p>
 * Not covered: share groups and streams groups (KIP-932 / KIP-1071 APIs), and ACL or config APIs
 * addressing a GROUP resource. Those pass through unprefixed.
 */
class ConsumerGroupPrefixFilter implements
        FindCoordinatorRequestFilter, FindCoordinatorResponseFilter,
        OffsetCommitRequestFilter,
        OffsetFetchRequestFilter, OffsetFetchResponseFilter,
        DescribeGroupsRequestFilter, DescribeGroupsResponseFilter,
        DeleteGroupsRequestFilter, DeleteGroupsResponseFilter,
        OffsetDeleteRequestFilter,
        ListGroupsResponseFilter,
        JoinGroupRequestFilter, SyncGroupRequestFilter, HeartbeatRequestFilter, LeaveGroupRequestFilter,
        TxnOffsetCommitRequestFilter, AddOffsetsToTxnRequestFilter,
        ConsumerGroupHeartbeatRequestFilter,
        ConsumerGroupDescribeRequestFilter, ConsumerGroupDescribeResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupPrefixFilter.class);

    /** FindCoordinator {@code keyType} for a consumer group (1 is a transactional ID). */
    private static final byte COORDINATOR_TYPE_GROUP = 0;
    /** Last FindCoordinator version with the singular {@code key} field. */
    private static final short FIND_COORDINATOR_LAST_SINGLE_KEY_VERSION = 3;
    /** Last OffsetFetch version with the singular {@code groupId} field. */
    private static final short OFFSET_FETCH_LAST_SINGLE_GROUP_VERSION = 7;

    private final String prefix;
    private final boolean stripOnResponse;
    private final boolean filterListGroups;
    private final RewriteMetrics metrics;

    /**
     * FindCoordinator responses don't say whether an entry is a group or a transactional ID, so the
     * correlation IDs of in-flight GROUP lookups are remembered between request and response. A
     * filter instance serves one connection and sees its messages sequentially, so no locking.
     */
    private final Set<Integer> pendingGroupLookups = new HashSet<>();
    private final Set<ApiKeys> warnedApis = EnumSet.noneOf(ApiKeys.class);

    ConsumerGroupPrefixFilter(ConsumerGroupPrefixConfig config, RewriteMetrics metrics) {
        this.prefix = config.prefix();
        this.stripOnResponse = config.stripOnResponse();
        this.filterListGroups = config.filterListGroups();
        this.metrics = metrics;
    }

    // ---- coordinator lookup ----

    @Override
    public CompletionStage<RequestFilterResult> onFindCoordinatorRequest(short apiVersion, RequestHeaderData header, FindCoordinatorRequestData request,
                                                                          FilterContext context) {
        if (request.keyType() == COORDINATOR_TYPE_GROUP) {
            pendingGroupLookups.add(header.correlationId());
            if (apiVersion <= FIND_COORDINATOR_LAST_SINGLE_KEY_VERSION) {
                request.setKey(addPrefix(ApiKeys.FIND_COORDINATOR, apiVersion, request.key()));
            }
            else {
                request.setCoordinatorKeys(addPrefix(ApiKeys.FIND_COORDINATOR, apiVersion, request.coordinatorKeys()));
            }
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFindCoordinatorResponse(short apiVersion, ResponseHeaderData header, FindCoordinatorResponseData response,
                                                                            FilterContext context) {
        // v0-3 responses carry no key; franz-go fills it in from its own (unprefixed) request.
        if (pendingGroupLookups.remove(header.correlationId()) && stripOnResponse) {
            response.coordinators().forEach(c -> c.setKey(stripPrefix(ApiKeys.FIND_COORDINATOR, apiVersion, c.key())));
        }
        return context.forwardResponse(header, response);
    }

    // ---- offsets ----

    @Override
    public CompletionStage<RequestFilterResult> onOffsetCommitRequest(short apiVersion, RequestHeaderData header, OffsetCommitRequestData request,
                                                                       FilterContext context) {
        request.setGroupId(addPrefix(ApiKeys.OFFSET_COMMIT, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetFetchRequest(short apiVersion, RequestHeaderData header, OffsetFetchRequestData request,
                                                                      FilterContext context) {
        if (apiVersion <= OFFSET_FETCH_LAST_SINGLE_GROUP_VERSION) {
            request.setGroupId(addPrefix(ApiKeys.OFFSET_FETCH, apiVersion, request.groupId()));
        }
        else {
            // Entries are rewritten independently: the migrator sends one entry per group partition,
            // so duplicates are normal and must not be collapsed.
            request.groups().forEach(g -> g.setGroupId(addPrefix(ApiKeys.OFFSET_FETCH, apiVersion, g.groupId())));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetFetchResponse(short apiVersion, ResponseHeaderData header, OffsetFetchResponseData response,
                                                                        FilterContext context) {
        // v0-7 responses carry no group ID; only v8+ batches need stripping.
        if (stripOnResponse) {
            response.groups().forEach(g -> g.setGroupId(stripPrefix(ApiKeys.OFFSET_FETCH, apiVersion, g.groupId())));
        }
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetDeleteRequest(short apiVersion, RequestHeaderData header, OffsetDeleteRequestData request,
                                                                       FilterContext context) {
        request.setGroupId(addPrefix(ApiKeys.OFFSET_DELETE, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    // ---- group admin ----

    @Override
    public CompletionStage<RequestFilterResult> onDescribeGroupsRequest(short apiVersion, RequestHeaderData header, DescribeGroupsRequestData request,
                                                                         FilterContext context) {
        request.setGroups(addPrefix(ApiKeys.DESCRIBE_GROUPS, apiVersion, request.groups()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDescribeGroupsResponse(short apiVersion, ResponseHeaderData header, DescribeGroupsResponseData response,
                                                                           FilterContext context) {
        if (stripOnResponse) {
            response.groups().forEach(g -> g.setGroupId(stripPrefix(ApiKeys.DESCRIBE_GROUPS, apiVersion, g.groupId())));
        }
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onDeleteGroupsRequest(short apiVersion, RequestHeaderData header, DeleteGroupsRequestData request,
                                                                       FilterContext context) {
        request.setGroupsNames(addPrefix(ApiKeys.DELETE_GROUPS, apiVersion, request.groupsNames()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDeleteGroupsResponse(short apiVersion, ResponseHeaderData header, DeleteGroupsResponseData response,
                                                                         FilterContext context) {
        if (stripOnResponse) {
            // results() is a collection hashed on groupId, so rebuild it rather than renaming in place.
            var rewritten = new DeleteGroupsResponseData.DeletableGroupResultCollection(response.results().size());
            response.results().forEach(r -> rewritten.add(r.duplicate().setGroupId(stripPrefix(ApiKeys.DELETE_GROUPS, apiVersion, r.groupId()))));
            response.setResults(rewritten);
        }
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListGroupsResponse(short apiVersion, ResponseHeaderData header, ListGroupsResponseData response,
                                                                       FilterContext context) {
        if (filterListGroups) {
            var visible = response.groups().stream()
                    .filter(g -> g.groupId() != null && g.groupId().startsWith(prefix))
                    .toList();
            if (stripOnResponse) {
                visible.forEach(g -> g.setGroupId(stripPrefix(ApiKeys.LIST_GROUPS, apiVersion, g.groupId())));
            }
            response.setGroups(visible);
        }
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onConsumerGroupDescribeRequest(short apiVersion, RequestHeaderData header, ConsumerGroupDescribeRequestData request,
                                                                                FilterContext context) {
        request.setGroupIds(addPrefix(ApiKeys.CONSUMER_GROUP_DESCRIBE, apiVersion, request.groupIds()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onConsumerGroupDescribeResponse(short apiVersion, ResponseHeaderData header,
                                                                                  ConsumerGroupDescribeResponseData response, FilterContext context) {
        if (stripOnResponse) {
            response.groups().forEach(g -> g.setGroupId(stripPrefix(ApiKeys.CONSUMER_GROUP_DESCRIBE, apiVersion, g.groupId())));
        }
        return context.forwardResponse(header, response);
    }

    // ---- group membership and transactions: not expected from the migrator's output ----
    // Their responses don't echo the group ID, so there is nothing to strip.

    @Override
    public CompletionStage<RequestFilterResult> onJoinGroupRequest(short apiVersion, RequestHeaderData header, JoinGroupRequestData request,
                                                                    FilterContext context) {
        warnUnexpected(ApiKeys.JOIN_GROUP);
        request.setGroupId(addPrefix(ApiKeys.JOIN_GROUP, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onSyncGroupRequest(short apiVersion, RequestHeaderData header, SyncGroupRequestData request,
                                                                    FilterContext context) {
        warnUnexpected(ApiKeys.SYNC_GROUP);
        request.setGroupId(addPrefix(ApiKeys.SYNC_GROUP, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onHeartbeatRequest(short apiVersion, RequestHeaderData header, HeartbeatRequestData request,
                                                                    FilterContext context) {
        warnUnexpected(ApiKeys.HEARTBEAT);
        request.setGroupId(addPrefix(ApiKeys.HEARTBEAT, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onLeaveGroupRequest(short apiVersion, RequestHeaderData header, LeaveGroupRequestData request,
                                                                     FilterContext context) {
        warnUnexpected(ApiKeys.LEAVE_GROUP);
        request.setGroupId(addPrefix(ApiKeys.LEAVE_GROUP, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onTxnOffsetCommitRequest(short apiVersion, RequestHeaderData header, TxnOffsetCommitRequestData request,
                                                                          FilterContext context) {
        warnUnexpected(ApiKeys.TXN_OFFSET_COMMIT);
        request.setGroupId(addPrefix(ApiKeys.TXN_OFFSET_COMMIT, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    /** Must match TxnOffsetCommit: the broker adds the group's __consumer_offsets partition to the transaction. */
    @Override
    public CompletionStage<RequestFilterResult> onAddOffsetsToTxnRequest(short apiVersion, RequestHeaderData header, AddOffsetsToTxnRequestData request,
                                                                          FilterContext context) {
        warnUnexpected(ApiKeys.ADD_OFFSETS_TO_TXN);
        request.setGroupId(addPrefix(ApiKeys.ADD_OFFSETS_TO_TXN, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onConsumerGroupHeartbeatRequest(short apiVersion, RequestHeaderData header, ConsumerGroupHeartbeatRequestData request,
                                                                                 FilterContext context) {
        warnUnexpected(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        request.setGroupId(addPrefix(ApiKeys.CONSUMER_GROUP_HEARTBEAT, apiVersion, request.groupId()));
        return context.forwardRequest(header, request);
    }

    // ---- helpers ----

    private String addPrefix(ApiKeys api, short apiVersion, String group) {
        if (group == null) {
            return null;
        }
        String rewritten = prefix + group;
        LOG.debug("api={} version={} request group '{}' -> '{}'", api, apiVersion, group, rewritten);
        metrics.increment(api, Direction.REQUEST);
        return rewritten;
    }

    private List<String> addPrefix(ApiKeys api, short apiVersion, List<String> groups) {
        return groups.stream().map(g -> addPrefix(api, apiVersion, g)).toList();
    }

    private String stripPrefix(ApiKeys api, short apiVersion, String group) {
        if (group == null) {
            return null;
        }
        if (!group.startsWith(prefix)) {
            // Every group ID in these responses was prefixed on the way in, so this means the
            // broker answered for a name we didn't ask about. Leave it as is rather than guess.
            LOG.warn("api={} version={} response group '{}' lacks prefix '{}'; leaving it unchanged", api, apiVersion, group, prefix);
            return group;
        }
        String rewritten = group.substring(prefix.length());
        LOG.debug("api={} version={} response group '{}' -> '{}'", api, apiVersion, group, rewritten);
        metrics.increment(api, Direction.RESPONSE);
        return rewritten;
    }

    private void warnUnexpected(ApiKeys api) {
        if (warnedApis.add(api)) {
            LOG.warn("api={} seen on a ConsumerGroupPrefix connection (prefix '{}'); the Redpanda Migrator output is not expected to send it. "
                    + "Prefixing the group ID anyway. Logged once per connection.", api, prefix);
        }
    }
}
