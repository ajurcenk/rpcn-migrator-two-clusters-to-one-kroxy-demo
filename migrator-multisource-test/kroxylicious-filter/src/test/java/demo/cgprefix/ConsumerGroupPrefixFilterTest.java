package demo.cgprefix;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.ByteBufferAccessor;
import io.kroxylicious.kafka.common.protocol.ObjectSerializationCache;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.testing.filter.context.MockFilterContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupPrefixFilterTest {

    private static final String PREFIX = "a_";
    private static final byte KEY_TYPE_GROUP = 0;
    private static final byte KEY_TYPE_TRANSACTION = 1;

    private SimpleMeterRegistry registry;
    private ConsumerGroupPrefixFilter filter;

    @BeforeEach
    void beforeEach() {
        filter = newFilter(true, true);
    }

    private ConsumerGroupPrefixFilter newFilter(boolean stripOnResponse, boolean filterListGroups) {
        registry = new SimpleMeterRegistry();
        return new ConsumerGroupPrefixFilter(new ConsumerGroupPrefixConfig(PREFIX, stripOnResponse, filterListGroups),
                new RewriteMetrics(registry, PREFIX));
    }

    // ---- FindCoordinator ----

    static Stream<Short> findCoordinatorVersions() {
        return versions(FindCoordinatorRequestData.LOWEST_SUPPORTED_VERSION, FindCoordinatorRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("findCoordinatorVersions")
    void findCoordinatorGroupLookupIsPrefixedAndStripped(short v) {
        var request = new FindCoordinatorRequestData().setKeyType(KEY_TYPE_GROUP);
        if (v <= 3) {
            request.setKey("app-group");
        }
        else {
            request.setCoordinatorKeys(List.of("app-group", "other-group"));
        }

        var onWire = onRequest(request, v, 7, FindCoordinatorRequestData::new,
                (h, r, c) -> filter.onFindCoordinatorRequest(v, h, r, c));
        if (v <= 3) {
            assertThat(onWire.key()).isEqualTo("a_app-group");
        }
        else {
            assertThat(onWire.coordinatorKeys()).containsExactly("a_app-group", "a_other-group");
        }

        var response = new FindCoordinatorResponseData();
        if (v >= 4) {
            response.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_app-group").setNodeId(0).setHost("h").setPort(9092));
            response.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_other-group").setNodeId(0).setHost("h").setPort(9092));
        }
        var toClient = onResponse(response, v, 7, FindCoordinatorResponseData::new,
                (h, r, c) -> filter.onFindCoordinatorResponse(v, h, r, c));
        if (v >= 4) {
            assertThat(toClient.coordinators()).extracting(FindCoordinatorResponseData.Coordinator::key).containsExactly("app-group", "other-group");
        }
    }

    @Test
    void findCoordinatorTransactionLookupIsUntouchedEvenWhenInterleavedWithGroupLookups() {
        short v = 4;
        var groupReq = new FindCoordinatorRequestData().setKeyType(KEY_TYPE_GROUP).setCoordinatorKeys(List.of("app-group"));
        // A transactional ID that happens to start with the prefix must not be stripped either.
        var txnReq = new FindCoordinatorRequestData().setKeyType(KEY_TYPE_TRANSACTION).setCoordinatorKeys(List.of("a_txn"));

        assertThat(onRequest(groupReq, v, 1, FindCoordinatorRequestData::new, (h, r, c) -> filter.onFindCoordinatorRequest(v, h, r, c)).coordinatorKeys())
                .containsExactly("a_app-group");
        assertThat(onRequest(txnReq, v, 2, FindCoordinatorRequestData::new, (h, r, c) -> filter.onFindCoordinatorRequest(v, h, r, c)).coordinatorKeys())
                .containsExactly("a_txn");

        // Responses arrive in the opposite order to the requests.
        var txnResp = new FindCoordinatorResponseData();
        txnResp.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_txn"));
        assertThat(onResponse(txnResp, v, 2, FindCoordinatorResponseData::new, (h, r, c) -> filter.onFindCoordinatorResponse(v, h, r, c)).coordinators().get(0).key())
                .isEqualTo("a_txn");

        var groupResp = new FindCoordinatorResponseData();
        groupResp.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_app-group"));
        assertThat(onResponse(groupResp, v, 1, FindCoordinatorResponseData::new, (h, r, c) -> filter.onFindCoordinatorResponse(v, h, r, c)).coordinators().get(0).key())
                .isEqualTo("app-group");
    }

    @Test
    void findCoordinatorSingleKeyTransactionLookupIsUntouched() {
        short v = 3;
        var txnReq = new FindCoordinatorRequestData().setKeyType(KEY_TYPE_TRANSACTION).setKey("txn-1");
        assertThat(onRequest(txnReq, v, 1, FindCoordinatorRequestData::new, (h, r, c) -> filter.onFindCoordinatorRequest(v, h, r, c)).key())
                .isEqualTo("txn-1");
    }

    // ---- OffsetFetch ----

    static Stream<Short> offsetFetchVersions() {
        return versions(OffsetFetchRequestData.LOWEST_SUPPORTED_VERSION, OffsetFetchRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("offsetFetchVersions")
    void offsetFetchIsPrefixedAndStripped(short v) {
        var request = new OffsetFetchRequestData();
        if (v <= 7) {
            request.setGroupId("app-group");
        }
        else {
            // The migrator sends one entry per group partition, so duplicates are normal.
            request.setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("app-group"),
                    new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("app-group"),
                    new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("billing")));
        }

        var onWire = onRequest(request, v, 1, OffsetFetchRequestData::new, (h, r, c) -> filter.onOffsetFetchRequest(v, h, r, c));
        if (v <= 7) {
            assertThat(onWire.groupId()).isEqualTo("a_app-group");
        }
        else {
            assertThat(onWire.groups()).extracting(OffsetFetchRequestData.OffsetFetchRequestGroup::groupId)
                    .containsExactly("a_app-group", "a_app-group", "a_billing");
        }

        var response = new OffsetFetchResponseData();
        if (v >= 8) {
            response.setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("a_app-group"),
                    new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("a_billing")));
        }
        var toClient = onResponse(response, v, 1, OffsetFetchResponseData::new, (h, r, c) -> filter.onOffsetFetchResponse(v, h, r, c));
        if (v >= 8) {
            assertThat(toClient.groups()).extracting(OffsetFetchResponseData.OffsetFetchResponseGroup::groupId).containsExactly("app-group", "billing");
        }
    }

    // ---- OffsetCommit ----

    static Stream<Short> offsetCommitVersions() {
        return versions(OffsetCommitRequestData.LOWEST_SUPPORTED_VERSION, OffsetCommitRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("offsetCommitVersions")
    void offsetCommitIsPrefixed(short v) {
        var onWire = onRequest(new OffsetCommitRequestData().setGroupId("app-group"), v, 1, OffsetCommitRequestData::new,
                (h, r, c) -> filter.onOffsetCommitRequest(v, h, r, c));
        assertThat(onWire.groupId()).isEqualTo("a_app-group");
    }

    // ---- DescribeGroups / DeleteGroups / OffsetDelete / ListGroups ----

    static Stream<Short> describeGroupsVersions() {
        return versions(DescribeGroupsRequestData.LOWEST_SUPPORTED_VERSION, DescribeGroupsRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("describeGroupsVersions")
    void describeGroupsIsPrefixedAndStripped(short v) {
        var onWire = onRequest(new DescribeGroupsRequestData().setGroups(List.of("app-group", "billing")), v, 1, DescribeGroupsRequestData::new,
                (h, r, c) -> filter.onDescribeGroupsRequest(v, h, r, c));
        assertThat(onWire.groups()).containsExactly("a_app-group", "a_billing");

        var response = new DescribeGroupsResponseData().setGroups(List.of(
                new DescribeGroupsResponseData.DescribedGroup().setGroupId("a_app-group"),
                new DescribeGroupsResponseData.DescribedGroup().setGroupId("a_billing")));
        var toClient = onResponse(response, v, 1, DescribeGroupsResponseData::new, (h, r, c) -> filter.onDescribeGroupsResponse(v, h, r, c));
        assertThat(toClient.groups()).extracting(DescribeGroupsResponseData.DescribedGroup::groupId).containsExactly("app-group", "billing");
    }

    static Stream<Short> deleteGroupsVersions() {
        return versions(DeleteGroupsRequestData.LOWEST_SUPPORTED_VERSION, DeleteGroupsRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("deleteGroupsVersions")
    void deleteGroupsIsPrefixedAndStripped(short v) {
        var onWire = onRequest(new DeleteGroupsRequestData().setGroupsNames(List.of("app-group")), v, 1, DeleteGroupsRequestData::new,
                (h, r, c) -> filter.onDeleteGroupsRequest(v, h, r, c));
        assertThat(onWire.groupsNames()).containsExactly("a_app-group");

        var response = new DeleteGroupsResponseData();
        response.results().add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("a_app-group"));
        response.results().add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("a_billing").setErrorCode((short) 69));
        var toClient = onResponse(response, v, 1, DeleteGroupsResponseData::new, (h, r, c) -> filter.onDeleteGroupsResponse(v, h, r, c));
        assertThat(toClient.results()).extracting(DeleteGroupsResponseData.DeletableGroupResult::groupId).containsExactly("app-group", "billing");
        // The collection is hashed on groupId: lookups by the stripped name must work.
        assertThat(toClient.results().find("billing").errorCode()).isEqualTo((short) 69);
    }

    @Test
    void offsetDeleteIsPrefixed() {
        short v = OffsetDeleteRequestData.HIGHEST_SUPPORTED_VERSION;
        var onWire = onRequest(new OffsetDeleteRequestData().setGroupId("app-group"), v, 1, OffsetDeleteRequestData::new,
                (h, r, c) -> filter.onOffsetDeleteRequest(v, h, r, c));
        assertThat(onWire.groupId()).isEqualTo("a_app-group");
    }

    static Stream<Short> listGroupsVersions() {
        return versions(ListGroupsResponseData.LOWEST_SUPPORTED_VERSION, ListGroupsResponseData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("listGroupsVersions")
    void listGroupsHidesUnprefixedGroupsAndStripsTheRest(short v) {
        var toClient = onResponse(listGroups("a_app-group", "other", "b_app-group", "a_a_x"), v, 1, ListGroupsResponseData::new,
                (h, r, c) -> filter.onListGroupsResponse(v, h, r, c));
        assertThat(toClient.groups()).extracting(ListGroupsResponseData.ListedGroup::groupId).containsExactly("app-group", "a_x");
    }

    @Test
    void listGroupsPassesThroughWhenFilteringDisabled() {
        var unfiltered = newFilter(true, false);
        short v = ListGroupsResponseData.HIGHEST_SUPPORTED_VERSION;
        var toClient = onResponse(listGroups("a_app-group", "other"), v, 1, ListGroupsResponseData::new,
                (h, r, c) -> unfiltered.onListGroupsResponse(v, h, r, c));
        assertThat(toClient.groups()).extracting(ListGroupsResponseData.ListedGroup::groupId).containsExactly("a_app-group", "other");
    }

    // ---- ConsumerGroupDescribe (KIP-848 admin) ----

    static Stream<Short> consumerGroupDescribeVersions() {
        return versions(ConsumerGroupDescribeRequestData.LOWEST_SUPPORTED_VERSION, ConsumerGroupDescribeRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    @ParameterizedTest
    @MethodSource("consumerGroupDescribeVersions")
    void consumerGroupDescribeIsPrefixedAndStripped(short v) {
        var onWire = onRequest(new ConsumerGroupDescribeRequestData().setGroupIds(List.of("app-group")), v, 1, ConsumerGroupDescribeRequestData::new,
                (h, r, c) -> filter.onConsumerGroupDescribeRequest(v, h, r, c));
        assertThat(onWire.groupIds()).containsExactly("a_app-group");

        var response = new ConsumerGroupDescribeResponseData().setGroups(List.of(new ConsumerGroupDescribeResponseData.DescribedGroup().setGroupId("a_app-group")));
        var toClient = onResponse(response, v, 1, ConsumerGroupDescribeResponseData::new, (h, r, c) -> filter.onConsumerGroupDescribeResponse(v, h, r, c));
        assertThat(toClient.groups().get(0).groupId()).isEqualTo("app-group");
    }

    // ---- membership / transactional APIs (defensive) ----

    @Test
    void membershipAndTransactionalApisArePrefixed() {
        short join = JoinGroupRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new JoinGroupRequestData().setGroupId("g").setProtocolType("consumer"), join, 1, JoinGroupRequestData::new,
                (h, r, c) -> filter.onJoinGroupRequest(join, h, r, c)).groupId()).isEqualTo("a_g");
        short sync = SyncGroupRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new SyncGroupRequestData().setGroupId("g").setProtocolType("consumer").setProtocolName("range"), sync, 2, SyncGroupRequestData::new,
                (h, r, c) -> filter.onSyncGroupRequest(sync, h, r, c)).groupId()).isEqualTo("a_g");
        short hb = HeartbeatRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new HeartbeatRequestData().setGroupId("g"), hb, 3, HeartbeatRequestData::new,
                (h, r, c) -> filter.onHeartbeatRequest(hb, h, r, c)).groupId()).isEqualTo("a_g");
        short leave = LeaveGroupRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new LeaveGroupRequestData().setGroupId("g"), leave, 4, LeaveGroupRequestData::new,
                (h, r, c) -> filter.onLeaveGroupRequest(leave, h, r, c)).groupId()).isEqualTo("a_g");
        short txn = TxnOffsetCommitRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new TxnOffsetCommitRequestData().setGroupId("g").setTransactionalId("t"), txn, 5, TxnOffsetCommitRequestData::new,
                (h, r, c) -> filter.onTxnOffsetCommitRequest(txn, h, r, c)).groupId()).isEqualTo("a_g");
        short add = AddOffsetsToTxnRequestData.HIGHEST_SUPPORTED_VERSION;
        var addOnWire = onRequest(new AddOffsetsToTxnRequestData().setGroupId("g").setTransactionalId("t"), add, 6, AddOffsetsToTxnRequestData::new,
                (h, r, c) -> filter.onAddOffsetsToTxnRequest(add, h, r, c));
        assertThat(addOnWire.groupId()).isEqualTo("a_g");
        assertThat(addOnWire.transactionalId()).isEqualTo("t");
        short cgh = ConsumerGroupHeartbeatRequestData.HIGHEST_SUPPORTED_VERSION;
        assertThat(onRequest(new ConsumerGroupHeartbeatRequestData().setGroupId("g").setMemberId("m"), cgh, 7, ConsumerGroupHeartbeatRequestData::new,
                (h, r, c) -> filter.onConsumerGroupHeartbeatRequest(cgh, h, r, c)).groupId()).isEqualTo("a_g");
    }

    // ---- edge cases ----

    @Test
    void groupAlreadyStartingWithPrefixIsPrefixedAgainAndRoundTrips() {
        short v = 9;
        var onWire = onRequest(new OffsetFetchRequestData().setGroups(List.of(new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("a_x"))),
                v, 1, OffsetFetchRequestData::new, (h, r, c) -> filter.onOffsetFetchRequest(v, h, r, c));
        assertThat(onWire.groups().get(0).groupId()).isEqualTo("a_a_x");

        var response = new OffsetFetchResponseData().setGroups(List.of(new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("a_a_x")));
        assertThat(onResponse(response, v, 1, OffsetFetchResponseData::new, (h, r, c) -> filter.onOffsetFetchResponse(v, h, r, c)).groups().get(0).groupId())
                .isEqualTo("a_x");
    }

    @Test
    void emptyGroupIdIsPrefixedAndRoundTrips() {
        short v = 6;
        var onWire = onRequest(new FindCoordinatorRequestData().setKeyType(KEY_TYPE_GROUP).setCoordinatorKeys(List.of("")), v, 1,
                FindCoordinatorRequestData::new, (h, r, c) -> filter.onFindCoordinatorRequest(v, h, r, c));
        assertThat(onWire.coordinatorKeys()).containsExactly("a_");

        var response = new FindCoordinatorResponseData();
        response.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_"));
        assertThat(onResponse(response, v, 1, FindCoordinatorResponseData::new, (h, r, c) -> filter.onFindCoordinatorResponse(v, h, r, c)).coordinators().get(0).key())
                .isEmpty();

        short oc = 9;
        assertThat(onRequest(new OffsetCommitRequestData().setGroupId(""), oc, 2, OffsetCommitRequestData::new,
                (h, r, c) -> filter.onOffsetCommitRequest(oc, h, r, c)).groupId()).isEqualTo("a_");
    }

    @Test
    void responseGroupWithoutPrefixIsLeftUnchanged() {
        short v = 5;
        var response = new DescribeGroupsResponseData().setGroups(List.of(new DescribeGroupsResponseData.DescribedGroup().setGroupId("unexpected")));
        assertThat(onResponse(response, v, 1, DescribeGroupsResponseData::new, (h, r, c) -> filter.onDescribeGroupsResponse(v, h, r, c)).groups().get(0).groupId())
                .isEqualTo("unexpected");
    }

    @Test
    void stripOnResponseDisabledLeavesResponsesPrefixed() {
        var noStrip = newFilter(false, true);
        short v = 6;
        onRequest(new FindCoordinatorRequestData().setKeyType(KEY_TYPE_GROUP).setCoordinatorKeys(List.of("app-group")), v, 1,
                FindCoordinatorRequestData::new, (h, r, c) -> noStrip.onFindCoordinatorRequest(v, h, r, c));
        var response = new FindCoordinatorResponseData();
        response.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey("a_app-group"));
        assertThat(onResponse(response, v, 1, FindCoordinatorResponseData::new, (h, r, c) -> noStrip.onFindCoordinatorResponse(v, h, r, c)).coordinators().get(0).key())
                .isEqualTo("a_app-group");

        short lg = 5;
        assertThat(onResponse(listGroups("a_app-group", "other"), lg, 2, ListGroupsResponseData::new, (h, r, c) -> noStrip.onListGroupsResponse(lg, h, r, c)).groups())
                .extracting(ListGroupsResponseData.ListedGroup::groupId).containsExactly("a_app-group");
    }

    @Test
    void rewritesAreCountedPerApiAndDirection() {
        short v = 9;
        onRequest(new OffsetFetchRequestData().setGroups(List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("g1"),
                new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("g2"))),
                v, 1, OffsetFetchRequestData::new, (h, r, c) -> filter.onOffsetFetchRequest(v, h, r, c));
        onResponse(new OffsetFetchResponseData().setGroups(List.of(new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("a_g1"))),
                v, 1, OffsetFetchResponseData::new, (h, r, c) -> filter.onOffsetFetchResponse(v, h, r, c));

        assertThat(count("OFFSET_FETCH", "request")).isEqualTo(2.0);
        assertThat(count("OFFSET_FETCH", "response")).isEqualTo(1.0);
    }

    // ---- helpers ----

    @FunctionalInterface
    interface RequestCall<M extends ApiMessage> {
        java.util.concurrent.CompletionStage<RequestFilterResult> apply(RequestHeaderData header, M request, MockFilterContext context);
    }

    @FunctionalInterface
    interface ResponseCall<M extends ApiMessage> {
        java.util.concurrent.CompletionStage<ResponseFilterResult> apply(ResponseHeaderData header, M response, MockFilterContext context);
    }

    /**
     * Decodes the request at {@code version} (as the proxy would), runs the filter, and re-decodes
     * what it forwards at the same version (as the broker would).
     */
    @SuppressWarnings("unchecked")
    private static <M extends ApiMessage> M onRequest(M request, short version, int correlationId, Supplier<M> fresh, RequestCall<M> call) {
        M decoded = roundTrip(request, version, fresh);
        var header = new RequestHeaderData().setCorrelationId(correlationId).setRequestApiVersion(version);
        var context = MockFilterContext.builder(header, decoded).build();
        var result = call.apply(header, decoded, context).toCompletableFuture().join();
        assertThat(result.drop()).isFalse();
        return roundTrip((M) result.message(), version, fresh);
    }

    @SuppressWarnings("unchecked")
    private static <M extends ApiMessage> M onResponse(M response, short version, int correlationId, Supplier<M> fresh, ResponseCall<M> call) {
        M decoded = roundTrip(response, version, fresh);
        var header = new ResponseHeaderData().setCorrelationId(correlationId);
        var context = MockFilterContext.builder(header, decoded).build();
        var result = call.apply(header, decoded, context).toCompletableFuture().join();
        assertThat(result.drop()).isFalse();
        return roundTrip((M) result.message(), version, fresh);
    }

    private static <M extends ApiMessage> M roundTrip(M message, short version, Supplier<M> fresh) {
        var cache = new ObjectSerializationCache();
        var buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version);
        buffer.flip();
        M copy = fresh.get();
        copy.read(new ByteBufferAccessor(buffer), version);
        return copy;
    }

    private static ListGroupsResponseData listGroups(String... ids) {
        return new ListGroupsResponseData().setGroups(Stream.of(ids).map(id -> new ListGroupsResponseData.ListedGroup().setGroupId(id).setProtocolType("consumer")).toList());
    }

    private double count(String api, String direction) {
        var counter = registry.find(RewriteMetrics.METER_NAME).tag("api", api).tag("direction", direction).tag("prefix", PREFIX).counter();
        return counter == null ? 0 : counter.count();
    }

    private static Stream<Short> versions(short lowest, short highest) {
        return IntStream.rangeClosed(lowest, highest).mapToObj(i -> (short) i);
    }
}
