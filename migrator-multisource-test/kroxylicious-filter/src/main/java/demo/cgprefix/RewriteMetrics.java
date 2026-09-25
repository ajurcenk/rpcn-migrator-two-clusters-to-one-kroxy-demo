package demo.cgprefix;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

/**
 * Counts group ID rewrites per API and direction, on the proxy's global Micrometer registry (the
 * same registry the proxy's Prometheus endpoint scrapes; record-encryption registers its meters
 * there too). Exposed as {@code kroxylicious_consumer_group_prefix_rewrites_total}.
 */
final class RewriteMetrics {

    static final String METER_NAME = "kroxylicious_consumer_group_prefix_rewrites";

    enum Direction {
        REQUEST,
        RESPONSE
    }

    private final MeterRegistry registry;
    private final String prefix;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    RewriteMetrics(MeterRegistry registry, String prefix) {
        this.registry = registry;
        this.prefix = prefix;
    }

    static RewriteMetrics forPrefix(String prefix) {
        return new RewriteMetrics(Metrics.globalRegistry, prefix);
    }

    void increment(ApiKeys api, Direction direction) {
        counters.computeIfAbsent(api.name() + '/' + direction, k -> Counter.builder(METER_NAME)
                .description("Consumer group IDs rewritten by the ConsumerGroupPrefix filter")
                .tag("api", api.name())
                .tag("direction", direction.name().toLowerCase())
                .tag("prefix", prefix)
                .register(registry))
                .increment();
    }
}
