package demo.cgprefix;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

/**
 * Filter factory for {@link ConsumerGroupPrefixFilter}.
 * <p>
 * Same direction as Kroxylicious's built-in {@code MultiTenant} filter (prefix on the way in, strip
 * on the way out), but it only touches consumer group IDs: topic names, transactional IDs and
 * everything else pass through unchanged. That keeps it compatible with the Redpanda Migrator's own
 * {@code topic: 'a_${! @kafka_topic }'} topic prefixing.
 */
@Plugin(configType = ConsumerGroupPrefixConfig.class)
public class ConsumerGroupPrefix implements FilterFactory<ConsumerGroupPrefixConfig, ConsumerGroupPrefixConfig> {

    @Override
    public ConsumerGroupPrefixConfig initialize(FilterFactoryContext context, ConsumerGroupPrefixConfig config) throws PluginConfigurationException {
        Plugins.requireConfig(this, config);
        if (config.prefix() == null || config.prefix().isEmpty()) {
            throw new PluginConfigurationException("'prefix' must be a non-empty string");
        }
        return config;
    }

    @Override
    public ConsumerGroupPrefixFilter createFilter(FilterFactoryContext context, ConsumerGroupPrefixConfig config) {
        return new ConsumerGroupPrefixFilter(config, RewriteMetrics.forPrefix(config.prefix()));
    }
}
