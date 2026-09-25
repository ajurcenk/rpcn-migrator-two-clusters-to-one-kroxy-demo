package demo.cgprefix;

import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerGroupPrefixTest {

    @Test
    void isRegisteredAsFilterFactory() {
        assertThat(ServiceLoader.load(FilterFactory.class).stream().map(ServiceLoader.Provider::type))
                .contains(ConsumerGroupPrefix.class);
    }

    @Test
    void optionalSettingsDefaultToTrue() {
        var config = new ConsumerGroupPrefixConfig("a_", null, null);
        assertThat(config.stripOnResponse()).isTrue();
        assertThat(config.filterListGroups()).isTrue();
    }

    @Test
    void rejectsMissingOrEmptyPrefix() {
        var factory = new ConsumerGroupPrefix();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class);
        assertThatThrownBy(() -> factory.initialize(null, new ConsumerGroupPrefixConfig(null, null, null))).isInstanceOf(PluginConfigurationException.class);
        assertThatThrownBy(() -> factory.initialize(null, new ConsumerGroupPrefixConfig("", null, null))).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void createsFilter() {
        var factory = new ConsumerGroupPrefix();
        var config = factory.initialize(null, new ConsumerGroupPrefixConfig("a_", null, false));
        assertThat(factory.createFilter(null, config)).isInstanceOf(ConsumerGroupPrefixFilter.class);
    }
}
