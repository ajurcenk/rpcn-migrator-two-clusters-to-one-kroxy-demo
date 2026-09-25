package demo.cgprefix;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for {@link ConsumerGroupPrefix}:
 * <pre>{@code
 * config:
 *   prefix: "a_"            # required, non-empty
 *   stripOnResponse: true   # default true
 *   filterListGroups: true  # default true
 * }</pre>
 * {@code stripOnResponse: false} is a debugging aid only: franz-go matches FindCoordinator v4+ and
 * OffsetFetch v8+ responses to its requests by group name, so clients break without stripping.
 */
public class ConsumerGroupPrefixConfig {

    private final String prefix;
    private final boolean stripOnResponse;
    private final boolean filterListGroups;

    public ConsumerGroupPrefixConfig(@JsonProperty(value = "prefix", required = true) String prefix,
                                     @JsonProperty("stripOnResponse") Boolean stripOnResponse,
                                     @JsonProperty("filterListGroups") Boolean filterListGroups) {
        this.prefix = prefix;
        this.stripOnResponse = stripOnResponse == null || stripOnResponse;
        this.filterListGroups = filterListGroups == null || filterListGroups;
    }

    public String prefix() {
        return prefix;
    }

    public boolean stripOnResponse() {
        return stripOnResponse;
    }

    /**
     * When true, ListGroups responses only contain groups carrying the prefix (with it stripped).
     * When false, ListGroups responses pass through untouched, showing raw backend names.
     */
    public boolean filterListGroups() {
        return filterListGroups;
    }
}
