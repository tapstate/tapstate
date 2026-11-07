package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/** Explicit validation semantics declared in the retained PDK connector specification. */
final class PdkValidationCapabilities {
    static final String EXACT_COUNT = "validation_exact_count_v1";
    static final String KEY_ORDER = "validation_key_order_v1";
    static final String STABLE_MODEL = "validation_stable_model_v1";

    private PdkValidationCapabilities() { }

    static boolean supports(ConnectorRef artifact, String id) {
        return definition(artifact, id) != null;
    }

    /** Only the connector may identify its table attributes that are observation statistics. */
    static Set<String> observedAttributes(ConnectorRef artifact) {
        var capability = definition(artifact, STABLE_MODEL);
        if (capability == null || !(capability.get("alternatives") instanceof List<?> attributes)) return Set.of();
        var keys = new HashSet<String>();
        for (var value : attributes) {
            if (!(value instanceof String key) || key.isBlank() || !keys.add(key)) {
                throw new IllegalArgumentException("Invalid PDK validation model attributes");
            }
        }
        return Set.copyOf(keys);
    }

    private static Map<?, ?> definition(ConnectorRef artifact, String id) {
        if (artifact.spec() == null
                || !(JsonReader.parse(artifact.spec()) instanceof Map<?, ?> spec)
                || !(spec.get("configOptions") instanceof Map<?, ?> config)
                || !(config.get("capabilities") instanceof List<?> capabilities)) return null;
        for (var value : capabilities) {
            if (value instanceof Map<?, ?> capability && id.equals(capability.get("id"))) return capability;
        }
        return null;
    }
}
