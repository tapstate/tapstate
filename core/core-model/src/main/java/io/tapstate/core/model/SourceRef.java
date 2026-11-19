package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One use of a shared Source in a Pipeline. */
@Doc("A source this pipeline reads from: a bare source id or a reference with local settings.")
public sealed interface SourceRef {

    /** Id of the referenced source. */
    String id();

    static Bare bare(String id) {
        return new Bare(id);
    }

    default Map<String, Object> nodeConfig() {
        return Map.of();
    }

    static SourceRef of(String id, Boolean srs, Map<String, Object> nodeConfig) {
        return srs == null && (nodeConfig == null || nodeConfig.isEmpty())
                ? bare(id) : new Spec(id, srs, nodeConfig);
    }

    static Spec spec(String id, Boolean srs) {
        return new Spec(id, srs);
    }

    /** Bare source id -- no srs switch recorded on this reference. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Bare(String id) implements SourceRef {
        public Bare {
            Objects.requireNonNull(id, "id");
        }
    }

    /** A Source reference with settings owned by the Pipeline. */
    @Doc("A source reference with optional Pipeline-owned settings.")
    record Spec(
            @Doc(value = "Id of the source this pipeline reads from.", required = true)
            String id,
            @Doc(value = "Whether this pipeline reads that source through the shared replay store. "
                    + "Materialized on apply when omitted.")
            Boolean srs,
            @Doc(value = "Connector node parameters for this Pipeline. Omitted fields inherit from the Source.",
                    key = "nodeConfig")
            Map<String, Object> nodeConfig)
            implements SourceRef {
        public Spec(String id, Boolean srs) {
            this(id, srs, Map.of());
        }

        public Spec {
            Objects.requireNonNull(id, "id");
            nodeConfig = nodeConfig == null ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(nodeConfig));
            if (srs == null && nodeConfig.isEmpty()) {
                throw new IllegalArgumentException("a Source reference with no settings must use the bare form");
            }
        }
    }
}
