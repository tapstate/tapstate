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

    /** Optional Pipeline-local name for an independently configured use of this Source. */
    default String alias() {
        return null;
    }

    default String binding() {
        return alias() == null ? id() : alias();
    }

    static SourceRef of(String id, Boolean srs, Map<String, Object> nodeConfig) {
        return of(id, srs, nodeConfig, null);
    }

    static SourceRef of(String id, Boolean srs, Map<String, Object> nodeConfig, String alias) {
        return srs == null && alias == null && (nodeConfig == null || nodeConfig.isEmpty())
                ? bare(id) : new Spec(id, srs, nodeConfig, alias);
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
            @Doc(value = "Connector node parameters for this Pipeline. Omitted fields use connector defaults when available.",
                    key = "nodeConfig")
            Map<String, Object> nodeConfig,
            @Doc(value = "Pipeline-local input name used in from references.", key = "as")
            String alias)
            implements SourceRef {
        public Spec(String id, Boolean srs) {
            this(id, srs, Map.of(), null);
        }

        public Spec(String id, Boolean srs, Map<String, Object> nodeConfig) {
            this(id, srs, nodeConfig, null);
        }

        public Spec {
            Objects.requireNonNull(id, "id");
            nodeConfig = nodeConfig == null ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(nodeConfig));
            if (alias != null && (alias.isBlank() || alias.contains("."))) {
                throw new IllegalArgumentException("source alias must be non-blank and contain no dot");
            }
            if (srs == null && alias == null && nodeConfig.isEmpty()) {
                throw new IllegalArgumentException("a Source reference with no settings must use the bare form");
            }
        }
    }
}
