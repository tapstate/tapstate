package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves one Pipeline's effective connector settings without changing the shared Source. */
public final class PipelineSourceConfig {
    private PipelineSourceConfig() {}

    /** Shared capture identity for this Pipeline's effective use of a Source. */
    public static String chain(PipelineResource pipeline, SourceResource source) {
        return chain(pipeline.sourceReference(source.id()), source);
    }

    public static String chain(SourceRef reference, SourceResource source) {
        return io.tapstate.core.model.canonical.SourceCaptureIdentity.resolve(source.connector(),
                settings(reference, source), source.srs() == null ? null : source.srs().key(), overrides(reference, source));
    }

    /** The connector/configuration fingerprint used by preparation guards. */
    public static String physical(PipelineResource pipeline, SourceResource source) {
        return io.tapstate.core.model.canonical.SourceCaptureIdentity.physical(source.connector(), settings(pipeline, source));
    }

    /** A detached runtime view used by connector operations that accept a Source value. */
    public static SourceResource effective(PipelineResource pipeline, SourceResource source) {
        return effective(pipeline.sourceReference(source.id()), source);
    }

    public static SourceResource effective(SourceRef reference, SourceResource source) {
        var settings = settings(reference, source);
        return settings == source.config() ? source : new SourceResource(source.id(), source.metadata(), source.connector(),
                settings, source.mode(), source.tables(), source.srs(), source.execution(), source.experimental());
    }

    /** Local values that change the connector's effective configuration. */
    public static Map<String, Object> overrides(PipelineResource pipeline, SourceResource source) {
        return overrides(pipeline.sourceReference(source.id()), source);
    }

    public static Map<String, Object> overrides(SourceRef reference, SourceResource source) {
        if (!reference.id().equals(source.id())) throw new IllegalArgumentException("Source binding mismatch");
        if (reference.nodeConfig().isEmpty()) return Map.of();
        var result = new LinkedHashMap<String, Object>();
        reference.nodeConfig().forEach((name, value) -> {
            if (!source.config().containsKey(name) || !Objects.equals(source.config().get(name), value)) {
                result.put(name, value);
            }
        });
        return Collections.unmodifiableMap(result);
    }

    /** Explicit local false and null values take precedence just as any other authored value does. */
    public static Map<String, Object> settings(PipelineResource pipeline, SourceResource source) {
        return settings(pipeline.sourceReference(source.id()), source);
    }

    public static Map<String, Object> settings(SourceRef reference, SourceResource source) {
        var overrides = overrides(reference, source);
        if (overrides.isEmpty()) return source.config();
        var result = new LinkedHashMap<>(source.config());
        result.putAll(overrides);
        return Collections.unmodifiableMap(result);
    }
}
