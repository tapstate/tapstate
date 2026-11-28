package io.tapstate.core.model;

import io.tapstate.core.model.canonical.SourceCaptureIdentity;

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
        return SourceCaptureIdentity.resolve(source.connector(),
                settings(reference, source), source.srs() == null ? null : source.srs().key(), overrides(reference, source));
    }

    /** Independent captures retain their persisted coordinates for unchanged Pipeline definitions. */
    public static String directCoordinate(SourceRef reference, SourceResource source) {
        boolean qualified = overrides(reference, source).entrySet().stream().anyMatch(entry ->
                !source.config().containsKey(entry.getKey())
                        || !Objects.equals(source.config().get(entry.getKey()), entry.getValue()));
        return SourceCaptureIdentity.directCoordinate(source.connector(), settings(reference, source),
                source.srs() == null ? null : source.srs().key(), qualified);
    }

    /** The connector/configuration fingerprint used by preparation guards. */
    public static String physical(PipelineResource pipeline, SourceResource source) {
        return SourceCaptureIdentity.physical(source.connector(), settings(pipeline, source));
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

    /** Authored node values, including values that happen to equal a connection setting. */
    public static Map<String, Object> overrides(PipelineResource pipeline, SourceResource source) {
        return overrides(pipeline.sourceReference(source.id()), source);
    }

    public static Map<String, Object> overrides(SourceRef reference, SourceResource source) {
        if (!reference.id().equals(source.id())) throw new IllegalArgumentException("Source binding mismatch");
        return reference.nodeConfig();
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
