package io.tapstate.spi.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One entry in a derived schema's history: a versioned snapshot of the columns a pipeline step works
 * out for itself, together with what it was worked out from. The history is append-only — a new
 * version is added when the derivation changes, never mutated in place — so a start can tell a schema
 * it has seen before from one that has moved under it.
 *
 * <p>Fields — {@code version} (the monotonic version, non-negative), {@code schema} (the derived
 * columns at this version, name to declared type, a shallow-unmodifiable defensive copy that preserves
 * column order), {@code statement} (a fingerprint of what the author wrote), {@code derivedFrom} (a
 * fingerprint of what the derivation read from the world), and {@code derivedBy} (the version of the
 * derivation itself).
 *
 * <p><b>Three provenance fields, because a schema that no longer matches is three different things and
 * they want three different answers.</b> The author edited the query, and the new shape is simply what
 * they asked for. The sources evolved under a query nobody edited, which is ordinary in a change-data
 * product and is the operator's call. Or the derivation itself started answering differently, which is
 * a compatibility break on our side. All three produce a byte-identical difference report, so nothing
 * in the difference can tell them apart — only a record of what the earlier answer was computed from
 * can, and that record has to keep the author's input and the world's input apart or an ordinary edit
 * reads as the sources having moved.
 *
 * <p>A pure value over {@code java..} only (rule R2).
 */
public record DerivedSchema(long version, Map<String, String> schema, String statement,
        String derivedFrom, String derivedBy) {

    public DerivedSchema {
        if (version < 0) {
            throw new IllegalArgumentException("derived schema version must be non-negative");
        }
        if (schema == null) {
            throw new IllegalArgumentException("derived schema columns must be set");
        }
        if (statement == null || statement.isBlank()) {
            throw new IllegalArgumentException("derived schema statement must be non-blank");
        }
        if (derivedFrom == null || derivedFrom.isBlank()) {
            throw new IllegalArgumentException("derived schema derivedFrom must be non-blank");
        }
        if (derivedBy == null || derivedBy.isBlank()) {
            throw new IllegalArgumentException("derived schema derivedBy must be non-blank");
        }
        schema = Collections.unmodifiableMap(new LinkedHashMap<>(schema));
    }
}
