package io.tapstate.runtime.srs;

import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One cdc change as it sits in the per-table change ring: the source position it was captured at, the
 * change kind and event time, the before/after row images, and the schema version in force. The ring
 * assigns each item a monotonic sequence on append — that sequence is the consumers' read cursor, held
 * by the ring itself and not carried in the item.
 *
 * <p>The ring holds only cdc mutations: ops {@code i} / {@code u} / {@code d} / {@code ddl}. A snapshot
 * read (op {@code r}) goes straight to the sink and is rejected here by construction. Which row image is
 * present follows the op, exactly as in the event envelope this is projected from: an insert carries
 * {@code after}, a delete {@code before}, an update both, a ddl neither. An absent image is
 * {@code null}; a present image is a shallow-unmodifiable defensive copy. The {@code schemaVer} points
 * into the mining chain's schema history rather than repeating the schema in every item, keeping the
 * hot buffer small.
 *
 * <p>An immutable value. The source position travels as its opaque token across any persistence
 * boundary, never as a connector object.
 *
 * <p>{@code srcPos} is null on a change the source stated no position at. A source names one position
 * for a run of changes, meaning "everything up to here has been handed over", so only the change that
 * closes such a run carries one. Filling the others in with it would say a change had been read that had
 * not, and a run interrupted between them would resume past changes it never delivered. Nothing here
 * ranks by the position anyway: ordering is the ring's own sequence paired with its generation.
 */
public record SrsItem(
        SourcePosition srcPos,
        Op op,
        long ts,
        Map<String, Object> before,
        Map<String, Object> after,
        long schemaVer) {

    public SrsItem {
        Objects.requireNonNull(op, "op");
        if (op == Op.READ) {
            throw new IllegalArgumentException("a snapshot read (op r) never enters the change ring");
        }
        if (schemaVer < 0) {
            throw new IllegalArgumentException("schemaVer must be non-negative");
        }
        before = copyOrNull(before);
        after = copyOrNull(after);
    }

    private static Map<String, Object> copyOrNull(Map<String, Object> map) {
        return map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }
}
