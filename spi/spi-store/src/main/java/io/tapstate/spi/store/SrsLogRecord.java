package io.tapstate.spi.store;

import io.tapstate.core.event.Op;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One change as the durable change log holds it: the source's own position token, the change kind and
 * event time, the before/after row images, and the schema version in force. The sequence it sits at is
 * the log's key rather than a field of the record -- the ring assigns it, and the store writes it as
 * half of the key.
 *
 * <p>The position travels as its opaque token, never as a connector object. A record written by one run
 * is read back by another, possibly a later build, and only the connector that issued the offset can
 * interpret it. The token is null on a change the source stated no position at: a source names one
 * position for a run of changes, so only the change that closes such a run carries one.
 *
 * <p>Which row image is present follows the op -- an insert carries {@code after}, a delete
 * {@code before}, an update both, a ddl neither. An absent image is null; a present one is a
 * shallow-unmodifiable defensive copy. A snapshot read (op {@code r}) never enters the change log and is
 * rejected here by construction, the same way the ring rejects it: the log holds what the ring held.
 */
public record SrsLogRecord(
        String srcToken,
        Op op,
        long ts,
        Map<String, Object> before,
        Map<String, Object> after,
        long schemaVer) {

    public SrsLogRecord {
        Objects.requireNonNull(op, "op");
        if (op == Op.READ) {
            throw new IllegalArgumentException("a snapshot read (op r) never enters the change log");
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
