package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One entry in the control-plane audit log: the record written before an audited operation runs.
 *
 * <p>Fields — {@code timestamp} (when the operation was attempted), {@code principal} (the subject
 * that invoked it: a user id or a token id), {@code operationId} (the registry id of the operation,
 * e.g. {@code artifact.apply}), {@code resourceId} (the target the operation acts on), and
 * {@code expectedContentHash} (the version the caller declared it was acting on, where the operation
 * takes one; null otherwise), and {@code detail} (what the invoker decided while asking for it, where
 * the operation takes decisions -- the answers a start was given, say; null otherwise). These are the
 * fields knowable before the operation runs, which is what
 * the audit-before-execute guarantee needs. Richer fields the mature record carries — the originating
 * face, the outcome — attach where their determining step lands and are not modelled here.
 *
 * <p>{@code expectedContentHash} is what the caller <em>declared</em>, never what was found or what
 * was destroyed. The record is written before the operation runs, and confirming the version that was
 * actually removed happens inside the store's own atomic compare, which is after this point; an
 * attempt refused by that compare leaves a record whose declared version is, correctly, the stale one
 * the caller offered. It earns its place on a record that is otherwise all "who and what" because a
 * destroyed resource leaves nothing behind to compare an entry against later.
 *
 * <p>A pure value over {@code java..} only (rule R2): the port stays free of any face or store type.
 */
public record AuditRecord(
        Instant timestamp,
        String principal,
        String operationId,
        String resourceId,
        String expectedContentHash,
        Map<String, Object> detail) {

    /**
     * The most entries a record's detail keeps in any one list. A detail says what a person decided
     * while invoking the operation, and an audit log that grew with the size of a pipeline would be one
     * nobody can afford to keep; what is cut is counted rather than dropped silently.
     */
    public static final int DETAIL_LIST_LIMIT = 100;

    public AuditRecord {
        if (timestamp == null) {
            throw new IllegalArgumentException("audit record timestamp must be set");
        }
        requireText(principal, "principal");
        requireText(operationId, "operationId");
        requireText(resourceId, "resourceId");
        detail = detail == null || detail.isEmpty() ? null : bounded(detail);
    }

    /** A record with no detail beyond who did what to which version, which is most of them. */
    public AuditRecord(
            Instant timestamp, String principal, String operationId, String resourceId, String expectedContentHash) {
        this(timestamp, principal, operationId, resourceId, expectedContentHash, null);
    }

    /** A record for an operation that declares no version precondition, which is most of them. */
    public AuditRecord(Instant timestamp, String principal, String operationId, String resourceId) {
        this(timestamp, principal, operationId, resourceId, null, null);
    }

    /**
     * The detail with every list cut to {@link #DETAIL_LIST_LIMIT} entries, the number cut recorded beside
     * it under {@code <key>Omitted}.
     */
    private static Map<String, Object> bounded(Map<String, Object> detail) {
        Map<String, Object> kept = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : detail.entrySet()) {
            if (entry.getValue() instanceof List<?> list && list.size() > DETAIL_LIST_LIMIT) {
                kept.put(entry.getKey(), List.copyOf(list.subList(0, DETAIL_LIST_LIMIT)));
                kept.put(entry.getKey() + "Omitted", list.size() - DETAIL_LIST_LIMIT);
            } else {
                kept.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(kept);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("audit record " + field + " must be non-blank");
        }
    }
}
