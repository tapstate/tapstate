package io.tapstate.control.core;

import java.util.Map;

/**
 * The audit facts an invoker supplies for one operation: {@code principal} (the authenticated subject
 * — a user id or a token id), {@code resourceId} (the target the operation acts on), and
 * {@code expectedContentHash} (the version the caller declared it was acting on, for the operations
 * that take a precondition), and {@code detail} (what the caller decided while asking, for the operations
 * that take decisions). The audit gate combines these with the operation's own id and the current
 * time to form the audit record, so the caller supplies only what the operation's own metadata cannot.
 */
public record AuditContext(
        String principal, String resourceId, String expectedContentHash, Map<String, Object> detail) {

    public AuditContext {
        requireText(principal, "principal");
        requireText(resourceId, "resourceId");
    }

    /** A context with no decisions to record, which is most of them. */
    public AuditContext(String principal, String resourceId, String expectedContentHash) {
        this(principal, resourceId, expectedContentHash, null);
    }

    /** A context for an operation that declares no version precondition, which is most of them. */
    public AuditContext(String principal, String resourceId) {
        this(principal, resourceId, null, null);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("audit context " + field + " must be non-blank");
        }
    }
}
