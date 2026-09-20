package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code store} domain's error codes: reaching the backing store at startup. These are
 * user-facing, diagnosable failures — the operator supplied an invalid store setting, pointed the
 * server at a store that is not reachable, or used a standalone server when a replica-set is required
 * (the checkpoint compare-and-swap runs inside a multi-document transaction, which needs one).
 *
 * <p>Driver exceptions are translated into these coded diagnostics inside this module, so no
 * driver type escapes it (rule R3). {@code placeholders()} is the named-argument contract: every
 * throw site supplies a value for each name, and the build-time placeholder gate checks the
 * catalog templates against it.
 */
public enum StoreError implements TapstateErrorCode {

    /** The store could not be reached: {@code target} is the connection target that failed. */
    UNREACHABLE("store.unreachable", Set.of("target")),

    /** The store was reached but is not a replica-set: {@code target} is the connection target. */
    NOT_REPLICA_SET("store.not-replica-set", Set.of("target")),

    /** The configured durable operator-state database name is not valid for MongoDB. */
    INVALID_OPERATOR_STATE_DATABASE("store.invalid-operator-state-database", Set.of()),

    /**
     * The configured connection string is not a valid Mongo URI. Carries no placeholder on purpose:
     * echoing the raw URI back could leak an embedded credential.
     */
    INVALID_URI("store.invalid-uri", Set.of()),

    /**
     * The configured TLS CA certificate file could not be read or parsed. {@code path} is the CA
     * file path (a filesystem path, not a credential, so it is safe to echo back). Reached only when
     * TLS is enabled (opt-in via the URI) and a CA file is configured.
     */
    TLS_CA_UNREADABLE("store.tls-ca-unreadable", Set.of("path"));

    private final String code;
    private final Set<String> placeholders;

    StoreError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
