package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.RequestErrorDefinitions;

import java.util.Set;

/**
 * The {@code control} domain's error codes. These are user-facing, diagnosable failures of the
 * control layer, carried through the error-code system and rendered through the shared message
 * catalog.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum ControlError implements TapstateErrorCode {

    AI_NOT_CONFIGURED(RequestErrorDefinitions.AI_NOT_CONFIGURED),
    AI_PROVIDER_AUTH_FAILED(RequestErrorDefinitions.AI_PROVIDER_AUTH_FAILED),
    AI_PROVIDER_REQUEST_REJECTED(RequestErrorDefinitions.AI_PROVIDER_REQUEST_REJECTED),
    AI_PROVIDER_RATE_LIMITED(RequestErrorDefinitions.AI_PROVIDER_RATE_LIMITED),
    AI_PROVIDER_FAILED(RequestErrorDefinitions.AI_PROVIDER_FAILED),
    AI_PROVIDER_OUTPUT_LIMIT(RequestErrorDefinitions.AI_PROVIDER_OUTPUT_LIMIT),
    AI_INVALID_PROPOSAL(RequestErrorDefinitions.AI_INVALID_PROPOSAL),
    AI_OVERLOADED(RequestErrorDefinitions.AI_OVERLOADED),
    /** Structurally invalid client input; programmer invariant violations remain bare failures. */
    MALFORMED_REQUEST(RequestErrorDefinitions.MALFORMED_REQUEST),

    /** A valid preview request could not enter the bounded candidate-compilation pool. */
    PREVIEW_OVERLOADED(RequestErrorDefinitions.PREVIEW_OVERLOADED),

    /**
     * An audited operation was refused because its mandatory audit record could not be written first;
     * {@code op} is the operation id. No audit, no execute — the operation never ran.
     */
    AUDIT_BLOCKED(RequestErrorDefinitions.AUDIT_BLOCKED),

    /** Artifacts committed, but one pipeline's model refresh was skipped or only partly completed. */
    SCHEMA_DERIVATION_INCOMPLETE(RequestErrorDefinitions.SCHEMA_DERIVATION_INCOMPLETE),

    /**
     * The zero-user bootstrap channel was reached from a non-loopback caller and refused. It carries no
     * placeholder on purpose: a remote caller is refused before the user table is consulted, so this
     * single answer — returned whether or not the server has been bootstrapped — leaks nothing about
     * that state. Distinct from {@code bootstrap-closed}, which a trusted loopback caller sees once an
     * admin already exists.
     */
    BOOTSTRAP_FORBIDDEN(RequestErrorDefinitions.BOOTSTRAP_FORBIDDEN),

    /**
     * The zero-user bootstrap channel was reached after an admin already exists and refused: the one
     * exception is closed for good the moment the user table stops being empty. Reached only by a
     * loopback caller — a remote one is turned away earlier with {@code bootstrap-forbidden}.
     */
    BOOTSTRAP_CLOSED(RequestErrorDefinitions.BOOTSTRAP_CLOSED),

    /**
     * A login was rejected: the username does not exist, or the password did not match. One code
     * covers both cases and it carries no placeholder on purpose — echoing nothing back means the
     * failure cannot be used to tell an existing username from an absent one (no user enumeration).
     */
    AUTH_FAILED(RequestErrorDefinitions.AUTH_FAILED),

    /** The caller reached an authentication flow that the selected deployment mode does not expose. */
    AUTH_MODE_UNAVAILABLE(RequestErrorDefinitions.AUTH_MODE_UNAVAILABLE),

    /** The configured browser authentication public-origin allow-list is invalid. */
    BROWSER_AUTH_CONFIG_INVALID(RequestErrorDefinitions.BROWSER_AUTH_CONFIG_INVALID),

    /** Managed authentication was reached before its real SDK/validated deployment binding was installed. */
    CLOUD_AUTH_UNAVAILABLE(RequestErrorDefinitions.CLOUD_AUTH_UNAVAILABLE),

    /** A Cloud author attempted to select the deployment-owned Nest state database. */
    STATE_DATABASE_UNAVAILABLE(RequestErrorDefinitions.STATE_DATABASE_UNAVAILABLE),

    /** The reserved Source plaintext flow has no production authorizer in this release. */
    SOURCE_CONFIG_REVEAL_UNAVAILABLE(RequestErrorDefinitions.SOURCE_CONFIG_REVEAL_UNAVAILABLE),

    /**
     * A protected operation was reached with no valid credential — none was presented, or the one
     * presented is malformed, unknown, revoked, expired or unsigned. It carries no placeholder on
     * purpose: the refusal is identical for every reason, so it cannot be used to probe which
     * credentials exist or why one failed. Distinct from {@code auth-failed}, which is the login
     * operation's own rejection of a username / password, and from {@code forbidden}, which is a known
     * caller who simply lacks the grade.
     */
    UNAUTHENTICATED(RequestErrorDefinitions.UNAUTHENTICATED),

    /** The HTTP control service could not be reached within the configured request budget. */
    UNREACHABLE(RequestErrorDefinitions.UNREACHABLE),

    /**
     * An authenticated caller's credential does not carry the capability grade the operation requires;
     * {@code op} is the operation id and {@code required} the grade it needs. Distinct from
     * {@code auth-failed}, which is pre-authentication: here the caller is known, they simply lack the
     * grade, so echoing the operation and the grade it needs is a help, not an information leak.
     */
    FORBIDDEN(RequestErrorDefinitions.FORBIDDEN),

    STORAGE_DATABASES_OVERLAP(RequestErrorDefinitions.STORAGE_DATABASES_OVERLAP),
    STORAGE_TEST_REQUIRED(RequestErrorDefinitions.STORAGE_TEST_REQUIRED),
    STORAGE_TEST_FAILED(RequestErrorDefinitions.STORAGE_TEST_FAILED),
    /** A fresh CACHE candidate contains one or more collections. */
    STORAGE_CACHE_DATABASE_OCCUPIED(RequestErrorDefinitions.STORAGE_CACHE_DATABASE_OCCUPIED),
    /** An occupied CACHE candidate has a separately checked empty database suggestion. */
    STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED(RequestErrorDefinitions.STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED),
    STORAGE_SCOPE_CONFLICT(RequestErrorDefinitions.STORAGE_SCOPE_CONFLICT),
    STORAGE_UNAVAILABLE(RequestErrorDefinitions.STORAGE_UNAVAILABLE),
    STORAGE_QUOTA_EXCEEDED(RequestErrorDefinitions.STORAGE_QUOTA_EXCEEDED);

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    ControlError(ProductErrorDefinition definition) {
        this.code = definition.code();
        this.placeholders = definition.parameters();
        this.severity = definition.severity();
    }


    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
