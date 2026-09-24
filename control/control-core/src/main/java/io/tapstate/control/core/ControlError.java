package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

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

    /**
     * A request was refused at the HTTP boundary because it is structurally malformed — a required field
     * left null or blank, a missing body — before it could reach a service; {@code reason} is a short,
     * human-readable statement of what was wrong (e.g. which field is required). This is a
     * client-attributable input error, deliberately distinct from a service-layer invariant violation (a
     * programmer bug), which stays a bare crash rather than being laundered into a coded error.
     */
    MALFORMED_REQUEST("control.malformed-request", Set.of("reason")),

    /**
     * An audited operation was refused because its mandatory audit record could not be written first;
     * {@code op} is the operation id. No audit, no execute — the operation never ran.
     */
    AUDIT_BLOCKED("control.audit-blocked", Set.of("op")),

    /** Artifacts committed, but one pipeline's model refresh was skipped or only partly completed. */
    SCHEMA_DERIVATION_INCOMPLETE("control.schema-derivation-incomplete",
            Set.of("pipeline", "causeCode", "causeParams")),

    /**
     * The zero-user bootstrap channel was reached from a non-loopback caller and refused. It carries no
     * placeholder on purpose: a remote caller is refused before the user table is consulted, so this
     * single answer — returned whether or not the server has been bootstrapped — leaks nothing about
     * that state. Distinct from {@code bootstrap-closed}, which a trusted loopback caller sees once an
     * admin already exists.
     */
    BOOTSTRAP_FORBIDDEN("control.bootstrap-forbidden", Set.of()),

    /**
     * The zero-user bootstrap channel was reached after an admin already exists and refused: the one
     * exception is closed for good the moment the user table stops being empty. Reached only by a
     * loopback caller — a remote one is turned away earlier with {@code bootstrap-forbidden}.
     */
    BOOTSTRAP_CLOSED("control.bootstrap-closed", Set.of()),

    /**
     * A login was rejected: the username does not exist, or the password did not match. One code
     * covers both cases and it carries no placeholder on purpose — echoing nothing back means the
     * failure cannot be used to tell an existing username from an absent one (no user enumeration).
     */
    AUTH_FAILED("control.auth-failed", Set.of()),

    /**
     * A protected operation was reached with no valid credential — none was presented, or the one
     * presented is malformed, unknown, revoked, expired or unsigned. It carries no placeholder on
     * purpose: the refusal is identical for every reason, so it cannot be used to probe which
     * credentials exist or why one failed. Distinct from {@code auth-failed}, which is the login
     * operation's own rejection of a username / password, and from {@code forbidden}, which is a known
     * caller who simply lacks the grade.
     */
    UNAUTHENTICATED("control.unauthenticated", Set.of()),

    /** The HTTP control service could not be reached within the configured request budget. */
    UNREACHABLE("control.unreachable", Set.of()),

    /**
     * An authenticated caller's credential does not carry the capability grade the operation requires;
     * {@code op} is the operation id and {@code required} the grade it needs. Distinct from
     * {@code auth-failed}, which is pre-authentication: here the caller is known, they simply lack the
     * grade, so echoing the operation and the grade it needs is a help, not an information leak.
     */
    FORBIDDEN("control.forbidden", Set.of("op", "required"));

    private final String code;
    private final Set<String> placeholders;

    ControlError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return this == SCHEMA_DERIVATION_INCOMPLETE ? Severity.WARNING : Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
