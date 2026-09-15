package io.tapstate.core.common;

import java.util.Set;

/**
 * The structural contract every first-party error code implements.
 *
 * <p>Each error code is an {@code enum} constant of an {@code enum implements TapstateErrorCode};
 * the constructor carries this metadata. Enums are chosen over the legacy annotation + runtime
 * reflection scanner because {@code values()} enumerates the whole code set without scanning and
 * GraalVM native-image needs zero reflection configuration for them — the system has zero
 * start-up cost (the head "never copy" item from the legacy design).
 *
 * <p>Message text lives elsewhere: the per-locale catalog in the presentation
 * layer, never here. This contract exposes only structural metadata.
 *
 * <p>Global uniqueness of {@link #code()} across enums and modules is <em>not</em> guaranteed by
 * the compiler (it only protects constant names within one enum) — it is a build-time gate in
 * {@code arch-tests}.
 */
public interface TapstateErrorCode {

    /** Canonical code {@code <domain>.<symbol>}; the single, stable external identity. */
    String code();

    /** Severity used by exit codes, structured output, and log levels. */
    Severity severity();

    /**
     * The names of the dynamic placeholders this code's message templates reference.
     * Throw sites must supply a named argument for each; the build-time
     * placeholder gate (once the catalog lands) checks templates against this contract.
     */
    Set<String> placeholders();
}
