package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;

import java.util.Map;

/**
 * Boundary validation for request bodies. A structurally malformed request — a missing required field left
 * null or blank — is a client-attributable, diagnosable error, so it is refused right at the HTTP boundary
 * with a coded {@link ControlError#MALFORMED_REQUEST} (a 400) carrying a human {@code reason}, rather than being
 * left to trip a bare invariant crash (a 500) deeper inside a control-core service. The service layer keeps
 * its own bare invariant guards for genuine programmer errors; this layer catches the client's malformed
 * input first, before the service call is made.
 */
final class MalformedRequest {

    private MalformedRequest() {
    }

    /** Refuses the request with {@code reason} unless {@code value} is present; returns it when it is. */
    static <T> T require(T value, String reason) {
        if (value == null) {
            throw refuse(reason, null);
        }
        return value;
    }

    /** Refuses the request with {@code reason} unless {@code value} is present and non-blank. */
    static void requireText(String value, String reason) {
        if (value == null || value.isBlank()) {
            throw refuse(reason, null);
        }
    }

    /**
     * The coded boundary refusal for {@code reason}, carrying {@code cause}, to throw where a specific
     * check (e.g. decoding a malformed body field) does not fit {@link #require}/{@link #requireText}.
     */
    static TapstateException rejecting(String reason, Throwable cause) {
        return refuse(reason, cause);
    }

    private static TapstateException refuse(String reason, Throwable cause) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), cause);
    }
}
