package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * The incremental login response. {@code token} remains the short-lived bearer field old clients already
 * read. Identity and expiry metadata accompany every new response; the three session fields appear only
 * when the request explicitly opted in to a persistent user session.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LoginResponse(
        String token,
        String accessExpiresAt,
        String issuer,
        String principal,
        List<String> scopes,
        String sessionToken,
        String sessionIdleExpiresAt,
        String sessionAbsoluteExpiresAt) {

    public LoginResponse {
        scopes = scopes == null ? null : List.copyOf(scopes);
    }

    /** Preserves the existing response until the session authority can supply the incremental metadata. */
    public LoginResponse(String token) {
        this(token, null, null, null, null, null, null, null);
    }
}
