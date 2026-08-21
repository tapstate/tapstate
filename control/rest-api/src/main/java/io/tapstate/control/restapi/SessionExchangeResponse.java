package io.tapstate.control.restapi;

import java.util.List;

/** A short-lived process credential minted from a valid opaque user session. */
record SessionExchangeResponse(
        String token,
        String accessExpiresAt,
        String issuer,
        String principal,
        List<String> scopes) {

    SessionExchangeResponse {
        scopes = List.copyOf(scopes);
    }
}
