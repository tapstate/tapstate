package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * The login request body: the username and password to verify, plus an explicit opt-in for a revocable user
 * session. Omitting {@code createSession}, as every older client does, is exactly the same as sending false
 * and continues to request only a short-lived access token. The raw password is used only to verify and is
 * never stored or echoed.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public record LoginRequest(String username, String password, Boolean createSession) {

    public LoginRequest {
        createSession = Boolean.TRUE.equals(createSession);
    }

    /** Preserves the source and wire behavior of clients that predate persistent user sessions. */
    public LoginRequest(String username, String password) {
        this(username, password, false);
    }
}
